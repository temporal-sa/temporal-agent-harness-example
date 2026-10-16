from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import httpx
from mcp.client.auth import OAuthClientProvider, TokenStorage
from mcp.client.auth.utils import (
    build_oauth_authorization_server_metadata_discovery_urls,
    build_protected_resource_metadata_discovery_urls,
    create_client_info_from_metadata_url,
    create_client_registration_request,
    create_oauth_metadata_request,
    get_client_metadata_scopes,
    handle_auth_metadata_response,
    handle_protected_resource_response,
    handle_registration_response,
    should_use_client_metadata_url,
)
from mcp.client.streamable_http import MCP_PROTOCOL_VERSION
from mcp.shared.auth import (
    OAuthClientInformationFull,
    OAuthClientMetadata,
    OAuthMetadata,
    OAuthToken,
    ProtectedResourceMetadata,
)
from mcp.types import LATEST_PROTOCOL_VERSION

from simple_chat_agent.common.mcp_auth import mcp_oauth_provider
from simple_chat_agent.common.store import AppStore, OAuthConnectionRecord


_TOKEN_REFRESH_SKEW = timedelta(seconds=60)


@dataclass
class PendingMcpOAuthFlow:
    user_id: str
    server_id: str
    server_url: str
    tool_prefix: str
    label: str
    flow_id: str = field(default_factory=lambda: uuid4().hex)
    auth_url: str | None = None
    start_error: str | None = None
    auth_url_ready: asyncio.Event = field(default_factory=asyncio.Event)
    callback: asyncio.Future[tuple[str, str | None]] | None = None
    task: asyncio.Task[Any] | None = None

    async def redirect(self, url: str) -> None:
        self.auth_url = url
        self.auth_url_ready.set()

    async def wait_for_callback(self) -> tuple[str, str | None]:
        if self.callback is None:
            self.callback = asyncio.get_running_loop().create_future()
        return await self.callback

    def complete(self, code: str, state: str | None) -> None:
        if self.callback is None:
            self.callback = asyncio.get_running_loop().create_future()
        if not self.callback.done():
            self.callback.set_result((code, state))

    def fail(self, error: BaseException) -> None:
        self.start_error = str(error)
        self.auth_url_ready.set()
        if self.callback is None:
            self.callback = asyncio.get_running_loop().create_future()
        if not self.callback.done():
            self.callback.set_exception(error)


class AppMcpTokenStorage(TokenStorage):
    def __init__(self, *, store: AppStore, user_id: str, server_id: str) -> None:
        self._store = store
        self._user_id = user_id
        self._provider = mcp_oauth_provider(server_id)

    async def get_tokens(self) -> OAuthToken | None:
        connection = self._store.get_oauth_connection(
            user_id=self._user_id,
            provider=self._provider,
        )
        if connection is None:
            return None
        token_payload = connection.metadata.get("oauth_token")
        if isinstance(token_payload, dict):
            return OAuthToken.model_validate(token_payload)
        if connection.access_token:
            return OAuthToken(
                access_token=connection.access_token,
                token_type="Bearer",
                scope=connection.scope or None,
            )
        return None

    async def set_tokens(self, tokens: OAuthToken) -> None:
        metadata = await self._metadata()
        tokens = _merge_existing_token_fields(tokens, metadata.get("oauth_token"))
        metadata["oauth_token"] = tokens.model_dump(mode="json")
        metadata["oauth_token_updated_at"] = _utc_now().isoformat()
        expires_at = _token_expires_at(tokens)
        if expires_at is None:
            metadata.pop("oauth_token_expires_at", None)
        else:
            metadata["oauth_token_expires_at"] = expires_at.isoformat()
        connection = self._store.get_oauth_connection(
            user_id=self._user_id,
            provider=self._provider,
        )
        self._store.upsert_oauth_connection(
            user_id=self._user_id,
            provider=self._provider,
            access_token=tokens.access_token,
            token_type=tokens.token_type,
            scope=tokens.scope or "",
            provider_user_id=connection.provider_user_id if connection else None,
            provider_user_login=connection.provider_user_login if connection else None,
            metadata=metadata,
        )

    async def get_token_refresh_time(self) -> float | None:
        connection = self._store.get_oauth_connection(
            user_id=self._user_id,
            provider=self._provider,
        )
        if connection is None:
            return None
        token = await self.get_tokens()
        if token is None:
            return None
        expires_at = _stored_token_expires_at(connection, token)
        if expires_at is None:
            return None
        return (expires_at - _TOKEN_REFRESH_SKEW).timestamp()

    async def get_client_info(self) -> OAuthClientInformationFull | None:
        metadata = await self._metadata()
        client_info = metadata.get("client_info")
        if isinstance(client_info, dict):
            return OAuthClientInformationFull.model_validate(client_info)
        return None

    async def set_client_info(self, client_info: OAuthClientInformationFull) -> None:
        metadata = await self._metadata()
        metadata["client_info"] = client_info.model_dump(mode="json")
        connection = self._store.get_oauth_connection(
            user_id=self._user_id,
            provider=self._provider,
        )
        self._store.upsert_oauth_connection(
            user_id=self._user_id,
            provider=self._provider,
            access_token=connection.access_token if connection else "",
            token_type=connection.token_type if connection else "Bearer",
            scope=connection.scope if connection else "",
            provider_user_id=connection.provider_user_id if connection else None,
            provider_user_login=connection.provider_user_login if connection else None,
            metadata=metadata,
        )

    async def get_oauth_context(
        self,
    ) -> tuple[ProtectedResourceMetadata | None, OAuthMetadata | None, str | None]:
        metadata = await self._metadata()
        protected_resource_payload = metadata.get("protected_resource_metadata")
        oauth_metadata_payload = metadata.get("oauth_metadata")
        protected_resource_metadata = (
            ProtectedResourceMetadata.model_validate(protected_resource_payload)
            if isinstance(protected_resource_payload, dict)
            else None
        )
        oauth_metadata = (
            OAuthMetadata.model_validate(oauth_metadata_payload)
            if isinstance(oauth_metadata_payload, dict)
            else None
        )
        auth_server_url = metadata.get("auth_server_url")
        return (
            protected_resource_metadata,
            oauth_metadata,
            auth_server_url if isinstance(auth_server_url, str) else None,
        )

    async def set_oauth_context(
        self,
        *,
        protected_resource_metadata: ProtectedResourceMetadata | None,
        oauth_metadata: OAuthMetadata | None,
        auth_server_url: str | None,
    ) -> None:
        metadata = await self._metadata()
        if protected_resource_metadata is None:
            metadata.pop("protected_resource_metadata", None)
        else:
            metadata["protected_resource_metadata"] = (
                protected_resource_metadata.model_dump(mode="json")
            )
        if oauth_metadata is None:
            metadata.pop("oauth_metadata", None)
        else:
            metadata["oauth_metadata"] = oauth_metadata.model_dump(mode="json")
        if auth_server_url:
            metadata["auth_server_url"] = auth_server_url
        else:
            metadata.pop("auth_server_url", None)

        connection = self._store.get_oauth_connection(
            user_id=self._user_id,
            provider=self._provider,
        )
        self._store.upsert_oauth_connection(
            user_id=self._user_id,
            provider=self._provider,
            access_token=connection.access_token if connection else "",
            token_type=connection.token_type if connection else "Bearer",
            scope=connection.scope if connection else "",
            provider_user_id=connection.provider_user_id if connection else None,
            provider_user_login=connection.provider_user_login if connection else None,
            metadata=metadata,
        )

    async def _metadata(self) -> dict[str, Any]:
        connection = self._store.get_oauth_connection(
            user_id=self._user_id,
            provider=self._provider,
        )
        return dict(connection.metadata) if connection is not None else {}


class AppMcpOAuthClientProvider(OAuthClientProvider):
    async def _initialize(self) -> None:
        await super()._initialize()
        storage = self.context.storage
        if not isinstance(storage, AppMcpTokenStorage):
            return

        self.context.token_expiry_time = await storage.get_token_refresh_time()
        (
            self.context.protected_resource_metadata,
            self.context.oauth_metadata,
            self.context.auth_server_url,
        ) = await storage.get_oauth_context()
        if (
            self.context.oauth_metadata is None
            and not self.context.is_token_valid()
            and self.context.can_refresh_token()
        ):
            async with httpx.AsyncClient() as client:
                await _discover_oauth_metadata(self, client)
            await _persist_oauth_context(self)

    async def _handle_refresh_response(self, response: httpx.Response) -> bool:
        refreshed = await super()._handle_refresh_response(response)
        storage = self.context.storage
        if refreshed and isinstance(storage, AppMcpTokenStorage):
            self.context.current_tokens = await storage.get_tokens()
            self.context.token_expiry_time = await storage.get_token_refresh_time()
        return refreshed


def mcp_redirect_uri(flow_id: str) -> str:
    base = os.environ.get("SIMPLE_CHAT_PUBLIC_URL", "http://127.0.0.1:8000").rstrip("/")
    return f"{base}/oauth/mcp/callback?flow_id={flow_id}"


def mcp_oauth_provider_for_flow(
    *,
    flow: PendingMcpOAuthFlow,
    store: AppStore,
    redirect_uri: str | None = None,
) -> OAuthClientProvider:
    return AppMcpOAuthClientProvider(
        server_url=flow.server_url,
        client_metadata=OAuthClientMetadata(
            redirect_uris=[redirect_uri or mcp_redirect_uri(flow.flow_id)],
            client_name="Temporal Agent Harness Example",
            scope=None,
        ),
        storage=AppMcpTokenStorage(
            store=store,
            user_id=flow.user_id,
            server_id=flow.server_id,
        ),
        redirect_handler=flow.redirect,
        callback_handler=flow.wait_for_callback,
    )


async def authorize_mcp_oauth_flow(
    *,
    flow: PendingMcpOAuthFlow,
    store: AppStore,
    redirect_uri: str | None = None,
    http_client: httpx.AsyncClient | None = None,
) -> None:
    """Run one MCP OAuth authorization. ``redirect_uri`` overrides the old app's callback
    URL; ``http_client`` (e.g. one whose transport applies the SSRF guard) carries every
    discovery, registration and token request."""
    provider = mcp_oauth_provider_for_flow(flow=flow, store=store, redirect_uri=redirect_uri)

    # The MCP SDK currently starts OAuth from an HTTPX 401/403 auth flow. Some
    # servers expose list_tools anonymously, so the app needs an explicit path.
    await provider._initialize()
    provider.context.protocol_version = LATEST_PROTOCOL_VERSION

    async with http_client or httpx.AsyncClient() as client:
        await _discover_oauth_metadata(provider, client)
        await _persist_oauth_context(provider)
        provider.context.client_metadata.scope = get_client_metadata_scopes(
            None,
            provider.context.protected_resource_metadata,
            provider.context.oauth_metadata,
        )
        await _ensure_client_info(provider, client)

        token_request = await provider._perform_authorization()
        token_response = await client.send(token_request)
        await provider._handle_token_response(token_response)


def mcp_oauth_provider_for_connection(
    *,
    connection: OAuthConnectionRecord,
    server_url: str,
    store: AppStore,
) -> OAuthClientProvider:
    server_id = _server_id_from_provider(connection.provider)
    return AppMcpOAuthClientProvider(
        server_url=server_url,
        client_metadata=OAuthClientMetadata(
            redirect_uris=[mcp_redirect_uri("reauthorize")],
            client_name="Temporal Agent Harness Example",
            scope=None,
        ),
        storage=AppMcpTokenStorage(
            store=store,
            user_id=connection.user_id,
            server_id=server_id,
        ),
        redirect_handler=_raise_reauthorization_required,
        callback_handler=_raise_reauthorization_required_callback,
    )


async def _discover_oauth_metadata(
    provider: OAuthClientProvider,
    client: httpx.AsyncClient,
) -> None:
    context = provider.context
    headers = {MCP_PROTOCOL_VERSION: LATEST_PROTOCOL_VERSION}

    for url in build_protected_resource_metadata_discovery_urls(
        None,
        context.server_url,
    ):
        response = await client.get(url, headers=headers)
        protected_resource_metadata = await handle_protected_resource_response(
            response
        )
        if protected_resource_metadata is None:
            continue

        await provider._validate_resource_match(protected_resource_metadata)
        context.protected_resource_metadata = protected_resource_metadata
        if protected_resource_metadata.authorization_servers:
            context.auth_server_url = str(
                protected_resource_metadata.authorization_servers[0]
            )
        break

    for url in build_oauth_authorization_server_metadata_discovery_urls(
        context.auth_server_url,
        context.server_url,
    ):
        response = await client.send(create_oauth_metadata_request(url))
        ok, oauth_metadata = await handle_auth_metadata_response(response)
        if not ok:
            break
        if oauth_metadata is not None:
            context.oauth_metadata = oauth_metadata
            break


async def _ensure_client_info(
    provider: OAuthClientProvider,
    client: httpx.AsyncClient,
) -> None:
    context = provider.context
    if context.client_info is not None:
        return

    if should_use_client_metadata_url(
        context.oauth_metadata,
        context.client_metadata_url,
    ):
        client_info = create_client_info_from_metadata_url(
            context.client_metadata_url,
            redirect_uris=context.client_metadata.redirect_uris,
        )
    else:
        registration_request = create_client_registration_request(
            context.oauth_metadata,
            context.client_metadata,
            context.get_authorization_base_url(context.server_url),
        )
        registration_response = await client.send(registration_request)
        client_info = await handle_registration_response(registration_response)

    context.client_info = client_info
    await context.storage.set_client_info(client_info)


async def _persist_oauth_context(provider: OAuthClientProvider) -> None:
    storage = provider.context.storage
    if not isinstance(storage, AppMcpTokenStorage):
        return
    await storage.set_oauth_context(
        protected_resource_metadata=provider.context.protected_resource_metadata,
        oauth_metadata=provider.context.oauth_metadata,
        auth_server_url=provider.context.auth_server_url,
    )


def _server_id_from_provider(provider: str) -> str:
    if not provider.startswith("mcp:"):
        raise ValueError(f"Connection is not an MCP connection: {provider}")
    server_id = provider.removeprefix("mcp:")
    if not server_id:
        raise ValueError("MCP connection provider is missing a server id.")
    return server_id


def _merge_existing_token_fields(
    tokens: OAuthToken,
    existing_payload: Any,
) -> OAuthToken:
    if not isinstance(existing_payload, dict):
        return tokens

    token_payload = tokens.model_dump(mode="json")
    for field_name in ("refresh_token", "scope"):
        if token_payload.get(field_name):
            continue
        existing_value = existing_payload.get(field_name)
        if existing_value:
            token_payload[field_name] = existing_value
    return OAuthToken.model_validate(token_payload)


def _token_expires_at(tokens: OAuthToken) -> datetime | None:
    if tokens.expires_in is None:
        return None
    return _utc_now() + timedelta(seconds=int(tokens.expires_in))


def _stored_token_expires_at(
    connection: OAuthConnectionRecord,
    token: OAuthToken,
) -> datetime | None:
    metadata_expires_at = _parse_datetime(
        connection.metadata.get("oauth_token_expires_at")
    )
    if metadata_expires_at is not None:
        return metadata_expires_at

    if token.expires_in is None:
        return None

    stored_at = _parse_datetime(connection.metadata.get("oauth_token_updated_at"))
    if stored_at is None:
        stored_at = _parse_datetime(connection.updated_at)
    if stored_at is None:
        return None
    return stored_at + timedelta(seconds=int(token.expires_in))


def _parse_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _utc_now() -> datetime:
    return datetime.now(UTC)


async def _raise_reauthorization_required(url: str) -> None:
    raise RuntimeError("MCP OAuth token requires reauthorization.")


async def _raise_reauthorization_required_callback() -> tuple[str, str | None]:
    raise RuntimeError("MCP OAuth token requires reauthorization.")
