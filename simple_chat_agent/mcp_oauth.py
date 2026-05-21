from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4

from mcp.client.auth import OAuthClientProvider, TokenStorage
from mcp.shared.auth import (
    OAuthClientInformationFull,
    OAuthClientMetadata,
    OAuthToken,
)

from simple_chat_agent.mcp_auth import mcp_oauth_provider
from simple_chat_agent.store import AppStore, OAuthConnectionRecord


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
        metadata["oauth_token"] = tokens.model_dump(mode="json")
        self._store.upsert_oauth_connection(
            user_id=self._user_id,
            provider=self._provider,
            access_token=tokens.access_token,
            token_type=tokens.token_type,
            scope=tokens.scope or "",
            provider_user_id=None,
            provider_user_login=None,
            metadata=metadata,
        )

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
            provider_user_id=None,
            provider_user_login=None,
            metadata=metadata,
        )

    async def _metadata(self) -> dict[str, Any]:
        connection = self._store.get_oauth_connection(
            user_id=self._user_id,
            provider=self._provider,
        )
        return dict(connection.metadata) if connection is not None else {}


def mcp_redirect_uri(flow_id: str) -> str:
    base = os.environ.get("SIMPLE_CHAT_PUBLIC_URL", "http://127.0.0.1:8000").rstrip("/")
    return f"{base}/oauth/mcp/callback?flow_id={flow_id}"


def mcp_oauth_provider_for_flow(
    *,
    flow: PendingMcpOAuthFlow,
    store: AppStore,
) -> OAuthClientProvider:
    return OAuthClientProvider(
        server_url=flow.server_url,
        client_metadata=OAuthClientMetadata(
            redirect_uris=[mcp_redirect_uri(flow.flow_id)],
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


def mcp_oauth_provider_for_connection(
    *,
    connection: OAuthConnectionRecord,
    server_url: str,
    store: AppStore,
) -> OAuthClientProvider:
    server_id = _server_id_from_provider(connection.provider)
    return OAuthClientProvider(
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


def _server_id_from_provider(provider: str) -> str:
    if not provider.startswith("mcp:"):
        raise ValueError(f"Connection is not an MCP connection: {provider}")
    server_id = provider.removeprefix("mcp:")
    if not server_id:
        raise ValueError("MCP connection provider is missing a server id.")
    return server_id


async def _raise_reauthorization_required(url: str) -> None:
    raise RuntimeError("MCP OAuth token requires reauthorization.")


async def _raise_reauthorization_required_callback() -> tuple[str, str | None]:
    raise RuntimeError("MCP OAuth token requires reauthorization.")
