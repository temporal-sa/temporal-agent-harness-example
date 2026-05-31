from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from simple_chat_agent.common.store import app_store
from simple_chat_agent.common.store import OAuthConnectionRecord


def mcp_oauth_provider(server_id: str) -> str:
    return f"mcp:{server_id}"


def resolve_mcp_auth_headers(auth_ref: str) -> Mapping[str, str]:
    connection = _connection_for_auth_ref(auth_ref)
    if connection is None or not connection.access_token:
        raise RuntimeError("MCP auth connection was not found.")
    if _is_oauth_connection(connection.metadata):
        return {}

    token_type = connection.token_type or "Bearer"
    return {"Authorization": f"{token_type} {connection.access_token}"}


def resolve_mcp_http_auth(auth_ref: str, server_url: str) -> Any | None:
    from simple_chat_agent.common.mcp_oauth import mcp_oauth_provider_for_connection

    connection = _connection_for_auth_ref(auth_ref)
    if connection is None or not connection.access_token:
        raise RuntimeError("MCP auth connection was not found.")
    if not _is_oauth_connection(connection.metadata):
        return None

    return mcp_oauth_provider_for_connection(
        connection=connection,
        server_url=server_url,
        store=app_store(),
    )


def _connection_for_auth_ref(auth_ref: str) -> OAuthConnectionRecord | None:
    store = app_store()
    connection = store.get_oauth_connection_by_id(auth_ref)
    if connection is not None:
        return connection

    for provider in _provider_candidates(auth_ref):
        connection = store.get_oauth_connection_by_provider(provider)
        if connection is not None:
            return connection

    return None


def _provider_candidates(auth_ref: str) -> list[str]:
    candidates: list[str] = []
    if auth_ref.startswith("mcp:"):
        candidates.append(auth_ref)
        if "_" in auth_ref:
            candidates.append(auth_ref.rsplit("_", 1)[0])
    elif auth_ref.startswith("mcp-"):
        candidates.append(mcp_oauth_provider(auth_ref))
    return _dedupe(candidates)


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def _is_oauth_connection(metadata: dict[str, Any]) -> bool:
    return "oauth_token" in metadata or "client_info" in metadata
