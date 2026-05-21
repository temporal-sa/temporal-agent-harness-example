from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from simple_chat_agent.store import app_store


def mcp_oauth_provider(server_id: str) -> str:
    return f"mcp:{server_id}"


def resolve_mcp_auth_headers(connection_id: str) -> Mapping[str, str]:
    connection = app_store().get_oauth_connection_by_id(connection_id)
    if connection is None or not connection.access_token:
        raise RuntimeError("MCP auth connection was not found.")
    if _is_oauth_connection(connection.metadata):
        return {}

    token_type = connection.token_type or "Bearer"
    return {"Authorization": f"{token_type} {connection.access_token}"}


def resolve_mcp_http_auth(connection_id: str, server_url: str) -> Any | None:
    from simple_chat_agent.mcp_oauth import mcp_oauth_provider_for_connection

    connection = app_store().get_oauth_connection_by_id(connection_id)
    if connection is None or not connection.access_token:
        raise RuntimeError("MCP auth connection was not found.")
    if not _is_oauth_connection(connection.metadata):
        return None

    return mcp_oauth_provider_for_connection(
        connection=connection,
        server_url=server_url,
        store=app_store(),
    )


def _is_oauth_connection(metadata: dict[str, Any]) -> bool:
    return "oauth_token" in metadata or "client_info" in metadata
