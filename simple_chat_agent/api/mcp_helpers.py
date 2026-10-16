from __future__ import annotations

import re
from typing import TYPE_CHECKING
from urllib.parse import urlparse
from uuid import uuid4

from simple_chat_agent.common.mcp_auth import mcp_oauth_provider

if TYPE_CHECKING:  # annotations only: the agent API imports this without the old harness
    from agent_harness.mcp_types import HttpMcpServerConfig
    from simple_chat_agent.api.auth import AuthenticatedUser
    from simple_chat_agent.common.store import AppStore


def mcp_server_url_candidates(server_url: str) -> list[str]:
    normalized = server_url.strip().rstrip("/")
    if not normalized:
        return []

    candidates = [normalized]
    parsed = urlparse(normalized)
    if parsed.scheme in ("http", "https") and parsed.path in ("", "/"):
        candidates.append(f"{normalized}/mcp")
    return candidates


def mcp_server_id(server_id: str | None) -> str:
    if server_id is None:
        return f"mcp-{uuid4().hex[:12]}"

    sanitized = re.sub(r"[^A-Za-z0-9_-]+", "-", server_id.strip())
    sanitized = sanitized.strip("-_")
    return sanitized or f"mcp-{uuid4().hex[:12]}"


def mcp_server_connected(
    user: AuthenticatedUser,
    server: HttpMcpServerConfig,
    store: AppStore,
) -> bool:
    if server.auth_mode == "none":
        return True
    if server.auth_ref is None:
        return False

    connection = store.get_oauth_connection_by_id(server.auth_ref)
    if connection is None:
        connection = store.get_oauth_connection(
            user_id=user.user_id,
            provider=mcp_oauth_provider(server.server_id),
        )
    return bool(connection and connection.access_token)


def mcp_discovery_error_message(err: BaseException) -> str:
    if mcp_error_requires_auth(err):
        return (
            "MCP server requires authentication. Select OAuth authorization if the "
            "server supports MCP OAuth, or use bearer auth if you already have "
            "an access token."
        )

    message = first_exception_message(err)
    if message:
        return f"MCP discovery failed: {message}"
    return "MCP discovery failed."


def mcp_error_requires_auth(err: BaseException) -> bool:
    for nested in walk_exception_tree(err):
        response = getattr(nested, "response", None)
        status_code = getattr(response, "status_code", None)
        if status_code in (401, 403):
            return True
        message = str(nested)
        if "401 Unauthorized" in message or "403 Forbidden" in message:
            return True
    return False


def first_exception_message(err: BaseException) -> str:
    for nested in walk_exception_tree(err):
        message = str(nested).strip()
        if message:
            return message
    return ""


def walk_exception_tree(err: BaseException) -> list[BaseException]:
    if isinstance(err, BaseExceptionGroup):
        nested_errors: list[BaseException] = []
        for nested in err.exceptions:
            nested_errors.extend(walk_exception_tree(nested))
        return nested_errors
    return [err]
