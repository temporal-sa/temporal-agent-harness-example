"""MCP client for the coding agent (PRD A8, §5.4, §5.6): registrations, the SSRF guard,
owner-only token resolution, and the two calls we make (list tools, call a tool).

Key-free and workflow-free, so the API (register-time checks, status) and the worker
activities (agent/mcp.py) share one implementation.

* **SSRF guard.** Every HTTP request an MCP session makes, including OAuth token refreshes to
  the authorization server, goes through :class:`GuardedTransport`: the destination is vetted
  by :mod:`net_guard` and the socket is pinned to the vetted IP. Redirects are not followed.
  Loopback is refused like any private address, except in local development when the
  operator names the loopback ports to allow (``AGENT_MCP_LOOPBACK_PORTS``, see
  :func:`mcp_net_policy`).
* **Tokens.** ``auth_ref`` is the id of an OAuth connection in the app store. It resolves only
  when the connection's ``user_id`` is the session owner (:func:`resolve_auth`); anything else
  raises :class:`McpAuthForbidden`.
* **Reauth.** A 401/403, or an OAuth token that can no longer be refreshed, raises
  :class:`McpReauthRequired`; the agent turns it into a non-retryable error whose message
  starts with :data:`RECONNECT_CODE` (the UI's "Reconnect" chip).
"""

from __future__ import annotations

import hashlib
import ipaddress
import json
import os
import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, Literal

import httpx
from pydantic import BaseModel, Field

from simple_chat_agent.agent import net_guard

# -- limits and names -------------------------------------------------------------------

MAX_SERVERS = 10
MAX_TOOLS_PER_SERVER = 40
MAX_DESCRIPTION_CHARS = 1000
MAX_SCHEMA_CHARS = 8000
MAX_RESULT_CHARS = 20_000
MAX_TOOL_NAME_CHARS = 64  # provider limit on tool names
CONNECT_TIMEOUT_SECONDS = 20.0
CALL_TIMEOUT_SECONDS = 120.0

# Server names: lowercase, no underscores, so ``mcp__<server>__<tool>`` parses unambiguously.
SERVER_NAME = re.compile(r"^[a-z][a-z0-9-]{0,23}$")
TOOL_PREFIX = "mcp__"

AuthMode = Literal["none", "bearer", "oauth"]

# Error types (ApplicationError.type) and the code the UI keys its "Reconnect" chip on.
REAUTH_ERROR_TYPE = "McpReauthRequired"
FORBIDDEN_ERROR_TYPE = "McpAuthRefForbidden"
BLOCKED_ERROR_TYPE = "BlockedDestination"
REMOVED_ERROR_TYPE = "McpServerRemoved"
RECONNECT_CODE = "mcp_reauth_required"

# Local development only: loopback ports MCP may reach (e.g. "8765"). Ignored unless the
# process runs with AGENT_ENVIRONMENT=local AND the session is a local one.
LOOPBACK_PORTS_ENV = "AGENT_MCP_LOOPBACK_PORTS"
_LOOPBACK = (ipaddress.ip_network("127.0.0.0/8"), ipaddress.ip_network("::1/128"))


class McpError(Exception):
    """Base class for MCP failures we explain."""


class McpReauthRequired(McpError):
    """The server refused our credentials, or the OAuth token can't be refreshed."""

    def __init__(self, server: str, detail: str = "") -> None:
        self.server = server
        super().__init__(reconnect_message(server, detail))


class McpAuthForbidden(McpError):
    """``auth_ref`` names a connection that is not the session owner's."""


def reconnect_message(server: str, detail: str = "") -> str:
    """The text a reauth error carries: the code first, so a client can match it."""
    tail = f" ({detail})" if detail else ""
    return (
        f"{RECONNECT_CODE}: MCP server '{server}' needs you to reconnect. Open MCP settings "
        f"and choose Reconnect, then try again.{tail}"
    )


# -- models -----------------------------------------------------------------------------


class McpServerRecord(BaseModel):
    """One registration, as stored in the user's CodingSessions settings. No secrets: the
    token lives in the app store under ``auth_ref``."""

    name: str
    url: str
    auth_mode: AuthMode = "none"
    auth_ref: str | None = None
    added_at: str = ""

    def fingerprint(self) -> str:
        raw = json.dumps([self.name, self.url, self.auth_mode, self.auth_ref])
        return hashlib.sha256(raw.encode()).hexdigest()[:16]


class McpToolInfo(BaseModel):
    """One MCP tool as the agent offers it."""

    name: str  # the harness tool name, mcp__<server>__<tool>
    mcp_name: str  # the tool's name on the server
    description: str = ""  # server-supplied: shown to the model, never to the judge
    input_schema: dict[str, Any] = Field(default_factory=lambda: {"type": "object", "properties": {}})


class McpServerTools(BaseModel):
    """A server's tools as last listed, cached in the agent's state."""

    server: McpServerRecord
    fingerprint: str = ""
    listed_at: float = 0.0
    status: Literal["ok", "needs_reauth", "error"] = "ok"
    error: str | None = None
    tools: list[McpToolInfo] = Field(default_factory=list)


class McpCallOutput(BaseModel):
    """The result of one MCP tool call. ``str()`` is what the model and the stream see."""

    server: str
    tool: str
    text: str = ""
    is_error: bool = False
    truncated: bool = False

    def __str__(self) -> str:
        prefix = "The MCP tool reported an error: " if self.is_error else ""
        return f"{prefix}{self.text}" if self.text else f"{prefix}(no content)"


# -- names ------------------------------------------------------------------------------


def valid_server_name(name: str) -> bool:
    return bool(SERVER_NAME.fullmatch(name or ""))


def public_tool_name(server: str, tool: str) -> str:
    """``mcp__<server>__<tool>``, limited to what every provider accepts
    (``[A-Za-z0-9_-]{1,64}``). A long or odd name keeps a hash suffix so it stays unique."""
    safe = re.sub(r"[^A-Za-z0-9_-]", "_", tool) or "tool"
    name = f"{TOOL_PREFIX}{server}__{safe}"
    if len(name) <= MAX_TOOL_NAME_CHARS and safe == tool:
        return name
    digest = hashlib.sha256(tool.encode()).hexdigest()[:6]
    keep = MAX_TOOL_NAME_CHARS - len(TOOL_PREFIX) - len(server) - 2 - 7
    return f"{TOOL_PREFIX}{server}__{safe[:keep]}_{digest}"


def owner_digest(user_id: str) -> str:
    """Same digest the API uses for the owner search attribute and per-user keys."""
    return hashlib.sha256(user_id.encode("utf-8")).hexdigest()[:32]


def sessions_workflow_id_for(user_id: str) -> str:
    from simple_chat_agent.agent.sessions import sessions_workflow_id

    return sessions_workflow_id(owner_digest(user_id))


def clean_schema(schema: Any) -> dict[str, Any]:
    """An MCP input schema as a bounded JSON-schema object the providers accept."""
    if not isinstance(schema, dict):
        schema = {}
    schema = {k: v for k, v in schema.items() if k not in ("$schema", "$id")}
    schema.setdefault("type", "object")
    if schema.get("type") != "object":
        schema = {"type": "object", "properties": {}}
    schema.setdefault("properties", {})
    if len(json.dumps(schema, default=str)) > MAX_SCHEMA_CHARS:
        # Too large to send every turn: offer a free-form object instead.
        schema = {"type": "object", "properties": {}, "additionalProperties": True}
    return schema


# -- SSRF guard -------------------------------------------------------------------------


def process_environment() -> str:
    return "local" if os.environ.get("AGENT_ENVIRONMENT", "catalog").strip().lower() == "local" else "catalog"


def mcp_net_policy(environment: str) -> net_guard.NetPolicy:
    """The guard's policy for MCP traffic. Production: :func:`net_guard.default_policy`.
    Local development may also reach loopback on the ports in ``AGENT_MCP_LOOPBACK_PORTS``
    (and on no other port), and only when both the session and this process are local.
    Public hosts stay on the base port list."""
    base = net_guard.default_policy()
    raw = os.environ.get(LOOPBACK_PORTS_ENV, "").strip()
    if not raw or environment != "local" or process_environment() != "local":
        return base
    ports = frozenset(int(p) for p in raw.replace(" ", "").split(",") if p.isdigit())
    return net_guard.NetPolicy(
        allowed_schemes=base.allowed_schemes,
        allowed_ports=base.allowed_ports,
        max_redirects=0,
        allow_networks=_LOOPBACK,
        exception_ports=ports,
    )


async def check_server_url(url: str, environment: str) -> net_guard.VettedDestination:
    """Vet a server URL (register time and call time). Raises ``net_guard.BlockedDestination``
    or ``net_guard.DestinationUnresolvable``."""
    return await net_guard.check_url_async(url, policy=mcp_net_policy(environment))


class GuardedTransport(httpx.AsyncBaseTransport):
    """Vets every request's destination and pins its connection to the vetted IP.

    One pinned transport per (scheme, host, port), so the MCP endpoint and an OAuth
    authorization server are each checked before the first byte goes out."""

    def __init__(self, policy: net_guard.NetPolicy) -> None:
        self._policy = policy
        self._pins: dict[tuple[str, str, int | None], httpx.AsyncBaseTransport] = {}

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        key = (request.url.scheme, request.url.host, request.url.port)
        transport = self._pins.get(key)
        if transport is None:
            dest = await net_guard.check_url_async(str(request.url), policy=self._policy)
            transport = net_guard.PinnedTransport(dest.host, dest.port, dest.pinned_ip)
            self._pins[key] = transport
        return await transport.handle_async_request(request)

    async def aclose(self) -> None:
        for transport in self._pins.values():
            await transport.aclose()
        self._pins.clear()


# -- auth -------------------------------------------------------------------------------


def resolve_auth(
    store: Any, *, owner_id: str, record: McpServerRecord
) -> tuple[dict[str, str], httpx.Auth | None]:
    """Headers and an httpx auth for ``record``, for ``owner_id`` only.

    Raises :class:`McpAuthForbidden` when ``auth_ref`` names another user's connection, and
    :class:`McpReauthRequired` when the connection is gone or holds no token."""
    if record.auth_mode == "none" or not record.auth_ref:
        return {}, None
    if not owner_id:
        raise McpAuthForbidden("No session owner to resolve MCP credentials for.")
    connection = store.get_oauth_connection_by_id(record.auth_ref)
    if connection is not None and connection.user_id != owner_id:
        raise McpAuthForbidden(f"MCP server '{record.name}': its credentials belong to another user.")
    if connection is None or not connection.access_token:
        raise McpReauthRequired(record.name, "no stored credentials")
    metadata = connection.metadata or {}
    if "oauth_token" in metadata or "client_info" in metadata:
        from simple_chat_agent.common.mcp_oauth import mcp_oauth_provider_for_connection

        return {}, mcp_oauth_provider_for_connection(connection=connection, server_url=record.url, store=store)
    token_type = connection.token_type or "Bearer"
    return {"Authorization": f"{token_type} {connection.access_token}"}, None


def walk_exceptions(err: BaseException) -> list[BaseException]:
    if isinstance(err, BaseExceptionGroup):
        found: list[BaseException] = []
        for nested in err.exceptions:
            found.extend(walk_exceptions(nested))
        return found
    chain = [err]
    cause = err.__cause__ or err.__context__
    while cause is not None and len(chain) < 10:
        chain.append(cause)
        cause = cause.__cause__ or cause.__context__
    return chain


def is_auth_failure(err: BaseException) -> bool:
    for nested in walk_exceptions(err):
        if isinstance(nested, McpReauthRequired):
            return True
        response = getattr(nested, "response", None)
        if getattr(response, "status_code", None) in (401, 403):
            return True
        text = str(nested)
        if "401 Unauthorized" in text or "403 Forbidden" in text or "requires reauthorization" in text:
            return True
    return False


def find(err: BaseException, kind: type[BaseException]) -> BaseException | None:
    return next((e for e in walk_exceptions(err) if isinstance(e, kind)), None)


def short_error(err: BaseException) -> str:
    for nested in walk_exceptions(err):
        text = str(nested).strip()
        if text:
            return text[:300]
    return type(err).__name__


# -- sessions ---------------------------------------------------------------------------


@asynccontextmanager
async def open_session(
    record: McpServerRecord, *, owner_id: str, store: Any, environment: str, timeout: float = CALL_TIMEOUT_SECONDS
) -> AsyncIterator[Any]:
    """An initialized ``mcp.ClientSession`` to ``record``, with every request guarded."""
    from mcp import ClientSession
    from mcp.client.streamable_http import streamable_http_client

    if not valid_server_name(record.name):
        raise McpError(f"invalid MCP server name {record.name!r}")
    # Fail fast with a clear refusal before any MCP machinery starts.
    await check_server_url(record.url, environment)
    headers, auth = resolve_auth(store, owner_id=owner_id, record=record)
    client = httpx.AsyncClient(
        transport=GuardedTransport(mcp_net_policy(environment)),
        headers=headers,
        auth=auth,
        timeout=httpx.Timeout(timeout, connect=CONNECT_TIMEOUT_SECONDS),
        follow_redirects=False,
        trust_env=False,
    )
    async with client:
        async with streamable_http_client(record.url, http_client=client) as (read, write, _sid):
            async with ClientSession(read, write) as session:
                await session.initialize()
                yield session


async def list_tools(record: McpServerRecord, *, owner_id: str, store: Any, environment: str) -> list[McpToolInfo]:
    """The server's tools, bounded, as harness tool descriptions."""
    async with open_session(
        record, owner_id=owner_id, store=store, environment=environment, timeout=CONNECT_TIMEOUT_SECONDS
    ) as session:
        listed = await session.list_tools()
    tools: list[McpToolInfo] = []
    seen: set[str] = set()
    for tool in listed.tools[:MAX_TOOLS_PER_SERVER]:
        name = public_tool_name(record.name, tool.name)
        if name in seen:
            continue
        seen.add(name)
        tools.append(
            McpToolInfo(
                name=name,
                mcp_name=tool.name,
                description=(tool.description or "").strip()[:MAX_DESCRIPTION_CHARS],
                input_schema=clean_schema(tool.inputSchema),
            )
        )
    return tools


async def call_tool(
    record: McpServerRecord,
    tool: str,
    arguments: dict[str, Any],
    *,
    owner_id: str,
    store: Any,
    environment: str,
) -> McpCallOutput:
    async with open_session(record, owner_id=owner_id, store=store, environment=environment) as session:
        result = await session.call_tool(tool, arguments or {})
    return call_output(record.name, tool, result)


def call_output(server: str, tool: str, result: Any) -> McpCallOutput:
    """A bounded, text-only view of an MCP ``CallToolResult``."""
    parts: list[str] = []
    for block in result.content:
        kind = getattr(block, "type", "")
        if kind == "text":
            parts.append(block.text)
        elif kind == "resource" and hasattr(getattr(block, "resource", None), "text"):
            parts.append(block.resource.text)
        else:
            parts.append(f"[{kind or 'content'} block omitted]")
    if not parts and result.structuredContent is not None:
        parts.append(json.dumps(result.structuredContent, default=str))
    text = "\n".join(parts)
    return McpCallOutput(
        server=server,
        tool=tool,
        text=text[:MAX_RESULT_CHARS],
        is_error=bool(result.isError),
        truncated=len(text) > MAX_RESULT_CHARS,
    )
