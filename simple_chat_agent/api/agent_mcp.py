"""MCP server settings routes for the coding agent (PRD A8). Mounted inside the agent router,
so every path is under ``/api/agent/mcp``. Local development only in release 1 (D7): in the
catalog ``GET /config`` reports MCP off and these routes refuse to register anything.

Owner-scoped throughout: a user only ever sees, changes or authorizes their own servers, and
tokens live in the app store under the user's own id (``auth_ref`` is the connection id; it
is never accepted from the client).

Storage follows the session list (P2 decision 1): the registration list is written to Redis
first (browsers read it from there, never by a Query), then sent to the user's
``CodingSessions`` with signal-with-start, so a change lands while workers are down. A lost
Redis copy is rebuilt from the ``CodingSessions`` memo (Describe, served by Temporal).

Routes:

* ``GET    /mcp/servers``                list, with whether each one has credentials
* ``POST   /mcp/servers``                add or replace (auth ``none`` or ``bearer``)
* ``DELETE /mcp/servers/{name}``         remove, and delete its stored token
* ``GET    /mcp/servers/{name}/status``  live check: connected, needs_reauth, unreachable, blocked
* ``POST   /mcp/oauth/start``            start OAuth (add or reconnect); returns the URL to open
* ``GET    /mcp/oauth/callback``         the provider's redirect back; finishes and redirects
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Any, Literal
from urllib.parse import quote

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse, RedirectResponse
from pydantic import BaseModel, Field

from simple_chat_agent.agent import mcp_client, net_guard
from simple_chat_agent.agent.mcp_client import McpServerRecord, valid_server_name

log = logging.getLogger(__name__)

MCP_SIGNAL_UPSERT = "upsert_mcp_server_signal"
MCP_SIGNAL_REMOVE = "remove_mcp_server_signal"
MAX_TOKEN_CHARS = 8192
MAX_URL_CHARS = 2000
OAUTH_START_TIMEOUT = 30.0
OAUTH_FINISH_TIMEOUT = 60.0
REBUILD_BACKOFF_SECONDS = 60
CALLBACK_PATH = "/api/agent/mcp/oauth/callback"
APP_PATH = "/"
LOCAL_ONLY = "MCP servers are available in local development only."


def mcp_feature(environment: str) -> dict[str, Any]:
    """What ``GET /config`` reports under ``mcp``. Off in the catalog (D7)."""
    off = os.environ.get("AGENT_MCP_ENABLED", "1").strip().lower() in ("0", "false", "no", "off")
    if environment != "local":
        return {"enabled": False, "reason": LOCAL_ONLY}
    if off:
        return {"enabled": False, "reason": "MCP servers are switched off on this server."}
    return {"enabled": True, "reason": None, "max_servers": mcp_client.MAX_SERVERS, "auth_modes": ["none", "bearer", "oauth"]}


def mcp_enabled(environment: str) -> bool:
    return bool(mcp_feature(environment)["enabled"])


class AddServer(BaseModel):
    name: str = Field(min_length=1, max_length=24)
    url: str = Field(min_length=1, max_length=MAX_URL_CHARS)
    auth_mode: Literal["none", "bearer"] = "none"
    bearer_token: str | None = Field(default=None, max_length=MAX_TOKEN_CHARS)


class OAuthStart(BaseModel):
    name: str = Field(min_length=1, max_length=24)
    url: str = Field(min_length=1, max_length=MAX_URL_CHARS)


def create_mcp_router(
    *,
    user_id: Callable[..., str],
    redis: Any,
    directory: Any,
    temporal: Callable[[], Awaitable[Any]] | None,
    app_store: Callable[[], Any] | None,
    environment: Callable[[], str],
    error: Callable[..., Exception],
    key_prefix: Callable[[], str],
    digest: Callable[[str], str],
    route_class: type | None = None,
) -> APIRouter:
    """``error(status, code, message)`` builds the agent API's harness error;
    ``directory._send`` is the session directory's signal-with-start; ``route_class`` is the
    agent router's (harness error bodies, CSRF gate)."""
    router = APIRouter(route_class=route_class) if route_class is not None else APIRouter()
    flows: dict[str, Any] = {}

    def key(uid: str) -> str:
        return f"{key_prefix()}:u:{digest(uid)}:mcp"

    def require_enabled() -> str:
        env = environment()
        feature = mcp_feature(env)
        if not feature["enabled"]:
            raise error(403, "mcp_disabled", feature["reason"])
        return env

    def store() -> Any:
        if app_store is None:
            raise error(503, "mcp_store_unavailable", "Credential storage isn't configured on this server.")
        return app_store()

    def provider(name: str) -> str:
        return f"mcp:{name}"

    async def records(uid: str) -> list[McpServerRecord]:
        raw = await redis.hgetall(key(uid))
        if not raw and await rebuild(uid):
            raw = await redis.hgetall(key(uid))
        found = [McpServerRecord.model_validate_json(v) for v in raw.values()]
        return sorted(found, key=lambda r: r.added_at, reverse=True)

    async def rebuild(uid: str) -> bool:
        """Redis lost the list: refill it from the CodingSessions memo (Describe, no Query)."""
        if temporal is None or not await redis.set(f"{key(uid)}:rebuilt", "1", nx=True, ex=REBUILD_BACKOFF_SECONDS):
            return False
        try:
            client = await temporal()
            handle = client.get_workflow_handle(mcp_client.sessions_workflow_id_for(uid))
            memo = await (await handle.describe()).memo()
        except Exception as exc:  # noqa: BLE001 - no sessions workflow yet, or Temporal down
            log.info("MCP list rebuild for %s skipped: %s", digest(uid), exc)
            return False
        items = memo.get("mcp_servers") if isinstance(memo, dict) else None
        if not items:
            return False
        rows = {}
        for item in items:
            try:
                record = McpServerRecord.model_validate(item)
            except Exception:  # noqa: BLE001
                continue
            rows[record.name] = record.model_dump_json()
        if rows:
            await redis.hset(key(uid), mapping=rows)
        return bool(rows)

    def has_credentials(uid: str, record: McpServerRecord) -> bool:
        if record.auth_mode == "none":
            return True
        if not record.auth_ref or app_store is None:
            return False
        connection = app_store().get_oauth_connection_by_id(record.auth_ref)
        return bool(connection and connection.user_id == uid and connection.access_token)

    def view(uid: str, record: McpServerRecord, **extra: Any) -> dict[str, Any]:
        return {
            "name": record.name,
            "url": record.url,
            "auth_mode": record.auth_mode,
            "has_credentials": has_credentials(uid, record),
            "added_at": record.added_at,
            "tool_prefix": f"{mcp_client.TOOL_PREFIX}{record.name}__",
            **extra,
        }

    async def save(uid: str, record: McpServerRecord) -> None:
        await redis.hset(key(uid), record.name, record.model_dump_json())
        await directory._send(uid, MCP_SIGNAL_UPSERT, record)

    async def vet(name: str, url: str, env: str) -> str:
        if not valid_server_name(name):
            raise error(422, "invalid_name", "Name: 1-24 of a-z, 0-9 and '-', starting with a letter.")
        url = url.strip()
        try:
            await mcp_client.check_server_url(url, env)
        except net_guard.BlockedDestination as exc:
            raise error(422, "blocked_destination", f"That URL isn't allowed: {exc}") from None
        except net_guard.NetGuardError as exc:
            raise error(422, "unreachable", f"That URL can't be reached: {exc}") from None
        return url

    async def discover(uid: str, record: McpServerRecord, env: str) -> list[dict[str, str]]:
        try:
            tools = await mcp_client.list_tools(
                record, owner_id=uid, store=app_store() if record.auth_ref and app_store else None, environment=env
            )
        except mcp_client.McpAuthForbidden as exc:
            raise error(403, "auth_ref_forbidden", str(exc)) from None
        except Exception as exc:  # noqa: BLE001 - classified for the dialog
            if mcp_client.find(exc, net_guard.BlockedDestination) is not None:
                raise error(422, "blocked_destination", mcp_client.short_error(exc)) from None
            if mcp_client.is_auth_failure(exc):
                raise error(
                    401,
                    "mcp_auth_required",
                    "The server needs authorization. Use OAuth if it supports MCP OAuth, or a bearer token.",
                ) from None
            raise error(502, "mcp_unreachable", f"MCP discovery failed: {mcp_client.short_error(exc)}") from None
        return [{"name": t.name, "mcp_name": t.mcp_name} for t in tools]

    # -- routes ------------------------------------------------------------------------

    @router.get("/mcp/servers")
    async def list_servers(uid: str = Depends(user_id)):
        feature = mcp_feature(environment())
        servers = [view(uid, r) for r in await records(uid)] if feature["enabled"] else []
        return JSONResponse(
            {"enabled": feature["enabled"], "reason": feature["reason"], "servers": servers},
            headers={"Cache-Control": "no-store"},
        )

    @router.post("/mcp/servers")
    async def add_server(body: AddServer, uid: str = Depends(user_id)):
        env = require_enabled()
        url = await vet(body.name, body.url, env)
        existing = {r.name for r in await records(uid)}
        if body.name not in existing and len(existing) >= mcp_client.MAX_SERVERS:
            raise error(422, "too_many_servers", f"At most {mcp_client.MAX_SERVERS} MCP servers.")
        auth_ref = None
        if body.auth_mode == "bearer":
            token = (body.bearer_token or "").strip()
            if not token:
                raise error(422, "token_required", "A bearer token is required.")
            auth_ref = store().upsert_oauth_connection(
                user_id=uid,
                provider=provider(body.name),
                access_token=token,
                token_type="Bearer",
                scope="",
                provider_user_id=None,
                provider_user_login=None,
                metadata={"auth_mode": "bearer"},
            )
        record = McpServerRecord(
            name=body.name,
            url=url,
            auth_mode=body.auth_mode,
            auth_ref=auth_ref,
            added_at=datetime.now(timezone.utc).isoformat(),
        )
        try:
            tools = await discover(uid, record, env)
        except Exception:
            if auth_ref is not None and body.name not in existing:
                store().delete_oauth_connection(user_id=uid, provider=provider(body.name))
            raise
        await save(uid, record)
        return JSONResponse({"server": view(uid, record, status="connected", tools=tools)})

    @router.delete("/mcp/servers/{name}")
    async def remove_server(name: str, uid: str = Depends(user_id)):
        if not valid_server_name(name):
            raise error(404, "not_found", "No such MCP server.")
        removed = await redis.hdel(key(uid), name)
        await directory._send(uid, MCP_SIGNAL_REMOVE, name)
        if app_store is not None:
            app_store().delete_oauth_connection(user_id=uid, provider=provider(name))
        return JSONResponse({"removed": bool(removed), "name": name})

    @router.get("/mcp/servers/{name}/status")
    async def server_status(name: str, uid: str = Depends(user_id)):
        env = require_enabled()
        record = next((r for r in await records(uid) if r.name == name), None)
        if record is None:
            raise error(404, "not_found", "No such MCP server.")
        try:
            tools = await discover(uid, record, env)
        except Exception as exc:
            code = getattr(exc, "code", "")
            status = {
                "mcp_auth_required": "needs_reauth",
                "blocked_destination": "blocked",
                "auth_ref_forbidden": "needs_reauth",
            }.get(code, "unreachable")
            return JSONResponse(
                view(
                    uid,
                    record,
                    status=status,
                    code=mcp_client.RECONNECT_CODE if status == "needs_reauth" else code,
                    message=getattr(exc, "message", str(exc)),
                    tools=[],
                ),
                headers={"Cache-Control": "no-store"},
            )
        return JSONResponse(
            view(uid, record, status="connected", code=None, message=None, tools=tools),
            headers={"Cache-Control": "no-store"},
        )

    @router.post("/mcp/oauth/start")
    async def oauth_start(body: OAuthStart, request: Request, uid: str = Depends(user_id)):
        from simple_chat_agent.common.mcp_oauth import PendingMcpOAuthFlow

        env = require_enabled()
        url = await vet(body.name, body.url, env)
        existing = {r.name for r in await records(uid)}
        if body.name not in existing and len(existing) >= mcp_client.MAX_SERVERS:
            raise error(422, "too_many_servers", f"At most {mcp_client.MAX_SERVERS} MCP servers.")
        app = store()
        flow = PendingMcpOAuthFlow(user_id=uid, server_id=body.name, server_url=url, tool_prefix=body.name, label=body.name)
        flows[flow.flow_id] = flow
        flow.task = asyncio.create_task(finish(flow, app, env, callback_url(request, flow.flow_id)))
        try:
            await asyncio.wait_for(flow.auth_url_ready.wait(), timeout=OAUTH_START_TIMEOUT)
        except asyncio.TimeoutError:
            flows.pop(flow.flow_id, None)
            flow.task.cancel()
            raise error(504, "oauth_timeout", "Timed out starting MCP OAuth.") from None
        if flow.start_error or not flow.auth_url:
            flows.pop(flow.flow_id, None)
            raise error(400, "oauth_failed", flow.start_error or "MCP OAuth did not start.")
        return JSONResponse({"authorization_url": flow.auth_url, "flow_id": flow.flow_id})

    def callback_url(request: Request, flow_id: str) -> str:
        base = os.environ.get("SIMPLE_CHAT_PUBLIC_URL", "").strip().rstrip("/") or str(request.base_url).rstrip("/")
        return f"{base}{CALLBACK_PATH}?flow_id={quote(flow_id, safe='')}"

    async def finish(flow: Any, app: Any, env: str, redirect_uri: str) -> None:
        from simple_chat_agent.common.mcp_oauth import authorize_mcp_oauth_flow

        import httpx

        try:
            client = httpx.AsyncClient(
                transport=mcp_client.GuardedTransport(mcp_client.mcp_net_policy(env)),
                follow_redirects=False,
                trust_env=False,
                timeout=httpx.Timeout(30.0),
            )
            await authorize_mcp_oauth_flow(flow=flow, store=app, redirect_uri=redirect_uri, http_client=client)
            connection = app.get_oauth_connection(user_id=flow.user_id, provider=provider(flow.server_id))
            if connection is None or not connection.access_token:
                raise RuntimeError("MCP OAuth finished without a token.")
            record = McpServerRecord(
                name=flow.server_id,
                url=flow.server_url,
                auth_mode="oauth",
                auth_ref=connection.connection_id,
                added_at=datetime.now(timezone.utc).isoformat(),
            )
            await discover(flow.user_id, record, env)
            await save(flow.user_id, record)
        except BaseException as exc:
            flow.fail(exc if isinstance(exc, Exception) else RuntimeError("cancelled"))
            raise
        finally:
            flows.pop(flow.flow_id, None)

    @router.get("/mcp/oauth/callback")
    async def oauth_callback(request: Request, flow_id: str, code: str | None = None, state: str | None = None, uid: str = Depends(user_id)):
        flow = flows.get(flow_id)
        # Only the user who started the flow may finish it.
        if flow is None or flow.user_id != uid:
            return RedirectResponse(f"{APP_PATH}?mcp_error={quote('Unknown or expired MCP authorization.', safe='')}")
        provider_error = request.query_params.get("error")
        if provider_error:
            message = request.query_params.get("error_description") or provider_error
            flow.fail(RuntimeError(message))
            return RedirectResponse(f"{APP_PATH}?mcp_error={quote(message[:200], safe='')}")
        if not code:
            flow.fail(RuntimeError("Missing authorization code."))
            return RedirectResponse(f"{APP_PATH}?mcp_error={quote('Missing authorization code.', safe='')}")
        flow.complete(code, state)
        try:
            await asyncio.wait_for(asyncio.shield(flow.task), timeout=OAUTH_FINISH_TIMEOUT)
        except Exception as exc:  # noqa: BLE001 - reported to the browser
            message = getattr(exc, "message", None) or mcp_client.short_error(exc)
            return RedirectResponse(f"{APP_PATH}?mcp_error={quote(str(message)[:200], safe='')}")
        return RedirectResponse(f"{APP_PATH}?mcp=connected&mcp_server={quote(flow.server_id, safe='')}")

    return router


__all__ = ["create_mcp_router", "mcp_enabled", "mcp_feature"]
