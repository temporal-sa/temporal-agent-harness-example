from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass, replace
from typing import Any, Callable
from urllib.parse import quote

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import RedirectResponse

from agent_harness.mcp import discover_http_mcp_tools, public_mcp_tool_name
from agent_harness.mcp_types import HttpMcpServerConfig
from simple_chat_agent.api.auth import AuthenticatedUser
from simple_chat_agent.api.github_oauth import (
    GITHUB_PROVIDER,
    GitHubOAuthError,
    exchange_github_code,
    fetch_github_user,
    github_authorize_url,
    github_oauth_configured,
    github_scopes,
)
from simple_chat_agent.api.mcp_helpers import (
    mcp_discovery_error_message,
    mcp_error_requires_auth,
    mcp_server_connected,
    mcp_server_id,
    mcp_server_url_candidates,
)
from simple_chat_agent.api.schemas import (
    McpServerEnabledRequest,
    McpServerRequest,
)
from simple_chat_agent.common.mcp_auth import mcp_oauth_provider
from simple_chat_agent.common.mcp_oauth import (
    PendingMcpOAuthFlow,
    authorize_mcp_oauth_flow,
)
from simple_chat_agent.common.store import AppStore
from simple_chat_agent.worker.tools import (
    CREATE_ARTIFACT_TOOL,
    CREATE_SUBAGENT_TOOL,
    FETCH_URL_TOOL,
    GITHUB_TOOL_NAMES,
    WORKSPACE_SHELL_TOOL,
    configured_research_tool_names,
)
from simple_chat_agent.worker.user_chats_workflow import (
    DeleteMcpServerRequest,
    UserChatsWorkflow,
)


@dataclass(frozen=True)
class ToolRouteDeps:
    store: Callable[[], AppStore]
    current_user: Callable[[Request], AuthenticatedUser]
    ensure_user_chats_workflow: Callable[..., Any]
    upsert_user_mcp_server: Callable[..., Any]
    github_tools_enabled: Callable[[], bool]
    mcp_oauth_flows: Callable[[], dict[str, PendingMcpOAuthFlow]]


def create_tools_router(deps: ToolRouteDeps) -> APIRouter:
    router = APIRouter()

    @router.get("/api/tools")
    async def tools(request: Request) -> dict[str, Any]:
        user = deps.current_user(request)
        github_enabled = deps.github_tools_enabled()
        github_connection = (
            deps.store().get_oauth_connection(
                user_id=user.user_id,
                provider=GITHUB_PROVIDER,
            )
            if github_enabled
            else None
        )
        mcp_servers = await (
            await deps.ensure_user_chats_workflow(user.user_id, user.username)
        ).query(UserChatsWorkflow.list_mcp_servers)
        tools = [
            {
                "provider": "builtin:core",
                "label": "Harness tools",
                "configured": True,
                "connected": True,
                "enabled": True,
                "login": "local workflow activities",
                "scopes": "local",
                "available_tools": [
                    FETCH_URL_TOOL,
                    WORKSPACE_SHELL_TOOL,
                    CREATE_ARTIFACT_TOOL,
                    CREATE_SUBAGENT_TOOL,
                ],
            },
        ]
        research_tool_names = configured_research_tool_names()
        if research_tool_names:
            tools.append(
                {
                    "provider": "builtin:research",
                    "label": "Research tools",
                    "configured": True,
                    "connected": True,
                    "enabled": True,
                    "login": "configured research providers",
                    "scopes": "web/search",
                    "available_tools": research_tool_names,
                },
            )
        if github_enabled:
            tools.append(
                {
                    "provider": GITHUB_PROVIDER,
                    "label": "GitHub",
                    "configured": github_oauth_configured(),
                    "connected": github_connection is not None,
                    "enabled": github_connection is not None,
                    "login": (
                        github_connection.provider_user_login
                        if github_connection is not None
                        else None
                    ),
                    "scopes": (
                        github_connection.scope
                        if github_connection is not None
                        else github_scopes()
                    ),
                    "available_tools": GITHUB_TOOL_NAMES,
                },
            )
        tools.extend(
            {
                "provider": f"mcp:{server.server_id}",
                "server_id": server.server_id,
                "server_url": server.server_url,
                "tool_prefix": server.tool_prefix,
                "auth_mode": server.auth_mode,
                "label": server.label,
                "configured": True,
                "connected": mcp_server_connected(
                    user,
                    server,
                    deps.store(),
                ),
                "enabled": server.enabled,
                "login": server.server_url,
                "scopes": server.auth_mode,
                "available_tools": [
                    tool.public_name
                    or public_mcp_tool_name(server.tool_prefix, tool.name)
                    for tool in server.tools
                ],
            }
            for server in mcp_servers
        )
        return {"tools": tools}

    @router.post("/api/tools/github/disconnect")
    async def disconnect_github(request: Request) -> dict[str, str]:
        if not deps.github_tools_enabled():
            raise HTTPException(status_code=404, detail="GitHub tools are disabled.")
        user = deps.current_user(request)
        deps.store().delete_oauth_connection(
            user_id=user.user_id,
            provider=GITHUB_PROVIDER,
        )
        return {"status": "ok"}

    @router.get("/api/mcp-servers")
    async def list_mcp_servers(request: Request) -> dict[str, Any]:
        user = deps.current_user(request)
        servers = await (
            await deps.ensure_user_chats_workflow(user.user_id, user.username)
        ).query(UserChatsWorkflow.list_mcp_servers)
        return {"servers": [asdict(server) for server in servers]}

    @router.post("/api/mcp-servers")
    async def add_mcp_server(
        http_request: Request,
        request: McpServerRequest,
    ) -> dict[str, Any]:
        user = deps.current_user(http_request)
        server_id = mcp_server_id(None)
        auth_ref = None

        if request.auth_mode == "oauth":
            raise HTTPException(
                status_code=400,
                detail="Use the MCP OAuth flow to add OAuth-discovered MCP servers.",
            )

        if request.auth_mode == "bearer":
            if not request.bearer_token:
                raise HTTPException(status_code=400, detail="Bearer token is required.")
            auth_ref = mcp_oauth_provider(server_id)
            deps.store().upsert_oauth_connection(
                user_id=user.user_id,
                provider=auth_ref,
                access_token=request.bearer_token,
                token_type="Bearer",
                scope="",
                provider_user_id=None,
                provider_user_login=request.label,
                metadata={"auth_mode": "bearer"},
            )

        try:
            discovered_url, tools = await _discover_mcp_tools_for_user_request(
                request.server_url,
                tool_prefix=request.tool_prefix,
                auth_ref=auth_ref,
            )
        except Exception as err:
            if auth_ref is not None:
                deps.store().delete_oauth_connection(
                    user_id=user.user_id,
                    provider=mcp_oauth_provider(server_id),
                )
            raise HTTPException(
                status_code=400,
                detail=mcp_discovery_error_message(err),
            ) from err

        if not tools:
            if auth_ref is not None:
                deps.store().delete_oauth_connection(
                    user_id=user.user_id,
                    provider=mcp_oauth_provider(server_id),
                )
            raise HTTPException(
                status_code=400,
                detail="MCP discovery succeeded, but the server returned no tools.",
            )

        server = HttpMcpServerConfig(
            server_id=server_id,
            label=request.label,
            server_url=discovered_url,
            tool_prefix=request.tool_prefix,
            auth_ref=auth_ref,
            auth_mode=request.auth_mode,
            tools=tools,
        )
        await deps.upsert_user_mcp_server(user, server)
        return {"server": asdict(server)}

    @router.post("/api/mcp-servers/{server_id}/enabled")
    async def set_mcp_server_enabled(
        request: Request,
        server_id: str,
        update: McpServerEnabledRequest,
    ) -> dict[str, Any]:
        user = deps.current_user(request)
        registry = await deps.ensure_user_chats_workflow(user.user_id, user.username)
        servers = await registry.query(UserChatsWorkflow.list_mcp_servers)
        existing = next(
            (server for server in servers if server.server_id == server_id),
            None,
        )
        if existing is None:
            raise HTTPException(status_code=404, detail="MCP server not found.")

        updated = replace(existing, enabled=update.enabled)
        await deps.upsert_user_mcp_server(user, updated)
        return {"server": asdict(updated)}

    @router.delete("/api/mcp-servers/{server_id}")
    async def delete_mcp_server(request: Request, server_id: str) -> dict[str, str]:
        user = deps.current_user(request)
        registry = await deps.ensure_user_chats_workflow(user.user_id, user.username)
        await registry.execute_update(
            UserChatsWorkflow.delete_mcp_server,
            DeleteMcpServerRequest(server_id=server_id),
        )
        deps.store().delete_oauth_connection(
            user_id=user.user_id,
            provider=mcp_oauth_provider(server_id),
        )
        return {"status": "ok"}

    @router.get("/api/mcp-servers/oauth/start")
    async def start_mcp_oauth(
        request: Request,
        label: str,
        server_url: str,
        tool_prefix: str,
        server_id: str | None = None,
    ) -> RedirectResponse:
        user = deps.current_user(request)
        auth_url = await _start_mcp_oauth_flow(
            deps,
            user=user,
            label=label,
            server_url=server_url,
            tool_prefix=tool_prefix,
            server_id=server_id,
        )
        return RedirectResponse(auth_url)

    @router.get("/oauth/mcp/callback")
    async def mcp_oauth_callback(
        flow_id: str,
        code: str | None = None,
        state: str | None = None,
        error: str | None = None,
        error_description: str | None = None,
    ) -> RedirectResponse:
        flow = deps.mcp_oauth_flows().get(flow_id)
        if flow is None:
            return RedirectResponse("/?oauth_error=Unknown%20MCP%20OAuth%20flow")
        if error is not None:
            flow.fail(RuntimeError(error_description or error))
            return RedirectResponse(
                f"/?oauth_error={quote(error_description or error, safe='')}"
            )
        if not code:
            flow.fail(RuntimeError("Missing MCP OAuth callback code"))
            return RedirectResponse("/?oauth_error=Missing%20MCP%20OAuth%20code")

        flow.complete(code, state)
        if flow.task is not None:
            try:
                await asyncio.wait_for(asyncio.shield(flow.task), timeout=30)
            except Exception as err:
                return RedirectResponse(f"/?oauth_error={quote(str(err), safe='')}")
        return RedirectResponse("/?mcp=connected")

    @router.get("/oauth/github/start")
    async def github_oauth_start(request: Request) -> RedirectResponse:
        if not deps.github_tools_enabled():
            raise HTTPException(status_code=404, detail="GitHub tools are disabled.")
        user = deps.current_user(request)
        if not github_oauth_configured():
            raise HTTPException(status_code=400, detail="GitHub OAuth is not configured")

        state = deps.store().create_oauth_state(
            user_id=user.user_id,
            provider=GITHUB_PROVIDER,
        )
        return RedirectResponse(github_authorize_url(state=state))

    @router.get("/oauth/github/callback")
    async def github_oauth_callback(
        code: str | None = None,
        state: str | None = None,
        error: str | None = None,
        error_description: str | None = None,
    ) -> RedirectResponse:
        if not deps.github_tools_enabled():
            return RedirectResponse("/?oauth_error=GitHub%20tools%20are%20disabled")
        if error is not None:
            return RedirectResponse(
                f"/?oauth_error={quote(error_description or error, safe='')}"
            )
        if not code or not state:
            return RedirectResponse("/?oauth_error=Missing%20GitHub%20OAuth%20callback")

        consumed = deps.store().consume_oauth_state(
            state=state,
            provider=GITHUB_PROVIDER,
        )
        if consumed is None:
            return RedirectResponse(
                "/?oauth_error=Invalid%20or%20expired%20OAuth%20state"
            )

        user_id, _metadata = consumed
        try:
            token_payload = await asyncio.to_thread(exchange_github_code, code)
            access_token = token_payload.get("access_token")
            if not isinstance(access_token, str):
                raise GitHubOAuthError("GitHub did not return an access token.")
            github_user = await asyncio.to_thread(fetch_github_user, access_token)
        except GitHubOAuthError as err:
            return RedirectResponse(f"/?oauth_error={quote(str(err), safe='')}")

        deps.store().upsert_oauth_connection(
            user_id=user_id,
            provider=GITHUB_PROVIDER,
            access_token=access_token,
            token_type=str(token_payload.get("token_type") or "bearer"),
            scope=str(token_payload.get("scope") or ""),
            provider_user_id=(
                str(github_user.get("id")) if github_user.get("id") is not None else None
            ),
            provider_user_login=(
                str(github_user.get("login"))
                if github_user.get("login") is not None
                else None
            ),
        )
        return RedirectResponse("/?github=connected")

    return router


async def _start_mcp_oauth_flow(
    deps: ToolRouteDeps,
    *,
    user: AuthenticatedUser,
    label: str,
    server_url: str,
    tool_prefix: str,
    server_id: str | None = None,
) -> str:
    normalized_label = label.strip()
    normalized_server_url = server_url.strip()
    normalized_tool_prefix = tool_prefix.strip()
    if not normalized_label:
        raise HTTPException(status_code=400, detail="MCP server label is required.")
    if not normalized_server_url:
        raise HTTPException(status_code=400, detail="MCP server URL is required.")
    if not normalized_tool_prefix:
        raise HTTPException(status_code=400, detail="MCP tool prefix is required.")

    existing_server = None
    if server_id is None:
        existing_server = await _find_matching_mcp_server(
            deps,
            user,
            server_url=normalized_server_url,
            tool_prefix=normalized_tool_prefix,
        )

    flow = PendingMcpOAuthFlow(
        user_id=user.user_id,
        server_id=mcp_server_id(
            server_id or (existing_server.server_id if existing_server else None)
        ),
        server_url=normalized_server_url,
        tool_prefix=normalized_tool_prefix,
        label=normalized_label,
    )
    deps.mcp_oauth_flows()[flow.flow_id] = flow
    flow.task = asyncio.create_task(_complete_mcp_oauth_flow(deps, flow))

    try:
        await asyncio.wait_for(flow.auth_url_ready.wait(), timeout=30)
    except asyncio.TimeoutError as err:
        deps.mcp_oauth_flows().pop(flow.flow_id, None)
        if flow.task is not None:
            flow.task.cancel()
        raise HTTPException(
            status_code=504,
            detail="Timed out starting MCP OAuth.",
        ) from err

    if flow.start_error:
        deps.mcp_oauth_flows().pop(flow.flow_id, None)
        raise HTTPException(status_code=400, detail=flow.start_error)
    if not flow.auth_url:
        raise HTTPException(status_code=500, detail="MCP OAuth did not start.")
    return flow.auth_url


async def _complete_mcp_oauth_flow(
    deps: ToolRouteDeps,
    flow: PendingMcpOAuthFlow,
) -> None:
    try:
        await authorize_mcp_oauth_flow(flow=flow, store=deps.store())
        connection = deps.store().get_oauth_connection(
            user_id=flow.user_id,
            provider=mcp_oauth_provider(flow.server_id),
        )
        if connection is None or not connection.access_token:
            raise RuntimeError("MCP OAuth completed without storing a connection.")

        discovered_url, tools = await _discover_mcp_tools_for_user_request(
            flow.server_url,
            tool_prefix=flow.tool_prefix,
            auth_ref=mcp_oauth_provider(flow.server_id),
        )
        server = HttpMcpServerConfig(
            server_id=flow.server_id,
            label=flow.label,
            server_url=discovered_url,
            tool_prefix=flow.tool_prefix,
            auth_ref=mcp_oauth_provider(flow.server_id),
            auth_mode="oauth",
            tools=tools,
        )
        await deps.upsert_user_mcp_server(
            AuthenticatedUser(user_id=flow.user_id, username=""),
            server,
        )
    except Exception as err:
        flow.fail(err)
        raise
    finally:
        deps.mcp_oauth_flows().pop(flow.flow_id, None)


async def _discover_mcp_tools_for_user_request(
    server_url: str,
    *,
    tool_prefix: str,
    auth_ref: str | None,
) -> tuple[str, list[Any]]:
    first_error: Exception | None = None
    for candidate_url in mcp_server_url_candidates(server_url):
        try:
            return candidate_url, await discover_http_mcp_tools(
                server_url=candidate_url,
                tool_prefix=tool_prefix,
                auth_ref=auth_ref,
            )
        except Exception as err:
            if first_error is None:
                first_error = err
            if mcp_error_requires_auth(err):
                raise err

    if first_error is not None:
        raise first_error
    raise ValueError("MCP server URL is required.")


async def _find_matching_mcp_server(
    deps: ToolRouteDeps,
    user: AuthenticatedUser,
    *,
    server_url: str,
    tool_prefix: str,
) -> HttpMcpServerConfig | None:
    normalized_url = server_url.strip().rstrip("/")
    candidate_urls = set(mcp_server_url_candidates(normalized_url))
    for server in await (
        await deps.ensure_user_chats_workflow(user.user_id, user.username)
    ).query(UserChatsWorkflow.list_mcp_servers):
        if server.tool_prefix != tool_prefix:
            continue
        server_urls = set(mcp_server_url_candidates(server.server_url))
        if (
            normalized_url in server_urls
            or server.server_url.rstrip("/") in candidate_urls
        ):
            return server
    return None
