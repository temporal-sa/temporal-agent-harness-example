from __future__ import annotations

import asyncio
import json
import os
import re
from contextlib import asynccontextmanager, suppress
from dataclasses import asdict, is_dataclass, replace
from pathlib import Path
from typing import Any, AsyncIterator, Literal
from urllib.parse import quote, urlparse
from uuid import uuid4

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response, StreamingResponse
from pydantic import BaseModel
from temporalio.client import Client
from temporalio.common import WorkflowIDConflictPolicy
from temporalio.service import RPCError, RPCStatusCode

from claude_harness.mcp import (
    discover_http_mcp_tools,
    configure_mcp_auth_resolver,
    configure_mcp_http_auth_resolver,
    public_mcp_tool_name,
)
from claude_harness.mcp_types import HttpMcpServerConfig
from simple_chat_agent import TASK_QUEUE
from simple_chat_agent.auth import (
    DEFAULT_SESSION_SECONDS,
    SESSION_COOKIE,
    AuthError,
    AuthenticatedUser,
    authenticate_user,
    create_session_token,
    user_from_session_token,
)
from simple_chat_agent.env import load_dotenv
from simple_chat_agent.external_storage import simple_chat_data_converter
from simple_chat_agent.github_oauth import (
    GITHUB_PROVIDER,
    GitHubOAuthError,
    exchange_github_code,
    fetch_github_user,
    github_authorize_url,
    github_oauth_configured,
    github_scopes,
)
from simple_chat_agent.mcp_auth import (
    mcp_oauth_provider,
    resolve_mcp_auth_headers,
    resolve_mcp_http_auth,
)
from simple_chat_agent.mcp_oauth import (
    PendingMcpOAuthFlow,
    mcp_oauth_provider_for_flow,
)
from simple_chat_agent.streaming import stream_path
from simple_chat_agent.store import AppStore, ArtifactRecord
from simple_chat_agent.tools import (
    CREATE_ARTIFACT_TOOL,
    CREATE_SUBAGENT_TOOL,
    FETCH_URL_TOOL,
    GITHUB_TOOL_NAMES,
    PYTHON_SANDBOX_TOOL,
    tool_names_for_connections,
)
from simple_chat_agent.user_chats_workflow import (
    ChatRecord,
    CreateChatRequest,
    DeleteMcpServerRequest,
    UpdateMcpServerRequest,
    TouchChatRequest,
    UserChatsInput,
    UserChatsWorkflow,
    user_chats_workflow_id,
)
from simple_chat_agent.workflow import (
    DEFAULT_MAX_TOKENS,
    SimpleChatState,
    SimpleChatWorkflow,
)

STATE_POLL_INTERVAL_SECONDS = 0.1
STREAM_POLL_INTERVAL_SECONDS = 0.02


class CreateSessionRequest(BaseModel):
    system_prompt: str = "You are a concise test chatbot."
    model: str | None = None
    max_tokens: int = DEFAULT_MAX_TOKENS
    max_turns: int = 20


class LoginRequest(BaseModel):
    username: str
    password: str


class MessageRequest(BaseModel):
    message: str


class SteerRequest(MessageRequest):
    mode: Literal["immediate", "after_next_tool_result"] = "immediate"


class ApprovalDecisionRequest(BaseModel):
    decision: Literal["allow", "always_allow", "deny"]


class McpServerRequest(BaseModel):
    label: str
    server_url: str
    tool_prefix: str
    auth_mode: Literal["none", "bearer", "oauth"] = "none"
    bearer_token: str | None = None


class McpServerEnabledRequest(BaseModel):
    enabled: bool


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    load_dotenv()
    configure_mcp_auth_resolver(resolve_mcp_auth_headers)
    configure_mcp_http_auth_resolver(resolve_mcp_http_auth)
    app.state.temporal_client = await Client.connect(
        "localhost:7233",
        data_converter=simple_chat_data_converter(),
    )
    app.state.store = AppStore()
    app.state.mcp_oauth_flows = {}
    yield


app = FastAPI(lifespan=lifespan)


@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    return INDEX_HTML


@app.get("/api/me")
async def me(request: Request) -> dict[str, str]:
    user = _current_user(request)
    return {"user_id": user.user_id, "username": user.username}


@app.post("/api/login")
async def login(request: LoginRequest) -> Response:
    user = authenticate_user(request.username, request.password)
    if user is None:
        raise HTTPException(status_code=401, detail="Invalid username or password")

    response = Response(
        content=json.dumps({"status": "ok", "username": user.username}),
        media_type="application/json",
    )
    response.set_cookie(
        SESSION_COOKIE,
        create_session_token(user),
        max_age=DEFAULT_SESSION_SECONDS,
        httponly=True,
        samesite="lax",
    )
    return response


@app.post("/api/logout")
async def logout() -> Response:
    response = Response(
        content=json.dumps({"status": "ok"}),
        media_type="application/json",
    )
    response.delete_cookie(SESSION_COOKIE)
    return response


@app.get("/api/conversations")
async def conversations(request: Request) -> dict[str, Any]:
    user = _current_user(request)
    conversations = await _list_user_chats(user.user_id)
    return {
        "conversations": [
            {
                **asdict(conversation),
                "temporal_ui_url": _temporal_ui_url(
                    namespace=_client().namespace,
                    workflow_id=conversation.workflow_id,
                    run_id=conversation.run_id,
                ),
            }
            for conversation in conversations
        ]
    }


@app.post("/api/sessions")
async def create_session(
    request: Request,
    session_request: CreateSessionRequest,
) -> dict[str, str]:
    user = _current_user(request)
    client = _client()
    github_connection = _store().get_oauth_connection(
        user_id=user.user_id,
        provider=GITHUB_PROVIDER,
    )
    github_connection_id = (
        github_connection.connection_id if github_connection is not None else None
    )
    registry = await _ensure_user_chats_workflow(user.user_id)
    mcp_servers = await registry.query(UserChatsWorkflow.list_mcp_servers)
    conversation = await registry.execute_update(
        UserChatsWorkflow.create_chat,
        CreateChatRequest(
            system_prompt=session_request.system_prompt,
            model=session_request.model
            or os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-5"),
            max_tokens=session_request.max_tokens,
            max_turns=session_request.max_turns,
            available_tool_names=tool_names_for_connections(
                github_connection_id=github_connection_id,
                mcp_servers=mcp_servers,
            ),
            github_connection_id=github_connection_id,
            mcp_servers=mcp_servers,
        ),
    )
    stream_path(conversation.workflow_id).unlink(missing_ok=True)
    return {
        "workflow_id": conversation.workflow_id,
        "run_id": conversation.run_id,
        "temporal_ui_url": _temporal_ui_url(
            namespace=client.namespace,
            workflow_id=conversation.workflow_id,
            run_id=conversation.run_id,
        ),
    }


@app.get("/api/sessions/{workflow_id}/state")
async def get_state(request: Request, workflow_id: str) -> dict[str, Any]:
    user = await _require_conversation_owner(request, workflow_id)
    try:
        state = await _query_state(workflow_id)
    except Exception as err:
        if not _is_temporal_not_found(err):
            raise
        await _forget_conversation(user.user_id, workflow_id)
        raise HTTPException(
            status_code=404,
            detail="Workflow execution not found. Start a new chat.",
        ) from err
    return _state_to_dict(
        state,
        artifacts=_store().list_artifacts(
            user_id=user.user_id,
            workflow_id=workflow_id,
        ),
    )


@app.post("/api/sessions/{workflow_id}/chat")
async def chat(
    http_request: Request,
    workflow_id: str,
    request: MessageRequest,
) -> dict[str, str]:
    user = await _require_conversation_owner(http_request, workflow_id)
    await _signal_workflow(
        http_request,
        workflow_id,
        SimpleChatWorkflow.chat,
        request.message,
    )
    await _touch_conversation(
        user.user_id,
        workflow_id,
        title=_conversation_title(request.message),
    )
    return {"status": "ok"}


@app.post("/api/sessions/{workflow_id}/steer")
async def steer(
    http_request: Request,
    workflow_id: str,
    request: SteerRequest,
) -> dict[str, str]:
    user = await _require_conversation_owner(http_request, workflow_id)
    await _signal_workflow(
        http_request,
        workflow_id,
        SimpleChatWorkflow.steer,
        args=[request.message, request.mode],
    )
    await _touch_conversation(user.user_id, workflow_id)
    return {"status": "ok"}


@app.post("/api/sessions/{workflow_id}/interrupt")
async def interrupt(
    http_request: Request,
    workflow_id: str,
    request: MessageRequest,
) -> dict[str, str]:
    user = await _require_conversation_owner(http_request, workflow_id)
    await _signal_workflow(
        http_request,
        workflow_id,
        SimpleChatWorkflow.interrupt,
        request.message,
    )
    await _touch_conversation(user.user_id, workflow_id)
    return {"status": "ok"}


@app.post("/api/sessions/{workflow_id}/approvals/{approval_id}")
async def resolve_approval(
    http_request: Request,
    workflow_id: str,
    approval_id: str,
    request: ApprovalDecisionRequest,
) -> dict[str, str]:
    user = await _require_conversation_owner(http_request, workflow_id)
    await _signal_workflow(
        http_request,
        workflow_id,
        SimpleChatWorkflow.resolve_approval,
        args=[approval_id, request.decision],
    )
    await _touch_conversation(user.user_id, workflow_id)
    return {"status": "ok"}


@app.get("/api/sessions/{workflow_id}/artifacts")
async def list_session_artifacts(
    request: Request,
    workflow_id: str,
) -> dict[str, Any]:
    user = await _require_conversation_owner(request, workflow_id)
    return {
        "artifacts": _artifact_dicts(
            _store().list_artifacts(user_id=user.user_id, workflow_id=workflow_id)
        )
    }


@app.get("/api/artifacts/{artifact_id}")
async def view_artifact(request: Request, artifact_id: str) -> Response:
    user = _current_user(request)
    artifact = _store().get_artifact(
        user_id=user.user_id,
        artifact_id=artifact_id,
    )
    if artifact is None:
        raise HTTPException(status_code=404, detail="Artifact not found")
    return _artifact_response(artifact, disposition="inline")


@app.get("/api/artifacts/{artifact_id}/download")
async def download_artifact(request: Request, artifact_id: str) -> Response:
    user = _current_user(request)
    artifact = _store().get_artifact(
        user_id=user.user_id,
        artifact_id=artifact_id,
    )
    if artifact is None:
        raise HTTPException(status_code=404, detail="Artifact not found")
    return _artifact_response(artifact, disposition="attachment")


@app.get("/api/sessions/{workflow_id}/events")
async def events(workflow_id: str, request: Request) -> StreamingResponse:
    await _require_conversation_owner(request, workflow_id)
    return StreamingResponse(
        _event_stream(workflow_id, request),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.delete("/api/sessions/{workflow_id}")
async def delete_session(request: Request, workflow_id: str) -> dict[str, str]:
    user = await _require_conversation_owner(request, workflow_id)
    await (await _ensure_user_chats_workflow(user.user_id)).execute_update(
        UserChatsWorkflow.delete_chat,
        workflow_id,
    )
    _store().delete_artifacts_for_conversation(
        user_id=user.user_id,
        workflow_id=workflow_id,
    )
    stream_path(workflow_id).unlink(missing_ok=True)
    return {"status": "ok"}


@app.get("/api/tools")
async def tools(request: Request) -> dict[str, Any]:
    user = _current_user(request)
    github_connection = _store().get_oauth_connection(
        user_id=user.user_id,
        provider=GITHUB_PROVIDER,
    )
    mcp_servers = await (await _ensure_user_chats_workflow(user.user_id)).query(
        UserChatsWorkflow.list_mcp_servers
    )
    return {
        "tools": [
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
                    PYTHON_SANDBOX_TOOL,
                    CREATE_ARTIFACT_TOOL,
                    CREATE_SUBAGENT_TOOL,
                ],
            },
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
            *[
                {
                    "provider": f"mcp:{server.server_id}",
                    "label": server.label,
                    "configured": True,
                    "connected": server.auth_ref is not None
                    or server.auth_mode == "none",
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
            ],
        ]
    }


@app.post("/api/tools/github/disconnect")
async def disconnect_github(request: Request) -> dict[str, str]:
    user = _current_user(request)
    _store().delete_oauth_connection(
        user_id=user.user_id,
        provider=GITHUB_PROVIDER,
    )
    await _update_user_workflows_tool_connections(user)
    return {"status": "ok"}


@app.get("/api/mcp-servers")
async def list_mcp_servers(request: Request) -> dict[str, Any]:
    user = _current_user(request)
    servers = await (await _ensure_user_chats_workflow(user.user_id)).query(
        UserChatsWorkflow.list_mcp_servers
    )
    return {"servers": [asdict(server) for server in servers]}


@app.post("/api/mcp-servers")
async def add_mcp_server(
    http_request: Request,
    request: McpServerRequest,
) -> dict[str, Any]:
    user = _current_user(http_request)
    server_id = f"mcp-{uuid4().hex[:12]}"
    auth_ref = None

    if request.auth_mode == "oauth":
        raise HTTPException(
            status_code=400,
            detail="Use the MCP OAuth flow to add OAuth-discovered MCP servers.",
        )

    if request.auth_mode == "bearer":
        if not request.bearer_token:
            raise HTTPException(status_code=400, detail="Bearer token is required.")
        auth_ref = _store().upsert_oauth_connection(
            user_id=user.user_id,
            provider=mcp_oauth_provider(server_id),
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
            _store().delete_oauth_connection(
                user_id=user.user_id,
                provider=mcp_oauth_provider(server_id),
            )
        raise HTTPException(
            status_code=400,
            detail=_mcp_discovery_error_message(err),
        ) from err

    if not tools:
        if auth_ref is not None:
            _store().delete_oauth_connection(
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
    await _upsert_user_mcp_server(user, server)
    return {"server": asdict(server)}


@app.post("/api/mcp-servers/{server_id}/enabled")
async def set_mcp_server_enabled(
    request: Request,
    server_id: str,
    update: McpServerEnabledRequest,
) -> dict[str, Any]:
    user = _current_user(request)
    registry = await _ensure_user_chats_workflow(user.user_id)
    servers = await registry.query(UserChatsWorkflow.list_mcp_servers)
    existing = next(
        (server for server in servers if server.server_id == server_id),
        None,
    )
    if existing is None:
        raise HTTPException(status_code=404, detail="MCP server not found.")

    updated = replace(existing, enabled=update.enabled)
    await _upsert_user_mcp_server(user, updated)
    return {"server": asdict(updated)}


@app.delete("/api/mcp-servers/{server_id}")
async def delete_mcp_server(request: Request, server_id: str) -> dict[str, str]:
    user = _current_user(request)
    registry = await _ensure_user_chats_workflow(user.user_id)
    remaining_servers = [
        server
        for server in await registry.query(UserChatsWorkflow.list_mcp_servers)
        if server.server_id != server_id
    ]
    await registry.execute_update(
        UserChatsWorkflow.delete_mcp_server,
        DeleteMcpServerRequest(
            server_id=server_id,
            available_tool_names=tool_names_for_connections(
                github_connection_id=_github_connection_id_for_user(user),
                mcp_servers=remaining_servers,
            ),
            github_connection_id=_github_connection_id_for_user(user),
        ),
    )
    _store().delete_oauth_connection(
        user_id=user.user_id,
        provider=mcp_oauth_provider(server_id),
    )
    return {"status": "ok"}


@app.get("/api/mcp-servers/oauth/start")
async def start_mcp_oauth(
    request: Request,
    label: str,
    server_url: str,
    tool_prefix: str,
    server_id: str | None = None,
) -> RedirectResponse:
    user = _current_user(request)
    normalized_label = label.strip()
    normalized_server_url = server_url.strip()
    normalized_tool_prefix = tool_prefix.strip()
    if not normalized_label:
        raise HTTPException(status_code=400, detail="MCP server label is required.")
    if not normalized_server_url:
        raise HTTPException(status_code=400, detail="MCP server URL is required.")
    if not normalized_tool_prefix:
        raise HTTPException(status_code=400, detail="MCP tool prefix is required.")

    flow = PendingMcpOAuthFlow(
        user_id=user.user_id,
        server_id=_mcp_server_id(server_id),
        server_url=normalized_server_url,
        tool_prefix=normalized_tool_prefix,
        label=normalized_label,
    )
    _mcp_oauth_flows()[flow.flow_id] = flow
    flow.task = asyncio.create_task(_complete_mcp_oauth_flow(flow))

    try:
        await asyncio.wait_for(flow.auth_url_ready.wait(), timeout=30)
    except asyncio.TimeoutError as err:
        _mcp_oauth_flows().pop(flow.flow_id, None)
        if flow.task is not None:
            flow.task.cancel()
        raise HTTPException(
            status_code=504,
            detail="Timed out starting MCP OAuth.",
        ) from err

    if flow.start_error:
        _mcp_oauth_flows().pop(flow.flow_id, None)
        raise HTTPException(status_code=400, detail=flow.start_error)
    if not flow.auth_url:
        raise HTTPException(status_code=500, detail="MCP OAuth did not start.")
    return RedirectResponse(flow.auth_url)


@app.get("/oauth/mcp/callback")
async def mcp_oauth_callback(
    flow_id: str,
    code: str | None = None,
    state: str | None = None,
    error: str | None = None,
    error_description: str | None = None,
) -> RedirectResponse:
    flow = _mcp_oauth_flows().get(flow_id)
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


@app.get("/oauth/github/start")
async def github_oauth_start(request: Request) -> RedirectResponse:
    user = _current_user(request)
    if not github_oauth_configured():
        raise HTTPException(status_code=400, detail="GitHub OAuth is not configured")

    state = _store().create_oauth_state(
        user_id=user.user_id,
        provider=GITHUB_PROVIDER,
    )
    return RedirectResponse(github_authorize_url(state=state))


@app.get("/oauth/github/callback")
async def github_oauth_callback(
    code: str | None = None,
    state: str | None = None,
    error: str | None = None,
    error_description: str | None = None,
) -> RedirectResponse:
    if error is not None:
        return RedirectResponse(
            f"/?oauth_error={quote(error_description or error, safe='')}"
        )
    if not code or not state:
        return RedirectResponse("/?oauth_error=Missing%20GitHub%20OAuth%20callback")

    consumed = _store().consume_oauth_state(
        state=state,
        provider=GITHUB_PROVIDER,
    )
    if consumed is None:
        return RedirectResponse("/?oauth_error=Invalid%20or%20expired%20OAuth%20state")

    user_id, _metadata = consumed
    try:
        token_payload = await asyncio.to_thread(exchange_github_code, code)
        access_token = token_payload.get("access_token")
        if not isinstance(access_token, str):
            raise GitHubOAuthError("GitHub did not return an access token.")
        github_user = await asyncio.to_thread(fetch_github_user, access_token)
    except GitHubOAuthError as err:
        return RedirectResponse(f"/?oauth_error={quote(str(err), safe='')}")

    _store().upsert_oauth_connection(
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
    await _update_user_workflows_tool_connections(
        AuthenticatedUser(user_id=user_id, username="")
    )
    return RedirectResponse("/?github=connected")


async def _complete_mcp_oauth_flow(flow: PendingMcpOAuthFlow) -> None:
    try:
        discovered_url, tools = await _discover_oauth_mcp_tools_for_flow(flow)
        connection = _store().get_oauth_connection(
            user_id=flow.user_id,
            provider=mcp_oauth_provider(flow.server_id),
        )
        if connection is None:
            raise RuntimeError("MCP OAuth completed without storing a connection.")

        server = HttpMcpServerConfig(
            server_id=flow.server_id,
            label=flow.label,
            server_url=discovered_url,
            tool_prefix=flow.tool_prefix,
            auth_ref=connection.connection_id,
            auth_mode="oauth",
            tools=tools,
        )
        await _upsert_user_mcp_server(
            AuthenticatedUser(user_id=flow.user_id, username=""),
            server,
        )
    except Exception as err:
        flow.fail(err)
        raise
    finally:
        _mcp_oauth_flows().pop(flow.flow_id, None)


async def _discover_oauth_mcp_tools_for_flow(
    flow: PendingMcpOAuthFlow,
) -> tuple[str, list[Any]]:
    first_error: Exception | None = None
    original_url = flow.server_url
    for candidate_url in _mcp_server_url_candidates(original_url):
        flow.server_url = candidate_url
        try:
            auth = mcp_oauth_provider_for_flow(flow=flow, store=_store())
            return candidate_url, await discover_http_mcp_tools(
                server_url=candidate_url,
                tool_prefix=flow.tool_prefix,
                http_auth=auth,
            )
        except Exception as err:
            if flow.auth_url is not None:
                raise err
            if first_error is None:
                first_error = err

    flow.server_url = original_url
    if first_error is not None:
        raise first_error
    raise ValueError("MCP server URL is required.")


async def _discover_mcp_tools_for_user_request(
    server_url: str,
    *,
    tool_prefix: str,
    auth_ref: str | None,
) -> tuple[str, list[Any]]:
    first_error: Exception | None = None
    for candidate_url in _mcp_server_url_candidates(server_url):
        try:
            return candidate_url, await discover_http_mcp_tools(
                server_url=candidate_url,
                tool_prefix=tool_prefix,
                auth_ref=auth_ref,
            )
        except Exception as err:
            if first_error is None:
                first_error = err
            if _mcp_error_requires_auth(err):
                raise err

    if first_error is not None:
        raise first_error
    raise ValueError("MCP server URL is required.")


def _mcp_server_url_candidates(server_url: str) -> list[str]:
    normalized = server_url.strip().rstrip("/")
    if not normalized:
        return []

    candidates = [normalized]
    parsed = urlparse(normalized)
    if parsed.scheme in ("http", "https") and parsed.path in ("", "/"):
        candidates.append(f"{normalized}/mcp")
    return candidates


def _mcp_server_id(server_id: str | None) -> str:
    if server_id is None:
        return f"mcp-{uuid4().hex[:12]}"

    sanitized = re.sub(r"[^A-Za-z0-9_-]+", "-", server_id.strip())
    sanitized = sanitized.strip("-_")
    return sanitized or f"mcp-{uuid4().hex[:12]}"


def _mcp_discovery_error_message(err: BaseException) -> str:
    if _mcp_error_requires_auth(err):
        return (
            "MCP server requires authentication. Select OAuth discovery if the "
            "server supports MCP OAuth, or use bearer auth if you already have "
            "an access token."
        )

    message = _first_exception_message(err)
    if message:
        return f"MCP discovery failed: {message}"
    return "MCP discovery failed."


def _mcp_error_requires_auth(err: BaseException) -> bool:
    for nested in _walk_exception_tree(err):
        response = getattr(nested, "response", None)
        status_code = getattr(response, "status_code", None)
        if status_code in (401, 403):
            return True
        message = str(nested)
        if "401 Unauthorized" in message or "403 Forbidden" in message:
            return True
    return False


def _first_exception_message(err: BaseException) -> str:
    for nested in _walk_exception_tree(err):
        message = str(nested).strip()
        if message:
            return message
    return ""


def _walk_exception_tree(err: BaseException) -> list[BaseException]:
    if isinstance(err, BaseExceptionGroup):
        nested_errors: list[BaseException] = []
        for nested in err.exceptions:
            nested_errors.extend(_walk_exception_tree(nested))
        return nested_errors
    return [err]


async def _event_stream(workflow_id: str, request: Request) -> AsyncIterator[str]:
    path = stream_path(workflow_id)
    offset = 0
    last_state_json: str | None = None
    state_elapsed = STATE_POLL_INTERVAL_SECONDS
    user = _current_user(request)

    while not await request.is_disconnected():
        if path.exists():
            with path.open("r", encoding="utf-8") as stream:
                stream.seek(offset)
                lines = stream.readlines()
                offset = stream.tell()

            for line in lines:
                with suppress(json.JSONDecodeError):
                    yield _sse("stream", json.loads(line))

        state_elapsed += STREAM_POLL_INTERVAL_SECONDS
        if state_elapsed >= STATE_POLL_INTERVAL_SECONDS:
            state_elapsed = 0
            try:
                state = _state_to_dict(
                    await _query_state(workflow_id),
                    artifacts=_store().list_artifacts(
                        user_id=user.user_id,
                        workflow_id=workflow_id,
                    ),
                )
            except Exception as err:
                if _is_temporal_not_found(err):
                    user = _current_user(request)
                    await _forget_conversation(user.user_id, workflow_id)
                    yield _sse(
                        "missing",
                        {
                            "workflow_id": workflow_id,
                            "message": "Workflow execution was not found.",
                        },
                    )
                    break

                yield _sse(
                    "error",
                    {"message": f"{type(err).__name__}: {err}"},
                )
            else:
                state_json = json.dumps(state, separators=(",", ":"))
                if state_json != last_state_json:
                    last_state_json = state_json
                    yield _sse("state", state)

        await asyncio.sleep(STREAM_POLL_INTERVAL_SECONDS)


async def _query_state(workflow_id: str) -> SimpleChatState:
    return await _handle(workflow_id).query(SimpleChatWorkflow.state)


async def _signal_workflow(
    request: Request,
    workflow_id: str,
    signal: Any,
    *signal_args: Any,
    args: list[Any] | None = None,
) -> None:
    try:
        if args is not None:
            if signal_args:
                raise TypeError("Use either positional signal args or args=, not both")
            await _handle(workflow_id).signal(signal, args=args)
        else:
            await _handle(workflow_id).signal(signal, *signal_args)
    except Exception as err:
        if not _is_temporal_not_found(err):
            raise

        user = _current_user(request)
        await _forget_conversation(user.user_id, workflow_id)
        raise HTTPException(
            status_code=404,
            detail="Workflow execution not found. Start a new chat.",
        ) from err


async def _list_user_chats(user_id: str) -> list[ChatRecord]:
    handle = await _ensure_user_chats_workflow(user_id)
    return await handle.query(UserChatsWorkflow.list_chats)


async def _touch_conversation(
    user_id: str,
    workflow_id: str,
    *,
    title: str | None = None,
) -> None:
    await (await _ensure_user_chats_workflow(user_id)).execute_update(
        UserChatsWorkflow.touch_chat,
        TouchChatRequest(workflow_id=workflow_id, title=title),
    )


async def _forget_conversation(user_id: str, workflow_id: str) -> None:
    await (await _ensure_user_chats_workflow(user_id)).execute_update(
        UserChatsWorkflow.forget_chat,
        workflow_id,
    )
    _store().delete_artifacts_for_conversation(
        user_id=user_id,
        workflow_id=workflow_id,
    )
    stream_path(workflow_id).unlink(missing_ok=True)


def _is_temporal_not_found(err: BaseException) -> bool:
    return isinstance(err, RPCError) and err.status == RPCStatusCode.NOT_FOUND


def _handle(workflow_id: str) -> Any:
    return _client().get_workflow_handle(workflow_id)


async def _ensure_user_chats_workflow(user_id: str) -> Any:
    workflow_id = user_chats_workflow_id(user_id)
    return await _client().start_workflow(
        UserChatsWorkflow.run,
        UserChatsInput(user_id=user_id),
        id=workflow_id,
        task_queue=TASK_QUEUE,
        id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
        static_summary="simple chat user registry",
    )


def _client() -> Client:
    client = getattr(app.state, "temporal_client", None)
    if client is None:
        raise HTTPException(status_code=503, detail="Temporal client is not ready")
    return client


def _store() -> AppStore:
    store = getattr(app.state, "store", None)
    if store is None:
        raise HTTPException(status_code=503, detail="App store is not ready")
    return store


def _current_user(request: Request) -> AuthenticatedUser:
    token = request.cookies.get(SESSION_COOKIE)
    if token is None:
        raise HTTPException(status_code=401, detail="Not authenticated")
    try:
        return user_from_session_token(token)
    except AuthError as err:
        raise HTTPException(status_code=401, detail=str(err)) from err


async def _require_conversation_owner(
    request: Request,
    workflow_id: str,
) -> AuthenticatedUser:
    user = _current_user(request)
    if not await (await _ensure_user_chats_workflow(user.user_id)).query(
        UserChatsWorkflow.has_chat,
        workflow_id,
    ):
        raise HTTPException(status_code=404, detail="Conversation not found")
    return user


async def _update_user_workflows_tool_connections(
    user: AuthenticatedUser,
) -> None:
    github_connection_id = _github_connection_id_for_user(user)
    mcp_servers = await (await _ensure_user_chats_workflow(user.user_id)).query(
        UserChatsWorkflow.list_mcp_servers
    )
    available_tool_names = tool_names_for_connections(
        github_connection_id=github_connection_id,
        mcp_servers=mcp_servers,
    )

    for conversation in await _list_user_chats(user.user_id):
        with suppress(Exception):
            await _handle(conversation.workflow_id).signal(
                SimpleChatWorkflow.update_tool_connections,
                args=[available_tool_names, github_connection_id, mcp_servers],
            )


async def _upsert_user_mcp_server(
    user: AuthenticatedUser,
    server: HttpMcpServerConfig,
) -> None:
    registry = await _ensure_user_chats_workflow(user.user_id)
    await registry.execute_update(
        UserChatsWorkflow.upsert_mcp_server,
        UpdateMcpServerRequest(
            server=server,
            available_tool_names=tool_names_for_connections(
                github_connection_id=_github_connection_id_for_user(user),
                mcp_servers=[
                    *[
                        existing
                        for existing in await registry.query(
                            UserChatsWorkflow.list_mcp_servers
                        )
                        if existing.server_id != server.server_id
                    ],
                    server,
                ],
            ),
            github_connection_id=_github_connection_id_for_user(user),
        ),
    )


async def _available_tool_names_for_user(user: AuthenticatedUser) -> list[str]:
    mcp_servers = await (await _ensure_user_chats_workflow(user.user_id)).query(
        UserChatsWorkflow.list_mcp_servers
    )
    return tool_names_for_connections(
        github_connection_id=_github_connection_id_for_user(user),
        mcp_servers=mcp_servers,
    )


def _github_connection_id_for_user(user: AuthenticatedUser) -> str | None:
    github_connection = _store().get_oauth_connection(
        user_id=user.user_id,
        provider=GITHUB_PROVIDER,
    )
    return github_connection.connection_id if github_connection is not None else None


def _mcp_oauth_flows() -> dict[str, PendingMcpOAuthFlow]:
    flows = getattr(app.state, "mcp_oauth_flows", None)
    if flows is None:
        flows = {}
        app.state.mcp_oauth_flows = flows
    return flows


def _state_to_dict(
    state: Any,
    *,
    artifacts: list[ArtifactRecord] | None = None,
) -> dict[str, Any]:
    if is_dataclass(state):
        state_dict = asdict(state)
    elif isinstance(state, dict):
        state_dict = dict(state)
    else:
        raise TypeError(f"Unsupported state type: {type(state).__name__}")

    if artifacts is not None:
        state_dict["artifacts"] = _artifact_dicts(artifacts)
    return state_dict


def _artifact_dicts(artifacts: list[ArtifactRecord]) -> list[dict[str, Any]]:
    return [_artifact_dict(artifact) for artifact in artifacts]


def _artifact_dict(artifact: ArtifactRecord) -> dict[str, Any]:
    return {
        "artifact_id": artifact.artifact_id,
        "conversation_id": artifact.conversation_id,
        "workflow_id": artifact.workflow_id,
        "name": artifact.name,
        "mime_type": artifact.mime_type,
        "size_bytes": artifact.size_bytes,
        "created_at": artifact.created_at,
        "metadata": artifact.metadata,
        "view_url": f"/api/artifacts/{artifact.artifact_id}",
        "download_url": f"/api/artifacts/{artifact.artifact_id}/download",
    }


def _artifact_response(
    artifact: ArtifactRecord,
    *,
    disposition: Literal["inline", "attachment"],
) -> Response:
    path = Path(artifact.path)
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="Artifact file not found")

    return Response(
        path.read_bytes(),
        media_type=(
            artifact.mime_type
            if disposition == "attachment"
            else _safe_inline_media_type(artifact.mime_type)
        ),
        headers={
            "Content-Disposition": _content_disposition(disposition, artifact.name),
            "X-Content-Type-Options": "nosniff",
        },
    )


def _content_disposition(disposition: str, filename: str) -> str:
    ascii_filename = re.sub(r'["\\\r\n]+', "_", filename) or "artifact"
    encoded_filename = quote(filename, safe="")
    return (
        f'{disposition}; filename="{ascii_filename}"; '
        f"filename*=UTF-8''{encoded_filename}"
    )


def _safe_inline_media_type(mime_type: str) -> str:
    if mime_type == "application/pdf":
        return mime_type
    if mime_type.startswith("image/") and mime_type != "image/svg+xml":
        return mime_type
    return "text/plain; charset=utf-8"


def _sse(event: str, data: Any) -> str:
    return f"event: {event}\ndata: {json.dumps(data, default=str)}\n\n"


def _temporal_ui_url(*, namespace: str, workflow_id: str, run_id: str) -> str:
    base_url = os.environ.get("TEMPORAL_UI_URL", "http://localhost:8233").rstrip("/")
    namespace_path = quote(namespace, safe="")
    workflow_path = quote(workflow_id, safe="")
    run_path = quote(run_id, safe="")
    if run_path:
        return (
            f"{base_url}/namespaces/{namespace_path}/workflows/"
            f"{workflow_path}/{run_path}/history"
        )
    return f"{base_url}/namespaces/{namespace_path}/workflows/{workflow_path}"


def _conversation_title(message: str) -> str:
    normalized = " ".join(message.split())
    if not normalized:
        return "New chat"
    if len(normalized) <= 64:
        return normalized
    return f"{normalized[:61]}..."


INDEX_HTML = r"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Simple Chat Agent</title>
  <style>
    :root {
      color-scheme: dark;
      --color-bg-primary: #0d1117;
      --color-bg-secondary: #161b22;
      --color-bg-tertiary: #21262d;
      --color-bg-elevated: #1c2128;
      --color-bg-hover: #30363d;
      --color-bg-active: #388bfd1a;
      --color-border: #30363d;
      --color-border-light: #21262d;
      --color-border-focus: #388bfd;
      --color-text-primary: #e6edf3;
      --color-text-secondary: #8b949e;
      --color-text-tertiary: #6e7681;
      --color-text-link: #58a6ff;
      --color-text-inverse: #0d1117;
      --color-primary: #238636;
      --color-primary-hover: #2ea043;
      --color-danger: #da3633;
      --color-danger-hover: #f85149;
      --color-warning: #d29922;
      --color-info: #58a6ff;
      --color-success: #3fb950;
      --color-queued: #a371f7;
      --shadow-lg: 0 10px 20px rgba(0, 0, 0, 0.4);
      --space-xs: 4px;
      --space-sm: 8px;
      --space-md: 16px;
      --space-lg: 24px;
      --radius-sm: 4px;
      --radius-md: 6px;
      --radius-lg: 8px;
      --sidebar-width: 260px;
      --details-width: 360px;
      --top-bar-height: 56px;
      --font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, "Courier New", monospace;
      --font-size-xs: 11px;
      --font-size-sm: 12px;
      --font-size-md: 14px;
      --font-size-lg: 16px;
      --transition-fast: 150ms ease;
    }
    *, *::before, *::after { box-sizing: border-box; }
    * { margin: 0; padding: 0; }
    html, body { height: 100%; }
    body {
      background: var(--color-bg-primary);
      color: var(--color-text-primary);
      font: var(--font-size-md)/1.5 var(--font-family);
      overflow: hidden;
      -webkit-font-smoothing: antialiased;
      -moz-osx-font-smoothing: grayscale;
    }
    ::selection {
      background: var(--color-bg-active);
      color: var(--color-text-primary);
    }
    ::-webkit-scrollbar {
      width: 8px;
      height: 8px;
    }
    ::-webkit-scrollbar-track { background: var(--color-bg-secondary); }
    ::-webkit-scrollbar-thumb {
      background: var(--color-bg-hover);
      border-radius: 999px;
    }
    ::-webkit-scrollbar-thumb:hover { background: var(--color-text-tertiary); }
    * {
      scrollbar-width: thin;
      scrollbar-color: var(--color-bg-hover) var(--color-bg-secondary);
    }
    [hidden] { display: none !important; }
    .login-screen {
      display: flex;
      align-items: center;
      justify-content: center;
      min-height: 100vh;
      padding: var(--space-md);
      background: var(--color-bg-primary);
    }
    .login-card {
      width: min(380px, 100%);
      padding: var(--space-lg);
      border: 1px solid var(--color-border);
      border-radius: var(--radius-lg);
      background: var(--color-bg-secondary);
      box-shadow: var(--shadow-lg);
    }
    .login-card h1 {
      margin-bottom: var(--space-md);
    }
    .login-form {
      display: grid;
      gap: var(--space-md);
    }
    .login-field {
      display: grid;
      gap: var(--space-xs);
    }
    .login-field label {
      color: var(--color-text-secondary);
      font-size: var(--font-size-sm);
    }
    .login-field input {
      width: 100%;
      height: 40px;
      padding: 0 var(--space-md);
      border: 1px solid var(--color-border);
      border-radius: var(--radius-md);
      background: var(--color-bg-tertiary);
      color: var(--color-text-primary);
      font: inherit;
    }
    .login-error {
      min-height: 18px;
      color: var(--color-danger-hover);
      font-size: var(--font-size-sm);
    }
    .app {
      display: grid;
      grid-template-columns: var(--sidebar-width) minmax(0, 1fr) var(--details-width);
      grid-template-rows: minmax(0, 1fr) auto;
      height: 100vh;
      min-height: 0;
    }
    header {
      display: flex;
      flex-direction: column;
      align-items: stretch;
      grid-column: 1;
      grid-row: 1 / -1;
      min-height: 0;
      background: var(--color-bg-secondary);
      border-right: 1px solid var(--color-border);
    }
    .header-left {
      display: flex;
      flex-direction: column;
      align-items: stretch;
      gap: var(--space-sm);
      padding: var(--space-md);
      border-bottom: 1px solid var(--color-border);
    }
    .header-left::after {
      content: "Harness";
      display: flex;
      align-items: center;
      min-height: 36px;
      margin-top: var(--space-xs);
      padding: var(--space-sm) var(--space-md);
      border-radius: var(--radius-md);
      background: var(--color-bg-active);
      color: var(--color-text-link);
      font-size: var(--font-size-sm);
      font-weight: 500;
    }
    h1 {
      display: flex;
      align-items: center;
      gap: var(--space-sm);
      color: var(--color-text-primary);
      font-size: var(--font-size-lg);
      font-weight: 600;
      line-height: 1.25;
    }
    h1::before {
      content: "";
      width: 32px;
      height: 32px;
      border-radius: var(--radius-md);
      background:
        linear-gradient(135deg, rgba(35, 134, 54, 0.95), rgba(63, 185, 80, 0.72));
      box-shadow: inset 0 0 0 1px rgba(255, 255, 255, 0.12);
      flex: 0 0 auto;
    }
    .temporal-link {
      display: none;
      min-height: 36px;
      align-items: center;
      justify-content: center;
      border: 1px solid var(--color-border);
      border-radius: var(--radius-md);
      padding: var(--space-sm) var(--space-md);
      color: var(--color-text-primary);
      text-decoration: none;
      font-size: var(--font-size-sm);
      font-weight: 500;
      white-space: nowrap;
      background: var(--color-bg-tertiary);
      transition: background-color var(--transition-fast), border-color var(--transition-fast), color var(--transition-fast);
    }
    .temporal-link:hover {
      border-color: var(--color-text-tertiary);
      background: var(--color-bg-hover);
      color: var(--color-text-primary);
      text-decoration: none;
    }
    .status {
      margin-top: auto;
      padding: var(--space-md);
      border-top: 1px solid var(--color-border);
      color: var(--color-text-secondary);
      font-size: var(--font-size-sm);
      white-space: normal;
    }
    .status::before {
      content: "";
      display: inline-block;
      width: 8px;
      height: 8px;
      margin-right: var(--space-sm);
      border-radius: 50%;
      background: var(--color-success);
    }
    .side-panel {
      min-height: 0;
      overflow-y: auto;
      padding: var(--space-sm) var(--space-md);
    }
    .side-section {
      margin-bottom: var(--space-lg);
    }
    .side-section-title {
      margin: var(--space-md) 0 var(--space-xs);
      color: var(--color-text-tertiary);
      font-size: var(--font-size-xs);
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0;
    }
    .side-actions {
      display: grid;
      grid-template-columns: 1fr;
      gap: var(--space-sm);
      margin-top: var(--space-sm);
    }
    .conversation-row {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: var(--space-xs);
      align-items: center;
      margin-bottom: 2px;
    }
    .conversation-item,
    .tool-card {
      width: 100%;
      margin-bottom: 2px;
      padding: var(--space-sm);
      border: 0;
      border-radius: var(--radius-sm);
      background: transparent;
      color: var(--color-text-secondary);
      text-align: left;
      font-size: var(--font-size-sm);
    }
    .conversation-row .conversation-item {
      margin-bottom: 0;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .conversation-delete {
      width: 44px;
      min-height: 32px;
      padding: 0;
      color: var(--color-text-tertiary);
      font-size: var(--font-size-xs);
    }
    .conversation-delete:hover {
      border-color: rgba(218, 54, 51, 0.68);
      color: #ffb4af;
    }
    .conversation-item:hover,
    .tool-card:hover {
      background: var(--color-bg-hover);
      color: var(--color-text-primary);
    }
    .conversation-item.active {
      background: var(--color-bg-active);
      color: var(--color-text-link);
    }
    .tool-card {
      display: grid;
      gap: var(--space-xs);
      border: 1px solid var(--color-border-light);
      background: var(--color-bg-primary);
    }
    .tool-card.connected {
      border-color: rgba(63, 185, 80, 0.34);
    }
    .tool-card.disabled {
      border-color: rgba(110, 118, 129, 0.3);
      opacity: 0.72;
    }
    .tool-actions {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
      gap: var(--space-xs);
      margin-top: var(--space-xs);
    }
    .tool-actions button {
      height: 34px;
      min-height: 34px;
      padding: 0 var(--space-sm);
      font-size: var(--font-size-xs);
    }
    .tool-actions .danger {
      border-color: rgba(218, 54, 51, 0.68);
      color: #ffb4af;
    }
    .tool-actions .danger:hover {
      background: rgba(218, 54, 51, 0.16);
      border-color: var(--color-danger-hover);
      color: #ffd4d0;
    }
    .tool-chip-list {
      display: flex;
      flex-wrap: wrap;
      gap: var(--space-xs);
      margin-top: var(--space-xs);
      min-width: 0;
    }
    .tool-chip {
      display: inline-flex;
      align-items: center;
      max-width: 100%;
      min-width: 0;
      min-height: 22px;
      padding: 0 var(--space-xs);
      border: 1px solid var(--color-border-light);
      border-radius: var(--radius-sm);
      background: rgba(110, 118, 129, 0.1);
      color: var(--color-text-secondary);
      font-size: var(--font-size-xs);
      overflow-wrap: anywhere;
    }
    .mcp-form {
      display: grid;
      gap: var(--space-sm);
      margin: var(--space-sm) 0;
      padding: var(--space-sm);
      border: 1px solid rgba(88, 166, 255, 0.28);
      border-radius: var(--radius-md);
      background: rgba(88, 166, 255, 0.06);
    }
    .mcp-field {
      display: grid;
      gap: 3px;
    }
    .mcp-field label {
      color: var(--color-text-tertiary);
      font-size: var(--font-size-xs);
      font-weight: 600;
      text-transform: uppercase;
    }
    .mcp-field input,
    .mcp-field select {
      width: 100%;
      min-height: 34px;
      border: 1px solid var(--color-border);
      border-radius: var(--radius-sm);
      padding: 0 var(--space-sm);
      background: var(--color-bg-tertiary);
      color: var(--color-text-primary);
      font: inherit;
      font-size: var(--font-size-sm);
    }
    .mcp-field input:focus,
    .mcp-field select:focus {
      outline: none;
      border-color: var(--color-border-focus);
      box-shadow: 0 0 0 3px rgba(56, 139, 253, 0.14);
    }
    .mcp-form-actions {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: var(--space-xs);
    }
    .mcp-form-actions button {
      height: 34px;
      padding: 0 var(--space-sm);
      font-size: var(--font-size-xs);
    }
    .mcp-error {
      color: #ffb4af;
      font-size: var(--font-size-xs);
      overflow-wrap: anywhere;
    }
    .tool-title {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      align-items: center;
      gap: var(--space-sm);
      color: var(--color-text-primary);
      font-weight: 600;
      min-width: 0;
    }
    .tool-label {
      min-width: 0;
      overflow-wrap: anywhere;
    }
    .tool-meta {
      color: var(--color-text-secondary);
      font-size: var(--font-size-xs);
      overflow-wrap: anywhere;
    }
    .tool-status {
      display: inline-flex;
      align-items: center;
      gap: var(--space-xs);
      color: var(--color-text-secondary);
      font-size: var(--font-size-xs);
      max-width: 100%;
      white-space: nowrap;
    }
    .tool-status::before {
      content: "";
      width: 6px;
      height: 6px;
      border-radius: 50%;
      background: var(--color-text-tertiary);
    }
    .tool-card.connected .tool-status::before {
      background: var(--color-success);
    }
    .tool-card.disabled .tool-status::before {
      background: var(--color-warning);
    }
    .tools-overlay {
      position: fixed;
      inset: 0;
      z-index: 20;
      display: flex;
      align-items: stretch;
      justify-content: flex-end;
      background: rgba(1, 4, 9, 0.62);
    }
    .tools-window {
      width: min(760px, calc(100vw - 24px));
      height: 100%;
      display: grid;
      grid-template-rows: auto minmax(0, 1fr);
      border-left: 1px solid var(--color-border);
      background: var(--color-bg-secondary);
      box-shadow: var(--shadow-lg);
    }
    .tools-window-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: var(--space-md);
      padding: var(--space-md);
      border-bottom: 1px solid var(--color-border);
      background: var(--color-bg-elevated);
    }
    .tools-window-title {
      color: var(--color-text-primary);
      font-size: var(--font-size-lg);
      font-weight: 600;
    }
    .tools-window-body {
      min-height: 0;
      overflow-y: auto;
      padding: var(--space-md);
    }
    .tools-section {
      display: grid;
      gap: var(--space-sm);
      margin-bottom: var(--space-lg);
    }
    .tools-section-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: var(--space-sm);
    }
    .tools-section-title {
      color: var(--color-text-tertiary);
      font-size: var(--font-size-xs);
      font-weight: 600;
      text-transform: uppercase;
    }
    .tools-section-actions {
      display: flex;
      flex-wrap: wrap;
      gap: var(--space-xs);
    }
    .tools-section-actions button {
      height: 34px;
      min-height: 34px;
      padding: 0 var(--space-sm);
      font-size: var(--font-size-xs);
    }
    .tools-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(min(100%, 260px), 1fr));
      gap: var(--space-sm);
    }
    .approval-panel {
      position: sticky;
      bottom: var(--space-sm);
      z-index: 3;
      width: min(980px, 92%);
      margin: 0 auto var(--space-md);
      display: grid;
      gap: var(--space-sm);
    }
    .approval-panel-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: var(--space-sm);
      color: #ffb85c;
      font-size: var(--font-size-xs);
      font-weight: 700;
      text-transform: uppercase;
    }
    .approval-panel-count {
      color: var(--color-text-tertiary);
      font-weight: 500;
      text-transform: none;
    }
    .approval-card {
      position: relative;
      display: grid;
      gap: var(--space-sm);
      padding: var(--space-md);
      border: 1px solid rgba(248, 81, 73, 0.34);
      border-radius: var(--radius-md);
      background:
        linear-gradient(90deg, rgba(248, 81, 73, 0.16), rgba(210, 153, 34, 0.08));
      box-shadow: inset 3px 0 0 rgba(248, 81, 73, 0.88), 0 1px 2px rgba(0, 0, 0, 0.3);
    }
    .approval-title {
      color: var(--color-text-primary);
      font-size: var(--font-size-md);
      font-weight: 700;
      overflow-wrap: anywhere;
    }
    .approval-meta {
      display: grid;
      gap: 2px;
      color: var(--color-text-secondary);
      font-size: var(--font-size-xs);
      overflow-wrap: anywhere;
    }
    .approval-meta strong {
      color: var(--color-text-primary);
      font-weight: 600;
    }
    .approval-details {
      max-height: 130px;
      overflow: auto;
      padding: var(--space-sm);
      border: 1px solid rgba(248, 81, 73, 0.2);
      border-radius: var(--radius-sm);
      background: rgba(13, 17, 23, 0.42);
      color: var(--color-text-secondary);
      font-size: var(--font-size-xs);
      overflow-wrap: anywhere;
    }
    .approval-actions {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: var(--space-xs);
    }
    .approval-actions button {
      height: 36px;
      min-height: 36px;
      padding: 0 var(--space-sm);
      font-size: var(--font-size-xs);
      font-weight: 600;
    }
    .approval-actions .allow {
      border-color: rgba(63, 185, 80, 0.52);
      color: #aff5b4;
    }
    .approval-actions .allow:hover {
      background: rgba(63, 185, 80, 0.14);
      border-color: var(--color-success);
    }
    .approval-actions .always {
      border-color: rgba(210, 153, 34, 0.54);
      color: #ffd58a;
    }
    .approval-actions .always:hover {
      background: rgba(210, 153, 34, 0.14);
      border-color: var(--color-warning);
    }
    .approval-actions .deny {
      border-color: rgba(248, 81, 73, 0.72);
      color: #ffb4af;
    }
    .approval-actions .deny:hover {
      background: rgba(248, 81, 73, 0.16);
      border-color: var(--color-danger-hover);
      color: #ffd4d0;
    }
    .artifact-panel {
      display: grid;
      align-content: start;
      gap: var(--space-sm);
      padding: var(--space-md);
      border: 1px solid rgba(88, 166, 255, 0.22);
      border-radius: var(--radius-md);
      background: rgba(88, 166, 255, 0.045);
      box-shadow: inset 3px 0 0 rgba(126, 231, 135, 0.72), 0 1px 2px rgba(0, 0, 0, 0.25);
    }
    .artifacts-sidebar {
      grid-column: 3;
      grid-row: 1 / -1;
      min-height: 0;
      overflow-y: auto;
      padding: var(--space-md);
      border-left: 1px solid var(--color-border);
      background: var(--color-bg-secondary);
    }
    .artifacts-sidebar .artifact-panel {
      min-height: 100%;
      border-color: var(--color-border-light);
      background: transparent;
      box-shadow: none;
      padding: 0;
    }
    .artifact-empty {
      color: var(--color-text-tertiary);
      font-size: var(--font-size-xs);
      line-height: 1.45;
    }
    .artifact-panel-header {
      display: flex;
      align-items: baseline;
      justify-content: space-between;
      gap: var(--space-sm);
      color: var(--color-text-secondary);
      font-size: var(--font-size-xs);
      font-weight: 700;
      text-transform: uppercase;
    }
    .artifact-panel-count {
      color: var(--color-text-tertiary);
      font-weight: 500;
      text-transform: none;
    }
    .artifact-list {
      display: grid;
      grid-template-columns: 1fr;
      gap: var(--space-sm);
    }
    .artifact-card {
      display: grid;
      gap: var(--space-xs);
      min-width: 0;
      padding: var(--space-sm);
      border: 1px solid rgba(63, 185, 80, 0.26);
      border-radius: var(--radius-sm);
      background: rgba(13, 17, 23, 0.44);
    }
    .artifact-name {
      color: var(--color-text-primary);
      font-weight: 700;
      overflow-wrap: anywhere;
    }
    .artifact-meta {
      color: var(--color-text-secondary);
      font-size: var(--font-size-xs);
      overflow-wrap: anywhere;
    }
    .artifact-actions {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: var(--space-xs);
      margin-top: var(--space-xs);
    }
    .artifact-actions a,
    .artifact-actions button {
      min-height: 32px;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      border: 1px solid var(--color-border);
      border-radius: var(--radius-sm);
      background: var(--color-bg-tertiary);
      color: var(--color-text-primary);
      text-decoration: none;
      font-size: var(--font-size-xs);
      font-weight: 600;
      transition: background-color var(--transition-fast), border-color var(--transition-fast);
    }
    .artifact-actions a:hover,
    .artifact-actions button:hover {
      border-color: var(--color-border-focus);
      background: var(--color-bg-hover);
      text-decoration: none;
    }
    .artifact-viewer-overlay {
      position: fixed;
      inset: 0;
      z-index: 30;
      display: grid;
      place-items: center;
      padding: var(--space-lg);
      background: rgba(1, 4, 9, 0.72);
    }
    .artifact-viewer {
      width: min(1100px, calc(100vw - var(--details-width) - var(--sidebar-width) - 64px));
      height: min(780px, 90vh);
      min-width: min(720px, 96vw);
      display: grid;
      grid-template-rows: auto minmax(0, 1fr);
      border: 1px solid var(--color-border);
      border-radius: var(--radius-lg);
      background: var(--color-bg-secondary);
      box-shadow: var(--shadow-lg);
      overflow: hidden;
    }
    .artifact-viewer-header {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: var(--space-sm);
      align-items: center;
      padding: var(--space-md);
      border-bottom: 1px solid var(--color-border);
      background: var(--color-bg-elevated);
    }
    .artifact-viewer-title {
      min-width: 0;
      display: grid;
      gap: 2px;
    }
    .artifact-viewer-name {
      color: var(--color-text-primary);
      font-weight: 700;
      overflow-wrap: anywhere;
    }
    .artifact-viewer-meta {
      color: var(--color-text-secondary);
      font-size: var(--font-size-xs);
      overflow-wrap: anywhere;
    }
    .artifact-viewer-actions {
      display: flex;
      gap: var(--space-xs);
      align-items: center;
      justify-content: flex-end;
      min-width: max-content;
    }
    .artifact-viewer-actions a,
    .artifact-viewer-actions button {
      min-height: 34px;
      height: 34px;
      min-width: 78px;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      padding: 0 var(--space-sm);
      border: 1px solid var(--color-border);
      border-radius: var(--radius-sm);
      background: var(--color-bg-tertiary);
      color: var(--color-text-primary);
      text-decoration: none;
      font-size: var(--font-size-xs);
      font-weight: 600;
    }
    .artifact-viewer-body {
      min-height: 0;
      overflow: auto;
      padding: var(--space-md);
      background: var(--color-bg-primary);
    }
    .artifact-viewer-body .bubble-content pre {
      min-height: 100%;
    }
    .artifact-viewer-image {
      display: block;
      max-width: 100%;
      max-height: 100%;
      margin: 0 auto;
      object-fit: contain;
      border-radius: var(--radius-sm);
    }
    .artifact-viewer-frame {
      width: 100%;
      height: 100%;
      min-height: 520px;
      border: 1px solid var(--color-border);
      border-radius: var(--radius-sm);
      background: white;
    }
    .artifact-viewer-error {
      color: #ffb4af;
      font-size: var(--font-size-sm);
    }
    main {
      display: grid;
      grid-column: 2;
      grid-row: 1;
      grid-template-columns: minmax(0, 1fr);
      min-height: 0;
      background: var(--color-bg-primary);
    }
    .messages {
      min-height: 0;
      overflow-y: auto;
      padding: var(--space-md);
      scroll-behavior: smooth;
    }
    .sidebar { display: none; }
    .bubble {
      position: relative;
      max-width: min(980px, 88%);
      margin: 0 0 var(--space-md);
      padding: var(--space-md);
      background: var(--color-bg-secondary);
      border: 1px solid var(--color-border);
      border-radius: var(--radius-md);
      white-space: pre-wrap;
      overflow-wrap: anywhere;
      box-shadow: 0 1px 2px rgba(0, 0, 0, 0.3);
    }
    .bubble:hover {
      background: var(--color-bg-tertiary);
      border-color: var(--color-text-tertiary);
    }
    .bubble::after {
      content: "";
      position: absolute;
      left: 0;
      top: 0;
      bottom: 0;
      width: 3px;
      border-radius: var(--radius-md) 0 0 var(--radius-md);
      background: var(--color-text-tertiary);
    }
    .bubble.user, .bubble.pending { margin-left: auto; }
    .bubble.user {
      background: rgba(63, 185, 80, 0.08);
      border-color: rgba(63, 185, 80, 0.28);
    }
    .bubble.user::after { background: var(--color-success); }
    .bubble.assistant {
      background: rgba(88, 166, 255, 0.08);
      border-color: rgba(88, 166, 255, 0.24);
    }
    .bubble.assistant::after { background: var(--color-info); }
    .bubble.system {
      background: rgba(210, 153, 34, 0.08);
      border-color: rgba(210, 153, 34, 0.26);
      color: var(--color-warning);
    }
    .bubble.system::after { background: var(--color-warning); }
    .bubble.pending {
      background: rgba(163, 113, 247, 0.1);
      border-color: rgba(163, 113, 247, 0.28);
      color: #d2b6ff;
      font-style: italic;
    }
    .bubble.pending::after { background: var(--color-queued); }
    .stream-panel {
      width: min(980px, 92%);
      margin: 0 auto var(--space-md);
      border: 1px solid rgba(88, 166, 255, 0.28);
      border-radius: var(--radius-md);
      background: rgba(88, 166, 255, 0.055);
      box-shadow: inset 3px 0 0 rgba(88, 166, 255, 0.75), 0 1px 2px rgba(0, 0, 0, 0.25);
      overflow: hidden;
    }
    .stream-panel.complete {
      border-color: rgba(110, 118, 129, 0.32);
      background: rgba(110, 118, 129, 0.075);
      box-shadow: inset 3px 0 0 rgba(110, 118, 129, 0.78), 0 1px 2px rgba(0, 0, 0, 0.25);
    }
    .stream-panel.collapsed {
      width: min(780px, 86%);
    }
    .stream-panel-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: var(--space-sm);
      padding: var(--space-sm) var(--space-md);
      border-bottom: 1px solid rgba(88, 166, 255, 0.18);
      background: rgba(13, 17, 23, 0.34);
    }
    .stream-panel-title {
      display: flex;
      align-items: baseline;
      gap: var(--space-sm);
      min-width: 0;
      color: var(--color-text-secondary);
      font-size: var(--font-size-xs);
      font-weight: 600;
      text-transform: uppercase;
    }
    .stream-panel-status {
      color: var(--color-text-tertiary);
      font-size: var(--font-size-xs);
      font-weight: 400;
      text-transform: none;
    }
    .stream-panel-toggle {
      min-height: 26px;
      padding: 0 var(--space-sm);
      font-size: var(--font-size-xs);
      color: var(--color-text-secondary);
    }
    .stream-panel-body {
      display: grid;
      gap: var(--space-sm);
      padding: var(--space-sm) var(--space-md) var(--space-md);
      color: var(--color-text-secondary);
      font-size: var(--font-size-xs);
      line-height: 1.45;
    }
    .stream-preview {
      overflow: hidden;
      color: var(--color-text-secondary);
      font-size: var(--font-size-xs);
      line-height: 1.45;
      white-space: nowrap;
      text-overflow: ellipsis;
    }
    .stream-text {
      max-height: 220px;
      overflow-y: auto;
      padding: var(--space-sm);
      border: 1px solid rgba(88, 166, 255, 0.14);
      border-radius: var(--radius-sm);
      background: rgba(13, 17, 23, 0.38);
      color: #b9c7d8;
      font-size: var(--font-size-xs);
      white-space: pre-wrap;
      overflow-wrap: anywhere;
    }
    .stream-finished-list {
      display: grid;
      gap: var(--space-sm);
    }
    .stream-finished-turn {
      display: grid;
      gap: var(--space-xs);
      padding: var(--space-sm);
      border: 1px solid rgba(63, 185, 80, 0.18);
      border-radius: var(--radius-sm);
      background: rgba(63, 185, 80, 0.055);
      color: #b9c7d8;
      font-size: var(--font-size-xs);
      white-space: pre-wrap;
      overflow-wrap: anywhere;
    }
    .stream-finished-title {
      color: var(--color-text-tertiary);
      font-weight: 600;
      text-transform: uppercase;
    }
    .stream-current-turn {
      display: grid;
      gap: var(--space-sm);
      padding: var(--space-sm);
      border: 1px solid rgba(88, 166, 255, 0.18);
      border-radius: var(--radius-sm);
      background: rgba(88, 166, 255, 0.04);
    }
    .stream-tool-list {
      display: grid;
      gap: var(--space-xs);
    }
    .stream-tool-event {
      display: grid;
      gap: 2px;
      padding: var(--space-xs) var(--space-sm);
      border: 1px solid rgba(110, 118, 129, 0.24);
      border-radius: var(--radius-sm);
      background: rgba(13, 17, 23, 0.28);
      color: var(--color-text-secondary);
      font-size: var(--font-size-xs);
    }
    .stream-tool-event.input-streaming {
      border-color: rgba(210, 153, 34, 0.26);
      background: rgba(210, 153, 34, 0.06);
    }
    .stream-tool-name {
      color: var(--color-text-primary);
      font-weight: 600;
    }
    .stream-tool-payload {
      color: var(--color-text-tertiary);
      overflow-wrap: anywhere;
      white-space: pre-wrap;
    }
    .label {
      display: block;
      margin-bottom: var(--space-xs);
      color: var(--color-text-tertiary);
      font-size: var(--font-size-xs);
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0;
    }
    .bubble-content {
      white-space: normal;
    }
    .bubble-content > * + * {
      margin-top: var(--space-sm);
    }
    .bubble-content p {
      margin: 0;
    }
    .bubble-content ul,
    .bubble-content ol {
      margin: 0;
      padding-left: 22px;
    }
    .bubble-content li + li {
      margin-top: 2px;
    }
    .bubble-content strong {
      color: var(--color-text-primary);
      font-weight: 700;
    }
    .bubble-content em {
      color: var(--color-text-secondary);
      font-style: italic;
    }
    .bubble-content code {
      padding: 1px 4px;
      border: 1px solid var(--color-border-light);
      border-radius: var(--radius-sm);
      background: var(--color-bg-primary);
      color: var(--color-text-primary);
      font: inherit;
      font-size: var(--font-size-sm);
    }
    .bubble-content pre {
      position: relative;
      margin: 0;
      padding: var(--space-sm);
      border: 1px solid var(--color-border-light);
      border-radius: var(--radius-sm);
      background: var(--color-bg-primary);
      overflow-x: auto;
      white-space: pre;
    }
    .bubble-content pre[data-language] {
      padding-top: calc(var(--space-sm) + 18px);
    }
    .bubble-content pre[data-language]::before {
      content: attr(data-language);
      position: absolute;
      top: 5px;
      right: 8px;
      color: var(--color-text-tertiary);
      font-size: var(--font-size-xs);
      text-transform: uppercase;
    }
    .bubble-content pre code {
      display: block;
      padding: 0;
      border: 0;
      background: transparent;
      white-space: pre;
    }
    .hl-comment { color: #8b949e; font-style: italic; }
    .hl-keyword { color: #ff7b72; }
    .hl-string { color: #a5d6ff; }
    .hl-number { color: #79c0ff; }
    .hl-function { color: #d2a8ff; }
    .hl-operator { color: #ff7b72; }
    .hl-property { color: #7ee787; }
    .hl-type { color: #ffa657; }
    .hl-tag { color: #7ee787; }
    .hl-attr { color: #d2a8ff; }
    .md-heading {
      color: var(--color-text-primary);
      font-weight: 700;
    }
    .composer {
      display: grid;
      grid-template-columns: 1fr auto auto auto auto;
      grid-column: 2;
      grid-row: 2;
      gap: var(--space-sm);
      padding: var(--space-sm) var(--space-md);
      border-top: 1px solid var(--color-border);
      background: var(--color-bg-secondary);
    }
    textarea {
      width: 100%;
      min-height: 44px;
      max-height: 140px;
      resize: vertical;
      border: 1px solid var(--color-border);
      border-radius: var(--radius-md);
      padding: 11px 12px;
      background: var(--color-bg-tertiary);
      color: var(--color-text-primary);
      font: inherit;
      transition: border-color var(--transition-fast), box-shadow var(--transition-fast);
    }
    textarea:hover { border-color: var(--color-text-tertiary); }
    textarea:focus {
      outline: none;
      border-color: var(--color-border-focus);
      box-shadow: 0 0 0 3px rgba(56, 139, 253, 0.15);
    }
    textarea::placeholder {
      color: var(--color-text-tertiary);
    }
    button {
      height: 44px;
      min-width: 44px;
      border: 1px solid var(--color-border);
      border-radius: var(--radius-md);
      padding: 0 13px;
      background: var(--color-bg-tertiary);
      color: var(--color-text-primary);
      font: inherit;
      font-size: var(--font-size-sm);
      font-weight: 500;
      cursor: pointer;
      transition: background-color var(--transition-fast), border-color var(--transition-fast), color var(--transition-fast);
      white-space: nowrap;
    }
    button.primary {
      border-color: var(--color-primary);
      background: var(--color-primary);
      color: var(--color-text-inverse);
    }
    button.primary:hover {
      border-color: var(--color-primary-hover);
      background: var(--color-primary-hover);
    }
    button:hover {
      background: var(--color-bg-hover);
      border-color: var(--color-text-tertiary);
    }
    #interrupt {
      border-color: rgba(218, 54, 51, 0.68);
      color: #ffb4af;
    }
    #interrupt:hover {
      background: rgba(218, 54, 51, 0.16);
      border-color: var(--color-danger-hover);
      color: #ffd4d0;
    }
    button:disabled { opacity: .55; cursor: wait; }
    .events-title {
      margin: 0 0 var(--space-sm);
      color: var(--color-text-tertiary);
      font-size: var(--font-size-xs);
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0;
    }
    .event {
      margin: 0 0 var(--space-sm);
      padding: var(--space-sm);
      border: 1px solid var(--color-border-light);
      border-radius: var(--radius-sm);
      background: var(--color-bg-primary);
      color: var(--color-text-secondary);
      font-size: var(--font-size-xs);
      overflow-wrap: anywhere;
    }
    .empty {
      color: var(--color-text-secondary);
      margin: 28px auto;
      max-width: 520px;
      text-align: center;
    }
    @media (max-width: 860px) {
      .app {
        grid-template-columns: 1fr;
        grid-template-rows: auto minmax(0, 1fr) auto auto;
      }
      header {
        grid-column: 1;
        grid-row: 1;
        flex-direction: row;
        align-items: center;
        gap: var(--space-md);
        padding: var(--space-sm) var(--space-md);
        border-right: 0;
        border-bottom: 1px solid var(--color-border);
      }
      .header-left {
        flex: 1;
        flex-direction: row;
        align-items: center;
        min-width: 0;
        padding: 0;
        border-bottom: 0;
      }
      .header-left::after { display: none; }
      .side-panel { display: none; }
      h1 { font-size: var(--font-size-md); }
      h1::before {
        width: 24px;
        height: 24px;
      }
      .status {
        margin-top: 0;
        padding: 0;
        border-top: 0;
        white-space: nowrap;
      }
      main {
        grid-column: 1;
        grid-row: 2;
        grid-template-columns: 1fr;
      }
      .artifacts-sidebar {
        grid-column: 1;
        grid-row: 3;
        max-height: 180px;
        border-left: 0;
        border-top: 1px solid var(--color-border);
      }
      .sidebar { display: none; }
      .composer {
        grid-column: 1;
        grid-row: 4;
        grid-template-columns: 1fr 1fr;
      }
      textarea { grid-column: 1 / -1; }
      .bubble { max-width: 100%; }
      .artifact-panel { width: 100%; }
      .approval-panel { width: 100%; }
      .approval-actions { grid-template-columns: 1fr; }
      .artifact-viewer {
        width: min(96vw, 100%);
        min-width: 0;
      }
      .artifact-viewer-header {
        grid-template-columns: 1fr;
      }
      .artifact-viewer-actions {
        justify-content: stretch;
      }
      .artifact-viewer-actions a,
      .artifact-viewer-actions button {
        flex: 1;
      }
    }
  </style>
</head>
<body>
  <section class="login-screen" id="loginScreen" hidden>
    <div class="login-card">
      <h1>Simple Chat Agent</h1>
      <form class="login-form" id="loginForm">
        <div class="login-field">
          <label for="username">Username</label>
          <input id="username" name="username" autocomplete="username" required value="demo" />
        </div>
        <div class="login-field">
          <label for="password">Password</label>
          <input id="password" name="password" type="password" autocomplete="current-password" required />
        </div>
        <button class="primary" type="submit">Login</button>
        <p class="login-error" id="loginError"></p>
      </form>
    </div>
  </section>
  <div class="app" id="appRoot" hidden>
    <header>
      <div class="header-left">
        <h1>Simple Chat Agent</h1>
        <a class="temporal-link" id="temporalLink" href="#" target="_blank" rel="noreferrer">Temporal UI</a>
      </div>
      <div class="side-panel">
        <section class="side-section">
          <div class="side-actions">
            <button class="primary" type="button" id="newChat">New Chat</button>
            <button type="button" id="toolsButton">Tools</button>
            <button type="button" id="logout">Logout</button>
          </div>
        </section>
        <section class="side-section">
          <p class="side-section-title">Chats</p>
          <div id="conversationList"></div>
        </section>
      </div>
      <div class="status" id="status">connecting...</div>
    </header>
    <main>
      <section class="messages" id="messages">
        <div class="empty">Starting a Temporal workflow...</div>
      </section>
      <aside class="sidebar">
        <p class="events-title">Sideband Stream</p>
        <div id="events"></div>
      </aside>
    </main>
    <aside class="artifacts-sidebar" id="artifactsSidebar"></aside>
    <form class="composer" id="composer">
      <textarea id="message" placeholder="Type to chat. While responding, Send becomes steering."></textarea>
      <button class="primary" type="submit">Send</button>
      <button type="button" id="queue">Queue</button>
      <button type="button" id="afterTool">After Tool</button>
      <button type="button" id="interrupt">Interrupt</button>
    </form>
    <section class="tools-overlay" id="toolsOverlay" hidden>
      <div class="tools-window" role="dialog" aria-modal="true" aria-labelledby="toolsWindowTitle">
        <div class="tools-window-header">
          <div class="tools-window-title" id="toolsWindowTitle">Tools</div>
          <button type="button" id="closeTools">Close</button>
        </div>
        <div class="tools-window-body" id="toolsWindowBody"></div>
      </div>
    </section>
    <section class="artifact-viewer-overlay" id="artifactViewerOverlay" hidden></section>
  </div>
  <script>
    const state = {
      user: null,
      conversations: [],
      tools: [],
      workflowId: null,
      runId: null,
      temporalUiUrl: null,
      workflowState: null,
      eventSource: null,
      streamTurn: null,
      streamPanelCollapsed: false,
      currentClaudeSequence: null,
      ignoreClaudeUntilStart: false,
      localPending: [],
      lastAssistantCount: 0,
      recoveringMissingWorkflow: false,
      toolsWindowOpen: false,
      artifactViewer: {
        open: false,
        artifact: null,
        loading: false,
        error: "",
        text: "",
        objectUrl: "",
      },
      mcpFormOpen: false,
      mcpFormSubmitting: false,
      mcpFormError: "",
      mcpFormValues: {
        label: "",
        server_url: "",
        tool_prefix: "",
        auth_mode: "none",
        bearer_token: "",
      },
    };

    const appRootEl = document.getElementById("appRoot");
    const loginScreenEl = document.getElementById("loginScreen");
    const loginFormEl = document.getElementById("loginForm");
    const loginErrorEl = document.getElementById("loginError");
    const conversationListEl = document.getElementById("conversationList");
    const toolsOverlayEl = document.getElementById("toolsOverlay");
    const toolsWindowBodyEl = document.getElementById("toolsWindowBody");
    const artifactsSidebarEl = document.getElementById("artifactsSidebar");
    const artifactViewerOverlayEl = document.getElementById("artifactViewerOverlay");
    const messagesEl = document.getElementById("messages");
    const eventsEl = document.getElementById("events");
    const statusEl = document.getElementById("status");
    const temporalLinkEl = document.getElementById("temporalLink");
    const inputEl = document.getElementById("message");
    const formEl = document.getElementById("composer");

    boot().catch((err) => {
      statusEl.textContent = `failed: ${err}`;
    });

    async function boot() {
      const authenticated = await refreshUser();
      if (!authenticated) {
        showLogin();
        return;
      }

      showApp();
      await Promise.all([loadTools(), loadConversations()]);

      const savedWorkflowId = localStorage.getItem("simpleChatWorkflowId");
      const savedConversation = state.conversations.find((conversation) => conversation.workflow_id === savedWorkflowId);
      const conversation = savedConversation || state.conversations[0];
      if (conversation) {
        selectConversation(conversation.workflow_id);
      } else {
        await createConversation();
      }
      showOAuthCallbackStatus();
    }

    async function refreshUser() {
      const response = await fetch("/api/me");
      if (response.status === 401) return false;
      if (!response.ok) throw new Error(await response.text());
      state.user = await response.json();
      return true;
    }

    function showLogin() {
      appRootEl.hidden = true;
      loginScreenEl.hidden = false;
      if (state.eventSource) state.eventSource.close();
    }

    function showApp() {
      loginScreenEl.hidden = true;
      appRootEl.hidden = false;
    }

    async function loadConversations() {
      const response = await fetch("/api/conversations");
      if (response.status === 401) {
        showLogin();
        return;
      }
      if (!response.ok) throw new Error(await response.text());
      const body = await response.json();
      state.conversations = body.conversations || [];
      renderSidebar();
    }

    async function loadTools() {
      const response = await fetch("/api/tools");
      if (response.status === 401) {
        showLogin();
        return;
      }
      if (!response.ok) throw new Error(await response.text());
      const body = await response.json();
      state.tools = body.tools || [];
      renderSidebar();
      renderToolsWindow();
    }

    async function createConversation() {
      const response = await fetch("/api/sessions", { method: "POST", headers: jsonHeaders(), body: JSON.stringify({}) });
      if (response.status === 401) {
        showLogin();
        return;
      }
      if (!response.ok) throw new Error(await response.text());
      const body = await response.json();
      await loadConversations();
      selectConversation(body.workflow_id);
    }

    function selectConversation(workflowId) {
      const conversation = state.conversations.find((item) => item.workflow_id === workflowId);
      if (!conversation) return;
      if (state.eventSource) state.eventSource.close();
      state.workflowId = conversation.workflow_id;
      state.runId = conversation.run_id;
      state.temporalUiUrl = temporalUiUrl(conversation);
      state.workflowState = null;
      state.streamTurn = null;
      state.streamPanelCollapsed = false;
      state.currentClaudeSequence = null;
      state.ignoreClaudeUntilStart = false;
      state.localPending = [];
      closeArtifactViewer();
      localStorage.setItem("simpleChatWorkflowId", state.workflowId);
      temporalLinkEl.href = state.temporalUiUrl;
      temporalLinkEl.style.display = "inline-flex";
      renderSidebar();
      render();
      connectEvents();
    }

    function connectEvents() {
      if (!state.workflowId) return;
      state.eventSource = new EventSource(`/api/sessions/${state.workflowId}/events`);
      state.eventSource.addEventListener("state", (event) => {
        const nextState = JSON.parse(event.data);
        updateWorkflowState(nextState);
      });
      state.eventSource.addEventListener("stream", (event) => {
        handleStreamEvent(JSON.parse(event.data));
      });
      state.eventSource.addEventListener("missing", async () => {
        await handleMissingWorkflow();
      });
      state.eventSource.addEventListener("error", () => {
        statusEl.textContent = "event stream reconnecting...";
      });
    }

    loginFormEl.addEventListener("submit", async (event) => {
      event.preventDefault();
      loginErrorEl.textContent = "";
      const form = new FormData(loginFormEl);
      try {
        await post("/api/login", {
          username: String(form.get("username") || ""),
          password: String(form.get("password") || ""),
        });
        await boot();
      } catch (err) {
        loginErrorEl.textContent = String(err);
      }
    });
    document.getElementById("newChat").addEventListener("click", () => createConversation());
    document.getElementById("toolsButton").addEventListener("click", () => {
      state.toolsWindowOpen = true;
      renderToolsWindow();
    });
    document.getElementById("closeTools").addEventListener("click", () => {
      state.toolsWindowOpen = false;
      state.mcpFormOpen = false;
      state.mcpFormError = "";
      renderToolsWindow();
    });
    toolsOverlayEl.addEventListener("click", (event) => {
      if (event.target !== toolsOverlayEl) return;
      state.toolsWindowOpen = false;
      state.mcpFormOpen = false;
      state.mcpFormError = "";
      renderToolsWindow();
    });
    artifactViewerOverlayEl.addEventListener("click", (event) => {
      if (event.target !== artifactViewerOverlayEl) return;
      closeArtifactViewer();
    });
    document.getElementById("logout").addEventListener("click", async () => {
      await post("/api/logout", {});
      localStorage.removeItem("simpleChatWorkflowId");
      state.user = null;
      state.conversations = [];
      state.workflowId = null;
      state.workflowState = null;
      closeArtifactViewer();
      state.toolsWindowOpen = false;
      state.mcpFormOpen = false;
      showLogin();
    });
    formEl.addEventListener("submit", (event) => {
      event.preventDefault();
      sendDefault();
    });
    document.getElementById("queue").addEventListener("click", () => sendAction("chat", "you", "sending"));
    document.getElementById("afterTool").addEventListener("click", () => sendAction("after-tool", "you after tool", "sending"));
    document.getElementById("interrupt").addEventListener("click", () => sendAction("interrupt", "you interrupt", "sending"));
    inputEl.addEventListener("keydown", (event) => {
      if (event.key === "Enter" && !event.shiftKey) {
        event.preventDefault();
        sendDefault();
      }
    });

    function sendDefault() {
      const busy = state.workflowState?.status === "responding";
      sendAction(busy ? "steer" : "chat", busy ? "you steering" : "you", "sending");
    }

    async function sendAction(action, label, phase) {
      let message = inputEl.value.trim();
      if (!message && action === "interrupt") {
        message = "Stop the current response.";
      }
      if (!message) return;
      if (!state.workflowId) {
        if (action === "interrupt" || action === "steer" || action === "after-tool") return;
        await createConversation();
      }
      inputEl.value = "";
      const pending = { id: crypto.randomUUID(), label, content: message, phase };
      state.localPending.push(pending);
      if (action === "interrupt") {
        markStreamInterrupted();
        state.ignoreClaudeUntilStart = true;
      }
      render();

      try {
        if (action === "chat") {
          await post(`/api/sessions/${state.workflowId}/chat`, { message });
        } else if (action === "steer") {
          await post(`/api/sessions/${state.workflowId}/steer`, { message, mode: "immediate" });
        } else if (action === "after-tool") {
          await post(`/api/sessions/${state.workflowId}/steer`, { message, mode: "after_next_tool_result" });
        } else if (action === "interrupt") {
          await post(`/api/sessions/${state.workflowId}/interrupt`, { message });
        }
        await loadConversations();
      } catch (err) {
        pending.phase = `failed: ${err}`;
        render();
      }
    }

    async function post(url, payload) {
      const response = await fetch(url, { method: "POST", headers: jsonHeaders(), body: JSON.stringify(payload) });
      if (response.status === 401) {
        showLogin();
      }
      if (response.status === 404) {
        await handleMissingWorkflow();
      }
      if (!response.ok) throw new Error(await responseErrorText(response));
      const contentType = response.headers.get("content-type") || "";
      if (contentType.includes("application/json")) return await response.json();
      return {};
    }

    async function responseErrorText(response) {
      const text = await response.text();
      try {
        const body = JSON.parse(text);
        if (typeof body.detail === "string") return body.detail;
        if (body.detail) return JSON.stringify(body.detail);
      } catch (_err) {
      }
      return text || `${response.status} ${response.statusText}`;
    }

    async function handleMissingWorkflow() {
      if (state.recoveringMissingWorkflow) return;
      state.recoveringMissingWorkflow = true;
      const missingWorkflowId = state.workflowId;
      try {
        if (state.eventSource) {
          state.eventSource.close();
          state.eventSource = null;
        }
        if (missingWorkflowId) {
          statusEl.textContent = "Workflow no longer exists; selecting a live chat...";
          if (localStorage.getItem("simpleChatWorkflowId") === missingWorkflowId) {
            localStorage.removeItem("simpleChatWorkflowId");
          }
        }

        await loadConversations();
        const nextConversation = state.conversations[0];
        if (nextConversation) {
          selectConversation(nextConversation.workflow_id);
        } else {
          await createConversation();
        }
      } finally {
        state.recoveringMissingWorkflow = false;
      }
    }

    async function deleteConversation(workflowId) {
      if (!confirm("Delete this chat?")) return;
      const response = await fetch(`/api/sessions/${workflowId}`, { method: "DELETE" });
      if (response.status === 401) {
        showLogin();
        return;
      }
      if (!response.ok) throw new Error(await response.text());

      if (state.workflowId === workflowId) {
        if (state.eventSource) {
          state.eventSource.close();
          state.eventSource = null;
        }
        if (localStorage.getItem("simpleChatWorkflowId") === workflowId) {
          localStorage.removeItem("simpleChatWorkflowId");
        }
        state.workflowId = null;
        state.workflowState = null;
        state.streamTurn = null;
        state.localPending = [];
        closeArtifactViewer();
      }

      await loadConversations();
      if (!state.workflowId) {
        const nextConversation = state.conversations[0];
        if (nextConversation) {
          selectConversation(nextConversation.workflow_id);
        } else {
          await createConversation();
        }
      }
    }

    async function resolveApproval(approvalId, decision) {
      await post(`/api/sessions/${state.workflowId}/approvals/${approvalId}`, { decision });
    }

    function renderSidebar() {
      const conversationFragment = document.createDocumentFragment();
      for (const conversation of state.conversations) {
        const row = document.createElement("div");
        row.className = "conversation-row";
        const button = document.createElement("button");
        button.type = "button";
        button.className = `conversation-item${conversation.workflow_id === state.workflowId ? " active" : ""}`;
        button.textContent = conversation.title || "New chat";
        button.addEventListener("click", () => selectConversation(conversation.workflow_id));

        const deleteButton = document.createElement("button");
        deleteButton.type = "button";
        deleteButton.className = "conversation-delete";
        deleteButton.textContent = "Del";
        deleteButton.title = "Delete chat";
        deleteButton.addEventListener("click", (event) => {
          event.stopPropagation();
          deleteConversation(conversation.workflow_id).catch((err) => {
            statusEl.textContent = `delete failed: ${err}`;
          });
        });

        row.append(button, deleteButton);
        conversationFragment.append(row);
      }
      if (state.conversations.length === 0) {
        const empty = document.createElement("div");
        empty.className = "tool-meta";
        empty.textContent = "No chats yet.";
        conversationFragment.append(empty);
      }
      conversationListEl.replaceChildren(conversationFragment);
    }

    function renderApprovalsPanel() {
      const approvals = state.workflowState?.pending_approvals || [];
      if (approvals.length === 0) return null;

      const panel = document.createElement("section");
      panel.className = "approval-panel";

      const header = document.createElement("div");
      header.className = "approval-panel-header";
      const title = document.createElement("span");
      title.textContent = "Approval Required";
      const count = document.createElement("span");
      count.className = "approval-panel-count";
      count.textContent = `${approvals.length} pending`;
      header.append(title, count);
      panel.append(header);

      for (const approval of approvals) {
        panel.append(renderApprovalCard(approval));
      }

      return panel;
    }

    function renderApprovalCard(approval) {
      const card = document.createElement("div");
      card.className = "approval-card";

      const title = document.createElement("div");
      title.className = "approval-title";
      title.textContent = approval.summary || approval.tool_name;
      card.append(title);

      const meta = document.createElement("div");
      meta.className = "approval-meta";
      meta.append(
        approvalMetaRow("Tool", approval.tool_name),
        approvalMetaRow("Scope", approval.memory_key || "one time"),
      );
      card.append(meta);

      const details = document.createElement("div");
      details.className = "approval-details bubble-content";
      renderApprovalArgs(details, approval.tool_args || {});
      card.append(details);

      const actions = document.createElement("div");
      actions.className = "approval-actions";
      actions.append(
        approvalButton("Allow", approval.approval_id, "allow", "allow"),
        approvalButton("Always Allow", approval.approval_id, "always_allow", "always"),
        approvalButton("Deny", approval.approval_id, "deny", "deny"),
      );
      card.append(actions);

      return card;
    }

    function approvalMetaRow(label, value) {
      const row = document.createElement("div");
      const labelNode = document.createElement("strong");
      labelNode.textContent = `${label}: `;
      row.append(labelNode, document.createTextNode(value || "unknown"));
      return row;
    }

    function renderApprovalArgs(container, args) {
      if (typeof args.code === "string") {
        container.append(createCodeBlock(args.code, "python"));
        const rest = { ...args };
        delete rest.code;
        if (Object.keys(rest).length > 0) {
          container.append(createCodeBlock(JSON.stringify(rest, null, 2), "json"));
        }
        return;
      }

      if (typeof args.content === "string" && typeof args.name === "string") {
        const metadata = { ...args };
        delete metadata.content;
        container.append(createCodeBlock(JSON.stringify(metadata, null, 2), "json"));
        const truncated = args.content.length > 12000;
        const preview = truncated
          ? `${args.content.slice(0, 12000)}\n...[truncated for approval preview]`
          : args.content;
        container.append(createCodeBlock(preview, languageFromFileName(args.name)));
        return;
      }

      container.append(createCodeBlock(JSON.stringify(args, null, 2), "json"));
    }

    function renderToolsWindow() {
      toolsOverlayEl.hidden = !state.toolsWindowOpen;
      if (!state.toolsWindowOpen) {
        toolsWindowBodyEl.replaceChildren();
        return;
      }

      const fragment = document.createDocumentFragment();
      const builtInTools = state.tools.filter((tool) => !tool.provider?.startsWith("mcp:"));
      const mcpTools = state.tools.filter((tool) => tool.provider?.startsWith("mcp:"));

      fragment.append(renderBuiltInToolsSection(builtInTools));
      fragment.append(renderMcpToolsSection(mcpTools));
      toolsWindowBodyEl.replaceChildren(fragment);
    }

    function renderBuiltInToolsSection(tools) {
      const section = document.createElement("section");
      section.className = "tools-section";
      section.append(toolsSectionHeader("Built-in tools"));

      const grid = document.createElement("div");
      grid.className = "tools-grid";
      for (const tool of tools) {
        grid.append(renderBuiltInToolCard(tool));
      }
      section.append(grid);
      return section;
    }

    function renderMcpToolsSection(tools) {
      const section = document.createElement("section");
      section.className = "tools-section";

      const actions = document.createElement("div");
      actions.className = "tools-section-actions";
      const addMcpButton = document.createElement("button");
      addMcpButton.type = "button";
      addMcpButton.textContent = "Add HTTP MCP";
      addMcpButton.addEventListener("click", () => {
        state.mcpFormOpen = true;
        state.mcpFormError = "";
        renderToolsWindow();
      });
      actions.append(addMcpButton);
      section.append(toolsSectionHeader("MCP servers", actions));

      if (state.mcpFormOpen) {
        section.append(renderMcpForm());
      }

      const grid = document.createElement("div");
      grid.className = "tools-grid";
      for (const tool of tools) {
        grid.append(renderMcpToolCard(tool));
      }
      if (tools.length === 0) {
        const empty = document.createElement("div");
        empty.className = "tool-meta";
        empty.textContent = "No MCP servers connected.";
        grid.append(empty);
      }
      section.append(grid);
      return section;
    }

    function toolsSectionHeader(titleText, actions = null) {
      const header = document.createElement("div");
      header.className = "tools-section-header";
      const title = document.createElement("div");
      title.className = "tools-section-title";
      title.textContent = titleText;
      header.append(title);
      if (actions) header.append(actions);
      return header;
    }

    function renderBuiltInToolCard(tool) {
      const card = baseToolCard(tool, {
        status: tool.connected ? "Connected" : "Disconnected",
        connected: Boolean(tool.connected),
        disabled: false,
      });

      if (tool.provider === "github") {
        const actions = document.createElement("div");
        actions.className = "tool-actions";
        const action = document.createElement("button");
        action.type = "button";
        action.textContent = tool.connected ? "Disconnect" : "Connect";
        action.disabled = !tool.configured;
        action.addEventListener("click", async () => {
          if (tool.connected) {
            await post("/api/tools/github/disconnect", {});
            statusEl.textContent = "GitHub disconnected";
            await loadTools();
          } else {
            window.location.href = "/oauth/github/start";
          }
        });
        actions.append(action);
        card.append(actions);
      }

      return card;
    }

    function renderMcpToolCard(tool) {
      const connected = Boolean(tool.connected);
      const enabled = Boolean(tool.enabled);
      const card = baseToolCard(tool, {
        status: connected ? (enabled ? "Enabled" : "Disabled") : "Disconnected",
        connected: connected && enabled,
        disabled: !enabled,
      });

      const actions = document.createElement("div");
      actions.className = "tool-actions";
      const toggle = document.createElement("button");
      toggle.type = "button";
      toggle.textContent = enabled ? "Disable" : "Enable";
      toggle.addEventListener("click", async () => {
        await setMcpServerEnabled(tool, !enabled);
      });
      const remove = document.createElement("button");
      remove.type = "button";
      remove.className = "danger";
      remove.textContent = "Delete";
      remove.addEventListener("click", async () => {
        await deleteMcpServer(tool);
      });
      actions.append(toggle, remove);
      card.append(actions);
      return card;
    }

    function baseToolCard(tool, { status, connected, disabled }) {
      const card = document.createElement("div");
      card.className = `tool-card${connected ? " connected" : ""}${disabled ? " disabled" : ""}`;

      const title = document.createElement("div");
      title.className = "tool-title";
      const label = document.createElement("span");
      label.className = "tool-label";
      label.textContent = tool.label;
      const statusNode = document.createElement("span");
      statusNode.className = "tool-status";
      statusNode.textContent = status;
      title.append(label, statusNode);
      card.append(title);

      const meta = document.createElement("div");
      meta.className = "tool-meta";
      if (!tool.configured) {
        meta.textContent = "Set GITHUB_OAUTH_CLIENT_ID and GITHUB_OAUTH_CLIENT_SECRET.";
      } else if (tool.provider?.startsWith("mcp:")) {
        meta.textContent = `${tool.login || "HTTP MCP"} | ${tool.available_tools?.length || 0} tools | ${tool.scopes}`;
      } else if (tool.connected && tool.login) {
        meta.textContent = `@${tool.login} | ${tool.scopes || "no scopes returned"}`;
      } else {
        meta.textContent = `Scopes: ${tool.scopes || "none"}`;
      }
      card.append(meta);

      if (tool.available_tools?.length) {
        const chips = document.createElement("div");
        chips.className = "tool-chip-list";
        for (const toolName of tool.available_tools.slice(0, 8)) {
          const chip = document.createElement("span");
          chip.className = "tool-chip";
          chip.textContent = toolName;
          chips.append(chip);
        }
        if (tool.available_tools.length > 8) {
          const chip = document.createElement("span");
          chip.className = "tool-chip";
          chip.textContent = `+${tool.available_tools.length - 8}`;
          chips.append(chip);
        }
        card.append(chips);
      }

      return card;
    }

    async function setMcpServerEnabled(tool, enabled) {
      const serverId = tool.provider.slice("mcp:".length);
      await post(`/api/mcp-servers/${encodeURIComponent(serverId)}/enabled`, { enabled });
      statusEl.textContent = `${tool.label} ${enabled ? "enabled" : "disabled"}`;
      await loadTools();
    }

    async function deleteMcpServer(tool) {
      if (!confirm(`Delete ${tool.label}?`)) return;
      const serverId = tool.provider.slice("mcp:".length);
      const response = await fetch(`/api/mcp-servers/${encodeURIComponent(serverId)}`, { method: "DELETE" });
      if (response.status === 401) {
        showLogin();
        return;
      }
      if (!response.ok) throw new Error(await responseErrorText(response));
      statusEl.textContent = `${tool.label} deleted`;
      await loadTools();
    }

    function renderMcpForm() {
      const values = state.mcpFormValues;
      const form = document.createElement("form");
      form.className = "mcp-form";
      form.append(
        mcpField("Label", "label", "Temporal docs", "text", values.label),
        mcpField("HTTP URL", "server_url", "https://example.com/mcp", "text", values.server_url),
        mcpField("Tool prefix", "tool_prefix", "temporal", "text", values.tool_prefix),
        mcpAuthField(values.auth_mode),
        mcpField("Bearer token", "bearer_token", "", "password", values.bearer_token),
      );

      const bearerField = form.querySelector('[data-field="bearer_token"]');
      const authMode = form.querySelector('[name="auth_mode"]');
      bearerField.hidden = authMode.value !== "bearer";
      authMode.addEventListener("change", () => {
        bearerField.hidden = authMode.value !== "bearer";
      });

      const labelInput = form.querySelector('[name="label"]');
      const prefixInput = form.querySelector('[name="tool_prefix"]');
      let prefixTouched = false;
      prefixInput.addEventListener("input", () => {
        prefixTouched = true;
      });
      labelInput.addEventListener("input", () => {
        if (!prefixTouched) prefixInput.value = toolPrefixFromLabel(labelInput.value);
      });

      if (state.mcpFormError) {
        const error = document.createElement("div");
        error.className = "mcp-error";
        error.textContent = state.mcpFormError;
        form.append(error);
      }

      const actions = document.createElement("div");
      actions.className = "mcp-form-actions";
      const submit = document.createElement("button");
      submit.type = "submit";
      submit.className = "primary";
      submit.textContent = state.mcpFormSubmitting ? "Adding..." : "Add";
      submit.disabled = state.mcpFormSubmitting;
      const cancel = document.createElement("button");
      cancel.type = "button";
      cancel.textContent = "Cancel";
      cancel.disabled = state.mcpFormSubmitting;
      cancel.addEventListener("click", () => {
        state.mcpFormOpen = false;
        state.mcpFormError = "";
        resetMcpFormValues();
        renderToolsWindow();
      });
      actions.append(submit, cancel);
      form.append(actions);

      form.addEventListener("submit", (event) => {
        event.preventDefault();
        addHttpMcpServer(form).catch((err) => {
          state.mcpFormError = String(err);
          state.mcpFormSubmitting = false;
          renderToolsWindow();
        });
      });

      return form;
    }

    function mcpField(label, name, placeholder, type = "text", value = "") {
      const field = document.createElement("div");
      field.className = "mcp-field";
      field.dataset.field = name;
      const labelNode = document.createElement("label");
      labelNode.textContent = label;
      const input = document.createElement("input");
      input.name = name;
      input.type = type;
      input.placeholder = placeholder;
      input.value = value;
      input.required = name !== "bearer_token";
      field.append(labelNode, input);
      return field;
    }

    function mcpAuthField(value = "none") {
      const field = document.createElement("div");
      field.className = "mcp-field";
      const labelNode = document.createElement("label");
      labelNode.textContent = "Auth";
      const select = document.createElement("select");
      select.name = "auth_mode";
      const none = document.createElement("option");
      none.value = "none";
      none.textContent = "No auth";
      const oauth = document.createElement("option");
      oauth.value = "oauth";
      oauth.textContent = "OAuth discovery";
      const bearer = document.createElement("option");
      bearer.value = "bearer";
      bearer.textContent = "Bearer token";
      select.append(none, oauth, bearer);
      select.value = value;
      field.append(labelNode, select);
      return field;
    }

    async function addHttpMcpServer(form) {
      const formData = new FormData(form);
      const label = String(formData.get("label") || "").trim();
      const serverUrl = String(formData.get("server_url") || "").trim();
      const toolPrefix = String(formData.get("tool_prefix") || "").trim();
      const authMode = String(formData.get("auth_mode") || "none");
      const bearerToken = String(formData.get("bearer_token") || "").trim();
      state.mcpFormValues = {
        label,
        server_url: serverUrl,
        tool_prefix: toolPrefix,
        auth_mode: authMode,
        bearer_token: bearerToken,
      };

      if (authMode === "oauth") {
        window.location.href = mcpOAuthStartUrl({
          label,
          serverUrl,
          toolPrefix,
        });
        return;
      }

      state.mcpFormSubmitting = true;
      state.mcpFormError = "";
      renderToolsWindow();

      const body = await post("/api/mcp-servers", {
        label,
        server_url: serverUrl,
        tool_prefix: toolPrefix,
        auth_mode: authMode,
        bearer_token: authMode === "bearer" ? bearerToken : null,
      });
      state.mcpFormOpen = false;
      state.mcpFormSubmitting = false;
      state.mcpFormError = "";
      resetMcpFormValues();
      statusEl.textContent = `Added MCP server: ${body.server?.label || label}`;
      await loadTools();
    }

    function mcpOAuthStartUrl({ label, serverUrl, toolPrefix, serverId = "" }) {
      const params = new URLSearchParams({
        label,
        server_url: serverUrl,
        tool_prefix: toolPrefix,
      });
      if (serverId) params.set("server_id", serverId);
      return `/api/mcp-servers/oauth/start?${params.toString()}`;
    }

    function resetMcpFormValues() {
      state.mcpFormValues = {
        label: "",
        server_url: "",
        tool_prefix: "",
        auth_mode: "none",
        bearer_token: "",
      };
    }

    function toolPrefixFromLabel(label) {
      return label.toLowerCase().replace(/[^a-z0-9_-]+/g, "_").replace(/^_+|_+$/g, "") || "mcp";
    }

    function approvalButton(label, approvalId, decision, className = "") {
      const button = document.createElement("button");
      button.type = "button";
      button.textContent = label;
      if (className) button.className = className;
      button.addEventListener("click", () => resolveApproval(approvalId, decision));
      return button;
    }

    function temporalUiUrl(conversation) {
      if (conversation.temporal_ui_url) return conversation.temporal_ui_url;
      const workflow = encodeURIComponent(conversation.workflow_id);
      const run = encodeURIComponent(conversation.run_id || "");
      if (run) {
        return `http://localhost:8233/namespaces/default/workflows/${workflow}/${run}/history`;
      }
      return `http://localhost:8233/namespaces/default/workflows/${workflow}`;
    }

    function showOAuthCallbackStatus() {
      const params = new URLSearchParams(window.location.search);
      if (params.has("oauth_error")) {
        statusEl.textContent = `OAuth failed: ${params.get("oauth_error")}`;
      } else if (params.has("github")) {
        statusEl.textContent = "GitHub connected";
      } else if (params.has("mcp")) {
        statusEl.textContent = "MCP server connected";
        loadTools().catch((err) => {
          statusEl.textContent = `tool refresh failed: ${err}`;
        });
      }
      if (params.has("oauth_error") || params.has("github") || params.has("mcp")) {
        history.replaceState({}, "", "/");
      }
    }

    function updateWorkflowState(nextState) {
      const previousAssistantCount = state.workflowState
        ? state.workflowState.transcript.filter((m) => m.role === "assistant").length
        : 0;
      const nextAssistantCount = nextState.transcript.filter((m) => m.role === "assistant").length;
      state.workflowState = nextState;
      state.localPending = state.localPending.filter((pending) => !isAcknowledged(pending, nextState));
      if (nextAssistantCount > previousAssistantCount) markStreamCommitted();
      render();
    }

    function handleStreamEvent(event) {
      const sequence = event.payload?.sequence ?? null;
      if (event.kind === "claude_start") {
        state.currentClaudeSequence = sequence;
        state.ignoreClaudeUntilStart = false;
        if (isOpenStreamTurn(state.streamTurn)) {
          registerStreamSequence(state.streamTurn, sequence);
          state.streamTurn.status = "streaming";
          state.streamTurn.activeSequence = sequence;
        }
      } else if (event.kind === "claude_text_delta" && event.payload?.text) {
        if (state.ignoreClaudeUntilStart) return;
        if (sequence !== state.currentClaudeSequence) return;
        const turn = ensureStreamTurn(sequence);
        turn.status = "streaming";
        turn.text += event.payload.text;
      } else if (event.kind === "claude_cancelled") {
        if (sequence === state.currentClaudeSequence) {
          markStreamInterrupted();
          state.ignoreClaudeUntilStart = true;
        }
      } else if (event.kind === "claude_complete") {
        if (sequence !== state.currentClaudeSequence) return;
        const turn = ensureStreamTurn(sequence);
        if (turn) {
          finishStreamClaudeTurn(turn, event.payload || {});
          turn.status = turn.currentEvents.length ? "tooling" : "waiting";
          turn.lastClaudeCompletedAt = new Date().toISOString();
        }
      } else if (isClaudeToolEvent(event)) {
        const turn = ensureStreamTurn(state.currentClaudeSequence);
        appendStreamToolEvent(turn, event);
        if (turn.status !== "complete" && turn.status !== "interrupted") {
          turn.status = "tooling";
        }
      } else if (!event.kind?.startsWith("claude_")) {
        const turn = ensureStreamTurn(state.currentClaudeSequence);
        appendStreamToolEvent(turn, event);
        if (turn.status !== "complete" && turn.status !== "interrupted") {
          turn.status = "tooling";
        }
      }
      render();
    }

    function ensureStreamTurn(sequence) {
      if (!isOpenStreamTurn(state.streamTurn)) {
        state.streamTurn = createStreamTurn(sequence);
      } else {
        registerStreamSequence(state.streamTurn, sequence);
      }
      return state.streamTurn;
    }

    function streamTurnForSequence(sequence) {
      if (!isOpenStreamTurn(state.streamTurn)) return null;
      if (sequence === null) return state.streamTurn;
      return state.streamTurn.sequences.includes(sequence) ? state.streamTurn : null;
    }

    function isOpenStreamTurn(turn) {
      return Boolean(
        turn &&
        turn.status !== "complete" &&
        turn.status !== "interrupted"
      );
    }

    function registerStreamSequence(turn, sequence) {
      if (sequence !== null && !turn.sequences.includes(sequence)) {
        turn.sequences.push(sequence);
      }
    }

    function createStreamTurn(sequence) {
      return {
        sequence,
        sequences: sequence === null ? [] : [sequence],
        activeSequence: sequence,
        status: "streaming",
        text: "",
        finishedTurns: [],
        currentEvents: [],
        startedAt: new Date().toISOString(),
        completedAt: null,
        lastClaudeCompletedAt: null,
        interrupted: false,
      };
    }

    function finishStreamClaudeTurn(turn, payload) {
      const text = String(payload.text || turn.text || "").trim();
      const stopReason = payload.stop_reason || "unknown";
      const sequence = payload.sequence ?? turn.activeSequence;
      turn.finishedTurns.push({
        sequence,
        text,
        stopReason,
        usage: payload.usage || null,
        events: turn.currentEvents,
        completedAt: new Date().toISOString(),
      });
      turn.finishedTurns = turn.finishedTurns.slice(-12);
      turn.text = "";
      turn.currentEvents = [];
    }

    function appendStreamToolEvent(turn, event) {
      const finishedTurn = latestFinishedToolUseTurn(turn);
      if (finishedTurn) {
        finishedTurn.events = mergeStreamToolEvent(finishedTurn.events || [], event);
        return;
      }

      turn.currentEvents = mergeStreamToolEvent(turn.currentEvents, event);
    }

    function isClaudeToolEvent(event) {
      return (
        event.kind === "claude_tool_start" ||
        event.kind === "claude_tool_complete" ||
        event.kind?.startsWith("claude_tool_input_")
      );
    }

    function mergeStreamToolEvent(events, event) {
      if (!event.kind?.startsWith("claude_tool_input_")) {
        return [...events, event].slice(-5);
      }

      const key = streamToolInputKey(event);
      const nextEvents = [...events];
      const existingIndex = nextEvents.findIndex((candidate) => (
        candidate.kind?.startsWith("claude_tool_input_") &&
        streamToolInputKey(candidate) === key
      ));
      const existing = existingIndex >= 0 ? nextEvents[existingIndex] : null;
      const merged = mergeToolInputEvent(existing, event, key);
      if (existingIndex >= 0) {
        nextEvents[existingIndex] = merged;
      } else {
        nextEvents.push(merged);
      }
      return nextEvents.slice(-5);
    }

    function mergeToolInputEvent(existing, event, key) {
      const existingPayload = existing?.payload || {};
      const payload = event.payload || {};
      const nextPayload = { ...existingPayload, ...payload };
      const existingPartial = String(existingPayload.input_partial || "");

      if (event.kind === "claude_tool_input_delta") {
        nextPayload.input_partial = existingPartial + String(payload.partial_json || "");
        nextPayload.status = "streaming input";
      } else if (event.kind === "claude_tool_input_complete") {
        nextPayload.input_partial = existingPartial;
        nextPayload.status = "input complete";
      } else {
        nextPayload.input_partial = existingPartial;
        nextPayload.status = "building input";
      }

      return {
        ...(existing || event),
        kind: event.kind,
        payload: nextPayload,
        streamToolInputKey: key,
      };
    }

    function streamToolInputKey(event) {
      return (
        event.streamToolInputKey ||
        event.payload?.tool_use_id ||
        `block:${event.payload?.content_block_index ?? "unknown"}`
      );
    }

    function latestFinishedToolUseTurn(turn) {
      const latest = turn.finishedTurns[turn.finishedTurns.length - 1];
      if (!latest || latest.stopReason !== "tool_use") return null;
      return latest;
    }

    function markStreamCommitted() {
      state.streamTurn = null;
      state.currentClaudeSequence = null;
      state.ignoreClaudeUntilStart = false;
    }

    function markStreamInterrupted() {
      state.streamTurn = null;
      state.currentClaudeSequence = null;
    }

    function isAcknowledged(pending, workflowState) {
      return workflowState.transcript.some((message) => {
        if (message.role === "user" && message.content === pending.content) return true;
        if (message.role === "system" && message.content.includes(pending.content)) return true;
        return false;
      });
    }

    function render() {
      const workflowState = state.workflowState;
      statusEl.textContent = workflowState
        ? `${workflowState.status}${workflowState.pending_messages ? `, queued: ${workflowState.pending_messages}` : ""}`
        : "starting...";
      renderSidebar();
      renderArtifactsPanel();
      renderArtifactViewer();

      const fragment = document.createDocumentFragment();
      if (!workflowState && state.localPending.length === 0) {
        const empty = document.createElement("div");
        empty.className = "empty";
        empty.textContent = "Starting a Temporal workflow...";
        fragment.append(empty);
      }

      for (const [index, message] of (workflowState?.transcript || []).entries()) {
        fragment.append(renderMessage(message, index, workflowState));
      }
      for (const pending of state.localPending) {
        fragment.append(bubble("pending", pending.label, `${pending.content} (${pending.phase})`));
      }
      const streamPanel = renderStreamPanel();
      if (streamPanel) {
        fragment.append(streamPanel);
      }
      const approvalsPanel = renderApprovalsPanel();
      if (approvalsPanel) {
        fragment.append(approvalsPanel);
      }

      messagesEl.replaceChildren(fragment);
      messagesEl.scrollTop = messagesEl.scrollHeight;
      eventsEl.replaceChildren();
    }

    function renderMessage(message, index, workflowState) {
      if (message.role === "user") {
        if (workflowState.active_message_index === index) {
          return bubble("pending", "you -> agent", `${message.content} (delivered)`);
        }
        if (workflowState.queued_message_indices.includes(index)) {
          return bubble("pending", "you", `${message.content} (queued)`);
        }
        return bubble("user", "you", message.content);
      }
      if (message.role === "assistant") return bubble("assistant", "assistant", message.content);
      return bubble("system", "system", message.content);
    }

    function renderArtifactsPanel() {
      const artifacts = state.workflowState?.artifacts || [];
      const panel = document.createElement("section");
      panel.className = "artifact-panel";

      const header = document.createElement("div");
      header.className = "artifact-panel-header";
      const title = document.createElement("span");
      title.textContent = "Artifacts";
      const count = document.createElement("span");
      count.className = "artifact-panel-count";
      count.textContent = artifacts.length === 1 ? "1 file" : `${artifacts.length} files`;
      header.append(title, count);
      panel.append(header);

      if (artifacts.length === 0) {
        const empty = document.createElement("div");
        empty.className = "artifact-empty";
        empty.textContent = "Artifacts created by the agent will appear here.";
        panel.append(empty);
        artifactsSidebarEl.replaceChildren(panel);
        return;
      }

      const list = document.createElement("div");
      list.className = "artifact-list";
      for (const artifact of [...artifacts].reverse()) {
        list.append(renderArtifactCard(artifact));
      }
      panel.append(list);
      artifactsSidebarEl.replaceChildren(panel);
    }

    function renderArtifactCard(artifact) {
      const card = document.createElement("article");
      card.className = "artifact-card";

      const name = document.createElement("div");
      name.className = "artifact-name";
      name.textContent = artifact.name || artifact.artifact_id;

      const meta = document.createElement("div");
      meta.className = "artifact-meta";
      meta.textContent = `${artifact.mime_type || "application/octet-stream"} | ${formatBytes(artifact.size_bytes || 0)}`;

      const actions = document.createElement("div");
      actions.className = "artifact-actions";
      actions.append(artifactViewButton(artifact));
      actions.append(artifactLink(artifact.download_url, "Download", true));

      card.append(name, meta, actions);
      return card;
    }

    function artifactViewButton(artifact) {
      const button = document.createElement("button");
      button.type = "button";
      button.textContent = "View";
      button.addEventListener("click", () => {
        openArtifactViewer(artifact).catch((err) => {
          state.artifactViewer.error = String(err);
          state.artifactViewer.loading = false;
          renderArtifactViewer();
        });
      });
      return button;
    }

    function artifactLink(url, label, download) {
      const link = document.createElement("a");
      link.href = url;
      link.textContent = label;
      if (download) {
        link.setAttribute("download", "");
      } else {
        link.target = "_blank";
        link.rel = "noreferrer";
      }
      return link;
    }

    async function openArtifactViewer(artifact) {
      closeArtifactObjectUrl();
      state.artifactViewer = {
        open: true,
        artifact,
        loading: true,
        error: "",
        text: "",
        objectUrl: "",
      };
      renderArtifactViewer();

      if (isImageArtifact(artifact) || isPdfArtifact(artifact)) {
        state.artifactViewer.loading = false;
        renderArtifactViewer();
        return;
      }

      const response = await fetch(artifact.view_url);
      if (!response.ok) throw new Error(await responseErrorText(response));
      state.artifactViewer.text = await response.text();
      state.artifactViewer.loading = false;
      renderArtifactViewer();
    }

    function closeArtifactViewer() {
      closeArtifactObjectUrl();
      state.artifactViewer = {
        open: false,
        artifact: null,
        loading: false,
        error: "",
        text: "",
        objectUrl: "",
      };
      renderArtifactViewer();
    }

    function closeArtifactObjectUrl() {
      if (state.artifactViewer?.objectUrl) {
        URL.revokeObjectURL(state.artifactViewer.objectUrl);
      }
    }

    function renderArtifactViewer() {
      const viewer = state.artifactViewer;
      artifactViewerOverlayEl.hidden = !viewer.open;
      if (!viewer.open || !viewer.artifact) {
        artifactViewerOverlayEl.replaceChildren();
        return;
      }

      const artifact = viewer.artifact;
      const shell = document.createElement("div");
      shell.className = "artifact-viewer";

      const header = document.createElement("div");
      header.className = "artifact-viewer-header";

      const title = document.createElement("div");
      title.className = "artifact-viewer-title";
      const name = document.createElement("div");
      name.className = "artifact-viewer-name";
      name.textContent = artifact.name || artifact.artifact_id;
      const meta = document.createElement("div");
      meta.className = "artifact-viewer-meta";
      meta.textContent = `${artifact.mime_type || "application/octet-stream"} | ${formatBytes(artifact.size_bytes || 0)}`;
      title.append(name, meta);

      const actions = document.createElement("div");
      actions.className = "artifact-viewer-actions";
      actions.append(artifactLink(artifact.download_url, "Download", true));
      const close = document.createElement("button");
      close.type = "button";
      close.textContent = "Close";
      close.addEventListener("click", closeArtifactViewer);
      actions.append(close);

      header.append(title, actions);
      shell.append(header);

      const body = document.createElement("div");
      body.className = "artifact-viewer-body";
      renderArtifactViewerBody(body, viewer);
      shell.append(body);

      artifactViewerOverlayEl.replaceChildren(shell);
    }

    function renderArtifactViewerBody(body, viewer) {
      const artifact = viewer.artifact;
      if (viewer.loading) {
        const loading = document.createElement("div");
        loading.className = "empty";
        loading.textContent = "Loading artifact...";
        body.append(loading);
        return;
      }
      if (viewer.error) {
        const error = document.createElement("div");
        error.className = "artifact-viewer-error";
        error.textContent = viewer.error;
        body.append(error);
        return;
      }
      if (isImageArtifact(artifact)) {
        const image = document.createElement("img");
        image.className = "artifact-viewer-image";
        image.src = artifact.view_url;
        image.alt = artifact.name || "Artifact";
        body.append(image);
        return;
      }
      if (isPdfArtifact(artifact)) {
        const frame = document.createElement("iframe");
        frame.className = "artifact-viewer-frame";
        frame.src = artifact.view_url;
        body.append(frame);
        return;
      }

      const content = document.createElement("div");
      content.className = "bubble-content";
      content.append(createCodeBlock(viewer.text, languageFromFileName(artifact.name)));
      body.append(content);
    }

    function isImageArtifact(artifact) {
      const mimeType = artifact?.mime_type || "";
      return mimeType.startsWith("image/") && mimeType !== "image/svg+xml";
    }

    function isPdfArtifact(artifact) {
      return artifact?.mime_type === "application/pdf";
    }

    function formatBytes(size) {
      if (!Number.isFinite(size)) return "0 B";
      if (size < 1024) return `${size} B`;
      if (size < 1024 * 1024) return `${(size / 1024).toFixed(1)} KB`;
      return `${(size / 1024 / 1024).toFixed(1)} MB`;
    }

    function languageFromFileName(name) {
      const extension = String(name || "").split(".").pop()?.toLowerCase();
      const languages = {
        bash: "bash",
        css: "css",
        html: "html",
        js: "javascript",
        json: "json",
        md: "markdown",
        py: "python",
        sh: "bash",
        sql: "sql",
        ts: "typescript",
        xml: "xml",
        yaml: "yaml",
        yml: "yaml",
      };
      return languages[extension] || null;
    }

    function bubble(kind, label, content) {
      const node = document.createElement("div");
      node.className = `bubble ${kind}`;
      const labelNode = document.createElement("span");
      labelNode.className = "label";
      labelNode.textContent = label;
      const contentNode = document.createElement("div");
      contentNode.className = "bubble-content";
      renderFormattedContent(contentNode, content);
      node.append(labelNode, contentNode);
      return node;
    }

    function renderStreamPanel() {
      const turn = state.streamTurn;
      if (!turn) return null;
      if (!turn.text && turn.currentEvents.length === 0 && turn.finishedTurns.length === 0) return null;

      const collapsed = state.streamPanelCollapsed;
      const node = document.createElement("section");
      node.className = `stream-panel ${turn.status}${collapsed ? " collapsed" : ""}`;

      const header = document.createElement("div");
      header.className = "stream-panel-header";

      const title = document.createElement("div");
      title.className = "stream-panel-title";
      title.textContent = "Streaming visibility";
      const status = document.createElement("span");
      status.className = "stream-panel-status";
      status.textContent = streamPanelStatus(turn);
      title.append(status);

      const toggle = document.createElement("button");
      toggle.type = "button";
      toggle.className = "stream-panel-toggle";
      toggle.textContent = collapsed ? "Expand" : "Collapse";
      toggle.addEventListener("click", () => {
        state.streamPanelCollapsed = !state.streamPanelCollapsed;
        render();
      });

      header.append(title, toggle);
      node.append(header);

      const body = document.createElement("div");
      body.className = "stream-panel-body";

      if (collapsed) {
        const preview = document.createElement("div");
        preview.className = "stream-preview";
        preview.textContent = streamPanelPreview(turn);
        body.append(preview);
        node.append(body);
        return node;
      }

      if (turn.finishedTurns.length) {
        const finishedList = document.createElement("div");
        finishedList.className = "stream-finished-list";
        for (const finishedTurn of turn.finishedTurns) {
          finishedList.append(renderFinishedStreamTurn(finishedTurn));
        }
        body.append(finishedList);
      }

      if (turn.text) {
        const currentTurn = document.createElement("div");
        currentTurn.className = "stream-current-turn";

        const title = document.createElement("div");
        title.className = "stream-finished-title";
        title.textContent = `Claude turn ${turn.activeSequence ?? ""} streaming`;
        currentTurn.append(title);

        const text = document.createElement("div");
        text.className = "stream-text";
        text.textContent = turn.text;
        currentTurn.append(text);

        if (turn.currentEvents.length) {
          currentTurn.append(renderStreamToolList(turn.currentEvents));
        }

        body.append(currentTurn);
      }

      if (!turn.text && turn.currentEvents.length) {
        body.append(renderStreamToolList(turn.currentEvents));
      }

      if (!turn.text && !turn.currentEvents.length && !turn.finishedTurns.length) {
        const preview = document.createElement("div");
        preview.className = "stream-preview";
        preview.textContent = "Waiting for streamed tokens or tool activity...";
        body.append(preview);
      }

      node.append(body);
      return node;
    }

    function renderFinishedStreamTurn(finishedTurn) {
      const node = document.createElement("div");
      node.className = "stream-finished-turn";
      const title = document.createElement("div");
      title.className = "stream-finished-title";
      title.textContent = `Claude turn ${finishedTurn.sequence ?? ""} complete | ${finishedTurn.stopReason}`;
      const text = document.createElement("div");
      text.textContent = finishedTurn.text || `Completed without text (${finishedTurn.stopReason}).`;
      node.append(title, text);
      if (finishedTurn.events?.length) {
        node.append(renderStreamToolList(finishedTurn.events));
      }
      return node;
    }

    function renderStreamToolList(events) {
      const toolList = document.createElement("div");
      toolList.className = "stream-tool-list";
      for (const event of events.slice(-5)) {
        toolList.append(renderStreamToolEvent(event));
      }
      return toolList;
    }

    function renderStreamToolEvent(event) {
      const node = document.createElement("div");
      node.className = "stream-tool-event";
      if (event.kind?.startsWith("claude_tool_input_")) {
        node.classList.add("input-streaming");
      }

      const name = document.createElement("div");
      name.className = "stream-tool-name";
      name.textContent = streamToolLabel(event);
      node.append(name);

      const payload = document.createElement("div");
      payload.className = "stream-tool-payload";
      payload.textContent = streamToolPayloadText(event);
      node.append(payload);

      return node;
    }

    function streamToolPayloadText(event) {
      const payload = event.payload || {};
      if (event.kind?.startsWith("claude_tool_input_")) {
        const status = payload.status || "building input";
        if (event.kind === "claude_tool_input_complete") {
          return `${status}:\n${truncateStreamText(formatStreamValue(payload.input ?? payload.input_partial ?? payload.input_preview))}`;
        }
        const partial = payload.input_partial || payload.partial_json || "";
        return `${status}:\n${truncateStreamText(String(partial))}`;
      }

      return `${event.kind}: ${truncateStreamText(formatStreamValue(payload))}`;
    }

    function streamPanelStatus(turn) {
      const count = turn.currentEvents.length + turn.finishedTurns.reduce(
        (total, finishedTurn) => total + (finishedTurn.events?.length || 0),
        0,
      );
      const toolText = count === 1 ? "1 tool event" : `${count} tool events`;
      const turnCount = turn.finishedTurns.length;
      const turnText = turnCount === 1 ? "1 Claude turn" : `${turnCount} Claude turns`;
      if (turn.status === "interrupted") return `interrupted | ${toolText}`;
      if (turn.status === "complete") return `complete | ${turnText} | ${toolText}`;
      if (turn.status === "tooling") return `tool activity | ${turnText} | ${toolText}`;
      if (turn.status === "waiting") return `finalizing | ${turnText} | ${toolText}`;
      return `streaming | ${turnText} | ${toolText}`;
    }

    function streamPanelPreview(turn) {
      const text = turn.text.trim();
      const latestEvent = turn.currentEvents[turn.currentEvents.length - 1];
      if (text) return text.replace(/\s+/g, " ").slice(-240);
      const latestFinished = turn.finishedTurns[turn.finishedTurns.length - 1];
      if (latestFinished?.text) {
        return latestFinished.text.replace(/\s+/g, " ").slice(-240);
      }
      if (latestEvent) {
        return `${streamToolLabel(latestEvent)} | ${latestEvent.kind}`;
      }
      return streamPanelStatus(turn);
    }

    function streamToolLabel(event) {
      const payloadToolName = event.payload?.tool_name;
      const name = payloadToolName || event.tool_name || "stream";
      return event.step ? `${name}:${event.step}` : name;
    }

    function formatStreamValue(value) {
      if (typeof value === "string") return value;
      try {
        return JSON.stringify(value, null, 2);
      } catch (_err) {
        return String(value);
      }
    }

    function truncateStreamText(value) {
      const text = String(value || "");
      if (text.length <= 4000) return text;
      return text.slice(-4000);
    }

    function renderFormattedContent(container, content) {
      const lines = content.replace(/\r\n/g, "\n").split("\n");
      let paragraphLines = [];
      let listNode = null;
      let listType = null;
      let codeLines = null;
      let codeLanguage = null;

      function flushParagraph() {
        if (paragraphLines.length === 0) return;
        const paragraph = document.createElement("p");
        paragraphLines.forEach((line, index) => {
          if (index > 0) paragraph.append(document.createElement("br"));
          renderInline(paragraph, line);
        });
        container.append(paragraph);
        paragraphLines = [];
      }

      function flushList() {
        if (!listNode) return;
        container.append(listNode);
        listNode = null;
        listType = null;
      }

      function flushCode() {
        if (codeLines === null) return;
        const source = codeLines.join("\n");
        container.append(createCodeBlock(source, codeLanguage));
        codeLines = null;
        codeLanguage = null;
      }

      for (const line of lines) {
        const fence = line.trim().match(/^```(?:\s*([A-Za-z0-9_+.#-]+))?.*$/);
        if (fence) {
          if (codeLines === null) {
            flushParagraph();
            flushList();
            codeLines = [];
            codeLanguage = fence[1] || null;
          } else {
            flushCode();
          }
          continue;
        }

        if (codeLines !== null) {
          codeLines.push(line);
          continue;
        }

        if (line.trim() === "") {
          flushParagraph();
          flushList();
          continue;
        }

        const heading = line.match(/^(#{1,4})\s+(.+)$/);
        if (heading) {
          flushParagraph();
          flushList();
          const headingNode = document.createElement("div");
          headingNode.className = "md-heading";
          renderInline(headingNode, heading[2]);
          container.append(headingNode);
          continue;
        }

        const unordered = line.match(/^\s*[-*]\s+(.+)$/);
        const ordered = line.match(/^\s*\d+[.)]\s+(.+)$/);
        if (unordered || ordered) {
          flushParagraph();
          const nextType = unordered ? "ul" : "ol";
          if (!listNode || listType !== nextType) {
            flushList();
            listNode = document.createElement(nextType);
            listType = nextType;
          }
          const item = document.createElement("li");
          renderInline(item, unordered ? unordered[1] : ordered[1]);
          listNode.append(item);
          continue;
        }

        flushList();
        paragraphLines.push(line);
      }

      flushParagraph();
      flushList();
      flushCode();
    }

    function createCodeBlock(source, languageHint = null) {
      const pre = document.createElement("pre");
      const code = document.createElement("code");
      const language = normalizeCodeLanguage(languageHint) || inferCodeLanguage(source);
      if (language) {
        pre.dataset.language = language;
        code.className = `language-${language}`;
        renderHighlightedCode(code, source, language);
      } else {
        code.textContent = source;
      }
      pre.append(code);
      return pre;
    }

    function renderInline(parent, text) {
      const pattern = /(`[^`]+`|\*\*[^*]+\*\*|\*[^*]+\*)/g;
      let index = 0;
      for (const match of text.matchAll(pattern)) {
        if (match.index > index) {
          parent.append(document.createTextNode(text.slice(index, match.index)));
        }
        const token = match[0];
        if (token.startsWith("`")) {
          const code = document.createElement("code");
          code.textContent = token.slice(1, -1);
          parent.append(code);
        } else if (token.startsWith("**")) {
          const strong = document.createElement("strong");
          strong.textContent = token.slice(2, -2);
          parent.append(strong);
        } else {
          const emphasis = document.createElement("em");
          emphasis.textContent = token.slice(1, -1);
          parent.append(emphasis);
        }
        index = match.index + token.length;
      }
      if (index < text.length) {
        parent.append(document.createTextNode(text.slice(index)));
      }
    }

    function renderHighlightedCode(parent, source, language) {
      const rules = highlightRules(language);
      let index = 0;

      while (index < source.length) {
        const chunk = source.slice(index);
        let matched = false;

        for (const [className, rule] of rules) {
          const match = chunk.match(rule);
          if (!match) continue;

          const text = match[0];
          if (!text) continue;

          if (className === null) {
            parent.append(document.createTextNode(text));
          } else {
            const span = document.createElement("span");
            span.className = className;
            span.textContent = text;
            parent.append(span);
          }
          index += text.length;
          matched = true;
          break;
        }

        if (!matched) {
          parent.append(document.createTextNode(source[index]));
          index += 1;
        }
      }
    }

    function highlightRules(language) {
      const common = [
        [null, /^\s+/],
        ["hl-number", /^\b(?:0x[\da-fA-F]+|\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)\b/],
        ["hl-operator", /^[{}()[\].,;:+\-*/%<>=!&|^~?]+/],
      ];

      if (language === "python") {
        return [
          [null, /^\s+/],
          ["hl-comment", /^#[^\n]*/],
          ["hl-string", /^(?:(?:[rubfRUBF]{0,3})(?:"{3}[\s\S]*?"{3}|'{3}[\s\S]*?'{3}|"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'))/],
          ["hl-keyword", wordRule("and|as|assert|async|await|break|class|continue|def|del|elif|else|except|False|finally|for|from|global|if|import|in|is|lambda|None|nonlocal|not|or|pass|raise|return|True|try|while|with|yield")],
          ["hl-function", wordRule("abs|all|any|bool|dict|enumerate|filter|float|int|len|list|map|max|min|open|print|range|set|str|sum|super|tuple|zip")],
          ...common.slice(1),
        ];
      }

      if (language === "javascript" || language === "typescript") {
        return [
          [null, /^\s+/],
          ["hl-comment", /^\/\/[^\n]*/],
          ["hl-comment", /^\/\*[\s\S]*?\*\//],
          ["hl-string", /^`(?:\\.|[^`\\])*`/],
          ["hl-string", /^"(?:\\.|[^"\\])*"/],
          ["hl-string", /^'(?:\\.|[^'\\])*'/],
          ["hl-keyword", wordRule("async|await|break|case|catch|class|const|continue|debugger|default|delete|do|else|export|extends|false|finally|for|from|function|if|import|in|instanceof|let|new|null|of|return|static|super|switch|this|throw|true|try|typeof|undefined|var|void|while|with|yield")],
          ["hl-type", wordRule("interface|type|implements|private|protected|public|readonly|enum|namespace|abstract|declare")],
          ["hl-function", /^[A-Za-z_$][\w$]*(?=\s*\()/],
          ...common.slice(1),
        ];
      }

      if (language === "json") {
        return [
          [null, /^\s+/],
          ["hl-property", /^"(?:\\.|[^"\\])*"(?=\s*:)/],
          ["hl-string", /^"(?:\\.|[^"\\])*"/],
          ["hl-keyword", wordRule("true|false|null")],
          ...common.slice(1),
        ];
      }

      if (language === "bash") {
        return [
          [null, /^\s+/],
          ["hl-comment", /^#[^\n]*/],
          ["hl-string", /^"(?:\\.|[^"\\])*"/],
          ["hl-string", /^'(?:\\.|[^'\\])*'/],
          ["hl-property", /^\$[A-Za-z_][\w]*/],
          ["hl-keyword", wordRule("alias|case|do|done|elif|else|esac|export|fi|for|function|if|in|local|readonly|return|set|shift|source|then|unalias|unset|while")],
          ["hl-function", /^[A-Za-z_][\w.-]*(?=\s)/],
          ...common.slice(1),
        ];
      }

      if (language === "sql") {
        return [
          [null, /^\s+/],
          ["hl-comment", /^--[^\n]*/],
          ["hl-comment", /^\/\*[\s\S]*?\*\//],
          ["hl-string", /^'(?:''|[^'])*'/],
          ["hl-keyword", wordRule("alter|and|as|avg|by|case|count|create|delete|desc|distinct|drop|else|end|from|group|having|in|inner|insert|into|is|join|left|limit|max|min|not|null|offset|on|or|order|outer|right|select|set|sum|table|then|update|values|view|when|where", "i")],
          ...common.slice(1),
        ];
      }

      if (language === "html" || language === "xml") {
        return [
          [null, /^\s+/],
          ["hl-comment", /^<!--[\s\S]*?-->/],
          ["hl-tag", /^<!doctype[^>]*>/i],
          ["hl-tag", /^<\/?[A-Za-z][\w:-]*/],
          ["hl-attr", /^[A-Za-z_:][\w:.-]*(?=\=)/],
          ["hl-string", /^"(?:\\.|[^"\\])*"/],
          ["hl-string", /^'(?:\\.|[^'\\])*'/],
          ["hl-operator", /^\/?>/],
          ...common.slice(1),
        ];
      }

      if (language === "css") {
        return [
          [null, /^\s+/],
          ["hl-comment", /^\/\*[\s\S]*?\*\//],
          ["hl-string", /^"(?:\\.|[^"\\])*"/],
          ["hl-string", /^'(?:\\.|[^'\\])*'/],
          ["hl-property", /^--?[A-Za-z_][\w-]*(?=\s*:)/],
          ["hl-keyword", /^@[A-Za-z-]+/],
          ["hl-number", /^\b\d+(?:\.\d+)?(?:px|rem|em|vh|vw|%|s|ms)?\b/],
          ["hl-operator", /^[{}()[\].,;:+\-*/%<>=!&|^~?]+/],
        ];
      }

      if (language === "yaml") {
        return [
          [null, /^\s+/],
          ["hl-comment", /^#[^\n]*/],
          ["hl-property", /^[A-Za-z_][\w.-]*(?=\s*:)/],
          ["hl-string", /^"(?:\\.|[^"\\])*"/],
          ["hl-string", /^'(?:\\.|[^'\\])*'/],
          ["hl-keyword", wordRule("true|false|null|yes|no|on|off")],
          ...common.slice(1),
        ];
      }

      return [
        [null, /^\s+/],
        ["hl-comment", /^#[^\n]*/],
        ["hl-comment", /^\/\/[^\n]*/],
        ["hl-comment", /^\/\*[\s\S]*?\*\//],
        ["hl-string", /^`(?:\\.|[^`\\])*`/],
        ["hl-string", /^"(?:\\.|[^"\\])*"/],
        ["hl-string", /^'(?:\\.|[^'\\])*'/],
        ["hl-function", /^[A-Za-z_$][\w$]*(?=\s*\()/],
        ...common.slice(1),
      ];
    }

    function wordRule(words, flags = "") {
      return new RegExp(`^\\b(?:${words})\\b`, flags);
    }

    function normalizeCodeLanguage(language) {
      if (!language) return null;
      const normalized = language.toLowerCase();
      const aliases = {
        bash: "bash",
        cjs: "javascript",
        css: "css",
        html: "html",
        javascript: "javascript",
        js: "javascript",
        json: "json",
        jsonc: "json",
        jsx: "javascript",
        mjs: "javascript",
        py: "python",
        python: "python",
        sh: "bash",
        shell: "bash",
        sql: "sql",
        ts: "typescript",
        tsx: "typescript",
        typescript: "typescript",
        xml: "xml",
        yaml: "yaml",
        yml: "yaml",
        zsh: "bash",
      };
      return aliases[normalized] || null;
    }

    function inferCodeLanguage(source) {
      const trimmed = source.trim();
      if (!trimmed) return null;
      if ((trimmed.startsWith("{") || trimmed.startsWith("[")) && looksLikeJson(trimmed)) return "json";
      if (/^\s*(from\s+\w+\s+import|import\s+\w+|def\s+\w+|class\s+\w+|async\s+def\s+\w+)\b/m.test(source)) return "python";
      if (/\b(print|range|len)\s*\(/.test(source) && /(^|\n)\s*#/.test(source)) return "python";
      if (/\b(const|let|function|console\.log|=>|import\s+.+\s+from)\b/.test(source)) return "javascript";
      if (/^#!.*\b(?:bash|sh|zsh)\b/m.test(source) || /\b(?:echo|curl|export|chmod|sudo)\b/.test(source)) return "bash";
      if (/\bselect\b[\s\S]+\bfrom\b/i.test(source)) return "sql";
      if (/^\s*</.test(source) && /<\/?[A-Za-z][\s\S]*>/.test(source)) return "html";
      if (/^[\s\S]*\{[\s\S]*:[\s\S]*\}/.test(source) && /[.#]?[A-Za-z][\w-]*\s*\{/.test(source)) return "css";
      if (/^[A-Za-z_][\w.-]*\s*:/m.test(source)) return "yaml";
      return null;
    }

    function looksLikeJson(source) {
      try {
        JSON.parse(source);
        return true;
      } catch (_err) {
        return false;
      }
    }

    function jsonHeaders() {
      return { "content-type": "application/json" };
    }
  </script>
</body>
</html>
"""


if __name__ == "__main__":
    uvicorn.run("simple_chat_agent.web:app", host="127.0.0.1", port=8000, reload=True)
