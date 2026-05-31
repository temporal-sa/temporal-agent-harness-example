from __future__ import annotations

import asyncio
import json
import os
import re
import time
from contextlib import asynccontextmanager, suppress
from dataclasses import asdict, is_dataclass, replace
from pathlib import Path
from typing import Any, AsyncIterator, Literal
from urllib.parse import quote, urlparse
from uuid import uuid4

import uvicorn
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import (
    FileResponse,
    RedirectResponse,
    Response,
    StreamingResponse,
)
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from starlette.middleware.gzip import GZipMiddleware
from temporalio.client import Client
from temporalio.common import WorkflowIDConflictPolicy
from temporalio.service import RPCError, RPCStatusCode

from claude_harness.claude_agent import (
    DEFAULT_THINKING_BUDGET_TOKENS,
    MIN_THINKING_BUDGET_TOKENS,
    ClaudeThinkingConfig,
    ClaudeThinkingEffort,
    ClaudeThinkingMode,
)
from claude_harness.mcp import (
    configure_mcp_auth_resolver,
    configure_mcp_http_auth_resolver,
    discover_http_mcp_tools,
    public_mcp_tool_name,
)
from claude_harness.mcp_types import HttpMcpServerConfig
from simple_chat_agent import TASK_QUEUE
from simple_chat_agent.api.auth import (
    DEFAULT_SESSION_SECONDS,
    SESSION_COOKIE,
    AuthenticatedUser,
    AuthError,
    create_session_token,
    user_from_google_subject,
    user_from_session_token,
)
from simple_chat_agent.common.env import load_dotenv
from simple_chat_agent.common.external_storage import (
    purge_workflow_payloads,
    simple_chat_data_converter,
)
from simple_chat_agent.api.github_oauth import (
    GITHUB_PROVIDER,
    GitHubOAuthError,
    exchange_github_code,
    fetch_github_user,
    github_authorize_url,
    github_oauth_configured,
    github_scopes,
)
from simple_chat_agent.api.anthropic_models import (
    EFFORT_ORDER,
    AnthropicModelCatalog,
    default_effort,
    default_thinking_mode,
    get_anthropic_model_catalog,
)
from simple_chat_agent.api.google_oauth import (
    GOOGLE_PROVIDER,
    GoogleOAuthError,
    exchange_google_code,
    google_allowed_domain,
    google_authorize_url,
    google_oauth_configured,
    google_redirect_uri_from_base,
    identity_from_id_token,
)
from simple_chat_agent.common.mcp_auth import (
    mcp_oauth_provider,
    resolve_mcp_auth_headers,
    resolve_mcp_http_auth,
)
from simple_chat_agent.common.mcp_oauth import (
    PendingMcpOAuthFlow,
    authorize_mcp_oauth_flow,
)
from simple_chat_agent.common.store import AppStore, ArtifactRecord
from simple_chat_agent.common.streaming import stream_path
from simple_chat_agent.worker.tools import (
    CREATE_ARTIFACT_TOOL,
    CREATE_SUBAGENT_TOOL,
    FETCH_URL_TOOL,
    GITHUB_TOOL_NAMES,
    PYTHON_SANDBOX_TOOL,
    tool_names_for_connections,
)
from simple_chat_agent.worker.user_chats_workflow import (
    ChatRecord,
    CreateChatRequest,
    DeleteMcpServerRequest,
    TouchChatRequest,
    UpdateMcpServerRequest,
    UserChatsInput,
    UserChatsWorkflow,
    user_chats_workflow_id,
    user_email_search_attributes,
)
from simple_chat_agent.worker.workflow import (
    DEFAULT_MAX_TOKENS,
    SimpleChatSnapshot,
    SimpleChatState,
    SimpleChatWorkflow,
    TranscriptPage,
)

STREAM_ACTIVE_POLL_INTERVAL_SECONDS = 0.02
STREAM_IDLE_POLL_INTERVAL_SECONDS = 0.5
DEFAULT_THINKING_EFFORT: ClaudeThinkingEffort = "max"


class ThinkingSessionRequest(BaseModel):
    enabled: bool = False
    mode: ClaudeThinkingMode | None = None
    budget_tokens: int = DEFAULT_THINKING_BUDGET_TOKENS
    effort: ClaudeThinkingEffort = DEFAULT_THINKING_EFFORT


class CreateSessionRequest(BaseModel):
    system_prompt: str = "You are a concise test chatbot."
    model: str | None = None
    max_tokens: int = DEFAULT_MAX_TOKENS
    max_turns: int = 20
    thinking: ThinkingSessionRequest = Field(default_factory=ThinkingSessionRequest)
    initial_message: str | None = None


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

    client_config = {
        "namespace": os.environ.get("TEMPORAL_NAMESPACE", "default"),
        "data_converter": simple_chat_data_converter(),
        "tls": os.environ.get("TEMPORAL_TLS", "false").lower() in ["true", "1"],
    }
    if os.environ.get("TEMPORAL_API_KEY"):
        client_config["api_key"] = os.environ.get("TEMPORAL_API_KEY")

    app.state.temporal_client = await Client.connect(
        os.environ.get("TEMPORAL_ENDPOINT", "localhost:7233"), **client_config
    )

    app.state.store = AppStore()
    app.state.mcp_oauth_flows = {}
    # In-memory per-stream event buffers, used when streaming arrives over the
    # API-owned HTTP endpoint (deployment) instead of local files (local dev).
    app.state.stream_buffers = {}
    yield


app = FastAPI(lifespan=lifespan)
# Starlette excludes text/event-stream from gzip, so live SSE latency is not
# traded for buffering while large JSON state snapshots still compress.
app.add_middleware(GZipMiddleware, minimum_size=1024, compresslevel=6)

# Local-dev fallback: the dedicated frontend server owns static assets in
# deployment, but FastAPI can still serve them when run directly.
_STATIC_DIR = Path(__file__).resolve().parent.parent / "frontend" / "static"
_FRONTEND_INDEX = _STATIC_DIR / "dist" / "index.html"
app.mount("/static", StaticFiles(directory=_STATIC_DIR), name="static")


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(_FRONTEND_INDEX)


@app.get("/api/me")
async def me(request: Request) -> dict[str, Any]:
    user = _current_user(request)
    return {
        "user_id": user.user_id,
        "username": user.username,
        # Link to all of this user's workflows in the Temporal UI, filtered by
        # the UserEmail search attribute. None when the attribute is disabled.
        "temporal_ui_workflows_url": _temporal_ui_user_workflows_url(user.username),
    }


@app.get("/api/config")
async def config(request: Request) -> dict[str, Any]:
    _current_user(request)
    model_catalog = await asyncio.to_thread(get_anthropic_model_catalog)
    default_model = model_catalog.model_by_id(model_catalog.default_model)
    return {
        "default_model": model_catalog.default_model,
        "model_options": model_catalog.model_ids(),
        "models": [model.to_api_dict() for model in model_catalog.models],
        "model_source": model_catalog.source,
        "model_error": model_catalog.error,
        "thinking": {
            "enabled": False,
            "budget_tokens": DEFAULT_THINKING_BUDGET_TOKENS,
            "min_budget_tokens": MIN_THINKING_BUDGET_TOKENS,
            "mode": default_thinking_mode(default_model),
            "mode_options": list(default_model.thinking_modes if default_model else ()),
            "effort": default_effort(default_model),
            "effort_options": list(default_model.effort_options if default_model else EFFORT_ORDER),
        },
    }


@app.get("/api/auth/google/configured")
async def google_auth_configured() -> dict[str, Any]:
    return {
        "configured": google_oauth_configured(),
        "allowed_domain": google_allowed_domain(),
    }


@app.get("/oauth/google/start")
async def google_oauth_start(request: Request) -> RedirectResponse:
    if not google_oauth_configured():
        raise HTTPException(status_code=400, detail="Google OAuth is not configured")

    state = _store().create_oauth_state(
        user_id="",
        provider=GOOGLE_PROVIDER,
    )
    redirect_uri = google_redirect_uri_from_base(str(request.base_url))
    return RedirectResponse(
        google_authorize_url(state=state, redirect_uri=redirect_uri)
    )


@app.get("/oauth/google/callback")
async def google_oauth_callback(
    request: Request,
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
        return RedirectResponse("/?oauth_error=Missing%20Google%20OAuth%20callback")

    consumed = _store().consume_oauth_state(
        state=state,
        provider=GOOGLE_PROVIDER,
    )
    if consumed is None:
        return RedirectResponse("/?oauth_error=Invalid%20or%20expired%20OAuth%20state")

    redirect_uri = google_redirect_uri_from_base(str(request.base_url))
    try:
        token_payload = await asyncio.to_thread(
            exchange_google_code, code, redirect_uri=redirect_uri
        )
        id_token = token_payload.get("id_token")
        if not isinstance(id_token, str):
            raise GoogleOAuthError("Google did not return an ID token.")
        identity = identity_from_id_token(id_token)
    except GoogleOAuthError as err:
        return RedirectResponse(f"/?oauth_error={quote(str(err), safe='')}")

    user = user_from_google_subject(subject=identity.subject, email=identity.email)
    response = RedirectResponse("/")
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
    conversations = await _list_user_chats(user.user_id, user.username)
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
    registry = await _ensure_user_chats_workflow(user.user_id, user.username)
    mcp_servers = await registry.query(UserChatsWorkflow.list_mcp_servers)
    model_catalog = await asyncio.to_thread(get_anthropic_model_catalog)
    model = session_request.model or _default_model(model_catalog)
    conversation = await registry.execute_update(
        UserChatsWorkflow.create_chat,
        CreateChatRequest(
            system_prompt=session_request.system_prompt,
            model=model,
            max_tokens=session_request.max_tokens,
            max_turns=session_request.max_turns,
            thinking=_thinking_config_from_request(
                session_request.thinking,
                model=model,
                max_tokens=session_request.max_tokens,
            ),
            initial_message=session_request.initial_message,
            available_tool_names=tool_names_for_connections(
                github_connection_id=github_connection_id,
                mcp_servers=mcp_servers,
            ),
            github_connection_id=github_connection_id,
            mcp_servers=mcp_servers,
            good_place_censor=_good_place_enabled(),
        ),
    )
    _clear_stream(conversation.workflow_id)
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
async def get_state(
    request: Request,
    workflow_id: str,
    response: Response,
) -> dict[str, Any]:
    user = await _require_conversation_owner(request, workflow_id)
    response.headers["Cache-Control"] = "no-store"
    response.headers["X-Stream-Cursor"] = _stream_cursor(workflow_id)
    try:
        state = await _query_state(workflow_id)
    except Exception as err:
        if not _is_temporal_not_found(err):
            raise
        await _forget_conversation(user.user_id, workflow_id, user.username)
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


@app.get("/api/sessions/{workflow_id}/snapshot")
async def get_snapshot(
    request: Request,
    workflow_id: str,
    response: Response,
    limit: int = Query(default=60, ge=1, le=200),
) -> dict[str, Any]:
    timings: list[tuple[str, float]] = []
    started = time.perf_counter()
    user = await _require_conversation_owner(request, workflow_id)
    _record_timing(timings, "owner", started)

    response.headers["Cache-Control"] = "no-store"
    response.headers["X-Stream-Cursor"] = _stream_cursor(workflow_id)
    query_started = time.perf_counter()
    try:
        snapshot = await _query_snapshot(workflow_id, limit=limit)
    except Exception as err:
        if not _is_temporal_not_found(err):
            raise
        await _forget_conversation(user.user_id, workflow_id, user.username)
        raise HTTPException(
            status_code=404,
            detail="Workflow execution not found. Start a new chat.",
        ) from err
    _record_timing(timings, "temporal", query_started)

    artifacts_started = time.perf_counter()
    artifacts = _store().list_artifacts(
        user_id=user.user_id,
        workflow_id=workflow_id,
    )
    _record_timing(timings, "artifacts", artifacts_started)

    body = _snapshot_to_dict(snapshot, artifacts=artifacts)
    _set_transcript_headers(response, body)
    response.headers["Server-Timing"] = _server_timing(timings)
    return body


@app.get("/api/sessions/{workflow_id}/messages")
async def get_messages(
    request: Request,
    workflow_id: str,
    response: Response,
    before: int | None = Query(default=None, ge=0),
    limit: int = Query(default=60, ge=1, le=200),
) -> dict[str, Any]:
    timings: list[tuple[str, float]] = []
    started = time.perf_counter()
    user = await _require_conversation_owner(request, workflow_id)
    _record_timing(timings, "owner", started)
    response.headers["Cache-Control"] = "no-store"

    query_started = time.perf_counter()
    try:
        page = await _query_transcript_page(workflow_id, before=before, limit=limit)
    except Exception as err:
        if not _is_temporal_not_found(err):
            raise
        await _forget_conversation(user.user_id, workflow_id, user.username)
        raise HTTPException(
            status_code=404,
            detail="Workflow execution not found. Start a new chat.",
        ) from err
    _record_timing(timings, "temporal", query_started)

    body = _transcript_page_to_dict(page)
    response.headers["Server-Timing"] = _server_timing(timings)
    response.headers["X-Transcript-Start"] = str(body["start"])
    response.headers["X-Transcript-End"] = str(body["end"])
    response.headers["X-Transcript-Total"] = str(body["total"])
    return body


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
        user_email=user.username,
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
    await _touch_conversation(user.user_id, workflow_id, user_email=user.username)
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
    await _touch_conversation(user.user_id, workflow_id, user_email=user.username)
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
    await _touch_conversation(user.user_id, workflow_id, user_email=user.username)
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


@app.post("/internal/stream")
async def internal_stream(request: Request) -> dict[str, str]:
    # Worker -> web: append a stream event to the in-memory per-stream buffer.
    # Authenticated with a shared token (cluster-internal); not user-facing.
    token = os.environ.get("SIMPLE_CHAT_STREAM_TOKEN", "").strip()
    if not token or request.headers.get("x-stream-token") != token:
        raise HTTPException(status_code=401, detail="Invalid stream token.")
    event = await request.json()
    stream_id = event.get("stream_id")
    if stream_id:
        _append_stream_event(stream_id, event)
    return {"status": "ok"}


@app.delete("/api/sessions/{workflow_id}")
async def delete_session(request: Request, workflow_id: str) -> dict[str, str]:
    user = await _require_conversation_owner(request, workflow_id)
    await (
        await _ensure_user_chats_workflow(user.user_id, user.username)
    ).execute_update(
        UserChatsWorkflow.delete_chat,
        workflow_id,
    )
    _store().delete_artifacts_for_conversation(
        user_id=user.user_id,
        workflow_id=workflow_id,
    )
    _clear_stream(workflow_id)
    return {"status": "ok"}


@app.get("/api/tools")
async def tools(request: Request) -> dict[str, Any]:
    user = _current_user(request)
    github_connection = _store().get_oauth_connection(
        user_id=user.user_id,
        provider=GITHUB_PROVIDER,
    )
    mcp_servers = await (
        await _ensure_user_chats_workflow(user.user_id, user.username)
    ).query(UserChatsWorkflow.list_mcp_servers)
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
                    "server_id": server.server_id,
                    "server_url": server.server_url,
                    "tool_prefix": server.tool_prefix,
                    "auth_mode": server.auth_mode,
                    "label": server.label,
                    "configured": True,
                    "connected": _mcp_server_connected(user, server),
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
    servers = await (
        await _ensure_user_chats_workflow(user.user_id, user.username)
    ).query(UserChatsWorkflow.list_mcp_servers)
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
        auth_ref = mcp_oauth_provider(server_id)
        _store().upsert_oauth_connection(
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
    registry = await _ensure_user_chats_workflow(user.user_id, user.username)
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
    registry = await _ensure_user_chats_workflow(user.user_id, user.username)
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
    auth_url = await _start_mcp_oauth_flow(
        user=user,
        label=label,
        server_url=server_url,
        tool_prefix=tool_prefix,
        server_id=server_id,
    )
    return RedirectResponse(auth_url)


async def _start_mcp_oauth_flow(
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
            user,
            server_url=normalized_server_url,
            tool_prefix=normalized_tool_prefix,
        )

    flow = PendingMcpOAuthFlow(
        user_id=user.user_id,
        server_id=_mcp_server_id(
            server_id or (existing_server.server_id if existing_server else None)
        ),
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
    return flow.auth_url


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
        await authorize_mcp_oauth_flow(flow=flow, store=_store())
        connection = _store().get_oauth_connection(
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
        await _upsert_user_mcp_server(
            AuthenticatedUser(user_id=flow.user_id, username=""),
            server,
        )
    except Exception as err:
        flow.fail(err)
        raise
    finally:
        _mcp_oauth_flows().pop(flow.flow_id, None)


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


async def _find_matching_mcp_server(
    user: AuthenticatedUser,
    *,
    server_url: str,
    tool_prefix: str,
) -> HttpMcpServerConfig | None:
    normalized_url = server_url.strip().rstrip("/")
    candidate_urls = set(_mcp_server_url_candidates(normalized_url))
    for server in await (
        await _ensure_user_chats_workflow(user.user_id, user.username)
    ).query(UserChatsWorkflow.list_mcp_servers):
        if server.tool_prefix != tool_prefix:
            continue
        server_urls = set(_mcp_server_url_candidates(server.server_url))
        if (
            normalized_url in server_urls
            or server.server_url.rstrip("/") in candidate_urls
        ):
            return server
    return None


def _mcp_server_connected(
    user: AuthenticatedUser,
    server: HttpMcpServerConfig,
) -> bool:
    if server.auth_mode == "none":
        return True
    if server.auth_ref is None:
        return False

    connection = _store().get_oauth_connection_by_id(server.auth_ref)
    if connection is None:
        connection = _store().get_oauth_connection(
            user_id=user.user_id,
            provider=mcp_oauth_provider(server.server_id),
        )
    return bool(connection and connection.access_token)


def _mcp_discovery_error_message(err: BaseException) -> str:
    if _mcp_error_requires_auth(err):
        return (
            "MCP server requires authentication. Select OAuth authorization if the "
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


def _stream_http_enabled() -> bool:
    # When a shared stream token is configured, streaming arrives over the
    # API-owned HTTP endpoint and is served from the in-memory buffer. Otherwise
    # (local dev) it is tailed from per-stream files on disk.
    return bool(os.environ.get("SIMPLE_CHAT_STREAM_TOKEN", "").strip())


STREAM_BUFFER_TTL_SECONDS = 1800.0


def _stream_buffers() -> dict[str, dict[str, Any]]:
    buffers = getattr(app.state, "stream_buffers", None)
    if buffers is None:
        buffers = {}
        app.state.stream_buffers = buffers
    return buffers


def _ensure_stream_buffer(stream_id: str) -> dict[str, Any]:
    buffers = _stream_buffers()
    entry = buffers.get(stream_id)
    if entry is None:
        entry = {
            "events": [],
            "generation": uuid4().hex[:12],
            "updated": time.monotonic(),
        }
        buffers[stream_id] = entry
    return entry


def _append_stream_event(stream_id: str, event: dict[str, Any]) -> None:
    buffers = _stream_buffers()
    entry = _ensure_stream_buffer(stream_id)
    entry["events"].append(event)
    now = time.monotonic()
    entry["updated"] = now
    # Lazily evict whole streams that have gone idle, to bound memory.
    for stale in [
        sid
        for sid, value in buffers.items()
        if now - value["updated"] > STREAM_BUFFER_TTL_SECONDS
    ]:
        buffers.pop(stale, None)


def _buffer_event_id(entry: dict[str, Any], position: int) -> str:
    return f"{entry['generation']}:{position}"


def _parse_buffer_event_id(
    last_event_id: str | None,
    entry: dict[str, Any],
) -> int | None:
    if not last_event_id:
        return None
    generation, separator, position = last_event_id.partition(":")
    if separator != ":" or generation != entry.get("generation"):
        return None
    with suppress(ValueError):
        return max(0, int(position))
    return None


def _clear_stream(stream_id: str) -> None:
    if _stream_http_enabled():
        _stream_buffers().pop(stream_id, None)
    else:
        stream_path(stream_id).unlink(missing_ok=True)


def _stream_cursor(stream_id: str) -> str:
    if _stream_http_enabled():
        entry = _ensure_stream_buffer(stream_id)
        return _buffer_event_id(entry, len(entry["events"]))

    path = stream_path(stream_id)
    return str(path.stat().st_size if path.exists() else 0)


def _stream_reconcile_event(
    workflow_id: str,
    *,
    reason: str,
    event_id: str,
) -> str:
    return _sse(
        "reconcile",
        {
            "workflow_id": workflow_id,
            "reason": reason,
        },
        event_id=event_id,
    )


async def _event_stream(workflow_id: str, request: Request) -> AsyncIterator[str]:
    source = (
        _buffer_event_stream(workflow_id, request)
        if _stream_http_enabled()
        else _file_event_stream(workflow_id, request)
    )
    async for chunk in source:
        yield chunk


async def _file_event_stream(workflow_id: str, request: Request) -> AsyncIterator[str]:
    path = stream_path(workflow_id)
    # Resume from where this EventSource left off (the browser replays its last
    # received id on auto-reconnect, e.g. after a backgrounded tab). Without
    # this the whole stream file is re-sent on every reconnect, which duplicates
    # already-finalized turns in the UI.
    offset = 0
    needs_reconcile = False
    last_event_id = request.headers.get("last-event-id") or request.query_params.get(
        "cursor"
    )
    if last_event_id:
        try:
            offset = max(0, int(last_event_id))
        except ValueError:
            needs_reconcile = True
            offset = path.stat().st_size if path.exists() else 0
        if not path.exists() or offset > path.stat().st_size:
            needs_reconcile = True
            offset = path.stat().st_size if path.exists() else 0
    else:
        offset = path.stat().st_size if path.exists() else 0
        needs_reconcile = True

    if needs_reconcile:
        yield _stream_reconcile_event(
            workflow_id,
            reason="stream cursor unavailable",
            event_id=str(offset),
        )
        return

    sleep_seconds = STREAM_ACTIVE_POLL_INTERVAL_SECONDS
    while not await request.is_disconnected():
        emitted = False
        if path.exists():
            if offset > path.stat().st_size:
                offset = path.stat().st_size
                yield _stream_reconcile_event(
                    workflow_id,
                    reason="stream cursor reset",
                    event_id=str(offset),
                )
                break

            new_lines: list[tuple[str, int]] = []
            with path.open("r", encoding="utf-8") as stream:
                stream.seek(offset)
                while True:
                    line = stream.readline()
                    if not line:
                        break
                    new_lines.append((line, stream.tell()))
                offset = stream.tell()

            for line, position in new_lines:
                with suppress(json.JSONDecodeError):
                    emitted = True
                    yield _sse("stream", json.loads(line), event_id=str(position))

        sleep_seconds = (
            STREAM_ACTIVE_POLL_INTERVAL_SECONDS
            if emitted
            else STREAM_IDLE_POLL_INTERVAL_SECONDS
        )
        await asyncio.sleep(sleep_seconds)


async def _buffer_event_stream(
    workflow_id: str, request: Request
) -> AsyncIterator[str]:
    # Resume by generation-scoped buffer index (the browser replays its last
    # received id). If the generation changed, this web process no longer has
    # the exact missed events and asks the browser to fetch a JSON snapshot.
    entry = _ensure_stream_buffer(workflow_id)
    events = entry["events"]
    last_event_id = request.headers.get("last-event-id") or request.query_params.get(
        "cursor"
    )
    needs_reconcile = False
    if last_event_id:
        parsed_resume = _parse_buffer_event_id(last_event_id, entry)
        if parsed_resume is None or parsed_resume > len(events):
            needs_reconcile = True
            resume = len(events)
        else:
            resume = parsed_resume
    else:
        resume = len(events)
        needs_reconcile = True

    if needs_reconcile:
        yield _stream_reconcile_event(
            workflow_id,
            reason="stream cursor unavailable",
            event_id=_buffer_event_id(entry, resume),
        )
        return
    cursor_generation = entry["generation"]

    sleep_seconds = STREAM_ACTIVE_POLL_INTERVAL_SECONDS
    while not await request.is_disconnected():
        emitted = False
        entry = _ensure_stream_buffer(workflow_id)
        events = entry["events"]
        if entry["generation"] != cursor_generation or resume > len(events):
            resume = len(events)
            cursor_generation = entry["generation"]
            yield _stream_reconcile_event(
                workflow_id,
                reason="stream buffer reset",
                event_id=_buffer_event_id(entry, resume),
            )
            break

        for index in range(resume, len(events)):
            emitted = True
            yield _sse(
                "stream",
                events[index],
                event_id=_buffer_event_id(entry, index + 1),
            )
        resume = len(events)

        sleep_seconds = (
            STREAM_ACTIVE_POLL_INTERVAL_SECONDS
            if emitted
            else STREAM_IDLE_POLL_INTERVAL_SECONDS
        )
        await asyncio.sleep(sleep_seconds)


async def _query_state(workflow_id: str) -> SimpleChatState:
    return await _handle(workflow_id).query(SimpleChatWorkflow.state)


async def _query_snapshot(workflow_id: str, *, limit: int) -> SimpleChatSnapshot:
    return await _handle(workflow_id).query(SimpleChatWorkflow.snapshot, limit)


async def _query_transcript_page(
    workflow_id: str,
    *,
    before: int | None,
    limit: int,
) -> TranscriptPage:
    return await _handle(workflow_id).query(
        SimpleChatWorkflow.transcript_page,
        before,
        limit,
    )


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
        await _forget_conversation(user.user_id, workflow_id, user.username)
        raise HTTPException(
            status_code=404,
            detail="Workflow execution not found. Start a new chat.",
        ) from err


async def _list_user_chats(user_id: str, user_email: str = "") -> list[ChatRecord]:
    handle = await _ensure_user_chats_workflow(user_id, user_email)
    return await handle.query(UserChatsWorkflow.list_chats)


async def _touch_conversation(
    user_id: str,
    workflow_id: str,
    *,
    title: str | None = None,
    user_email: str = "",
) -> None:
    await (await _ensure_user_chats_workflow(user_id, user_email)).execute_update(
        UserChatsWorkflow.touch_chat,
        TouchChatRequest(workflow_id=workflow_id, title=title),
    )


async def _forget_conversation(
    user_id: str, workflow_id: str, user_email: str = ""
) -> None:
    await (await _ensure_user_chats_workflow(user_id, user_email)).execute_update(
        UserChatsWorkflow.forget_chat,
        workflow_id,
    )
    _store().delete_artifacts_for_conversation(
        user_id=user_id,
        workflow_id=workflow_id,
    )
    _clear_stream(workflow_id)
    # Purge the chat's offloaded payloads from external storage. Best-effort:
    # a purge failure must not block forgetting the conversation. No-op when
    # S3 storage is not configured (local dev).
    try:
        await asyncio.to_thread(
            purge_workflow_payloads,
            namespace=_client().namespace,
            workflow_id=workflow_id,
        )
    except Exception as err:  # noqa: BLE001 - cleanup is best-effort
        print(f"Failed to purge external payloads for {workflow_id}: {err!r}")


def _is_temporal_not_found(err: BaseException) -> bool:
    return isinstance(err, RPCError) and err.status == RPCStatusCode.NOT_FOUND


def _handle(workflow_id: str) -> Any:
    return _client().get_workflow_handle(workflow_id)


def _user_email_sa_name() -> str:
    return os.environ.get("SIMPLE_CHAT_USER_EMAIL_SEARCH_ATTR", "").strip()


async def _ensure_user_chats_workflow(user_id: str, user_email: str = "") -> Any:
    workflow_id = user_chats_workflow_id(user_id)
    search_attr_name = _user_email_sa_name()
    return await _client().start_workflow(
        UserChatsWorkflow.run,
        UserChatsInput(
            user_id=user_id,
            user_email=user_email,
            search_attr_name=search_attr_name,
        ),
        id=workflow_id,
        task_queue=TASK_QUEUE,
        id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
        static_summary="simple chat user registry",
        search_attributes=user_email_search_attributes(
            search_attr_name=search_attr_name,
            user_email=user_email,
        ),
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
    if not await (await _ensure_user_chats_workflow(user.user_id, user.username)).query(
        UserChatsWorkflow.has_chat,
        workflow_id,
    ):
        raise HTTPException(status_code=404, detail="Conversation not found")
    return user


async def _update_user_workflows_tool_connections(
    user: AuthenticatedUser,
) -> None:
    github_connection_id = _github_connection_id_for_user(user)
    mcp_servers = await (
        await _ensure_user_chats_workflow(user.user_id, user.username)
    ).query(UserChatsWorkflow.list_mcp_servers)
    available_tool_names = tool_names_for_connections(
        github_connection_id=github_connection_id,
        mcp_servers=mcp_servers,
    )

    for conversation in await _list_user_chats(user.user_id, user.username):
        with suppress(Exception):
            await _handle(conversation.workflow_id).signal(
                SimpleChatWorkflow.update_tool_connections,
                args=[available_tool_names, github_connection_id, mcp_servers],
            )


async def _upsert_user_mcp_server(
    user: AuthenticatedUser,
    server: HttpMcpServerConfig,
) -> None:
    registry = await _ensure_user_chats_workflow(user.user_id, user.username)
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
    mcp_servers = await (
        await _ensure_user_chats_workflow(user.user_id, user.username)
    ).query(UserChatsWorkflow.list_mcp_servers)
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


def _snapshot_to_dict(
    snapshot: SimpleChatSnapshot,
    *,
    artifacts: list[ArtifactRecord],
) -> dict[str, Any]:
    state = _state_to_dict(snapshot.state, artifacts=artifacts)
    page = _transcript_page_to_dict(snapshot.transcript_page)
    state["transcript"] = page["messages"]
    state["transcript_offset"] = page["start"]
    state["transcript_total"] = page["total"]
    state["transcript_has_more_before"] = page["has_more_before"]
    state["transcript_revision"] = max(
        int(state.get("transcript_revision") or 0),
        int(page.get("revision") or 0),
    )
    state["transcript_length"] = page["total"]
    return state


def _transcript_page_to_dict(page: TranscriptPage) -> dict[str, Any]:
    return {
        "messages": [
            asdict(message) if is_dataclass(message) else dict(message)
            for message in page.messages
        ],
        "start": page.start,
        "end": page.end,
        "total": page.total,
        "revision": page.transcript_revision,
        "has_more_before": page.start > 0,
    }


def _set_transcript_headers(response: Response, state: dict[str, Any]) -> None:
    response.headers["X-Transcript-Start"] = str(state.get("transcript_offset", 0))
    response.headers["X-Transcript-End"] = str(
        int(state.get("transcript_offset", 0)) + len(state.get("transcript", []))
    )
    response.headers["X-Transcript-Total"] = str(
        state.get("transcript_total", len(state.get("transcript", [])))
    )


def _record_timing(
    timings: list[tuple[str, float]],
    name: str,
    started: float,
) -> None:
    timings.append((name, (time.perf_counter() - started) * 1000))


def _server_timing(timings: list[tuple[str, float]]) -> str:
    return ", ".join(f"{name};dur={duration:.1f}" for name, duration in timings)


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
    try:
        content = _store().read_artifact_bytes(artifact)
    except Exception as err:
        raise HTTPException(status_code=404, detail="Artifact file not found") from err

    return Response(
        content,
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
    if mime_type.startswith("audio/") or mime_type.startswith("video/"):
        return mime_type
    return "text/plain; charset=utf-8"


def _sse(event: str, data: Any, *, event_id: str | None = None) -> str:
    prefix = f"id: {event_id}\n" if event_id is not None else ""
    return f"{prefix}event: {event}\ndata: {json.dumps(data, default=str)}\n\n"


def _temporal_ui_base_url() -> str:
    # Explicit override always wins.
    explicit = os.environ.get("TEMPORAL_UI_URL")
    if explicit:
        return explicit
    # Otherwise derive from the environment: a Temporal Cloud endpoint maps to
    # the Cloud Web UI; anything else falls back to the local dev server.
    endpoint = os.environ.get("TEMPORAL_ENDPOINT", "")
    if "tmprl.cloud" in endpoint:
        return "https://cloud.temporal.io"
    return "http://localhost:8233"


def _temporal_ui_user_workflows_url(email: str) -> str | None:
    """Temporal UI workflow-list URL filtered by the UserEmail search attribute.

    Returns None when the search attribute is not configured.
    """
    search_attr_name = _user_email_sa_name()
    if not search_attr_name or not email:
        return None
    try:
        namespace = _client().namespace
    except HTTPException:
        return None
    base_url = _temporal_ui_base_url().rstrip("/")
    namespace_path = quote(namespace, safe="")
    # UserEmail is a Text (tokenized) search attribute, so including the email
    # domain would match the "temporal"/"io" tokens shared by every user and
    # return everyone. Filter on the local part only for a per-user result.
    local_part = email.split("@", 1)[0]
    query = quote(f'{search_attr_name} = "{local_part}"', safe="")
    return f"{base_url}/namespaces/{namespace_path}/workflows?query={query}"


def _temporal_ui_url(*, namespace: str, workflow_id: str, run_id: str) -> str:
    base_url = _temporal_ui_base_url().rstrip("/")
    namespace_path = quote(namespace, safe="")
    workflow_path = quote(workflow_id, safe="")
    run_path = quote(run_id, safe="")
    if run_path:
        return (
            f"{base_url}/namespaces/{namespace_path}/workflows/"
            f"{workflow_path}/{run_path}/timeline"
        )
    return f"{base_url}/namespaces/{namespace_path}/workflows/{workflow_path}"


def _conversation_title(message: str) -> str:
    normalized = " ".join(message.split())
    if not normalized:
        return "New chat"
    if len(normalized) <= 64:
        return normalized
    return f"{normalized[:61]}..."


def _default_model(model_catalog: AnthropicModelCatalog | None = None) -> str:
    catalog = model_catalog or get_anthropic_model_catalog()
    return catalog.default_model


def _good_place_enabled() -> bool:
    return os.environ.get("SIMPLE_CHAT_GOOD_PLACE", "1").lower() in ("1", "true", "yes")


def _thinking_config_from_request(
    request: ThinkingSessionRequest,
    *,
    model: str,
    max_tokens: int,
) -> ClaudeThinkingConfig | None:
    if not request.enabled:
        return None
    mode = request.mode or _default_thinking_mode_for_model(model)
    if mode == "adaptive":
        return ClaudeThinkingConfig(
            enabled=True,
            mode="adaptive",
            effort=request.effort,
        )
    if max_tokens <= MIN_THINKING_BUDGET_TOKENS:
        raise HTTPException(
            status_code=400,
            detail="max_tokens must be greater than 1024 for extended thinking.",
        )
    budget_tokens = min(
        max(request.budget_tokens, MIN_THINKING_BUDGET_TOKENS),
        max_tokens - 1,
    )
    return ClaudeThinkingConfig(
        enabled=True,
        mode="enabled",
        budget_tokens=budget_tokens,
    )


def _default_thinking_mode_for_model(model_id: str) -> ClaudeThinkingMode:
    model = get_anthropic_model_catalog().model_by_id(model_id)
    mode = default_thinking_mode(model)
    return "adaptive" if mode == "adaptive" else "enabled"

def main() -> None:
    uvicorn.run(
        "simple_chat_agent.api.main:app",
        host="127.0.0.1",
        port=8000,
        reload=True,
        proxy_headers=True,
        forwarded_allow_ips=os.environ.get("FORWARDED_ALLOW_IPS", "127.0.0.1"),
    )


if __name__ == "__main__":
    main()
