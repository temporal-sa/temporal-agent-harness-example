from __future__ import annotations

import asyncio
import json
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import (
    FileResponse,
    Response,
)
from fastapi.staticfiles import StaticFiles
from starlette.middleware.gzip import GZipMiddleware
from temporalio.client import Client
from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.service import RPCError, RPCStatusCode

from agent_harness.mcp import (
    configure_mcp_auth_resolver,
    configure_mcp_http_auth_resolver,
)
from agent_harness.mcp_types import HttpMcpServerConfig
from agent_harness.providers.claude import (
    DEFAULT_THINKING_BUDGET_TOKENS,
    MIN_THINKING_BUDGET_TOKENS,
)
from simple_chat_agent import TASK_QUEUE
from simple_chat_agent.api.agent_routes import AgentApi
from simple_chat_agent.api.anthropic_models import (
    EFFORT_ORDER,
    default_effort,
    default_thinking_mode,
    get_anthropic_model_catalog,
)
from simple_chat_agent.api.auth import (
    DEFAULT_SESSION_SECONDS,
    SESSION_COOKIE,
    AuthenticatedUser,
    AuthError,
    create_session_token,
    catalog_auth_enabled,
    user_from_catalog_headers,
    user_from_local_username,
    user_from_session_token,
)
from simple_chat_agent.api.features import (
    github_tools_enabled,
)
from simple_chat_agent.api.github_oauth import (
    GITHUB_PROVIDER,
)

from simple_chat_agent.api.local_auth import (
    local_auth_credentials_valid,
    local_auth_requested,
)

from simple_chat_agent.api.routes.sessions import (
    SessionRouteDeps,
    create_sessions_router,
)
from simple_chat_agent.api.routes.tools import (
    ToolRouteDeps,
    create_tools_router,
)
from simple_chat_agent.api.streaming import StreamBroker
from simple_chat_agent.api.temporal_ui import temporal_ui_user_workflows_url
from simple_chat_agent.common.env import load_dotenv
from simple_chat_agent.common.external_storage import (
    purge_workflow_payloads,
    simple_chat_data_converter,
)
from simple_chat_agent.common.mcp_auth import (
    resolve_mcp_auth_headers,
    resolve_mcp_http_auth,
)
from simple_chat_agent.common.mcp_oauth import PendingMcpOAuthFlow
from simple_chat_agent.common.store import AppStore

from simple_chat_agent.worker.user_chats_workflow import (
    ChatRecord,
    TouchChatRequest,
    UpdateMcpServerRequest,
    UserChatsInput,
    UserChatsWorkflow,
    user_chats_workflow_id,
    user_email_search_attributes,
)
from simple_chat_agent.worker.workflow import (
    SimpleChatSnapshot,
    SimpleChatState,
    SimpleChatWorkflow,
    TranscriptDeltaResult,
    TranscriptPage,
)

load_dotenv()


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
        os.environ.get("TEMPORAL_ADDRESS", os.environ.get("TEMPORAL_ENDPOINT", "localhost:7233")), **client_config
    )

    app.state.store = AppStore()
    app.state.mcp_oauth_flows = {}
    stream_broker = StreamBroker()
    await stream_broker.start()
    app.state.stream_broker = stream_broker
    try:
        yield
    finally:
        await stream_broker.close()
        await _agent_api.aclose()


app = FastAPI(lifespan=lifespan)
# Starlette excludes text/event-stream from gzip, so live SSE latency is not
# traded for buffering while large JSON state snapshots still compress.
app.add_middleware(GZipMiddleware, minimum_size=1024, compresslevel=6)

# Serve the built UI from the API in catalog deployments and local development.
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
        "temporal_ui_workflows_url": temporal_ui_user_workflows_url(
            user.username,
            namespace=_client().namespace,
            search_attr_name=_user_email_sa_name(),
        ),
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
            "effort_options": list(
                default_model.effort_options if default_model else EFFORT_ORDER
            ),
        },
        "features": {
            "github_tools": github_tools_enabled(),
        },
    }


def _local_auth_route_enabled() -> bool:
    return local_auth_requested() and not catalog_auth_enabled()


@app.get("/api/auth/config")
async def auth_config() -> dict[str, Any]:
    return {
        "mode": "catalog" if catalog_auth_enabled() else "local",
        "local_enabled": _local_auth_route_enabled(),
    }


@app.get("/healthz")
async def health() -> dict[str, str]:
    _client()
    return {"status": "ok"}


if _local_auth_route_enabled():

    @app.post("/api/auth/local/login")
    async def local_auth_login(request: Request) -> Response:
        if not _local_auth_route_enabled():
            raise HTTPException(status_code=404, detail="Not found")
        try:
            body = await request.json()
        except json.JSONDecodeError:
            body = {}
        if not isinstance(body, dict):
            body = {}
        username = body.get("username")
        password = body.get("password")
        if not isinstance(username, str) or not isinstance(password, str):
            raise HTTPException(status_code=401, detail="Invalid local credentials")
        if not local_auth_credentials_valid(username, password):
            raise HTTPException(status_code=401, detail="Invalid local credentials")

        user = user_from_local_username(username)
        response = Response(
            content=json.dumps(
                {"status": "ok", "user_id": user.user_id, "username": user.username}
            ),
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


@app.post("/internal/stream")
async def internal_stream(request: Request) -> dict[str, str]:
    # External executor/old worker -> API -> Redis. Authenticated with a shared
    # token and retained for sandbox callbacks plus rolling compatibility.
    token = os.environ.get("SIMPLE_CHAT_STREAM_TOKEN", "").strip()
    if not token or request.headers.get("x-stream-token") != token:
        raise HTTPException(status_code=401, detail="Invalid stream token.")
    event = await request.json()
    stream_id = event.get("stream_id")
    if stream_id:
        await _stream_broker().append(stream_id, event)
    return {"status": "ok"}


@app.post("/internal/stream/event")
async def internal_stream_event(request: Request) -> dict[str, str]:
    # Worker -> web: append a named stream event that may be retried by a
    # workflow activity. The idempotency key lets the broker preserve exactly one
    # terminal projection event per completed turn while keeping live chunks
    # best-effort.
    token = os.environ.get("SIMPLE_CHAT_STREAM_TOKEN", "").strip()
    if not token or request.headers.get("x-stream-token") != token:
        raise HTTPException(status_code=401, detail="Invalid stream token.")
    payload = await request.json()
    stream_id = str(payload.get("stream_id") or "")
    event = str(payload.get("event") or "")
    data = payload.get("data")
    idempotency_key = str(payload.get("idempotency_key") or "")
    if not stream_id or not event or not isinstance(data, dict):
        raise HTTPException(status_code=400, detail="Invalid stream event payload.")
    cursor = await _stream_broker().append_event(
        stream_id,
        event,
        data,
        idempotency_key=idempotency_key or None,
    )
    return {"status": "ok", "cursor": cursor}


async def _query_state(workflow_id: str) -> SimpleChatState:
    return await _handle(workflow_id).query(SimpleChatWorkflow.state)


async def _query_snapshot(
    workflow_id: str,
    *,
    limit: int,
    max_bytes: int,
) -> SimpleChatSnapshot:
    return await _handle(workflow_id).query(
        SimpleChatWorkflow.snapshot,
        args=[limit, max_bytes],
    )


async def _query_transcript_page(
    workflow_id: str,
    *,
    before: int | None,
    limit: int,
    max_bytes: int,
) -> TranscriptPage:
    return await _handle(workflow_id).query(
        SimpleChatWorkflow.transcript_page,
        args=[before, limit, max_bytes],
    )


async def _query_transcript_deltas_since(
    workflow_id: str,
    *,
    after_revision: int,
    max_bytes: int,
) -> TranscriptDeltaResult:
    return await _handle(workflow_id).query(
        SimpleChatWorkflow.transcript_deltas_since,
        args=[after_revision, max_bytes],
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
    # This bookkeeping must not make delivery of a chat signal depend on a
    # worker. The registry workflow ID is deterministic, and a Temporal Signal
    # is accepted durably by the service even while no worker is polling.
    await _handle(user_chats_workflow_id(user_id)).signal(
        UserChatsWorkflow.touch_chat_signal,
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
    await _stream_broker().clear(workflow_id)
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
    # Start for its side effect only, then return a fresh handle. The handle from
    # start_workflow(USE_EXISTING) pins first_execution_run_id to the run it attached
    # to; after the registry entity continues-as-new, execute_update forwards that
    # stale run id as a chain-identity guard and the server returns NOT_FOUND. A
    # get_workflow_handle by ID carries no first_execution_run_id, so updates keep
    # working across continue-as-new.
    await _client().start_workflow(
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
    return _handle(workflow_id)


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


def _stream_broker() -> StreamBroker:
    broker = getattr(app.state, "stream_broker", None)
    if broker is None:
        broker = StreamBroker()
        app.state.stream_broker = broker
    return broker


def _current_user(request: Request) -> AuthenticatedUser:
    if catalog_auth_enabled():
        try:
            return user_from_catalog_headers(request.headers, task_queue=TASK_QUEUE)
        except AuthError as err:
            raise HTTPException(status_code=401, detail=str(err)) from err
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

    # Workflow Queries execute on a worker, so they cannot authorize a Signal
    # that must remain deliverable during a worker outage. Describe is served by
    # Temporal itself. Chats are children of the authenticated user's
    # deterministic registry workflow, which gives us a worker-independent
    # ownership check without introducing another state store.
    try:
        description = await _handle(workflow_id).describe()
    except Exception as err:
        if not _is_temporal_not_found(err):
            raise
        raise HTTPException(status_code=404, detail="Conversation not found") from err

    if (
        description.workflow_type != SimpleChatWorkflow.__name__
        or description.parent_id != user_chats_workflow_id(user.user_id)
    ):
        raise HTTPException(status_code=404, detail="Conversation not found")
    return user


async def _upsert_user_mcp_server(
    user: AuthenticatedUser,
    server: HttpMcpServerConfig,
) -> None:
    registry = await _ensure_user_chats_workflow(user.user_id, user.username)
    await registry.execute_update(
        UserChatsWorkflow.upsert_mcp_server,
        UpdateMcpServerRequest(server=server),
    )


def _github_connection_id_for_user(user: AuthenticatedUser) -> str | None:
    if not github_tools_enabled():
        return None
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


app.include_router(
    create_sessions_router(
        SessionRouteDeps(
            client=_client,
            store=_store,
            stream_broker=_stream_broker,
            current_user=_current_user,
            require_conversation_owner=_require_conversation_owner,
            ensure_user_chats_workflow=_ensure_user_chats_workflow,
            list_user_chats=_list_user_chats,
            query_state=_query_state,
            query_snapshot=_query_snapshot,
            query_transcript_page=_query_transcript_page,
            query_transcript_deltas_since=_query_transcript_deltas_since,
            signal_workflow=_signal_workflow,
            touch_conversation=_touch_conversation,
            forget_conversation=_forget_conversation,
            is_temporal_not_found=_is_temporal_not_found,
            github_connection_id_for_user=_github_connection_id_for_user,
        )
    )
)
app.include_router(
    create_tools_router(
        ToolRouteDeps(
            store=_store,
            current_user=_current_user,
            ensure_user_chats_workflow=_ensure_user_chats_workflow,
            upsert_user_mcp_server=_upsert_user_mcp_server,
            github_tools_enabled=github_tools_enabled,
            mcp_oauth_flows=_mcp_oauth_flows,
        )
    )
)

# The coding agent's routes (P2): the harness client's HTTP contract plus sessions, config,
# uploads and tool output, behind the same catalog / local auth as everything else.
_agent_api = AgentApi(current_user=lambda request: _current_user(request))
app.state.agent_api = _agent_api
app.include_router(_agent_api.router(app_store=_store), prefix="/api/agent")


def main() -> None:
    uvicorn.run(
        "simple_chat_agent.api.main:app",
        host=os.environ.get("SIMPLE_CHAT_API_HOST", "127.0.0.1"),
        port=8000,
        reload=os.environ.get("SIMPLE_CHAT_API_RELOAD", "false").lower() in {"1", "true"},
        proxy_headers=True,
        forwarded_allow_ips=os.environ.get("FORWARDED_ALLOW_IPS", "127.0.0.1"),
    )


if __name__ == "__main__":
    main()
