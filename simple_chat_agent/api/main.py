from __future__ import annotations

import asyncio
import json
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator
from urllib.parse import quote, urlencode, urlparse

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import (
    FileResponse,
    RedirectResponse,
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
    user_from_google_subject,
    user_from_local_username,
    user_from_session_token,
)
from simple_chat_agent.api.features import (
    demo_workspace_mode,
    demo_workspaces_enabled,
    github_tools_enabled,
)
from simple_chat_agent.api.github_oauth import (
    GITHUB_PROVIDER,
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
from simple_chat_agent.api.local_auth import (
    local_auth_credentials_valid,
    local_auth_requested,
)
from simple_chat_agent.api.routes.demo_workspace import (
    DemoWorkspaceRouteDeps,
    create_demo_workspace_router,
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
from simple_chat_agent.worker.demo_workspace_workflow import (
    DemoWorkspaceConfig,
    DemoWorkspaceInput,
    DemoWorkspaceWorkflow,
    WorkspaceChatRecord,
    demo_workspace_search_attributes,
    demo_workspace_workflow_id,
)
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
        os.environ.get("TEMPORAL_ENDPOINT", "localhost:7233"), **client_config
    )

    app.state.store = AppStore()
    app.state.mcp_oauth_flows = {}
    # In-memory per-stream event buffers, used when streaming arrives over the
    # API-owned HTTP endpoint (deployment) instead of local files (local dev).
    app.state.stream_broker = StreamBroker()
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
            "demo_workspace": demo_workspace_mode(),
            "demo_workspaces": demo_workspaces_enabled(),
            "github_tools": github_tools_enabled(),
        },
    }


@app.get("/api/auth/google/configured")
async def google_auth_configured() -> dict[str, Any]:
    configured = google_oauth_configured()
    if demo_workspace_mode():
        configured = bool(
            os.environ.get("SIMPLE_CHAT_DEMO_PARENT_PUBLIC_URL", "").strip()
        )
    return {
        "configured": configured,
        "allowed_domain": google_allowed_domain(),
    }


def _local_auth_route_enabled() -> bool:
    return (
        local_auth_requested()
        and not google_oauth_configured()
        and not demo_workspace_mode()
    )


@app.get("/api/auth/config")
async def auth_config() -> dict[str, Any]:
    google_configured = google_oauth_configured()
    local_enabled = _local_auth_route_enabled()
    if demo_workspace_mode():
        google_configured = bool(
            os.environ.get("SIMPLE_CHAT_DEMO_PARENT_PUBLIC_URL", "").strip()
        )
        local_enabled = False
    mode = "none"
    if google_configured:
        mode = "google"
    elif local_enabled:
        mode = "local"
    return {
        "mode": mode,
        "google_configured": google_configured,
        "local_enabled": local_enabled,
        "allowed_domain": google_allowed_domain(),
    }


@app.get("/oauth/google/start")
async def google_oauth_start(request: Request) -> RedirectResponse:
    if demo_workspace_mode():
        parent_url = os.environ.get("SIMPLE_CHAT_DEMO_PARENT_PUBLIC_URL", "").strip()
        if not parent_url:
            raise HTTPException(
                status_code=400,
                detail="Demo workspace parent URL is not configured",
            )
        return RedirectResponse(
            f"{parent_url.rstrip('/')}/oauth/google/start?"
            f"{urlencode({'return_to': str(request.base_url).rstrip('/')})}"
        )

    if not google_oauth_configured():
        raise HTTPException(status_code=400, detail="Google OAuth is not configured")

    return_to = _allowed_demo_workspace_return_to(
        str(request.query_params.get("return_to") or "")
    )
    state = _store().create_oauth_state(
        user_id="",
        provider=GOOGLE_PROVIDER,
        metadata={"return_to": return_to} if return_to else None,
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

    _state_user_id, state_metadata = consumed
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
    return_to = _allowed_demo_workspace_return_to(state_metadata.get("return_to", ""))
    if return_to:
        token = quote(create_session_token(user), safe="")
        response = RedirectResponse(
            f"{return_to.rstrip('/')}/api/demo-workspace/login?token={token}"
        )
        response.set_cookie(
            SESSION_COOKIE,
            create_session_token(user),
            max_age=DEFAULT_SESSION_SECONDS,
            httponly=True,
            samesite="lax",
        )
        return response

    response = RedirectResponse("/")
    response.set_cookie(
        SESSION_COOKIE,
        create_session_token(user),
        max_age=DEFAULT_SESSION_SECONDS,
        httponly=True,
        samesite="lax",
    )
    return response


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
    # Worker -> web: append a stream event to the in-memory per-stream buffer.
    # Authenticated with a shared token (cluster-internal); not user-facing.
    token = os.environ.get("SIMPLE_CHAT_STREAM_TOKEN", "").strip()
    if not token or request.headers.get("x-stream-token") != token:
        raise HTTPException(status_code=401, detail="Invalid stream token.")
    event = await request.json()
    stream_id = event.get("stream_id")
    if stream_id:
        _stream_broker().append(stream_id, event)
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
    cursor = _stream_broker().append_event(
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
    _stream_broker().clear(workflow_id)
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


def _is_demo_workspace_absent(err: BaseException) -> bool:
    return isinstance(err, RPCError) and err.status in {
        RPCStatusCode.NOT_FOUND,
        RPCStatusCode.FAILED_PRECONDITION,
    }


def _handle(workflow_id: str) -> Any:
    return _client().get_workflow_handle(workflow_id)


def _user_email_sa_name() -> str:
    return os.environ.get("SIMPLE_CHAT_USER_EMAIL_SEARCH_ATTR", "").strip()


async def _ensure_user_chats_workflow(user_id: str, user_email: str = "") -> Any:
    await _require_demo_workspace_can_start_registry()
    workflow_id = user_chats_workflow_id(user_id)
    search_attr_name = _user_email_sa_name()
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
    return _client().get_workflow_handle(workflow_id)


async def _require_demo_workspace_can_start_registry() -> None:
    if not demo_workspace_mode():
        return

    handle = _demo_workspace_parent_workflow()
    if handle is None:
        raise HTTPException(
            status_code=410,
            detail="Demo workspace is no longer available.",
        )

    try:
        state = await handle.query(DemoWorkspaceWorkflow.state)
    except Exception as err:
        if _is_demo_workspace_absent(err):
            raise HTTPException(
                status_code=410,
                detail="Demo workspace is no longer available.",
            ) from err
        raise

    if state.status not in {"active", "provisioning"}:
        raise HTTPException(
            status_code=410,
            detail=f"Demo workspace is {state.status}.",
        )


async def _ensure_demo_workspace_workflow(user: AuthenticatedUser) -> Any:
    workflow_id = demo_workspace_workflow_id(user.user_id)
    search_attr_name = _user_email_sa_name()
    return await _client().start_workflow(
        DemoWorkspaceWorkflow.run,
        DemoWorkspaceInput(
            user_id=user.user_id,
            user_email=user.username,
            search_attr_name=search_attr_name,
        ),
        id=workflow_id,
        task_queue=TASK_QUEUE,
        id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
        static_summary="simple chat demo workspace controller",
        search_attributes=demo_workspace_search_attributes(
            search_attr_name=search_attr_name,
            user_email=user.username,
        ),
    )


def _demo_workspace_parent_workflow() -> Any | None:
    workflow_id = os.environ.get("SIMPLE_CHAT_DEMO_PARENT_WORKFLOW_ID", "").strip()
    if not workflow_id:
        return None
    return _client().get_workflow_handle(workflow_id)


def _demo_workspace_config(user: AuthenticatedUser) -> DemoWorkspaceConfig:
    source_namespace = os.environ.get("SIMPLE_CHAT_DEMO_SOURCE_NAMESPACE", "").strip()
    if not source_namespace:
        source_namespace = _pod_namespace() or "temporal-michaelj-agent-harness-demo"

    testing_defaults = TASK_QUEUE.endswith("-testing") or "testing" in TASK_QUEUE
    default_web = (
        "agent-harness-web-testing" if testing_defaults else "agent-harness-web"
    )
    default_api = (
        "agent-harness-api-testing" if testing_defaults else "agent-harness-api"
    )
    default_worker = (
        "agent-harness-worker-testing" if testing_defaults else "agent-harness-worker"
    )
    default_tls = (
        "agent-harness-workspace-testing-tls"
        if testing_defaults
        else "agent-harness-workspace-tls"
    )
    default_host_suffix = (
        "-agent-harness-demo.testing.tmprl-demo.cloud"
        if testing_defaults
        else "-agent-harness-demo.tmprl-demo.cloud"
    )
    parent_public_url = os.environ.get("SIMPLE_CHAT_PUBLIC_URL", "").strip()
    if not parent_public_url:
        raise HTTPException(
            status_code=500,
            detail="SIMPLE_CHAT_PUBLIC_URL is required to create demo workspaces.",
        )

    return DemoWorkspaceConfig(
        user_id=user.user_id,
        user_email=user.username,
        control_workflow_id=demo_workspace_workflow_id(user.user_id),
        temporal_namespace=_client().namespace,
        source_namespace=source_namespace,
        source_secret_name=os.environ.get(
            "SIMPLE_CHAT_DEMO_SOURCE_SECRET_NAME",
            "agent-harness-secrets",
        ),
        tls_secret_name=os.environ.get(
            "SIMPLE_CHAT_DEMO_TLS_SECRET_NAME",
            default_tls,
        ),
        host_suffix=os.environ.get(
            "SIMPLE_CHAT_DEMO_WORKSPACE_HOST_SUFFIX",
            default_host_suffix,
        ),
        parent_public_url=parent_public_url,
        task_queue_prefix=os.environ.get(
            "SIMPLE_CHAT_DEMO_TASK_QUEUE_PREFIX",
            f"{TASK_QUEUE}-workspace",
        ),
        workflow_prefix_prefix=os.environ.get(
            "SIMPLE_CHAT_DEMO_WORKFLOW_PREFIX_PREFIX",
            "workspace-",
        ),
        source_web_deployment=os.environ.get(
            "SIMPLE_CHAT_DEMO_SOURCE_WEB_DEPLOYMENT",
            default_web,
        ),
        source_api_deployment=os.environ.get(
            "SIMPLE_CHAT_DEMO_SOURCE_API_DEPLOYMENT",
            default_api,
        ),
        source_worker_deployment=os.environ.get(
            "SIMPLE_CHAT_DEMO_SOURCE_WORKER_DEPLOYMENT",
            default_worker,
        ),
        service_account_role_arn=os.environ.get(
            "SIMPLE_CHAT_DEMO_WORKSPACE_ROLE_ARN",
            "",
        ).strip(),
        search_attr_name=_user_email_sa_name(),
    )


def _allowed_demo_workspace_return_to(value: str) -> str:
    if not value:
        return ""
    parsed = urlparse(value)
    if parsed.scheme != "https" or not parsed.netloc:
        return ""
    if (
        parsed.username
        or parsed.password
        or parsed.params
        or parsed.query
        or parsed.fragment
    ):
        return ""

    host_suffix = os.environ.get(
        "SIMPLE_CHAT_DEMO_WORKSPACE_HOST_SUFFIX",
        "-agent-harness-demo.testing.tmprl-demo.cloud"
        if TASK_QUEUE.endswith("-testing") or "testing" in TASK_QUEUE
        else "-agent-harness-demo.tmprl-demo.cloud",
    ).strip()
    allowed_host = host_suffix.lstrip(".")
    hostname = (parsed.hostname or "").lower()
    if not hostname or not allowed_host:
        return ""
    if not hostname.endswith(allowed_host.lower()):
        return ""
    if parsed.path not in {"", "/"}:
        return ""
    return f"https://{parsed.netloc}"


async def _register_demo_workspace_chat(conversation: ChatRecord) -> None:
    parent_workflow_id = os.environ.get(
        "SIMPLE_CHAT_DEMO_PARENT_WORKFLOW_ID", ""
    ).strip()
    if not parent_workflow_id:
        return
    try:
        await (
            _client()
            .get_workflow_handle(parent_workflow_id)
            .signal(
                DemoWorkspaceWorkflow.register_chat,
                WorkspaceChatRecord(
                    workflow_id=conversation.workflow_id,
                    run_id=conversation.run_id,
                    task_queue=conversation.task_queue,
                ),
            )
        )
    except Exception as err:  # noqa: BLE001 - parent tracking is best-effort
        print(f"Failed to register demo workspace chat: {err!r}")


async def _unregister_demo_workspace_chat(workflow_id: str) -> None:
    parent_workflow_id = os.environ.get(
        "SIMPLE_CHAT_DEMO_PARENT_WORKFLOW_ID", ""
    ).strip()
    if not parent_workflow_id:
        return
    try:
        await (
            _client()
            .get_workflow_handle(parent_workflow_id)
            .signal(
                DemoWorkspaceWorkflow.unregister_chat,
                workflow_id,
            )
        )
    except Exception as err:  # noqa: BLE001 - parent tracking is best-effort
        print(f"Failed to unregister demo workspace chat: {err!r}")


async def _touch_demo_workspace() -> None:
    parent_workflow_id = os.environ.get(
        "SIMPLE_CHAT_DEMO_PARENT_WORKFLOW_ID", ""
    ).strip()
    if not parent_workflow_id:
        return
    try:
        await (
            _client()
            .get_workflow_handle(parent_workflow_id)
            .signal(DemoWorkspaceWorkflow.touch_workspace)
        )
    except Exception as err:  # noqa: BLE001 - parent tracking is best-effort
        print(f"Failed to touch demo workspace: {err!r}")


def _pod_namespace() -> str:
    try:
        return (
            Path("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
            .read_text(encoding="utf-8")
            .strip()
        )
    except OSError:
        return ""


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
            register_demo_workspace_chat=_register_demo_workspace_chat,
            unregister_demo_workspace_chat=_unregister_demo_workspace_chat,
            touch_demo_workspace=_touch_demo_workspace,
        )
    )
)
app.include_router(
    create_demo_workspace_router(
        DemoWorkspaceRouteDeps(
            current_user=_current_user,
            workflow_handle=_handle,
            ensure_demo_workspace_workflow=_ensure_demo_workspace_workflow,
            ensure_user_chats_workflow=_ensure_user_chats_workflow,
            demo_workspace_parent_workflow=_demo_workspace_parent_workflow,
            demo_workspaces_enabled=demo_workspaces_enabled,
            demo_workspace_mode=demo_workspace_mode,
            demo_workspace_config=_demo_workspace_config,
            is_demo_workspace_absent=_is_demo_workspace_absent,
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
