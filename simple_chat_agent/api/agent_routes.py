"""The harness client's HTTP contract, served by our API (PRD S1, S2, S6, S7, S8).

``@temporalio/agent-harness-client``'s ``HttpTransport`` calls these routes relative to its
``baseUrl`` (``/api/agent/`` on the real app). Differences from the packaged harness app, all
deliberate:

* Every route checks that the signed-in user owns the session or subagent (404 otherwise,
  for unknown and foreign ids alike). See :class:`Ownership`.
* ``attach``, ``status`` and ``GET sessions`` read from Redis (filled by the bridge and the
  session directory), never from a Temporal Query, so they cost no Actions and keep working
  while every worker is down. Describe (served by Temporal itself) is the only Temporal read.
* ``messages`` and ``approve`` go through the Signal front door: they land in the workflow's
  history immediately, with or without a running worker. The message id is chosen here, so the
  client can match the later ``message_accepted`` to the message it sent.
* ``agent-interface`` is built from the agents' contract models, not Queried.
* Errors, including FastAPI's own 401 and 422, use the harness body
  ``{"error": <code>, "message": <text>}`` (see :class:`HarnessErrorRoute`).
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import hashlib
import hmac
import json
import logging
import os
import re
import time
import uuid
from urllib.parse import quote, urlsplit
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

from fastapi import APIRouter, Depends, File, HTTPException, Request, UploadFile
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response, StreamingResponse
from fastapi.routing import APIRoute
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from temporalio.client import (
    Client,
    WorkflowExecutionStatus,
    WorkflowUpdateFailedError,
    WorkflowUpdateRPCTimeoutOrCancelledError,
)
from temporalio.common import (
    SearchAttributeKey,
    SearchAttributePair,
    TypedSearchAttributes,
    WorkflowIDConflictPolicy,
    WorkflowIDReusePolicy,
)
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.service import RPCError, RPCStatusCode

from temporal_agent_harness.harness.agent_protocol import (
    TOOL_APPROVAL_UPDATE,
    AgentConfig,
    MidTurn,
    ToolApprovalDecision,
)
from temporal_agent_harness.harness.agent_protocol.agent_interface import AcceptedFunction

from simple_chat_agent.agent import approvals
from simple_chat_agent.agent.approvals import DEFAULT_JUDGE_MODEL, JUDGE_MODEL_ENV, configured_judge_backend
from simple_chat_agent.agent.contracts import (
    AGENT_WORKFLOW_TYPE,
    ENQUEUE_APPROVAL_SIGNAL,
    ENQUEUE_CALLBACK_SIGNAL,
    ENQUEUE_MESSAGE_SIGNAL,
    PICK_MAX_FILE_BYTES,
    PICK_MAX_FILES,
    PICK_MAX_NAME_CHARS,
    PICK_UPLOAD_DIR,
    Ack,
    AlwaysAllowedForgotten,
    ChatMessage,
    ChatReply,
    CodingAgentInit,
    Findings,
    ForgetAlwaysAllowed,
    FrontDoorApproval,
    FrontDoorCallbackResult,
    FrontDoorMessage,
    InterruptRequest,
    Question,
    Review,
    ReviewRequest,
    ModelChoice,
    Posture,
    SetModel,
    SetPosture,
    SettingsAck,
    SetWebSearch,
    SteerMessage,
)
# Key-free: the executor log reader is a plain HTTP helper (no tools runtime, no LLM keys).
from simple_chat_agent.agent.tools.workspace import ExecutionLogNotFound, ExecutorRequestError, read_execution_log
from simple_chat_agent.common.attachments import MAX_ATTACHMENT_BYTES
from simple_chat_agent.api import agent_models
from simple_chat_agent.api.agent_mcp import create_mcp_router, mcp_enabled, mcp_feature
from simple_chat_agent.api.agent_bridge import Bridge
from simple_chat_agent.api.agent_store import MAX_SEQ, AgentStore, prefix, session_ttl_seconds

log = logging.getLogger(__name__)

KEEPALIVE_SECONDS = 15.0
READ_PAGE = 500
# Demo seeding: the claim's life (a crashed seeder frees it), and how long a concurrent create
# of the same id waits for the seeder before answering 409 ``creating``.
SEEDING = "seeding"
SEED_CLAIM_SECONDS = 120
SEED_WAIT_SECONDS = 20.0
SEED_POLL_SECONDS = 0.25
_UNSET = object()
NO_STORE = {"Cache-Control": "no-store"}
SSE_HEADERS = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no", "Connection": "keep-alive"}

# Every CodingAgent carries its owner as a keyword search attribute, so Describe can answer
# "who owns this workflow" with no worker and no Redis record (P2 decision 1).
OWNER_SEARCH_ATTRIBUTE = SearchAttributeKey.for_keyword("CodingAgentOwner")
# How far Describe walks up parent links from a subagent id to its root session.
MAX_PARENT_HOPS = 4
# Workflow ids we will even look up: what we mint, and what Temporal can hold.
_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$")
_SESSION_ID = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
# Client-supplied ids and attachment fields (P3 security review #5, #6).
_REQUEST_ID = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
_ATTACHMENT_ID = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
_MIME_TYPE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9!#$&^_.+-]{0,63}/[A-Za-z0-9][A-Za-z0-9!#$&^_.+-]{0,126}$")
_CONTROL_CHARS = re.compile(r"[\x00-\x1f\x7f-\x9f\u2028\u2029]")
TITLE_MAX = 200
# Browser callbacks (PRD A12): tool ids we accept, the answer's size, and an error note's length.
_TOOL_ID = re.compile(r"^[A-Za-z0-9_.:/-]{1,200}$")
CALLBACK_RESULT_MAX_BYTES = 16 * 1024
# Per-session caps on pick_files uploads, across all tool ids (P4 review). Uploads are also
# accepted only while that pick waits (P5, mirrored by the bridge); the caps bound the total.
PICK_SESSION_MAX_FILES = 50
PICK_SESSION_MAX_BYTES = 50 * 1024 * 1024
CALLBACK_ERROR_MAX = 500
# How long a subagent tool approval waits for a worker to take the harness Update.
CHILD_APPROVAL_TIMEOUT = timedelta(seconds=10)
# The envelope allows for multipart framing around the largest allowed attachment.
MAX_UPLOAD_BYTES = MAX_ATTACHMENT_BYTES
UPLOAD_ENVELOPE_BYTES = 64 * 1024
SESSIONS_LIST_MAX = 500
# Rebuilding a lost session list Queries CodingSessions (server-side, rare); at most this often.
REBUILD_BACKOFF_SECONDS = 600
# ... but only this long after a failed Query (no worker), so the list comes back soon.
REBUILD_RETRY_SECONDS = 30
REBUILD_QUERY_TIMEOUT = timedelta(seconds=5)
# Describe-fallback misses (unknown or foreign ids) are remembered this long per caller.
OWNER_MISS_TTL_SECONDS = 30.0
OWNER_MISS_CACHE_MAX = 10_000


def offered_research_tools() -> list[str]:
    """Research tools new sessions offer, from a non-secret flag (the API holds no search or
    LLM keys). Default: fetch_url only; set AGENT_RESEARCH_TOOLS=fetch_url,search_web where
    the workers have a search backend configured."""
    raw = os.environ.get("AGENT_RESEARCH_TOOLS", "fetch_url")
    allowed = {"fetch_url", "search_web"}
    return [name for name in (n.strip() for n in raw.split(",")) if name in allowed]


def agent_environment() -> str:
    """``catalog`` (default, stricter approval rules) or ``local`` for development."""
    return "local" if os.environ.get("AGENT_ENVIRONMENT", "catalog").strip().lower() == "local" else "catalog"


def web_search_allowed(environment: str) -> bool:
    """Whether new sessions may turn on the provider's built-in web search (P4 4.5, PM
    decision 2). Its queries run inside the provider, where our approval gate can't see them,
    so: available locally, off in the catalog unless AGENT_WEB_SEARCH_CATALOG=1, and off
    everywhere with AGENT_WEB_SEARCH=off. Per session it still starts off."""
    if os.environ.get("AGENT_WEB_SEARCH", "").strip().lower() in ("0", "off", "false", "no"):
        return False
    if environment == "local":
        return True
    return os.environ.get("AGENT_WEB_SEARCH_CATALOG", "").strip().lower() in ("1", "true", "yes", "on")


def agent_task_queue() -> str:
    return os.environ.get("CODING_AGENT_TASK_QUEUE", "coding-agent")


# -- what the product view's header and composer need (P3 step 3.0) ----------------------
# Static: no provider calls, no keys, no Temporal calls.

_LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1", "[::1]"}
_OFF = {"0", "false", "no", "off", "none", "disabled"}


def auto_mode_status(judge: str) -> dict[str, Any]:
    """Whether Auto mode's judge can run here. The API holds no keys, so this checks only what
    it can see: the judge model setting the workers share (``AGENT_JUDGE_MODEL``) is well formed
    and names a provider we offer. Jev needs no model setting."""
    spec = os.environ.get(JUDGE_MODEL_ENV, "").strip() or DEFAULT_JUDGE_MODEL
    provider, sep, model = spec.partition(":")
    available = judge == "jev" or bool(sep and model and provider in agent_models.DEFAULT_ALLOWLIST)
    return {
        "available": available,
        "judge": judge,
        "judge_model": None if judge == "jev" else spec,
        "reason": None if available else "Auto mode's judge isn't set up here.",
    }


def posture_refusal(posture: str, environment: str, auto_mode: dict[str, Any]) -> str | None:
    """Why ``posture`` can't be chosen here, or ``None`` when it can. One rule for the posture
    menu, ``POST sessions`` and a ``set_posture`` message, so their reasons always match."""
    if posture not in agent_models.postures_for(environment):
        return "Local development only."
    if posture == "auto" and not auto_mode["available"]:
        return auto_mode["reason"]
    return None


def posture_options(environment: str, auto_mode: dict[str, Any]) -> list[dict[str, Any]]:
    """Every posture, in menu order, with whether it can be chosen here and why not."""
    options = []
    for posture, (label, description) in agent_models.POSTURES.items():
        reason = posture_refusal(posture, environment, auto_mode)
        options.append(
            {
                "id": posture,
                "label": label,
                "description": description,
                "available": reason is None,
                "reason": reason,
                "default": posture == agent_models.DEFAULT_POSTURE,
            }
        )
    return options


def tool_risk_info(environment: str) -> dict[str, Any]:
    """Each known tool's risk class and the classes "Always approve" covers, read from the
    agent's own rulebook (``approvals.default_criteria`` / ``risk_class_for``), never a copy.
    Tools not listed (MCP, anything new) fall to ``default_tool_risk_class``."""
    criteria = approvals.default_criteria(environment)  # type: ignore[arg-type]
    return {
        "risk_classes": list(approvals.RISK_CLASSES),
        "tool_risk_classes": {name: approvals.risk_class_for(name, criteria) for name in sorted(criteria.tools)},
        "default_tool_risk_class": criteria.default,
        "remember_allowed_classes": [c for c in approvals.RISK_CLASSES if approvals.remember_allowed(c)],
    }


def temporal_ui_base() -> str | None:
    """The Temporal UI's base URL, or ``None`` when it is switched off (``TEMPORAL_UI_URL=off``).

    ``TEMPORAL_UI_URL`` wins. Otherwise a Temporal Cloud address maps to the Cloud UI, and a
    local dev server at ``host:port`` to its UI at ``port + 1000`` (the ``temporal server
    start-dev`` default: 7233 -> 8233, 7244 -> 8244)."""
    explicit = os.environ.get("TEMPORAL_UI_URL", "").strip()
    if explicit:
        return None if explicit.lower() in _OFF else explicit.rstrip("/")
    address = os.environ.get("TEMPORAL_ADDRESS", os.environ.get("TEMPORAL_ENDPOINT", "")).strip()
    if "tmprl.cloud" in address:
        return "https://cloud.temporal.io"
    host, _, port = address.rpartition(":")
    if host in _LOCAL_HOSTS and port.isdigit():
        return f"http://{host}:{int(port) + 1000}"
    return "http://localhost:8233"


def console_link_status() -> tuple[bool, str | None]:
    """Mirrors :func:`simple_chat_agent.api.console.mount_console`'s decision (imported lazily:
    the console module imports this one)."""
    from simple_chat_agent.api import console

    if not console.console_enabled():
        return False, "The harness console is off in this environment."
    try:
        installed = console.console_dist() is not None
    except Exception:  # noqa: BLE001 - a missing optional UI must not break config
        installed = False
    if not installed:
        return False, "The harness console isn't installed on this server."
    return True, None


def view_links() -> dict[str, Any]:
    """The header's view links. ``url_template`` placeholders: ``{session_id}`` (the session's
    root workflow id) for the console, ``{workflow_id}`` for the Temporal UI; substitute them
    URL-encoded. A disabled link has ``url_template: null`` and a ``reason``."""
    console_on, console_reason = console_link_status()
    base = temporal_ui_base()
    namespace = quote(os.environ.get("TEMPORAL_NAMESPACE", "default"), safe="")
    return {
        "console": {
            "enabled": console_on,
            "url_template": "/console/?s={session_id}" if console_on else None,
            "reason": console_reason,
        },
        "temporal_ui": {
            "enabled": base is not None,
            "url_template": f"{base}/namespaces/{namespace}/workflows/{{workflow_id}}" if base else None,
            "reason": None if base else "The Temporal UI isn't linked in this environment.",
        },
    }


def owner_digest(user_id: str) -> str:
    """The owner search attribute value and the per-user key: never the raw user id."""
    return hashlib.sha256(user_id.encode("utf-8")).hexdigest()[:32]


def owner_search_attributes(user_id: str) -> TypedSearchAttributes:
    return TypedSearchAttributes([SearchAttributePair(OWNER_SEARCH_ATTRIBUTE, owner_digest(user_id))])


async def connect_agent_client() -> Client:
    """The API's Temporal client for the agent: the app's connection settings (as in
    ``simple_chat_agent.api.main``), with the agent plugins and the worker's payload storage
    instead of the old app's data converter."""
    from pydantic_ai.durable_exec.temporal import PydanticAIPlugin
    from temporal_agent_harness.plugin import AgentHarnessPlugin

    from simple_chat_agent.agent.harness_ext import payload_storage

    kwargs: dict[str, Any] = {
        "namespace": os.environ.get("TEMPORAL_NAMESPACE", "default"),
        "tls": os.environ.get("TEMPORAL_TLS", "false").lower() in ("true", "1"),
        "plugins": [PydanticAIPlugin(), AgentHarnessPlugin(large_payload_offload=payload_storage())],
    }
    if os.environ.get("TEMPORAL_API_KEY"):
        kwargs["api_key"] = os.environ["TEMPORAL_API_KEY"]
    address = os.environ.get("TEMPORAL_ADDRESS", os.environ.get("TEMPORAL_ENDPOINT", "localhost:7233"))
    return await Client.connect(address, **kwargs)


# -- errors ----------------------------------------------------------------------------


class HarnessError(Exception):
    """Raised by a route to answer with the harness error body."""

    def __init__(self, status: int, code: str, message: str, **extra: Any) -> None:
        super().__init__(message)
        self.status, self.code, self.message, self.extra = status, code, message, extra


def _error(status: int, code: str, message: str, headers: dict[str, str] | None = None, **extra: Any) -> JSONResponse:
    return JSONResponse(
        status_code=status,
        content={"error": code, "message": message, **extra},
        headers={**NO_STORE, **(headers or {})},
    )


def check_request_id(request_id: str | None) -> None:
    """A client ``request_id`` (dedup key, Update id) is short, plain, and never claims to be
    one of the bridge's ``x_rejected:`` ids."""
    from simple_chat_agent.api.agent_telemetry import REJECTION_MESSAGE_PREFIX

    if request_id is None:
        return
    if not _REQUEST_ID.fullmatch(request_id) or request_id.startswith(REJECTION_MESSAGE_PREFIX):
        raise HarnessError(422, "invalid_request_id", "request_id must be 1-128 of [A-Za-z0-9_.:-].")


def not_found(message: str = "No such session.") -> HarnessError:
    return HarnessError(404, "not_found", message)


_HTTP_CODES = {400: "bad_request", 401: "unauthorized", 403: "forbidden", 404: "not_found", 405: "method_not_allowed"}


_UNSAFE_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})
CSRF_HEADER = "x-requested-with"


def _media_type(request: Request) -> str:
    return (request.headers.get("content-type") or "").split(";", 1)[0].strip().lower()


def _content_length(request: Request) -> int:
    try:
        return int(request.headers.get("content-length") or 0)
    except ValueError:
        return 0


def _has_body(request: Request) -> bool:
    length = request.headers.get("content-length")
    if length is not None:
        return length.strip() not in ("", "0")
    return "transfer-encoding" in request.headers


def origin_allowed(request: Request) -> bool:
    """A state-changing request is ours when it carries the custom header, or its ``Origin``
    (else, with no ``Origin`` at all, its ``Referer``) is this host or
    ``SIMPLE_CHAT_PUBLIC_URL``. Browsers always send ``Origin`` on a cross-site POST, and
    can't add a custom header without a CORS preflight, which this API never grants.

    ``Origin: null`` (sandboxed frames, ``data:`` pages, some redirects) is refused outright;
    it never falls back to ``Referer``.

    Deployment: the comparison is against the request's ``Host`` header. Behind a proxy that
    rewrites ``Host`` (to an internal name), set ``SIMPLE_CHAT_PUBLIC_URL`` to the public
    origin (scheme and host), or every browser request is refused."""
    if request.headers.get(CSRF_HEADER):
        return True
    source = request.headers.get("origin")
    if source == "null":
        return False
    if not source:
        source = request.headers.get("referer")
    if not source:
        return False
    parts = urlsplit(source)
    netloc = parts.netloc.lower()
    if not netloc:
        return False
    if netloc == (request.headers.get("host") or "").lower():
        return True
    public = os.environ.get("SIMPLE_CHAT_PUBLIC_URL", "").strip()
    if public:
        pub = urlsplit(public)
        return pub.netloc.lower() == netloc and pub.scheme.lower() == parts.scheme.lower()
    return False


class HarnessErrorRoute(APIRoute):
    """Answers every error raised while handling one of this router's routes with the harness
    error body: FastAPI's own 401 (from the auth dependency) and 422 (request validation), and
    :class:`HarnessError`. Scoped to this router; the app's other routes are unchanged.

    Also the CSRF gate for this router's state-changing routes (:func:`origin_allowed`), and
    the body type check: JSON everywhere except the multipart upload."""

    def get_route_handler(self) -> Callable[[Request], Awaitable[Response]]:
        handler = super().get_route_handler()

        # Session attachments and pick_files uploads are the only multipart routes.
        multipart = self.path.endswith("/attachments") or self.path.endswith("/files")

        async def wrapped(request: Request) -> Response:
            if request.method in _UNSAFE_METHODS:
                # CSRF: the session cookie rides cross-site requests, so a state change needs
                # proof it came from our own pages (same-origin Origin/Referer) or from a
                # script (a custom header, which a cross-site page can't send without CORS).
                if not origin_allowed(request):
                    return _error(403, "forbidden", "Cross-site request refused.")
                if _has_body(request):
                    expected = "multipart/form-data" if multipart else "application/json"
                    if _media_type(request) != expected:
                        return _error(415, "unsupported_media_type", f"Send the body as {expected}.")
                    if multipart and _content_length(request) > MAX_UPLOAD_BYTES + UPLOAD_ENVELOPE_BYTES:
                        # Refused before the multipart parser spools it.
                        return _error(413, "too_large", f"Attachments are limited to {MAX_UPLOAD_BYTES} bytes.")
            limit = _limit_body(request, MAX_UPLOAD_BYTES + UPLOAD_ENVELOPE_BYTES) if multipart else None
            try:
                return await handler(request)
            except Exception as exc:
                # A body cut off at the limit surfaces from FastAPI's form parsing as a 400.
                if limit is not None and limit.over:
                    return _error(413, "too_large", f"Attachments are limited to {MAX_UPLOAD_BYTES} bytes.")
                return _harness_error_response(exc)

        return wrapped


def _harness_error_response(exc: Exception) -> Response:
    """The harness error body for an exception a route raised; anything else re-raises."""
    if isinstance(exc, HarnessError):
        return _error(exc.status, exc.code, exc.message, **exc.extra)
    if isinstance(exc, RequestValidationError):
        errors = jsonable_encoder(exc.errors())
        first = errors[0] if errors else {}
        where = ".".join(str(p) for p in first.get("loc", ()) if p != "body")
        message = f"{where}: {first.get('msg')}" if where else str(first.get("msg") or "Invalid request.")
        return _error(422, "invalid_request", message, errors=errors)
    if isinstance(exc, HTTPException):
        code = _HTTP_CODES.get(exc.status_code, "http_error")
        return _error(exc.status_code, code, str(exc.detail), headers=exc.headers)
    raise exc


class _BodyLimit:
    """Counts request body bytes as the app reads them and stops at ``limit``: covers chunked
    bodies (no Content-Length), and stops the multipart parser from spooling past it."""

    def __init__(self, request: Request, limit: int) -> None:
        self.limit, self.seen, self.over = limit, 0, False
        self._receive = request._receive

    async def __call__(self) -> Any:
        message = await self._receive()
        if message.get("type") == "http.request":
            self.seen += len(message.get("body", b""))
            if self.seen > self.limit:
                self.over = True
                raise _BodyTooLarge()
        return message


class _BodyTooLarge(Exception):
    pass


def _limit_body(request: Request, limit: int) -> _BodyLimit:
    guard = _BodyLimit(request, limit)
    request._receive = guard
    return guard


# -- request bodies ----------------------------------------------------------------------


class CreateSession(BaseModel):
    """Our create body, also accepted as the harness ``CreateSessionRequest.data``."""

    model_config = ConfigDict(extra="forbid")

    model: ModelChoice | None = None
    posture: Posture = agent_models.DEFAULT_POSTURE
    title: str | None = Field(default=None, max_length=TITLE_MAX)
    demo: Literal["refund_service"] | None = None
    # Local only: adds the agent's gated test tools (live stacked-approval checks).
    test_tools: bool = False
    # Start with the provider's web search on (refused where it isn't available).
    web_search: bool = False


class HarnessCreateSession(BaseModel):
    """The harness client's ``CreateSessionRequest`` (client start.ts)."""

    agent_workflow_type: str
    session_id: str | None = None
    data: dict[str, Any] | None = None


class SubmitMessage(BaseModel):
    session_id: str
    message: dict[str, Any]
    request_id: str | None = None


class Approve(BaseModel):
    session_id: str
    tool_id: str
    approved: bool
    reason: str | None = None
    remember: bool = False
    request_id: str | None = None


class CallbackResult(BaseModel):
    session_id: str
    tool_id: str
    result: Any = None
    error: str | None = Field(default=None, max_length=CALLBACK_ERROR_MAX)
    request_id: str | None = None


class Rename(BaseModel):
    title: str = Field(min_length=1, max_length=TITLE_MAX)


# -- agent interface ---------------------------------------------------------------------

# CodingAgent's ``@agent.accepts`` handlers, from its contract models. The workflow module
# can't be imported here: it imports the worker runtime, which builds every provider model
# from LLM keys the API doesn't hold. tests/test_agent_routes.py compares this table with
# ``agent_handlers(CodingAgent)`` (imported there with dummy keys), so a handler change on the
# workflow fails a test until this is updated. Ordered by name, as the harness discovers them.
_CODING_AGENT_HANDLERS: tuple[tuple[str, type[BaseModel], type[BaseModel], str, MidTurn, bool], ...] = (
    (
        "chat",
        ChatMessage,
        ChatReply,
        "Ask the coding agent to do something in its workspace, or ask it a question.",
        MidTurn.ENQUEUE,
        True,
    ),
    (
        "forget_always_allowed",
        ForgetAlwaysAllowed,
        AlwaysAllowedForgotten,
        'Forget this session\'s "always allow" approvals. Those tools ask again unless the posture allows them.',
        MidTurn.ACCEPT,
        False,
    ),
    (
        "interrupt",
        InterruptRequest,
        Ack,
        "Stop the turn in progress. Pending approvals in it are declined.",
        MidTurn.ACCEPT,
        False,
    ),
    (
        "set_model",
        SetModel,
        SettingsAck,
        "Switch the provider and/or model. Takes effect from the next turn.",
        MidTurn.ACCEPT,
        False,
    ),
    (
        "set_posture",
        SetPosture,
        SettingsAck,
        "Change how tool calls are approved. Applies to new calls; pending ones stay with you.",
        MidTurn.ACCEPT,
        False,
    ),
    (
        "set_web_search",
        SetWebSearch,
        SettingsAck,
        "Turn the model provider's built-in web search on or off. Applies from the next turn.",
        MidTurn.ACCEPT,
        False,
    ),
    (
        "steer",
        SteerMessage,
        Ack,
        "Guide the turn in progress. The text reaches the agent before its next model call.",
        MidTurn.ACCEPT,
        False,
    ),
)


# The read-only subagents (simple_chat_agent.agent.subagents), whose models also live in
# contracts.py. Bare ``@agent.accepts``: the decorator's defaults (reject mid-turn, callable
# by a parent's model).
_SUBAGENT_HANDLERS: dict[str, tuple[tuple[str, type[BaseModel], type[BaseModel], str, MidTurn, bool], ...]] = {
    "ExplorerAgent": (
        (
            "investigate",
            Question,
            Findings,
            "Investigate one question about the workspace's code (read-only) and report findings with file and line evidence.",
            MidTurn.REJECT,
            True,
        ),
    ),
    "ReviewerAgent": (
        (
            "review",
            ReviewRequest,
            Review,
            "Review the workspace's changes (the uncommitted diff, else the last commit) and return a verdict with concrete issues.",
            MidTurn.REJECT,
            True,
        ),
    ),
}

AGENT_HANDLERS = {AGENT_WORKFLOW_TYPE: _CODING_AGENT_HANDLERS, **_SUBAGENT_HANDLERS}


@functools.cache
def agent_interface_for(workflow_type: str) -> tuple[dict[str, Any], ...] | None:
    """The ``agent_interface`` answer for an agent workflow type, built once per process
    (None for a type we don't run)."""
    handlers = AGENT_HANDLERS.get(workflow_type)
    if handlers is None:
        return None
    return tuple(
        AcceptedFunction(
            name=name,
            description=description,
            parameters=input_type.model_json_schema(),
            output=output_type.model_json_schema(),
            mid_turn=mid_turn,
            model_callable=model_callable,
        ).model_dump(mode="json")
        for name, input_type, output_type, description, mid_turn, model_callable in handlers
    )


def coding_agent_interface() -> tuple[dict[str, Any], ...]:
    return agent_interface_for(AGENT_WORKFLOW_TYPE)  # type: ignore[return-value]


# -- the session directory ---------------------------------------------------------------


class SessionDirectory:
    """Each user's session list, served to browsers from Redis and kept durably in the user's
    ``CodingSessions`` workflow.

    Redis: a sorted set of session ids by creation time and a hash of records, per owner
    digest. Every change is written to Redis first (browsers read it at once), then sent to the
    workflow as a Signal (signal-with-start), which needs no worker. ``sync_workflow=False``
    keeps the list in Redis only. A lost Redis list is rebuilt from Temporal Visibility and
    the sessions' memos (:meth:`_rebuild`), never by querying ``CodingSessions``.
    """

    def __init__(
        self,
        redis_client: Any,
        *,
        temporal: Callable[[], Awaitable[Client]] | None,
        task_queue: str,
        sync_workflow: bool = True,
    ) -> None:
        self.r = redis_client
        self._temporal = temporal
        self._task_queue = task_queue
        self._sync = sync_workflow and temporal is not None

    def _keys(self, user_id: str) -> tuple[str, str]:
        base = f"{prefix()}:u:{owner_digest(user_id)}"
        return f"{base}:sessions", f"{base}:records"

    def _rebuilt_key(self, user_id: str) -> str:
        return f"{prefix()}:u:{owner_digest(user_id)}:rebuilt"

    def _touch(self, pipe: Any, user_id: str) -> None:
        """Extend both keys' life: an active user's list never expires under them."""
        ttl = session_ttl_seconds()
        for key in self._keys(user_id):
            pipe.expire(key, ttl)

    async def put(self, user_id: str, record: dict[str, Any]) -> None:
        index, records = self._keys(user_id)
        pipe = self.r.pipeline(transaction=True)
        pipe.zadd(index, {record["session_id"]: float(record["created_at"])})
        pipe.hset(records, record["session_id"], json.dumps(record))
        self._touch(pipe, user_id)
        await pipe.execute()
        await self._send(user_id, "register_session_signal", record)

    async def get(self, user_id: str, session_id: str) -> dict[str, Any] | None:
        _index, records = self._keys(user_id)
        pipe = self.r.pipeline(transaction=True)
        pipe.hget(records, session_id)
        self._touch(pipe, user_id)
        raw, *_ = await pipe.execute()
        if raw is None:
            index, _ = self._keys(user_id)
            # The whole list is gone (expired, Redis lost): rebuild it, then look again.
            if not await self.r.exists(index) and await self._rebuild(user_id):
                raw = await self.r.hget(records, session_id)
        return json.loads(raw) if raw else None

    async def list(self, user_id: str) -> list[dict[str, Any]]:
        index, records = self._keys(user_id)
        pipe = self.r.pipeline(transaction=True)
        pipe.zrevrange(index, 0, SESSIONS_LIST_MAX - 1)
        self._touch(pipe, user_id)
        ids, *_ = await pipe.execute()
        if not ids:
            if not await self._rebuild(user_id):
                return []
            ids = await self.r.zrevrange(index, 0, SESSIONS_LIST_MAX - 1)
            if not ids:
                return []
        raws = await self.r.hmget(records, ids)
        return [json.loads(raw) for raw in raws if raw]

    async def _update_record(self, user_id: str, session_id: str, **fields: Any) -> dict[str, Any] | None:
        """Change fields of an existing record only (never recreates a removed one), in one
        atomic step, and extend the keys' life."""
        _index, records = self._keys(user_id)
        raw = await self.r.eval(_UPDATE_RECORD, 1, records, session_id, json.dumps(fields), session_ttl_seconds())
        if not raw:
            return None
        index, _ = self._keys(user_id)
        await self.r.expire(index, session_ttl_seconds())
        return json.loads(raw)

    async def rename(self, user_id: str, session_id: str, title: str) -> dict[str, Any] | None:
        record = await self._update_record(user_id, session_id, title=title)
        if record is None:
            return None
        await self._send(user_id, "rename_session_signal", {"session_id": session_id, "title": title})
        return record

    async def update_settings(
        self,
        user_id: str,
        session_id: str,
        *,
        model: ModelChoice | None = None,
        posture: str | None = None,
        web_search: bool | None = None,
    ) -> dict[str, Any] | None:
        """The session's current model, posture or web search, after the agent accepted a
        change: Redis at once, then (model and posture) ``update_session_signal``
        (signal-with-start, so workers can be down). Web search lives in Redis only; a rebuilt
        list has the start value from the memo, and the client folds later toggles on top."""
        fields: dict[str, Any] = {}
        if web_search is not None:
            fields["web_search"] = web_search
        if model is not None:
            fields["model"] = model.registry_key
            fields["model_choice"] = model.model_dump(mode="json", exclude_none=True)
        if posture is not None:
            fields["posture"] = posture
        if not fields:
            return None
        record = await self._update_record(user_id, session_id, **fields)
        if record is None:
            return None
        if model is not None or posture is not None:
            await self._send(
                user_id,
                "update_session_signal",
                {"session_id": session_id, "model": fields.get("model"), "posture": posture},
            )
        return record

    async def mark_closed(self, user_id: str, session_id: str) -> None:
        await self._update_record(user_id, session_id, closed=True)

    async def remove(self, user_id: str, session_id: str) -> None:
        index, records = self._keys(user_id)
        pipe = self.r.pipeline(transaction=True)
        pipe.zrem(index, session_id)
        pipe.hdel(records, session_id)
        await pipe.execute()
        await self._send(user_id, "remove_session_signal", session_id)

    async def _retry_rebuild_soon(self, user_id: str, exc: Exception) -> None:
        """Visibility failed (Temporal unreachable): try again after a short wait rather than
        leaving the list empty for the whole backoff."""
        log.info("session directory rebuild for %s failed, retrying soon: %s", owner_digest(user_id), exc)
        await self.r.expire(self._rebuilt_key(user_id), REBUILD_RETRY_SECONDS)

    async def _rebuild(self, user_id: str) -> bool:
        """The Redis list is gone (expired or Redis lost): rebuild it from Temporal Visibility,
        never a Query, since this runs on browser paths and must work with workers down.

        Lists the user's *running* CodingAgent workflows by the ``CodingAgentOwner`` search
        attribute (one entry per session: only the latest run of a continued-as-new chain is
        running). Title, model, posture, demo and creation time come from the memo set at
        start (:func:`session_memo`; the server carries it across continue-as-new).

        Limits, accepted because rebuilds are rare: a rename or a model/posture change made
        after start shows the start-time value again; and closed sessions are left out,
        because Visibility can't tell a closed session from a deleted one (both are closed by
        the ``close`` Signal), and a deleted session must never come back.

        At most once per REBUILD_BACKOFF_SECONDS per user.

        Known gap, deferred to P5 (security review #8): with workers down and Redis lost, a
        just-deleted session whose ``close`` Signal hasn't run yet is still Running here and
        comes back."""
        if not self._sync:
            return False
        if not await self.r.set(self._rebuilt_key(user_id), "1", nx=True, ex=REBUILD_BACKOFF_SECONDS):
            return False
        # The digest is hex, so it is safe inside the quoted literal.
        query = (
            f"WorkflowType = '{AGENT_WORKFLOW_TYPE}' AND {OWNER_SEARCH_ATTRIBUTE.name} = '{owner_digest(user_id)}' "
            "AND ExecutionStatus = 'Running'"
        )
        found: list[dict[str, Any]] = []
        try:
            assert self._temporal is not None
            client = await self._temporal()
            async for execution in client.list_workflows(
                query, limit=SESSIONS_LIST_MAX, rpc_timeout=REBUILD_QUERY_TIMEOUT
            ):
                found.append(await _rebuilt_record(execution))
        except Exception as exc:  # noqa: BLE001 - Temporal unreachable or Visibility error
            await self._retry_rebuild_soon(user_id, exc)
            return False
        if not found:
            return False
        index, records = self._keys(user_id)
        pipe = self.r.pipeline(transaction=True)
        for record in found:
            pipe.zadd(index, {record["session_id"]: record["created_at"]}, nx=True)
            pipe.hsetnx(records, record["session_id"], json.dumps(record))
        self._touch(pipe, user_id)
        await pipe.execute()
        return True

    # -- workflow sync -----------------------------------------------------------------

    async def _send(self, user_id: str, signal: str, arg: Any) -> None:
        """Record the change in the user's ``CodingSessions`` with signal-with-start: one RPC
        that starts the manager if needed and lands the Signal in its history, with or without
        a worker running. Awaited inline, so a user's changes reach it in order. A failure
        (Temporal unreachable) is logged, not raised: Redis already holds the change."""
        if not self._sync:
            return
        from simple_chat_agent.agent.sessions import (
            SESSIONS_WORKFLOW_TYPE,
            RenameRequest,
            SessionRecord,
            SessionsState,
            SessionUpdate,
            sessions_workflow_id,
        )

        if signal == "register_session_signal":
            payload: Any = SessionRecord(
                session_id=arg["session_id"],
                title=arg.get("title") or "New session",
                model=arg.get("model", ""),
                posture=arg.get("posture") or "",
                created_at=datetime.fromtimestamp(float(arg["created_at"]), timezone.utc).isoformat(),
            )
        elif signal == "rename_session_signal":
            payload = RenameRequest(**arg)
        elif signal == "update_session_signal":
            payload = SessionUpdate(**arg)
        else:
            payload = arg
        digest = owner_digest(user_id)
        try:
            assert self._temporal is not None
            client = await self._temporal()
            await client.start_workflow(
                SESSIONS_WORKFLOW_TYPE,
                SessionsState(owner_digest=digest),
                id=sessions_workflow_id(digest),
                task_queue=self._task_queue,
                id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
                search_attributes=owner_search_attributes(user_id),
                static_summary="coding agent session list",
                start_signal=signal,
                start_signal_args=[payload],
            )
        except Exception as exc:  # noqa: BLE001 - Redis already holds the change
            log.warning("session directory %s for %s not recorded in CodingSessions: %s", signal, digest, exc)


SESSION_MEMO_FIELDS = ("title", "created_at", "model", "model_choice", "posture", "environment", "demo", "web_search")


def session_memo(record: dict[str, Any]) -> dict[str, Any]:
    """The memo a CodingAgent starts with: what a Query-free rebuild of the session list
    needs (:meth:`SessionDirectory._rebuild`). Small, and nothing secret: it is visible to
    anyone who can see the workflow, like its summary."""
    return {k: record[k] for k in SESSION_MEMO_FIELDS if record.get(k) is not None}


async def _rebuilt_record(execution: Any) -> dict[str, Any]:
    """A session list record from one Visibility entry (memo first, then the run's own data)."""
    try:
        memo = await execution.memo()
    except Exception:  # noqa: BLE001 - an unreadable memo still lists the session
        memo = {}
    if not isinstance(memo, dict):
        memo = {}
    start = getattr(execution, "start_time", None)
    created = memo.get("created_at")
    if isinstance(created, bool) or not isinstance(created, (int, float)):
        created = start.timestamp() if start is not None else 0.0
    model_choice = memo.get("model_choice") if isinstance(memo.get("model_choice"), dict) else {}
    return {
        "session_id": execution.id,
        "title": str(memo.get("title") or "New session")[:TITLE_MAX],
        "created_at": float(created),
        "model": str(memo.get("model") or ""),
        "model_choice": model_choice,
        "posture": memo.get("posture") or None,
        "environment": memo.get("environment"),
        "demo": memo.get("demo"),
        # The start value (the memo isn't updated); the client folds later toggles on top.
        "web_search": memo.get("web_search") if isinstance(memo.get("web_search"), bool) else None,
        "rebuilt": True,
    }


_UPDATE_RECORD = """
local raw = redis.call('HGET', KEYS[1], ARGV[1])
if not raw then return false end
local record = cjson.decode(raw)
for k, v in pairs(cjson.decode(ARGV[2])) do record[k] = v end
local out = cjson.encode(record)
redis.call('HSET', KEYS[1], ARGV[1], out)
redis.call('EXPIRE', KEYS[1], ARGV[3])
return out
"""


# -- ownership ---------------------------------------------------------------------------


class Ownership:
    """Who may touch a workflow id: its root session's owner.

    Redis first (``root_of`` resolves a subagent id to its root session, then ``owner_of``).
    With no Redis record (Redis wiped, or a subagent the bridge hasn't registered yet), Describe
    the workflow, walk up its parent links to the root ``CodingAgent`` and compare that root's
    ``CodingAgentOwner`` search attribute; a match is written back to Redis. Describe needs no
    worker. Anything else (unknown id, foreign id, a workflow that isn't ours) is "not owned".
    """

    def __init__(self, store: AgentStore, temporal: Callable[[], Awaitable[Client]]) -> None:
        self._store = store
        self._temporal = temporal
        # (workflow id, caller) -> expiry: Describe-fallback misses, so probing unknown or
        # foreign ids costs at most one Describe chain per OWNER_MISS_TTL_SECONDS.
        self._misses: dict[tuple[str, str], float] = {}

    def _missed(self, key: tuple[str, str]) -> bool:
        expiry = self._misses.get(key)
        if expiry is None:
            return False
        if expiry > time.monotonic():
            return True
        del self._misses[key]
        return False

    def _remember_miss(self, key: tuple[str, str]) -> None:
        now = time.monotonic()
        if len(self._misses) >= OWNER_MISS_CACHE_MAX:
            self._misses = {k: v for k, v in self._misses.items() if v > now}
            if len(self._misses) >= OWNER_MISS_CACHE_MAX:
                self._misses.clear()
        self._misses[key] = now + OWNER_MISS_TTL_SECONDS

    async def root_if_owned(self, workflow_id: str, user_id: str) -> str | None:
        """The root session id when ``user_id`` owns ``workflow_id``, else None."""
        if not _ID.fullmatch(workflow_id or ""):
            return None
        root = await self._store.root_of(workflow_id) or workflow_id
        owner = await self._store.owner_of(root)
        if owner is not None:
            return root if hmac.compare_digest(owner, user_id) else None
        key = (workflow_id, user_id)
        if self._missed(key):
            return None
        found = await self._owner_from_temporal(workflow_id, user_id)
        if found is None:
            self._remember_miss(key)
        return found

    async def _owner_from_temporal(self, workflow_id: str, user_id: str) -> str | None:
        client = await self._temporal()
        current = workflow_id
        for _ in range(MAX_PARENT_HOPS):
            try:
                desc = await client.get_workflow_handle(current).describe()
            except RPCError as exc:
                if exc.status in (RPCStatusCode.NOT_FOUND, RPCStatusCode.INVALID_ARGUMENT):
                    return None
                raise
            if desc.workflow_type == AGENT_WORKFLOW_TYPE and not desc.parent_id:
                owner = desc.typed_search_attributes.get(OWNER_SEARCH_ATTRIBUTE)
                if not owner or not hmac.compare_digest(owner, owner_digest(user_id)):
                    return None
                await self._store.set_owner(current, user_id)
                if current != workflow_id:
                    await self._store.register_subagent(workflow_id, current)
                return current
            if not desc.parent_id:
                return None
            current = desc.parent_id
        return None


# -- the router --------------------------------------------------------------------------


def _is_closed(status: WorkflowExecutionStatus | None) -> bool:
    return status is not None and status not in (
        WorkflowExecutionStatus.RUNNING,
        WorkflowExecutionStatus.CONTINUED_AS_NEW,
    )


async def closed_flags(store: AgentStore, session_ids: list[str]) -> list[bool]:
    """``SessionMeta.closed`` for many sessions in one Redis round trip (the list routes)."""
    return [m.closed for m in await store.metas(session_ids)]


SeedFn = Callable[[str, str], Awaitable[Any]]
ReadLogFn = Callable[..., Awaitable[dict[str, Any]]]
# Writes one picked file into a workspace: (workspace_id, path, content) -> None. Raises
# FileExistsError when the path is taken.
WriteUploadFn = Callable[[str, str, bytes], Awaitable[None]]


async def _default_write_upload(workspace_id: str, path: str, content: bytes) -> None:
    import base64

    from temporalio.exceptions import ApplicationError

    from simple_chat_agent.agent.tools.workspace import _files_call

    try:
        await _files_call(
            "write",
            {
                "workspace_id": workspace_id,
                "path": path,
                "content": base64.b64encode(content).decode("ascii"),
                "encoding": "base64",
                "create_parents": True,
                "overwrite": False,
            },
        )
    except ApplicationError as exc:
        if exc.type == "Workspace_exists":
            raise FileExistsError(path) from None
        raise


_UNSAFE_NAME_CHARS = re.compile(r"[^A-Za-z0-9._-]+")


def upload_file_name(raw: str | None) -> str:
    """A browser file name made safe for ``uploads/<name>``: its last path component, runs of
    other characters as ``_``, no leading dots or dashes, at most PICK_MAX_NAME_CHARS (keeping
    the extension), ``file`` when nothing is left."""
    base = (raw or "").replace("\\", "/").rsplit("/", 1)[-1]
    name = _UNSAFE_NAME_CHARS.sub("_", base).lstrip("._-")
    if len(name) > PICK_MAX_NAME_CHARS:
        stem, dot, ext = name.rpartition(".")
        ext = ext[:16] if dot and stem else ""
        name = name[: PICK_MAX_NAME_CHARS - (len(ext) + 1 if ext else 0)].rstrip("._-")
        name = f"{name}.{ext}" if ext else name
    return name or "file"


def _numbered(name: str, n: int) -> str:
    stem, dot, ext = name.rpartition(".")
    if not (dot and stem):
        stem, ext = name, ""
    suffix = f"-{n}"
    stem = stem[: PICK_MAX_NAME_CHARS - len(suffix) - (len(ext) + 1 if ext else 0)]
    return f"{stem}{suffix}.{ext}" if ext else f"{stem}{suffix}"


async def _default_seed(workspace_id: str, fixture: str) -> Any:
    from simple_chat_agent.agent.fixtures import seed_workspace

    return await seed_workspace(workspace_id, fixture)


def _default_read_log() -> ReadLogFn | None:
    return read_execution_log


def _summary(record: dict[str, Any], *, closed: bool = False) -> dict[str, Any]:
    """The harness ``SessionSummary`` (client start.ts) plus our fields."""
    return {
        "workflow_id": record["session_id"],
        "created_at": record["created_at"],
        "label": record.get("title") or "New session",
        "agent_workflow_type": AGENT_WORKFLOW_TYPE,
        "execution_status": "COMPLETED" if closed else "RUNNING",
        "closed": closed,
        "session_id": record["session_id"],
        "agent": AGENT_WORKFLOW_TYPE,
        "title": record.get("title") or "New session",
        "model": record.get("model_choice") or {},
        "posture": record.get("posture"),
        "environment": record.get("environment"),
        "demo": record.get("demo"),
        "web_search": record.get("web_search"),
    }


def create_agent_router(
    *,
    temporal: Callable[[], Awaitable[Client]],
    store: AgentStore,
    bridge: Callable[[], Awaitable[Bridge]],
    current_user: Callable[[Request], Any],
    task_queue: str,
    directory: SessionDirectory | None = None,
    app_store: Callable[[], Any] | None = None,
    seed: SeedFn | None = None,
    read_log: ReadLogFn | None = None,
    write_upload: WriteUploadFn | None = None,
) -> APIRouter:
    """The agent routes. Mount with ``prefix="/api/agent"``.

    ``directory`` defaults to a Redis-only list on ``store``'s Redis; ``app_store`` (the app's
    ``AppStore``) enables uploads; ``seed`` and ``read_log`` default to the fixture seeder and
    the executor log reader."""
    router = APIRouter(route_class=HarnessErrorRoute)
    owners = Ownership(store, temporal)
    sessions = directory or SessionDirectory(store.r, temporal=None, task_queue=task_queue, sync_workflow=False)
    seed_fn = seed or _default_seed
    read_log_fn = read_log if read_log is not None else _default_read_log()
    write_upload_fn = write_upload or _default_write_upload

    def user_id(request: Request) -> str:
        return current_user(request).user_id

    async def require(workflow_id: str, uid: str) -> str:
        root = await owners.root_if_owned(workflow_id, uid)
        if root is None:
            raise not_found()
        return root

    async def mark_closed(session_id: str, uid: str) -> None:
        """The session's workflow closed (or was asked to): ``GET sessions`` shows it."""
        await store.mark_closed(session_id)
        await sessions.mark_closed(uid, session_id)

    async def require_root(session_id: str, uid: str) -> str:
        """Routes that act on a whole session take only its root id."""
        root = await require(session_id, uid)
        if root != session_id:
            raise not_found()
        return root

    # -- config --------------------------------------------------------------------------

    @router.get("/config")
    async def config(uid: str = Depends(user_id)):
        environment = agent_environment()
        judge = configured_judge_backend()
        body = agent_models.catalog(environment=environment, research_tools=offered_research_tools(), judge=judge)
        auto_mode = auto_mode_status(judge)
        # ``postures`` (above) lists only what can be chosen; ``posture_options`` is the whole
        # menu, with the reason an option is greyed out.
        body["posture_options"] = posture_options(environment, auto_mode)
        body["auto_mode"] = auto_mode
        # Provider web search: whether the composer offers the toggle. Sessions start with it off.
        body["web_search"] = {"available": web_search_allowed(environment), "default": False}
        body["links"] = view_links()
        body.update(tool_risk_info(environment))
        # MCP servers (PRD A8): local development only (D7); the settings dialog reads this.
        body["mcp"] = mcp_feature(environment)
        # How long an idle session and its list entry are kept (the store's own TTL).
        ttl = session_ttl_seconds()
        body["session_ttl_seconds"] = ttl
        body["session_ttl_days"] = ttl // 86400 if ttl % 86400 == 0 else round(ttl / 86400, 1)
        return JSONResponse(body, headers=NO_STORE)

    # -- MCP servers (PRD A8): agent_mcp.py --------------------------------------------

    router.include_router(
        create_mcp_router(
            user_id=user_id,
            redis=store.r,
            directory=sessions,
            temporal=temporal,
            app_store=app_store,
            environment=agent_environment,
            error=HarnessError,
            key_prefix=prefix,
            digest=owner_digest,
            route_class=HarnessErrorRoute,
        )
    )

    # -- sessions ------------------------------------------------------------------------

    @router.post("/sessions")
    async def create_session(request: Request, uid: str = Depends(user_id)):
        try:
            raw = await request.json()
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise HarnessError(422, "invalid_request", "The body must be JSON.") from None
        if not isinstance(raw, dict):
            raise HarnessError(422, "invalid_request", "The body must be a JSON object.")
        requested_id: str | None = None
        if "agent_workflow_type" in raw:
            try:
                harness = HarnessCreateSession.model_validate(raw)
            except ValidationError as exc:
                raise HarnessError(422, "invalid_request", str(exc), errors=jsonable_encoder(exc.errors())) from None
            if harness.agent_workflow_type != AGENT_WORKFLOW_TYPE:
                raise HarnessError(
                    422, "unknown_agent", f"Unknown agent {harness.agent_workflow_type!r}; this API runs {AGENT_WORKFLOW_TYPE}."
                )
            requested_id = harness.session_id
            body_data, code = harness.data or {}, "invalid_init_data"
        else:
            body_data, code = raw, "invalid_request"
        try:
            body = CreateSession.model_validate(body_data)
        except ValidationError as exc:
            raise HarnessError(422, code, str(exc), errors=jsonable_encoder(exc.errors())) from None

        environment = agent_environment()
        model = body.model or agent_models.default_model()
        # The configured allowlist, not this process's keys: the API holds no LLM keys. The
        # worker re-checks against the models it can actually call.
        if not agent_models.is_allowed(model):
            raise HarnessError(422, "model_not_allowed", f"{model.registry_key} is not available.")
        refusal = posture_refusal(body.posture, environment, auto_mode_status(configured_judge_backend()))
        if refusal is not None:
            raise HarnessError(422, "posture_not_allowed", refusal)
        if body.test_tools and environment != "local":
            raise HarnessError(422, "test_tools_not_allowed", "Test tools are local development only.")
        if body.web_search and not web_search_allowed(environment):
            raise HarnessError(422, "web_search_not_allowed", "Web search isn't available here.")

        if requested_id is not None:
            # Caller-chosen ids are idempotent per user and can't collide across users: the
            # workflow id is derived from the owner and the requested id.
            if not _SESSION_ID.fullmatch(requested_id):
                raise HarnessError(422, "invalid_request", "session_id must be 1-64 of [A-Za-z0-9_-].")
            digest = hashlib.sha256(f"{owner_digest(uid)}:{requested_id}".encode()).hexdigest()[:16]
            session_id = f"coding-agent-{digest}"
        else:
            session_id = f"coding-agent-{uuid.uuid4().hex[:16]}"

        # Ownership first (a crash after starting the workflow must not orphan it), claimed
        # atomically: an id another user already holds is theirs (404, like any foreign id).
        claimed = await store.claim_owner(session_id, uid)
        if not claimed and await store.owner_of(session_id) != uid:
            raise not_found()
        if requested_id is not None or not claimed:
            # A retry of a create that got partway (or a create after DELETE): if the workflow
            # exists, answer with it. Never seed a live workspace again.
            existing = await existing_summary(session_id, uid)
            if existing is not None:
                return JSONResponse(existing, headers=NO_STORE)

        data = CodingAgentInit(
            owner_id=uid,
            session_id=session_id,
            workspace_id=session_id,
            model=model,
            posture=body.posture,
            environment=environment,  # type: ignore[arg-type]
            judge=configured_judge_backend(),
            research_tools=offered_research_tools(),
            test_tools=body.test_tools,
            mcp=mcp_enabled(environment),
            web_search_allowed=web_search_allowed(environment),
            web_search=body.web_search,
        )
        title = (body.title or "").strip() or (
            agent_models.DEMOS[body.demo] if body.demo else "New session"
        )
        seeded: dict[str, Any] | None = None
        if body.demo:
            # Before the workflow starts, so the first message already sees the repo, and a
            # failed seed leaves no session behind. Once per workspace: a retry after the seed
            # succeeded but the start failed skips it.
            seeded = await seed_once(session_id, body.demo)
        record = {
            "session_id": session_id,
            "title": title,
            "created_at": time.time(),
            "model": model.registry_key,
            "model_choice": model.model_dump(mode="json", exclude_none=True),
            "posture": body.posture,
            "environment": environment,
            "demo": body.demo,
            # Whether provider web search is on (P5: the toggle reads it after a reload). The
            # bridge keeps it current from each set_web_search result.
            "web_search": bool(body.web_search),
        }
        client = await temporal()
        try:
            started = await client.start_workflow(
                AGENT_WORKFLOW_TYPE,
                args=[AgentConfig(), data],
                id=session_id,
                task_queue=task_queue,
                search_attributes=owner_search_attributes(uid),
                id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
                static_summary=title[:200],
                # What a Query-free rebuild of the session list reads (SessionDirectory._rebuild).
                memo=session_memo(record),
            )
        except WorkflowAlreadyStartedError:
            # A concurrent create of the same caller-chosen id won the start.
            existing = await existing_summary(session_id, uid)
            if existing is not None:
                return JSONResponse(existing, headers=NO_STORE)
            raise HarnessError(409, "session_exists", "That session id is taken.") from None
        await sessions.put(uid, record)
        run_id = getattr(started, "result_run_id", None)
        if isinstance(run_id, str) and run_id:
            await store.set_tracking(session_id, run_id=run_id)
        summary = _summary(record)
        if seeded is not None:
            summary["seed"] = seeded
        return JSONResponse(summary, headers=NO_STORE)

    async def seed_once(session_id: str, fixture: str) -> dict[str, Any]:
        """Seed the workspace exactly once, even across concurrent creates of the same
        caller-chosen id and retries: the marker is claimed (SET NX "seeding") before seeding
        and holds the result after. A loser waits for the winner, briefly, else 409."""
        marker = f"{prefix()}:seeded:{session_id}"
        if not await store.r.set(marker, SEEDING, nx=True, ex=SEED_CLAIM_SECONDS):
            deadline = time.monotonic() + SEED_WAIT_SECONDS
            while True:
                value = await store.r.get(marker)
                if value and value != SEEDING:
                    return json.loads(value)
                if value is None:
                    # The winner failed and released it: this request may seed after all.
                    if await store.r.set(marker, SEEDING, nx=True, ex=SEED_CLAIM_SECONDS):
                        break
                elif time.monotonic() >= deadline:
                    raise HarnessError(409, "creating", "This session is still being set up; try again shortly.")
                await asyncio.sleep(SEED_POLL_SECONDS)
        try:
            result = await seed_fn(session_id, fixture)
        except Exception:  # noqa: BLE001 - executor refused or unreachable
            await store.r.delete(marker)
            log.exception("seeding %s for %s failed", fixture, session_id)
            raise HarnessError(502, "seed_failed", "Couldn't set up the demo workspace. Try again in a moment.") from None
        seeded = {
            "fixture": fixture,
            "files": len(getattr(result, "files", []) or []),
            "commit": getattr(result, "commit", ""),
        }
        await store.r.set(marker, json.dumps(seeded), ex=session_ttl_seconds())
        return seeded

    async def existing_summary(session_id: str, uid: str) -> dict[str, Any] | None:
        """The summary of an existing session workflow the caller owns (open or closed), or
        None when there is no such workflow. Describe needs no worker."""
        try:
            desc = await (await temporal()).get_workflow_handle(session_id).describe()
        except RPCError as exc:
            if exc.status != RPCStatusCode.NOT_FOUND:
                raise
            return None
        owner = desc.typed_search_attributes.get(OWNER_SEARCH_ATTRIBUTE)
        if desc.workflow_type != AGENT_WORKFLOW_TYPE or not owner or not hmac.compare_digest(owner, owner_digest(uid)):
            raise HarnessError(409, "session_exists", "That session id is taken.")
        closed = _is_closed(desc.status)
        record = await sessions.get(uid, session_id)
        if record is None:
            start = getattr(desc, "start_time", None)
            record = {
                "session_id": session_id,
                "title": "New session",
                "created_at": start.timestamp() if start else time.time(),
            }
        if closed:
            await mark_closed(session_id, uid)
        summary = _summary(record, closed=closed or bool(record.get("closed")))
        if desc.status is not None:
            summary["execution_status"] = desc.status.name
        return summary

    @router.get("/sessions")
    async def list_sessions(uid: str = Depends(user_id)):
        records = await sessions.list(uid)
        flags = await closed_flags(store, [r["session_id"] for r in records])
        return JSONResponse(
            [_summary(r, closed=closed or bool(r.get("closed"))) for r, closed in zip(records, flags)],
            headers=NO_STORE,
        )

    @router.get("/sessions/{session_id}")
    async def get_session(session_id: str, uid: str = Depends(user_id)):
        await require_root(session_id, uid)
        record = await sessions.get(uid, session_id)
        if record is None:
            raise not_found()
        closed = bool(record.get("closed")) or (await closed_flags(store, [session_id]))[0]
        return JSONResponse(_summary(record, closed=closed), headers=NO_STORE)

    @router.post("/sessions/{session_id}/title")
    async def rename_session(session_id: str, body: Rename, uid: str = Depends(user_id)):
        await require_root(session_id, uid)
        title = body.title.strip()
        if not title:
            raise HarnessError(422, "invalid_request", "title: must not be blank")
        record = await sessions.rename(uid, session_id, title)
        if record is None:
            raise not_found()
        return JSONResponse(_summary(record), headers=NO_STORE)

    @router.delete("/sessions/{session_id}")
    async def delete_session(session_id: str, uid: str = Depends(user_id)):
        await require_root(session_id, uid)
        try:
            await (await temporal()).get_workflow_handle(session_id).signal("close")
        except RPCError as exc:
            if exc.status != RPCStatusCode.NOT_FOUND:
                raise
        await store.mark_closed(session_id)
        await sessions.remove(uid, session_id)
        return JSONResponse({"ok": True, "workflow_id": session_id}, headers=NO_STORE)

    @router.post("/sessions/{session_id}/close")
    async def close(session_id: str, uid: str = Depends(user_id)):
        root = await require(session_id, uid)
        try:
            await (await temporal()).get_workflow_handle(session_id).signal("close")
        except RPCError as exc:
            if exc.status != RPCStatusCode.NOT_FOUND:
                raise
        if root == session_id:
            await mark_closed(session_id, uid)
        return JSONResponse({"ok": True}, headers=NO_STORE)

    # -- uploads and tool output ---------------------------------------------------------

    @router.post("/sessions/{session_id}/attachments")
    async def upload_attachment(session_id: str, file: UploadFile = File(...), uid: str = Depends(user_id)):
        root = await require(session_id, uid)
        if app_store is None:
            raise HarnessError(501, "not_implemented", "Uploads aren't configured here.")
        from simple_chat_agent.common.attachments import (
            AttachmentValidationError,
            attachment_dict,
            create_user_attachment,
        )

        # Read one byte past the limit, never the whole (possibly huge) part.
        content = await file.read(MAX_UPLOAD_BYTES + 1)
        if len(content) > MAX_UPLOAD_BYTES:
            raise HarnessError(413, "too_large", f"Attachments are limited to {MAX_UPLOAD_BYTES} bytes.")
        try:
            # workflow_id = the session id: read_attachment and import_attachment_to_workspace
            # look attachments up by (owner_id, session_id).
            artifact = await asyncio.to_thread(
                create_user_attachment,
                app_store(),
                user_id=uid,
                conversation_id=root,
                workflow_id=root,
                name=file.filename or "attachment",
                content=content,
                mime_type=file.content_type,
                upload_kind="file",
            )
        except AttachmentValidationError as exc:
            raise HarnessError(400, "invalid_attachment", str(exc)) from None
        return JSONResponse({"attachment": attachment_dict(artifact)}, headers=NO_STORE)

    @router.get("/sessions/{session_id}/logs/{log_ref}")
    async def tool_output(
        session_id: str,
        log_ref: str,
        stream: Literal["stdout", "stderr", "both"] = "both",
        offset: int = 0,
        limit: int = 262_144,
        uid: str = Depends(user_id),
    ):
        # Subagents share their root's workspace; the workspace id is the root session id.
        root = await require(session_id, uid)
        if read_log_fn is None:
            raise HarnessError(501, "not_implemented", "Tool output lookup isn't available yet.")
        try:
            page = await read_log_fn(root, log_ref, stream=stream, offset=offset, limit=limit)
        except ExecutionLogNotFound:
            raise HarnessError(404, "output_expired", "Output no longer retained.") from None
        except ExecutorRequestError:
            log.exception("reading log %s of %s failed", log_ref, root)
            raise HarnessError(502, "executor_unavailable", "The sandbox isn't answering; try again shortly.") from None
        return JSONResponse(page, headers=NO_STORE)

    # -- the harness transport -----------------------------------------------------------

    @router.get("/agent-interface/{session_id}")
    async def agent_interface(session_id: str, uid: str = Depends(user_id)):
        root = await require(session_id, uid)
        workflow_type = AGENT_WORKFLOW_TYPE
        if root != session_id:
            # A subagent: its handler set depends on its type, which Describe (no worker) gives.
            try:
                workflow_type = (await (await temporal()).get_workflow_handle(session_id).describe()).workflow_type
            except RPCError as exc:
                if exc.status != RPCStatusCode.NOT_FOUND:
                    raise
                raise not_found() from None
        functions = agent_interface_for(workflow_type)
        if functions is None:
            raise HarnessError(501, "not_implemented", f"No interface is known for {workflow_type}.")
        return JSONResponse(list(functions), headers={"Cache-Control": "private, max-age=300"})

    @router.get("/attach")
    async def attach(session_id: str, from_offset: int = 0, uid: str = Depends(user_id)):
        root = await require(session_id, uid)
        b = await bridge()
        # A subagent's frames are already in its root's stream (the harness merge): serve
        # those, filtered to the subagent, from the root's forwarder. Never a second Temporal
        # reader for the child.
        child = session_id if root != session_id else None
        return StreamingResponse(
            _reading(b, root, _frames(store, root, from_offset, child=child)),
            media_type="text/event-stream",
            headers=SSE_HEADERS,
        )

    async def checked_attachments(attachments: Any, uid: str, session_id: str) -> list[dict[str, Any]]:
        """A chat message's attachments, each one this user's upload to this session, with a
        plain id, a MIME-shaped content type and a name without control characters. 422
        otherwise. Returns the cleaned list that goes to the workflow."""
        if not isinstance(attachments, list):
            raise HarnessError(422, "invalid_attachment", "attachments must be a list.")
        from simple_chat_agent.common.attachments import artifact_is_user_attachment

        cleaned = []
        for item in attachments:
            if not isinstance(item, dict):
                raise HarnessError(422, "invalid_attachment", "Each attachment must be an object.")
            attachment_id = item.get("attachment_id")
            if not isinstance(attachment_id, str) or not _ATTACHMENT_ID.fullmatch(attachment_id):
                raise HarnessError(422, "invalid_attachment", "attachment_id must be 1-128 of [A-Za-z0-9_-].")
            content_type = item.get("content_type")
            if content_type is not None and (not isinstance(content_type, str) or not _MIME_TYPE.fullmatch(content_type)):
                raise HarnessError(422, "invalid_attachment", "content_type must be a MIME type.")
            name = item.get("name", "")
            if not isinstance(name, str):
                raise HarnessError(422, "invalid_attachment", "name must be a string.")
            artifact = None
            if app_store is not None:
                artifact = await asyncio.to_thread(app_store().get_artifact, user_id=uid, artifact_id=attachment_id)
            if artifact is None or artifact.workflow_id != session_id or not artifact_is_user_attachment(artifact):
                # Same answer for someone else's id, another session's and an unknown one.
                raise HarnessError(422, "invalid_attachment", "That attachment isn't part of this session.")
            cleaned.append({**item, "name": _CONTROL_CHARS.sub("", name).strip()})
        return cleaned

    @router.post("/messages")
    async def submit_message(body: SubmitMessage, uid: str = Depends(user_id)):
        check_request_id(body.request_id)
        root = await require(body.session_id, uid)
        if root != body.session_id:
            # Subagents have no front door; their parent drives them.
            raise HarnessError(409, "not_supported", "Messages go to the session, not to a subagent.")
        message_payload = body.message.get("payload") or {}
        if body.message.get("type") == "chat" and isinstance(message_payload, dict) and "attachments" in message_payload:
            message_payload = {
                **message_payload,
                "attachments": await checked_attachments(message_payload["attachments"], uid, body.session_id),
            }
        # set_posture / set_model get the same checks as POST sessions. A malformed payload is
        # left to the workflow's own validation.
        kind = body.message.get("type")
        payload = body.message.get("payload")
        new_model: ModelChoice | None = None
        if kind == "set_posture" and isinstance(payload, dict) and isinstance(payload.get("posture"), str):
            refusal = posture_refusal(payload["posture"], agent_environment(), auto_mode_status(configured_judge_backend()))
            if refusal is not None:
                raise HarnessError(422, "posture_not_allowed", refusal)
        elif kind == "set_web_search" and isinstance(payload, dict) and payload.get("enabled") is True:
            if not web_search_allowed(agent_environment()):
                raise HarnessError(422, "web_search_not_allowed", "Web search isn't available here.")
        elif kind == "set_model" and isinstance(payload, dict):
            try:
                new_model = ModelChoice.model_validate(payload.get("model"))
            except ValidationError:
                new_model = None
            if new_model is not None and not agent_models.is_allowed(new_model):
                raise HarnessError(422, "model_not_allowed", f"{new_model.registry_key} is not available.")
        message_id = body.request_id or str(uuid.uuid4())
        handle = (await temporal()).get_workflow_handle(body.session_id)
        try:
            await handle.signal(
                ENQUEUE_MESSAGE_SIGNAL,
                FrontDoorMessage(
                    request_id=message_id,
                    type=str(body.message.get("type", "")),
                    payload=message_payload,
                ),
            )
        except RPCError as exc:
            if exc.status == RPCStatusCode.NOT_FOUND:
                raise not_found("The session has ended.") from None
            raise
        await store.note_sent(body.session_id, message_id)
        # The session list changes when the agent applies the change, not here: the bridge
        # copies each set_model / set_posture result into it (session_settings_updater), so a
        # change the worker refuses never shows. Until then (workers down) it shows the old value.
        await _wake(await bridge(), body.session_id)
        meta = await store.meta(body.session_id)
        # The real turn number and disposition arrive on the stream with message_accepted;
        # until then the message is recorded in Temporal and waiting for a worker.
        return JSONResponse(
            {
                "message_id": message_id,
                "turn_id": "",
                "turn_number": 0,
                "accepted_offset": meta.head,
                "disposition": "queued",
                "landed": True,
            },
            headers=NO_STORE,
        )

    @router.post("/approve")
    async def approve(body: Approve, uid: str = Depends(user_id)):
        check_request_id(body.request_id)
        root = await require(body.session_id, uid)
        if root != body.session_id:
            await approve_child(body, root)
            return JSONResponse({"tool_id": body.tool_id, "accepted": True}, headers=NO_STORE)
        handle = (await temporal()).get_workflow_handle(body.session_id)
        try:
            await handle.signal(
                ENQUEUE_APPROVAL_SIGNAL,
                FrontDoorApproval(
                    request_id=body.request_id or str(uuid.uuid4()),
                    tool_id=body.tool_id,
                    approved=body.approved,
                    reason=body.reason,
                    remember=body.remember,
                ),
            )
        except RPCError as exc:
            if exc.status == RPCStatusCode.NOT_FOUND:
                raise not_found("The session has ended.") from None
            raise
        await _wake(await bridge(), body.session_id)
        return JSONResponse({"tool_id": body.tool_id, "accepted": True}, headers=NO_STORE)

    async def approve_child(body: Approve, root: str) -> None:
        """A subagent's tool approval goes to the child's own harness ``tool_approval`` Update
        (subagents have no Signal front door). That needs a worker: with none answering in
        CHILD_APPROVAL_TIMEOUT, 503. No outbox for children in this release."""
        handle = (await temporal()).get_workflow_handle(body.session_id)
        try:
            await handle.execute_update(
                TOOL_APPROVAL_UPDATE,
                ToolApprovalDecision(
                    tool_id=body.tool_id, approved=body.approved, reason=body.reason, remember=body.remember
                ),
                # Stable across retries: a retry after a 503 whose first attempt did apply
                # dedups to the same Update instead of failing as already decided.
                id=body.request_id or f"approve:{body.tool_id}",
                rpc_timeout=CHILD_APPROVAL_TIMEOUT,
            )
        except WorkflowUpdateRPCTimeoutOrCancelledError:
            raise HarnessError(
                503, "worker_unavailable", "The subagent isn't running right now; try the approval again shortly."
            ) from None
        except WorkflowUpdateFailedError as exc:
            log.info("subagent approval %s on %s refused: %s", body.tool_id, body.session_id, exc.cause)
            raise HarnessError(409, "tool_approval_error", "That approval is unknown or already decided.") from None
        except RPCError as exc:
            if exc.status == RPCStatusCode.NOT_FOUND:
                raise not_found("The subagent has ended.") from None
            if exc.status in (RPCStatusCode.DEADLINE_EXCEEDED, RPCStatusCode.CANCELLED, RPCStatusCode.UNAVAILABLE):
                raise HarnessError(
                    503, "worker_unavailable", "The subagent isn't running right now; try the approval again shortly."
                ) from None
            raise
        await _wake(await bridge(), root)

    def upload_key(session_id: str, tool_id: str) -> str:
        # Which uploads/ paths this tool call's picker wrote: a pick_files answer may name only these.
        return f"{prefix()}:cb-uploads:{session_id}:{hashlib.sha256(tool_id.encode()).hexdigest()[:32]}"

    def check_tool_id(tool_id: str) -> None:
        if not _TOOL_ID.fullmatch(tool_id or ""):
            raise HarnessError(422, "invalid_tool_id", "tool_id must be 1-200 of [A-Za-z0-9_.:/-].")

    @router.post("/sessions/{session_id}/callbacks/{tool_id}/files")
    async def upload_picked_file(
        session_id: str, tool_id: str, file: UploadFile = File(...), uid: str = Depends(user_id)
    ):
        """One file the user chose for a ``pick_files`` call: written into the session's
        workspace as ``uploads/<safe name>`` (never overwriting; ``-1``, ``-2``... on a clash).
        At most PICK_MAX_FILES per call and PICK_MAX_FILE_BYTES each. Returns the path, which
        the browser then sends back in the callback result."""
        check_tool_id(tool_id)
        root = await require_root(session_id, uid)
        content = await file.read(PICK_MAX_FILE_BYTES + 1)
        if len(content) > PICK_MAX_FILE_BYTES:
            raise HarnessError(413, "too_large", f"Each file is limited to {PICK_MAX_FILE_BYTES} bytes.")
        if (await store.meta(root)).closed:
            raise HarnessError(409, "session_closed", "This session is closed. Start a new one.")
        # Only while that pick_files call waits: the bridge mirrors open calls from
        # callback_requested / callback_resolved into Redis (P5), so this needs no Query.
        if not await store.pick_open(root, tool_id):
            raise HarnessError(409, "not_waiting", "That file request isn't waiting for files any more.")
        key = upload_key(root, tool_id)
        # Reserve before writing, atomically (INCR), so parallel uploads can't pass the caps
        # together. The per-session caps also bound what one session can write in total.
        ttl = session_ttl_seconds()
        counters = (
            (f"{key}:n", 1, PICK_MAX_FILES, "too_many_files", f"At most {PICK_MAX_FILES} files can be picked at once."),
            (f"{prefix()}:cb-uploads-n:{root}", 1, PICK_SESSION_MAX_FILES, "session_upload_limit",
             f"This session has reached its limit of {PICK_SESSION_MAX_FILES} picked files."),
            (f"{prefix()}:cb-uploads-bytes:{root}", len(content), PICK_SESSION_MAX_BYTES, "session_upload_limit",
             "This session has reached its limit for picked files."),
        )
        taken: list[tuple[str, int]] = []

        async def release() -> None:
            for counter, amount in taken:
                await store.r.decrby(counter, amount)

        for counter, amount, cap, code, message in counters:
            total = await store.r.incrby(counter, amount)
            await store.r.expire(counter, ttl)
            taken.append((counter, amount))
            if total > cap:
                await release()
                raise HarnessError(409, code, message)
        name = upload_file_name(file.filename)
        for attempt in range(10):
            path = f"{PICK_UPLOAD_DIR}/{name if attempt == 0 else _numbered(name, attempt)}"
            try:
                await write_upload_fn(root, path, content)
                break
            except FileExistsError:
                continue
            except Exception:  # noqa: BLE001 - executor refused or unreachable
                await release()
                log.exception("pick_files upload to %s failed", root)
                raise HarnessError(502, "upload_failed", "Couldn't put the file in the workspace. Try again.") from None
        else:
            await release()
            raise HarnessError(409, "name_taken", "Too many files with that name; rename it and try again.")
        await store.r.sadd(key, path)
        await store.r.expire(key, ttl)
        return JSONResponse(
            {"path": path, "name": (file.filename or "")[:PICK_MAX_NAME_CHARS * 2], "size": len(content)},
            headers=NO_STORE,
        )

    @router.post("/callback-result")
    async def callback_result(body: CallbackResult, uid: str = Depends(user_id)):
        """The browser's answer to an ``ask_user`` / ``pick_files`` call, through the Signal
        front door (lands with or without a worker). The agent checks it against the question
        (offered option ids; picked paths and count) and refuses a bad one as ``x_rejected``
        (kind ``callback``); the card stays open."""
        check_request_id(body.request_id)
        check_tool_id(body.tool_id)
        root = await require(body.session_id, uid)
        if root != body.session_id:
            raise HarnessError(409, "not_supported", "Callback answers go to the session, not to a subagent.")
        if len(json.dumps(body.result, default=str)) > CALLBACK_RESULT_MAX_BYTES:
            raise HarnessError(413, "too_large", "That answer is too large.")
        error = _CONTROL_CHARS.sub(" ", body.error).strip() or "The user declined." if body.error is not None else None
        if error is None and isinstance(body.result, dict) and "paths" in body.result:
            # A pick_files answer names only files this call's picker uploaded.
            paths = body.result.get("paths")
            if not isinstance(paths, list) or not all(isinstance(p, str) for p in paths):
                raise HarnessError(422, "invalid_result", "paths must be a list of strings.")
            uploaded = await store.r.smembers(upload_key(root, body.tool_id)) if paths else set()
            if any(p not in uploaded for p in paths):
                raise HarnessError(422, "unknown_upload", "Every path must be a file uploaded for this request.")
        request_id = body.request_id or str(uuid.uuid4())
        handle = (await temporal()).get_workflow_handle(body.session_id)
        try:
            await handle.signal(
                ENQUEUE_CALLBACK_SIGNAL,
                FrontDoorCallbackResult(
                    request_id=request_id, tool_id=body.tool_id, result=body.result, error=error
                ),
            )
        except RPCError as exc:
            if exc.status == RPCStatusCode.NOT_FOUND:
                raise not_found("The session has ended.") from None
            raise
        await _wake(await bridge(), body.session_id)
        return JSONResponse({"tool_id": body.tool_id, "accepted": True, "request_id": request_id}, headers=NO_STORE)

    @router.get("/status/{session_id}")
    async def status(session_id: str, uid: str = Depends(user_id)):
        await require(session_id, uid)
        meta = await store.meta(session_id)
        # run_id: recorded at create, then by the bridge from Describe (follows continue-as-new).
        return JSONResponse(meta.to_status(), headers=NO_STORE)

    @router.get("/workflow-status/{session_id}")
    async def workflow_status(session_id: str, uid: str = Depends(user_id)):
        root = await require(session_id, uid)
        try:
            desc = await (await temporal()).get_workflow_handle(session_id).describe()
        except RPCError as exc:
            if exc.status != RPCStatusCode.NOT_FOUND:
                raise
            if root == session_id:
                await mark_closed(session_id, uid)
            return JSONResponse(
                {"workflow_id": session_id, "execution_status": "NOT_FOUND", "closed": True}, headers=NO_STORE
            )
        status = desc.status or WorkflowExecutionStatus.RUNNING
        closed = _is_closed(status)
        if closed and root == session_id:
            await mark_closed(session_id, uid)
        return JSONResponse(
            {"workflow_id": session_id, "execution_status": status.name, "closed": closed},
            headers=NO_STORE,
        )

    return router


def session_settings_updater(
    directory: SessionDirectory, store: AgentStore
) -> Callable[[str, dict[str, Any]], Awaitable[None]]:
    """The bridge's ``on_settings``: copy a set_model / set_posture / set_web_search result
    (``SettingsAck``)
    into the owner's session list. The ack carries the value the agent holds, so a refused
    change puts the previous value back."""

    async def update(session_id: str, ack: dict[str, Any]) -> None:
        owner = await store.owner_of(session_id)
        if owner is None:
            return
        if ack.get("setting") == "model" and ack.get("model"):
            await directory.update_settings(owner, session_id, model=ModelChoice.model_validate(ack["model"]))
        elif ack.get("setting") == "posture" and ack.get("posture"):
            await directory.update_settings(owner, session_id, posture=str(ack["posture"]))
        elif ack.get("setting") == "web_search" and isinstance(ack.get("web_search"), bool):
            await directory.update_settings(owner, session_id, web_search=ack["web_search"])

    return update


class AgentApi:
    """Everything the agent routes need in one app: a lazily connected Temporal client (with
    the agent plugins), the Redis store, the bridge and the session directory. ``aclose`` from
    the app's lifespan."""

    def __init__(
        self,
        *,
        current_user: Callable[[Request], Any],
        task_queue: str | None = None,
        connect: Callable[[], Awaitable[Client]] = connect_agent_client,
        store: AgentStore | None = None,
    ) -> None:
        self.current_user = current_user
        self.task_queue = task_queue or agent_task_queue()
        self.store = store or AgentStore.from_env()
        self._connect = connect
        self._client: Client | None = None
        self._bridge: Bridge | None = None
        self._lock = asyncio.Lock()
        self.directory = SessionDirectory(self.store.r, temporal=self.temporal, task_queue=self.task_queue)

    async def temporal(self) -> Client:
        if self._client is None:
            async with self._lock:
                if self._client is None:
                    self._client = await self._connect()
        return self._client

    async def bridge(self) -> Bridge:
        if self._bridge is None:
            client = await self.temporal()  # takes the lock itself; not re-entrant
            async with self._lock:
                if self._bridge is None:
                    self._bridge = Bridge(
                        client, self.store, on_settings=session_settings_updater(self.directory, self.store)
                    )
        return self._bridge

    def router(self, *, app_store: Callable[[], Any] | None = None) -> APIRouter:
        return create_agent_router(
            temporal=self.temporal,
            store=self.store,
            bridge=self.bridge,
            current_user=self.current_user,
            task_queue=self.task_queue,
            directory=self.directory,
            app_store=app_store,
        )

    async def aclose(self) -> None:
        if self._bridge is not None:
            await self._bridge.aclose()


async def _wake(bridge: Any, session_id: str) -> None:
    """New work for the session: wake whichever replica forwards it (Bridge.wake publishes
    across replicas; a bridge without it only starts the local forwarder)."""
    wake = getattr(bridge, "wake", None)
    if wake is not None:
        await wake(session_id)
    else:
        bridge.ensure(session_id)


async def _reading(bridge: Any, session_id: str, frames: AsyncIterator[bytes]) -> AsyncIterator[bytes]:
    """Serve ``frames`` while registered as a reader of the session on this replica
    (Bridge.reading), so the local forwarder stands by to take over while anyone reads here."""
    reading = getattr(bridge, "reading", None)
    if reading is None:
        bridge.ensure(session_id)
        async for chunk in frames:
            yield chunk
        return
    async with reading(session_id):
        async for chunk in frames:
            yield chunk


def _belongs(agent_id: Any, subagent: str) -> bool:
    """A frame from the subagent itself or from one of its descendants (agent ids extend
    their parent's with ``-<segment>``)."""
    return isinstance(agent_id, str) and (agent_id == subagent or agent_id.startswith(subagent + "-"))


async def _subagent_agent_id(store: AgentStore, root: str, child: str) -> str | None:
    """The merged-stream ``agent_id`` of subagent workflow ``child``: the store's registry
    (filled by the bridge from ``subagent_started``), else a scan of the root's stored frames
    for that ``subagent_started``. None while the subagent hasn't started yet; ``_frames``
    then picks it up from the stream as it arrives."""
    known = await store.agent_id_of(child)
    if known:
        return known
    after = "0-0"
    while True:
        frames = await store.read_backfilled(root, after=after, count=500)
        if not frames:
            return None
        for frame in frames:
            if frame.event == "subagent_started" and frame.data.get("workflow_id") == child:
                agent_id = frame.data.get("subagent_id")
                if agent_id:
                    # Remembered in the store's registry, so the next attach skips the scan.
                    await store.register_subagent(child, root, agent_id=agent_id)
                    return agent_id
        after = frames[-1].entry_id


async def _frames(
    store: AgentStore, session_id: str, from_offset: int, *, child: str | None = None
) -> AsyncIterator[bytes]:
    """Replay frames from ``from_offset`` from Redis, then tail until the agent is idle and
    every frame is delivered (client transport.ts ``attach``: "Ends when the agent is idle and
    caught up").

    ``from_offset`` is the client's last ``resume_offset``. Replay INCLUDES the frames at that
    position: several frames share one offset (subagent frames, and telemetry such as the
    confirmed ``x_usage`` appended at the newest frame's position after ``turn_end``), and
    the client deduplicates what it already has (root frames by ``agent_id|event_offset``,
    others by content). While a turn is open (including while no worker is running), or while
    the bridge is still confirming a finished turn's telemetry (``meta.confirming``), the
    stream stays open with keep-alive comments rather than ending, since the client treats an
    attach that ends mid-turn as a dropped connection.

    With ``child`` (a subagent workflow id), ``session_id`` is its root and only frames from
    that subagent (and its descendants) are sent, with their root ``resume_offset``s.
    """
    after = "0-0" if from_offset <= 0 else f"{from_offset}-{MAX_SEQ}"
    subagent = await _subagent_agent_id(store, session_id, child) if child else None
    loop = asyncio.get_running_loop()
    last_sent = loop.time()
    # Redis's first entry id when read_backfilled last came back empty (caught up, archive
    # exhausted past ``after``); _UNSET until then. Only a change in it (Redis gets its first
    # entry, or is rebuilt) sends a read back through the archive.
    archive_done: Any = _UNSET
    while True:
        # Meta first, frames second. The bridge appends a frame before it saves the meta that
        # frame produces, so an idle meta read here guarantees the read below already sees the
        # turn_end. Reading frames first could miss a turn_end appended between the two reads
        # and end the stream with the client's turn still open, which the client treats as a
        # dropped connection (client session.ts: "a stream that ends with a turn still open
        # was cut off").
        meta = await store.meta(session_id)
        # Every non-blocking read goes through read_backfilled (store contract): from the
        # turn archive while ``after`` is behind what Redis holds (Redis expired, lost or
        # rebuilt), then from Redis. Only an empty result means caught up; only then block.
        if archive_done is not _UNSET and await store.first_entry_id(session_id) == archive_done:
            # The archive had nothing past ``after`` and Redis's stream hasn't (re)started
            # since: read Redis alone, so an idle poll never lists the archive again.
            frames = await store.read(session_id, after=after, count=READ_PAGE)
        else:
            frames = await store.read_backfilled(session_id, after=after, count=READ_PAGE)
            archive_done = await store.first_entry_id(session_id) if not frames else _UNSET
        if not frames:
            if not meta.turn_active and not meta.pending and not meta.confirming:
                return
            frames = await store.wait(session_id, after=after, block_ms=1000)
        for frame in frames:
            after = frame.entry_id
            if child is not None:
                if frame.event == "subagent_started" and frame.data.get("workflow_id") == child:
                    subagent = frame.data.get("subagent_id") or subagent
                if subagent is None or not _belongs(frame.data.get("agent_id"), subagent):
                    continue
            payload = {**frame.data, "resume_offset": frame.resume_offset}
            yield f"event: {frame.event}\ndata: {json.dumps(payload)}\n\n".encode()
            last_sent = loop.time()
        if loop.time() - last_sent > KEEPALIVE_SECONDS:
            yield b": keep-alive\n\n"
            last_sent = loop.time()
