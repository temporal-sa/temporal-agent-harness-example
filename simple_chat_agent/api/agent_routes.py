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
from urllib.parse import urlsplit
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

from simple_chat_agent.agent.approvals import configured_judge_backend
from simple_chat_agent.agent.contracts import (
    AGENT_WORKFLOW_TYPE,
    ENQUEUE_APPROVAL_SIGNAL,
    ENQUEUE_MESSAGE_SIGNAL,
    Ack,
    ChatMessage,
    ChatReply,
    CodingAgentInit,
    Findings,
    FrontDoorApproval,
    FrontDoorMessage,
    InterruptRequest,
    Question,
    Review,
    ReviewRequest,
    ModelChoice,
    Posture,
    SetModel,
    SetPosture,
    SteerMessage,
)
# Key-free: the executor log reader is a plain HTTP helper (no tools runtime, no LLM keys).
from simple_chat_agent.agent.tools.workspace import ExecutionLogNotFound, ExecutorRequestError, read_execution_log
from simple_chat_agent.common.attachments import MAX_ATTACHMENT_BYTES
from simple_chat_agent.api import agent_models
from simple_chat_agent.api.agent_bridge import Bridge
from simple_chat_agent.api.agent_store import MAX_SEQ, AgentStore, prefix, session_ttl_seconds

log = logging.getLogger(__name__)

KEEPALIVE_SECONDS = 15.0
READ_PAGE = 500
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
TITLE_MAX = 200
# How long a subagent tool approval waits for a worker to take the harness Update.
CHILD_APPROVAL_TIMEOUT = timedelta(seconds=10)
# The envelope allows for multipart framing around the largest allowed attachment.
MAX_UPLOAD_BYTES = MAX_ATTACHMENT_BYTES
UPLOAD_ENVELOPE_BYTES = 64 * 1024
SESSIONS_LIST_MAX = 500
# Rebuilding a lost session list Queries CodingSessions (server-side, rare); at most this often.
REBUILD_BACKOFF_SECONDS = 600
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


def agent_task_queue() -> str:
    return os.environ.get("CODING_AGENT_TASK_QUEUE", "coding-agent")


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
    (else ``Referer``) is this host or ``SIMPLE_CHAT_PUBLIC_URL``. Browsers always send
    ``Origin`` on a cross-site POST, and can't add a custom header without a CORS preflight,
    which this API never grants."""
    if request.headers.get(CSRF_HEADER):
        return True
    source = request.headers.get("origin")
    if not source or source == "null":
        source = None if source == "null" else request.headers.get("referer")
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

        multipart = self.path.endswith("/attachments")

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
            try:
                return await handler(request)
            except HarnessError as exc:
                return _error(exc.status, exc.code, exc.message, **exc.extra)
            except RequestValidationError as exc:
                errors = jsonable_encoder(exc.errors())
                first = errors[0] if errors else {}
                where = ".".join(str(p) for p in first.get("loc", ()) if p != "body")
                message = f"{where}: {first.get('msg')}" if where else str(first.get("msg") or "Invalid request.")
                return _error(422, "invalid_request", message, errors=errors)
            except HTTPException as exc:
                code = _HTTP_CODES.get(exc.status_code, "http_error")
                return _error(exc.status_code, code, str(exc.detail), headers=exc.headers)

        return wrapped


# -- request bodies ----------------------------------------------------------------------


class CreateSession(BaseModel):
    """Our create body, also accepted as the harness ``CreateSessionRequest.data``."""

    model_config = ConfigDict(extra="forbid")

    model: ModelChoice | None = None
    posture: Posture = agent_models.DEFAULT_POSTURE
    title: str | None = Field(default=None, max_length=TITLE_MAX)
    demo: Literal["refund_service"] | None = None


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
    error: str | None = None


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
        Ack,
        "Switch the provider and/or model. Takes effect from the next turn.",
        MidTurn.ACCEPT,
        False,
    ),
    (
        "set_posture",
        SetPosture,
        Ack,
        "Change how tool calls are approved. Applies to new calls; pending ones stay with you.",
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
            "Investigate a question about the code and report findings with evidence.",
            MidTurn.REJECT,
            True,
        ),
    ),
    "ReviewerAgent": (
        (
            "review",
            ReviewRequest,
            Review,
            "Review the current diff and return a verdict with concrete issues.",
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
    keeps the list in Redis only.
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

    async def mark_closed(self, user_id: str, session_id: str) -> None:
        await self._update_record(user_id, session_id, closed=True)

    async def remove(self, user_id: str, session_id: str) -> None:
        index, records = self._keys(user_id)
        pipe = self.r.pipeline(transaction=True)
        pipe.zrem(index, session_id)
        pipe.hdel(records, session_id)
        await pipe.execute()
        await self._send(user_id, "remove_session_signal", session_id)

    async def _rebuild(self, user_id: str) -> bool:
        """The Redis list is gone (expired or Redis lost): rebuild it from the user's
        ``CodingSessions`` (its ``list_sessions`` Query). A rare, server-side path; at most
        once per REBUILD_BACKOFF_SECONDS per user, so a user with no sessions costs no Query."""
        if not self._sync:
            return False
        if not await self.r.set(self._rebuilt_key(user_id), "1", nx=True, ex=REBUILD_BACKOFF_SECONDS):
            return False
        from simple_chat_agent.agent.sessions import SessionRecord, sessions_workflow_id

        try:
            assert self._temporal is not None
            client = await self._temporal()
            handle = client.get_workflow_handle(sessions_workflow_id(owner_digest(user_id)))
            found = await handle.query(
                "list_sessions", result_type=list[SessionRecord], rpc_timeout=REBUILD_QUERY_TIMEOUT
            )
        except Exception as exc:  # noqa: BLE001 - no manager yet, or no worker: nothing to rebuild
            log.info("session directory rebuild for %s skipped: %s", owner_digest(user_id), exc)
            return False
        if not found:
            return False
        index, records = self._keys(user_id)
        pipe = self.r.pipeline(transaction=True)
        for rec in found:
            if getattr(rec, "archived", False):
                continue
            try:
                created = datetime.fromisoformat(rec.created_at).timestamp() if rec.created_at else 0.0
            except ValueError:
                created = 0.0
            provider, _, model = (rec.model or "").partition(":")
            record = {
                "session_id": rec.session_id,
                "title": rec.title,
                "created_at": created,
                "model": rec.model,
                "model_choice": {"provider": provider, "model": model} if model else {},
                "rebuilt": True,
            }
            pipe.zadd(index, {rec.session_id: created}, nx=True)
            pipe.hsetnx(records, rec.session_id, json.dumps(record))
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
            sessions_workflow_id,
        )

        if signal == "register_session_signal":
            payload: Any = SessionRecord(
                session_id=arg["session_id"],
                title=arg.get("title") or "New session",
                model=arg.get("model", ""),
                created_at=datetime.fromtimestamp(float(arg["created_at"]), timezone.utc).isoformat(),
            )
        elif signal == "rename_session_signal":
            payload = RenameRequest(**arg)
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


async def claim_owner(store: AgentStore, session_id: str, user_id: str) -> bool:
    """Record ``user_id`` as the session's owner unless someone already is (SET NX). True when
    this call set it."""
    return await store.claim_owner(session_id, user_id)


def _is_closed(status: WorkflowExecutionStatus | None) -> bool:
    return status is not None and status not in (
        WorkflowExecutionStatus.RUNNING,
        WorkflowExecutionStatus.CONTINUED_AS_NEW,
    )


async def closed_flags(store: AgentStore, session_ids: list[str]) -> list[bool]:
    """``SessionMeta.closed`` for many sessions in one Redis round trip (the list routes)."""
    return [m.closed for m in await store.metas(session_ids)]


async def mark_meta_closed(store: AgentStore, session_id: str) -> None:
    await store.mark_closed(session_id)



SeedFn = Callable[[str, str], Awaitable[Any]]
ReadLogFn = Callable[..., Awaitable[dict[str, Any]]]


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

    def user_id(request: Request) -> str:
        return current_user(request).user_id

    async def require(workflow_id: str, uid: str) -> str:
        root = await owners.root_if_owned(workflow_id, uid)
        if root is None:
            raise not_found()
        return root

    async def mark_closed(session_id: str, uid: str) -> None:
        """The session's workflow closed (or was asked to): ``GET sessions`` shows it."""
        await mark_meta_closed(store, session_id)
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
        return JSONResponse(
            agent_models.catalog(
                environment=agent_environment(),
                research_tools=offered_research_tools(),
                judge=configured_judge_backend(),
            ),
            headers=NO_STORE,
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
        if body.posture not in agent_models.postures_for(environment):
            raise HarnessError(422, "posture_not_allowed", "\"Never ask\" isn't available here.")

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
        claimed = await claim_owner(store, session_id, uid)
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
        )
        title = (body.title or "").strip() or (
            agent_models.DEMOS[body.demo] if body.demo else "New session"
        )
        seeded: dict[str, Any] | None = None
        if body.demo:
            # Before the workflow starts, so the first message already sees the repo, and a
            # failed seed leaves no session behind. Once per workspace: a retry after the seed
            # succeeded but the start failed skips it.
            marker = f"{prefix()}:seeded:{session_id}"
            done = await store.r.get(marker)
            if done:
                seeded = json.loads(done)
            else:
                try:
                    result = await seed_fn(session_id, body.demo)
                except Exception:  # noqa: BLE001 - executor refused or unreachable
                    log.exception("seeding %s for %s failed", body.demo, session_id)
                    raise HarnessError(
                        502, "seed_failed", "Couldn't set up the demo workspace. Try again in a moment."
                    ) from None
                seeded = {
                    "fixture": body.demo,
                    "files": len(getattr(result, "files", []) or []),
                    "commit": getattr(result, "commit", ""),
                }
                await store.r.set(marker, json.dumps(seeded), ex=session_ttl_seconds())
        client = await temporal()
        try:
            await client.start_workflow(
                AGENT_WORKFLOW_TYPE,
                args=[AgentConfig(), data],
                id=session_id,
                task_queue=task_queue,
                search_attributes=owner_search_attributes(uid),
                id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
                static_summary=title[:200],
            )
        except WorkflowAlreadyStartedError:
            # A concurrent create of the same caller-chosen id won the start.
            existing = await existing_summary(session_id, uid)
            if existing is not None:
                return JSONResponse(existing, headers=NO_STORE)
            raise HarnessError(409, "session_exists", "That session id is taken.") from None
        record = {
            "session_id": session_id,
            "title": title,
            "created_at": time.time(),
            "model": model.registry_key,
            "model_choice": model.model_dump(mode="json", exclude_none=True),
            "posture": body.posture,
            "environment": environment,
            "demo": body.demo,
        }
        await sessions.put(uid, record)
        summary = _summary(record)
        if seeded is not None:
            summary["seed"] = seeded
        return JSONResponse(summary, headers=NO_STORE)

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
        await mark_meta_closed(store, session_id)
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

    @router.post("/messages")
    async def submit_message(body: SubmitMessage, uid: str = Depends(user_id)):
        root = await require(body.session_id, uid)
        if root != body.session_id:
            # Subagents have no front door; their parent drives them.
            raise HarnessError(409, "not_supported", "Messages go to the session, not to a subagent.")
        message_id = body.request_id or str(uuid.uuid4())
        handle = (await temporal()).get_workflow_handle(body.session_id)
        try:
            await handle.signal(
                ENQUEUE_MESSAGE_SIGNAL,
                FrontDoorMessage(
                    request_id=message_id,
                    type=str(body.message.get("type", "")),
                    payload=body.message.get("payload") or {},
                ),
            )
        except RPCError as exc:
            if exc.status == RPCStatusCode.NOT_FOUND:
                raise not_found("The session has ended.") from None
            raise
        await store.note_sent(body.session_id, message_id)
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
                id=body.request_id or None,
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

    @router.post("/callback-result")
    async def callback_result(body: CallbackResult, uid: str = Depends(user_id)):
        await require(body.session_id, uid)
        raise HarnessError(501, "not_implemented", "Callback tools arrive in P4.")

    @router.get("/status/{session_id}")
    async def status(session_id: str, uid: str = Depends(user_id)):
        await require(session_id, uid)
        meta = await store.meta(session_id)
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
                    self._bridge = Bridge(client, self.store)
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


def _subagent_key(child: str) -> str:
    """The Redis key of a subagent's merged-stream agent id. The store owns it now
    (``AgentStore.agent_id_of`` / ``register_subagent``); kept because
    tests/test_agent_bridge.py pins the two to the same key."""
    return f"{prefix()}:subaid:{{{child}}}"


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
        frames = await store.read_backfilled(session_id, after=after, count=READ_PAGE)
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
