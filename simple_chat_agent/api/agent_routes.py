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
* ``agent-interface`` is built from the agent's contract models, not Queried.
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
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

from fastapi import APIRouter, Depends, File, HTTPException, Request, UploadFile
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response, StreamingResponse
from fastapi.routing import APIRoute
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.common import (
    SearchAttributeKey,
    SearchAttributePair,
    TypedSearchAttributes,
    WorkflowIDConflictPolicy,
    WorkflowIDReusePolicy,
)
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.service import RPCError, RPCStatusCode

from temporal_agent_harness.harness.agent_protocol import AgentConfig, MidTurn
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
    FrontDoorApproval,
    FrontDoorMessage,
    InterruptRequest,
    ModelChoice,
    Posture,
    SetModel,
    SetPosture,
    SteerMessage,
)
# Key-free: the executor log reader is a plain HTTP helper (no tools runtime, no LLM keys).
from simple_chat_agent.agent.tools.workspace import ExecutionLogNotFound, ExecutorRequestError, read_execution_log
from simple_chat_agent.api import agent_models
from simple_chat_agent.api.agent_bridge import Bridge
from simple_chat_agent.api.agent_store import AgentStore, prefix, session_ttl_seconds

log = logging.getLogger(__name__)

KEEPALIVE_SECONDS = 15.0
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
UPDATE_RPC_TIMEOUT = timedelta(seconds=5)
SESSIONS_LIST_MAX = 500


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


class HarnessErrorRoute(APIRoute):
    """Answers every error raised while handling one of this router's routes with the harness
    error body: FastAPI's own 401 (from the auth dependency) and 422 (request validation), and
    :class:`HarnessError`. Scoped to this router; the app's other routes are unchanged."""

    def get_route_handler(self) -> Callable[[Request], Awaitable[Response]]:
        handler = super().get_route_handler()

        async def wrapped(request: Request) -> Response:
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


@functools.cache
def coding_agent_interface() -> tuple[dict[str, Any], ...]:
    """The ``agent_interface`` answer for a CodingAgent, built once per process."""
    return tuple(
        AcceptedFunction(
            name=name,
            description=description,
            parameters=input_type.model_json_schema(),
            output=output_type.model_json_schema(),
            mid_turn=mid_turn,
            model_callable=model_callable,
        ).model_dump(mode="json")
        for name, input_type, output_type, description, mid_turn, model_callable in _CODING_AGENT_HANDLERS
    )


# -- the session directory ---------------------------------------------------------------


class SessionDirectory:
    """Each user's session list, served to browsers from Redis and kept durably in the user's
    ``CodingSessions`` workflow.

    Redis: a sorted set of session ids by creation time and a hash of records, per owner
    digest. Every change is written to Redis first (browsers read it at once), then sent to the
    workflow as an Update from a background task, in order per user, so creating a session
    never waits for a worker. ``sync_workflow=False`` keeps the list in Redis only.
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
        self._locks: dict[str, asyncio.Lock] = {}
        self._tasks: set[asyncio.Task[None]] = set()
        self._closing = False

    def _keys(self, user_id: str) -> tuple[str, str]:
        base = f"{prefix()}:u:{owner_digest(user_id)}"
        return f"{base}:sessions", f"{base}:records"

    async def put(self, user_id: str, record: dict[str, Any]) -> None:
        index, records = self._keys(user_id)
        ttl = session_ttl_seconds()
        pipe = self.r.pipeline(transaction=True)
        pipe.zadd(index, {record["session_id"]: float(record["created_at"])})
        pipe.hset(records, record["session_id"], json.dumps(record))
        pipe.expire(index, ttl)
        pipe.expire(records, ttl)
        await pipe.execute()
        self._send(user_id, "register_session", record)

    async def get(self, user_id: str, session_id: str) -> dict[str, Any] | None:
        _index, records = self._keys(user_id)
        raw = await self.r.hget(records, session_id)
        return json.loads(raw) if raw else None

    async def list(self, user_id: str) -> list[dict[str, Any]]:
        index, records = self._keys(user_id)
        ids = await self.r.zrevrange(index, 0, SESSIONS_LIST_MAX - 1)
        if not ids:
            return []
        raws = await self.r.hmget(records, ids)
        return [json.loads(raw) for raw in raws if raw]

    async def rename(self, user_id: str, session_id: str, title: str) -> dict[str, Any] | None:
        record = await self.get(user_id, session_id)
        if record is None:
            return None
        record["title"] = title
        _index, records = self._keys(user_id)
        await self.r.hset(records, session_id, json.dumps(record))
        self._send(user_id, "rename_session", {"session_id": session_id, "title": title})
        return record

    async def remove(self, user_id: str, session_id: str) -> None:
        index, records = self._keys(user_id)
        pipe = self.r.pipeline(transaction=True)
        pipe.zrem(index, session_id)
        pipe.hdel(records, session_id)
        await pipe.execute()
        self._send(user_id, "remove_session", session_id)

    async def aclose(self) -> None:
        """Cancel pending syncs (Redis already holds the change). Bounded: a cancelled
        ``execute_update`` can take a while to unwind, and shutdown must not wait on it."""
        self._closing = True
        tasks = list(self._tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.wait(tasks, timeout=2.0)

    async def drain(self) -> None:
        """Wait for queued workflow syncs (tests)."""
        while self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)

    # -- workflow sync -----------------------------------------------------------------

    def _send(self, user_id: str, update: str, arg: Any) -> None:
        if not self._sync or self._closing:
            return
        task = asyncio.create_task(self._send_ordered(user_id, update, arg), name=f"sessions:{update}")
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _send_ordered(self, user_id: str, update: str, arg: Any) -> None:
        lock = self._locks.setdefault(owner_digest(user_id), asyncio.Lock())
        # One update id across retries, so Temporal deduplicates a retry of an Update that
        # was admitted before its RPC timed out. The three Updates are idempotent anyway.
        update_id = f"{update}-{uuid.uuid4().hex}"
        async with lock:
            delay = 1.0
            for attempt in range(6):
                try:
                    await self._update(user_id, update, arg, update_id)
                    return
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001 - Temporal unavailable: retry, then log
                    # A cancel that arrived mid-RPC can surface as the RPC's own error.
                    if self._closing or (task := asyncio.current_task()) is not None and task.cancelling():
                        raise asyncio.CancelledError from exc
                    log.info("session directory %s attempt %d failed: %s", update, attempt + 1, exc)
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, 30.0)
            log.warning("session directory %s for %s not recorded in CodingSessions", update, owner_digest(user_id))

    async def _update(self, user_id: str, update: str, arg: Any, update_id: str) -> None:
        from simple_chat_agent.agent.sessions import (
            SESSIONS_WORKFLOW_TYPE,
            RenameRequest,
            SessionRecord,
            SessionsState,
            sessions_workflow_id,
        )

        assert self._temporal is not None
        client = await self._temporal()
        digest = owner_digest(user_id)
        workflow_id = sessions_workflow_id(digest)
        await client.start_workflow(
            SESSIONS_WORKFLOW_TYPE,
            SessionsState(owner_digest=digest),
            id=workflow_id,
            task_queue=self._task_queue,
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
            search_attributes=owner_search_attributes(user_id),
            static_summary="coding agent session list",
            rpc_timeout=UPDATE_RPC_TIMEOUT,
        )
        # A fresh handle by id (no first_execution_run_id), so Updates keep working after
        # the manager continues as new (see main._ensure_user_chats_workflow).
        handle = client.get_workflow_handle(workflow_id)
        if update == "register_session":
            payload: Any = SessionRecord(
                session_id=arg["session_id"],
                title=arg.get("title") or "New session",
                model=arg.get("model", ""),
                created_at=datetime.fromtimestamp(float(arg["created_at"]), timezone.utc).isoformat(),
            )
        elif update == "rename_session":
            payload = RenameRequest(**arg)
        else:
            payload = arg
        # Bounded RPCs: a cancelled long-poll for an Update's outcome doesn't unwind until the
        # RPC returns, which would hold up API shutdown while no worker runs.
        await handle.execute_update(update, payload, id=update_id, rpc_timeout=UPDATE_RPC_TIMEOUT)


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

    async def root_if_owned(self, workflow_id: str, user_id: str) -> str | None:
        """The root session id when ``user_id`` owns ``workflow_id``, else None."""
        if not _ID.fullmatch(workflow_id or ""):
            return None
        root = workflow_id
        root_of = getattr(self._store, "root_of", None)
        if root_of is not None:
            root = await root_of(workflow_id) or workflow_id
        owner = await self._store.owner_of(root)
        if owner is not None:
            return root if hmac.compare_digest(owner, user_id) else None
        return await self._owner_from_temporal(workflow_id, user_id)

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
                register = getattr(self._store, "register_subagent", None)
                if register is not None and current != workflow_id:
                    await register(workflow_id, current)
                return current
            if not desc.parent_id:
                return None
            current = desc.parent_id
        return None


# -- the router --------------------------------------------------------------------------


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
            if await owners.root_if_owned(session_id, uid) == session_id:
                existing = await sessions.get(uid, session_id)
                if existing is not None:
                    return JSONResponse(_summary(existing), headers=NO_STORE)
        else:
            session_id = f"coding-agent-{uuid.uuid4().hex[:16]}"

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
        # Ownership first: a crash after starting the workflow must not orphan it.
        await store.set_owner(session_id, uid)
        seeded: dict[str, Any] | None = None
        if body.demo:
            # Before the workflow starts, so the first message already sees the repo, and a
            # failed seed leaves no session behind.
            try:
                result = await seed_fn(session_id, body.demo)
            except Exception as exc:  # noqa: BLE001 - executor refused or unreachable
                log.warning("seeding %s for %s failed: %s", body.demo, session_id, exc)
                raise HarnessError(502, "seed_failed", f"Couldn't set up the demo workspace: {exc}") from None
            seeded = {
                "fixture": body.demo,
                "files": len(getattr(result, "files", []) or []),
                "commit": getattr(result, "commit", ""),
            }
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

    @router.get("/sessions")
    async def list_sessions(uid: str = Depends(user_id)):
        records = await sessions.list(uid)
        metas = await asyncio.gather(*(store.meta(r["session_id"]) for r in records))
        return JSONResponse([_summary(r, closed=m.closed) for r, m in zip(records, metas)], headers=NO_STORE)

    @router.get("/sessions/{session_id}")
    async def get_session(session_id: str, uid: str = Depends(user_id)):
        await require_root(session_id, uid)
        record = await sessions.get(uid, session_id)
        if record is None:
            raise not_found()
        return JSONResponse(_summary(record, closed=(await store.meta(session_id)).closed), headers=NO_STORE)

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
        await sessions.remove(uid, session_id)
        return JSONResponse({"ok": True, "workflow_id": session_id}, headers=NO_STORE)

    @router.post("/sessions/{session_id}/close")
    async def close(session_id: str, uid: str = Depends(user_id)):
        await require(session_id, uid)
        try:
            await (await temporal()).get_workflow_handle(session_id).signal("close")
        except RPCError as exc:
            if exc.status != RPCStatusCode.NOT_FOUND:
                raise
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

        content = await file.read()
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
        except ExecutorRequestError as exc:
            raise HarnessError(502, "executor_unavailable", str(exc)) from None
        return JSONResponse(page, headers=NO_STORE)

    # -- the harness transport -----------------------------------------------------------

    @router.get("/agent-interface/{session_id}")
    async def agent_interface(session_id: str, uid: str = Depends(user_id)):
        root = await require(session_id, uid)
        if root != session_id:
            # Subagents have their own handler set, whose models live with the worker-only
            # subagent module; not served until they move to a key-free module.
            raise HarnessError(501, "not_implemented", "Subagent interfaces aren't served here yet.")
        return JSONResponse(list(coding_agent_interface()), headers={"Cache-Control": "private, max-age=300"})

    @router.get("/attach")
    async def attach(session_id: str, from_offset: int = 0, uid: str = Depends(user_id)):
        await require(session_id, uid)
        b = await bridge()
        return StreamingResponse(
            _reading(b, session_id, _frames(store, session_id, from_offset)),
            media_type="text/event-stream",
            headers=SSE_HEADERS,
        )

    @router.post("/messages")
    async def submit_message(body: SubmitMessage, uid: str = Depends(user_id)):
        await require(body.session_id, uid)
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
        await require(body.session_id, uid)
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
        await require(session_id, uid)
        try:
            desc = await (await temporal()).get_workflow_handle(session_id).describe()
        except RPCError as exc:
            if exc.status != RPCStatusCode.NOT_FOUND:
                raise
            return JSONResponse(
                {"workflow_id": session_id, "execution_status": "NOT_FOUND", "closed": True}, headers=NO_STORE
            )
        status = desc.status or WorkflowExecutionStatus.RUNNING
        closed = status not in (WorkflowExecutionStatus.RUNNING, WorkflowExecutionStatus.CONTINUED_AS_NEW)
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
            self._bridge = Bridge(await self.temporal(), self.store)
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
        await self.directory.aclose()
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


async def _frames(store: AgentStore, session_id: str, from_offset: int) -> AsyncIterator[bytes]:
    """Replay frames after ``from_offset`` from Redis, then tail until the agent is idle and
    every frame is delivered (client transport.ts ``attach``: "Ends when the agent is idle and
    caught up").

    ``from_offset`` is the client's last ``resume_offset``; frames at that position were
    already delivered, so replay starts after it. While a turn is open (including while no
    worker is running) the stream stays open with keep-alive comments rather than ending,
    since the client treats an attach that ends mid-turn as a dropped connection.
    """
    after = "0-0" if from_offset <= 0 else f"{from_offset + 1}-{2**64 - 1}"
    loop = asyncio.get_running_loop()
    last_sent = loop.time()
    first = True
    while True:
        # Meta first, frames second. The bridge appends a frame before it saves the meta that
        # frame produces, so an idle meta read here guarantees the read below already sees the
        # turn_end. Reading frames first could miss a turn_end appended between the two reads
        # and end the stream with the client's turn still open, which the client treats as a
        # dropped connection (client session.ts: "a stream that ends with a turn still open
        # was cut off").
        meta = await store.meta(session_id)
        # The first read backfills from the turn archive when Redis no longer holds the old
        # frames (expired, or Redis lost); later reads continue past it from Redis.
        backfilled = getattr(store, "read_backfilled", None) if first else None
        frames = await (backfilled or store.read)(session_id, after=after)
        first = False
        if not frames:
            if not meta.turn_active and not meta.pending:
                return
            frames = await store.wait(session_id, after=after, block_ms=1000)
        for frame in frames:
            payload = {**frame.data, "resume_offset": frame.resume_offset}
            yield f"event: {frame.event}\ndata: {json.dumps(payload)}\n\n".encode()
            after = frame.entry_id
            last_sent = loop.time()
        if not frames and loop.time() - last_sent > KEEPALIVE_SECONDS:
            yield b": keep-alive\n\n"
            last_sent = loop.time()
