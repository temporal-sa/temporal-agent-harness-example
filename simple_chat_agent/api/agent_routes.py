"""The harness client's HTTP contract, served by our API (PRD S1, S2, S8).

``@temporalio/agent-harness-client``'s ``HttpTransport`` calls these routes relative to its
``baseUrl``. Differences from the packaged harness app, all deliberate:

* Every route checks that the signed-in user owns the session (404 otherwise).
* ``attach`` and ``status`` read from Redis (filled by the bridge), never from Temporal, so
  they cost no Actions and keep working while every worker is down.
* ``messages`` and ``approve`` go through the Signal front door: they land in the workflow's
  history immediately, with or without a running worker. The message id is chosen here, so the
  client can match the later ``message_accepted`` to the message it sent.
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any, Literal

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field
from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.service import RPCError, RPCStatusCode

from temporal_agent_harness.harness.agent_protocol import AgentConfig

from simple_chat_agent.agent.contracts import (
    AGENT_WORKFLOW_TYPE,
    ENQUEUE_APPROVAL_SIGNAL,
    ENQUEUE_MESSAGE_SIGNAL,
    CodingAgentInit,
    FrontDoorApproval,
    FrontDoorMessage,
    ModelChoice,
    Posture,
)
from simple_chat_agent.agent.approvals import configured_judge_backend
from simple_chat_agent.agent.tools.research import research_tools_available
from simple_chat_agent.agent.model_registry import is_allowed
from simple_chat_agent.api.agent_bridge import Bridge
from simple_chat_agent.api.agent_store import AgentStore

KEEPALIVE_SECONDS = 15.0


def agent_environment() -> str:
    """``catalog`` (default, stricter approval rules) or ``local`` for development."""
    return "local" if os.environ.get("AGENT_ENVIRONMENT", "catalog").strip().lower() == "local" else "catalog"
NO_STORE = {"Cache-Control": "no-store"}
SSE_HEADERS = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no", "Connection": "keep-alive"}


class CreateSession(BaseModel):
    model: ModelChoice = Field(default_factory=ModelChoice)
    posture: Posture = "trust_safe"


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


def _error(status: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(status_code=status, content={"error": code, "message": message}, headers=NO_STORE)


def create_agent_router(
    *,
    temporal: Callable[[], Awaitable[Client]],
    store: AgentStore,
    bridge: Callable[[], Awaitable[Bridge]],
    current_user: Callable[[Request], Any],
    task_queue: str,
) -> APIRouter:
    router = APIRouter()

    def user_id(request: Request) -> str:
        return current_user(request).user_id

    async def owned(session_id: str, uid: str) -> bool:
        return await store.owner_of(session_id) == uid

    @router.post("/sessions")
    async def create_session(body: CreateSession, uid: str = Depends(user_id)):
        # The configured allowlist, not this process's keys: the API holds no LLM keys. The
        # worker re-checks against the models it can actually call.
        if not is_allowed(body.model, require_keys=False):
            return _error(422, "model_not_allowed", f"{body.model.registry_key} is not available.")
        session_id = f"coding-agent-{uuid.uuid4().hex[:16]}"
        data = CodingAgentInit(
            owner_id=uid,
            session_id=session_id,
            workspace_id=session_id,
            model=body.model,
            posture=body.posture,
            environment=agent_environment(),
            judge=configured_judge_backend(),
            research_tools=[t.__name__ for t in research_tools_available()],
        )
        if body.posture == "never_ask" and data.environment == "catalog":
            return _error(422, "posture_not_allowed", "\"Never ask\" isn't available here.")
        # Ownership first: a crash after starting the workflow must not orphan it.
        await store.set_owner(session_id, uid)
        client = await temporal()
        await client.start_workflow(
            AGENT_WORKFLOW_TYPE, args=[AgentConfig(), data], id=session_id, task_queue=task_queue
        )
        return JSONResponse({"session_id": session_id, "agent": AGENT_WORKFLOW_TYPE}, headers=NO_STORE)

    @router.get("/attach")
    async def attach(session_id: str, from_offset: int = 0, uid: str = Depends(user_id)):
        if not await owned(session_id, uid):
            return _error(404, "not_found", "No such session.")
        (await bridge()).ensure(session_id)
        return StreamingResponse(
            _frames(store, session_id, from_offset), media_type="text/event-stream", headers=SSE_HEADERS
        )

    @router.post("/messages")
    async def submit_message(body: SubmitMessage, uid: str = Depends(user_id)):
        if not await owned(body.session_id, uid):
            return _error(404, "not_found", "No such session.")
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
                return _error(404, "not_found", "The session has ended.")
            raise
        await store.note_sent(body.session_id, message_id)
        (await bridge()).ensure(body.session_id)
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
        if not await owned(body.session_id, uid):
            return _error(404, "not_found", "No such session.")
        handle = (await temporal()).get_workflow_handle(body.session_id)
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
        (await bridge()).ensure(body.session_id)
        return JSONResponse({"tool_id": body.tool_id, "accepted": True}, headers=NO_STORE)

    @router.post("/callback-result")
    async def callback_result(request: Request, uid: str = Depends(user_id)):
        return _error(501, "not_implemented", "Callback tools arrive in P4.")

    @router.get("/status/{session_id}")
    async def status(session_id: str, uid: str = Depends(user_id)):
        if not await owned(session_id, uid):
            return _error(404, "not_found", "No such session.")
        meta = await store.meta(session_id)
        return JSONResponse(meta.to_status(), headers=NO_STORE)

    @router.get("/workflow-status/{session_id}")
    async def workflow_status(session_id: str, uid: str = Depends(user_id)):
        if not await owned(session_id, uid):
            return _error(404, "not_found", "No such session.")
        desc = await (await temporal()).get_workflow_handle(session_id).describe()
        status = desc.status or WorkflowExecutionStatus.RUNNING
        closed = status not in (WorkflowExecutionStatus.RUNNING, WorkflowExecutionStatus.CONTINUED_AS_NEW)
        return JSONResponse(
            {"workflow_id": session_id, "execution_status": status.name, "closed": closed},
            headers=NO_STORE,
        )

    @router.post("/sessions/{session_id}/close")
    async def close(session_id: str, uid: str = Depends(user_id)):
        if not await owned(session_id, uid):
            return _error(404, "not_found", "No such session.")
        await (await temporal()).get_workflow_handle(session_id).signal("close")
        return JSONResponse({"ok": True}, headers=NO_STORE)

    return router


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
    while True:
        # Meta first, frames second. The bridge appends a frame before it saves the meta that
        # frame produces, so an idle meta read here guarantees the read below already sees the
        # turn_end. Reading frames first could miss a turn_end appended between the two reads
        # and end the stream with the client's turn still open, which the client treats as a
        # dropped connection (client session.ts: "a stream that ends with a turn still open
        # was cut off").
        meta = await store.meta(session_id)
        frames = await store.read(session_id, after=after)
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
