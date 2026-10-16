"""The Redis bridge: one forwarder per active session copies the agent's event stream into Redis.

Each forwarder holds a Redis lease, so across API replicas a session has exactly one Temporal
reader however many browser tabs are open (every Temporal poll is an Update recorded in the
workflow's history). It runs the harness's own merged ``AgentClient.attach`` (root agent plus
subagents) and appends each frame to the session's Redis stream (agent_store frame format v1).
Browsers read only from Redis, so the transcript and status keep working while every worker
is down.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import os
import uuid
from typing import Any

from temporalio.client import Client

from temporal_agent_harness.harness.agent_client import AgentClient
from temporal_agent_harness.harness.agent_protocol import AgentEvent

from simple_chat_agent.api.agent_store import AgentStore, fold

log = logging.getLogger(__name__)

# How long a forwarder keeps re-checking an idle session before it stops (it restarts on the
# next send or attach).
IDLE_STOP_SECONDS = float(os.environ.get("AGENT_BRIDGE_IDLE_STOP_SECONDS", "600"))
# How often an idle forwarder re-attaches to notice work another client started.
IDLE_RECHECK_SECONDS = float(os.environ.get("AGENT_BRIDGE_RECHECK_SECONDS", "5"))


def sse_data(item: AgentEvent) -> tuple[str, dict[str, Any]]:
    """The harness web app's SSE ``event`` and ``data`` for one event (web/app.py
    ``_yield_item``), without ``resume_offset``, which the store keeps as the entry id."""
    payload = item.event
    return payload.type.value, {
        **payload.model_dump(mode="json"),
        "agent_id": item.agent_id,
        "turn_id": item.turn_id,
        "turn_number": item.turn_number,
        "message_id": item.message_id,
        "timestamp": item.timestamp.isoformat() if hasattr(item.timestamp, "isoformat") else item.timestamp,
    }


def _digest(event: str, data: dict[str, Any]) -> str:
    return hashlib.sha256(f"{event}|{json.dumps(data, sort_keys=True)}".encode()).hexdigest()


class Bridge:
    def __init__(self, temporal: Client, store: AgentStore) -> None:
        self._temporal = temporal
        self._store = store
        self._holder = f"api-{uuid.uuid4().hex[:8]}"
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._wake: dict[str, asyncio.Event] = {}

    def ensure(self, session: str) -> None:
        """Start (or wake) the forwarder for ``session``."""
        self._wake.setdefault(session, asyncio.Event()).set()
        task = self._tasks.get(session)
        if task is None or task.done():
            self._tasks[session] = asyncio.create_task(self._run(session), name=f"bridge:{session}")

    async def aclose(self) -> None:
        for task in self._tasks.values():
            task.cancel()
        for task in self._tasks.values():
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task

    async def _run(self, session: str) -> None:
        if not await self._store.acquire_lease(session, self._holder):
            return  # another replica is forwarding this session
        renew = asyncio.create_task(self._renew(session))
        wake = self._wake.setdefault(session, asyncio.Event())
        idle_for = 0.0
        backoff = 1.0
        try:
            while idle_for < IDLE_STOP_SECONDS:
                wake.clear()
                try:
                    forwarded = await self._forward_once(session)
                    backoff = 1.0
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001 - workers down, network: retry
                    log.info("bridge %s: attach failed (%s); retrying in %.0fs", session, exc, backoff)
                    forwarded = 0
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 15.0)
                    continue
                idle_for = 0.0 if forwarded else idle_for + IDLE_RECHECK_SECONDS
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(wake.wait(), timeout=IDLE_RECHECK_SECONDS)
                    idle_for = 0.0
        finally:
            renew.cancel()
            await self._store.release_lease(session, self._holder)

    async def _renew(self, session: str) -> None:
        while True:
            await asyncio.sleep(5)
            if not await self._store.renew_lease(session, self._holder):
                log.warning("bridge %s: lost lease", session)
                task = self._tasks.get(session)
                if task:
                    task.cancel()
                return

    async def _forward_once(self, session: str) -> int:
        """Attach from the last stored position and copy frames until the agent is idle."""
        last = await self._store.last_position(session)
        offset, seq = last if last is not None else (0, -1)
        # The merge can re-send frames that share the resume position we restart from
        # (subagent frames between two root events); skip those by content.
        seen = {_digest(f.event, f.data) for f in await self._store.read_at(session, offset)}
        meta = await self._store.meta(session)
        client = AgentClient(self._temporal, session)
        count = 0
        frames = await client.attach(on_item=lambda item, ro: (item, ro), from_offset=offset)
        async for item, resume_offset in frames:
            if not isinstance(item, AgentEvent):
                continue
            event, data = sse_data(item)
            digest = _digest(event, data)
            if digest in seen:
                continue
            if resume_offset == offset:
                seq += 1
            else:
                offset, seq, seen = resume_offset, 0, set()
            seen.add(digest)
            await self._store.append(session, event, data, resume_offset, seq)
            fold(meta, event, data)
            meta.head = resume_offset
            await self._store.save_meta(session, meta)
            count += 1
        return count
