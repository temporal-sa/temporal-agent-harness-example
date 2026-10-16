"""The Redis bridge: one forwarder per active session copies the agent's event stream into Redis.

Each forwarder holds a Redis lease, so across API replicas a session has exactly one Temporal
reader however many browser tabs are open (every Temporal poll is an Update recorded in the
workflow's history). It runs the harness's own merged ``AgentClient.attach`` (root agent plus
subagents) and appends each frame to the session's Redis stream (agent_store frame format v1).
Browsers read only from Redis, so the transcript and status keep working while every worker
is down.

Around the copy, the forwarder:

* registers each subagent it sees start (``subagent_started``) under the root session, so the
  routes can check ownership of subagent ids;
* derives telemetry frames (``x_usage``, ``x_recovery``, ``x_worker``; see agent_telemetry);
* at each root ``turn_end``: confirms the turn's telemetry from history, archives the frames
  since the last archive (S3 or disk), then Signals the agent ``stream_acked`` with the highest
  stored resume offset, once (the agent truncates its carried stream to it at continue-as-new).

Wake: a send on any replica publishes on the session's wake channel; every replica listens on
one pub/sub connection and wakes its local forwarder, if it has one.

Lease takeover: a replica that wanted to forward but found the lease taken stands by while the
session is busy or has local readers, polling the lease every ``STANDBY_POLL_SECONDS``; a
graceful release wakes standbys at once. Writes are fenced on the lease, and a new holder
replays the open turn from its ``turn_started`` and drops frames already stored (by content),
so subagents mounted in that turn are re-mounted and nothing is stored twice.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import os
import time
import uuid
from collections import Counter
from collections.abc import AsyncIterator
from typing import Any

from temporalio.client import Client
from temporalio.service import RPCError, RPCStatusCode

from temporal_agent_harness.harness.agent_client import AgentClient
from temporal_agent_harness.harness.agent_protocol import AgentEvent

from simple_chat_agent.api import agent_telemetry as telemetry
from simple_chat_agent.api.agent_store import AgentStore, LeaseLost, SessionMeta, fold, wake_pattern

try:
    from simple_chat_agent.agent.contracts import STREAM_ACKED_SIGNAL, StreamAck
except ImportError:  # pragma: no cover - older contracts
    # TODO: remove once contracts.py carries the constant everywhere.
    STREAM_ACKED_SIGNAL = "stream_acked"
    StreamAck = None  # type: ignore[assignment,misc]

log = logging.getLogger(__name__)

# How long a forwarder keeps re-checking an idle session before it stops (it restarts on the
# next send or attach).
IDLE_STOP_SECONDS = float(os.environ.get("AGENT_BRIDGE_IDLE_STOP_SECONDS", "600"))
# How often an idle forwarder re-attaches to notice work another client started.
IDLE_RECHECK_SECONDS = float(os.environ.get("AGENT_BRIDGE_RECHECK_SECONDS", "5"))
LEASE_TTL_MS = int(os.environ.get("AGENT_BRIDGE_LEASE_TTL_MS", "15000"))
LEASE_RENEW_SECONDS = float(os.environ.get("AGENT_BRIDGE_LEASE_RENEW_SECONDS", "5"))
# How often a standby replica checks whether the lease is free.
STANDBY_POLL_SECONDS = float(os.environ.get("AGENT_BRIDGE_STANDBY_POLL_SECONDS", "2"))
# Silence (no frame) during an open turn before Describe is asked about the workers.
WORKER_SILENCE_SECONDS = float(os.environ.get("AGENT_BRIDGE_WORKER_SILENCE_SECONDS", "8"))
WORKER_CHECK_EVERY_SECONDS = float(os.environ.get("AGENT_BRIDGE_WORKER_CHECK_SECONDS", "4"))


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
    """Content identity of a harness frame (``event_offset`` is ours, not the harness's)."""
    body = {k: v for k, v in data.items() if k != "event_offset"}
    return hashlib.sha256(f"{event}|{json.dumps(body, sort_keys=True)}".encode()).hexdigest()


class _LeaseLostError(Exception):
    pass


async def _stop(task: asyncio.Task[Any] | None) -> None:
    """Cancel ``task`` and wait for it, without swallowing a cancellation of the caller."""
    if task is None:
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        current = asyncio.current_task()
        if current is not None and current.cancelling():
            raise
    except Exception:  # noqa: BLE001 - it is being stopped; its error doesn't matter
        pass


class Bridge:
    def __init__(self, temporal: Client, store: AgentStore, *, holder: str | None = None) -> None:
        self._temporal = temporal
        self._store = store
        self._holder = holder or f"api-{uuid.uuid4().hex[:8]}"
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._wake: dict[str, asyncio.Event] = {}
        self._readers: Counter[str] = Counter()
        self._leading: set[str] = set()
        self._listener: asyncio.Task[None] | None = None
        self._closed = False

    @property
    def holder(self) -> str:
        return self._holder

    def is_forwarding(self, session: str) -> bool:
        """True while this replica holds the session's lease and is forwarding it."""
        return session in self._leading

    # -- public API ---------------------------------------------------------------

    def ensure(self, session: str) -> None:
        """Start (or wake) this replica's forwarder for ``session``. If another replica holds
        the lease, the local one stands by while the session is busy or read here."""
        if self._closed:
            return
        self._start_listener()
        self._wake.setdefault(session, asyncio.Event()).set()
        task = self._tasks.get(session)
        if task is None or task.done():
            self._tasks[session] = asyncio.create_task(self._run(session), name=f"bridge:{session}")

    async def wake(self, session: str) -> None:
        """New work for ``session`` (a send, an approval): wake the local forwarder and every
        other replica's (whichever holds the lease forwards it)."""
        self.ensure(session)
        try:
            await self._store.publish_wake(session, "send")
        except Exception as exc:  # noqa: BLE001 - local wake already happened
            log.warning("bridge %s: wake publish failed: %s", session, exc)

    @contextlib.asynccontextmanager
    async def reading(self, session: str) -> AsyncIterator[None]:
        """Hold this around an attach stream served from this replica: the local forwarder
        then stands by to take over the session while anyone here is reading it."""
        self._readers[session] += 1
        self.ensure(session)
        try:
            yield
        finally:
            self._readers[session] -= 1
            if self._readers[session] <= 0:
                del self._readers[session]

    async def aclose(self) -> None:
        self._closed = True
        # Copy: finishing forwarders remove themselves from _tasks.
        tasks = list(self._tasks.values())
        for task in tasks:
            task.cancel()
        for task in tasks:
            await _stop(task)
        await _stop(self._listener)

    # -- wake listener --------------------------------------------------------------

    def _start_listener(self) -> None:
        if self._listener is None or self._listener.done():
            self._listener = asyncio.create_task(self._listen(), name="bridge:wake-listener")

    async def _listen(self) -> None:
        """One pub/sub connection per process: wake the local forwarder of a session another
        replica has news for."""
        backoff = 0.5
        while not self._closed:
            pubsub = self._store.r.pubsub(ignore_subscribe_messages=True)
            try:
                await pubsub.psubscribe(wake_pattern())
                backoff = 0.5
                while True:
                    msg = await pubsub.get_message(timeout=5.0)
                    if msg is None:
                        continue
                    channel = msg.get("channel") or ""
                    if isinstance(channel, bytes):
                        channel = channel.decode()
                    session = channel.rsplit(":wake:", 1)[-1]
                    event = self._wake.get(session)
                    task = self._tasks.get(session)
                    if event is not None and task is not None and not task.done():
                        event.set()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - Redis blip: resubscribe
                log.info("bridge: wake listener error (%s); resubscribing", exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 10.0)
            finally:
                with contextlib.suppress(Exception):
                    await pubsub.aclose()

    # -- per-session lifecycle -------------------------------------------------------

    async def _run(self, session: str) -> None:
        wake = self._wake.setdefault(session, asyncio.Event())
        standby_since: float | None = None
        loop = asyncio.get_running_loop()
        try:
            while True:
                if await self._store.acquire_lease(session, self._holder, LEASE_TTL_MS):
                    standby_since = None
                    if await self._lead(session, wake) == "idle":
                        return
                    # Lost the lease to another replica: stand by.
                if standby_since is None:
                    standby_since = loop.time()
                if loop.time() - standby_since > IDLE_STOP_SECONDS or not await self._worth_standby(session):
                    return
                wake.clear()
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(wake.wait(), timeout=STANDBY_POLL_SECONDS)
        finally:
            if self._tasks.get(session) is asyncio.current_task():
                self._tasks.pop(session, None)
                self._wake.pop(session, None)

    async def _worth_standby(self, session: str) -> bool:
        if self._readers.get(session, 0) > 0:
            return True
        meta = await self._store.meta(session)
        return meta.turn_active or bool(meta.pending)

    async def _lead(self, session: str, wake: asyncio.Event) -> str:
        """Forward while holding the lease. ``idle`` when the session went quiet for good,
        ``lost`` when another replica took the lease."""
        fwd = _Forwarder(self, session)
        inner = asyncio.create_task(fwd.run(wake), name=f"bridge:{session}:forward")
        renew = asyncio.create_task(self._renew(session, inner))
        self._leading.add(session)
        try:
            await inner
            return "idle"
        except (asyncio.CancelledError, LeaseLost, _LeaseLostError):
            current = asyncio.current_task()
            if current is not None and current.cancelling():
                raise
            log.warning("bridge %s: lost the lease; standing by", session)
            return "lost"
        finally:
            self._leading.discard(session)
            await _stop(renew)
            await _stop(inner)
            await fwd.cancel_jobs()
            with contextlib.suppress(Exception):
                await self._store.release_lease(session, self._holder)
                await self._store.publish_wake(session, "released")

    async def _renew(self, session: str, inner: asyncio.Task[Any]) -> None:
        while True:
            await asyncio.sleep(LEASE_RENEW_SECONDS)
            try:
                ok = await self._store.renew_lease(session, self._holder, LEASE_TTL_MS)
            except Exception as exc:  # noqa: BLE001 - Redis blip: try again next tick
                log.info("bridge %s: lease renew failed: %s", session, exc)
                continue
            if not ok:
                inner.cancel()
                return

    async def _session_closed(self, session: str) -> bool:
        """True when the workflow has ended (Describe needs no worker)."""
        from temporalio.client import WorkflowExecutionStatus

        try:
            desc = await self._temporal.get_workflow_handle(session).describe()
        except Exception:  # noqa: BLE001 - can't tell; assume open
            return False
        return desc.status not in (None, WorkflowExecutionStatus.RUNNING)


class _Forwarder:
    """One lease-holding period of one session's forwarder."""

    def __init__(self, bridge: Bridge, session: str) -> None:
        self.b = bridge
        self.session = session
        self.store = bridge._store
        self.temporal = bridge._temporal
        self.tel = telemetry.Telemetry(session)
        self.pos: tuple[int, int] = (0, -1)  # (resume_offset, seq) of the newest stored frame
        self._write_lock = asyncio.Lock()
        self._jobs: set[asyncio.Task[Any]] = set()
        self.last_frame_at = time.monotonic()
        self.turn_active = False
        self.worker_state: str | None = None
        self.highest_root = 0  # highest root resume_offset stored

    # -- main loop -------------------------------------------------------------------

    async def run(self, wake: asyncio.Event) -> None:
        meta = await self.store.meta(self.session)
        self.worker_state = (meta.worker or {}).get("state")
        idle_for = 0.0
        backoff = 1.0
        while idle_for < IDLE_STOP_SECONDS:
            wake.clear()
            try:
                forwarded = await self._forward_once()
                backoff = 1.0
            except (asyncio.CancelledError, LeaseLost):
                raise
            except Exception as exc:  # noqa: BLE001 - workers down, network: retry
                if await self.b._session_closed(self.session):
                    await self.store.clear_pending(self.session)
                    await self._settle()
                    return
                log.info("bridge %s: attach failed (%s); retrying in %.0fs", self.session, exc, backoff)
                await asyncio.sleep(backoff)
                # Failures count toward the idle limit, so a forwarder whose attach keeps
                # failing (workers gone for good) still stops eventually.
                idle_for += backoff
                backoff = min(backoff * 2, 15.0)
                continue
            await self._settle()
            if not forwarded and (await self.store.meta(self.session)).pending:
                # Attach on a closed, idle workflow returns nothing without raising.
                if await self.b._session_closed(self.session):
                    await self.store.clear_pending(self.session)
                    return
            idle_for = 0.0 if forwarded else idle_for + IDLE_RECHECK_SECONDS
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(wake.wait(), timeout=IDLE_RECHECK_SECONDS)
                idle_for = 0.0

    async def _forward_once(self) -> int:
        """Attach from the last stored position (or the open turn's start, after a takeover)
        and copy frames until the agent is idle."""
        meta = await self.store.meta(self.session)
        last = await self.store.last_position(self.session)
        if last is None:
            # Redis lost the stream: continue after what the archive holds.
            with contextlib.suppress(Exception):
                last = await self.store.archive.last_position(self.session)
        self.pos = last if last is not None else (0, -1)
        from_offset = self.pos[0]
        if meta.turn_active and meta.turn_start is not None and meta.turn_start < from_offset:
            from_offset = meta.turn_start
        # Frames already stored at or after where we restart: skip them by content.
        seen = {_digest(f.event, f.data) for f in await self.store.read_from(self.session, from_offset)
                if not f.event.startswith("x_")}
        self.highest_root = max(self.highest_root, meta.head)
        self.turn_active = meta.turn_active
        max_ro = from_offset
        client = AgentClient(self.temporal, self.session)
        count = 0
        frames = await client.attach(on_item=lambda item, ro: (item, ro), from_offset=from_offset)
        watcher = asyncio.create_task(self._watch_workers())
        try:
            async for item, resume_offset in frames:
                if not isinstance(item, AgentEvent):
                    continue
                event, data = sse_data(item)
                # Only root events advance the resume offset (harness attach contract).
                is_root = resume_offset > max_ro
                max_ro = max(max_ro, resume_offset)
                if is_root:
                    data["event_offset"] = resume_offset - 1
                self.last_frame_at = time.monotonic()
                obs = self.tel.observe(event, data, resume_offset, is_root=is_root)
                digest = _digest(event, data)
                dup = digest in seen
                if dup:
                    # Stored by the previous holder. Still fold it below: that holder may have
                    # died between storing a frame and saving the meta it produces.
                    seen.discard(digest)
                else:
                    await self._write(event, data, resume_offset)
                    count += 1
                if is_root:
                    before = (meta.turn_active, meta.current_turn, meta.turn_start, meta.head)
                    change = fold(meta, event, data)
                    meta.head = max(meta.head, resume_offset)
                    self.highest_root = max(self.highest_root, resume_offset)
                    if event == "turn_started":
                        meta.turn_start = resume_offset - 1
                    self.turn_active = meta.turn_active
                    after = (meta.turn_active, meta.current_turn, meta.turn_start, meta.head)
                    if not dup or before != after or change.added or change.removed:
                        await self.store.save_meta(
                            self.session, meta, pending_added=change.added, pending_removed=change.removed
                        )
                if event == "subagent_started" and data.get("workflow_id"):
                    await self.store.register_subagent(data["workflow_id"], self.session)
                if dup:
                    if is_root and event == "turn_end" and resume_offset > meta.acked:
                        # The previous holder died before confirming and acking this turn.
                        for job in obs.jobs:
                            self._spawn(job, root_turn_end=True)
                    continue
                if self.worker_state == "suspect":
                    await self._set_worker("ok", "frames are arriving again", None)
                for x_event, x_data in obs.frames:
                    await self._emit(x_event, x_data)
                for job in obs.jobs:
                    self._spawn(job, root_turn_end=is_root and event == "turn_end")
        finally:
            await _stop(watcher)
        return count

    # -- writing -----------------------------------------------------------------------

    async def _write(self, event: str, data: dict[str, Any], resume_offset: int | None) -> None:
        """Append at ``resume_offset`` (or after the newest frame for telemetry), fenced on
        the lease. Positions never go backwards: a frame a takeover finds missing at an older
        position is stored at the current one."""
        async with self._write_lock:
            for _ in range(3):
                ro = self.pos[0] if resume_offset is None else max(resume_offset, self.pos[0])
                seq = self.pos[1] + 1 if ro == self.pos[0] else 0
                if await self.store.append(self.session, event, data, ro, seq, holder=self.b.holder):
                    self.pos = (ro, seq)
                    return
                # Someone stored at this position (we were behind): catch up and retry.
                self.pos = await self.store.last_position(self.session) or self.pos
            raise RuntimeError(f"bridge {self.session}: could not place a {event} frame")

    async def _emit(self, event: str, data: dict[str, Any]) -> None:
        """Store a telemetry frame once (its ``id`` + confirmation state)."""
        key = f"{data.get('id')}|{'c' if data.get('confirmed') else 'l'}"
        if event == telemetry.X_WORKER:
            key = f"worker|{data.get('state')}|{data.get('since')}"
        if await self.store.was_emitted(self.session, key):
            return
        await self._write(event, data, None)
        await self.store.mark_emitted(self.session, key)

    # -- background jobs ---------------------------------------------------------------

    def _spawn(self, job: telemetry.Job, *, root_turn_end: bool) -> None:
        if root_turn_end:
            # Root turn end: confirm, archive, ack, in order, after any earlier jobs.
            prior = list(self._jobs)
            task = asyncio.create_task(self._root_turn_end(job, prior))
        else:
            task = asyncio.create_task(self._job(job))
        self._jobs.add(task)
        task.add_done_callback(self._jobs.discard)

    async def _job(self, job: telemetry.Job) -> None:
        try:
            frames = await telemetry.run_job(self.temporal, self.tel, job)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - telemetry never stops the transcript
            log.warning("bridge %s: telemetry %s failed: %s", self.session, job.kind, exc)
            return
        try:
            for event, data in frames:
                await self._emit(event, data)
        except LeaseLost:
            return  # the new holder confirms from history itself

    async def _root_turn_end(self, job: telemetry.Job, prior: list[asyncio.Task[Any]]) -> None:
        if prior:
            await asyncio.wait(prior, timeout=20.0)
        await self._job(job)
        await self._archive_and_ack()

    async def _settle(self) -> None:
        """The attach ended (agent idle): finish telemetry, then archive and ack anything
        stored since the last turn end."""
        await self.drain(timeout=30.0)
        await self._archive_and_ack()

    async def drain(self, timeout: float) -> None:
        if self._jobs:
            await asyncio.wait(list(self._jobs), timeout=timeout)

    async def cancel_jobs(self) -> None:
        jobs = list(self._jobs)
        for task in jobs:
            task.cancel()
        for task in jobs:
            await _stop(task)

    async def _archive_and_ack(self) -> None:
        meta = await self.store.meta(self.session)
        through = f"{self.pos[0] + 1}-{self.pos[1]}"
        if self.pos[1] >= 0 and meta.archived != through:
            try:
                frames = await self.store.read_range(self.session, after=meta.archived, through=through)
                if frames:
                    await self.store.archive.put(self.session, frames, turn=meta.current_turn or None)
                    await self.store.set_tracking(self.session, archived=frames[-1].entry_id)
            except Exception as exc:  # noqa: BLE001 - Redis still holds them; next turn retries
                log.warning("bridge %s: archive failed: %s", self.session, exc)
        offset = max(self.highest_root, meta.head)
        if offset > meta.acked:
            try:
                handle = self.temporal.get_workflow_handle(self.session)
                payload = StreamAck(offset=offset) if StreamAck is not None else {"offset": offset}
                await handle.signal(STREAM_ACKED_SIGNAL, payload)
                await self.store.set_tracking(self.session, acked=offset)
            except RPCError as exc:
                if exc.status == RPCStatusCode.NOT_FOUND:
                    # The workflow has ended: nothing left to truncate. Don't retry.
                    await self.store.set_tracking(self.session, acked=offset)
                else:
                    log.info("bridge %s: stream_acked failed: %s", self.session, exc)
            except Exception as exc:  # noqa: BLE001 - Temporal blip: the next settle retries
                log.info("bridge %s: stream_acked failed: %s", self.session, exc)

    # -- worker health -------------------------------------------------------------------

    async def _watch_workers(self) -> None:
        last_check = 0.0
        while True:
            await asyncio.sleep(1.0)
            now = time.monotonic()
            silent = now - self.last_frame_at
            if not self.turn_active or silent < WORKER_SILENCE_SECONDS:
                continue
            if now - last_check < WORKER_CHECK_EVERY_SECONDS:
                continue
            last_check = now
            try:
                health = await telemetry.check_health(self.temporal, self.tel)
            except Exception as exc:  # noqa: BLE001
                log.info("bridge %s: worker check failed: %s", self.session, exc)
                continue
            if health.state != (self.worker_state or "ok"):
                await self._set_worker(health.state, health.reason, health.pollers)
            for retry in health.tool_retries:
                turn = next((t for t in self.tel.open.values() if t.workflow_id == retry["workflow_id"]), None)
                if turn is not None:
                    await self._emit(*telemetry.live_tool_recovery_frame(self.tel, turn, retry))

    async def _set_worker(self, state: str, reason: str, pollers: int | None) -> None:
        since = time.time()
        self.worker_state = state
        turn = next((t for t in self.tel.open.values() if t.is_root), None)
        event, data = telemetry.worker_frame(state, reason, since, turn=turn, pollers=pollers)
        await self._emit(event, data)
        with contextlib.suppress(Exception):
            await self.store.set_tracking(
                self.session, worker={"state": state, "reason": reason, "since": since}
            )


__all__ = ["Bridge", "SessionMeta", "sse_data"]
