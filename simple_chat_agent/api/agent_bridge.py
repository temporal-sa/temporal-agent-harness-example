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
* at each root ``turn_end``: confirms the turn's telemetry from history (meta ``confirming`` is
  true meanwhile, at most ``CONFIRM_TIMEOUT_SECONDS``), archives the frames since the last
  archive (S3 or disk), then Signals the agent ``stream_acked`` once, with the highest root
  offset that is both stored and archived (the agent truncates its carried stream to it at
  continue-as-new).

Only root sessions are forwarded: ``ensure``/``wake``/``reading`` given a subagent workflow id
act on its root (``AgentStore.root_of``), and ids with no ownership record are ignored.

Wake: a send on any replica publishes on the session's wake channel; every replica listens on
one pub/sub connection and wakes its local forwarder, if it has one.

Idle: once an attach finds the agent idle and caught up, the forwarder stops (sends and
approvals wake it through ``wake``). While a reader on this replica is attached it stays, and
notices work started elsewhere (e.g. through the harness Update) by Describe's history length,
every ``IDLE_WATCH_SECONDS`` backing off to ``IDLE_WATCH_MAX_SECONDS``: no Queries while idle.

Lease takeover: a replica that wanted to forward but found the lease taken stands by while the
session is busy or has local readers, polling the lease every ``STANDBY_POLL_SECONDS``; a
graceful release wakes standbys at once. Writes are fenced on the lease, and a new holder
replays the open turn from its ``turn_started`` and drops frames already stored (by content),
so subagents mounted in that turn are re-mounted and nothing is stored twice. Root frames are
matched by ``event_offset``; other frames by content, counting repeats, so two identical frames
both survive.
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

from simple_chat_agent.agent.contracts import STREAM_ACKED_SIGNAL, StreamAck

log = logging.getLogger(__name__)

# Longest a forwarder keeps an idle session (with a local reader), or keeps retrying a failing
# attach, before it stops (it restarts on the next send or attach).
IDLE_STOP_SECONDS = float(os.environ.get("AGENT_BRIDGE_IDLE_STOP_SECONDS", "600"))
# While idle with a local reader: Describe this often (backing off to the max) for work started
# elsewhere. Describe is not a Query and needs no worker.
IDLE_WATCH_SECONDS = float(os.environ.get("AGENT_BRIDGE_IDLE_WATCH_SECONDS", "30"))
IDLE_WATCH_MAX_SECONDS = float(os.environ.get("AGENT_BRIDGE_IDLE_WATCH_MAX_SECONDS", "60"))
# Longest wait between retries of a failing attach (each attach costs two Queries).
ATTACH_RETRY_MAX_SECONDS = float(os.environ.get("AGENT_BRIDGE_ATTACH_RETRY_MAX_SECONDS", "30"))
# Longest a root turn end may hold attaches open while its telemetry is confirmed.
CONFIRM_TIMEOUT_SECONDS = float(os.environ.get("AGENT_BRIDGE_CONFIRM_TIMEOUT_SECONDS", "30"))
LEASE_TTL_MS = int(os.environ.get("AGENT_BRIDGE_LEASE_TTL_MS", "15000"))
LEASE_RENEW_SECONDS = float(os.environ.get("AGENT_BRIDGE_LEASE_RENEW_SECONDS", "5"))
# How often a standby replica checks whether the lease is free.
STANDBY_POLL_SECONDS = float(os.environ.get("AGENT_BRIDGE_STANDBY_POLL_SECONDS", "2"))
# Silence (no frame) during an open turn before Describe is asked about the workers.
WORKER_SILENCE_SECONDS = float(os.environ.get("AGENT_BRIDGE_WORKER_SILENCE_SECONDS", "8"))
WORKER_CHECK_EVERY_SECONDS = float(os.environ.get("AGENT_BRIDGE_WORKER_CHECK_SECONDS", "4"))
WORKER_CHECK_MAX_SECONDS = float(os.environ.get("AGENT_BRIDGE_WORKER_CHECK_MAX_SECONDS", "60"))


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
        self._roots: dict[str, str] = {}  # workflow id -> its root session (resolved)
        self._resolving: dict[str, asyncio.Task[None]] = {}

    @property
    def holder(self) -> str:
        return self._holder

    def is_forwarding(self, session: str) -> bool:
        """True while this replica holds the session's lease and is forwarding it."""
        return session in self._leading

    # -- public API ---------------------------------------------------------------

    async def root(self, workflow_id: str) -> str | None:
        """The root session of ``workflow_id`` (itself for a root), or None when it has no
        ownership record. Cached: a workflow's root never changes."""
        root = self._roots.get(workflow_id)
        if root is None:
            root = await self._store.root_of(workflow_id)
            if root is not None:
                if len(self._roots) > 10_000:
                    self._roots.clear()
                self._roots[workflow_id] = root
        return root

    def ensure(self, session: str) -> None:
        """Start (or wake) this replica's forwarder for ``session``'s root. If another replica
        holds the lease, the local one stands by while the session is busy or read here."""
        if self._closed:
            return
        root = self._roots.get(session)
        if root is not None:
            self._ensure_root(root)
        elif session not in self._resolving:
            self._resolving[session] = asyncio.create_task(self._resolve_and_ensure(session))

    async def _resolve_and_ensure(self, session: str) -> None:
        try:
            root = await self.root(session)
        finally:
            self._resolving.pop(session, None)
        if root is None:
            log.info("bridge %s: no ownership record; not forwarding", session)
            return
        self._ensure_root(root)

    def _ensure_root(self, root: str) -> None:
        if self._closed:
            return
        self._start_listener()
        self._wake.setdefault(root, asyncio.Event()).set()
        task = self._tasks.get(root)
        if task is None or task.done():
            self._tasks[root] = asyncio.create_task(self._run(root), name=f"bridge:{root}")

    async def wake(self, session: str) -> None:
        """New work for ``session`` (a send, an approval): wake the local forwarder of its root
        and every other replica's (whichever holds the lease forwards it)."""
        root = await self.root(session)
        if root is None:
            return
        self._ensure_root(root)
        try:
            await self._store.publish_wake(root, "send")
        except Exception as exc:  # noqa: BLE001 - local wake already happened
            log.warning("bridge %s: wake publish failed: %s", root, exc)

    @contextlib.asynccontextmanager
    async def reading(self, session: str) -> AsyncIterator[None]:
        """Hold this around an attach stream served from this replica (a subagent id counts
        for its root): the local forwarder then stands by to take over the session, and
        watches it while idle, while anyone here is reading it."""
        root = await self.root(session)
        if root is None:
            yield
            return
        self._readers[root] += 1
        self._ensure_root(root)
        try:
            yield
        finally:
            self._readers[root] -= 1
            if self._readers[root] <= 0:
                del self._readers[root]

    async def aclose(self) -> None:
        self._closed = True
        # Copy: finishing forwarders remove themselves from _tasks.
        tasks = list(self._tasks.values()) + list(self._resolving.values())
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

    def has_reader(self, session: str) -> bool:
        return self._readers.get(session, 0) > 0


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
        self._ack_lock = asyncio.Lock()
        self._archive_failures = 0

    # -- main loop -------------------------------------------------------------------

    async def run(self, wake: asyncio.Event) -> None:
        """Attach until idle; then stop, or (with a local reader) watch cheaply for new work.
        Each attach costs two Queries (harness status and stream offset), so attaches happen
        only on a wake, on new history seen by Describe, or while retrying a failure."""
        meta = await self.store.meta(self.session)
        self.worker_state = (meta.worker or {}).get("state")
        failing_for = 0.0
        backoff = 1.0
        while True:
            wake.clear()
            try:
                forwarded = await self._forward_once()
                backoff, failing_for = 1.0, 0.0
            except (asyncio.CancelledError, LeaseLost):
                raise
            except Exception as exc:  # noqa: BLE001 - workers down, network: retry
                if await self.b._session_closed(self.session):
                    await self.store.clear_pending(self.session)
                    await self._settle()
                    return
                if failing_for >= IDLE_STOP_SECONDS:
                    return  # workers gone for good; the next send or attach restarts us
                log.info("bridge %s: attach failed (%s); retrying in %.0fs", self.session, exc, backoff)
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(wake.wait(), timeout=backoff)
                failing_for += backoff
                backoff = min(backoff * 2, ATTACH_RETRY_MAX_SECONDS)
                continue
            await self._settle()
            if not forwarded and (await self.store.meta(self.session)).pending:
                # Attach on a closed, idle workflow returns nothing without raising.
                if await self.b._session_closed(self.session):
                    await self.store.clear_pending(self.session)
                    return
            if wake.is_set():
                continue  # a send arrived while we were attached
            if not self.b.has_reader(self.session):
                return  # idle and caught up: stop until the next wake
            if not await self._watch_idle(wake):
                return

    async def _watch_idle(self, wake: asyncio.Event) -> bool:
        """Idle with a local reader: wait for a wake, or for Describe to show new history
        (work started outside our routes). True to attach again, False to stop."""
        baseline = await self._history_length()
        interval = IDLE_WATCH_SECONDS
        waited = 0.0
        while self.b.has_reader(self.session) and waited < IDLE_STOP_SECONDS:
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(wake.wait(), timeout=interval)
            if wake.is_set():
                return True
            waited += interval
            interval = min(interval * 2, IDLE_WATCH_MAX_SECONDS)
            if (await self.store.meta(self.session)).pending:
                return True
            length = await self._history_length()
            if length == -1:
                return False  # the workflow is gone
            if length is None:
                continue  # Describe failed this time; try again later
            if baseline is None:
                baseline = length
            elif length > baseline:
                return True
        return False

    async def _history_length(self) -> int | None:
        """The workflow's history length from Describe; -1 if it no longer exists, None if
        Describe failed."""
        try:
            desc = await self.temporal.get_workflow_handle(self.session).describe()
        except RPCError as exc:
            if exc.status == RPCStatusCode.NOT_FOUND:
                return -1
            log.info("bridge %s: describe failed: %s", self.session, exc)
            return None
        except Exception as exc:  # noqa: BLE001
            log.info("bridge %s: describe failed: %s", self.session, exc)
            return None
        return int(getattr(desc, "history_length", 0) or 0)

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
        # Frames already stored at or after where we restart: root frames are known by their
        # event offset, the others by content, counted (identical frames can legitimately
        # repeat, e.g. two equal deltas published in one workflow task).
        seen_roots: set[int] = set()
        seen = Counter()
        for f in await self.store.read_from(self.session, from_offset):
            if f.event.startswith("x_"):
                continue
            if "event_offset" in f.data:
                seen_roots.add(int(f.data["event_offset"]))
            else:
                seen[_digest(f.event, f.data)] += 1
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
                if is_root:
                    dup = data["event_offset"] in seen_roots
                else:
                    digest = _digest(event, data)
                    dup = seen[digest] > 0
                    if dup:
                        seen[digest] -= 1
                root_turn_end = is_root and event == "turn_end"
                if root_turn_end and (not dup or resume_offset > meta.acked):
                    # Hold attaches open until this turn's telemetry is confirmed (bounded).
                    await self.store.set_tracking(
                        self.session, confirming_until=time.time() + CONFIRM_TIMEOUT_SECONDS
                    )
                if not dup:
                    # (A dup was stored by the previous holder. It is still folded below: that
                    # holder may have died between storing a frame and saving its meta.)
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
                    await self.store.register_subagent(
                        data["workflow_id"], self.session, agent_id=data.get("subagent_id")
                    )
                    self.b._roots[data["workflow_id"]] = self.session
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
        try:
            async with asyncio.timeout(CONFIRM_TIMEOUT_SECONDS):
                if prior:
                    await asyncio.wait(prior)
                await self._job(job)
        except TimeoutError:
            log.warning("bridge %s: turn telemetry not confirmed within %.0fs; giving up",
                        self.session, CONFIRM_TIMEOUT_SECONDS)
        finally:
            with contextlib.suppress(Exception):
                await self.store.set_tracking(self.session, confirming_until=0)
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
        """Archive what's stored since the last archive, then ack what is both stored and
        archived. Serialised: a turn end and an idle settle never run it concurrently."""
        async with self._ack_lock:
            await self._archive_and_ack_locked()

    async def _archive_and_ack_locked(self) -> None:
        meta = await self.store.meta(self.session)
        archived = meta.archived
        through = f"{self.pos[0] + 1}-{self.pos[1]}"
        if self.pos[1] >= 0 and archived != through:
            try:
                frames = await self.store.read_range(self.session, after=archived, through=through)
                if frames:
                    await self.store.archive.put(self.session, frames, turn=meta.current_turn or None)
                    await self.store.set_tracking(self.session, archived=frames[-1].entry_id)
                    archived = frames[-1].entry_id
                self._archive_failures = 0
            except Exception as exc:  # noqa: BLE001 - Redis still holds them; the next run retries
                self._archive_failures += 1
                level = logging.WARNING if self._archive_failures >= 2 else logging.INFO
                log.log(level, "bridge %s: archive failed (%d in a row; ack held back): %s",
                        self.session, self._archive_failures, exc)
        # Truncation must never drop frames the archive doesn't hold: ack only what is both
        # stored (highest root offset) and archived (the archive cursor's position).
        archived_offset = int(archived.split("-")[0]) - 1
        offset = min(max(self.highest_root, meta.head), archived_offset)
        if offset > meta.acked:
            try:
                handle = self.temporal.get_workflow_handle(self.session)
                await handle.signal(STREAM_ACKED_SIGNAL, StreamAck(offset=offset))
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
        interval = WORKER_CHECK_EVERY_SECONDS
        frame_seen = self.last_frame_at
        while True:
            await asyncio.sleep(1.0)
            now = time.monotonic()
            if self.last_frame_at != frame_seen:
                frame_seen, interval = self.last_frame_at, WORKER_CHECK_EVERY_SECONDS
            silent = now - self.last_frame_at
            if not self.turn_active or silent < WORKER_SILENCE_SECONDS:
                continue
            if self.tel.awaiting_human:
                continue  # waiting on an approval or a callback: silence is expected
            if now - last_check < interval:
                continue
            last_check = now
            # Back off while the silence lasts; a new frame resets the cadence.
            interval = min(interval * 2, WORKER_CHECK_MAX_SECONDS)
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
