"""P2 step 2.2: the bridge against real Temporal, Redis and Anthropic.

    RUN_INTEGRATION=1 SIMPLE_CHAT_REDIS_URL=redis://127.0.0.1:6390/0 \
        .venv/bin/python -m unittest tests.integration.test_p2_bridge -v

Covers 2.2a (one Temporal reader for three readers), 2.2c (lease takeover after the forwarding
API process is SIGKILLed), 2.2d (x_recovery after a worker SIGKILL, confirmed from history) and
2.2e (x_usage reconciles with history, with cache writes). Workers that get killed run as
``simple_chat_agent.agent.dev_worker`` subprocesses; the killed API replica runs this module as
a subprocess (``python -m tests.integration.test_p2_bridge replica <session>``).
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest
import uuid
from typing import Any

from tests.integration import support

HAIKU = {"provider": "anthropic", "model": "claude-haiku-5-5", "effort": "low"}
ESSAY = (
    "Write a detailed essay of about 900 words on why durable execution matters for AI "
    "agents. Plain prose, no tools."
)
TOOL_THEN_ESSAY = (
    "First call wait_seconds with seconds=1. After it returns, write a detailed essay of about "
    "900 words on why durable execution matters for AI agents (plain prose, no more tools)."
)
LONG_TURN = (
    "Say one short sentence about what you are about to do. Then call wait_seconds with "
    "seconds=20. After it returns, write about 300 words on why durable execution matters for "
    "AI agents (plain prose, no more tools)."
)
POLL_UPDATE = "__temporal_workflow_stream_poll"


def _env() -> None:
    os.environ.setdefault("SIMPLE_CHAT_REDIS_URL", "redis://127.0.0.1:6390/0")


def _digest(event: str, data: dict[str, Any]) -> str:
    body = {k: v for k, v in data.items() if k not in ("event_offset", "resume_offset")}
    return hashlib.sha256(f"{event}|{json.dumps(body, sort_keys=True)}".encode()).hexdigest()


def _ts(pb: Any) -> float:
    return pb.seconds + pb.nanos / 1e9


async def history_events(client: Any, workflow_id: str) -> list[Any]:
    return [e async for e in client.get_workflow_handle(workflow_id).fetch_history_events()]


async def model_activities(client: Any, workflow_id: str) -> list[dict[str, Any]]:
    """Every activity in the (latest run's) history, independently of agent_telemetry."""
    acts: dict[int, dict[str, Any]] = {}
    for ev in await history_events(client, workflow_id):
        if ev.HasField("activity_task_scheduled_event_attributes"):
            a = ev.activity_task_scheduled_event_attributes
            acts[ev.event_id] = {"name": a.activity_type.name, "activity_id": a.activity_id,
                                 "scheduled": ev.event_id, "attempt": 1, "completed": None,
                                 "completed_at": None, "result": None, "identity": None, "failure": None}
        elif ev.HasField("activity_task_started_event_attributes"):
            a = ev.activity_task_started_event_attributes
            acts[a.scheduled_event_id].update(attempt=a.attempt, identity=a.identity,
                                              failure=a.last_failure if a.HasField("last_failure") else None)
        elif ev.HasField("activity_task_completed_event_attributes"):
            a = ev.activity_task_completed_event_attributes
            payload = a.result.payloads[0] if a.result.payloads else None
            acts[a.scheduled_event_id].update(
                completed=ev.event_id, completed_at=_ts(ev.event_time),
                result=(await client.data_converter.decode([payload]))[0] if payload else None,
            )
    return sorted(acts.values(), key=lambda r: r["scheduled"])


def is_model(name: str) -> bool:
    return name.endswith("__model_request_stream")


def is_tool(name: str) -> bool:
    return not is_model(name) and not name.endswith("__event_stream_handler") and name not in (
        "judge_tool_call", "run_subagent_turn")


async def stored(store: Any, session: str) -> list[Any]:
    return await store.read_from(session, 0)


def of(frames: list[Any], kind: str) -> list[Any]:
    return [f for f in frames if f.event == kind]


async def wait_until(pred, timeout: float, what: str, interval: float = 0.2):
    deadline = time.monotonic() + timeout
    while True:
        result = await pred()
        if result:
            return result
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out waiting for {what}")
        await asyncio.sleep(interval)


class _Base(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        _env()
        self.prefix = f"aah-p2b-{uuid.uuid4().hex[:6]}"
        os.environ["AGENT_REDIS_PREFIX"] = self.prefix
        self.archive_dir = tempfile.mkdtemp(prefix="p2b-archive-")
        os.environ["AGENT_ARCHIVE_PREFIX"] = f"frames-{self.prefix}"
        from simple_chat_agent.api.agent_store import AgentStore, FrameArchive, _DiskArchive
        from pathlib import Path

        self.client = await support.connect()
        self.archive = FrameArchive(_DiskArchive(Path(self.archive_dir)), f"frames-{self.prefix}")
        self.store = AgentStore(AgentStore.from_env().r, archive=self.archive)
        self.task_queue = f"p2b-{uuid.uuid4().hex[:6]}"
        self.procs: list[subprocess.Popen] = []
        self.bridges: list[Any] = []

    async def asyncTearDown(self) -> None:
        for b in self.bridges:
            with contextlib.suppress(Exception):
                await b.aclose()
        for p in self.procs:
            if p.poll() is None:
                p.kill()
        with contextlib.suppress(Exception):
            await self.store.r.aclose()

    def bridge(self) -> Any:
        from simple_chat_agent.api.agent_bridge import Bridge

        b = Bridge(self.client, self.store)
        self.bridges.append(b)
        return b

    def dev_worker(self, name: str) -> subprocess.Popen:
        proc = subprocess.Popen(
            [sys.executable, "-m", "simple_chat_agent.agent.dev_worker", "--task-queue", self.task_queue,
             "--identity", name],
            cwd=str(support.ROOT), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        self.procs.append(proc)
        return proc

    async def start(self, **init: Any) -> Any:
        handle = await support.start_session(self.client, self.task_queue, **init)
        await self.store.set_owner(handle.id, "test-user")
        return handle

    async def wait_turn_end(self, session: str, n: int = 1, timeout: float = 240) -> list[Any]:
        async def done():
            frames = await stored(self.store, session)
            ends = [f for f in of(frames, "turn_end") if "event_offset" in f.data]
            return frames if len(ends) >= n else None

        return await wait_until(done, timeout, f"{n} root turn_end(s)")


@unittest.skipIf(support.skip_reason(), support.skip_reason() or "")
class OneReader(_Base):
    async def _turn_polls(self, readers: int) -> dict[str, Any]:
        handle = await self.start(model=HAIKU)
        bridges = [self.bridge() for _ in range(readers)]
        async with contextlib.AsyncExitStack() as stack:
            for b in bridges:
                await stack.enter_async_context(b.reading(handle.id))
            await support.send(handle, "chat", {"text": "Reply with three short sentences about rivers."})
            for b in bridges:
                await b.wake(handle.id)
            await self.wait_turn_end(handle.id, 1, timeout=120)
            await asyncio.sleep(2)
        accepted: dict[str, int] = {}
        outstanding, peak, total = 0, 0, 0
        for ev in await history_events(self.client, handle.id):
            if ev.HasField("workflow_execution_update_accepted_event_attributes"):
                a = ev.workflow_execution_update_accepted_event_attributes
                if a.accepted_request.input.name == POLL_UPDATE:
                    accepted[a.protocol_instance_id] = ev.event_id
                    total += 1
                    outstanding += 1
                    peak = max(peak, outstanding)
            elif ev.HasField("workflow_execution_update_completed_event_attributes"):
                a = ev.workflow_execution_update_completed_event_attributes
                if a.meta.update_id in accepted:
                    outstanding -= 1
        leaders = [b for b in bridges if b.is_forwarding(handle.id)]
        await handle.signal("close")
        return {"polls": total, "peak_concurrent": peak, "leaders": len(leaders)}

    async def test_2_2a_three_readers_one_subscription_chain(self) -> None:
        async with support.running_worker(self.client, self.task_queue):
            single = await self._turn_polls(1)
            triple = await self._turn_polls(3)
        print(f"\n[2.2a] poll Updates per turn: one reader {single}, three readers {triple}")
        self.assertEqual(triple["peak_concurrent"], 1, triple)
        self.assertLessEqual(triple["polls"], max(single["polls"] * 2, single["polls"] + 10), (single, triple))


@unittest.skipIf(support.skip_reason(), support.skip_reason() or "")
class Recovery(_Base):
    async def test_2_2d_sigkill_produces_confirmed_recovery(self) -> None:
        a = self.dev_worker("worker-a")
        await asyncio.sleep(4)
        handle = await self.start(model=HAIKU, test_tools=True)
        b = self.bridge()
        async with b.reading(handle.id):
            await support.send(handle, "chat", {"text": TOOL_THEN_ESSAY})
            await b.wake(handle.id)

            async def streaming_second_call():
                frames = await stored(self.store, handle.id)
                if len(of(frames, "model_interaction_started")) < 2:
                    return None
                second = max(f.entry_id for f in of(frames, "model_interaction_started"))
                text = "".join(f.data.get("text", "") for f in of(frames, "reply_delta") if f.entry_id > second)
                return len(text) > 150

            await wait_until(streaming_second_call, 120, "the second model call to stream")
            os.kill(a.pid, signal.SIGKILL)
            killed_at = time.time()
            self.dev_worker("worker-b")

            async def recovery(confirmed: bool):
                return [f for f in of(await stored(self.store, handle.id), "x_recovery")
                        if f.data["confirmed"] is confirmed]

            live = await wait_until(lambda: recovery(False), 60, "a live x_recovery")
            confirmed = await wait_until(lambda: recovery(True), 120, "a confirmed x_recovery")
            await self.wait_turn_end(handle.id, 1)
            worker_frames = of(await stored(self.store, handle.id), "x_worker")

        acts = await model_activities(self.client, handle.id)
        retried = [r for r in acts if is_model(r["name"]) and r["attempt"] > 1]
        self.assertEqual(len(retried), 1, acts)
        r = retried[0]
        before = [x for x in acts if x["completed"] and x["completed"] < r["scheduled"]]
        kept_models = [x for x in before if is_model(x["name"])]
        kept = {
            "model_calls": len(kept_models),
            "tool_calls": len([x for x in before if is_tool(x["name"])]),
            "tokens_in": sum(x["result"]["usage"]["input_tokens"] for x in kept_models),
            "tokens_out": sum(x["result"]["usage"]["output_tokens"] for x in kept_models),
        }
        c = confirmed[-1].data
        self.assertTrue(c["confirmed"])
        self.assertFalse(c["retracted"])
        self.assertEqual(c["kept"], kept)
        self.assertEqual(c["repeated"]["model_calls"], r["attempt"] - 1)
        self.assertEqual(c["resumed_on"], r["identity"])
        self.assertEqual(c["cause"], "heartbeat_timeout")
        lag = c["timestamp"] - r["completed_at"]
        detect = live[0].data["timestamp"] - killed_at
        print(
            f"\n[2.2d] kill -> live x_recovery stored {detect:.1f}s; attempt 2 completed -> confirmed "
            f"{lag:.2f}s; kept={c['kept']} repeated={c['repeated']} lost_worker={c['lost_worker']} "
            f"resumed_on={c['resumed_on']} x_worker={[ (w.data['state'], w.data['reason']) for w in worker_frames]}"
        )
        self.assertLess(lag, 10.0)
        self.assertEqual(c["kept"]["model_calls"], 1)
        self.assertEqual(c["kept"]["tool_calls"], 1)
        await handle.signal("close")


@unittest.skipIf(support.skip_reason(), support.skip_reason() or "")
class Usage(_Base):
    async def test_2_2e_usage_matches_history_with_cache_writes(self) -> None:
        async with support.running_worker(self.client, self.task_queue):
            handle = await self.start(model=HAIKU)
            b = self.bridge()
            async with b.reading(handle.id):
                for i, text in enumerate(("Reply with exactly: one", "Reply with exactly: two")):
                    await support.send(handle, "chat", {"text": text})
                    await b.wake(handle.id)
                    await self.wait_turn_end(handle.id, i + 1, timeout=120)
                await asyncio.sleep(3)
            frames = await stored(self.store, handle.id)
        acts = [a for a in await model_activities(self.client, handle.id) if is_model(a["name"])]
        confirmed = [f.data for f in of(frames, "x_usage") if f.data["confirmed"]]
        live = [f.data for f in of(frames, "x_usage") if not f.data["confirmed"]]
        self.assertEqual(len(confirmed), len(acts), (confirmed, acts))
        fields = ("input_tokens", "output_tokens", "cache_read_tokens", "cache_write_tokens")
        by_turn: dict[int, dict[str, int]] = {}
        for d in confirmed:
            t = by_turn.setdefault(d["turn_number"], dict.fromkeys(fields, 0))
            for k in fields:
                t[k] += d[k]
        hist = dict.fromkeys(fields, 0)
        for a in acts:
            for k in fields:
                hist[k] += a["result"]["usage"][k]
        total = {k: sum(t[k] for t in by_turn.values()) for k in fields}
        print(f"\n[2.2e] per turn (confirmed x_usage): {by_turn}; history totals {hist}; "
              f"live frames {len(live)} (cache_write in live: {[d['cache_write_tokens'] for d in live]}); "
              f"thinking {[d['thinking_tokens'] for d in confirmed]}")
        self.assertEqual(total, hist)
        self.assertGreater(by_turn[min(by_turn)]["cache_write_tokens"] + by_turn[min(by_turn)]["cache_read_tokens"], 0)
        self.assertGreater(by_turn[max(by_turn)]["cache_read_tokens"], 0)
        self.assertTrue(all(d["thinking_tokens"] is None for d in confirmed))  # Anthropic doesn't split it
        # The ack and the archive happened once per turn.
        sigs = [e for e in await history_events(self.client, handle.id)
                if e.HasField("workflow_execution_signaled_event_attributes")
                and e.workflow_execution_signaled_event_attributes.signal_name == "stream_acked"]
        self.assertEqual(len(sigs), 2)
        archived = await self.archive.read(handle.id)
        self.assertEqual([f.entry_id for f in archived], [f.entry_id for f in frames])
        await handle.signal("close")


@unittest.skipIf(support.skip_reason(), support.skip_reason() or "")
class Takeover(_Base):
    def replica(self, session: str) -> subprocess.Popen:
        env = {**os.environ, "AGENT_REDIS_PREFIX": self.prefix}
        proc = subprocess.Popen(
            [sys.executable, "-m", "tests.integration.test_p2_bridge", "replica", session, self.archive_dir],
            cwd=str(support.ROOT), env=env, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
        )
        self.procs.append(proc)
        return proc

    async def test_2_2c_lease_takeover_no_gaps_no_duplicates(self) -> None:
        from temporal_agent_harness.harness.agent_client import AgentClient

        from simple_chat_agent.api.agent_bridge import sse_data

        async with support.running_worker(self.client, self.task_queue):
            handle = await self.start(model=HAIKU, test_tools=True)
            a = self.replica(handle.id)
            holder_a = await asyncio.to_thread(a.stdout.readline)
            holder_a = holder_a.strip().removeprefix("holder=")
            await wait_until(lambda: self._holder_is(handle.id, holder_a), 30, "replica A to hold the lease")
            b = self.bridge()
            async with b.reading(handle.id):
                await support.send(handle, "chat", {"text": LONG_TURN})
                await self.store.publish_wake(handle.id)

                async def mid_turn():
                    frames = await stored(self.store, handle.id)
                    return any(f.data.get("tool_name") == "wait_seconds" for f in of(frames, "tool_start"))

                await wait_until(mid_turn, 120, "replica A to store part of the turn")
                self.assertFalse(b.is_forwarding(handle.id))
                os.kill(a.pid, signal.SIGKILL)
                killed = time.monotonic()
                at_kill = len(await stored(self.store, handle.id))
                await wait_until(lambda: self._is(b, handle.id), 30, "replica B to take over", interval=0.1)
                takeover = time.monotonic() - killed
                at_takeover = await stored(self.store, handle.id)
                # The turn was still open when B took over: B stores its end and the essay.
                self.assertFalse([f for f in of(at_takeover, "turn_end") if "event_offset" in f.data])
                frames = await self.wait_turn_end(handle.id, 1)
                await asyncio.sleep(2)
                frames = await stored(self.store, handle.id)

            # The canonical merged stream, read once from Temporal after the fact.
            canonical: list[tuple[str, dict[str, Any]]] = []
            it = await AgentClient(self.client, handle.id).attach(on_item=lambda i, ro: (i, ro), from_offset=0)
            async for item, _ro in it:
                canonical.append(sse_data(item))
        harness = [f for f in frames if not f.event.startswith("x_")]
        stored_digests = [_digest(f.event, f.data) for f in harness]
        canonical_digests = [_digest(e, d) for e, d in canonical]
        root_offsets = [f.data["event_offset"] for f in harness if "event_offset" in f.data]
        print(f"\n[2.2c] takeover after SIGKILL of the forwarding replica: {takeover:.1f}s; "
              f"frames stored at kill {at_kill}, at takeover {len(at_takeover)}, at end {len(frames)}; "
              f"stored {len(harness)} harness frames, canonical {len(canonical)}; "
              f"root offsets {root_offsets[0]}..{root_offsets[-1]}")
        self.assertLess(takeover, 20.0)
        self.assertEqual(len(stored_digests), len(set(stored_digests)), "duplicates stored")
        self.assertEqual(sorted(stored_digests), sorted(canonical_digests), "gap or extra frame")
        self.assertEqual(root_offsets, list(range(root_offsets[0], root_offsets[0] + len(root_offsets))))
        positions = [f.resume_offset for f in frames]
        self.assertEqual(positions, sorted(positions))
        await handle.signal("close")

    async def _holder_is(self, session: str, holder: str) -> bool:
        return await self.store.lease_holder(session) == holder

    async def _is(self, b: Any, session: str) -> bool:
        return b.is_forwarding(session)


async def _replica(session: str, archive_dir: str) -> None:
    """A stand-in API replica: holds a reader on ``session`` and forwards it."""
    from pathlib import Path

    from simple_chat_agent.api.agent_bridge import Bridge
    from simple_chat_agent.api.agent_store import AgentStore, FrameArchive, _DiskArchive

    _env()
    client = await support.connect()
    store = AgentStore(
        AgentStore.from_env().r,
        archive=FrameArchive(_DiskArchive(Path(archive_dir)), os.environ["AGENT_ARCHIVE_PREFIX"]),
    )
    bridge = Bridge(client, store)
    print(f"holder={bridge.holder}", flush=True)
    async with bridge.reading(session):
        await asyncio.Event().wait()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "replica":
        asyncio.run(_replica(sys.argv[2], sys.argv[3]))
    else:
        unittest.main()
