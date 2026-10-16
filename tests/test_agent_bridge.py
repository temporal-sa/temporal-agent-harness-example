"""The Redis bridge and store (P2 step 2.2): subagent registry, bounded done set, fenced
writes, wake across replicas, standby takeover, stream_acked, the frame archive and backfill,
root-only status folding.

Uses the real Redis at TEST_AGENT_REDIS_URL (default redis://127.0.0.1:6390/0) under a unique
key prefix, a fake Temporal client and a scripted ``AgentClient.attach``. Skipped when Redis
isn't reachable.
"""

from __future__ import annotations

import asyncio
import os
import tempfile
import time
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock

import redis
import redis.asyncio as aredis
from temporalio.client import WorkflowExecutionStatus

from temporal_agent_harness.harness.agent_protocol import AgentEvent
from temporal_agent_harness.harness.agent_protocol.events import (
    MessageAccepted,
    MessageHandlerStart,
    ModelInteractionEnded,
    ModelInteractionStarted,
    ReplyDelta,
    SubagentStarted,
    TokenUsage,
    TurnEnded,
    TurnStarted,
)

from simple_chat_agent.api import agent_bridge, agent_store
from simple_chat_agent.api.agent_bridge import Bridge, _Forwarder
from simple_chat_agent.api.agent_store import AgentStore, FrameArchive, LeaseLost, SessionMeta, _DiskArchive

REDIS_URL = os.environ.get("TEST_AGENT_REDIS_URL", "redis://127.0.0.1:6390/0")


def _redis_up() -> bool:
    try:
        redis.Redis.from_url(REDIS_URL, socket_connect_timeout=0.5).ping()
        return True
    except Exception:  # noqa: BLE001
        return False


REDIS_UP = _redis_up()


# -- fakes -------------------------------------------------------------------------------


class FakeHandle:
    def __init__(self, temporal: FakeTemporal, wid: str) -> None:
        self.t, self.id = temporal, wid

    async def signal(self, name: str, arg: Any = None) -> None:
        self.t.signals.append((self.id, name, arg))

    async def describe(self) -> Any:
        return SimpleNamespace(status=WorkflowExecutionStatus.RUNNING, run_id="r1", task_queue="tq",
                               raw_description=SimpleNamespace(pending_activities=[], HasField=lambda _f: False))


class FakeTemporal:
    namespace = "default"

    def __init__(self) -> None:
        self.signals: list[tuple[str, str, Any]] = []

    def get_workflow_handle(self, wid: str) -> FakeHandle:
        return FakeHandle(self, wid)


ROOT = "root"
CHILD = "root.sub1"


def item(event: Any, *, agent: str = ROOT, turn: str = "t1", n: int = 1, msg: str | None = "m1",
         ts: float | None = None) -> AgentEvent:
    return AgentEvent(agent_id=agent, turn_id=turn, turn_number=n, message_id=msg,
                      timestamp=ts if ts is not None else time.time(), event=event)


class Script:
    """A scripted merged stream: (item, resume_offset) pairs, with the stream offset of each
    root item (resume_offset - 1), as ``attach`` would replay them from ``from_offset``."""

    def __init__(self, entries: list[tuple[AgentEvent, int]]) -> None:
        self.entries = entries
        self.calls: list[int] = []
        self.block: asyncio.Event | None = None  # hold the stream open after the last entry

    def client(self, *_a: Any) -> Any:
        script = self

        class C:
            async def attach(self, *, on_item: Any, from_offset: int = 0, **_k: Any) -> Any:
                script.calls.append(from_offset)

                async def gen():
                    prev = -1
                    for it, ro in script.entries:
                        is_root = ro > prev
                        prev = max(prev, ro)
                        if (is_root and ro - 1 >= from_offset) or (not is_root and ro >= from_offset and ro > 0):
                            yield on_item(it, ro)
                    if script.block is not None:
                        await script.block.wait()

                return gen()

        return C()


def turn_script(with_child: bool = True) -> list[tuple[AgentEvent, int]]:
    t0 = 1000.0
    entries: list[tuple[AgentEvent, int]] = [
        (item(MessageAccepted(handler="chat", payload={}, disposition="opened"), ts=t0), 1),
        (item(TurnStarted(), msg=None, ts=t0 + 1), 2),
        (item(MessageHandlerStart(), ts=t0 + 2), 3),
        (item(ModelInteractionStarted(model="claude"), ts=t0 + 3), 4),
        (item(ReplyDelta(text="Hello "), ts=t0 + 4), 5),
        (item(ModelInteractionEnded(model="claude", usage=TokenUsage(input_tokens=10, output_tokens=2)), ts=t0 + 5), 6),
    ]
    if with_child:
        entries += [
            (item(SubagentStarted(subagent_id=CHILD, agent_key="helper", workflow_id="child-wf-1"), ts=t0 + 6), 7),
            # Child frames share the root position before them.
            (item(TurnStarted(), agent=CHILD, turn="ct", msg=None, ts=t0 + 7), 7),
            (item(ReplyDelta(text="child says hi"), agent=CHILD, turn="ct", ts=t0 + 8), 7),
            (item(TurnEnded(), agent=CHILD, turn="ct", msg=None, ts=t0 + 9), 7),
        ]
    entries.append((item(TurnEnded(), msg=None, ts=t0 + 10), 8 if with_child else 7))
    return entries


@unittest.skipUnless(REDIS_UP, f"Redis not reachable at {REDIS_URL}")
class _RedisCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.prefix = f"aah-ut-{uuid.uuid4().hex[:8]}"
        patcher = mock.patch.dict(os.environ, {"AGENT_REDIS_PREFIX": self.prefix})
        patcher.start()
        self.addCleanup(patcher.stop)
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.r = aredis.from_url(REDIS_URL, decode_responses=True)
        self.archive = FrameArchive(_DiskArchive(Path(self.tmp.name)), "frames")
        self.store = AgentStore(self.r, archive=self.archive)
        self.temporal = FakeTemporal()
        self.bridges: list[Bridge] = []
        # Telemetry jobs need real history; covered by tests/test_agent_telemetry.py.
        p = mock.patch.object(agent_bridge.telemetry, "run_job", mock.AsyncMock(return_value=[]))
        p.start()
        self.addCleanup(p.stop)

    async def asyncTearDown(self) -> None:
        for b in self.bridges:
            await b.aclose()
        keys = [k async for k in self.r.scan_iter(f"{self.prefix}:*")]
        if keys:
            await self.r.delete(*keys)
        await self.r.aclose()

    def bridge(self) -> Bridge:
        b = Bridge(self.temporal, self.store)  # type: ignore[arg-type]
        self.bridges.append(b)
        return b


class StoreTests(_RedisCase):
    async def test_subagent_registry(self) -> None:
        self.assertIsNone(await self.store.root_of("s1"))
        await self.store.set_owner("s1", "alice")
        self.assertEqual(await self.store.root_of("s1"), "s1")
        await self.store.register_subagent("s1-child", "s1")
        await self.store.register_subagent("s1-grandchild", "s1-child")  # resolves to the root
        self.assertEqual(await self.store.root_of("s1-child"), "s1")
        self.assertEqual(await self.store.root_of("s1-grandchild"), "s1")
        self.assertIsNone(await self.store.root_of("other"))
        ttl = await self.r.ttl(self.store._k("s1-child", "root"))
        self.assertGreater(ttl, 3600)

    async def test_done_set_is_bounded_and_trims_oldest(self) -> None:
        with mock.patch.object(agent_store, "DONE_CAP", 5):
            for i in range(8):
                await self.store.save_meta("s", SessionMeta(), pending_removed=f"m{i}")
                await asyncio.sleep(0.002)
        done = await self.r.zrange(self.store._k("s", "dz"), 0, -1)
        self.assertEqual(done, [f"m{i}" for i in range(3, 8)])
        # A retry of a remembered id is not re-added to pending; a trimmed one can be.
        await self.store.note_sent("s", "m7")
        await self.store.note_sent("s", "m0")
        self.assertEqual((await self.store.meta("s")).pending, {"m0"})

    async def test_fenced_append(self) -> None:
        self.assertTrue(await self.store.acquire_lease("s", "me"))
        self.assertTrue(await self.store.append("s", "e", {"a": 1}, 1, 0, holder="me"))
        self.assertFalse(await self.store.append("s", "e", {"a": 1}, 1, 0, holder="me"))  # same position
        await self.store.release_lease("s", "me")
        self.assertTrue(await self.store.acquire_lease("s", "them"))
        with self.assertRaises(LeaseLost):
            await self.store.append("s", "e", {"a": 2}, 2, 0, holder="me")
        self.assertEqual(await self.store.last_position("s"), (1, 0))

    async def test_emitted_ids_are_bounded(self) -> None:
        with mock.patch.object(agent_store, "EMITTED_CAP", 3):
            self.assertTrue(await self.store.mark_emitted("s", "a"))
            self.assertFalse(await self.store.mark_emitted("s", "a"))
            for k in "bcd":
                await self.store.mark_emitted("s", k)
        self.assertEqual(await self.r.zcard(self.store._k("s", "x")), 3)

    async def test_archive_and_backfill_after_redis_loss(self) -> None:
        for ro in range(1, 6):
            await self.store.append("s", "reply_delta", {"text": str(ro)}, ro, 0)
        frames = await self.store.read("s", after="0-0")
        await self.archive.put("s", frames[:3], turn=1)
        await self.archive.put("s", frames[2:], turn=2)  # overlapping re-archive after a takeover
        self.assertEqual(await self.archive.last_position("s"), (5, 0))
        self.assertEqual([f.entry_id for f in await self.archive.read("s", after="3-0")],
                         ["4-0", "5-0", "6-0"])
        # Redis loses the stream; the forwarder rebuilds from a later position.
        await self.r.delete(self.store._k("s", "f"))
        self.assertEqual([f.data["text"] for f in await self.store.read_backfilled("s", after="0-0")],
                         ["1", "2", "3", "4", "5"])
        await self.store.append("s", "reply_delta", {"text": "6"}, 6, 0)
        got = await self.store.read_backfilled("s", after="2-0")
        self.assertEqual([f.data["text"] for f in got], ["2", "3", "4", "5", "6"])
        got = await self.store.read_backfilled("s", after="0-0", count=2)
        self.assertEqual([f.data["text"] for f in got], ["1", "2"])

    async def test_complete_stream_does_not_touch_the_archive(self) -> None:
        await self.store.append("s", "e", {}, 1, 0)
        with mock.patch.object(self.archive, "read", side_effect=AssertionError("archive read")):
            self.assertEqual(len(await self.store.read_backfilled("s", after="0-0")), 1)

    async def test_s3_archive_from_env(self) -> None:
        with mock.patch.dict(os.environ, {"SIMPLE_CHAT_S3_BUCKET": "bkt", "AGENT_ARCHIVE_PREFIX": "p/x"}):
            a = FrameArchive.from_env()
        self.assertEqual(a.describe(), "s3://bkt/p/x")
        with mock.patch.dict(os.environ, {"SIMPLE_CHAT_S3_BUCKET": "", "SIMPLE_CHAT_ARTIFACT_DIR": self.tmp.name}):
            a = FrameArchive.from_env()
        self.assertTrue(a.describe().startswith(f"file://{self.tmp.name}"))


class ForwarderTests(_RedisCase):
    async def forward(self, script: Script, session: str = "s") -> Bridge:
        b = self.bridge()
        self.assertTrue(await self.store.acquire_lease(session, b.holder))
        fwd = _Forwarder(b, session)
        with mock.patch.object(agent_bridge, "AgentClient", script.client):
            await fwd._forward_once()
            await fwd._settle()
        return b

    async def test_turn_is_stored_folded_registered_archived_and_acked_once(self) -> None:
        await self.store.set_owner("s", "alice")
        script = Script(turn_script())
        await self.forward(script)
        frames = await self.store.read_from("s", 0)
        harness = [f for f in frames if not f.event.startswith("x_")]
        self.assertEqual(len(harness), len(script.entries))
        # event_offset only on root frames; child frames sit at the root position before them.
        roots = [f for f in harness if "event_offset" in f.data]
        self.assertEqual([f.data["event_offset"] for f in roots], list(range(0, 8)))
        self.assertTrue(all(f.data["agent_id"] == ROOT for f in roots))
        self.assertEqual({f.entry_id for f in harness if f.data["agent_id"] == CHILD}, {"8-1", "8-2", "8-3"})
        # The child's turn_end did not end the root turn early; the root's did.
        meta = await self.store.meta("s")
        self.assertFalse(meta.turn_active)
        self.assertIsNone(meta.turn_start)
        self.assertEqual(meta.head, 8)
        # Subagent registered under its root.
        self.assertEqual(await self.store.root_of("child-wf-1"), "s")
        # Live x_usage stored after the model call.
        self.assertEqual(len([f for f in frames if f.event == "x_usage"]), 1)
        # One ack with the highest stored offset; the idle settle didn't send another.
        acks = [s for s in self.temporal.signals if s[1] == "stream_acked"]
        self.assertEqual(len(acks), 1)
        self.assertEqual(acks[0][2].offset, 8)
        self.assertEqual(meta.acked, 8)
        # Archived, and the archive cursor moved to the last frame.
        archived = await self.archive.read("s")
        self.assertEqual([f.entry_id for f in archived], [f.entry_id for f in frames])
        self.assertEqual(meta.archived, frames[-1].entry_id)

    async def test_child_turn_end_alone_keeps_root_turn_active(self) -> None:
        entries = [e for e in turn_script() if not (e[0].agent_id == ROOT and e[0].event.type.value == "turn_end")]
        await self.forward(Script(entries))
        meta = await self.store.meta("s")
        self.assertTrue(meta.turn_active)
        self.assertEqual(meta.turn_start, 1)
        # No turn end: frames were acked at idle, once.
        self.assertEqual(len([s for s in self.temporal.signals if s[1] == "stream_acked"]), 1)

    async def test_takeover_replays_the_open_turn_without_duplicates(self) -> None:
        entries = turn_script()
        # Replica A stored the turn up to the child's first frame, then died (lease expired).
        full = Script(entries)
        partial = Script(entries[:8])
        await self.forward(partial)
        before = [f for f in await self.store.read_from("s", 0) if not f.event.startswith("x_")]
        await self.store.r.delete(self.store._k("s", "lease"))
        # Replica B: replays from the open turn's start, stores only what's missing.
        await self.forward(full)
        self.assertEqual(full.calls[0], 1)  # the turn_started's stream offset
        frames = [f for f in await self.store.read_from("s", 0) if not f.event.startswith("x_")]
        self.assertEqual(len(frames), len(entries))
        self.assertEqual(len({agent_bridge._digest(f.event, f.data) for f in frames}), len(entries))
        # What replica A stored is untouched; B only appended.
        self.assertEqual([f.entry_id for f in frames[: len(before)]], [f.entry_id for f in before])
        positions = [agent_store.parse_entry_id(f.entry_id) for f in frames]
        self.assertEqual(positions, sorted(positions))

    async def test_takeover_after_holder_died_between_frame_and_meta(self) -> None:
        """Replica A stored the root turn_end but died before saving meta and acking. B's
        replay finds it already stored, still folds it, and confirms and acks the turn."""
        await self.forward(Script(turn_script()))
        meta = await self.store.meta("s")
        meta.turn_active, meta.turn_start = True, 1
        await self.store.save_meta("s", meta)
        await self.r.delete(self.store._k("s", "t"), self.store._k("s", "lease"))
        self.temporal.signals.clear()
        agent_bridge.telemetry.run_job.reset_mock()
        await self.forward(Script(turn_script()))
        meta = await self.store.meta("s")
        self.assertFalse(meta.turn_active)
        self.assertEqual([s[2].offset for s in self.temporal.signals if s[1] == "stream_acked"], [8])
        kinds = [c.args[2].kind for c in agent_bridge.telemetry.run_job.await_args_list]
        self.assertIn("turn_confirm", kinds)
        frames = [f for f in await self.store.read_from("s", 0) if not f.event.startswith("x_")]
        self.assertEqual(len(frames), len(turn_script()))

    async def test_ack_to_an_ended_workflow_is_not_retried(self) -> None:
        from temporalio.service import RPCError, RPCStatusCode

        calls = []

        async def gone(self_: Any, name: str, arg: Any = None) -> None:
            calls.append(name)
            raise RPCError("workflow not found", RPCStatusCode.NOT_FOUND, b"")

        with mock.patch.object(FakeHandle, "signal", gone):
            b = self.bridge()
            await self.store.acquire_lease("s", b.holder)
            fwd = _Forwarder(b, "s")
            with mock.patch.object(agent_bridge, "AgentClient", Script(turn_script(False)).client):
                await fwd._forward_once()
                await fwd._settle()
                await fwd._settle()
        self.assertEqual(calls, ["stream_acked"])
        self.assertEqual((await self.store.meta("s")).acked, 7)

    async def test_lease_loss_stops_writes(self) -> None:
        b = self.bridge()
        self.assertTrue(await self.store.acquire_lease("s", b.holder))
        fwd = _Forwarder(b, "s")
        await self.store.r.set(self.store._k("s", "lease"), "someone-else")
        with mock.patch.object(agent_bridge, "AgentClient", Script(turn_script()).client):
            with self.assertRaises(LeaseLost):
                await fwd._forward_once()
        self.assertEqual(await self.store.read_from("s", 0), [])


class WakeAndStandbyTests(_RedisCase):
    async def test_wake_reaches_the_replica_holding_the_session(self) -> None:
        a, b = self.bridge(), self.bridge()
        event = asyncio.Event()
        a._wake["s"] = event
        a._tasks["s"] = asyncio.create_task(asyncio.sleep(30))
        self.addCleanup(a._tasks["s"].cancel)
        a._start_listener()
        await asyncio.sleep(0.3)  # subscribed
        await self.store.acquire_lease("s", "elsewhere")  # b's own forwarder won't lead
        await b.wake("s")
        await asyncio.wait_for(event.wait(), timeout=2)

    async def test_standby_takes_over_when_the_lease_is_released(self) -> None:
        script = Script(turn_script(with_child=False))
        script.block = asyncio.Event()
        await self.store.acquire_lease("s", "replica-a")
        b = self.bridge()
        with mock.patch.object(agent_bridge, "AgentClient", script.client), \
                mock.patch.object(agent_bridge, "STANDBY_POLL_SECONDS", 30.0):
            async with b.reading("s"):
                await asyncio.sleep(0.3)
                self.assertFalse(b.is_forwarding("s"))
                # Replica A shuts down gracefully: releases and announces it.
                await self.store.release_lease("s", "replica-a")
                await self.store.publish_wake("s", "released")
                t0 = time.monotonic()
                while not b.is_forwarding("s"):
                    await asyncio.sleep(0.02)
                    self.assertLess(time.monotonic() - t0, 3)
                self.assertEqual(await self.store.lease_holder("s"), b.holder)
                script.block.set()

    async def test_standby_takes_over_an_expired_lease(self) -> None:
        script = Script(turn_script(with_child=False))
        script.block = asyncio.Event()
        await self.store.acquire_lease("s", "replica-a", ttl_ms=600)  # replica A died
        b = self.bridge()
        with mock.patch.object(agent_bridge, "AgentClient", script.client), \
                mock.patch.object(agent_bridge, "STANDBY_POLL_SECONDS", 0.2):
            async with b.reading("s"):
                t0 = time.monotonic()
                while not b.is_forwarding("s"):
                    await asyncio.sleep(0.05)
                    self.assertLess(time.monotonic() - t0, 3)
                script.block.set()

    async def test_no_standby_without_readers_or_work(self) -> None:
        await self.store.acquire_lease("s", "replica-a")
        b = self.bridge()
        b.ensure("s")
        task = b._tasks["s"]
        await asyncio.wait_for(task, timeout=2)
        self.assertNotIn("s", b._tasks)


if __name__ == "__main__":
    unittest.main()
