"""Regression: a subagent showed "subagent stream unavailable" in a live run (P3).

Two causes, both reproduced with two parallel explorers (see the Eng G P3 report):

1. A subagent completes as soon as its parent stops it. A completed workflow's stream can't be
   polled, so a reader still behind lost the end of the subagent's turn. Fixed in the subagent:
   ``harness_ext.DrainableStream`` + ``drain_stream`` in its ``@agent.teardown``.
2. The harness merge gave a child up while it was still delivering, when the consumer was slow
   (the bridge writes each frame to Redis). Fixed by ``harness_ext.install_merge_fixes``.
"""

from __future__ import annotations

import asyncio
import uuid
import unittest
from collections.abc import AsyncIterator
from datetime import timedelta
from typing import Any
from unittest import mock

from temporalio import workflow
from temporalio.client import Client
from temporalio.contrib.workflow_streams import WorkflowStreamClient
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from temporal_agent_harness.harness.agent_protocol import AgentEvent
from temporal_agent_harness.harness.agent_protocol.events import (
    ReplyDelta,
    SubagentMessageSent,
    SubagentReplyReceived,
    TurnEnded,
    TurnStarted,
)
from temporal_agent_harness.harness.stream_merge import cursor as cursor_mod
from temporal_agent_harness.harness.stream_merge import merge as merge_mod

from simple_chat_agent.agent import harness_ext


# -- 1. drain before completing ------------------------------------------------------------


@workflow.defn(sandboxed=False)
class _ClosingStream:
    """Publishes a batch, then on ``finish`` publishes the rest and completes (as a subagent
    that is stopped right after its turn does)."""

    @workflow.init
    def __init__(self, drain: bool) -> None:
        self._stream = harness_ext.DrainableStream()
        self._finish = False

    @workflow.signal
    def finish(self) -> None:
        self._finish = True

    @workflow.run
    async def run(self, drain: bool) -> bool | None:
        topic = self._stream.topic("t", type=int)
        for i in range(3):
            topic.publish(i)
        await workflow.wait_condition(lambda: self._finish)
        for i in range(3, 8):
            topic.publish(i)
        if not drain:
            return None
        return await harness_ext.drain_stream(self._stream, timeout=timedelta(seconds=5))


class DrainTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.env = await WorkflowEnvironment.start_local()
        self.tq = f"drain-{uuid.uuid4().hex[:6]}"
        self.worker = Worker(self.env.client, task_queue=self.tq, workflows=[_ClosingStream])
        self.worker_task = asyncio.create_task(self.worker.run())

    async def asyncTearDown(self) -> None:
        await self.worker.shutdown()
        await self.env.shutdown()

    async def read_lagging(self, drain: bool) -> tuple[list[int], Any]:
        client: Client = self.env.client
        handle = await client.start_workflow(_ClosingStream.run, drain, id=f"drain-{uuid.uuid4().hex}", task_queue=self.tq)
        got: list[int] = []
        sub = WorkflowStreamClient.create(client, handle.id).subscribe(
            topics=["t"], result_type=int, poll_cooldown=timedelta(milliseconds=500)
        )
        async for item in sub:
            got.append(item.data)
            if item.data == 2:
                # The reader is behind (cooling down) when the workflow finishes.
                await handle.signal(_ClosingStream.finish)
        return got, await handle.result()

    async def test_reader_behind_at_completion_gets_every_event(self) -> None:
        got, caught_up = await self.read_lagging(drain=True)
        self.assertEqual(got, list(range(8)))
        self.assertTrue(caught_up)

    async def test_without_draining_the_reader_loses_the_end(self) -> None:
        """The failure the drain fixes (the stream of a completed workflow can't be read)."""
        got, _ = await self.read_lagging(drain=False)
        self.assertEqual(got, [0, 1, 2])


# -- 2. the merge keeps a slow consumer's child -----------------------------------------------


ROOT = "root-wf"


def ev(event: Any, *, agent: str, turn: int, turn_id: str = "t") -> AgentEvent:
    return AgentEvent(agent_id=agent, turn_id=turn_id, turn_number=turn, message_id=None, timestamp=0.0, event=event)


class _Item:
    def __init__(self, data: AgentEvent, offset: int) -> None:
        self.data, self.offset, self.topic = data, offset, "turn_events"


def _scripted(events: list[AgentEvent], delay: float, hold: asyncio.Event | None = None) -> AsyncIterator[_Item]:
    async def gen() -> AsyncIterator[_Item]:
        for i, e in enumerate(events):
            await asyncio.sleep(delay)
            yield _Item(e, i)
        if hold is not None:
            await hold.wait()  # a live stream: never ends on its own

    return gen()


class MergeReapTests(unittest.IsolatedAsyncioTestCase):
    async def merged(self) -> list[str]:
        """Two explorers streaming at once into a slow consumer (the live-run shape)."""

        def sent(sub: str) -> SubagentMessageSent:
            return SubagentMessageSent(subagent_id=sub, agent_key="explorer", workflow_id=f"wf-{sub}",
                                       handler="investigate", subagent_turn=1, from_offset=0)

        def reply(sub: str) -> SubagentReplyReceived:
            return SubagentReplyReceived(subagent_id=sub, agent_key="explorer", workflow_id=f"wf-{sub}",
                                         handler="investigate", subagent_turn=1, outcome="ok")

        def child(sub: str) -> list[AgentEvent]:
            events = [ev(TurnStarted(), agent=sub, turn=1, turn_id="c")]
            events += [ev(ReplyDelta(text=f"{i} "), agent=sub, turn=1, turn_id="c") for i in range(12)]
            return events + [ev(TurnEnded(), agent=sub, turn=1, turn_id="c")]

        root = [ev(TurnStarted(), agent="r", turn=1), ev(sent("a"), agent="r", turn=1), ev(sent("b"), agent="r", turn=1),
                ev(reply("a"), agent="r", turn=1), ev(reply("b"), agent="r", turn=1), ev(TurnEnded(), agent="r", turn=1)]
        hold = asyncio.Event()

        def mount(cls: Any, client: Any, *, workflow_id: str, is_child: bool, mount_index: int, from_offset: int,
                  skip_until_turn_id: str | None = None) -> Any:
            if workflow_id == ROOT:
                events = _scripted(root, 0.0, hold)
            else:
                events = _scripted(child(workflow_id.removeprefix("wf-")), 0.005, hold)
            return cursor_mod.Cursor(workflow_id=workflow_id, is_child=is_child, mount_index=mount_index,
                                     events=events, skip_until_turn_id=skip_until_turn_id)

        async def should_stop(cur: Any, e: AgentEvent) -> bool:
            return not cur.is_child and e.event.type.value == "turn_end"

        out: list[str] = []
        with mock.patch.object(cursor_mod.Cursor, "mount", classmethod(mount)):
            merged = merge_mod.merge_stream(client=None, root_workflow_id=ROOT, root_from_offset=0,  # type: ignore[arg-type]
                                            skip_until_turn_id=None, select=merge_mod.select_live,
                                            should_stop=should_stop, stall_grace_seconds=5.0)
            async with asyncio.timeout(20):
                async for e, _ro in merged:
                    out.append(f"{e.agent_id}:{e.event.type.value}")
                    await asyncio.sleep(0.02)  # a slow consumer, like the bridge's Redis writes
        hold.set()
        return out

    async def test_slow_consumer_keeps_every_child_event(self) -> None:
        harness_ext.install_merge_fixes()
        out = await self.merged()
        self.assertFalse([e for e in out if e.endswith("subagent_stream_unavailable")], out)
        for sub in "ab":
            self.assertEqual(out.count(f"{sub}:reply_delta"), 12)
            self.assertLess(out.index(f"{sub}:turn_end"), out.index(f"r:subagent_reply_received"))

    async def test_without_the_fix_a_child_is_given_up(self) -> None:
        """Pins the harness bug the fix works around; if this starts failing after a harness
        upgrade, the upstream merge is fixed and install_merge_fixes can go."""
        with mock.patch.object(merge_mod._Merge, "_ensure_pulls", _original_ensure_pulls()):
            out = await self.merged()
        self.assertTrue([e for e in out if e.endswith("subagent_stream_unavailable")], out)


def _original_ensure_pulls() -> Any:
    def _ensure_pulls(self: Any) -> None:  # the 0.7.0 body
        for c in self._cursors.values():
            if c.head is None and not c.exhausted and c.pull_task is None:
                c.pull_task = asyncio.create_task(c.pull())

    return _ensure_pulls


if __name__ == "__main__":
    unittest.main()
