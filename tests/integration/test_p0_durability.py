"""P0 steps 0.4 (interrupt), 0.5 (continue-as-new) and 0.9 (crash detection).

    RUN_INTEGRATION=1 .venv/bin/python -m unittest tests.integration.test_p0_durability -v

The crash test runs workers as subprocesses so it can SIGKILL one mid-stream. Set
P0_CRASH_RUNS / P0_INTERRUPT_RUNS to repeat for the go/no-go measurements.
"""

from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
import time
import unittest
import uuid

from tests.integration import support

HAIKU = {"provider": "anthropic", "model": "claude-haiku-5-5", "effort": "low"}
ESSAY = (
    "Write a detailed essay of about 900 words on why durable execution matters for AI "
    "agents. Plain prose, no tools."
)


def chat_replies(c) -> list[dict]:
    return [e.event.output for e in c.of("message_handler_end") if "text" in (e.event.output or {})]


async def status(client, workflow_id: str):
    from temporal_agent_harness.harness.agent_client import AgentClient

    return await AgentClient(client, workflow_id).get_status()


@unittest.skipIf(support.skip_reason(), support.skip_reason() or "")
class InterruptTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.client = await support.connect()
        self.task_queue = f"p0i-{uuid.uuid4().hex[:6]}"

    async def test_interrupt_pending_approval(self) -> None:
        async with support.running_worker(self.client, self.task_queue):
            handle = await support.start_session(self.client, self.task_queue)
            async with support.EventCollector(self.client, handle.id) as events:
                await support.send(handle, "chat", {"text": "Create notes/x.txt containing 'one'."})
                await events.wait_for(lambda c: c.of("tool_approval_requested"))
                await support.send(handle, "interrupt", {"reason": "Stopped by the test."})
                await events.wait_for(lambda c: c.turn_ends() >= 1)
                resolved = events.collected.of("tool_approval_resolved")
                self.assertEqual([r.event.approved for r in resolved], [False])
                self.assertEqual(chat_replies(events.collected)[-1]["interrupted"], True)
                st = await status(self.client, handle.id)
                self.assertEqual(st.pending_approvals, [])
                # The conversation continues coherently after the stop.
                await support.send(handle, "chat", {"text": "Did the file get created? Answer yes or no, no tools."})
                await events.wait_for(lambda c: len(chat_replies(c)) >= 2 or c.of("message_handler_error"))
                self.assertFalse(events.collected.of("message_handler_error"))
                self.assertIn("no", chat_replies(events.collected)[-1]["text"].lower())
            await handle.signal("close")

    async def test_interrupt_during_tool_activity(self) -> None:
        """Review finding 1: Stop while a tool activity runs must end the turn, not let the
        model carry on with a "tool failed" result."""
        runs = int(os.environ.get("P0_INTERRUPT_RUNS", "1"))
        stopped = 0
        async with support.running_worker(self.client, self.task_queue):
            for _ in range(runs):
                handle = await support.start_session(self.client, self.task_queue, model=HAIKU, test_tools=True)
                async with support.EventCollector(self.client, handle.id) as events:
                    await support.send(handle, "chat", {"text": "Call wait_seconds with seconds=60, then reply 'finished'."})
                    await events.wait_for(lambda c: any(e.event.tool_name == "wait_seconds" for e in c.of("tool_start")))
                    models_before = len(events.collected.of("model_interaction_started"))
                    t0 = asyncio.get_running_loop().time()
                    await support.send(handle, "interrupt", {})
                    await events.wait_for(lambda c: c.turn_ends() >= 1, timeout=30)
                    elapsed = asyncio.get_running_loop().time() - t0
                    reply = chat_replies(events.collected)[-1]
                    self.assertTrue(reply["interrupted"], reply)
                    # No new model call started after the stop.
                    self.assertEqual(len(events.collected.of("model_interaction_started")), models_before)
                    self.assertLess(elapsed, 10, elapsed)
                    stopped += 1
                    # The next turn still works, and the history is valid for the provider.
                    await support.send(handle, "chat", {"text": "Reply with exactly: ready"})
                    await events.wait_for(lambda c: len(chat_replies(c)) >= 2 or c.of("message_handler_error"), timeout=120)
                    errors = [e for e in events.collected.of("message_handler_error") if e.message_id]
                    self.assertFalse(errors, [e.event.message for e in errors])
                await handle.signal("close")
        print(f"\n[0.4] interrupt during tool activity: {stopped}/{runs} stopped cleanly")

    async def test_interrupt_during_stream(self) -> None:
        runs = int(os.environ.get("P0_INTERRUPT_RUNS", "1"))
        lags: list[float] = []
        async with support.running_worker(self.client, self.task_queue):
            for _ in range(runs):
                handle = await support.start_session(self.client, self.task_queue, model=HAIKU)
                async with support.EventCollector(self.client, handle.id) as events:
                    await support.send(handle, "chat", {"text": ESSAY})
                    await events.wait_for(lambda c: len(c.text()) > 200)
                    t_interrupt = asyncio.get_running_loop().time()
                    await support.send(handle, "interrupt", {})
                    await events.wait_for(lambda c: c.turn_ends() >= 1, timeout=60)
                    c = events.collected
                    late = [
                        c.arrivals[i]
                        for i, e in enumerate(c.events)
                        if e.event.type.value == "reply_delta" and c.arrivals[i] > t_interrupt
                    ]
                    lags.append(max(late) - t_interrupt if late else 0.0)
                    self.assertTrue(chat_replies(events.collected)[-1]["interrupted"])
                await handle.signal("close")
        print(f"\n[0.4] interrupt token lag seconds: {[round(x, 2) for x in lags]}")
        within = sum(1 for x in lags if x <= 2.0)
        self.assertGreaterEqual(within, max(1, int(0.9 * runs)), lags)


@unittest.skipIf(support.skip_reason(), support.skip_reason() or "")
class ContinueAsNewTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.client = await support.connect()
        self.task_queue = f"p0c-{uuid.uuid4().hex[:6]}"

    async def test_forced_continue_as_new_keeps_state(self) -> None:
        async with support.running_worker(self.client, self.task_queue):
            handle = await support.start_session(
                self.client, self.task_queue, model=HAIKU, force_continue_as_new_after_turns=1
            )
            first_run = (await handle.describe()).run_id
            async with support.EventCollector(self.client, handle.id) as events:
                await support.send(handle, "chat", {"text": "Create notes/a.txt containing 'alpha'."})
                await events.wait_for(lambda c: c.of("tool_approval_requested"))
                tool_id = events.collected.of("tool_approval_requested")[0].event.tool_id
                await support.approve(handle, tool_id, remember=True)
                await events.wait_for(lambda c: c.turn_ends() >= 1)

                # Wait for the run to roll over.
                for _ in range(60):
                    desc = await handle.describe()
                    stats = await self.client.get_workflow_handle(handle.id).query("history_stats")
                    if desc.run_id != first_run:
                        break
                    await asyncio.sleep(0.5)
                self.assertNotEqual((await handle.describe()).run_id, first_run)
                stats = await self.client.get_workflow_handle(handle.id).query("history_stats")
                self.assertIn("write_file", stats["allowed_tools"])
                self.assertGreaterEqual(stats["turn_counter"], 1)
                self.assertGreater(stats["messages"], 0)

                # Same request id twice across the handoff: applied exactly once.
                from simple_chat_agent.agent.contracts import ENQUEUE_MESSAGE_SIGNAL, FrontDoorMessage

                dup = FrontDoorMessage(
                    request_id="dup-1",
                    type="chat",
                    payload={"text": "Create notes/b.txt containing 'beta', then tell me what notes/a.txt contains."},
                )
                await handle.signal(ENQUEUE_MESSAGE_SIGNAL, dup)
                await handle.signal(ENQUEUE_MESSAGE_SIGNAL, dup)
                await events.wait_for(lambda c: len(chat_replies(c)) >= 2 or c.of("message_handler_error"), timeout=180)
                self.assertFalse(events.collected.of("message_handler_error"))
                # "Always allow" survived: no new approval was requested for the second write.
                self.assertEqual(len(events.collected.of("tool_approval_requested")), 1)
                self.assertIn("alpha", chat_replies(events.collected)[-1]["text"].lower())
                # Turn numbers kept counting across the run boundary; offsets stayed monotonic.
                turns = [e.turn_number for e in events.collected.of("turn_started")]
                self.assertEqual(turns, sorted(turns))
                self.assertEqual(len(set(turns)), len(turns))
                await asyncio.sleep(3)
                self.assertEqual(len(chat_replies(events.collected)), 2)  # the duplicate never ran
            await handle.signal("close")


@unittest.skipIf(support.skip_reason(), support.skip_reason() or "")
class CrashTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.client = await support.connect()
        self.task_queue = f"p0k-{uuid.uuid4().hex[:6]}"
        self.procs: list[subprocess.Popen] = []

    async def asyncTearDown(self) -> None:
        for p in self.procs:
            if p.poll() is None:
                p.kill()

    def _worker(self, name: str) -> subprocess.Popen:
        proc = subprocess.Popen(
            [sys.executable, "-m", "simple_chat_agent.agent.dev_worker", "--task-queue", self.task_queue, "--identity", name],
            cwd=str(support.ROOT),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self.procs.append(proc)
        return proc

    async def test_sigkill_mid_stream_resumes_once(self) -> None:
        runs = int(os.environ.get("P0_CRASH_RUNS", "1"))
        detections: list[float] = []
        for i in range(runs):
            a = self._worker(f"worker-a-{i}")
            await asyncio.sleep(4)  # import + poll
            handle = await support.start_session(self.client, self.task_queue, model=HAIKU)
            async with support.EventCollector(self.client, handle.id) as events:
                await support.send(handle, "chat", {"text": ESSAY})
                await events.wait_for(lambda c: len(c.text()) > 150, timeout=120)
                started_before = len(events.collected.of("model_interaction_started"))
                os.kill(a.pid, signal.SIGKILL)
                killed_at = time.monotonic()
                self._worker(f"worker-b-{i}")
                await events.wait_for(
                    lambda c: len(c.of("model_interaction_started")) > started_before, timeout=90
                )
                detections.append(time.monotonic() - killed_at)
                await events.wait_for(lambda c: c.turn_ends() >= 1, timeout=180)
                reply = chat_replies(events.collected)[-1]["text"]
                stats = await self.client.get_workflow_handle(handle.id).query("history_stats")
                self.assertEqual(stats["responses"], 1, stats)
                self.assertEqual(stats["last_response_text"].strip(), reply.strip())
                self.assertGreater(len(reply), 500)
            await handle.signal("close")
            for p in self.procs:
                if p.poll() is None:
                    p.kill()
            self.procs.clear()
        print(f"\n[0.9] crash detection seconds (kill -> attempt 2 started): {[round(d, 1) for d in detections]}")
        self.assertTrue(all(d <= 15.0 for d in detections), detections)


if __name__ == "__main__":
    unittest.main()
