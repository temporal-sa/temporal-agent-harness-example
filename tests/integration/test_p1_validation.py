"""P1 validation table checks that need live services.

    RUN_INTEGRATION=1 .venv/bin/python -m unittest tests.integration.test_p1_validation -v
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

import httpx

from tests.integration import support
from tests.integration.test_p1_agent import chat_replies, state_value

HAIKU = {"provider": "anthropic", "model": "claude-haiku-5-5", "effort": "low"}
SONNET = {"provider": "anthropic", "model": "claude-sonnet-5-5", "effort": "low"}


async def read_workspace_file(workspace_id: str, path: str) -> str | None:
    url = os.environ["WORKSPACE_EXECUTOR_URL"].rstrip("/")
    async with httpx.AsyncClient() as client:
        r = await client.post(
            f"{url}/files/read",
            json={"workspace_id": workspace_id, "path": path},
            headers={"Authorization": f"Bearer {os.environ['WORKSPACE_EXECUTOR_TOKEN']}"},
        )
    return r.json().get("content") if r.status_code == 200 else None


async def approve_all(handle, events, *, remember: bool = False, stop) -> None:
    seen: set[str] = set()
    while not stop():
        for e in events.collected.of("tool_approval_requested"):
            if e.event.tool_id not in seen:
                seen.add(e.event.tool_id)
                await support.approve(handle, e.event.tool_id, remember=remember)
        await asyncio.sleep(0.3)


@unittest.skipIf(support.skip_reason(), support.skip_reason() or "")
class P1Validation(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.client = await support.connect()
        self.task_queue = f"p1v-{uuid.uuid4().hex[:6]}"
        self.procs: list[subprocess.Popen] = []

    async def asyncTearDown(self) -> None:
        for p in self.procs:
            if p.poll() is None:
                p.kill()

    def _worker(self, name: str, **env: str) -> subprocess.Popen:
        proc = subprocess.Popen(
            [sys.executable, "-m", "simple_chat_agent.agent.dev_worker", "--task-queue", self.task_queue, "--identity", name],
            cwd=str(support.ROOT),
            env={**os.environ, **env},
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self.procs.append(proc)
        return proc

    async def test_1_1b_run_command_streams_and_records_state(self) -> None:
        async with support.running_worker(self.client, self.task_queue):
            h = await support.start_session(self.client, self.task_queue, model=HAIKU, posture="never_ask")
            async with support.EventCollector(self.client, h.id) as ev:
                await support.send(h, "chat", {"text": "Run exactly this with run_command: for i in 1 2 3; do echo tick $i; sleep 2; done"})
                await ev.wait_for(lambda c: c.turn_ends() >= 1, timeout=180)
                c = ev.collected
                deltas = [i for i, e in enumerate(c.events) if e.event.type.value == "tool_progress_delta"]
                ends = [i for i, e in enumerate(c.events) if e.event.type.value == "tool_end" and e.event.tool_name == "run_command"]
                self.assertTrue(deltas and ends, [e.event.type.value for e in c.events])
                # Output streamed while the command ran: the first chunk arrived seconds before the end.
                self.assertGreater(c.arrivals[ends[0]] - c.arrivals[deltas[0]], 2.5)
                self.assertIn("tick", "".join(c.events[i].event.progress_delta for i in deltas))
                self.assertEqual(state_value(c, "workspace").get("last_exit_code"), 0)
            await h.signal("close")

    async def test_1_1c_sigkill_during_command_runs_it_once(self) -> None:
        a = self._worker("worker-a")
        await asyncio.sleep(5)
        h = await support.start_session(self.client, self.task_queue, model=HAIKU, posture="never_ask")
        async with support.EventCollector(self.client, h.id) as ev:
            await support.send(h, "chat", {"text": (
                "Run exactly this with run_command (timeout_seconds 120): "
                "echo started >> runs.log; sleep 25; echo finished. Then say done."
            )})
            await ev.wait_for(lambda c: any(e.event.tool_name == "run_command" for e in c.of("tool_start")), timeout=120)
            await asyncio.sleep(5)
            os.kill(a.pid, signal.SIGKILL)
            self._worker("worker-b")
            await ev.wait_for(lambda c: c.turn_ends() >= 1, timeout=240)
            log = await read_workspace_file(h.id, "runs.log")
            self.assertEqual((log or "").count("started"), 1, log)
            out = "".join(e.event.progress_delta for e in ev.collected.of("tool_progress_delta"))
            self.assertIn("finished", out)
        await h.signal("close")

    async def test_1_2b_network_tools_ask_under_trust_safe(self) -> None:
        async with support.running_worker(self.client, self.task_queue):
            h = await support.start_session(self.client, self.task_queue, model=HAIKU, posture="trust_safe")
            async with support.EventCollector(self.client, h.id) as ev:
                await support.send(h, "chat", {"text": "Fetch https://example.com with fetch_url and tell me its title."})
                await ev.wait_for(lambda c: c.of("tool_approval_requested"), timeout=120)
                self.assertEqual(ev.collected.of("tool_approval_requested")[0].event.tool_name, "fetch_url")
                await support.send(h, "interrupt", {})
                await ev.wait_for(lambda c: c.turn_ends() >= 1, timeout=60)
            await h.signal("close")

    async def test_1_2b_auto_mode_judges_network_call(self) -> None:
        async with support.running_worker(self.client, self.task_queue):
            h = await support.start_session(self.client, self.task_queue, model=HAIKU, posture="auto")
            async with support.EventCollector(self.client, h.id) as ev:
                await support.send(h, "chat", {"text": "Fetch https://example.com with fetch_url and tell me its title."})
                await ev.wait_for(
                    lambda c: c.of("auto_approval_evaluation_ended") or c.of("auto_approval_evaluation_error"),
                    timeout=120,
                )
                ended = ev.collected.of("auto_approval_evaluation_ended")
                self.assertTrue(ended, "the judge evaluated the network call")
                print(f"\n[1.2b] judge verdict on fetch_url: {ended[0].event.verdict} - {ended[0].event.reason}")
                await ev.wait_for(lambda c: c.turn_ends() >= 1 or c.of("tool_approval_requested"), timeout=120)
                if not ev.collected.turn_ends():
                    await support.send(h, "interrupt", {})
            await h.signal("close")

    async def test_1_4c_always_allow_only_for_edits(self) -> None:
        async with support.running_worker(self.client, self.task_queue):
            h = await support.start_session(self.client, self.task_queue, model=HAIKU, posture="trust_safe")
            async with support.EventCollector(self.client, h.id) as ev:
                done = lambda: ev.collected.turn_ends() >= turns  # noqa: E731
                turns = 1
                approver = asyncio.create_task(approve_all(h, ev, remember=True, stop=lambda: False))
                try:
                    await support.send(h, "chat", {"text": "Run `echo one` with run_command."})
                    await ev.wait_for(lambda c: done(), timeout=120)
                    turns = 2
                    await support.send(h, "chat", {"text": "Run `echo two` with run_command."})
                    await ev.wait_for(lambda c: done(), timeout=120)
                    turns = 3
                    await support.send(h, "chat", {"text": "Create a.txt containing 'x' with write_file."})
                    await ev.wait_for(lambda c: done(), timeout=120)
                    turns = 4
                    await support.send(h, "chat", {"text": "Create b.txt containing 'y' with write_file."})
                    await ev.wait_for(lambda c: done(), timeout=120)
                finally:
                    approver.cancel()
                asked = [e.event.tool_name for e in ev.collected.of("tool_approval_requested")]
                self.assertEqual(asked.count("run_command"), 2, asked)  # remember ignored for shell
                self.assertEqual(asked.count("write_file"), 1, asked)  # remembered for edits
            await h.signal("close")

    async def test_1_8a_history_cap_checkpoints(self) -> None:
        async with support.running_worker(self.client, self.task_queue):
            h = await support.start_session(
                self.client, self.task_queue, model=HAIKU, posture="never_ask", test_history_event_cap=60
            )
            first_run = (await h.describe()).run_id
            async with support.EventCollector(self.client, h.id) as ev:
                await support.send(h, "chat", {"text": "One at a time, create files f1.txt through f8.txt each containing its number."})
                await ev.wait_for(lambda c: c.turn_ends() >= 1, timeout=240)
                reply = chat_replies(ev.collected)[-1]
                self.assertTrue(reply.get("checkpoint"), reply)
                for _ in range(40):
                    if (await h.describe()).run_id != first_run:
                        break
                    await asyncio.sleep(0.5)
                self.assertNotEqual((await h.describe()).run_id, first_run)
            await h.signal("close")

    async def test_1_8b_session_spend_cap(self) -> None:
        self._worker("capped", AGENT_SPEND_CAP_SESSION_TOKENS="200")
        await asyncio.sleep(5)
        h = await support.start_session(self.client, self.task_queue, model=HAIKU, posture="never_ask")
        async with support.EventCollector(self.client, h.id) as ev:
            await support.send(h, "chat", {"text": "Reply with one word: hello"})
            await ev.wait_for(lambda c: c.turn_ends() >= 1, timeout=120)
            await support.send(h, "chat", {"text": "Reply with one word: again"})
            await ev.wait_for(lambda c: c.turn_ends() >= 2, timeout=120)
            replies = chat_replies(ev.collected)
            self.assertEqual(replies[-1].get("error"), "SpendCapReached", replies)
            print(f"\n[1.8b] second turn: {replies[-1]['text']}")
        await h.signal("close")


if __name__ == "__main__":
    unittest.main()
