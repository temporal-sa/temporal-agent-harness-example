"""P1 integration: observable state, subagents, steer.

    RUN_INTEGRATION=1 .venv/bin/python -m unittest tests.integration.test_p1_agent -v
"""

from __future__ import annotations

import asyncio
import unittest
import uuid

from tests.integration import support

HAIKU = {"provider": "anthropic", "model": "claude-haiku-5-5", "effort": "low"}
SONNET = {"provider": "anthropic", "model": "claude-sonnet-5-5", "effort": "low"}


def chat_replies(c) -> list[dict]:
    return [e.event.output for e in c.of("message_handler_end") if "text" in (e.event.output or {})]


def state_value(c, state_id: str) -> dict:
    """Fold snapshots and patches for one state id (minimal RFC 6902 subset)."""
    import copy

    value: dict = {}
    for e in c.events:
        kind = e.event.type.value
        if kind == "state_snapshot" and e.event.state_id == state_id:
            value = copy.deepcopy(e.event.value)
        elif kind == "state_patch" and e.event.state_id == state_id:
            for op in e.event.ops:
                parts = [p for p in op["path"].split("/") if p]
                target = value
                for p in parts[:-1]:
                    target = target[int(p)] if isinstance(target, list) else target[p]
                last = parts[-1] if parts else ""
                if op["op"] in ("add", "replace"):
                    if isinstance(target, list):
                        if last == "-":
                            target.append(op["value"])
                        elif op["op"] == "add":
                            target.insert(int(last), op["value"])
                        else:
                            target[int(last)] = op["value"]
                    elif parts:
                        target[last] = op["value"]
                    else:
                        value = op["value"]
                elif op["op"] == "remove":
                    if isinstance(target, list):
                        target.pop(int(last))
                    else:
                        target.pop(last, None)
    return value


@unittest.skipIf(support.skip_reason(), support.skip_reason() or "")
class P1AgentTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.client = await support.connect()
        self.task_queue = f"p1-{uuid.uuid4().hex[:6]}"

    async def test_plan_and_changes_state(self) -> None:
        async with support.running_worker(self.client, self.task_queue):
            handle = await support.start_session(self.client, self.task_queue, model=SONNET, posture="never_ask")
            async with support.EventCollector(self.client, handle.id) as events:
                await support.send(handle, "chat", {"text": (
                    "Use update_plan to make a 2-step plan, then create src/app.py with a hello() "
                    "function and docs/notes.md with one line, marking plan steps done as you go."
                )})
                await events.wait_for(lambda c: c.turn_ends() >= 1, timeout=240)
                plan = state_value(events.collected, "plan")
                changes = state_value(events.collected, "changes")
                self.assertGreaterEqual(len(plan.get("items", [])), 2, plan)
                paths = {f["path"] for f in changes.get("files", [])}
                self.assertTrue({"src/app.py", "docs/notes.md"} <= paths, changes)
            await handle.signal("close")

    async def test_two_explorers_and_none_left_running(self) -> None:
        async with support.running_worker(self.client, self.task_queue):
            handle = await support.start_session(self.client, self.task_queue, model=SONNET, posture="never_ask")
            async with support.EventCollector(self.client, handle.id) as events:
                await support.send(handle, "chat", {"text": "Create a.txt containing 'apple' and b.txt containing 'banana'."})
                await events.wait_for(lambda c: c.turn_ends() >= 1, timeout=180)
                await support.send(handle, "chat", {"text": (
                    "Start two explorer subagents in parallel: ask one what a.txt contains and the "
                    "other what b.txt contains. Then tell me both answers."
                )})
                await events.wait_for(lambda c: c.turn_ends() >= 2, timeout=300)
                started = events.collected.of("subagent_started")
                stopped = events.collected.of("subagent_stopped")
                self.assertGreaterEqual(len(started), 2, [e.event for e in started])
                self.assertEqual(len(stopped), len(started))
                reply = chat_replies(events.collected)[-1]["text"].lower()
                self.assertIn("apple", reply)
                self.assertIn("banana", reply)
                from temporal_agent_harness.harness.agent_client import AgentClient

                status = await AgentClient(self.client, handle.id).get_status()
                self.assertEqual(status.subagents, [])
            await handle.signal("close")

    async def test_steer_reaches_the_running_turn(self) -> None:
        async with support.running_worker(self.client, self.task_queue):
            handle = await support.start_session(self.client, self.task_queue, model=HAIKU, test_tools=True, posture="never_ask")
            async with support.EventCollector(self.client, handle.id) as events:
                await support.send(handle, "chat", {"text": "Call wait_seconds with seconds=6, then reply with one short sentence."})
                await events.wait_for(lambda c: any(e.event.tool_name == "wait_seconds" for e in c.of("tool_start")))
                await support.send(handle, "steer", {"text": "End your reply with the exact word PINEAPPLE."})
                await events.wait_for(lambda c: len(chat_replies(c)) >= 1 or c.of("message_handler_error"), timeout=120)
                self.assertIn("PINEAPPLE", chat_replies(events.collected)[-1]["text"])
            await handle.signal("close")


if __name__ == "__main__":
    unittest.main()
