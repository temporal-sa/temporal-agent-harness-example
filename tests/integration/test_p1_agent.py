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
                # Review M3: subagents must run without approval clicks under the default posture.
                await support.send(handle, "set_posture", {"posture": "trust_safe"})
                asks_before = len(events.collected.of("tool_approval_requested"))
                await support.send(handle, "chat", {"text": (
                    "Start two explorer subagents in parallel: ask one what a.txt contains and the "
                    "other what b.txt contains. Then tell me both answers."
                )})
                await events.wait_for(lambda c: c.turn_ends() >= 3, timeout=300)
                started = events.collected.of("subagent_started")
                stopped = events.collected.of("subagent_stopped")
                self.assertGreaterEqual(len(started), 2, [e.event for e in started])
                self.assertEqual(len(events.collected.of("tool_approval_requested")), asks_before)
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

    async def test_p3_forget_always_allowed_survives_continue_as_new(self) -> None:
        """P3: forget_always_allowed clears "always allow" (here edit_file, given at start as a
        remembered approval would), says what it forgot, and stays forgotten in the next run."""
        from temporal_agent_harness.harness.agent_protocol import AgentConfig

        from simple_chat_agent.agent import approvals
        from simple_chat_agent.agent.contracts import AGENT_WORKFLOW_TYPE, CodingAgentInit

        policy = approvals.policy_for("trust_safe", "local", also_allow=approvals.SUBAGENT_TOOL_NAMES)
        workflow_id = f"p3-forget-{uuid.uuid4().hex[:8]}"
        async with support.running_worker(self.client, self.task_queue):
            handle = await self.client.start_workflow(
                AGENT_WORKFLOW_TYPE,
                args=[AgentConfig(approval_policy=policy.with_tool_allowed("edit_file")),
                      CodingAgentInit(session_id=workflow_id, workspace_id=workflow_id, owner_id="test-user",
                                      environment="local", posture="trust_safe", model=HAIKU,
                                      force_continue_as_new_after_turns=1)],
                id=workflow_id, task_queue=self.task_queue,
            )
            first_run = (await handle.describe()).run_id
            async with support.EventCollector(self.client, handle.id) as events:
                self.assertIn("edit_file", (await handle.query("history_stats"))["allowed_tools"])
                await support.send(handle, "forget_always_allowed", {})
                await events.wait_for(lambda c: any(e.event.output and "forgotten" in e.event.output
                                                    for e in c.of("message_handler_end")), timeout=60)
                out = [e.event.output for e in events.collected.of("message_handler_end") if "forgotten" in (e.event.output or {})][-1]
                self.assertEqual(out["forgotten"], ["edit_file"])
                self.assertNotIn("edit_file", (await handle.query("history_stats"))["allowed_tools"])
                # One chat turn, then the agent continues as new (forced after 1 turn).
                await support.send(handle, "chat", {"text": "Reply with the single word OK."})

                async def next_run() -> bool:
                    return (await handle.describe()).run_id != first_run

                deadline = asyncio.get_running_loop().time() + 120
                while not await next_run():
                    self.assertLess(asyncio.get_running_loop().time(), deadline, "no continue-as-new")
                    await asyncio.sleep(0.5)
            stats = await self.client.get_workflow_handle(workflow_id).query("history_stats")
            self.assertNotIn("edit_file", stats["allowed_tools"])
            self.assertTrue(set(policy.auto_approve_tools) <= set(stats["allowed_tools"]))
            await self.client.get_workflow_handle(workflow_id).signal("close")

    async def test_p3_gated_echo_stacked_approvals_apply_once(self) -> None:
        """P3 3e harness: the test-only gated_echo asks for every call (trust_safe), several
        calls in one turn (same and different text), and each approved call runs once."""
        async with support.running_worker(self.client, self.task_queue):
            handle = await support.start_session(self.client, self.task_queue, model=SONNET, test_tools=True,
                                                 posture="trust_safe")
            async with support.EventCollector(self.client, handle.id) as events:
                await support.send(handle, "chat", {"text": (
                    "In ONE response, call gated_echo three times in parallel: text='same', text='same' "
                    "and text='other'. After the results arrive, reply with the word DONE.")})
                approved: set[str] = set()
                deadline = asyncio.get_running_loop().time() + 240
                while events.collected.turn_ends() < 1:
                    self.assertLess(asyncio.get_running_loop().time(), deadline, "turn did not end")
                    for e in events.collected.of("tool_approval_requested"):
                        if e.event.tool_name == "gated_echo" and e.event.tool_id not in approved:
                            approved.add(e.event.tool_id)
                            await support.approve(handle, e.event.tool_id)
                    await asyncio.sleep(0.3)
                ends = [e for e in events.collected.of("tool_end") if e.event.tool_name == "gated_echo"]
            await handle.signal("close")
        asked = [e for e in events.collected.of("tool_approval_requested") if e.event.tool_name == "gated_echo"]
        print(f"\n[P3 gated_echo] asked {len(asked)}, approved {len(approved)}, ran {len(ends)}")
        self.assertGreaterEqual(len(asked), 3)
        self.assertEqual({e.event.tool_id for e in ends}, approved)  # every approved call ran
        self.assertEqual(len(ends), len(approved))  # once each
        import ast
        import json

        def parsed(text: str) -> dict:
            try:
                return json.loads(text)
            except ValueError:
                return ast.literal_eval(text)

        outputs = [parsed(e.event.tool_output) for e in ends]
        texts = sorted(o["text"] for o in outputs)
        self.assertEqual(texts.count("same"), 2, texts)
        self.assertIn("other", texts)
        self.assertTrue(all(o["attempt"] == 1 for o in outputs))
        self.assertEqual(len({o["activity_id"] for o in outputs}), len(outputs))


if __name__ == "__main__":
    unittest.main()
