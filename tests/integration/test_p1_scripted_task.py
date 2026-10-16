"""P1 step 1.11: the scripted coding task on each provider.

    RUN_INTEGRATION=1 P1_TASK_RUNS=5 .venv/bin/python -m unittest tests.integration.test_p1_scripted_task -v

Pass: at least 4 of 5 runs per provider fix the bug, get the suite green and commit.
Also prints per-run history size, for step 1.12.
"""

from __future__ import annotations

import asyncio
import json
import os
import unittest
import uuid

from tests.integration import support
from tests.integration.test_p1_agent import chat_replies, state_value
from tests.integration.test_p1_validation import read_workspace_file

TASK = (
    "The test test_refund_rest_of_partially_refunded_order fails. Find the cause, fix it, "
    "run the full test suite with `python -m pytest -q`, and when everything passes commit "
    "with a short message."
)

PROVIDERS = {
    "anthropic": {"provider": "anthropic", "model": "claude-sonnet-5-5", "effort": "medium"},
    "openai": {"provider": "openai", "model": "gpt-5.5", "effort": "medium"},
    "google": {"provider": "google", "model": "gemini-3.5-flash", "effort": "medium"},
}


@unittest.skipIf(support.skip_reason(), support.skip_reason() or "")
class ScriptedTask(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.client = await support.connect()

    async def _one(self, model: dict) -> dict:
        from simple_chat_agent.agent.fixtures import seed_workspace

        task_queue = f"p1t-{uuid.uuid4().hex[:6]}"
        async with support.running_worker(self.client, task_queue):
            handle = await support.start_session(self.client, task_queue, model=model, posture="never_ask")
            await seed_workspace(handle.id, "refund_service")
            async with support.EventCollector(self.client, handle.id) as ev:
                await support.send(handle, "chat", {"text": TASK})
                await ev.wait_for(lambda c: c.turn_ends() >= 1, timeout=900)
                c = ev.collected
                changes = state_value(c, "changes")
                workspace = state_value(c, "workspace")
                fixed = "balance_cents" in (await read_workspace_file(handle.id, "refund_service/refunds.py") or "")
                desc = await handle.describe()
                result = {
                    "fixed": fixed,
                    "tests_green": workspace.get("last_exit_code") == 0,
                    "committed": bool(changes.get("commits")),
                    "tool_calls": len(c.of("tool_end")),
                    "model_calls": len(c.of("model_interaction_ended")),
                    "history_events": desc.raw_description.workflow_execution_info.history_length,
                    "history_bytes": desc.raw_description.workflow_execution_info.history_size_bytes,
                    "error": (chat_replies(c)[-1:] or [{}])[0].get("error"),
                }
            await handle.signal("close")
        result["ok"] = result["fixed"] and result["tests_green"] and result["committed"]
        return result

    async def test_scripted_task_per_provider(self) -> None:
        runs = int(os.environ.get("P1_TASK_RUNS", "1"))
        only = os.environ.get("P1_TASK_PROVIDERS", ",".join(PROVIDERS)).split(",")
        summary: dict[str, list[dict]] = {}
        for name in only:
            summary[name] = []
            for _ in range(runs):
                try:
                    summary[name].append(await self._one(PROVIDERS[name]))
                except Exception as exc:  # noqa: BLE001 - a run that crashed counts as failed
                    summary[name].append({"ok": False, "exception": repr(exc)[:300]})
                print(f"[1.11] {name}: {json.dumps(summary[name][-1])}", flush=True)
        for name, results in summary.items():
            passed = sum(1 for r in results if r.get("ok"))
            print(f"[1.11] {name}: {passed}/{len(results)} passed")
            self.assertGreaterEqual(passed, max(1, (4 * len(results) + 4) // 5), results)


if __name__ == "__main__":
    unittest.main()
