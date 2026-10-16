"""Replay recorded CodingAgent histories on the current code (P1 step 1.13, PRD §6).

Offline: no server or keys needed. Histories come from tests/integration/record_histories.py
and must be re-recorded when workflow code changes on purpose (same-build replay only).
"""

from __future__ import annotations

import os
import unittest
from pathlib import Path

os.environ.setdefault("ANTHROPIC_API_KEY", "replay-not-used")
os.environ.setdefault("OPENAI_API_KEY", "replay-not-used")

HISTORIES = Path(__file__).parent / "fixtures" / "histories"


class ReplayTests(unittest.IsolatedAsyncioTestCase):
    async def test_recorded_histories_replay(self) -> None:
        files = sorted(HISTORIES.glob("*.json"))
        if not files:
            self.skipTest("no recorded histories (run tests/integration/record_histories.py)")

        from pydantic_ai.durable_exec.temporal import AgentPlugin, PydanticAIPlugin
        from temporalio.client import WorkflowHistory
        from temporalio.contrib.pydantic import pydantic_data_converter
        from temporalio.worker import Replayer

        from simple_chat_agent.agent.runtime import CODING_AGENT
        from simple_chat_agent.agent.subagents import SUBAGENT_WORKFLOWS
        from simple_chat_agent.agent.workflow import CodingAgent

        replayer = Replayer(
            workflows=[CodingAgent, *SUBAGENT_WORKFLOWS],
            data_converter=pydantic_data_converter,
            plugins=[PydanticAIPlugin(), AgentPlugin(CODING_AGENT)],
        )
        for path in files:
            with self.subTest(history=path.name):
                history = WorkflowHistory.from_json(path.stem, path.read_text())
                result = await replayer.replay_workflow(history, raise_on_replay_failure=False)
                self.assertIsNone(result.replay_failure, f"{path.name}: {result.replay_failure!r}")


if __name__ == "__main__":
    unittest.main()
