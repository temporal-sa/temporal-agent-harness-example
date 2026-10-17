"""PRD §5 rules that had no direct test (P5 security checklist, docs/SECURITY-CHECKLIST.md)."""

from __future__ import annotations

import os
import subprocess
import unittest
from datetime import timedelta
from pathlib import Path

os.environ.setdefault("ANTHROPIC_API_KEY", "unit-test-not-used")

ROOT = Path(__file__).resolve().parents[1]


class GuardrailHandlersTests(unittest.TestCase):
    """§5 rule 3: the model can't change its own guardrails."""

    def test_settings_handlers_are_not_model_callable(self) -> None:
        from temporal_agent_harness.harness.agent_protocol import MidTurn
        from temporal_agent_harness.harness.agent_workflow import agent_handlers  # type: ignore[attr-defined]

        from simple_chat_agent.agent.workflow import CodingAgent

        handlers = agent_handlers(CodingAgent)
        for name in ("set_model", "set_posture", "set_web_search", "forget_always_allowed"):
            with self.subTest(handler=name):
                self.assertFalse(handlers[name].model_callable)
                self.assertEqual(handlers[name].mid_turn, MidTurn.ACCEPT)


class SubagentDepthTests(unittest.TestCase):
    """§5 rule 7: subagent depth 1. Children get read-only workspace tools and nothing that
    starts another agent."""

    def test_children_get_read_only_tools_only(self) -> None:
        from simple_chat_agent.agent import subagents

        names = {t.__name__ for t in subagents._read_only_tools()}
        self.assertEqual(names, set(subagents.READ_ONLY_TOOL_NAMES))
        self.assertFalse(any("explorer" in n or "reviewer" in n or "subagent" in n for n in names))


class ModelRetryPolicyTests(unittest.TestCase):
    """§5 rule 8: model calls get 4 attempts, 2 s initial, x2, 60 s cap; a 4xx is final."""

    def test_model_activity_policy(self) -> None:
        from simple_chat_agent.agent.runtime import MODEL_ACTIVITY

        policy = MODEL_ACTIVITY["retry_policy"]
        self.assertEqual(policy.maximum_attempts, 4)
        self.assertEqual(policy.initial_interval, timedelta(seconds=2))
        self.assertEqual(policy.backoff_coefficient, 2.0)
        self.assertEqual(policy.maximum_interval, timedelta(seconds=60))
        self.assertIn("ProviderRejected", policy.non_retryable_error_types)
        self.assertEqual(MODEL_ACTIVITY["heartbeat_timeout"], timedelta(seconds=10))

    def test_provider_4xx_is_final_and_408_409_429_5xx_retry(self) -> None:
        from pydantic_ai.exceptions import ModelHTTPError
        from temporalio.exceptions import ApplicationError

        from simple_chat_agent.agent import harness_ext

        for status in (400, 401, 403, 404, 422):
            with self.subTest(status=status):
                out = harness_ext._classify(ModelHTTPError(status, "m", body={"error": "nope"}))
                self.assertIsInstance(out, ApplicationError)
                self.assertEqual((out.type, out.non_retryable), ("ProviderRejected", True))
        for status in (408, 409, 429, 500, 529):
            with self.subTest(status=status):
                err = ModelHTTPError(status, "m", body={"error": "busy"})
                self.assertIs(harness_ext._classify(err), err)


class EnvFilesIgnoredTests(unittest.TestCase):
    """§5 rule 6: ``**/.env*`` never reaches git or an image."""

    def test_gitignore_covers_env_files(self) -> None:
        for path in (".env", ".env.local", "simple_chat_agent/.env", "simple_chat_agent/web/.env.production"):
            with self.subTest(path=path):
                result = subprocess.run(
                    ["git", "-C", str(ROOT), "check-ignore", "-q", "--no-index", path], check=False
                )
                self.assertEqual(result.returncode, 0, f"{path} is not ignored by git")

    def test_dockerignore_covers_env_files(self) -> None:
        lines = {line.strip() for line in (ROOT / ".dockerignore").read_text().splitlines()}
        self.assertTrue({"**/.env", "**/.env.*"} <= lines or "**/.env*" in lines, sorted(lines))


if __name__ == "__main__":
    unittest.main()
