"""P3 operations on the coding agent: forget_always_allowed and chat attachments."""

from __future__ import annotations

import asyncio
import os
import unittest
from types import SimpleNamespace
from typing import Any

os.environ.setdefault("ANTHROPIC_API_KEY", "unit-test-not-used")
os.environ.setdefault("OPENAI_API_KEY", "unit-test-not-used")

from pydantic import ValidationError  # noqa: E402

from simple_chat_agent.agent import approvals  # noqa: E402
from simple_chat_agent.agent.contracts import (  # noqa: E402
    AlwaysAllowedForgotten,
    CarriedState,
    ChatAttachment,
    ChatMessage,
    ForgetAlwaysAllowed,
    FrontDoorMessage,
)
from simple_chat_agent.agent.workflow import CodingAgent, user_prompt  # noqa: E402


class _Status:
    def __init__(self, policy: Any) -> None:
        self.approval_policy = policy

    def set_approval_policy(self, policy: Any) -> None:
        self.approval_policy = policy


class _Runner:
    """The parts of AgentWorkflowRunner forget_always_allowed touches."""

    def __init__(self, policy: Any) -> None:
        self._status = _Status(policy)
        self._auto_mode_evaluator = object()

    @property
    def approval_policy(self) -> Any:
        return self._status.approval_policy


def agent_with(posture: str, *remembered: str) -> Any:
    policy = approvals.policy_for(posture, "local", also_allow=approvals.SUBAGENT_TOOL_NAMES)
    for tool in remembered:
        policy = policy.with_tool_allowed(tool)
    return SimpleNamespace(_data=SimpleNamespace(posture=posture, environment="local"), _runner=_Runner(policy))


def forget(agent: Any) -> AlwaysAllowedForgotten:
    return asyncio.run(CodingAgent.forget_always_allowed(agent, ForgetAlwaysAllowed()))


class ForgetAlwaysAllowedTests(unittest.TestCase):
    def test_forgets_remembered_tools_and_keeps_the_posture(self) -> None:
        agent = agent_with("trust_safe", "edit_file", "write_file")
        base = set(approvals.policy_for("trust_safe", "local", also_allow=approvals.SUBAGENT_TOOL_NAMES).auto_approve_tools)
        reply = forget(agent)
        self.assertTrue(reply.ok)
        self.assertEqual(reply.forgotten, ["edit_file", "write_file"])
        self.assertEqual(reply.detail, "These ask again: edit_file, write_file.")
        # What continue-as-new carries (AgentConfig.approval_policy) no longer has them.
        self.assertEqual(set(agent._runner.approval_policy.auto_approve_tools), base)
        self.assertTrue(agent._runner.approval_policy.auto_approve_inherently_safe)

    def test_nothing_to_forget(self) -> None:
        reply = forget(agent_with("ask_everything"))
        self.assertEqual((reply.ok, reply.forgotten, reply.detail), (True, [], "Nothing was always allowed."))

    def test_declared_mid_turn_accept_and_not_model_callable(self) -> None:
        from temporal_agent_harness.harness.agent_protocol import MidTurn
        from temporal_agent_harness.harness.agent_workflow import agent_handlers  # type: ignore[attr-defined]

        h = agent_handlers(CodingAgent)["forget_always_allowed"]
        self.assertEqual(h.mid_turn, MidTurn.ACCEPT)
        self.assertFalse(h.model_callable)
        self.assertIs(h.input_type, ForgetAlwaysAllowed)
        self.assertIs(h.output_type, AlwaysAllowedForgotten)

    def test_subagents_do_not_have_it(self) -> None:
        from temporal_agent_harness.harness.agent_workflow import agent_handlers  # type: ignore[attr-defined]

        from simple_chat_agent.agent.subagents import SUBAGENT_WORKFLOWS

        for cls in SUBAGENT_WORKFLOWS:
            self.assertNotIn("forget_always_allowed", set(agent_handlers(cls)))


class AttachmentTests(unittest.TestCase):
    def test_old_payloads_still_validate(self) -> None:
        self.assertEqual(ChatMessage.model_validate({"text": "hi"}).attachments, [])
        # A message deferred across continue-as-new before attachments existed.
        carried = CarriedState.model_validate({"pending_messages": [{"request_id": "r", "type": "chat", "payload": {"text": "hi"}}]})
        msg = ChatMessage.model_validate(carried.pending_messages[0].payload)
        self.assertEqual((msg.text, msg.attachments), ("hi", []))
        self.assertIsInstance(carried.pending_messages[0], FrontDoorMessage)

    def test_payload_shape(self) -> None:
        msg = ChatMessage.model_validate({"text": "see file", "attachments": [
            {"attachment_id": "att_1", "name": "notes.md", "size": 120, "content_type": "text/markdown"},
            {"attachment_id": "att_2"},
        ]})
        self.assertEqual(msg.attachments[1], ChatAttachment(attachment_id="att_2"))
        with self.assertRaises(ValidationError):
            ChatMessage.model_validate({"text": "x", "attachments": [{"attachment_id": ""}]})
        with self.assertRaises(ValidationError):
            ChatMessage.model_validate({"text": "x", "attachments": [{"attachment_id": f"a{i}"} for i in range(21)]})

    def test_prompt_without_attachments_is_unchanged(self) -> None:
        self.assertEqual(user_prompt("hello", []), "hello")

    def test_prompt_part_points_at_read_attachment(self) -> None:
        parts = user_prompt("Summarise these", [
            ChatAttachment(attachment_id="att_1", name="notes.md", size=120, content_type="text/markdown"),
            ChatAttachment(attachment_id="att_2", name="evil\nname.txt"),
        ])
        self.assertIsInstance(parts, list)
        self.assertEqual(parts[0], "Summarise these")
        note = parts[1]
        self.assertIn("read_attachment(attachment_id=...)", note)
        self.assertIn("- notes.md (text/markdown, 120 bytes): attachment_id=att_1", note)
        self.assertIn("- evil name.txt: attachment_id=att_2", note)  # names can't break lines
        self.assertTrue(note.startswith("(The user attached 2 files"))


if __name__ == "__main__":
    unittest.main()


class GatedEchoTests(unittest.TestCase):
    """Test-only tool for the stacked-approval e2e (3e)."""

    def test_in_test_tools(self) -> None:
        from simple_chat_agent.agent.tools.workspace import TEST_TOOLS, gated_echo

        self.assertIn(gated_echo, TEST_TOOLS)

    def test_asks_under_every_posture_but_never_ask(self) -> None:
        self.assertEqual(approvals.risk_class_for("gated_echo", approvals.default_criteria("local")),
                         approvals.EXTERNAL_SIDE_EFFECT)
        for env in ("local", "catalog"):
            self.assertNotIn(approvals.EXTERNAL_SIDE_EFFECT, approvals.JUDGE_APPROVABLE[env])
        self.assertFalse(approvals.remember_allowed(approvals.EXTERNAL_SIDE_EFFECT))
        for posture in ("ask_everything", "trust_safe", "auto"):
            policy = approvals.policy_for(posture, "local", also_allow=approvals.SUBAGENT_TOOL_NAMES)
            self.assertNotIn("gated_echo", policy.auto_approve_tools, posture)
            self.assertFalse(policy.dangerously_skip_all_approvals, posture)
        self.assertTrue(approvals.policy_for("never_ask", "local").dangerously_skip_all_approvals)

    def test_runs_at_most_once_per_call(self) -> None:
        from simple_chat_agent.agent.tools.workspace import GATED_ECHO_CONFIG

        self.assertEqual(GATED_ECHO_CONFIG["retry_policy"].maximum_attempts, 1)
