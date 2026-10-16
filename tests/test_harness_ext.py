"""Contract tests for the private harness and Pydantic AI internals harness_ext depends on.

If a harness or Pydantic AI upgrade changes any of these, this file fails first, pointing at
the helper to revisit (CTO memo 001, "Private harness internals we'd depend on").
"""

from __future__ import annotations

import dataclasses
import inspect
import os
import unittest

os.environ.setdefault("ANTHROPIC_API_KEY", "test-key-not-used")


class HarnessInternals(unittest.TestCase):
    def test_runner_private_members(self) -> None:
        from temporal_agent_harness.harness.agent_workflow import AgentWorkflowRunner, _WorkflowStatus

        for name in (
            "_validate_send_agent_message",
            "_handle_send_agent_message",
            "_validate_tool_approval",
            "_handle_tool_approval",
            "_resolve_and_publish",
            "_pub",
        ):
            self.assertTrue(callable(getattr(AgentWorkflowRunner, name, None)), name)
        for name in (
            "join_turn",
            "reserve_turn_number",
            "enqueue_message",
            "pending_approval_entries",
            "pending_callbacks",
            "active_subagents",
            "approval_entry",
            "remove_subagent",
            "set_approval_policy",
        ):
            self.assertTrue(callable(getattr(_WorkflowStatus, name, None)), name)
        init = inspect.getsource(_WorkflowStatus.__init__)
        self.assertIn("self._current_turn", init)
        self.assertIn("_agent_id", inspect.getsource(AgentWorkflowRunner.__init__))
        self.assertIn("self._joining", inspect.getsource(AgentWorkflowRunner.__init__))

    def test_admission_replica_matches_harness(self) -> None:
        """harness_ext.admit_message replicates _handle_send_agent_message; the original must
        still mint ids and publish MessageAccepted the way the replica assumes."""
        from temporal_agent_harness.harness.agent_workflow import AgentWorkflowRunner, _Admission

        src = inspect.getsource(AgentWorkflowRunner._handle_send_agent_message)
        for needle in (
            "message_id = str(workflow.uuid4())",
            "self._status.join_turn()",
            "self._status.reserve_turn_number()",
            "self._status.enqueue_message(",
            "MessageDisposition.JOINED",
            "MessageDisposition.QUEUED",
            "MessageDisposition.OPENED",
            "MessageAccepted(",
        ):
            self.assertIn(needle, src)
        self.assertEqual(
            [f.name for f in dataclasses.fields(_Admission)],
            ["message", "turn_id", "turn_number", "message_id"],
        )

    def test_update_registration_names(self) -> None:
        from temporal_agent_harness.harness.agent_protocol import SEND_AGENT_MESSAGE_UPDATE

        self.assertEqual(SEND_AGENT_MESSAGE_UPDATE, "send_agent_message")

    def test_workflow_stream_private_members(self) -> None:
        from temporalio.contrib.workflow_streams import WorkflowStream

        self.assertTrue(callable(getattr(WorkflowStream, "_on_offset", None)))
        self.assertIn("self._base_offset", inspect.getsource(WorkflowStream.__init__))

    def test_observer_private_members(self) -> None:
        from temporal_agent_harness.ai_sdks.pydantic_ai_harness import PydanticAIStreamObserver

        src = inspect.getsource(PydanticAIStreamObserver.__init__)
        for attr in ("self._context", "self._model", "self._stack", "self._publisher"):
            self.assertIn(attr, src)
        self.assertTrue(callable(getattr(PydanticAIStreamObserver, "_emit_tool_requested", None)))

    def test_adapter_helpers_exist(self) -> None:
        """per_run_toolset reuses the harness adapter's private schema and runner helpers."""
        from temporal_agent_harness.ai_sdks import pydantic_ai_harness as h

        self.assertTrue(callable(getattr(h, "_tool_schema", None)))
        self.assertTrue(callable(getattr(h, "_runner_from_ctx", None)))

    def test_harness_handler_still_calls_usage_as_method(self) -> None:
        """If upstream fixes this, harness_ext.stream_usage becomes unnecessary (harmless)."""
        from temporal_agent_harness.ai_sdks import pydantic_ai_harness

        self.assertIn("stream.usage()", inspect.getsource(pydantic_ai_harness.harness_event_stream_handler))


class PydanticAIInternals(unittest.TestCase):
    def test_usage_extract_fix_installed_and_keeps_tokens(self) -> None:
        from pydantic_ai.usage import RequestUsage

        import simple_chat_agent.agent.harness_ext  # noqa: F401 - installs the fix

        usage = RequestUsage.extract(
            {
                "model": "gpt-5.4-mini",
                "usage": {
                    "input_tokens": 8,
                    "input_tokens_details": {"cached_tokens": 3},
                    "output_tokens": 6,
                    "output_tokens_details": {"reasoning_tokens": 2},
                    "total_tokens": 14,
                },
            },
            provider="openai",
            provider_url="https://api.openai.com/v1/",
            provider_fallback="openai",
            api_flavor="responses",
        )
        self.assertEqual((usage.input_tokens, usage.output_tokens, usage.cache_read_tokens), (8, 6, 3))

    def test_temporal_agent_event_handler_property(self) -> None:
        from pydantic_ai.durable_exec.temporal import TemporalAgent

        self.assertIsInstance(inspect.getattr_static(TemporalAgent, "event_stream_handler"), property)
        self.assertIn(
            "_call_event_stream_handler_activity", inspect.getsource(TemporalAgent.event_stream_handler.fget)
        )

    def test_runtime_toolset_opt_out_key(self) -> None:
        from pydantic_ai.durable_exec.temporal import _agent

        self.assertIn("tool_config_key='temporal'", inspect.getsource(_agent))


class CloseWithNote(unittest.TestCase):
    def test_closes_tool_calls_and_alternates(self) -> None:
        from pydantic_ai.messages import (
            ModelRequest,
            ModelResponse,
            TextPart,
            ToolCallPart,
            ToolReturnPart,
            UserPromptPart,
        )

        from simple_chat_agent.agent.harness_ext import close_with_note

        history = [
            ModelRequest(parts=[UserPromptPart("do it")]),
            ModelResponse(parts=[TextPart("ok"), ToolCallPart("write_file", {"path": "a"}, tool_call_id="t1")]),
        ]
        out = close_with_note(history, tool_note="stopped", user_note="I stopped")
        self.assertIsInstance(out[-1], ModelRequest)
        kinds = [type(p) for p in out[-1].parts]
        self.assertEqual(kinds, [ToolReturnPart, UserPromptPart])
        self.assertEqual(out[-1].parts[0].tool_call_id, "t1")

        pending = [ModelRequest(parts=[UserPromptPart("do it")])]
        out = close_with_note(pending, tool_note="x", user_note="I stopped")
        self.assertEqual(len(out), 1)
        self.assertEqual([p.content for p in out[0].parts], ["do it", "I stopped"])


if __name__ == "__main__":
    unittest.main()
