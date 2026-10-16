from __future__ import annotations

import unittest
from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock, patch

from agent_harness.agent import Agent, ToolExecutionResult
from agent_harness.messages import text_message, tool_use_block
from agent_harness.providers.interface import ProviderResponse
from basic_file_agent.start_workflow import _parse_args
from simple_chat_agent.api.routes.sessions import _chat_signal_request
from simple_chat_agent.api.schemas import CreateSessionRequest, MessageRequest
from simple_chat_agent.worker.workflow import SimpleChatInput


def _tool_response(turn: int) -> ProviderResponse:
    return ProviderResponse(
        id=f"response-{turn}",
        model="test-model",
        message={
            "role": "assistant",
            "content": [
                tool_use_block(
                    tool_use_id=f"tool-{turn}",
                    name="research",
                    input={"turn": turn},
                )
            ],
        },
        stop_reason="tool_use",
        stop_sequence=None,
        usage={},
    )


def _final_response() -> ProviderResponse:
    return ProviderResponse(
        id="response-final",
        model="test-model",
        message=text_message("assistant", "Research complete."),
        stop_reason="end_turn",
        stop_sequence=None,
        usage={},
    )


class AgentBehaviorTests(unittest.IsolatedAsyncioTestCase):
    async def test_reference_time_is_added_to_system_context(self) -> None:
        tools = Mock()
        tools.tool_schemas.return_value = []
        provider = Mock()
        provider.response_with_visible_refusal.side_effect = lambda response: response
        provider.response_message.side_effect = lambda response: response.message
        agent = Agent(
            "Research thoroughly.",
            tools,
            provider=provider,
            model="test-model",
        )
        agent._call_provider = AsyncMock(  # type: ignore[method-assign]
            return_value=_final_response()
        )

        await agent.run(
            "What happened today?",
            reference_time="2026-07-29T18:42:00Z",
        )

        self.assertIn(
            "Current reference time: 2026-07-29T18:42:00Z (UTC).",
            agent._system_prompt,
        )
        self.assertIn(
            "verify time-sensitive claims with research tools",
            agent._system_prompt,
        )

    async def test_reference_time_survives_continue_as_new(self) -> None:
        tools = Mock()
        tools.tool_schemas.return_value = []
        provider = Mock()
        provider.response_with_visible_refusal.side_effect = lambda response: response
        provider.response_message.side_effect = lambda response: response.message
        provider.estimate_request_tokens.return_value = 0
        agent = Agent(
            "Research thoroughly.",
            tools,
            provider=provider,
            model="test-model",
        )
        agent._call_provider = AsyncMock(  # type: ignore[method-assign]
            return_value=_tool_response(1)
        )
        agent._execute_requested_tools = AsyncMock(  # type: ignore[method-assign]
            return_value=ToolExecutionResult(tool_results=[])
        )
        agent._should_return_continue_as_new = Mock(  # type: ignore[method-assign]
            return_value=True
        )

        result = await agent.run(
            "What happened today?",
            reference_time="2026-07-29T18:42:00Z",
        )

        self.assertIsNotNone(result.continuation_state)
        self.assertEqual(
            result.continuation_state.reference_time,
            "2026-07-29T18:42:00Z",
        )

    async def test_agent_has_no_turn_limit(self) -> None:
        tools = Mock()
        tools.tool_schemas.return_value = []
        provider = Mock()
        provider.response_with_visible_refusal.side_effect = lambda response: response
        provider.response_message.side_effect = lambda response: response.message
        agent = Agent(
            "Research thoroughly.",
            tools,
            provider=provider,
            model="test-model",
        )
        agent._call_provider = AsyncMock(  # type: ignore[method-assign]
            side_effect=[
                *[_tool_response(turn) for turn in range(1, 26)],
                _final_response(),
            ]
        )
        agent._execute_requested_tools = AsyncMock(  # type: ignore[method-assign]
            return_value=ToolExecutionResult(tool_results=[])
        )
        agent._should_return_continue_as_new = Mock(  # type: ignore[method-assign]
            return_value=False
        )

        result = await agent.run("Investigate this.")

        self.assertEqual(result.message, text_message("assistant", "Research complete."))
        self.assertEqual(result.turns, 26)
        self.assertEqual(agent._call_provider.await_count, 26)

class DefaultAgentBehaviorTests(unittest.TestCase):
    def test_api_and_workflow_use_same_deep_research_prompt(self) -> None:
        api_prompt = CreateSessionRequest().system_prompt

        self.assertEqual(api_prompt, SimpleChatInput().system_prompt)
        self.assertIn("rigorous deep-research agent", api_prompt)
        self.assertIn("multiple independent primary sources", api_prompt)
        self.assertIn("do not stop at the first plausible result", api_prompt)
        self.assertIn(
            "create_subagent is available, actually call it once per workstream",
            api_prompt,
        )
        self.assertIn(
            "same assistant turn so those calls run concurrently",
            api_prompt,
        )
        self.assertIn(
            "ten independent report topics may warrant ten subagents",
            api_prompt,
        )
        self.assertIn("ask focused clarifying questions", api_prompt)
        self.assertIn(
            "high-quality, self-contained HTML artifact as the primary deliverable",
            api_prompt,
        )
        self.assertIn("semantic, accessible, responsive HTML", api_prompt)
        self.assertIn(
            "fully functional without JavaScript or network-loaded assets",
            api_prompt,
        )

    def test_session_api_no_longer_exposes_max_turns(self) -> None:
        properties = CreateSessionRequest.model_json_schema()["properties"]

        self.assertNotIn("max_turns", properties)

    def test_basic_file_agent_cli_no_longer_exposes_max_turns(self) -> None:
        with patch(
            "sys.argv",
            ["basic-file-agent", "Investigate these files thoroughly."],
        ):
            args = _parse_args()

        self.assertFalse(hasattr(args, "max_turns"))


class ReferenceTimeRequestTests(unittest.IsolatedAsyncioTestCase):
    async def test_chat_can_be_buffered_when_worker_query_times_out(self) -> None:
        deps = Mock()
        registry = Mock()
        registry.query = AsyncMock(side_effect=TimeoutError)
        deps.ensure_user_chats_workflow = AsyncMock(return_value=registry)
        deps.github_connection_id_for_user.return_value = None
        user = Mock(user_id="user-123", username="researcher@example.com")

        request = await _chat_signal_request(
            deps,
            user=user,
            message_request=MessageRequest(message="Continue when workers return."),
            attachments=[],
        )

        self.assertEqual(request.message, "Continue when workers return.")
        self.assertIsNone(request.available_tool_names)

    async def test_chat_request_records_current_utc_reference_time(self) -> None:
        deps = Mock()
        registry = Mock()
        registry.query = AsyncMock(return_value=[])
        deps.ensure_user_chats_workflow = AsyncMock(return_value=registry)
        deps.github_connection_id_for_user.return_value = None
        user = Mock(user_id="user-123", username="researcher@example.com")

        with patch(
            "simple_chat_agent.api.routes.sessions."
            "configured_research_tool_names",
            return_value=[],
        ):
            request = await _chat_signal_request(
                deps,
                user=user,
                message_request=MessageRequest(message="Research today."),
                attachments=[],
            )

        parsed = datetime.fromisoformat(request.reference_time.replace("Z", "+00:00"))
        self.assertEqual(parsed.tzinfo, UTC)
        self.assertEqual(parsed.microsecond, 0)


if __name__ == "__main__":
    unittest.main()
