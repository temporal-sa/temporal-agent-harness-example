from __future__ import annotations

import json
import unittest
from types import SimpleNamespace

from agent_harness.llm_guards import LlmGuardAction, LlmGuardExecution
from agent_harness.messages import (
    message as agent_message,
    message_text,
    tool_result_block,
    tool_use_block,
)
from agent_harness.providers.chatgpt import (
    ChatGPTProvider,
    ChatGPTReasoningConfig,
    ChatGPTResponse,
    _ChatGPTStreamState,
    _agent_messages_to_chatgpt_input,
    _chatgpt_input_to_agent_messages,
    _record_chatgpt_stream_event,
    _record_final_openai_response,
)
from agent_harness.providers.gemini import (
    GeminiProvider,
    GeminiRequest,
    GeminiResponse,
    GeminiThinkingConfig,
    _agent_messages_to_gemini_contents,
    _gemini_contents_to_agent_messages,
    _gemini_generate_content_config,
    _gemini_http_options,
    _gemini_tools_from_agent_tools,
)


class ChatGPTProviderTests(unittest.IsolatedAsyncioTestCase):
    def test_serializes_request_response_and_provider_options(self) -> None:
        provider = ChatGPTProvider(
            reasoning=ChatGPTReasoningConfig(effort="low", summary="concise")
        )
        request = provider.create_request(
            system_prompt="Be useful.",
            model="gpt-test",
            max_tokens=128,
            context_token_limit=10_000,
            tools=[_agent_tool_schema()],
            chat_history=[agent_message("user", "hello")],
            stream_id="stream-1",
            stream_sequence=2,
            stream_attempt=1,
            stream_agent={
                "id": "chat-1-subagent-1",
                "parent_id": "chat-1",
                "kind": "subagent",
                "label": "Research one topic",
            },
        )

        self.assertEqual(
            request.tools,
            [
                {
                    "type": "function",
                    "name": "lookup",
                    "parameters": {"type": "object", "properties": {}},
                    "strict": False,
                    "description": "Look up a value.",
                }
            ],
        )
        self.assertEqual(
            provider.request_from_dict(provider.request_to_dict(request)),
            request,
        )

        response = ChatGPTResponse(
            id="resp-1",
            model="gpt-test",
            message={
                "role": "assistant",
                "content": [
                    {
                        "type": "message",
                        "role": "assistant",
                        "content": [
                            {
                                "type": "output_text",
                                "text": "done",
                                "annotations": [],
                            }
                        ],
                        "status": "completed",
                    }
                ],
            },
            stop_reason="completed",
            stop_sequence=None,
            usage={"input_tokens": 1, "output_tokens": 2},
            stop_details={"status": "ok"},
        )

        self.assertEqual(
            provider.response_from_dict(provider.response_to_dict(response)),
            response,
        )
        self.assertEqual(message_text(provider.response_message(response)), "done")

    def test_tool_call_round_trip_preserves_call_id_and_arguments(self) -> None:
        messages = [
            agent_message("user", "Use the tool."),
            agent_message(
                "assistant",
                [
                    tool_use_block(
                        tool_use_id="call-1",
                        name="lookup",
                        input={"query": "alpha"},
                    )
                ],
            ),
            agent_message(
                "user",
                [
                    tool_result_block(
                        tool_use_id="call-1",
                        content=json.dumps({"value": 42}),
                    )
                ],
            ),
        ]

        items = _agent_messages_to_chatgpt_input(messages)

        self.assertEqual(items[1]["type"], "function_call")
        self.assertEqual(items[1]["call_id"], "call-1")
        self.assertEqual(json.loads(items[1]["arguments"]), {"query": "alpha"})
        self.assertEqual(items[2]["type"], "function_call_output")
        self.assertEqual(items[2]["call_id"], "call-1")

        round_tripped = _chatgpt_input_to_agent_messages(items)
        self.assertEqual(round_tripped[1]["content"][0]["id"], "call-1")
        self.assertEqual(round_tripped[1]["content"][0]["input"], {"query": "alpha"})
        self.assertEqual(round_tripped[2]["content"][0]["tool_use_id"], "call-1")

    def test_guard_response_and_refusal_fallback_are_visible(self) -> None:
        provider = ChatGPTProvider()
        execution = LlmGuardExecution(
            request={},
            response=None,
            state={},
            action=LlmGuardAction.BLOCK,
            reason="policy",
        )

        guarded = provider.response_from_guard_execution(execution, model="gpt-test")

        self.assertEqual(guarded.guard_action, "block")
        self.assertEqual(guarded.guard_reason, "policy")
        self.assertIn("blocked by an LLM guard", message_text(provider.response_message(guarded)))

        refused = ChatGPTResponse(
            id="resp-refusal",
            model="gpt-test",
            message={"role": "assistant", "content": []},
            stop_reason="failed",
            stop_sequence=None,
            usage={},
            stop_details={"error": {"message": "Safety refusal."}},
        )

        visible = provider.response_with_visible_refusal(refused)
        self.assertIn("ChatGPT refused or failed", message_text(provider.response_message(visible)))
        self.assertIn("Safety refusal.", message_text(provider.response_message(visible)))

    async def test_stream_output_item_done_can_reconstruct_function_call(self) -> None:
        state = _ChatGPTStreamState(sequence=3)
        event = SimpleNamespace(
            type="response.output_item.done",
            output_index=0,
            item={
                "type": "function_call",
                "id": "fc-1",
                "call_id": "call-1",
                "name": "lookup",
                "arguments": '{"query":"alpha"}',
                "status": "completed",
            },
        )

        await _record_chatgpt_stream_event(
            stream=_NoopStream(),
            event=event,
            state=state,
            tool_input_blocks={},
        )

        message = ChatGPTProvider().response_message(
            ChatGPTResponse(
                id="resp-1",
                model="gpt-test",
                message=state.message(),
                stop_reason="completed",
                stop_sequence=None,
                usage={},
            )
        )
        tool_use = message["content"][0]
        self.assertEqual(tool_use["id"], "call-1")
        self.assertEqual(tool_use["name"], "lookup")
        self.assertEqual(tool_use["input"], {"query": "alpha"})

    def test_final_response_records_usage_stop_details_and_output(self) -> None:
        state = _ChatGPTStreamState(sequence=4)

        _record_final_openai_response(
            {
                "id": "resp-1",
                "model": "gpt-test",
                "status": "incomplete",
                "usage": {"input_tokens": 11, "output_tokens": 5},
                "incomplete_details": {"reason": "max_output_tokens"},
                "output": [
                    {
                        "type": "message",
                        "role": "assistant",
                        "content": [
                            {
                                "type": "output_text",
                                "text": "partial",
                                "annotations": [],
                            }
                        ],
                        "status": "incomplete",
                    }
                ],
            },
            state,
        )

        self.assertEqual(state.response_id, "resp-1")
        self.assertEqual(state.stop_reason, "incomplete")
        self.assertEqual(state.usage, {"input_tokens": 11, "output_tokens": 5})
        self.assertEqual(
            state.stop_details,
            {"incomplete_details": {"reason": "max_output_tokens"}},
        )
        self.assertEqual(state.output_items[0]["status"], "incomplete")


class GeminiProviderTests(unittest.TestCase):
    def test_serializes_request_response_and_provider_options(self) -> None:
        provider = GeminiProvider(
            thinking=GeminiThinkingConfig(include_thoughts=True, thinking_budget=1024)
        )
        request = provider.create_request(
            system_prompt="Be useful.",
            model="gemini-test",
            max_tokens=128,
            context_token_limit=10_000,
            tools=[_agent_tool_schema()],
            chat_history=[agent_message("user", "hello")],
            stream_id="stream-1",
            stream_sequence=2,
            stream_attempt=1,
            stream_agent={
                "id": "chat-1",
                "parent_id": None,
                "kind": "main",
                "label": "Main agent",
            },
        )

        self.assertEqual(
            request.tools,
            [
                {
                    "function_declarations": [
                        {
                            "name": "lookup",
                            "description": "Look up a value.",
                            "parameters_json_schema": {
                                "type": "object",
                                "properties": {},
                            },
                        }
                    ]
                }
            ],
        )
        self.assertEqual(
            request.thinking_config,
            {"include_thoughts": True, "thinking_budget": 1024},
        )
        self.assertEqual(
            provider.request_from_dict(provider.request_to_dict(request)),
            request,
        )

        response = GeminiResponse(
            id="resp-1",
            model="gemini-test",
            message={"role": "model", "parts": [{"text": "done"}]},
            stop_reason="STOP",
            stop_sequence=None,
            usage={"prompt_token_count": 1, "candidates_token_count": 2},
            stop_details={"safety_ratings": []},
        )

        self.assertEqual(
            provider.response_from_dict(provider.response_to_dict(response)),
            response,
        )
        self.assertEqual(message_text(provider.response_message(response)), "done")

    def test_tool_call_round_trip_preserves_call_id_name_and_result(self) -> None:
        messages = [
            agent_message("user", "Use the tool."),
            agent_message(
                "assistant",
                [
                    tool_use_block(
                        tool_use_id="call-1",
                        name="lookup",
                        input={"query": "alpha"},
                    )
                ],
            ),
            agent_message(
                "user",
                [
                    tool_result_block(
                        tool_use_id="call-1",
                        content=json.dumps({"value": 42}),
                    )
                ],
            ),
        ]

        contents = _agent_messages_to_gemini_contents(messages)

        self.assertEqual(
            contents[1]["parts"][0]["function_call"],
            {"name": "lookup", "args": {"query": "alpha"}, "id": "call-1"},
        )
        self.assertEqual(
            contents[2]["parts"][0]["function_response"],
            {
                "name": "lookup",
                "id": "call-1",
                "response": {"result": {"value": 42}},
            },
        )

        round_tripped = _gemini_contents_to_agent_messages(contents)
        self.assertEqual(round_tripped[1]["content"][0]["id"], "call-1")
        self.assertEqual(round_tripped[1]["content"][0]["name"], "lookup")
        self.assertEqual(round_tripped[1]["content"][0]["input"], {"query": "alpha"})
        self.assertEqual(round_tripped[2]["content"][0]["tool_use_id"], "call-1")

    def test_guard_response_and_refusal_fallback_are_visible(self) -> None:
        provider = GeminiProvider()
        execution = LlmGuardExecution(
            request={},
            response=None,
            state={},
            action=LlmGuardAction.TERMINATE,
            reason="policy",
        )

        guarded = provider.response_from_guard_execution(execution, model="gemini-test")

        self.assertEqual(guarded.guard_action, "terminate")
        self.assertEqual(guarded.guard_reason, "policy")
        self.assertIn("blocked by an LLM guard", message_text(provider.response_message(guarded)))

        refused = GeminiResponse(
            id="resp-refusal",
            model="gemini-test",
            message={"role": "model", "parts": []},
            stop_reason="SAFETY",
            stop_sequence=None,
            usage={},
            stop_details={"block_reason": "SAFETY"},
        )

        visible = provider.response_with_visible_refusal(refused)
        self.assertIn("Gemini refused", message_text(provider.response_message(visible)))
        self.assertIn("Block reason: SAFETY.", message_text(provider.response_message(visible)))

    def test_generate_content_config_disables_sdk_auto_tool_execution(self) -> None:
        request = GeminiRequest(
            system_prompt="sys",
            model="gemini-test",
            max_tokens=64,
            context_token_limit=None,
            tools=_gemini_tools_from_agent_tools([_agent_tool_schema()]),
            chat_history=[],
            thinking_config={"include_thoughts": True, "thinking_level": "low"},
        )

        config = _gemini_generate_content_config(request)
        payload = config.model_dump(mode="json", by_alias=False, exclude_none=True)

        self.assertEqual(payload["tool_config"]["function_calling_config"]["mode"], "AUTO")
        self.assertEqual(payload["automatic_function_calling"]["disable"], True)
        self.assertEqual(
            payload["thinking_config"],
            {"include_thoughts": True, "thinking_level": "LOW"},
        )

    def test_gemini_http_options_leave_retries_to_temporal(self) -> None:
        payload = _gemini_http_options().model_dump(
            mode="json",
            by_alias=False,
            exclude_none=True,
        )

        self.assertEqual(payload["retry_options"]["attempts"], 1)


class _NoopStream:
    async def text_delta(self, **_: object) -> None:
        pass

    async def thinking_started(self, **_: object) -> None:
        pass

    async def thinking_delta(self, **_: object) -> None:
        pass

    async def tool_input_started(self, **_: object) -> None:
        pass

    async def tool_input_delta(self, **_: object) -> None:
        pass

    async def tool_input_completed(self, **_: object) -> None:
        pass


def _agent_tool_schema() -> dict:
    return {
        "name": "lookup",
        "description": "Look up a value.",
        "input_schema": {"type": "object", "properties": {}},
    }


if __name__ == "__main__":
    unittest.main()
