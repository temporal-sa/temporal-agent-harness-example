from __future__ import annotations

import unittest

from temporalio.converter import DataConverter

from agent_harness.activity_options import DEFAULT_ACTIVITY_OPTIONS
from agent_harness.llm_guards import LlmGuardContext, LlmGuardPipeline, LlmGuardResult
from agent_harness.streaming import (
    AgentStreamEventKind,
    AgentStreamWriter,
    EmitStreamEventRequest,
    EmitStreamEventsRequest,
    StreamContext,
    StreamEvent,
    configure_stream_sink,
)
from simple_chat_agent.worker.good_place_guards import good_place_post_guard


class RecordingStreamSink:
    def __init__(self) -> None:
        self.events: list[StreamEvent] = []

    def emit(self, event: StreamEvent) -> None:
        self.events.append(event)


class StreamingTests(unittest.IsolatedAsyncioTestCase):
    def tearDown(self) -> None:
        configure_stream_sink(None)

    async def test_stream_sink_without_guard_emits_original_event(self) -> None:
        sink = RecordingStreamSink()
        configure_stream_sink(sink)

        await StreamContext(stream_id="stream-1").emit(
            {"text": "What the fuck?"},
            kind=AgentStreamEventKind.AGENT_TEXT_DELTA,
        )

        self.assertEqual(len(sink.events), 1)
        self.assertEqual(sink.events[0].payload, {"text": "What the fuck?"})

    async def test_emit_stream_event_request_decodes_nested_payload(self) -> None:
        request = EmitStreamEventRequest(
            stream_id="stream-1",
            tool_name="search_web",
            step="searxng",
            kind="harness_tool_activity_failed",
            payload={
                "activity_attempt": 2,
                "error": {
                    "type": "ApplicationError",
                    "message": "rate limited",
                },
            },
        )

        payloads = await DataConverter.default.encode([request])
        decoded = await DataConverter.default.decode(
            payloads,
            [EmitStreamEventRequest],
        )

        self.assertEqual(decoded, [request])

    async def test_emit_stream_events_request_decodes_a_batch(self) -> None:
        request = EmitStreamEventsRequest(
            events=[
                EmitStreamEventRequest(
                    stream_id="stream-1",
                    tool_name="good_place_pre_guard",
                    step=None,
                    kind="harness_llm_guard_start",
                    payload={"status": "running"},
                ),
                EmitStreamEventRequest(
                    stream_id="stream-1",
                    tool_name="good_place_pre_guard",
                    step=None,
                    kind="harness_llm_guard_complete",
                    payload={"status": "passed"},
                ),
            ]
        )

        payloads = await DataConverter.default.encode([request])
        decoded = await DataConverter.default.decode(
            payloads,
            [EmitStreamEventsRequest],
        )

        self.assertEqual(decoded, [request])

    async def test_llm_guard_can_defer_its_visibility_events_as_one_batch(self) -> None:
        def guard(ctx: LlmGuardContext) -> LlmGuardResult:
            return LlmGuardResult.allow(response=ctx.response)

        execution = await LlmGuardPipeline(post_guards=[guard]).execute_post(
            request={"model": "test", "stream_sequence": 2},
            response={
                "message": {"role": "assistant", "content": "hello"},
            },
            state={},
            stream_id="stream-1",
            stream_agent={"id": "main"},
            activity_options=DEFAULT_ACTIVITY_OPTIONS,
            defer_stream_events=True,
        )

        self.assertEqual(
            [event.kind for event in execution.stream_events],
            ["harness_llm_guard_start", "harness_llm_guard_complete"],
        )

    async def test_stream_event_preserves_agent_and_tool_call_identity(self) -> None:
        sink = RecordingStreamSink()
        configure_stream_sink(sink)
        agent = {
            "id": "chat-1-subagent-1",
            "parent_id": "chat-1",
            "kind": "subagent",
            "label": "Research conservation policy",
        }

        await StreamContext(
            stream_id="chat-1",
            tool_name="search_web",
            step="searxng",
            agent=agent,
            tool_call_id="tool-use-1",
        ).emit(
            {"query": "shark conservation policy"},
            kind="search_start",
        )

        self.assertEqual(sink.events[0].agent, agent)
        self.assertEqual(sink.events[0].tool_call_id, "tool-use-1")
        self.assertIsNotNone(sink.events[0].emitted_at)

    async def test_agent_failure_keeps_model_attempt_and_error_details(self) -> None:
        sink = RecordingStreamSink()
        configure_stream_sink(sink)
        stream = AgentStreamWriter.for_provider(
            stream_id="chat-1",
            provider="claude",
            attempt=2,
            agent={"id": "chat-1", "kind": "main", "label": "Main agent"},
        )

        await stream.agent_failed(
            sequence=4,
            model="claude-sonnet",
            error=ConnectionError("connection reset"),
        )

        event = sink.events[0]
        self.assertEqual(event.kind, AgentStreamEventKind.AGENT_FAILED)
        self.assertEqual(event.payload["operation_id"], "chat-1:llm:4")
        self.assertEqual(event.payload["model"], "claude-sonnet")
        self.assertEqual(event.payload["stream_attempt"], 2)
        self.assertEqual(
            event.payload["error"],
            {"type": "ConnectionError", "message": "connection reset"},
        )

    async def test_stream_sink_runs_text_delta_through_llm_guard(self) -> None:
        sink = RecordingStreamSink()
        configure_stream_sink(sink, llm_guard=good_place_post_guard)

        await StreamContext(stream_id="stream-1").emit(
            {
                "provider": "claude",
                "sequence": 3,
                "text": "What the fuck?",
            },
            kind=AgentStreamEventKind.AGENT_TEXT_DELTA,
        )

        self.assertEqual(len(sink.events), 1)
        self.assertEqual(
            sink.events[0].payload,
            {
                "provider": "claude",
                "sequence": 3,
                "text": "What the fork?",
            },
        )

    async def test_stream_sink_guards_thinking_and_complete_text(self) -> None:
        sink = RecordingStreamSink()
        configure_stream_sink(sink, llm_guard=good_place_post_guard)
        stream = StreamContext(stream_id="stream-1")

        await stream.emit(
            {"provider": "claude", "thinking": "This is bullshit."},
            kind=AgentStreamEventKind.AGENT_THINKING_DELTA,
        )
        await stream.emit(
            {
                "provider": "claude",
                "text": "Damn, that was shitty.",
                "usage": {"input_tokens": 10},
            },
            kind=AgentStreamEventKind.AGENT_COMPLETE,
        )

        self.assertEqual(
            sink.events[0].payload,
            {"provider": "claude", "thinking": "This is bullshirt."},
        )
        self.assertEqual(
            sink.events[1].payload,
            {
                "provider": "claude",
                "text": "Dang, that was shirty.",
                "usage": {"input_tokens": 10},
            },
        )

    async def test_stream_sink_leaves_non_text_event_untouched(self) -> None:
        calls = 0

        def guard(ctx: LlmGuardContext) -> LlmGuardResult:
            nonlocal calls
            calls += 1
            return LlmGuardResult.allow(response=ctx.response)

        sink = RecordingStreamSink()
        configure_stream_sink(sink, llm_guard=guard)

        await StreamContext(stream_id="stream-1").emit(
            {"sequence": 4},
            kind=AgentStreamEventKind.AGENT_START,
        )

        self.assertEqual(calls, 0)
        self.assertEqual(sink.events[0].payload, {"sequence": 4})

    async def test_stream_sink_awaits_async_llm_guard(self) -> None:
        async def guard(ctx: LlmGuardContext) -> LlmGuardResult:
            response = ctx.response or {}
            message = response["message"]
            message["content"] = str(message["content"]).upper()
            return LlmGuardResult.allow(response=response)

        sink = RecordingStreamSink()
        configure_stream_sink(sink, llm_guard=guard)

        await StreamContext(stream_id="stream-1").emit(
            {"text": "guard me"},
            kind=AgentStreamEventKind.AGENT_TEXT_DELTA,
        )

        self.assertEqual(sink.events[0].payload, {"text": "GUARD ME"})


if __name__ == "__main__":
    unittest.main()
