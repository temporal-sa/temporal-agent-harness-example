import inspect
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Awaitable, Protocol

if TYPE_CHECKING:
    from .llm_guards import LlmGuardFn

try:
    from temporalio import activity as temporal_activity
except ImportError:  # pragma: no cover - used by the minimal sandbox Lambda zip.
    temporal_activity = None


@dataclass(frozen=True)
class StreamEvent:
    stream_id: str | None
    tool_name: str | None
    step: str | None
    kind: str
    payload: object
    sequence: int
    agent: dict[str, str | None] | None = None
    tool_call_id: str | None = None
    emitted_at: str | None = None


class StreamSink(Protocol):
    def emit(self, event: StreamEvent) -> Awaitable[None] | None:
        pass


class AgentStreamEventKind(StrEnum):
    AGENT_START = "agent_start"
    AGENT_TEXT_DELTA = "agent_text_delta"
    AGENT_THINKING_START = "agent_thinking_start"
    AGENT_THINKING_DELTA = "agent_thinking_delta"
    AGENT_TOOL_INPUT_START = "agent_tool_input_start"
    AGENT_TOOL_INPUT_DELTA = "agent_tool_input_delta"
    AGENT_TOOL_INPUT_COMPLETE = "agent_tool_input_complete"
    AGENT_COMPLETE = "agent_complete"
    AGENT_CANCELLED = "agent_cancelled"
    AGENT_FAILED = "agent_failed"


@dataclass
class StreamContext:
    stream_id: str | None
    tool_name: str | None = None
    step: str | None = None
    agent: dict[str, str | None] | None = None
    tool_call_id: str | None = None
    _sequence: int = field(default=0, init=False)

    async def emit(self, payload: Any, *, kind: str = "message") -> None:
        sink = _stream_sink
        if sink is None:
            return

        self._sequence += 1
        event = StreamEvent(
            stream_id=self.stream_id,
            tool_name=self.tool_name,
            step=self.step,
            kind=kind,
            payload=payload,
            sequence=self._sequence,
            agent=dict(self.agent) if self.agent is not None else None,
            tool_call_id=self.tool_call_id,
            emitted_at=datetime.now(timezone.utc).isoformat(),
        )

        try:
            llm_guard = _stream_llm_guard
            content_key: str | None = None
            if (
                llm_guard is not None
                and isinstance(event.payload, dict)
                and event.kind
                in (
                    AgentStreamEventKind.AGENT_TEXT_DELTA,
                    AgentStreamEventKind.AGENT_THINKING_DELTA,
                    AgentStreamEventKind.AGENT_COMPLETE,
                )
            ):
                if event.kind == AgentStreamEventKind.AGENT_THINKING_DELTA:
                    content_key = "thinking"
                else:
                    content_key = "text"

                stream_payload = dict(event.payload)
                stream_content = stream_payload.get(content_key)
                if isinstance(stream_content, str):
                    from .activity_options import DEFAULT_ACTIVITY_OPTIONS
                    from .llm_guards import LlmGuardPipeline

                    model = str(stream_payload.get("provider") or "stream")
                    guarded = await LlmGuardPipeline(
                        post_guards=[llm_guard]
                    ).execute_post(
                        request={"model": model},
                        response={
                            "id": f"stream:{event.kind}",
                            "model": model,
                            "message": {
                                "role": "assistant",
                                "content": stream_content,
                            },
                            "stop_reason": stream_payload.get("stop_reason"),
                            "stop_sequence": None,
                            "usage": stream_payload.get("usage") or {},
                        },
                        state={},
                        stream_id=event.stream_id,
                        stream_agent=event.agent,
                        activity_options=DEFAULT_ACTIVITY_OPTIONS,
                        emit_stream_events=False,
                    )
                    guarded_message = (guarded.response or {}).get("message")
                    guarded_content = (
                        guarded_message.get("content")
                        if isinstance(guarded_message, dict)
                        else None
                    )
                    if not isinstance(guarded_content, str):
                        raise TypeError(
                            "Stream LLM guards must leave response.message.content "
                            "as a string"
                        )
                    stream_payload[content_key] = guarded_content
                    event = StreamEvent(
                        stream_id=event.stream_id,
                        tool_name=event.tool_name,
                        step=event.step,
                        kind=event.kind,
                        payload=stream_payload,
                        sequence=event.sequence,
                        agent=event.agent,
                        tool_call_id=event.tool_call_id,
                        emitted_at=event.emitted_at,
                    )

            result = sink.emit(event)
            if inspect.isawaitable(result):
                await result
        except Exception:
            if _raise_stream_errors:
                raise


@dataclass
class AgentStreamWriter:
    stream: StreamContext
    provider: str
    attempt: int | None = None

    @classmethod
    def for_provider(
        cls,
        *,
        stream_id: str | None,
        provider: str,
        step: str | None = None,
        attempt: int | None = None,
        agent: dict[str, str | None] | None = None,
    ) -> "AgentStreamWriter":
        return cls(
            stream=StreamContext(
                stream_id=stream_id,
                tool_name="agent",
                step=step,
                agent=agent,
            ),
            provider=provider,
            attempt=attempt,
        )

    async def agent_started(
        self,
        *,
        sequence: int | None,
        model: str | None = None,
    ) -> None:
        await self._emit(
            AgentStreamEventKind.AGENT_START,
            {"sequence": sequence, "model": model},
        )

    async def agent_cancelled(self, *, sequence: int | None) -> None:
        await self._emit(
            AgentStreamEventKind.AGENT_CANCELLED,
            {"sequence": sequence},
        )

    async def agent_failed(
        self,
        *,
        sequence: int | None,
        model: str | None,
        error: BaseException,
    ) -> None:
        await self._emit(
            AgentStreamEventKind.AGENT_FAILED,
            {
                "sequence": sequence,
                "model": model,
                "error": stream_error_payload(error),
            },
        )

    async def agent_completed(
        self,
        *,
        sequence: int | None,
        id: str | None,
        model: str | None,
        stop_reason: str | None,
        stop_details: dict[str, Any] | None,
        text: str,
        usage: dict[str, Any],
    ) -> None:
        await self._emit(
            AgentStreamEventKind.AGENT_COMPLETE,
            {
                "id": id,
                "model": model,
                "sequence": sequence,
                "stop_reason": stop_reason,
                "stop_details": stop_details,
                "text": text,
                "usage": usage,
            },
        )

    async def text_delta(self, *, sequence: int | None, text: str) -> None:
        await self._emit(
            AgentStreamEventKind.AGENT_TEXT_DELTA,
            {"sequence": sequence, "text": text},
        )

    async def thinking_started(
        self,
        *,
        sequence: int | None,
        content_block_index: int,
    ) -> None:
        await self._emit(
            AgentStreamEventKind.AGENT_THINKING_START,
            {
                "sequence": sequence,
                "content_block_index": content_block_index,
            },
        )

    async def thinking_delta(self, *, sequence: int | None, thinking: str) -> None:
        await self._emit(
            AgentStreamEventKind.AGENT_THINKING_DELTA,
            {"sequence": sequence, "thinking": thinking},
        )

    async def tool_input_started(
        self,
        *,
        sequence: int | None,
        content_block_index: int,
        tool_use_id: str | None,
        tool_name: str | None,
        tool_type: str | None,
    ) -> None:
        await self._emit(
            AgentStreamEventKind.AGENT_TOOL_INPUT_START,
            {
                "sequence": sequence,
                "content_block_index": content_block_index,
                "tool_use_id": tool_use_id,
                "tool_name": tool_name,
                "tool_type": tool_type,
            },
        )

    async def tool_input_delta(
        self,
        *,
        sequence: int | None,
        content_block_index: int,
        tool_use_id: str | None,
        tool_name: str | None,
        tool_type: str | None,
        partial_json: str,
    ) -> None:
        await self._emit(
            AgentStreamEventKind.AGENT_TOOL_INPUT_DELTA,
            {
                "sequence": sequence,
                "content_block_index": content_block_index,
                "tool_use_id": tool_use_id,
                "tool_name": tool_name,
                "tool_type": tool_type,
                "partial_json": partial_json,
            },
        )

    async def tool_input_completed(
        self,
        *,
        sequence: int | None,
        content_block_index: int,
        tool_use_id: str | None,
        tool_name: str | None,
        tool_type: str | None,
        input: Any,
        input_preview: str,
    ) -> None:
        await self._emit(
            AgentStreamEventKind.AGENT_TOOL_INPUT_COMPLETE,
            {
                "sequence": sequence,
                "content_block_index": content_block_index,
                "tool_use_id": tool_use_id,
                "tool_name": tool_name,
                "tool_type": tool_type,
                "input": input,
                "input_preview": input_preview,
            },
        )

    async def _emit(
        self,
        kind: AgentStreamEventKind,
        payload: dict[str, Any],
    ) -> None:
        sequence = payload.get("sequence")
        agent_id = (
            self.stream.agent.get("id")
            if self.stream.agent is not None
            else "main"
        )
        await self.stream.emit(
            {
                "provider": self.provider,
                "operation_id": f"{agent_id or 'main'}:llm:{sequence or 'unknown'}",
                **payload,
                **_attempt_payload(self.attempt),
            },
            kind=kind.value,
        )


_stream_sink: StreamSink | None = None
_stream_llm_guard: "LlmGuardFn | None" = None
_raise_stream_errors = False


def _activity_attempt() -> int | None:
    if temporal_activity is None:
        return None
    try:
        return temporal_activity.info().attempt
    except RuntimeError:
        return None


def _attempt_payload(stream_attempt: int | None) -> dict[str, int]:
    activity_attempt = _activity_attempt()
    payload: dict[str, int] = {}
    if stream_attempt is not None:
        payload["stream_attempt"] = stream_attempt
    if activity_attempt is not None:
        payload["activity_attempt"] = activity_attempt

    if stream_attempt is None:
        if activity_attempt is not None:
            payload["attempt"] = activity_attempt
        return payload

    # Keep the existing UI's single attempt value monotonic across both
    # workflow-level request retries and Temporal activity retries.
    payload["attempt"] = stream_attempt * 1000 + (activity_attempt or 1)
    return payload


def configure_stream_sink(
    sink: StreamSink | None,
    *,
    llm_guard: "LlmGuardFn | None" = None,
    raise_stream_errors: bool = False,
) -> None:
    global _stream_sink, _stream_llm_guard, _raise_stream_errors
    _stream_sink = sink
    _stream_llm_guard = llm_guard
    _raise_stream_errors = raise_stream_errors


@dataclass
class EmitStreamEventRequest:
    stream_id: str | None
    tool_name: str | None
    step: str | None
    kind: str
    payload: Any
    agent: dict[str, str | None] | None = None
    tool_call_id: str | None = None


@dataclass
class EmitStreamEventsRequest:
    events: list[EmitStreamEventRequest]


async def emit_stream_event_activity(request: EmitStreamEventRequest) -> None:
    stream = StreamContext(
        stream_id=request.stream_id,
        tool_name=request.tool_name,
        step=request.step,
        agent=request.agent,
        tool_call_id=request.tool_call_id,
    )
    await stream.emit(request.payload, kind=request.kind)


async def emit_stream_events_activity(request: EmitStreamEventsRequest) -> None:
    for event in request.events:
        await emit_stream_event_activity(event)


if temporal_activity is not None:
    emit_stream_event_activity = temporal_activity.defn(
        name="agent_harness.emit_stream_event"
    )(emit_stream_event_activity)
    emit_stream_events_activity = temporal_activity.defn(
        name="agent_harness.emit_stream_events"
    )(emit_stream_events_activity)


def stream_event_request(
    *,
    stream_id: str | None,
    kind: str,
    payload: object,
    agent: dict[str, str | None] | None = None,
    tool_name: str | None = None,
    tool_call_id: str | None = None,
    step: str | None = None,
) -> EmitStreamEventRequest | None:
    if stream_id is None:
        return None
    return EmitStreamEventRequest(
        stream_id=stream_id,
        tool_name=tool_name,
        step=step,
        kind=kind,
        payload=payload,
        agent=agent,
        tool_call_id=tool_call_id,
    )


def start_harness_event_batch(
    events: list[EmitStreamEventRequest],
) -> Any | None:
    if not events:
        return None

    from temporalio import workflow
    from temporalio.common import RetryPolicy

    return workflow.start_activity(
        "agent_harness.emit_stream_events",
        EmitStreamEventsRequest(events=list(events)),
        start_to_close_timeout=timedelta(seconds=10),
        retry_policy=RetryPolicy(maximum_attempts=1),
        summary=f"stream:batch:{len(events)}",
    )


async def wait_harness_event_batch(handle: Any | None) -> None:
    if handle is None:
        return
    try:
        await handle
    except Exception:
        # Visibility failures must not fail the user's agent run.
        return


async def emit_harness_events(events: list[EmitStreamEventRequest]) -> None:
    await wait_harness_event_batch(start_harness_event_batch(events))


async def emit_harness_event(
    *,
    stream_id: str | None,
    kind: str,
    payload: object,
    agent: dict[str, str | None] | None = None,
    tool_name: str | None = None,
    tool_call_id: str | None = None,
    step: str | None = None,
) -> None:
    event = stream_event_request(
        stream_id=stream_id,
        kind=kind,
        payload=payload,
        agent=agent,
        tool_name=tool_name,
        tool_call_id=tool_call_id,
        step=step,
    )
    if event is None:
        return

    # This function is used from deterministic Workflow code. The actual
    # sideband write stays in an Activity so replay never performs HTTP or file
    # I/O. Visibility failures must not fail the user's agent run.
    try:
        await emit_harness_events([event])
    except Exception:
        return


def stream_error_payload(error: BaseException) -> dict[str, str]:
    return {
        "type": type(error).__name__,
        "message": str(error),
    }
