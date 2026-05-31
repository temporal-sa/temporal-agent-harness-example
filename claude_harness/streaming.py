import inspect
from dataclasses import dataclass, field
from typing import Any, Awaitable, Protocol

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
    payload: Any
    sequence: int


class StreamSink(Protocol):
    def emit(self, event: StreamEvent) -> Awaitable[None] | None:
        pass


@dataclass
class StreamContext:
    stream_id: str | None
    tool_name: str | None = None
    step: str | None = None
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
        )

        try:
            result = sink.emit(event)
            if inspect.isawaitable(result):
                await result
        except Exception:
            if _raise_stream_errors:
                raise


_stream_sink: StreamSink | None = None
_raise_stream_errors = False


def configure_stream_sink(
    sink: StreamSink | None, *, raise_stream_errors: bool = False
) -> None:
    global _stream_sink, _raise_stream_errors
    _stream_sink = sink
    _raise_stream_errors = raise_stream_errors


def stream_sink_configured() -> bool:
    return _stream_sink is not None


@dataclass
class EmitStreamEventRequest:
    stream_id: str | None
    tool_name: str | None
    step: str | None
    kind: str
    payload: Any


async def emit_stream_event_activity(request: EmitStreamEventRequest) -> None:
    stream = StreamContext(
        stream_id=request.stream_id,
        tool_name=request.tool_name,
        step=request.step,
    )
    await stream.emit(request.payload, kind=request.kind)


if temporal_activity is not None:
    emit_stream_event_activity = temporal_activity.defn(
        name="claude_harness.emit_stream_event"
    )(emit_stream_event_activity)
