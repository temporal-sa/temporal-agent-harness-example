"""Gap-fillers for temporal-agent-harness 0.7.0 and Pydantic AI 2.14.1.

Every use of a private harness or Pydantic AI internal lives in this module, with the 0.7.0
source location it depends on, so a harness upgrade has one file to re-check
(CTO memo 001, "Private harness internals we'd depend on"). Contract tests in
tests/test_harness_ext.py pin the shapes these helpers rely on.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Sequence
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import timedelta
from typing import Any

from pydantic_ai import FunctionToolset, Tool
from pydantic_ai.durable_exec.temporal import TemporalAgent
from pydantic_ai.exceptions import ModelHTTPError
from pydantic_ai.messages import (
    AgentStreamEvent,
    BaseToolCallPart,
    ModelMessage,
    ModelRequest,
    ModelResponse,
    ToolCallPart,
    ToolReturnPart,
    UserPromptPart,
)
from pydantic_ai.models import ModelRequestParameters, StreamedResponse
from pydantic_ai.models.wrapper import WrapperModel
from pydantic_ai.settings import ModelSettings
from pydantic_ai.tools import RunContext
from temporalio import activity, workflow
from temporalio.contrib.workflow_streams import WorkflowStream
from temporalio.exceptions import ApplicationError

from temporal_agent_harness.ai_sdks.pydantic_ai_harness import (
    PydanticAIStreamObserver,
    _resolve_stream_context,
    as_pydantic_ai_tool,
)
from temporal_agent_harness.harness.agent_protocol import (
    SEND_AGENT_MESSAGE_UPDATE,
    AgentMessage,
    ModelInteractionStarted,
    ToolApprovalDecision,
)
from temporal_agent_harness.harness.agent_workflow import AgentWorkflowRunner

# Model deltas are flushed to the workflow as one Signal per batch. The harness default is
# 50 ms; 500 ms keeps a long coding turn inside Temporal's per-run limits (PRD A20, CTO-5).
STREAM_BATCH_INTERVAL = timedelta(milliseconds=500)

# How often the model activity heartbeats while a request is in flight (PRD A19). Pair with a
# heartbeat_timeout of ~10 s on the model activity and a worker heartbeat throttle of ~1 s.
HEARTBEAT_EVERY_SECONDS = 2.0

# HTTP statuses that may succeed on retry. Everything else in 4xx is the request's fault.
_RETRYABLE_4XX = frozenset({408, 409, 429})


# ---------------------------------------------------------------------------
# Usage extraction fix (Pydantic AI 2.14.1 + genai-prices >= 0.1.10)
# ---------------------------------------------------------------------------


def _install_usage_extract_fix() -> None:
    """Keep token usage when genai-prices returns fields RequestUsage doesn't know.

    ``RequestUsage.extract`` builds ``cls(**usage.__dict__)`` and swallows any error, so the
    ``output_reasoning_tokens`` field genai-prices now returns makes every OpenAI and Gemini
    usage come back as zero (pydantic_ai/usage.py, ``RequestUsage.extract``). This wrapper
    keeps the known fields and moves reasoning tokens into ``details``. Filed upstream.
    """
    import dataclasses

    from genai_prices.data_snapshot import get_snapshot
    from pydantic_ai.usage import RequestUsage

    if getattr(RequestUsage.extract, "__coding_agent_fix__", False):
        return
    known = {f.name for f in dataclasses.fields(RequestUsage)}
    original = RequestUsage.extract.__func__

    def extract(cls, data, *, provider, provider_url, provider_fallback, api_flavor="default", details=None):
        details = dict(details or {})
        for provider_id, api_url in [(None, provider_url), (provider, None), (provider_fallback, None)]:
            try:
                found = get_snapshot().find_provider(None, provider_id, api_url)
                _ref, usage = found.extract_usage(data, api_flavor=api_flavor)
            except Exception:  # noqa: BLE001 - same fallthrough as upstream
                continue
            values = {k: v for k, v in usage.__dict__.items() if v is not None}
            reasoning = values.pop("output_reasoning_tokens", None)
            if reasoning and "reasoning_tokens" not in details:
                details["reasoning_tokens"] = reasoning
            return cls(**{k: v for k, v in values.items() if k in known}, details=details)
        return original(cls, data, provider=provider, provider_url=provider_url,
                        provider_fallback=provider_fallback, api_flavor=api_flavor, details=details)

    extract.__coding_agent_fix__ = True  # type: ignore[attr-defined]
    RequestUsage.extract = classmethod(extract)  # type: ignore[method-assign]


_install_usage_extract_fix()


# ---------------------------------------------------------------------------
# Model activity: background heartbeat + non-retryable 4xx
# ---------------------------------------------------------------------------


class HeartbeatingModel(WrapperModel):
    """Wraps a provider model so its Temporal activity heartbeats for the whole request.

    Heartbeating from a background ticker (not per stream event) covers connection setup and
    long silent reasoning, so a 10 s heartbeat timeout detects a dead worker quickly without
    failing a slow-but-healthy call. Heartbeats are also how a cancellation (interrupt)
    reaches the activity. Provider 4xx errors other than 408/409/429 become non-retryable.
    """

    async def request(
        self,
        messages: list[ModelMessage],
        model_settings: ModelSettings | None,
        model_request_parameters: ModelRequestParameters,
    ) -> ModelResponse:
        async with _heartbeating():
            try:
                return await super().request(messages, model_settings, model_request_parameters)
            except ModelHTTPError as exc:
                raise _classify(exc) from exc

    @asynccontextmanager
    async def request_stream(
        self,
        messages: list[ModelMessage],
        model_settings: ModelSettings | None,
        model_request_parameters: ModelRequestParameters,
        run_context: RunContext[Any] | None = None,
    ) -> AsyncIterator[StreamedResponse]:
        async with _heartbeating():
            try:
                async with self.wrapped.request_stream(
                    messages, model_settings, model_request_parameters, run_context
                ) as response:
                    yield response
            except ModelHTTPError as exc:
                raise _classify(exc) from exc


def _classify(exc: ModelHTTPError) -> Exception:
    status = exc.status_code
    if 400 <= status < 500 and status not in _RETRYABLE_4XX:
        return ApplicationError(
            f"The model provider rejected the request ({status}).",
            {"status_code": status, "model": exc.model_name},
            type="ProviderRejected",
            non_retryable=True,
        )
    return exc


@asynccontextmanager
async def _heartbeating() -> AsyncIterator[None]:
    """Heartbeat every HEARTBEAT_EVERY_SECONDS while the body runs (no-op outside an activity)."""
    if not activity.in_activity():
        yield
        return

    async def tick() -> None:
        while True:
            activity.heartbeat()
            await asyncio.sleep(HEARTBEAT_EVERY_SECONDS)

    ticker = asyncio.create_task(tick())
    try:
        yield
    finally:
        ticker.cancel()


# ---------------------------------------------------------------------------
# Stream observer with a configurable batch interval
# ---------------------------------------------------------------------------


class BatchedStreamObserver(PydanticAIStreamObserver):
    """The harness's Pydantic AI observer, publishing on our batch interval.

    0.7.0's observer opens its publisher with the 50 ms default and offers no knob
    (pydantic_ai_harness.py, PydanticAIStreamObserver.__aenter__). We reimplement __aenter__
    only; it touches the private ``_stack``, ``_publisher``, ``_context`` and ``_model``.
    Native (provider-hosted) tool calls are skipped: they never get tool_start/tool_end, so a
    tool_requested for them would leave a dangling row in the client projection.
    """

    async def __aenter__(self) -> BatchedStreamObserver:
        self._stack = AsyncExitStack()
        self._publisher = await self._stack.enter_async_context(
            AgentWorkflowRunner.publisher_from_activity(
                self._context, batch_interval=STREAM_BATCH_INTERVAL
            )
        )
        self._publisher.publish(ModelInteractionStarted(model=self._model))
        return self

    def _emit_tool_requested(self, pub: Any, part: BaseToolCallPart) -> None:
        if not isinstance(part, ToolCallPart):
            return
        super()._emit_tool_requested(pub, part)


async def coding_event_stream_handler(
    ctx: RunContext[Any], stream: AsyncIterable[AgentStreamEvent]
) -> None:
    """Our event_stream_handler: harness_event_stream_handler with BatchedStreamObserver."""
    context = _resolve_stream_context(ctx)
    if context is None or not isinstance(stream, StreamedResponse):
        async for _ in stream:
            pass
        return
    model = getattr(stream, "model_name", None)
    async with BatchedStreamObserver(context, model=model) as obs:
        async for event in stream:
            await obs.on_event(event)
        obs.set_usage(stream_usage(stream))


def stream_usage(stream: StreamedResponse) -> Any:
    """A stream's usage, whether ``usage`` is a method or a property.

    The 0.7.0 harness handler calls ``stream.usage()``; in Pydantic AI 2.14.1 ``usage`` is a
    property on provider streams, so the harness's call raises and is swallowed, and every
    ``model_interaction_ended`` goes out with ``usage=None``. (Filed upstream.)
    """
    usage = stream.usage
    return usage() if callable(usage) else usage


async def _drain(_ctx: RunContext[Any], stream: AsyncIterable[AgentStreamEvent]) -> None:
    async for _ in stream:
        pass


class QuietTemporalAgent(TemporalAgent):
    """A TemporalAgent that runs no activity per in-workflow tool event.

    With an event_stream_handler set, TemporalAgent schedules one ``…__event_stream_handler``
    activity for every tool-call and tool-result event (pydantic_ai durable_exec/temporal
    _agent.py, ``_call_event_stream_handler_activity``). The harness already publishes tool
    lifecycle from run_tool, so those activities only add history. The model's live streaming
    handler is unaffected: TemporalModel captured the real handler at construction, outside
    any workflow.
    """

    @property
    def event_stream_handler(self) -> Any:  # type: ignore[override]
        handler = super().event_stream_handler
        if handler is not None and workflow.in_workflow():
            return _drain
        return handler


# ---------------------------------------------------------------------------
# Per-run harness toolsets
# ---------------------------------------------------------------------------


def per_run_toolset(
    tools: Sequence[Callable[..., Awaitable[Any]]],
    *,
    id: str,
    injections: dict[str, Any] | None = None,
) -> FunctionToolset[Any]:
    """Adapt harness tools into a FunctionToolset that may be passed to ``run(toolsets=...)``.

    Pydantic AI accepts a per-run FunctionToolset under Temporal only when every tool carries
    ``metadata={'temporal': False}`` (durable_exec/_runtime_toolsets.py). Harness tools run
    in-workflow through run_tool anyway, so the opt-out is exactly right, and building per run
    lets each session inject its own workspace.
    """
    adapted: list[Tool[Any]] = []
    for tool in tools:
        t = as_pydantic_ai_tool(tool, injections=injections)
        t.metadata = {**(t.metadata or {}), "temporal": False}
        adapted.append(t)
    return FunctionToolset(adapted, id=id)


# ---------------------------------------------------------------------------
# Signal front door: admit messages and approvals that arrived as Signals
# ---------------------------------------------------------------------------


async def admit_message(runner: AgentWorkflowRunner, message: AgentMessage) -> None:
    """Admit a message exactly as the ``send_agent_message`` Update would.

    Calls the runner's validator and admission handler (agent_workflow.py
    ``_validate_send_agent_message`` / ``_handle_send_agent_message``). Admission contains no
    yielding await, so running it from a Signal handler keeps the same invariants. Raises
    ApplicationError when the validator rejects the message.
    """
    runner._validate_send_agent_message(message)
    await runner._handle_send_agent_message(message)


async def admit_approval(runner: AgentWorkflowRunner, decision: ToolApprovalDecision) -> None:
    """Apply an approval exactly as the ``tool_approval`` Update would
    (``_validate_tool_approval`` / ``_handle_tool_approval``)."""
    runner._validate_tool_approval(decision)
    await runner._handle_tool_approval(decision)


# ---------------------------------------------------------------------------
# Interrupt and continue-as-new support
# ---------------------------------------------------------------------------


def decline_pending_approvals(runner: AgentWorkflowRunner, *, turn_id: str, reason: str) -> int:
    """Decline every approval still pending in ``turn_id`` and publish the resolutions.

    A task cancelled while parked at the approval gate leaves its entry PENDING forever; only
    ``close`` finalizes it (agent_workflow.py ``_apply_approval_policy``). Interrupt resolves
    them first through ``_resolve_and_publish`` so the client's cards close and a late approve
    is rejected by the validator.
    """
    declined = 0
    for entry in runner._status.pending_approval_entries():
        if entry.turn_id == turn_id:
            runner._resolve_and_publish(entry.tool_id, approved=False, reason=reason)
            declined += 1
    return declined


def is_quiescent(runner: AgentWorkflowRunner) -> bool:
    """True when nothing is running or waiting: safe to continue as new."""
    status = runner._status
    return (
        not status.has_pending_work
        and not runner._joining
        and not status.pending_approval_entries()
        and not status.pending_callbacks()
        and not status.active_subagents()
    )


def turn_counter(runner: AgentWorkflowRunner) -> int:
    return runner._status._current_turn


def restore_turn_counter(runner: AgentWorkflowRunner, value: int) -> None:
    """Continue turn numbering across continue-as-new (``_WorkflowStatus._current_turn``).

    The client's idle poll compares turn numbers, so restarting at 1 would hide new turns
    from a tab that was open across the handoff.
    """
    runner._status._current_turn = value


def close_with_note(
    messages: list[ModelMessage], *, tool_note: str, user_note: str
) -> list[ModelMessage]:
    """Make an interrupted history safe to continue from.

    If the last message is a model response with tool calls, every call gets a tool result
    (``tool_note``) so the exchange is complete. The ``user_note`` is then added to the
    trailing request, or as a new request after a plain response, so requests and responses
    keep alternating.
    """
    if not messages:
        return messages
    last = messages[-1]
    if isinstance(last, ModelResponse):
        calls = [p for p in last.parts if isinstance(p, ToolCallPart)]
        parts: list[Any] = [
            ToolReturnPart(tool_name=c.tool_name, tool_call_id=c.tool_call_id, content=tool_note)
            for c in calls
        ]
        parts.append(UserPromptPart(content=user_note))
        return [*messages, ModelRequest(parts=parts)]
    if isinstance(last, ModelRequest):
        # Interrupted before the model answered this request: drop it rather than leave two
        # requests in a row; the stop note replaces it.
        return [*messages[:-1], ModelRequest(parts=[*last.parts, UserPromptPart(content=user_note)])]
    return messages


def stream_head(stream: WorkflowStream) -> int:
    """The next offset the stream will assign (``WorkflowStream._on_offset``)."""
    return stream._on_offset()


def agent_id(runner: AgentWorkflowRunner) -> str:
    """The runner's short agent id (``AgentWorkflowRunner._agent_id``)."""
    return runner._agent_id


def guard_send_agent_message(
    runner: AgentWorkflowRunner, is_continuing: Callable[[], bool]
) -> None:
    """Re-register the ``send_agent_message`` Update with a validator that refuses messages
    while the agent hands off to its next run.

    The runner registers this Update in its constructor (agent_workflow.py, __init__);
    registering again replaces the validator. The refusal is retryable (``ContinuingAsNew``):
    a client that retries reaches the new run.
    """

    def validator(message: AgentMessage) -> None:
        if is_continuing():
            raise ApplicationError(
                "The agent is moving to a new run; retry this message.",
                type="ContinuingAsNew",
            )
        runner._validate_send_agent_message(message)

    workflow.set_update_handler(
        SEND_AGENT_MESSAGE_UPDATE, runner._handle_send_agent_message, validator=validator
    )
