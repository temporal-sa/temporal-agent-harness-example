"""Gap-fillers for temporal-agent-harness 0.7.0 and Pydantic AI 2.14.1.

Every use of a private harness or Pydantic AI internal lives in this module, with the 0.7.0
source location it depends on, so a harness upgrade has one file to re-check
(CTO memo 001, "Private harness internals we'd depend on"). Contract tests in
tests/test_harness_ext.py pin the shapes these helpers rely on.
"""

from __future__ import annotations

import asyncio
import os
import typing
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Sequence
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import timedelta
from typing import Any

from pydantic import BaseModel
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
    _runner_from_ctx,
    _tool_schema,
)
from temporal_agent_harness.harness.agent_protocol import (
    SEND_AGENT_MESSAGE_UPDATE,
    TOOL_APPROVAL_UPDATE,
    AgentMessage,
    ToolApprovalPolicy,
    MidTurn,
    ModelInteractionStarted,
    ToolApprovalDecision,
)
from temporal_agent_harness.harness.agent_workflow import AgentWorkflowRunner

# Model deltas are flushed to the workflow as one Signal per batch. The harness default is
# 50 ms; 500 ms keeps a long coding turn inside Temporal's per-run limits (PRD A20, CTO-5).
STREAM_BATCH_INTERVAL = timedelta(
    milliseconds=int(os.environ.get("AGENT_STREAM_BATCH_MS", "500"))
)

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
        session_id, owner_id = _spend_ids(run_context)
        guard = _spend_guard() if activity.in_activity() and session_id else None
        if guard is not None:
            # Hard caps (PRD A21): checked before every model call, fail closed.
            await guard.check(session_id, owner_id)
        async with _heartbeating():
            try:
                async with self.wrapped.request_stream(
                    messages, model_settings, model_request_parameters, run_context
                ) as response:
                    try:
                        yield response
                    finally:
                        if guard is not None:
                            usage = stream_usage(response)
                            await guard.record(
                                session_id,
                                owner_id,
                                getattr(usage, "input_tokens", 0) or 0,
                                getattr(usage, "output_tokens", 0) or 0,
                            )
            except ModelHTTPError as exc:
                raise _classify(exc) from exc


_SPEND_GUARD: Any = None


def _spend_guard() -> Any:
    """One SpendGuard per worker process (activity side only)."""
    global _SPEND_GUARD
    if _SPEND_GUARD is None:
        from simple_chat_agent.agent.budget import spend_guard_from_env

        _SPEND_GUARD = spend_guard_from_env()
    return _SPEND_GUARD


def _spend_ids(run_context: RunContext[Any] | None) -> tuple[str, str]:
    deps = getattr(run_context, "deps", None)
    return getattr(deps, "session_id", "") or "", getattr(deps, "owner_id", "") or ""


def _classify(exc: ModelHTTPError) -> Exception:
    from simple_chat_agent.agent import context

    if context.is_context_overflow(exc):
        # Keep overflow distinguishable from other 400s: the workflow retries it once with
        # a tighter history budget (PRD A17).
        return context.context_overflow_error(exc)
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
    is_interrupted: Callable[[], bool] = lambda: False,
    before_call: Callable[[str], str | None] | None = None,
    on_result: Callable[[str, str, dict[str, Any], Any], None] | None = None,
    after_call: Callable[[str], None] | None = None,
) -> FunctionToolset[Any]:
    """Adapt harness tools into a FunctionToolset that may be passed to ``run(toolsets=...)``.

    Pydantic AI accepts a per-run FunctionToolset under Temporal only when every tool carries
    ``metadata={'temporal': False}`` (durable_exec/_runtime_toolsets.py). Harness tools run
    in-workflow through run_tool anyway, so the opt-out is exactly right, and building per run
    lets each session inject its own workspace.

    This is the harness's ``as_pydantic_ai_tool`` with one change. The harness adapter turns
    every exception into a "Tool failed" string the model sees, including the ActivityError a
    cancelled tool activity raises (Temporal reports workflow-side activity cancellation as
    ActivityError, not CancelledError). After the user interrupts, that would let the turn
    carry on. Here a failure while ``is_interrupted()`` re-raises as CancelledError.

    ``before_call(name)`` may refuse a call by returning the text the model sees instead
    (e.g. a concurrency limit). ``on_result(name, tool_id, args, result)`` runs in the
    workflow after a successful call (e.g. to update observable state). ``after_call(name)``
    runs after every call that ``before_call`` let through, success or not.
    """
    adapted: list[Tool[Any]] = []
    for tool in tools:
        name, description, schema = _tool_schema(tool)
        adapted.append(
            Tool.from_schema(
                _invoker(tool, name, injections, is_interrupted, before_call, on_result, after_call),
                name=name,
                description=description,
                json_schema=schema,
                takes_ctx=True,
            )
        )
        adapted[-1].metadata = {"temporal": False}
    return FunctionToolset(adapted, id=id)


def _invoker(
    tool: Callable[..., Awaitable[Any]],
    name: str,
    injections: dict[str, Any] | None,
    is_interrupted: Callable[[], bool],
    before_call: Callable[[str], str | None] | None,
    on_result: Callable[[str, str, dict[str, Any], Any], None] | None,
    after_call: Callable[[str], None] | None = None,
) -> Callable[..., Awaitable[Any]]:
    # Pydantic AI flattens a tool whose only parameter is a model: the model sends that
    # model's fields as top-level arguments. Re-wrap them so run_tool binds the parameter.
    # (The harness's own adapter has the same gap; filed upstream.)
    single_arg = Tool(tool, takes_ctx=False).function_schema.single_arg_name
    # Pydantic AI hands tools JSON-shaped arguments. Activity tools are coerced by Temporal's
    # converter on the way into the activity, but inline tools would see plain dicts, so
    # coerce parameters annotated with a pydantic model here.
    model_params = {
        pname: ann
        for pname, ann in typing.get_type_hints(tool).items()
        if isinstance(ann, type) and issubclass(ann, BaseModel)
    }

    async def invoke(ctx: RunContext[Any], **kwargs: Any) -> Any:
        runner = _runner_from_ctx(ctx, tool_name=name)
        if single_arg:
            kwargs = {single_arg: kwargs}
        for pname, model in model_params.items():
            if isinstance(kwargs.get(pname), dict):
                kwargs[pname] = model.model_validate(kwargs[pname])
        if before_call is not None:
            refusal = before_call(name)
            if refusal:
                return refusal
        try:
            result = await runner.run_tool(ctx.tool_call_id, tool, injections=injections, **kwargs)
        except Exception as exc:  # noqa: BLE001 - mirror the harness adapter, except on interrupt
            if is_interrupted():
                raise asyncio.CancelledError(f"{name} stopped by the user") from exc
            return f"Tool {name!r} failed: {exc}"
        finally:
            if after_call is not None:
                after_call(name)
        if on_result is not None:
            on_result(name, ctx.tool_call_id or "", kwargs, result)
        # Result models carry structured fields (for state and the UI) and a compact __str__
        # written for the model; send the model the compact text.
        if isinstance(result, BaseModel) and type(result).__str__ is not BaseModel.__str__:
            return str(result)
        return result

    return invoke


class TurnInterrupted(Exception):
    """Raised in workflow code to stop a turn the user interrupted."""


class CheckpointNeeded(Exception):
    """Raised before a model call that would take the run too close to a history limit."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def is_cancellation(exc: BaseException) -> bool:
    """True for asyncio cancellation or a Temporal ActivityError caused by cancellation."""
    from temporalio.exceptions import ActivityError, CancelledError as TemporalCancelled

    if isinstance(exc, (asyncio.CancelledError, TurnInterrupted, TemporalCancelled)):
        return True
    if isinstance(exc, ActivityError):
        return isinstance(exc.cause, TemporalCancelled)
    return False


# Failures the product explains to the user instead of showing as an unexpected error.
EXPLAINED_FAILURES = {
    "SpendCapReached": "This session reached its usage limit, so I stopped. Start a new session or try again tomorrow.",
    "SpendGuardUnavailable": "I can't check the usage limit right now, so I stopped to be safe. Try again shortly.",
    "ProviderRejected": "The model provider rejected the request. Try another model.",
    "ContextOverflow": "This conversation no longer fits the model's context window, even after trimming. Start a new session.",
}


def explained_failure(exc: BaseException) -> str | None:
    """The ApplicationError type behind ``exc`` if it is one we explain, else None."""
    from temporalio.exceptions import ApplicationError as AppError

    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, AppError) and current.type in EXPLAINED_FAILURES:
            return current.type
        current = getattr(current, "cause", None) or current.__cause__
    return None


def publish_rejection(runner: AgentWorkflowRunner, *, message_id: str, reason: str) -> None:
    """Report a front-door message the agent refused, on the stream, as a harness event.

    The harness has no "rejected" event, and its event union is closed. A
    ``message_handler_error`` carrying the message's id does the job: the client marks that
    pending message as failed with the reason, and the bridge stops counting it as pending.
    """
    from temporal_agent_harness.harness.agent_protocol import (
        MessageAccepted,
        MessageDisposition,
        MessageHandlerError,
    )

    # Accepted first, so a client that hasn't yet matched its pending send still creates
    # the message before marking it failed (the projection ignores errors for unknown ids).
    runner._pub(
        "",
        0,
        MessageAccepted(handler="", payload={}, disposition=MessageDisposition.QUEUED),
        message_id=message_id,
    )
    runner._pub("", 0, MessageHandlerError(message=reason), message_id=message_id)


def payload_storage() -> Any:
    """Large-payload offload shared by worker, API and tests (PRD §3: 256 KB threshold).

    S3 when ``SIMPLE_CHAT_S3_BUCKET`` is set (the catalog), using the app's existing S3 driver
    so every process reads the same claims; local disk otherwise. Every process that reads or
    writes this namespace's payloads must use the same store.
    """
    threshold = int(os.environ.get("AGENT_PAYLOAD_OFFLOAD_BYTES", str(256 * 1024)))
    if os.environ.get("SIMPLE_CHAT_S3_BUCKET", "").strip() or os.environ.get(
        "SIMPLE_CHAT_REQUIRE_SHARED_STORAGE", ""
    ).lower() in {"1", "true"}:
        from temporalio.converter import ExternalStorage

        from simple_chat_agent.common.external_storage import simple_chat_data_converter

        shared = simple_chat_data_converter().external_storage
        assert shared is not None
        return ExternalStorage(drivers=shared.drivers, payload_size_threshold=threshold)
    from temporal_agent_harness.utils.large_payload import local_payload_storage

    return local_payload_storage(payload_size_threshold=threshold)


# ---------------------------------------------------------------------------
# Signal front door: admit messages and approvals that arrived as Signals
# ---------------------------------------------------------------------------


def admit_message(runner: AgentWorkflowRunner, message: AgentMessage, *, message_id: str) -> None:
    """Admit a message as the ``send_agent_message`` Update would, under a caller-chosen id.

    A replica of ``AgentWorkflowRunner._handle_send_agent_message`` (agent_workflow.py, 0.7.0)
    that takes ``message_id`` instead of minting one. The API chooses the id before the Signal
    is sent, so a browser that sent a message while no worker was running can match the
    ``message_accepted`` event to its pending message when a worker later admits it. The
    harness client keys messages only on ``message_id``. Synchronous, like the original: no
    yield between reading the head offset and publishing ``message_accepted``. Raises
    ApplicationError when the runner's validator rejects the message.
    """
    from temporal_agent_harness.harness.agent_protocol import MessageAccepted, MessageDisposition
    from temporal_agent_harness.harness.agent_workflow import _Admission

    runner._validate_send_agent_message(message)
    handler = runner._handlers[message.type]
    status = runner._status
    if handler.mid_turn is MidTurn.ACCEPT and status.turn_active:
        turn_id, turn_number = status.join_turn()
        disposition = MessageDisposition.JOINED
        runner._joining.append(
            _Admission(message=message, turn_id=turn_id, turn_number=turn_number, message_id=message_id)
        )
    else:
        turn_id = str(workflow.uuid4())
        turn_number = status.reserve_turn_number()
        disposition = MessageDisposition.QUEUED if status.has_pending_work else MessageDisposition.OPENED
        status.enqueue_message(
            _Admission(message=message, turn_id=turn_id, turn_number=turn_number, message_id=message_id)
        )
    runner._pub(
        turn_id,
        turn_number,
        MessageAccepted(handler=message.type, payload=message.payload, disposition=disposition),
        message_id=message_id,
    )


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


def active_subagent_ids(runner: AgentWorkflowRunner) -> list[str]:
    """Ids of the subagents this agent is driving (``_WorkflowStatus.active_subagents``)."""
    return [info.subagent_id for info in runner._status.active_subagents()]


def set_policy_keeping_pending(runner: AgentWorkflowRunner, policy: ToolApprovalPolicy) -> None:
    """Swap the approval policy WITHOUT auto-resolving calls already waiting for a human.

    ``runner.set_approval_policy`` cascades: pending calls the new policy allows are approved
    on the spot (agent_workflow.py ``_apply_policy_update``). A posture change applies to new
    calls only (PRD A14), so this sets ``_WorkflowStatus`` directly after the same
    auto-mode-needs-an-evaluator check.
    """
    from temporal_agent_harness.harness.agent_workflow import _assert_auto_mode_has_an_evaluator

    _assert_auto_mode_has_an_evaluator(policy, runner._auto_mode_evaluator)
    runner._status.set_approval_policy(policy)


def forget_subagent(runner: AgentWorkflowRunner, subagent_id: str) -> None:
    """Drop a subagent the runner can no longer stop (its workflow already closed), so it
    doesn't block continue-as-new forever (``_WorkflowStatus.remove_subagent``)."""
    runner._status.remove_subagent(subagent_id)


def is_closed(runner: AgentWorkflowRunner) -> bool:
    """Whether ``close`` was signalled (``AgentWorkflowRunner._closed``)."""
    return runner._closed


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


def stream_base(stream: WorkflowStream) -> int:
    """The oldest offset still held in the stream's log (``WorkflowStream._base_offset``)."""
    return stream._base_offset


def agent_id(runner: AgentWorkflowRunner) -> str:
    """The runner's short agent id (``AgentWorkflowRunner._agent_id``)."""
    return runner._agent_id


def pending_tool_name(runner: AgentWorkflowRunner, tool_id: str) -> str | None:
    """The tool a pending approval is for (``_WorkflowStatus.approval_entry``)."""
    entry = runner._status.approval_entry(tool_id)
    return entry.tool_name if entry is not None else None


def guard_tool_approval(
    runner: AgentWorkflowRunner,
    adjust: Callable[[ToolApprovalDecision], ToolApprovalDecision],
    on_update: Callable[[], None] = lambda: None,
) -> None:
    """Re-register the harness ``tool_approval`` Update so every decision passes through
    ``adjust`` first (used to drop "always allow" for tools where it isn't permitted). The
    Signal front door applies the same ``adjust``."""

    async def handler(decision: ToolApprovalDecision) -> Any:
        on_update()
        return await runner._handle_tool_approval(adjust(decision))

    workflow.set_update_handler(
        TOOL_APPROVAL_UPDATE, handler, validator=runner._validate_tool_approval
    )


def guard_send_agent_message(
    runner: AgentWorkflowRunner,
    is_continuing: Callable[[], bool],
    on_update: Callable[[], None] = lambda: None,
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

    async def handler(message: AgentMessage) -> Any:
        on_update()
        return await runner._handle_send_agent_message(message)

    workflow.set_update_handler(SEND_AGENT_MESSAGE_UPDATE, handler, validator=validator)
