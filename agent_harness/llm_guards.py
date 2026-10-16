from __future__ import annotations

import copy
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import timedelta
from enum import StrEnum
from typing import Any, Callable

from temporalio.common import Priority, RetryPolicy
from temporalio.workflow import ActivityCancellationType, VersioningIntent

from .activity_options import (
    DEFAULT_ACTIVITY_OPTIONS,
    ActivityOptions,
)
from .activity_router import ActivityFn, function_ref
from .guards import GuardActivityRequest, RUN_GUARD_ACTIVITY_NAME
from .invocation import maybe_await
from .streaming import (
    EmitStreamEventRequest,
    emit_harness_event,
    emit_harness_events,
    stream_error_payload,
    stream_event_request,
)
from .workflow_activities import execute_routed_activity, record_routed_activity_call

LlmGuardFn = Callable[["LlmGuardContext"], Any]


class LlmGuardTiming(StrEnum):
    PRE = "pre"
    POST = "post"


class LlmGuardAction(StrEnum):
    CONTINUE = "continue"
    BLOCK = "block"
    TERMINATE = "terminate"


@dataclass
class LlmGuardResult:
    action: LlmGuardAction = LlmGuardAction.CONTINUE
    request: dict | None = None
    response: dict | None = None
    message: str | dict | None = None
    reason: str | None = None
    state: dict | None = None

    @classmethod
    def allow(
        cls,
        *,
        request: dict[str, Any] | None = None,
        response: dict[str, Any] | None = None,
        message: str | dict[str, Any] | None = None,
        state: dict[str, Any] | None = None,
    ) -> "LlmGuardResult":
        return cls(
            action=LlmGuardAction.CONTINUE,
            request=request,
            response=response,
            message=message,
            state=state,
        )

    @classmethod
    def block(
        cls,
        message: str | dict[str, Any] | None = None,
        *,
        reason: str | None = None,
        state: dict[str, Any] | None = None,
    ) -> "LlmGuardResult":
        return cls(
            action=LlmGuardAction.BLOCK,
            message=message,
            reason=reason,
            state=state,
        )

    @classmethod
    def terminate(
        cls,
        message: str | dict[str, Any] | None = None,
        *,
        reason: str | None = None,
        state: dict[str, Any] | None = None,
    ) -> "LlmGuardResult":
        return cls(
            action=LlmGuardAction.TERMINATE,
            message=message,
            reason=reason,
            state=state,
        )


@dataclass
class LlmGuardContext:
    guard_name: str
    timing: LlmGuardTiming
    request: dict
    response: dict | None = None
    state: dict = field(default_factory=dict)
    stream_id: str | None = None
    stream_agent: dict[str, str | None] | None = None
    activity_options: ActivityOptions = DEFAULT_ACTIVITY_OPTIONS
    operation_id: str | None = None
    _activity_count: int = field(default=0, init=False)
    _used_unstepped_activity: bool = field(default=False, init=False)

    async def activity(
        self,
        fn: ActivityFn,
        *,
        step: str | None = None,
        args: dict[str, Any] | None = None,
        activity_options: ActivityOptions | None = None,
        task_queue: str | None = None,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        retry_policy: RetryPolicy | None = None,
        cancellation_type: ActivityCancellationType | None = None,
        activity_id: str | None = None,
        versioning_intent: VersioningIntent | None = None,
        priority: Priority | None = None,
    ) -> Any:
        self._activity_count, self._used_unstepped_activity = (
            record_routed_activity_call(
                owner=f"LLM guard {self.guard_name}",
                step=step,
                activity_count=self._activity_count,
                used_unstepped_activity=self._used_unstepped_activity,
            )
        )
        return await execute_routed_activity(
            activity_name=RUN_GUARD_ACTIVITY_NAME,
            request=GuardActivityRequest(
                function_ref=function_ref(fn),
                args=args or {},
                guard_name=self.guard_name,
                step=step,
                stream_id=self.stream_id,
                stream_agent=self.stream_agent,
                parent_operation_id=self.operation_id,
            ),
            summary_base=f"llm_guard:{self.timing.value}:{self.guard_name}",
            step=step,
            defaults=self.activity_options,
            activity_options=activity_options,
            task_queue=task_queue,
            schedule_to_close_timeout=schedule_to_close_timeout,
            schedule_to_start_timeout=schedule_to_start_timeout,
            start_to_close_timeout=start_to_close_timeout,
            heartbeat_timeout=heartbeat_timeout,
            retry_policy=retry_policy,
            cancellation_type=cancellation_type,
            activity_id=activity_id,
            versioning_intent=versioning_intent,
            priority=priority,
        )


@dataclass
class LlmGuardExecution:
    request: dict
    response: dict | None
    state: dict
    action: LlmGuardAction = LlmGuardAction.CONTINUE
    reason: str | None = None
    stream_events: list[EmitStreamEventRequest] = field(default_factory=list)

    @property
    def halted(self) -> bool:
        return self.action != LlmGuardAction.CONTINUE

    @property
    def terminated(self) -> bool:
        return self.action == LlmGuardAction.TERMINATE


class LlmGuardPipeline:
    def __init__(
        self,
        *,
        pre_guards: Iterable[LlmGuardFn] | None = None,
        post_guards: Iterable[LlmGuardFn] | None = None,
    ) -> None:
        self._pre_guards = _guard_list(pre_guards)
        self._post_guards = _guard_list(post_guards)

    async def execute_pre(
        self,
        *,
        request: dict[str, Any],
        state: dict[str, Any] | None = None,
        stream_id: str | None,
        stream_agent: dict[str, str | None] | None = None,
        activity_options: ActivityOptions,
        defer_stream_events: bool = False,
        emit_stream_events: bool = True,
    ) -> LlmGuardExecution:
        return await self._execute(
            self._pre_guards,
            timing=LlmGuardTiming.PRE,
            request=request,
            response=None,
            state=state or {},
            stream_id=stream_id,
            stream_agent=stream_agent,
            activity_options=activity_options,
            defer_stream_events=defer_stream_events,
            emit_stream_events=emit_stream_events,
        )

    async def execute_post(
        self,
        *,
        request: dict[str, Any],
        response: dict[str, Any],
        state: dict[str, Any],
        stream_id: str | None,
        stream_agent: dict[str, str | None] | None = None,
        activity_options: ActivityOptions,
        defer_stream_events: bool = False,
        emit_stream_events: bool = True,
    ) -> LlmGuardExecution:
        return await self._execute(
            self._post_guards,
            timing=LlmGuardTiming.POST,
            request=request,
            response=response,
            state=state,
            stream_id=stream_id,
            stream_agent=stream_agent,
            activity_options=activity_options,
            defer_stream_events=defer_stream_events,
            emit_stream_events=emit_stream_events,
        )

    async def _execute(
        self,
        guards: list[LlmGuardFn],
        *,
        timing: LlmGuardTiming,
        request: dict[str, Any],
        response: dict[str, Any] | None,
        state: dict[str, Any],
        stream_id: str | None,
        stream_agent: dict[str, str | None] | None,
        activity_options: ActivityOptions,
        defer_stream_events: bool,
        emit_stream_events: bool,
    ) -> LlmGuardExecution:
        current_request = _copy_dict(request)
        current_response = None if response is None else _copy_dict(response)
        current_state = _copy_dict(state)
        stream_events: list[EmitStreamEventRequest] = []

        llm_sequence = current_request.get("stream_sequence")
        agent_id = (
            stream_agent.get("id")
            if stream_agent is not None
            else "main"
        )
        llm_operation_id = f"{agent_id or 'main'}:llm:{llm_sequence or 'unknown'}"

        for guard_index, guard in enumerate(guards):
            guard_name = _guard_name(guard)
            operation_id = (
                f"{llm_operation_id}:guard:{timing.value}:{guard_index}:{guard_name}"
            )
            await _emit_llm_guard_event(
                stream_id=stream_id,
                kind="harness_llm_guard_start",
                payload={
                    "operation_id": operation_id,
                    "parent_operation_id": llm_operation_id,
                    "guard_name": guard_name,
                    "timing": timing.value,
                    "llm_sequence": llm_sequence,
                    "status": "running",
                },
                agent=stream_agent,
                tool_name=guard_name,
                deferred=stream_events if defer_stream_events else None,
                enabled=emit_stream_events,
            )
            ctx = LlmGuardContext(
                guard_name=guard_name,
                timing=timing,
                request=_copy_dict(current_request),
                response=None
                if current_response is None
                else _copy_dict(current_response),
                state=_copy_dict(current_state),
                stream_id=stream_id,
                stream_agent=stream_agent,
                activity_options=activity_options,
                operation_id=operation_id,
            )
            try:
                result = await call_llm_guard(guard, ctx)
            except Exception as err:
                await _emit_llm_guard_event(
                    stream_id=stream_id,
                    kind="harness_llm_guard_failed",
                    payload={
                        "operation_id": operation_id,
                        "parent_operation_id": llm_operation_id,
                        "guard_name": guard_name,
                        "timing": timing.value,
                        "llm_sequence": llm_sequence,
                        "status": "failed",
                        "error": stream_error_payload(err),
                    },
                    agent=stream_agent,
                    tool_name=guard_name,
                    deferred=stream_events if defer_stream_events else None,
                    enabled=emit_stream_events,
                )
                if defer_stream_events:
                    await emit_harness_events(stream_events)
                raise

            current_request = _copy_dict(result.request or ctx.request)
            if current_response is not None or result.response is not None:
                current_response = _copy_dict(result.response or ctx.response or {})
            current_state = _copy_dict(ctx.state)
            if result.state is not None:
                current_state.update(_copy_dict(result.state))

            if result.message is not None:
                current_response = _response_with_message(
                    current_response,
                    result.message,
                    model=str(current_request.get("model") or "guarded"),
                    guard_name=guard_name,
                )

            await _emit_llm_guard_event(
                stream_id=stream_id,
                kind="harness_llm_guard_complete",
                payload={
                    "operation_id": operation_id,
                    "parent_operation_id": llm_operation_id,
                    "guard_name": guard_name,
                    "timing": timing.value,
                    "llm_sequence": llm_sequence,
                    "status": (
                        "passed"
                        if result.action == LlmGuardAction.CONTINUE
                        else result.action.value
                    ),
                    "reason": result.reason,
                },
                agent=stream_agent,
                tool_name=guard_name,
                deferred=stream_events if defer_stream_events else None,
                enabled=emit_stream_events,
            )

            if result.action != LlmGuardAction.CONTINUE:
                current_response = _blocked_response(
                    current_response,
                    message=result.message,
                    model=str(current_request.get("model") or "guarded"),
                    guard_name=guard_name,
                )
                return LlmGuardExecution(
                    request=current_request,
                    response=current_response,
                    state=current_state,
                    action=result.action,
                    reason=result.reason,
                    stream_events=stream_events,
                )

        return LlmGuardExecution(
            request=current_request,
            response=current_response,
            state=current_state,
            stream_events=stream_events,
        )


async def _emit_llm_guard_event(
    *,
    stream_id: str | None,
    kind: str,
    payload: object,
    agent: dict[str, str | None] | None,
    tool_name: str,
    deferred: list[EmitStreamEventRequest] | None,
    enabled: bool,
) -> None:
    if not enabled:
        return
    if deferred is None:
        await emit_harness_event(
            stream_id=stream_id,
            kind=kind,
            payload=payload,
            agent=agent,
            tool_name=tool_name,
        )
        return
    event = stream_event_request(
        stream_id=stream_id,
        kind=kind,
        payload=payload,
        agent=agent,
        tool_name=tool_name,
    )
    if event is not None:
        deferred.append(event)


async def call_llm_guard(
    fn: LlmGuardFn,
    ctx: LlmGuardContext,
) -> LlmGuardResult:
    result = await maybe_await(fn(ctx))

    if result is None:
        return LlmGuardResult.allow()
    if not isinstance(result, LlmGuardResult):
        raise TypeError(f"LLM guard {_guard_name(fn)} must return LlmGuardResult")

    return result


def _guard_list(guards: Iterable[LlmGuardFn] | None) -> list[LlmGuardFn]:
    guard_list = list(guards or ())
    for guard in guard_list:
        if not callable(guard):
            raise TypeError("LLM guards must be callable")
    return guard_list


def _guard_name(fn: LlmGuardFn) -> str:
    return getattr(fn, "__name__", type(fn).__name__)


def _copy_dict(value: dict[str, Any]) -> dict[str, Any]:
    return copy.deepcopy(value)


def _response_with_message(
    response: dict[str, Any] | None,
    message: str | dict[str, Any],
    *,
    model: str,
    guard_name: str,
) -> dict[str, Any]:
    updated = _copy_dict(response or _empty_guard_response(model, guard_name))
    updated["message"] = _message_from_value(message)
    return updated


def _blocked_response(
    response: dict[str, Any] | None,
    *,
    message: str | dict[str, Any] | None,
    model: str,
    guard_name: str,
) -> dict[str, Any]:
    updated = _copy_dict(response or _empty_guard_response(model, guard_name))
    updated["message"] = _message_from_value(
        message or "The response was blocked by an LLM guard."
    )

    updated["stop_reason"] = "refusal"
    updated["stop_sequence"] = None
    return updated


def _empty_guard_response(model: str, guard_name: str) -> dict[str, Any]:
    return {
        "id": f"guard:{guard_name}",
        "model": model,
        "message": _message_from_value("The response was blocked by an LLM guard."),
        "stop_reason": "refusal",
        "stop_sequence": None,
        "usage": {},
    }


def _message_from_value(message: str | dict[str, Any]) -> dict[str, Any]:
    if isinstance(message, str):
        return {"role": "assistant", "content": message}

    updated = _copy_dict(message)
    updated.setdefault("role", "assistant")
    return updated
