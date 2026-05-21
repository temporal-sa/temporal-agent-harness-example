from __future__ import annotations

import copy
import inspect
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import timedelta
from enum import StrEnum
from typing import Any, Callable

from temporalio import workflow
from temporalio.common import Priority, RetryPolicy
from temporalio.workflow import ActivityCancellationType, VersioningIntent

from .activity_options import (
    DEFAULT_ACTIVITY_OPTIONS,
    ActivityOptions,
    activity_options_with_overrides,
)
from .activity_router import ActivityFn, function_ref
from .guards import GuardActivityRequest, RUN_GUARD_ACTIVITY_NAME

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
    request: dict[str, Any] | None = None
    response: dict[str, Any] | None = None
    message: str | dict[str, Any] | None = None
    reason: str | None = None
    state: dict[str, Any] | None = None

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
    request: dict[str, Any]
    response: dict[str, Any] | None = None
    state: dict[str, Any] = field(default_factory=dict)
    stream_id: str | None = None
    activity_options: ActivityOptions = DEFAULT_ACTIVITY_OPTIONS
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
        guard_step = self.guard_name if step is None else f"{self.guard_name}:{step}"
        summary = f"llm_guard:{self.timing.value}:{guard_step}"
        options = activity_options_with_overrides(
            self.activity_options,
            activity_options=activity_options,
            task_queue=task_queue,
            schedule_to_close_timeout=schedule_to_close_timeout,
            schedule_to_start_timeout=schedule_to_start_timeout,
            start_to_close_timeout=start_to_close_timeout,
            heartbeat_timeout=heartbeat_timeout,
            retry_policy=retry_policy,
            cancellation_type=cancellation_type,
            versioning_intent=versioning_intent,
            priority=priority,
        )
        activity_kwargs = options.to_execute_activity_kwargs()
        if activity_id is not None:
            activity_kwargs["activity_id"] = activity_id

        self._record_activity_call(step)

        return await workflow.execute_activity(
            RUN_GUARD_ACTIVITY_NAME,
            GuardActivityRequest(
                function_ref=function_ref(fn),
                args=args or {},
                guard_name=self.guard_name,
                step=step,
                stream_id=self.stream_id,
            ),
            summary=summary,
            **activity_kwargs,
        )

    def _record_activity_call(self, step: str | None) -> None:
        if step is None and self._activity_count > 0:
            raise ValueError(
                f"LLM guard {self.guard_name} called multiple activities; "
                "pass step=..."
            )
        if step is not None and self._used_unstepped_activity:
            raise ValueError(
                f"LLM guard {self.guard_name} mixed an unstepped activity with "
                "stepped activities"
            )

        self._activity_count += 1
        if step is None:
            self._used_unstepped_activity = True


@dataclass
class LlmGuardExecution:
    request: dict[str, Any]
    response: dict[str, Any] | None
    state: dict[str, Any]
    action: LlmGuardAction = LlmGuardAction.CONTINUE
    reason: str | None = None

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
        activity_options: ActivityOptions,
    ) -> LlmGuardExecution:
        return await self._execute(
            self._pre_guards,
            timing=LlmGuardTiming.PRE,
            request=request,
            response=None,
            state=state or {},
            stream_id=stream_id,
            activity_options=activity_options,
        )

    async def execute_post(
        self,
        *,
        request: dict[str, Any],
        response: dict[str, Any],
        state: dict[str, Any],
        stream_id: str | None,
        activity_options: ActivityOptions,
    ) -> LlmGuardExecution:
        return await self._execute(
            self._post_guards,
            timing=LlmGuardTiming.POST,
            request=request,
            response=response,
            state=state,
            stream_id=stream_id,
            activity_options=activity_options,
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
        activity_options: ActivityOptions,
    ) -> LlmGuardExecution:
        current_request = _copy_dict(request)
        current_response = None if response is None else _copy_dict(response)
        current_state = _copy_dict(state)

        for guard in guards:
            guard_name = _guard_name(guard)
            ctx = LlmGuardContext(
                guard_name=guard_name,
                timing=timing,
                request=_copy_dict(current_request),
                response=None
                if current_response is None
                else _copy_dict(current_response),
                state=_copy_dict(current_state),
                stream_id=stream_id,
                activity_options=activity_options,
            )
            result = await call_llm_guard(guard, ctx)

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
                )

        return LlmGuardExecution(
            request=current_request,
            response=current_response,
            state=current_state,
        )


async def call_llm_guard(
    fn: LlmGuardFn,
    ctx: LlmGuardContext,
) -> LlmGuardResult:
    result = fn(ctx)
    if inspect.isawaitable(result):
        result = await result

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
