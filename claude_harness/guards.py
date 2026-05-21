from __future__ import annotations

import inspect
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import timedelta
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Callable, get_type_hints

from temporalio import activity as temporal_activity
from temporalio import workflow
from temporalio.common import Priority, RetryPolicy
from temporalio.workflow import ActivityCancellationType, VersioningIntent

from .activity_options import (
    DEFAULT_ACTIVITY_OPTIONS,
    ActivityOptions,
    activity_options_with_overrides,
)
from .activity_router import ActivityFn, call_activity, function_ref, resolve_function_ref
from .streaming import StreamContext
from .tool_types import ToolType

if TYPE_CHECKING:
    from .tools import ToolResult

GuardFn = Callable[..., Any]
RUN_GUARD_ACTIVITY_NAME = "claude_harness.run_guard_activity"


class GuardTiming(StrEnum):
    PRE = "pre"
    POST = "post"


@dataclass(frozen=True)
class GuardPolicy:
    required_pre: frozenset[ToolType] = frozenset(
        {ToolType.ADMIN, ToolType.MUTATING}
    )
    required_post: frozenset[ToolType] = frozenset()


@dataclass
class GuardResult:
    passed: bool
    reason: str | None = None
    llm_payload: dict[str, Any] | None = None
    internal_payload: dict[str, Any] | None = None


@dataclass
class GuardContext:
    guard_name: str
    tool_name: str
    tool_type: ToolType
    tool_args: dict[str, Any]
    tool_result: ToolResult | None = None
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
        summary = self.guard_name if step is None else f"{self.guard_name}:{step}"
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
                f"Guard {self.guard_name} called multiple activities; pass step=..."
            )
        if step is not None and self._used_unstepped_activity:
            raise ValueError(
                f"Guard {self.guard_name} mixed an unstepped activity with stepped "
                "activities"
            )

        self._activity_count += 1
        if step is None:
            self._used_unstepped_activity = True


@dataclass
class GuardActivityRequest:
    function_ref: str
    args: dict[str, Any]
    guard_name: str | None = None
    step: str | None = None
    stream_id: str | None = None


@dataclass
class GuardDef:
    name: str
    fulfills: frozenset[ToolType]
    fn: GuardFn


@dataclass
class GuardFailure:
    payload: dict[str, Any]


class GuardSet:
    def __init__(self, *, guard_policy: GuardPolicy | None = None) -> None:
        self._guard_registry: dict[str, GuardDef] = {}
        self._guard_functions: dict[GuardFn, GuardDef] = {}
        self._guard_policy = guard_policy or GuardPolicy()

    def guard(
        self,
        *,
        name: str,
        fulfills: ToolType | Iterable[ToolType],
    ):
        def decorator(fn: GuardFn) -> GuardFn:
            if name in self._guard_registry:
                raise ValueError(f"Duplicate guard name: {name}")

            guard = GuardDef(
                name=name,
                fulfills=tool_type_set(fulfills),
                fn=fn,
            )
            self._guard_registry[name] = guard
            self._guard_functions[fn] = guard
            return fn

        return decorator

    def defs_for(self, guards: Iterable[GuardFn]) -> list[GuardDef]:
        return [self.def_for(guard) for guard in guards]

    def def_for(self, guard: GuardFn) -> GuardDef:
        try:
            return self._guard_functions[guard]
        except KeyError as err:
            raise ValueError(
                f"Guard {guard.__name__} is not registered; decorate it with "
                "claude_harness.tools.guard and register it before using it in a tool"
            ) from err

    def get_guard(self, name: str) -> GuardDef:
        try:
            return self._guard_registry[name]
        except KeyError as err:
            raise ValueError(f"Unknown guard: {name}") from err

    def validate_tool_guards(
        self,
        *,
        tool_type: ToolType,
        pre_guards: list[GuardDef],
        post_guards: list[GuardDef],
    ) -> None:
        if tool_type in self._guard_policy.required_pre and not pre_guards:
            raise ValueError(f"Tool type {tool_type} requires at least one pre guard")
        if tool_type in self._guard_policy.required_post and not post_guards:
            raise ValueError(f"Tool type {tool_type} requires at least one post guard")

        for guard in pre_guards:
            if tool_type not in guard.fulfills:
                raise ValueError(f"Pre guard {guard.name} does not fulfill {tool_type}")
        for guard in post_guards:
            if tool_type not in guard.fulfills:
                raise ValueError(
                    f"Post guard {guard.name} does not fulfill {tool_type}"
                )

    async def execute_guards(
        self,
        guards: list[GuardDef],
        timing: GuardTiming,
        *,
        tool_name: str,
        tool_type: ToolType,
        tool_args: dict[str, Any],
        tool_result: ToolResult | None,
        stream_id: str | None,
        activity_options: ActivityOptions,
    ) -> GuardFailure | None:
        for guard in guards:
            ctx = GuardContext(
                guard_name=guard.name,
                tool_name=tool_name,
                tool_type=tool_type,
                tool_args=tool_args,
                tool_result=tool_result,
                stream_id=stream_id,
                activity_options=activity_options,
            )
            result = await call_guard(guard.fn, ctx)
            if not result.passed:
                return GuardFailure(
                    payload=guard_failure_payload(guard, timing, result)
                )

        return None


@temporal_activity.defn(name=RUN_GUARD_ACTIVITY_NAME)
async def run_guard_activity(request: GuardActivityRequest) -> Any:
    fn = resolve_function_ref(request.function_ref)
    stream = StreamContext(
        stream_id=request.stream_id,
        tool_name=request.guard_name,
        step=request.step,
    )
    return await call_activity(fn, request.args, stream)


async def call_guard(fn: GuardFn, ctx: GuardContext) -> GuardResult:
    kwargs = _kwargs_for_guard(fn, ctx)
    result = fn(**kwargs)
    if inspect.isawaitable(result):
        result = await result

    if not isinstance(result, GuardResult):
        raise TypeError(f"Guard {fn.__name__} must return GuardResult")

    return result


def tool_type_set(fulfills: ToolType | Iterable[ToolType]) -> frozenset[ToolType]:
    if isinstance(fulfills, ToolType):
        return frozenset({fulfills})

    tool_types = frozenset(fulfills)
    if not tool_types:
        raise ValueError("Guard must fulfill at least one ToolType")
    invalid_tool_types = [t for t in tool_types if not isinstance(t, ToolType)]
    if invalid_tool_types:
        raise TypeError("Guard fulfills must contain only ToolType values")
    return tool_types


def guard_failure_payload(
    guard: GuardDef, timing: GuardTiming, result: GuardResult
) -> dict[str, Any]:
    return result.llm_payload or {
        "error": "Guard failed",
        "guard": guard.name,
        "timing": timing.value,
        "reason": result.reason or "Guard did not pass",
    }


def _kwargs_for_guard(fn: GuardFn, ctx: GuardContext) -> dict[str, Any]:
    signature = inspect.signature(fn)
    type_hints = get_type_hints(fn)
    kwargs: dict[str, Any] = {}

    for name, parameter in signature.parameters.items():
        if name in ("self", "cls"):
            continue

        annotation = type_hints.get(name, parameter.annotation)
        if annotation is GuardContext:
            kwargs[name] = ctx
            continue

        if parameter.default is inspect.Parameter.empty:
            raise TypeError(f"Missing required guard argument {fn.__name__}.{name}")

    return kwargs
