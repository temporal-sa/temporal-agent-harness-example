from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import timedelta
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Callable, cast

from temporalio import activity as temporal_activity
from temporalio.common import Priority, RetryPolicy
from temporalio.workflow import ActivityCancellationType, VersioningIntent

from .activity_options import (
    DEFAULT_ACTIVITY_OPTIONS,
    ActivityOptions,
)
from .activity_router import (
    ActivityFn,
    RoutedActivityContext,
    call_activity,
    function_ref,
    resolve_function_ref,
)
from .invocation import bind_keyword_arguments, maybe_await
from .streaming import StreamContext, emit_harness_event, stream_error_payload
from .tool_types import (
    ToolCategory,
    ToolType,
    normalize_tool_category,
    tool_category_set,
)
from .workflow_activities import execute_routed_activity
from .workflow_activities import record_routed_activity_call

if TYPE_CHECKING:
    from .tools import ToolResult

GuardFn = Callable[..., Any]
RUN_GUARD_ACTIVITY_NAME = "agent_harness.run_guard_activity"
_GUARD_METADATA_ATTR = "__agent_harness_guard__"


class GuardTiming(StrEnum):
    PRE = "pre"
    POST = "post"


@dataclass(frozen=True)
class GuardMetadata:
    name: str
    fulfills: ToolCategory | Iterable[ToolCategory]


def guard(
    *,
    name: str,
    fulfills: ToolCategory | Iterable[ToolCategory],
):
    def decorator(fn: GuardFn) -> GuardFn:
        setattr(fn, _GUARD_METADATA_ATTR, GuardMetadata(name=name, fulfills=fulfills))
        return fn

    return decorator


def guard_metadata(fn: Any) -> GuardMetadata | None:
    return cast(
        GuardMetadata | None,
        _decorator_metadata(fn, _GUARD_METADATA_ATTR),
    )


@dataclass(frozen=True)
class GuardPolicy:
    required_pre: frozenset[ToolCategory] = frozenset(
        {str(ToolType.ADMIN), str(ToolType.MUTATING), str(ToolType.MCP)}
    )
    required_post: frozenset[ToolCategory] = frozenset()

    @classmethod
    def require(
        cls,
        *,
        pre: ToolCategory | object | Iterable[ToolCategory | object] = (),
        post: ToolCategory | object | Iterable[ToolCategory | object] = (),
    ) -> "GuardPolicy":
        return cls(
            required_pre=_optional_category_set(pre),
            required_post=_optional_category_set(post),
        )

    @classmethod
    def require_pre(
        cls,
        *categories: ToolCategory | object,
    ) -> "GuardPolicy":
        return cls(required_pre=tool_category_set(categories), required_post=frozenset())

    @classmethod
    def require_post(
        cls,
        *categories: ToolCategory | object,
    ) -> "GuardPolicy":
        return cls(required_pre=frozenset(), required_post=tool_category_set(categories))

    @classmethod
    def allow_all(cls) -> "GuardPolicy":
        return cls(required_pre=frozenset(), required_post=frozenset())

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "required_pre",
            frozenset(normalize_tool_category(value) for value in self.required_pre),
        )
        object.__setattr__(
            self,
            "required_post",
            frozenset(normalize_tool_category(value) for value in self.required_post),
        )


@dataclass
class GuardResult:
    passed: bool
    reason: str | None = None
    llm_payload: dict | None = None
    internal_payload: dict | None = None


@dataclass
class GuardContext:
    guard_name: str
    tool_name: str
    tool_type: ToolCategory
    tool_args: dict
    tool_result: ToolResult | None = None
    stream_id: str | None = None
    stream_agent: dict[str, str | None] | None = None
    tool_call_id: str | None = None
    llm_sequence: int | None = None
    operation_id: str | None = None
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
        self._activity_count, self._used_unstepped_activity = (
            record_routed_activity_call(
                owner=f"Guard {self.guard_name}",
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
                tool_call_id=self.tool_call_id,
                parent_operation_id=self.operation_id,
            ),
            summary_base=self.guard_name,
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
class GuardActivityRequest:
    function_ref: str
    args: dict
    guard_name: str | None = None
    step: str | None = None
    stream_id: str | None = None
    stream_agent: dict[str, str | None] | None = None
    tool_call_id: str | None = None
    parent_operation_id: str | None = None


@dataclass
class GuardDef:
    name: str
    fulfills: frozenset[ToolCategory]
    fn: GuardFn


@dataclass
class GuardFailure:
    payload: dict


class GuardSet:
    def __init__(self, *, guard_policy: GuardPolicy | None = None) -> None:
        self._guard_registry: dict[str, GuardDef] = {}
        self._guard_functions: dict[GuardFn, GuardDef] = {}
        self._guard_policy = guard_policy or GuardPolicy()

    def guard(
        self,
        *,
        name: str,
        fulfills: ToolCategory | Iterable[ToolCategory],
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
                "agent_harness.guards.guard and register it before using it in a tool"
            ) from err

    def get_guard(self, name: str) -> GuardDef:
        try:
            return self._guard_registry[name]
        except KeyError as err:
            raise ValueError(f"Unknown guard: {name}") from err

    def validate_tool_guards(
        self,
        *,
        tool_type: ToolCategory,
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
        tool_type: ToolCategory,
        tool_args: dict[str, Any],
        tool_result: ToolResult | None,
        stream_id: str | None,
        stream_agent: dict[str, str | None] | None,
        tool_call_id: str | None,
        llm_sequence: int | None,
        parent_operation_id: str,
        activity_options: ActivityOptions,
    ) -> GuardFailure | None:
        for guard_index, guard in enumerate(guards):
            operation_id = (
                f"{parent_operation_id}:guard:{timing.value}:{guard_index}:{guard.name}"
            )
            await emit_harness_event(
                stream_id=stream_id,
                kind="harness_tool_guard_start",
                payload={
                    "operation_id": operation_id,
                    "parent_operation_id": parent_operation_id,
                    "guard_name": guard.name,
                    "timing": timing.value,
                    "tool_name": tool_name,
                    "tool_type": tool_type,
                    "llm_sequence": llm_sequence,
                    "status": "running",
                },
                agent=stream_agent,
                tool_name=guard.name,
                tool_call_id=tool_call_id,
            )
            ctx = GuardContext(
                guard_name=guard.name,
                tool_name=tool_name,
                tool_type=tool_type,
                tool_args=tool_args,
                tool_result=tool_result,
                stream_id=stream_id,
                stream_agent=stream_agent,
                tool_call_id=tool_call_id,
                llm_sequence=llm_sequence,
                operation_id=operation_id,
                activity_options=activity_options,
            )
            try:
                result = await call_guard(guard.fn, ctx)
            except Exception as err:
                await emit_harness_event(
                    stream_id=stream_id,
                    kind="harness_tool_guard_failed",
                    payload={
                        "operation_id": operation_id,
                        "parent_operation_id": parent_operation_id,
                        "guard_name": guard.name,
                        "timing": timing.value,
                        "tool_name": tool_name,
                        "tool_type": tool_type,
                        "llm_sequence": llm_sequence,
                        "status": "failed",
                        "error": stream_error_payload(err),
                    },
                    agent=stream_agent,
                    tool_name=guard.name,
                    tool_call_id=tool_call_id,
                )
                raise

            await emit_harness_event(
                stream_id=stream_id,
                kind="harness_tool_guard_complete",
                payload={
                    "operation_id": operation_id,
                    "parent_operation_id": parent_operation_id,
                    "guard_name": guard.name,
                    "timing": timing.value,
                    "tool_name": tool_name,
                    "tool_type": tool_type,
                    "llm_sequence": llm_sequence,
                    "status": "passed" if result.passed else "blocked",
                    "reason": result.reason,
                },
                agent=stream_agent,
                tool_name=guard.name,
                tool_call_id=tool_call_id,
            )
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
        agent=request.stream_agent,
        tool_call_id=request.tool_call_id,
    )
    activity_context = RoutedActivityContext(
        route_kind="guard",
        route_name=request.guard_name,
        step=request.step,
        stream_id=request.stream_id,
    )
    activity_attempt = temporal_activity.info().attempt
    parent_operation_id = request.parent_operation_id or (
        f"guard:{request.guard_name or 'unknown'}"
    )
    operation_id = (
        f"{parent_operation_id}:activity:{request.step or request.function_ref}"
    )
    await stream.emit(
        {
            "operation_id": operation_id,
            "parent_operation_id": parent_operation_id,
            "activity_name": request.function_ref,
            "guard_name": request.guard_name,
            "activity_attempt": activity_attempt,
            "status": "running" if activity_attempt == 1 else "retrying",
        },
        kind="harness_guard_activity_start",
    )
    try:
        result = await call_activity(
            fn,
            request.args,
            stream,
            activity_context=activity_context,
        )
    except Exception as err:
        await stream.emit(
            {
                "operation_id": operation_id,
                "parent_operation_id": parent_operation_id,
                "activity_name": request.function_ref,
                "guard_name": request.guard_name,
                "activity_attempt": activity_attempt,
                "status": "failed",
                "error": stream_error_payload(err),
            },
            kind="harness_guard_activity_failed",
        )
        raise

    await stream.emit(
        {
            "operation_id": operation_id,
            "parent_operation_id": parent_operation_id,
            "activity_name": request.function_ref,
            "guard_name": request.guard_name,
            "activity_attempt": activity_attempt,
            "status": "complete",
        },
        kind="harness_guard_activity_complete",
    )
    return result


async def call_guard(fn: GuardFn, ctx: GuardContext) -> GuardResult:
    kwargs = _kwargs_for_guard(fn, ctx)
    result = await maybe_await(fn(**kwargs))

    if not isinstance(result, GuardResult):
        raise TypeError(f"Guard {fn.__name__} must return GuardResult")

    return result


def tool_type_set(
    fulfills: ToolCategory | Iterable[ToolCategory],
) -> frozenset[ToolCategory]:
    return tool_category_set(fulfills)


def _optional_category_set(
    values: ToolCategory | object | Iterable[ToolCategory | object],
) -> frozenset[ToolCategory]:
    if isinstance(values, str):
        return tool_category_set(values)
    try:
        values_list = list(values)
    except TypeError:
        return tool_category_set(values)
    if not values_list:
        return frozenset()
    return tool_category_set(values_list)


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
    return bind_keyword_arguments(
        fn,
        special_values={GuardContext: ctx},
        argument_label="guard",
    )


def _decorator_metadata(fn: Any, attr: str) -> Any:
    metadata = getattr(fn, attr, None)
    if metadata is not None:
        return metadata

    wrapped = getattr(fn, "__func__", None)
    if wrapped is None:
        return None
    return getattr(wrapped, attr, None)
