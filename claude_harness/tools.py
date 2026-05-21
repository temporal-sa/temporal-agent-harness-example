from __future__ import annotations

import inspect
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Awaitable, Callable, Literal, cast, get_type_hints

from pydantic import create_model
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
from .guards import (
    GuardActivityRequest,
    GuardContext,
    GuardDef,
    GuardFn,
    GuardSet,
    GuardPolicy,
    GuardResult,
    GuardTiming,
    run_guard_activity,
)
from .streaming import StreamContext
from .tool_types import ToolType

ToolFn = Callable[..., Awaitable["ToolResult"]]
DynamicToolFn = Callable[["ToolContext", dict[str, Any]], Awaitable["ToolResult"]]
GuardReference = GuardFn | str
ToolParam = dict[str, Any]
ToolArgsMode = Literal["signature", "raw"]
RUN_TOOL_ACTIVITY_NAME = "claude_harness.run_tool_activity"
_TOOL_METADATA_ATTR = "__claude_harness_tool__"
_GUARD_METADATA_ATTR = "__claude_harness_guard__"


@dataclass(frozen=True)
class ToolMetadata:
    name: str
    description: str
    tool_type: ToolType
    pre_guards: tuple[GuardReference, ...] = ()
    post_guards: tuple[GuardReference, ...] = ()


@dataclass(frozen=True)
class GuardMetadata:
    name: str
    fulfills: ToolType | Iterable[ToolType]


def tool(
    *,
    name: str,
    description: str,
    tool_type: ToolType,
    pre_guards: Iterable[GuardReference] | None = None,
    post_guards: Iterable[GuardReference] | None = None,
):
    def decorator(fn: ToolFn) -> ToolFn:
        setattr(
            fn,
            _TOOL_METADATA_ATTR,
            ToolMetadata(
                name=name,
                description=description,
                tool_type=tool_type,
                pre_guards=tuple(pre_guards or ()),
                post_guards=tuple(post_guards or ()),
            ),
        )
        return fn

    return decorator


def guard(
    *,
    name: str,
    fulfills: ToolType | Iterable[ToolType],
):
    def decorator(fn: GuardFn) -> GuardFn:
        setattr(fn, _GUARD_METADATA_ATTR, GuardMetadata(name=name, fulfills=fulfills))
        return fn

    return decorator


@dataclass
class ToolContext:
    tool_name: str
    _tools: "ToolSet"
    stream_id: str | None = None
    activity_options: ActivityOptions = DEFAULT_ACTIVITY_OPTIONS
    _activity_count: int = field(default=0, init=False)
    _used_unstepped_activity: bool = field(default=False, init=False)

    def tool_names(self) -> list[str]:
        return self._tools.tool_names()

    def tool_schemas(self, names: Iterable[str] | None = None) -> list[ToolParam]:
        return self._tools.tool_schemas(names)

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
        summary = self.tool_name if step is None else f"{self.tool_name}:{step}"
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
            RUN_TOOL_ACTIVITY_NAME,
            ToolActivityRequest(
                function_ref=function_ref(fn),
                args=args or {},
                tool_name=self.tool_name,
                step=step,
                stream_id=self.stream_id,
            ),
            summary=summary,
            **activity_kwargs,
        )

    def _record_activity_call(self, step: str | None) -> None:
        if step is None and self._activity_count > 0:
            raise ValueError(
                f"Tool {self.tool_name} called multiple activities; pass step=..."
            )
        if step is not None and self._used_unstepped_activity:
            raise ValueError(
                f"Tool {self.tool_name} mixed an unstepped activity with stepped "
                "activities"
            )

        self._activity_count += 1
        if step is None:
            self._used_unstepped_activity = True


@dataclass
class ToolResult:
    payload: dict[str, Any]
    error: bool


@dataclass
class ToolActivityRequest:
    function_ref: str
    args: dict[str, Any]
    tool_name: str | None = None
    step: str | None = None
    stream_id: str | None = None


@dataclass
class ToolDef:
    schema: ToolParam
    tool_type: ToolType
    fn: ToolFn | DynamicToolFn
    pre_guards: list[GuardDef]
    post_guards: list[GuardDef]
    args_mode: ToolArgsMode = "signature"


class ToolSet:
    def __init__(self, *, guard_policy: GuardPolicy | None = None) -> None:
        self._tool_registry: dict[str, ToolDef] = {}
        self._guards = GuardSet(guard_policy=guard_policy)

    def tool_names(self) -> list[str]:
        return list(self._tool_registry)

    def tool_schemas(self, names: Iterable[str] | None = None) -> list[ToolParam]:
        if names is None:
            return [t.schema for t in self._tool_registry.values()]
        return [self.get_tool(name).schema for name in names]

    def get_tool(self, name: str) -> ToolDef:
        return self._tool_registry[name]

    def add_provider(self, provider: object) -> object:
        guard_defs: dict[str, GuardDef] = {}
        methods = list(_provider_methods(provider))

        for method in methods:
            metadata = _guard_metadata(method)
            if metadata is None:
                continue
            guard_defs[metadata.name] = self._register_guard(method)

        for method in methods:
            metadata = _tool_metadata(method)
            if metadata is None:
                continue
            self._register_tool(
                name=metadata.name,
                description=metadata.description,
                tool_type=metadata.tool_type,
                fn=method,
                pre_guards=self._resolve_guard_refs(
                    metadata.pre_guards,
                    provider_guards=guard_defs,
                ),
                post_guards=self._resolve_guard_refs(
                    metadata.post_guards,
                    provider_guards=guard_defs,
                ),
            )

        return provider

    def add_tool(self, *tools: ToolFn) -> None:
        for fn in tools:
            metadata = _tool_metadata(fn)
            if metadata is None:
                raise ValueError(
                    f"Tool {fn.__name__} is missing @tool metadata; decorate it "
                    "with claude_harness.tools.tool before registering it"
                )

            self._register_tool(
                name=metadata.name,
                description=metadata.description,
                tool_type=metadata.tool_type,
                fn=fn,
                pre_guards=self._resolve_guard_refs(
                    metadata.pre_guards,
                    provider_guards={},
                ),
                post_guards=self._resolve_guard_refs(
                    metadata.post_guards,
                    provider_guards={},
                ),
            )

    def add_guard(self, *guards: GuardFn) -> None:
        for fn in guards:
            metadata = _guard_metadata(fn)
            if metadata is None:
                raise ValueError(
                    f"Guard {fn.__name__} is missing @guard metadata; decorate it "
                    "with claude_harness.tools.guard before registering it"
                )
            self._register_guard(fn)

    def add_dynamic_tool(
        self,
        *,
        name: str,
        description: str,
        input_schema: dict[str, Any],
        tool_type: ToolType,
        fn: DynamicToolFn,
        pre_guards: Iterable[GuardReference] | None = None,
        post_guards: Iterable[GuardReference] | None = None,
    ) -> None:
        self._register_tool(
            name=name,
            description=description,
            tool_type=tool_type,
            fn=fn,
            pre_guards=self._resolve_guard_refs(
                pre_guards or (),
                provider_guards={},
            ),
            post_guards=self._resolve_guard_refs(
                post_guards or (),
                provider_guards={},
            ),
            input_schema=input_schema,
            args_mode="raw",
        )

    def add_mcp_provider(self, provider: object) -> object:
        register = getattr(provider, "register", None)
        if not callable(register):
            raise TypeError(
                f"MCP provider {type(provider).__name__} must expose register(tools)"
            )
        register(self)
        return provider

    async def execute_tool(
        self,
        name: str,
        args: dict[str, Any] | None = None,
        *,
        stream_id: str | None = None,
        activity_options: ActivityOptions | None = None,
    ) -> ToolResult:
        tool = self.get_tool(name)
        self._guards.validate_tool_guards(
            tool_type=tool.tool_type,
            pre_guards=tool.pre_guards,
            post_guards=tool.post_guards,
        )
        tool_args = args or {}
        resolved_activity_options = activity_options or DEFAULT_ACTIVITY_OPTIONS

        pre_guard_failure = await self._guards.execute_guards(
            tool.pre_guards,
            GuardTiming.PRE,
            tool_name=name,
            tool_type=tool.tool_type,
            tool_args=tool_args,
            tool_result=None,
            stream_id=stream_id,
            activity_options=resolved_activity_options,
        )
        if pre_guard_failure is not None:
            return ToolResult(payload=pre_guard_failure.payload, error=True)

        ctx = ToolContext(
            tool_name=name,
            _tools=self,
            stream_id=stream_id,
            activity_options=resolved_activity_options,
        )
        if tool.args_mode == "raw":
            tool_result = await _call_dynamic_tool(
                cast(DynamicToolFn, tool.fn),
                ctx,
                tool_args,
            )
        else:
            tool_result = await _call_tool(cast(ToolFn, tool.fn), ctx, tool_args)

        post_guard_failure = await self._guards.execute_guards(
            tool.post_guards,
            GuardTiming.POST,
            tool_name=name,
            tool_type=tool.tool_type,
            tool_args=tool_args,
            tool_result=tool_result,
            stream_id=stream_id,
            activity_options=resolved_activity_options,
        )
        if post_guard_failure is not None:
            return ToolResult(payload=post_guard_failure.payload, error=True)

        return tool_result

    def _register_tool(
        self,
        *,
        name: str,
        description: str,
        tool_type: ToolType,
        fn: ToolFn | DynamicToolFn,
        pre_guards: list[GuardDef],
        post_guards: list[GuardDef],
        input_schema: dict[str, Any] | None = None,
        args_mode: ToolArgsMode = "signature",
    ) -> None:
        if name in self._tool_registry:
            raise ValueError(f"Duplicate tool name: {name}")

        schema_input = (
            input_schema
            if input_schema is not None
            else _input_schema_for_tool(cast(ToolFn, fn))
        )
        self._tool_registry[name] = ToolDef(
            schema={
                "name": name,
                "description": description,
                "input_schema": schema_input,
            },
            tool_type=tool_type,
            fn=fn,
            pre_guards=pre_guards,
            post_guards=post_guards,
            args_mode=args_mode,
        )

    def _resolve_guard_refs(
        self,
        guards: Iterable[GuardReference],
        *,
        provider_guards: dict[str, GuardDef],
    ) -> list[GuardDef]:
        guard_defs: list[GuardDef] = []
        for guard_ref in guards:
            if isinstance(guard_ref, str):
                guard_defs.append(
                    provider_guards.get(guard_ref) or self._guards.get_guard(guard_ref)
                )
            else:
                guard_defs.append(self._register_guard(guard_ref))
        return guard_defs

    def _register_guard(self, fn: GuardFn) -> GuardDef:
        try:
            return self._guards.def_for(fn)
        except ValueError:
            pass

        metadata = _guard_metadata(fn)
        if metadata is None:
            raise ValueError(
                f"Guard {fn.__name__} is not registered; decorate it with "
                "claude_harness.tools.guard before using it in a tool"
            )

        registered_guard = self._guards.guard(
            name=metadata.name,
            fulfills=metadata.fulfills,
        )(fn)
        return self._guards.def_for(registered_guard)


def _provider_methods(provider: object) -> Iterable[Callable[..., Any]]:
    seen: set[str] = set()
    for cls in reversed(type(provider).mro()):
        for name, value in vars(cls).items():
            if name in seen:
                continue
            seen.add(name)
            if _tool_metadata(value) is None and _guard_metadata(value) is None:
                continue
            method = getattr(provider, name)
            if not callable(method):
                raise TypeError(
                    f"Provider attribute {type(provider).__name__}.{name} "
                    "is decorated but is not callable"
                )
            yield method


def _tool_metadata(fn: Any) -> ToolMetadata | None:
    return cast(
        ToolMetadata | None,
        _decorator_metadata(fn, _TOOL_METADATA_ATTR),
    )


def _guard_metadata(fn: Any) -> GuardMetadata | None:
    return cast(
        GuardMetadata | None,
        _decorator_metadata(fn, _GUARD_METADATA_ATTR),
    )


def _decorator_metadata(fn: Any, attr: str) -> Any:
    metadata = getattr(fn, attr, None)
    if metadata is not None:
        return metadata

    wrapped = getattr(fn, "__func__", None)
    if wrapped is None:
        return None
    return getattr(wrapped, attr, None)


def _input_schema_for_tool(fn: ToolFn) -> dict[str, Any]:
    signature = inspect.signature(fn)
    type_hints = get_type_hints(fn)
    model_fields: dict[str, tuple[Any, Any]] = {}

    for name, parameter in signature.parameters.items():
        if name in ("self", "cls"):
            continue
        if parameter.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        ):
            raise TypeError(
                f"Tool {fn.__name__} cannot use positional-only, *args, or **kwargs"
            )

        annotation = type_hints.get(name, parameter.annotation)
        if annotation is inspect.Parameter.empty:
            raise TypeError(f"Tool parameter {fn.__name__}.{name} must be typed")
        if annotation is ToolContext:
            continue

        default = (
            ...
            if parameter.default is inspect.Parameter.empty
            else parameter.default
        )
        model_fields[name] = (annotation, default)

    field_definitions = cast(dict[str, Any | tuple[Any, Any]], model_fields)
    model = create_model(f"{fn.__name__}_ToolInput", **field_definitions)
    return model.model_json_schema()


@temporal_activity.defn(name=RUN_TOOL_ACTIVITY_NAME)
async def run_tool_activity(request: ToolActivityRequest) -> Any:
    fn = resolve_function_ref(request.function_ref)
    stream = StreamContext(
        stream_id=request.stream_id,
        tool_name=request.tool_name,
        step=request.step,
    )
    return await call_activity(fn, request.args, stream)


async def _call_tool(
    fn: ToolFn, ctx: ToolContext, args: dict[str, Any]
) -> ToolResult:
    kwargs = _kwargs_for_tool(fn, ctx, args)
    result = fn(**kwargs)
    if inspect.isawaitable(result):
        return await result
    return result


async def _call_dynamic_tool(
    fn: DynamicToolFn, ctx: ToolContext, args: dict[str, Any]
) -> ToolResult:
    result = fn(ctx, args)
    if inspect.isawaitable(result):
        return await result
    return result


def _kwargs_for_tool(
    fn: ToolFn, ctx: ToolContext, args: dict[str, Any]
) -> dict[str, Any]:
    signature = inspect.signature(fn)
    type_hints = get_type_hints(fn)
    kwargs: dict[str, Any] = {}
    consumed_args: set[str] = set()

    for name, parameter in signature.parameters.items():
        if name in ("self", "cls"):
            continue

        annotation = type_hints.get(name, parameter.annotation)
        if annotation is ToolContext:
            kwargs[name] = ctx
            continue

        if name in args:
            kwargs[name] = args[name]
            consumed_args.add(name)
        elif parameter.default is inspect.Parameter.empty:
            raise TypeError(f"Missing required tool argument {fn.__name__}.{name}")

    unexpected_args = set(args) - consumed_args
    if unexpected_args:
        names = ", ".join(sorted(unexpected_args))
        raise TypeError(f"Unexpected tool argument(s) for {fn.__name__}: {names}")

    return kwargs
