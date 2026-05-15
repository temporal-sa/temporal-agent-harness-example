import inspect
import importlib
from dataclasses import dataclass, field
from datetime import timedelta
from enum import StrEnum
from collections.abc import Iterable
from typing import Any, Awaitable, Callable, cast, get_type_hints

from anthropic.types import ToolParam
from temporalio import activity as temporal_activity
from temporalio import workflow
from temporalio.exceptions import ApplicationError
from pydantic import create_model

from .streaming import StreamContext

ActivityFn = Callable[..., Any]
ToolFn = Callable[..., Awaitable["ToolResult"]]
GuardFn = Callable[..., Any]
RUN_TOOL_ACTIVITY_NAME = "claude_harness.run_tool_activity"
RUN_GUARD_ACTIVITY_NAME = "claude_harness.run_guard_activity"


class ToolType(StrEnum):
    READ = "read"
    MUTATING = "mutating"
    ADMIN = "admin"


class GuardTiming(StrEnum):
    PRE = "pre"
    POST = "post"


@dataclass(frozen=True)
class GuardPolicy:
    required_pre: frozenset[ToolType] = frozenset({ToolType.ADMIN})
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
    tool_result: "ToolResult | None" = None
    stream_id: str | None = None
    _activity_count: int = field(default=0, init=False)
    _used_unstepped_activity: bool = field(default=False, init=False)

    async def activity(
        self,
        fn: ActivityFn,
        *,
        step: str | None = None,
        args: dict[str, Any] | None = None,
        start_to_close_timeout: timedelta = timedelta(minutes=5),
    ) -> Any:
        function_ref = _function_ref(fn)
        summary = self.guard_name if step is None else f"{self.guard_name}:{step}"

        self._record_activity_call(step)

        return await workflow.execute_activity(
            RUN_GUARD_ACTIVITY_NAME,
            GuardActivityRequest(
                function_ref=function_ref,
                args=args or {},
                guard_name=self.guard_name,
                step=step,
                stream_id=self.stream_id,
            ),
            start_to_close_timeout=start_to_close_timeout,
            summary=summary,
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
class ToolContext:
    tool_name: str
    _tools: "ToolSet"
    stream_id: str | None = None
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
        start_to_close_timeout: timedelta = timedelta(minutes=5),
    ) -> Any:
        function_ref = _function_ref(fn)
        summary = self.tool_name if step is None else f"{self.tool_name}:{step}"

        self._record_activity_call(step)

        return await workflow.execute_activity(
            RUN_TOOL_ACTIVITY_NAME,
            ToolActivityRequest(
                function_ref=function_ref,
                args=args or {},
                tool_name=self.tool_name,
                step=step,
                stream_id=self.stream_id,
            ),
            start_to_close_timeout=start_to_close_timeout,
            summary=summary,
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
class ToolDef:
    schema: ToolParam
    tool_type: ToolType
    fn: ToolFn
    pre_guards: list[GuardDef]
    post_guards: list[GuardDef]


class ToolSet:
    def __init__(self, *, guard_policy: GuardPolicy | None = None) -> None:
        self._tool_registry: dict[str, ToolDef] = {}
        self._guard_registry: dict[str, GuardDef] = {}
        self._guard_functions: dict[GuardFn, GuardDef] = {}
        self._guard_policy = guard_policy or GuardPolicy()

    def tool_names(self) -> list[str]:
        return list(self._tool_registry)

    def tool_schemas(self, names: Iterable[str] | None = None) -> list[ToolParam]:
        if names is None:
            return [t.schema for t in self._tool_registry.values()]
        return [self.get_tool(name).schema for name in names]

    def get_tool(self, name: str) -> ToolDef:
        return self._tool_registry[name]

    async def execute_tool(
        self,
        name: str,
        args: dict[str, Any] | None = None,
        *,
        stream_id: str | None = None,
    ) -> ToolResult:
        tool = self.get_tool(name)
        self._validate_guard_requirements(tool)
        tool_args = args or {}

        pre_guard_result = await self._execute_guards(
            tool.pre_guards,
            GuardTiming.PRE,
            tool,
            name,
            tool_args,
            None,
            stream_id,
        )
        if pre_guard_result is not None:
            return pre_guard_result

        ctx = ToolContext(tool_name=name, _tools=self, stream_id=stream_id)
        tool_result = await _call_tool(tool.fn, ctx, tool_args)

        post_guard_result = await self._execute_guards(
            tool.post_guards,
            GuardTiming.POST,
            tool,
            name,
            tool_args,
            tool_result,
            stream_id,
        )
        if post_guard_result is not None:
            return post_guard_result

        return tool_result

    def tool(
        self,
        *,
        name: str,
        description: str,
        tool_type: ToolType,
        pre_guards: list[GuardFn] | None = None,
        post_guards: list[GuardFn] | None = None,
    ):
        def decorator(
            fn: Callable[..., Awaitable[ToolResult]],
        ) -> Callable[..., Awaitable[ToolResult]]:
            if name in self._tool_registry:
                raise ValueError(f"Duplicate tool name: {name}")

            self._tool_registry[name] = ToolDef(
                schema=ToolParam(
                    name=name,
                    description=description,
                    input_schema=_input_schema_for_tool(fn),
                ),
                tool_type=tool_type,
                fn=fn,
                pre_guards=self._guard_defs_for(pre_guards or []),
                post_guards=self._guard_defs_for(post_guards or []),
            )
            return fn

        return decorator

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
                fulfills=_tool_type_set(fulfills),
                fn=fn,
            )
            self._guard_registry[name] = guard
            self._guard_functions[fn] = guard
            return fn

        return decorator

    def _guard_defs_for(self, guards: Iterable[GuardFn]) -> list[GuardDef]:
        return [self._guard_def_for(guard) for guard in guards]

    def _guard_def_for(self, guard: GuardFn) -> GuardDef:
        try:
            return self._guard_functions[guard]
        except KeyError as err:
            raise ValueError(
                f"Guard {guard.__name__} is not registered; decorate it with "
                "ToolSet.guard before using it in a tool"
            ) from err

    def _validate_guard_requirements(self, tool: ToolDef) -> None:
        if tool.tool_type in self._guard_policy.required_pre and not tool.pre_guards:
            raise ValueError(
                f"Tool type {tool.tool_type} requires at least one pre guard"
            )
        if tool.tool_type in self._guard_policy.required_post and not tool.post_guards:
            raise ValueError(
                f"Tool type {tool.tool_type} requires at least one post guard"
            )

        for guard in tool.pre_guards:
            if tool.tool_type not in guard.fulfills:
                raise ValueError(
                    f"Pre guard {guard.name} does not fulfill {tool.tool_type}"
                )
        for guard in tool.post_guards:
            if tool.tool_type not in guard.fulfills:
                raise ValueError(
                    f"Post guard {guard.name} does not fulfill {tool.tool_type}"
                )

    async def _execute_guards(
        self,
        guards: list[GuardDef],
        timing: GuardTiming,
        tool: ToolDef,
        tool_name: str,
        tool_args: dict[str, Any],
        tool_result: ToolResult | None,
        stream_id: str | None,
    ) -> ToolResult | None:
        for guard in guards:
            ctx = GuardContext(
                guard_name=guard.name,
                tool_name=tool_name,
                tool_type=tool.tool_type,
                tool_args=tool_args,
                tool_result=tool_result,
                stream_id=stream_id,
            )
            result = await _call_guard(guard.fn, ctx)
            if not result.passed:
                return _guard_failure_tool_result(guard, timing, result)

        return None


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
    fn = _resolve_function_ref(request.function_ref)
    stream = StreamContext(
        stream_id=request.stream_id,
        tool_name=request.tool_name,
        step=request.step,
    )
    return await _call_activity(fn, request.args, stream)


@temporal_activity.defn(name=RUN_GUARD_ACTIVITY_NAME)
async def run_guard_activity(request: GuardActivityRequest) -> Any:
    fn = _resolve_function_ref(request.function_ref)
    stream = StreamContext(
        stream_id=request.stream_id,
        tool_name=request.guard_name,
        step=request.step,
    )
    return await _call_activity(fn, request.args, stream)


async def _call_tool(
    fn: ToolFn, ctx: ToolContext, args: dict[str, Any]
) -> ToolResult:
    kwargs = _kwargs_for_tool(fn, ctx, args)
    result = fn(**kwargs)
    if inspect.isawaitable(result):
        return await result
    return result


async def _call_guard(fn: GuardFn, ctx: GuardContext) -> GuardResult:
    kwargs = _kwargs_for_guard(fn, ctx)
    result = fn(**kwargs)
    if inspect.isawaitable(result):
        result = await result

    if not isinstance(result, GuardResult):
        raise TypeError(f"Guard {fn.__name__} must return GuardResult")

    return result


async def _call_activity(
    fn: ActivityFn, args: dict[str, Any], stream: StreamContext
) -> Any:
    result = fn(**_kwargs_for_activity(fn, stream, args))
    if inspect.isawaitable(result):
        return await result
    return result


def _function_ref(fn: ActivityFn) -> str:
    module_name = getattr(fn, "__module__", None)
    qualname = getattr(fn, "__qualname__", None)
    if not module_name or not qualname or "<locals>" in qualname:
        raise ValueError(
            f"Activity function {fn} must be an importable module-level function"
        )

    return f"{module_name}:{qualname}"


def _resolve_function_ref(function_ref: str) -> ActivityFn:
    module_name, separator, qualname = function_ref.partition(":")
    if not separator or not module_name or not qualname:
        raise ApplicationError(
            f"Invalid tool activity function reference: {function_ref}",
            type="InvalidToolActivityFunctionRef",
            non_retryable=True,
        )

    try:
        obj: Any = importlib.import_module(module_name)
        for attr in qualname.split("."):
            obj = getattr(obj, attr)
    except (ImportError, AttributeError) as err:
        raise ApplicationError(
            f"Unable to resolve tool activity function: {function_ref}",
            type="UnknownToolActivityFunction",
            non_retryable=True,
        ) from err

    if not callable(obj):
        raise ApplicationError(
            f"Tool activity function reference is not callable: {function_ref}",
            type="InvalidToolActivityFunctionRef",
            non_retryable=True,
        )

    return cast(ActivityFn, obj)


def _tool_type_set(fulfills: ToolType | Iterable[ToolType]) -> frozenset[ToolType]:
    if isinstance(fulfills, ToolType):
        return frozenset({fulfills})

    tool_types = frozenset(fulfills)
    if not tool_types:
        raise ValueError("Guard must fulfill at least one ToolType")
    invalid_tool_types = [t for t in tool_types if not isinstance(t, ToolType)]
    if invalid_tool_types:
        raise TypeError("Guard fulfills must contain only ToolType values")
    return tool_types


def _guard_failure_tool_result(
    guard: GuardDef, timing: GuardTiming, result: GuardResult
) -> ToolResult:
    return ToolResult(
        payload=result.llm_payload
        or {
            "error": "Guard failed",
            "guard": guard.name,
            "timing": timing.value,
            "reason": result.reason or "Guard did not pass",
        },
        error=True,
    )


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


def _kwargs_for_activity(
    fn: ActivityFn, stream: StreamContext, args: dict[str, Any]
) -> dict[str, Any]:
    signature = inspect.signature(fn)
    type_hints = get_type_hints(fn)
    kwargs: dict[str, Any] = {}
    consumed_args: set[str] = set()

    for name, parameter in signature.parameters.items():
        if name in ("self", "cls"):
            continue

        annotation = type_hints.get(name, parameter.annotation)
        if annotation is StreamContext:
            kwargs[name] = stream
            continue

        if name in args:
            kwargs[name] = args[name]
            consumed_args.add(name)
        elif parameter.default is inspect.Parameter.empty:
            raise TypeError(f"Missing required activity argument {fn.__name__}.{name}")

    unexpected_args = set(args) - consumed_args
    if unexpected_args:
        names = ", ".join(sorted(unexpected_args))
        raise TypeError(
            f"Unexpected activity argument(s) for {fn.__name__}: {names}"
        )

    return kwargs
