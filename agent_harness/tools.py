from __future__ import annotations

import hashlib
import inspect
import json
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Awaitable, Callable, Literal, cast, get_type_hints

from pydantic import create_model
from temporalio import activity as temporal_activity
from temporalio.common import Priority, RetryPolicy
from temporalio.workflow import ActivityCancellationType, VersioningIntent

from .activity_options import (
    DEFAULT_TOOL_ACTIVITY_OPTIONS,
    ActivityOptions,
)
from .activity_router import (
    ActivityFn,
    RoutedActivityContext,
    ToolActivityContext,
    call_activity,
    function_ref,
    resolve_function_ref,
)
from .guards import (
    GuardDef,
    GuardFn,
    GuardPolicy,
    GuardSet,
    GuardTiming,
    guard,
    guard_metadata,
)
from .invocation import bind_keyword_arguments, maybe_await
from .streaming import StreamContext, emit_harness_event, stream_error_payload
from .tool_types import ToolCategory, normalize_tool_category
from .workflow_activities import execute_routed_activity, record_routed_activity_call

ToolFn = Callable[..., Awaitable["ToolResult"]]
DynamicToolFn = Callable[["ToolContext", dict[str, Any]], Awaitable["ToolResult"]]
GuardReference = GuardFn | str
ToolParam = dict
ToolArgsMode = Literal["signature", "raw"]
RUN_TOOL_ACTIVITY_NAME = "agent_harness.run_tool_activity"
_TOOL_METADATA_ATTR = "__agent_harness_tool__"
_STREAM_RESULT_PREVIEW_CHARS = 8_000


@dataclass(frozen=True)
class ToolMetadata:
    name: str
    description: str
    tool_type: ToolCategory
    pre_guards: tuple[GuardReference, ...] = ()
    post_guards: tuple[GuardReference, ...] = ()


def tool(
    *,
    name: str,
    description: str,
    tool_type: ToolCategory,
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
                tool_type=normalize_tool_category(tool_type),
                pre_guards=tuple(pre_guards or ()),
                post_guards=tuple(post_guards or ()),
            ),
        )
        return fn

    return decorator


@dataclass
class ToolContext:
    tool_name: str
    _tools: "ToolSet"
    stream_id: str | None = None
    tool_call_id: str | None = None
    stream_agent: dict[str, str | None] | None = None
    activity_options: ActivityOptions = DEFAULT_TOOL_ACTIVITY_OPTIONS
    _activity_count: int = field(default=0, init=False)
    _used_unstepped_activity: bool = field(default=False, init=False)

    def tool_names(self) -> list[str]:
        return self._tools.tool_names()

    def tool_schemas(self, names: Iterable[str] | None = None) -> list[ToolParam]:
        return self._tools.tool_schemas(names)

    def idempotency_key(self, *parts: object) -> str:
        components = [
            self.stream_id or "stream:none",
            self.tool_name,
            self.tool_call_id or "tool-call:none",
        ]
        components.extend(_idempotency_part(part) for part in parts)
        digest = hashlib.sha256("\x1f".join(components).encode("utf-8")).hexdigest()
        return f"{self.tool_name}:{digest[:32]}"

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
                owner=f"Tool {self.tool_name}",
                step=step,
                activity_count=self._activity_count,
                used_unstepped_activity=self._used_unstepped_activity,
            )
        )
        return await execute_routed_activity(
            activity_name=RUN_TOOL_ACTIVITY_NAME,
            request=ToolActivityRequest(
                function_ref=function_ref(fn),
                args=args or {},
                tool_name=self.tool_name,
                step=step,
                stream_id=self.stream_id,
                tool_call_id=self.tool_call_id,
                stream_agent=self.stream_agent,
            ),
            summary_base=self.tool_name,
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
class ToolResult:
    payload: dict
    error: bool


@dataclass
class ToolActivityRequest:
    function_ref: str
    args: dict
    tool_name: str | None = None
    step: str | None = None
    stream_id: str | None = None
    tool_call_id: str | None = None
    stream_agent: dict[str, str | None] | None = None


@dataclass
class ToolDef:
    schema: ToolParam
    tool_type: ToolCategory
    fn: ToolFn | DynamicToolFn
    pre_guards: list[GuardDef]
    post_guards: list[GuardDef]
    args_mode: ToolArgsMode = "signature"


def _stream_tool_result_payload(result: ToolResult) -> dict[str, Any]:
    """Return a bounded result envelope suitable for Redis/SSE fan-out."""

    serialized = json.dumps(
        result.payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    payload: dict[str, Any] = {
        "result_preview": serialized[:_STREAM_RESULT_PREVIEW_CHARS],
        "result_size": len(serialized),
        "result_truncated": len(serialized) > _STREAM_RESULT_PREVIEW_CHARS,
    }
    if not result.error:
        return payload

    error = result.payload.get("error")
    if isinstance(error, dict):
        payload["error"] = dict(error)
    elif error is not None:
        payload["error"] = {
            "type": "ToolResultError",
            "message": str(error),
        }
    else:
        payload["error"] = {
            "type": "ToolResultError",
            "message": str(result.payload.get("message") or "Tool returned an error."),
        }
    return payload


class ToolSet:
    def __init__(
        self,
        *,
        guard_policy: GuardPolicy | None = None,
        tools: Iterable[ToolFn] | None = None,
        guards: Iterable[GuardFn] | None = None,
        providers: Iterable[object] | None = None,
        mcp_providers: Iterable[object] | None = None,
    ) -> None:
        self._tool_registry: dict[str, ToolDef] = {}
        self._guards = GuardSet(guard_policy=guard_policy)
        if guards is not None:
            self.add_guard(*guards)
        for provider in providers or ():
            self.add_provider(provider)
        for provider in mcp_providers or ():
            self.add_mcp_provider(provider)
        if tools is not None:
            self.add_tool(*tools)

    def tool_names(self) -> list[str]:
        return list(self._tool_registry)

    def tool_schemas(self, names: Iterable[str] | None = None) -> list[ToolParam]:
        if names is None:
            return [t.schema for t in self._tool_registry.values()]
        return [self.get_tool(name).schema for name in names]

    def get_tool(self, name: str) -> ToolDef:
        return self._tool_registry[name]

    def add_provider(
        self,
        provider: object,
        *,
        include_tools: Iterable[str] | None = None,
        exclude_tools: Iterable[str] | None = None,
    ) -> object:
        included = set(include_tools) if include_tools is not None else None
        excluded = set(exclude_tools or ())
        guard_defs: dict[str, GuardDef] = {}
        methods = list(_provider_methods(provider))

        for method in methods:
            metadata = guard_metadata(method)
            if metadata is None:
                continue
            guard_defs[metadata.name] = self._register_guard(method)

        for method in methods:
            metadata = _tool_metadata(method)
            if metadata is None:
                continue
            if included is not None and metadata.name not in included:
                continue
            if metadata.name in excluded:
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
                    "with agent_harness.tools.tool before registering it"
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
            metadata = guard_metadata(fn)
            if metadata is None:
                raise ValueError(
                    f"Guard {fn.__name__} is missing @guard metadata; decorate it "
                    "with agent_harness.guards.guard before registering it"
                )
            self._register_guard(fn)

    def add_dynamic_tool(
        self,
        *,
        name: str,
        description: str,
        input_schema: dict[str, Any],
        tool_type: ToolCategory,
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
        tool_call_id: str | None = None,
        stream_agent: dict[str, str | None] | None = None,
        llm_sequence: int | None = None,
        activity_options: ActivityOptions | None = None,
    ) -> ToolResult:
        tool = self.get_tool(name)
        tool_args = args or {}
        resolved_activity_options = activity_options or DEFAULT_TOOL_ACTIVITY_OPTIONS
        agent_id = (
            stream_agent.get("id")
            if stream_agent is not None
            else "main"
        )
        operation_id = tool_call_id or (
            f"{agent_id or 'main'}:tool:{llm_sequence or 'unknown'}:{name}"
        )
        parent_operation_id = (
            f"{agent_id or 'main'}:llm:{llm_sequence or 'unknown'}"
        )
        await emit_harness_event(
            stream_id=stream_id,
            kind="harness_tool_start",
            payload={
                "operation_id": operation_id,
                "parent_operation_id": parent_operation_id,
                "tool_name": name,
                "tool_type": tool.tool_type,
                "llm_sequence": llm_sequence,
                "status": "running",
            },
            agent=stream_agent,
            tool_name=name,
            tool_call_id=tool_call_id,
        )
        try:
            self._guards.validate_tool_guards(
                tool_type=tool.tool_type,
                pre_guards=tool.pre_guards,
                post_guards=tool.post_guards,
            )
        except Exception as err:
            await emit_harness_event(
                stream_id=stream_id,
                kind="harness_tool_failed",
                payload={
                    "operation_id": operation_id,
                    "parent_operation_id": parent_operation_id,
                    "tool_name": name,
                    "tool_type": tool.tool_type,
                    "llm_sequence": llm_sequence,
                    "status": "failed",
                    "error": stream_error_payload(err),
                },
                agent=stream_agent,
                tool_name=name,
                tool_call_id=tool_call_id,
            )
            raise

        pre_guard_failure = await self._guards.execute_guards(
            tool.pre_guards,
            GuardTiming.PRE,
            tool_name=name,
            tool_type=tool.tool_type,
            tool_args=tool_args,
            tool_result=None,
            stream_id=stream_id,
            stream_agent=stream_agent,
            tool_call_id=tool_call_id,
            llm_sequence=llm_sequence,
            parent_operation_id=operation_id,
            activity_options=resolved_activity_options,
        )
        if pre_guard_failure is not None:
            blocked_result = ToolResult(
                payload=pre_guard_failure.payload,
                error=True,
            )
            await emit_harness_event(
                stream_id=stream_id,
                kind="harness_tool_complete",
                payload={
                    "operation_id": operation_id,
                    "parent_operation_id": parent_operation_id,
                    "tool_name": name,
                    "tool_type": tool.tool_type,
                    "llm_sequence": llm_sequence,
                    "status": "blocked",
                    **_stream_tool_result_payload(blocked_result),
                },
                agent=stream_agent,
                tool_name=name,
                tool_call_id=tool_call_id,
            )
            return blocked_result

        ctx = ToolContext(
            tool_name=name,
            _tools=self,
            stream_id=stream_id,
            tool_call_id=tool_call_id,
            stream_agent=stream_agent,
            activity_options=resolved_activity_options,
        )
        try:
            if tool.args_mode == "raw":
                tool_result = await _call_dynamic_tool(
                    cast(DynamicToolFn, tool.fn),
                    ctx,
                    tool_args,
                )
            else:
                tool_result = await _call_tool(
                    cast(ToolFn, tool.fn),
                    ctx,
                    tool_args,
                )
        except Exception as err:
            await emit_harness_event(
                stream_id=stream_id,
                kind="harness_tool_failed",
                payload={
                    "operation_id": operation_id,
                    "parent_operation_id": parent_operation_id,
                    "tool_name": name,
                    "tool_type": tool.tool_type,
                    "llm_sequence": llm_sequence,
                    "status": "failed",
                    "error": stream_error_payload(err),
                },
                agent=stream_agent,
                tool_name=name,
                tool_call_id=tool_call_id,
            )
            raise

        post_guard_failure = await self._guards.execute_guards(
            tool.post_guards,
            GuardTiming.POST,
            tool_name=name,
            tool_type=tool.tool_type,
            tool_args=tool_args,
            tool_result=tool_result,
            stream_id=stream_id,
            stream_agent=stream_agent,
            tool_call_id=tool_call_id,
            llm_sequence=llm_sequence,
            parent_operation_id=operation_id,
            activity_options=resolved_activity_options,
        )
        if post_guard_failure is not None:
            blocked_result = ToolResult(
                payload=post_guard_failure.payload,
                error=True,
            )
            await emit_harness_event(
                stream_id=stream_id,
                kind="harness_tool_complete",
                payload={
                    "operation_id": operation_id,
                    "parent_operation_id": parent_operation_id,
                    "tool_name": name,
                    "tool_type": tool.tool_type,
                    "llm_sequence": llm_sequence,
                    "status": "blocked",
                    **_stream_tool_result_payload(blocked_result),
                },
                agent=stream_agent,
                tool_name=name,
                tool_call_id=tool_call_id,
            )
            return blocked_result

        await emit_harness_event(
            stream_id=stream_id,
            kind="harness_tool_complete",
            payload={
                "operation_id": operation_id,
                "parent_operation_id": parent_operation_id,
                "tool_name": name,
                "tool_type": tool.tool_type,
                "llm_sequence": llm_sequence,
                "status": "failed" if tool_result.error else "complete",
                **_stream_tool_result_payload(tool_result),
            },
            agent=stream_agent,
            tool_name=name,
            tool_call_id=tool_call_id,
        )
        return tool_result

    def _register_tool(
        self,
        *,
        name: str,
        description: str,
        tool_type: ToolCategory,
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
            tool_type=normalize_tool_category(tool_type),
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

        metadata = guard_metadata(fn)
        if metadata is None:
            raise ValueError(
                f"Guard {fn.__name__} is not registered; decorate it with "
                "agent_harness.guards.guard before using it in a tool"
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
            if _tool_metadata(value) is None and guard_metadata(value) is None:
                continue
            seen.add(name)
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


def _idempotency_part(value: object) -> str:
    if isinstance(value, str):
        return value
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
    except TypeError:
        return str(value)


@temporal_activity.defn(name=RUN_TOOL_ACTIVITY_NAME)
async def run_tool_activity(request: ToolActivityRequest) -> Any:
    fn = resolve_function_ref(request.function_ref)
    stream = StreamContext(
        stream_id=request.stream_id,
        tool_name=request.tool_name,
        step=request.step,
        agent=request.stream_agent,
        tool_call_id=request.tool_call_id,
    )
    activity_context = ToolActivityContext(
        route_kind="tool",
        route_name=request.tool_name,
        step=request.step,
        stream_id=request.stream_id,
    )
    activity_attempt = temporal_activity.info().attempt
    parent_operation_id = request.tool_call_id or (
        f"tool:{request.tool_name or 'unknown'}"
    )
    operation_id = (
        f"{parent_operation_id}:activity:{request.step or request.function_ref}"
    )
    await stream.emit(
        {
            "operation_id": operation_id,
            "parent_operation_id": parent_operation_id,
            "activity_name": request.function_ref,
            "tool_name": request.tool_name,
            "activity_attempt": activity_attempt,
            "status": "running" if activity_attempt == 1 else "retrying",
        },
        kind="harness_tool_activity_start",
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
                "tool_name": request.tool_name,
                "activity_attempt": activity_attempt,
                "status": "failed",
                "error": stream_error_payload(err),
            },
            kind="harness_tool_activity_failed",
        )
        raise

    await stream.emit(
        {
            "operation_id": operation_id,
            "parent_operation_id": parent_operation_id,
            "activity_name": request.function_ref,
            "tool_name": request.tool_name,
            "activity_attempt": activity_attempt,
            "status": "complete",
        },
        kind="harness_tool_activity_complete",
    )
    return result


async def _call_tool(
    fn: ToolFn, ctx: ToolContext, args: dict[str, Any]
) -> ToolResult:
    kwargs = _kwargs_for_tool(fn, ctx, args)
    return await maybe_await(fn(**kwargs))


async def _call_dynamic_tool(
    fn: DynamicToolFn, ctx: ToolContext, args: dict[str, Any]
) -> ToolResult:
    return await maybe_await(fn(ctx, args))


def _kwargs_for_tool(
    fn: ToolFn, ctx: ToolContext, args: dict[str, Any]
) -> dict[str, Any]:
    return bind_keyword_arguments(
        fn,
        args,
        special_values={ToolContext: ctx},
        argument_label="tool",
    )
