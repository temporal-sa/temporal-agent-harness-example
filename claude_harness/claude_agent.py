from __future__ import annotations

import asyncio
import json
from collections.abc import Iterable
from contextlib import suppress
from dataclasses import dataclass, field, replace
from datetime import timedelta
from typing import Any, Literal, Mapping, cast

from anthropic import AsyncAnthropic
from anthropic.types import MessageParam, ToolResultBlockParam
from temporalio import activity, workflow
from temporalio.exceptions import is_cancelled_exception

from .activity_options import DEFAULT_ACTIVITY_OPTIONS, ActivityOptions
from .context_manager import (
    ContextManagerFactory,
    ContextSnapshot,
    ContextTokenBudget,
    DEFAULT_CHARS_PER_TOKEN,
    DEFAULT_CONTEXT_SAFETY_MARGIN_TOKENS,
    DEFAULT_MAX_CONTEXT_TOKENS,
    SlidingWindowContextManager,
    estimate_token_count,
)
from .llm_guards import (
    LlmGuardAction,
    LlmGuardExecution,
    LlmGuardFn,
    LlmGuardPipeline,
)
from .streaming import StreamContext
from .tools import ToolResult, ToolSet

ClaudeStopReason = Literal[
    "end_turn", "max_tokens", "stop_sequence", "tool_use", "pause_turn", "refusal"
]
SteeringMode = Literal["immediate", "after_next_tool_result"]
InterruptPartialResponsePolicy = Literal["discard"]
ClaudeThinkingDisplay = Literal["summarized", "omitted"]
ClaudeThinkingMode = Literal["enabled", "adaptive"]
ClaudeThinkingEffort = Literal["low", "medium", "high", "xhigh", "max"]

DEFAULT_CLAUDE_ACTIVITY_OPTIONS = ActivityOptions(
    start_to_close_timeout=timedelta(minutes=10),
    heartbeat_timeout=timedelta(seconds=10),
)
CLAUDE_HEARTBEAT_INTERVAL_SECONDS = 5
FINE_GRAINED_TOOL_STREAMING_BETA = "fine-grained-tool-streaming-2025-05-14"
DEFAULT_THINKING_BUDGET_TOKENS = 4_096
MIN_THINKING_BUDGET_TOKENS = 1_024


@dataclass(frozen=True)
class ContinueAsNewPolicy:
    enabled: bool = True


@dataclass(frozen=True)
class ClaudeThinkingConfig:
    enabled: bool = False
    mode: ClaudeThinkingMode = "enabled"
    budget_tokens: int = DEFAULT_THINKING_BUDGET_TOKENS
    effort: ClaudeThinkingEffort | None = None
    display: ClaudeThinkingDisplay | None = None


class ClaudeAgent:
    def __init__(
        self,
        system_prompt: str,
        tools: ToolSet,
        *,
        model: str,
        max_tokens: int = 4096,
        thinking: ClaudeThinkingConfig | None = None,
        tool_names: list[str] | None = None,
        stream_id: str | None = None,
        activity_options: ActivityOptions | None = None,
        claude_activity_options: ActivityOptions | None = None,
        llm_guard_activity_options: ActivityOptions | None = None,
        pre_llm_guards: Iterable[LlmGuardFn] | None = None,
        post_llm_guards: Iterable[LlmGuardFn] | None = None,
        context_manager_factory: ContextManagerFactory | None = None,
        max_context_tokens: int = DEFAULT_MAX_CONTEXT_TOKENS,
        context_safety_margin_tokens: int = DEFAULT_CONTEXT_SAFETY_MARGIN_TOKENS,
        context_chars_per_token: float = DEFAULT_CHARS_PER_TOKEN,
        continue_as_new_policy: ContinueAsNewPolicy | None = None,
    ):
        self._system_prompt = system_prompt
        self._tools = tools
        self._model = model
        self._max_tokens = max_tokens
        self._thinking = thinking
        self._tool_names = tool_names
        self._stream_id = stream_id
        self._activity_options = activity_options
        self._claude_activity_options = (
            claude_activity_options or DEFAULT_CLAUDE_ACTIVITY_OPTIONS
        )
        self._llm_guard_activity_options = (
            llm_guard_activity_options or activity_options or DEFAULT_ACTIVITY_OPTIONS
        )
        self._llm_guards = LlmGuardPipeline(
            pre_guards=pre_llm_guards,
            post_guards=post_llm_guards,
        )
        self._max_context_tokens = max_context_tokens
        self._context_safety_margin_tokens = context_safety_margin_tokens
        self._context_chars_per_token = context_chars_per_token
        self._context_manager_factory: ContextManagerFactory = (
            context_manager_factory or SlidingWindowContextManager
        )
        self._continue_as_new_policy = (
            continue_as_new_policy or ContinueAsNewPolicy()
        )
        self._context = self._context_manager_factory()
        self._context_initialized = False
        self._pending_immediate_steering: list[str] = []
        self._pending_after_tool_steering: list[str] = []
        self._pending_interrupts: list[str] = []
        self._interrupt_requested = False
        self._claude_call_sequence = 0
        self._turn_user_prompt_index: int | None = None
        self._terminated = False
        self._termination_reason: str | None = None
        self._llm_guard_state: dict[str, Any] = {}

    def steer(
        self,
        message: str,
        *,
        mode: SteeringMode = "immediate",
    ) -> None:
        if mode == "immediate":
            self._pending_immediate_steering.append(message)
            return
        if mode == "after_next_tool_result":
            self._pending_after_tool_steering.append(message)
            return

        raise ValueError(f"Unknown steering mode: {mode}")

    def interrupt(
        self,
        message: str,
        *,
        partial_response_policy: InterruptPartialResponsePolicy = "discard",
    ) -> None:
        if partial_response_policy != "discard":
            raise ValueError(
                "Only partial_response_policy='discard' is currently supported"
            )

        self._pending_interrupts.append(message)
        self._interrupt_requested = True

    async def run(
        self,
        user_prompt: str | None = None,
        *,
        state: ClaudeAgentState | None = None,
        max_turns: int = 20,
    ) -> ClaudeAgentResult:
        if self._terminated:
            return self._terminated_result(turns=state.turns if state else 0)

        if state is None:
            if user_prompt is None:
                raise ValueError("user_prompt is required when state is not provided")
            if self._context_initialized:
                await self._context.record_user_message(user_prompt)
            else:
                await self._context.initialize(user_prompt)
                self._context_initialized = True
            self._turn_user_prompt_index = self._context.message_count() - 1
            completed_turns = 0
        else:
            self._context.restore(state.context_snapshot)
            self._context_initialized = True
            self._llm_guard_state = dict(state.llm_guard_state)
            self._turn_user_prompt_index = None
            completed_turns = state.turns

        tool_schemas = self._tools.tool_schemas(self._tool_names)
        turn = completed_turns

        while turn < max_turns:
            if self._interrupt_requested:
                await self._flush_interrupt_context()
            await self._flush_immediate_context()
            response = await self._call_claude(tool_schemas)
            if response is None:
                await self._flush_interrupt_context()
                continue

            turn += 1

            if self._interrupt_requested:
                await self._flush_interrupt_context()
                continue

            response_message = cast(MessageParam, response.message)
            tool_use_blocks = _tool_use_blocks(response_message)
            await self._context.record_assistant_message(response_message)

            if response.guard_action is not None:
                if response.guard_action == LlmGuardAction.TERMINATE.value:
                    self._terminated = True
                    self._termination_reason = response.guard_reason
                return ClaudeAgentResult(
                    message=response.message,
                    stop_reason=response.stop_reason,
                    turns=turn,
                    guard_action=response.guard_action,
                    guard_reason=response.guard_reason,
                )

            if not tool_use_blocks:
                if self._interrupt_requested:
                    await self._flush_interrupt_context()
                    continue

                return ClaudeAgentResult(
                    message=response.message,
                    stop_reason=response.stop_reason,
                    turns=turn,
                    guard_action=response.guard_action,
                    guard_reason=response.guard_reason,
                )

            tool_results = await self._execute_requested_tools(tool_use_blocks)
            await self._context.record_tool_results(tool_results)
            await self._flush_after_tool_context()

            if self._interrupt_requested:
                await self._flush_interrupt_context()
                continue

            if self._should_return_continue_as_new():
                return ClaudeAgentResult(
                    message=response.message,
                    stop_reason=response.stop_reason,
                    turns=turn,
                    continuation_state=ClaudeAgentState(
                        context_snapshot=self._context.snapshot(),
                        turns=turn,
                        llm_guard_state=dict(self._llm_guard_state),
                    ),
                    guard_action=response.guard_action,
                    guard_reason=response.guard_reason,
                )

        return ClaudeAgentResult(
            message={
                "role": "assistant",
                "content": f"Stopped after reaching max_turns={max_turns}.",
            },
            stop_reason="max_tokens",
            turns=max_turns,
        )

    async def effective_user_prompt(self) -> str | None:
        """The post-pre-guard text of this turn's initiating user message.

        Read back from the persisted (censored) history so the UI can snap the
        user's bubble to what actually entered the conversation. None on a
        resumed turn (no new user prompt) or if the index is out of range.
        """
        index = self._turn_user_prompt_index
        if index is None:
            return None
        messages = await self._context.full_messages()
        if not 0 <= index < len(messages):
            return None
        return _message_text(messages[index])

    async def _call_claude(
        self, tool_schemas: list[dict[str, Any]]
    ) -> ClaudeResponse | None:
        self._claude_call_sequence += 1
        tool_params = [_tool_param_to_dict(tool) for tool in tool_schemas]
        context_budget = ContextTokenBudget(
            max_context_tokens=self._max_context_tokens,
            reserved_output_tokens=self._max_tokens,
            reserved_input_tokens=estimate_token_count(
                {
                    "system": self._system_prompt,
                    "tools": tool_params,
                },
                chars_per_token=self._context_chars_per_token,
            ),
            safety_margin_tokens=self._context_safety_margin_tokens,
            chars_per_token=self._context_chars_per_token,
        )
        # Guards see the FULL durable history (un-windowed) so they can inspect
        # and mutate the whole conversation.
        guard_request = ClaudeRequest(
            system_prompt=self._system_prompt,
            model=self._model,
            max_tokens=self._max_tokens,
            **_thinking_request_params(self._thinking, max_tokens=self._max_tokens),
            tools=tool_params,
            chat_history=await self._context.full_messages(),
            stream_id=self._stream_id,
            stream_sequence=self._claude_call_sequence,
        )

        pre_guard_execution = await self._llm_guards.execute_pre(
            request=_claude_request_to_dict(guard_request),
            state=self._llm_guard_state,
            stream_id=self._stream_id,
            activity_options=self._llm_guard_activity_options,
        )
        guarded = _claude_request_from_dict(pre_guard_execution.request)
        if pre_guard_execution.halted:
            self._llm_guard_state = pre_guard_execution.state
            return _claude_response_from_guard_execution(
                pre_guard_execution,
                model=guarded.model,
            )

        # Persist the (possibly censored) full history: pre-guard mutations are
        # durable. Windowing/clearing below is transport-only and never stored.
        await self._context.replace_messages(guarded.chat_history)

        request = replace(
            guarded,
            chat_history=[
                _message_param_to_dict(message)
                for message in await self._context.messages_for_model(
                    context_budget
                )
            ],
        )

        claude_handle = workflow.start_activity(
            call_claude,
            request,
            summary="claude",
            **self._claude_activity_options.to_execute_activity_kwargs(),
        )

        try:
            await workflow.wait_condition(
                lambda: self._interrupt_requested or claude_handle.done()
            )
            if self._interrupt_requested:
                await self._discard_interrupted_claude_call(claude_handle)
                return None

            response = await claude_handle
            post_guard_execution = await self._llm_guards.execute_post(
                request=_claude_request_to_dict(
                    replace(request, chat_history=await self._context.full_messages())
                ),
                response=_claude_response_to_dict(response),
                state=pre_guard_execution.state,
                stream_id=self._stream_id,
                activity_options=self._llm_guard_activity_options,
            )
            if post_guard_execution.halted:
                self._llm_guard_state = post_guard_execution.state
                return _claude_response_from_guard_execution(
                    post_guard_execution,
                    model=response.model,
                )

            self._llm_guard_state = post_guard_execution.state
            return _claude_response_from_dict(
                post_guard_execution.response
                or _claude_response_to_dict(response)
            )
        except BaseException as err:
            if self._interrupt_requested and is_cancelled_exception(err):
                return None
            raise

    async def _discard_interrupted_claude_call(
        self, claude_handle: workflow.ActivityHandle[ClaudeResponse]
    ) -> None:
        if not claude_handle.done():
            claude_handle.cancel()

        try:
            await claude_handle
        except BaseException as err:
            if is_cancelled_exception(err):
                return
            raise

    async def _flush_immediate_context(self) -> None:
        await self._flush_steering_messages(
            self._pending_immediate_steering,
            tag="steering",
            description=(
                "This is out-of-band user steering. Use it to adjust how you "
                "proceed, but do not treat it as a new task."
            ),
        )

    async def _flush_after_tool_context(self) -> None:
        await self._flush_steering_messages(
            self._pending_after_tool_steering,
            tag="steering",
            description=(
                "This is out-of-band user steering supplied after a tool result. "
                "Use it to adjust the next reasoning step."
            ),
        )

    async def _flush_interrupt_context(self) -> None:
        await self._flush_steering_messages(
            self._pending_interrupts,
            tag="interrupt",
            description=(
                "The in-progress assistant response was interrupted by the user. "
                "Discard any uncommitted partial assistant output and use this "
                "new context before continuing."
            ),
        )
        self._interrupt_requested = False

    async def _flush_steering_messages(
        self,
        messages: list[str],
        *,
        tag: str,
        description: str,
    ) -> None:
        while messages:
            await self._context.record_user_message(
                _formatted_control_message(
                    tag=tag,
                    description=description,
                    message=messages.pop(0),
                )
            )

    def _should_return_continue_as_new(self) -> bool:
        return (
            self._continue_as_new_policy.enabled
            and workflow.info().is_continue_as_new_suggested()
        )

    async def _execute_tool(self, tool_name: str, **kwargs: Any) -> ToolResult:
        if self._tool_names is not None and tool_name not in self._tool_names:
            return ToolResult(
                payload={"error": f"Tool is not available to this agent: {tool_name}"},
                error=True,
            )
        return await self._tools.execute_tool(
            tool_name,
            kwargs,
            stream_id=self._stream_id,
            activity_options=self._activity_options,
        )

    async def _execute_requested_tools(
        self, tool_use_blocks: list[dict[str, Any]]
    ) -> list[ToolResultBlockParam]:
        return await asyncio.gather(
            *[
                self._execute_requested_tool(block)
                for block in tool_use_blocks
            ]
        )

    async def _execute_requested_tool(
        self, block: dict[str, Any]
    ) -> ToolResultBlockParam:
        tool_name = cast(str, block["name"])
        tool_input = cast(dict[str, Any], block["input"])
        tool_use_id = cast(str, block["id"])

        try:
            result = await self._execute_tool(tool_name, **tool_input)
        except Exception as err:
            result = ToolResult(
                payload={"error": str(err), "type": type(err).__name__},
                error=True,
            )

        return ToolResultBlockParam(
            type="tool_result",
            tool_use_id=tool_use_id,
            content=json.dumps(result.payload),
            is_error=result.error,
        )

    def _terminated_result(self, *, turns: int) -> ClaudeAgentResult:
        return ClaudeAgentResult(
            message={
                "role": "assistant",
                "content": "The agent was terminated.",
            },
            stop_reason="refusal",
            turns=turns,
            guard_action=LlmGuardAction.TERMINATE.value,
            guard_reason=self._termination_reason,
        )


@dataclass
class ClaudeAgentState:
    context_snapshot: ContextSnapshot
    turns: int
    llm_guard_state: dict[str, Any] = field(default_factory=dict)


@dataclass
class ClaudeAgentResult:
    message: dict[str, Any]
    stop_reason: ClaudeStopReason | None
    turns: int
    continuation_state: ClaudeAgentState | None = None
    guard_action: str | None = None
    guard_reason: str | None = None
    effective_user_prompt: str | None = None

    @property
    def needs_continue_as_new(self) -> bool:
        return self.continuation_state is not None

    @property
    def terminated(self) -> bool:
        return self.guard_action == LlmGuardAction.TERMINATE.value


@dataclass
class ClaudeRequest:
    system_prompt: str
    model: str
    max_tokens: int
    tools: list[dict[str, Any]]
    chat_history: list[dict[str, Any]]
    thinking: dict[str, Any] | None = None
    output_config: dict[str, Any] | None = None
    stream_id: str | None = None
    stream_sequence: int | None = None


@dataclass
class ClaudeResponse:
    id: str
    model: str
    message: dict[str, Any]
    stop_reason: ClaudeStopReason | None
    stop_sequence: str | None
    usage: dict[str, Any]
    guard_action: str | None = None
    guard_reason: str | None = None


@dataclass
class _ToolInputStreamState:
    content_block_index: int
    tool_use_id: str | None
    tool_name: str | None
    tool_type: str | None
    partial_json: str = ""


def _claude_request_to_dict(request: ClaudeRequest) -> dict[str, Any]:
    return {
        "system_prompt": request.system_prompt,
        "model": request.model,
        "max_tokens": request.max_tokens,
        "thinking": _copy_optional_mapping(request.thinking),
        "tools": [_copy_mapping(tool) for tool in request.tools],
        "chat_history": [_copy_mapping(message) for message in request.chat_history],
        "stream_id": request.stream_id,
        "stream_sequence": request.stream_sequence,
        "output_config": _copy_optional_mapping(request.output_config),
    }


def _claude_request_from_dict(request: dict[str, Any]) -> ClaudeRequest:
    return ClaudeRequest(
        system_prompt=cast(str, request["system_prompt"]),
        model=cast(str, request["model"]),
        max_tokens=cast(int, request["max_tokens"]),
        thinking=_copy_optional_mapping(request.get("thinking")),
        output_config=_copy_optional_mapping(request.get("output_config")),
        tools=_mapping_list(request.get("tools", [])),
        chat_history=_mapping_list(request.get("chat_history", [])),
        stream_id=cast(str | None, request.get("stream_id")),
        stream_sequence=cast(int | None, request.get("stream_sequence")),
    )


def _claude_response_to_dict(response: ClaudeResponse) -> dict[str, Any]:
    return {
        "id": response.id,
        "model": response.model,
        "message": _copy_mapping(response.message),
        "stop_reason": response.stop_reason,
        "stop_sequence": response.stop_sequence,
        "usage": _copy_mapping(response.usage),
        "guard_action": response.guard_action,
        "guard_reason": response.guard_reason,
    }


def _claude_response_from_dict(response: dict[str, Any]) -> ClaudeResponse:
    return ClaudeResponse(
        id=cast(str, response["id"]),
        model=cast(str, response["model"]),
        message=_copy_mapping(response["message"]),
        stop_reason=cast(ClaudeStopReason | None, response.get("stop_reason")),
        stop_sequence=cast(str | None, response.get("stop_sequence")),
        usage=_copy_mapping(response.get("usage", {})),
        guard_action=cast(str | None, response.get("guard_action")),
        guard_reason=cast(str | None, response.get("guard_reason")),
    )


def _claude_response_from_guard_execution(
    execution: LlmGuardExecution,
    *,
    model: str,
) -> ClaudeResponse:
    response = execution.response or {
        "id": "guard:llm",
        "model": model,
        "message": {
            "role": "assistant",
            "content": "The response was blocked by an LLM guard.",
        },
        "stop_reason": "refusal",
        "stop_sequence": None,
        "usage": {},
    }
    response["guard_action"] = execution.action.value
    response["guard_reason"] = execution.reason
    return _claude_response_from_dict(response)


@activity.defn
async def call_claude(request: ClaudeRequest) -> ClaudeResponse:
    create_params: dict[str, Any] = {
        "model": request.model,
        "max_tokens": request.max_tokens,
        "system": request.system_prompt,
        "messages": request.chat_history,
    }
    if request.tools:
        create_params["tools"] = request.tools
    if request.thinking is not None:
        create_params["thinking"] = request.thinking
    if request.output_config is not None:
        create_params["output_config"] = request.output_config

    async with AsyncAnthropic(max_retries=0) as client:
        response = await _stream_claude_message(
            client,
            create_params,
            stream_id=request.stream_id,
            stream_sequence=request.stream_sequence,
        )

    return ClaudeResponse(
        id=response.id,
        model=response.model,
        message={
            "role": response.role,
            "content": [block.to_dict() for block in response.content],
        },
        stop_reason=response.stop_reason,
        stop_sequence=response.stop_sequence,
        usage=response.usage.to_dict(),
    )


async def _stream_claude_message(
    client: AsyncAnthropic,
    create_params: dict[str, Any],
    *,
    stream_id: str | None,
    stream_sequence: int | None,
) -> Any:
    stream = StreamContext(stream_id=stream_id, tool_name="claude")
    heartbeat_state = _ClaudeHeartbeatState(sequence=stream_sequence)
    activity.heartbeat(heartbeat_state.payload("starting"))
    heartbeat_task = asyncio.create_task(_heartbeat_claude_stream(heartbeat_state))
    await stream.emit({"sequence": stream_sequence}, kind="claude_start")

    try:
        async with client.messages.stream(
            **create_params,
            extra_headers=_streaming_extra_headers(create_params),
        ) as message_stream:
            heartbeat_state.phase = "streaming"
            cancel_task = asyncio.create_task(activity.wait_for_cancelled())
            event_iterator = message_stream.__aiter__()
            tool_input_blocks: dict[int, _ToolInputStreamState] = {}
            try:
                while True:
                    next_event_task = asyncio.create_task(anext(event_iterator))
                    done, _pending = await asyncio.wait(
                        {next_event_task, cancel_task},
                        return_when=asyncio.FIRST_COMPLETED,
                    )

                    if cancel_task in done:
                        heartbeat_state.phase = "cancelled"
                        activity.heartbeat(heartbeat_state.payload("cancelled"))
                        next_event_task.cancel()
                        with suppress(asyncio.CancelledError):
                            await next_event_task
                        await stream.emit(
                            {"sequence": stream_sequence},
                            kind="claude_cancelled",
                        )
                        raise asyncio.CancelledError()

                    try:
                        event = next_event_task.result()
                    except StopAsyncIteration:
                        break

                    heartbeat_state.record_event(event)
                    activity.heartbeat(heartbeat_state.payload("event"))
                    await _emit_claude_raw_stream_event(
                        stream=stream,
                        event=event,
                        stream_sequence=stream_sequence,
                        tool_input_blocks=tool_input_blocks,
                    )
            finally:
                cancel_task.cancel()
                with suppress(asyncio.CancelledError):
                    await cancel_task

            response = await message_stream.get_final_message()
    finally:
        heartbeat_task.cancel()
        with suppress(asyncio.CancelledError):
            await heartbeat_task

    await stream.emit(
        {
            "id": response.id,
            "model": response.model,
            "sequence": stream_sequence,
            "stop_reason": response.stop_reason,
            "text": _text_from_content_blocks(response.content),
            "usage": response.usage.to_dict(),
        },
        kind="claude_complete",
    )
    heartbeat_state.phase = "complete"
    heartbeat_state.stop_reason = cast(str | None, response.stop_reason)
    activity.heartbeat(heartbeat_state.payload("complete"))
    return response


@dataclass
class _ClaudeHeartbeatState:
    sequence: int | None
    phase: str = "starting"
    events: int = 0
    last_event_type: str | None = None
    stop_reason: str | None = None

    def record_event(self, event: Any) -> None:
        self.events += 1
        self.last_event_type = cast(str | None, getattr(event, "type", None))

    def payload(self, heartbeat_reason: str) -> dict[str, Any]:
        return {
            "kind": "claude_stream",
            "heartbeat_reason": heartbeat_reason,
            "sequence": self.sequence,
            "phase": self.phase,
            "events": self.events,
            "last_event_type": self.last_event_type,
            "stop_reason": self.stop_reason,
        }


async def _heartbeat_claude_stream(state: _ClaudeHeartbeatState) -> None:
    while True:
        await asyncio.sleep(CLAUDE_HEARTBEAT_INTERVAL_SECONDS)
        activity.heartbeat(state.payload("timer"))


def _streaming_extra_headers(create_params: dict[str, Any]) -> dict[str, str] | None:
    if not create_params.get("tools"):
        return None
    return {"anthropic-beta": FINE_GRAINED_TOOL_STREAMING_BETA}


async def _emit_claude_raw_stream_event(
    *,
    stream: StreamContext,
    event: Any,
    stream_sequence: int | None,
    tool_input_blocks: dict[int, _ToolInputStreamState],
) -> None:
    event_type = getattr(event, "type", None)

    if event_type == "content_block_start":
        block_index = cast(int, getattr(event, "index"))
        block = _object_to_dict(getattr(event, "content_block", None))
        block_type = block.get("type")
        if block_type == "thinking":
            await stream.emit(
                {
                    "sequence": stream_sequence,
                    "content_block_index": block_index,
                },
                kind="claude_thinking_start",
            )
            return

        if block_type in ("tool_use", "server_tool_use"):
            state = _ToolInputStreamState(
                content_block_index=block_index,
                tool_use_id=cast(str | None, block.get("id")),
                tool_name=cast(str | None, block.get("name")),
                tool_type=cast(str | None, block_type),
            )
            tool_input_blocks[block_index] = state
            await stream.emit(
                {
                    "sequence": stream_sequence,
                    "content_block_index": block_index,
                    "tool_use_id": state.tool_use_id,
                    "tool_name": state.tool_name,
                    "tool_type": state.tool_type,
                },
                kind="claude_tool_input_start",
            )
        return

    if event_type == "content_block_delta":
        delta = _object_to_dict(getattr(event, "delta", None))
        delta_type = delta.get("type")
        if delta_type == "text_delta":
            text = delta.get("text")
            if isinstance(text, str) and text:
                await stream.emit(
                    {"sequence": stream_sequence, "text": text},
                    kind="claude_text_delta",
                )
            return

        if delta_type == "thinking_delta":
            thinking = delta.get("thinking")
            if isinstance(thinking, str) and thinking:
                await stream.emit(
                    {"sequence": stream_sequence, "thinking": thinking},
                    kind="claude_thinking_delta",
                )
            return

        if delta_type == "input_json_delta":
            block_index = cast(int, getattr(event, "index"))
            state = tool_input_blocks.get(block_index)
            partial_json = delta.get("partial_json")
            if state is None or not isinstance(partial_json, str):
                return

            state.partial_json += partial_json
            await stream.emit(
                {
                    "sequence": stream_sequence,
                    "content_block_index": block_index,
                    "tool_use_id": state.tool_use_id,
                    "tool_name": state.tool_name,
                    "tool_type": state.tool_type,
                    "partial_json": partial_json,
                },
                kind="claude_tool_input_delta",
            )
        return

    if event_type == "content_block_stop":
        block_index = cast(int, getattr(event, "index"))
        state = tool_input_blocks.pop(block_index, None)
        if state is None:
            return

        block = _object_to_dict(getattr(event, "content_block", None))
        input_value = block.get("input", state.partial_json)
        await stream.emit(
            {
                "sequence": stream_sequence,
                "content_block_index": block_index,
                "tool_use_id": state.tool_use_id,
                "tool_name": state.tool_name,
                "tool_type": state.tool_type,
                "input": input_value,
                "input_preview": _json_preview(input_value),
            },
            kind="claude_tool_input_complete",
        )


def _tool_use_blocks(message: MessageParam) -> list[dict[str, Any]]:
    content = message["content"]
    if isinstance(content, str):
        return []

    blocks: list[dict[str, Any]] = []
    for block in content:
        block_dict = (
            dict(cast(Mapping[str, Any], block))
            if isinstance(block, dict)
            else block.to_dict()
        )
        if block_dict.get("type") == "tool_use":
            blocks.append(block_dict)
    return blocks


def _message_text(message: dict[str, Any]) -> str:
    content = message.get("content")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "".join(
            block.get("text", "")
            for block in content
            if isinstance(block, dict) and block.get("type") == "text"
        )
    return ""


def _message_param_to_dict(message: MessageParam) -> dict[str, Any]:
    content = message["content"]
    if isinstance(content, str):
        message_content: str | list[Any] = content
    else:
        message_content = [_block_to_dict(block) for block in content]

    return {
        "role": message["role"],
        "content": message_content,
    }


def _mapping_list(value: Any) -> list[dict[str, Any]]:
    return [_copy_mapping(item) for item in cast(list[Any], value)]


def _copy_optional_mapping(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    return _copy_mapping(value)


def _copy_mapping(value: Any) -> dict[str, Any]:
    return dict(cast(Mapping[str, Any], value))


def _tool_param_to_dict(tool: dict[str, Any]) -> dict[str, Any]:
    return dict(cast(Mapping[str, Any], tool))


def _thinking_request_params(
    thinking: ClaudeThinkingConfig | None,
    *,
    max_tokens: int,
) -> dict[str, Any]:
    if thinking is None or not thinking.enabled:
        return {"thinking": None, "output_config": None}
    if thinking.mode == "adaptive":
        param: dict[str, Any] = {"type": "adaptive"}
        if thinking.display is not None:
            param["display"] = thinking.display
        output_config = (
            {"effort": thinking.effort}
            if thinking.effort is not None
            else None
        )
        return {"thinking": param, "output_config": output_config}

    if max_tokens <= MIN_THINKING_BUDGET_TOKENS:
        raise ValueError(
            "max_tokens must be greater than 1024 when extended thinking is enabled"
        )

    budget_tokens = min(
        max(MIN_THINKING_BUDGET_TOKENS, thinking.budget_tokens),
        max_tokens - 1,
    )
    param: dict[str, Any] = {
        "type": "enabled",
        "budget_tokens": budget_tokens,
    }
    if thinking.display is not None:
        param["display"] = thinking.display
    return {"thinking": param, "output_config": None}


def _block_to_dict(block: Any) -> dict[str, Any]:
    return _object_to_dict(block)


def _object_to_dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(cast(Mapping[str, Any], value))
    if hasattr(value, "to_dict"):
        return cast(dict[str, Any], value.to_dict())
    if hasattr(value, "model_dump"):
        return cast(dict[str, Any], value.model_dump(mode="json"))
    return {}


def _text_from_content_blocks(content: Any) -> str:
    text_parts: list[str] = []
    for block in content:
        block_dict = _block_to_dict(block)
        if block_dict.get("type") == "text":
            text = block_dict.get("text")
            if isinstance(text, str):
                text_parts.append(text)
    return "\n".join(text_parts)


def _json_preview(value: Any, *, max_chars: int = 2_000) -> str:
    try:
        encoded = json.dumps(value, sort_keys=True)
    except TypeError:
        encoded = repr(value)
    if len(encoded) <= max_chars:
        return encoded
    return encoded[-max_chars:]


def _formatted_control_message(
    *,
    tag: str,
    description: str,
    message: str,
) -> str:
    return f"<{tag}>\n{description}\n\n{message}\n</{tag}>"
