from __future__ import annotations

import asyncio
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any, Literal, cast

from temporalio import workflow
from temporalio.exceptions import ActivityError, ApplicationError, is_cancelled_exception

from .activity_options import DEFAULT_ACTIVITY_OPTIONS, ActivityOptions
from .attachments import AttachmentRef
from .context_manager import (
    ContextManagerFactory,
    ContextSnapshot,
    ContextTokenBudget,
    DEFAULT_CHARS_PER_TOKEN,
    DEFAULT_CONTEXT_SAFETY_MARGIN_TOKENS,
    DEFAULT_MAX_CONTEXT_TOKENS,
)
from .errors import UserFacingToolError
from .llm_guards import (
    LlmGuardAction,
    LlmGuardFn,
    LlmGuardPipeline,
)
from .messages import (
    json_content,
    text_message,
    tool_result_block,
    tool_use_blocks,
    visible_user_message_text,
)
from .providers.interface import (
    CONTEXT_WINDOW_EXCEEDED_ERROR_TYPE,
    AgentProvider,
    ProviderRequest,
    ProviderResponse,
    start_provider_activity,
)
from .sliding_window_context_manager import SlidingWindowContextManager
from .tools import ToolResult, ToolSet

AgentStopReason = str
SteeringMode = Literal["immediate", "after_next_tool_result"]
InterruptPartialResponsePolicy = Literal["discard"]


@dataclass(frozen=True)
class ContinueAsNewPolicy:
    enabled: bool = True


CONTEXT_OVERFLOW_RECOVERY_ATTEMPTS = 1
CONTEXT_OVERFLOW_RETRY_CHARS_PER_TOKEN = 2.5
CONTEXT_OVERFLOW_RETRY_SAFETY_MARGIN_MULTIPLIER = 4
CONTEXT_OVERFLOW_RETRY_MIN_SAFETY_MARGIN_TOKENS = 16_000


class Agent:
    def __init__(
        self,
        system_prompt: str,
        tools: ToolSet,
        *,
        provider: AgentProvider,
        model: str,
        max_tokens: int = 4096,
        tool_names: list[str] | None = None,
        stream_id: str | None = None,
        activity_options: ActivityOptions | None = None,
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
        self._provider = provider
        self._model = model
        self._max_tokens = max_tokens
        self._tool_names = tool_names
        self._stream_id = stream_id
        self._activity_options = activity_options
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
        self._provider_call_sequence = 0
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

    @property
    def interrupt_requested(self) -> bool:
        return self._interrupt_requested

    async def run(
        self,
        user_prompt: str | None = None,
        *,
        attachments: list[AttachmentRef] | None = None,
        state: AgentState | None = None,
        max_turns: int = 20,
    ) -> AgentResult:
        if self._terminated:
            return self._terminated_result(turns=state.turns if state else 0)

        if state is None:
            if user_prompt is None:
                raise ValueError("user_prompt is required when state is not provided")
            if self._context_initialized:
                await self._context.record_user_message(
                    user_prompt,
                    attachments=attachments,
                )
            else:
                await self._context.initialize(
                    user_prompt,
                    attachments=attachments,
                )
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
            response = await self._call_provider(tool_schemas)
            if response is None:
                await self._flush_interrupt_context()
                continue

            turn += 1

            if self._interrupt_requested:
                await self._flush_interrupt_context()
                continue

            response = self._provider.response_with_visible_refusal(response)
            response_message = self._provider.response_message(response)
            requested_tools = tool_use_blocks(response_message)
            await self._context.record_assistant_message(response_message)

            if response.guard_action is not None:
                if response.guard_action == LlmGuardAction.TERMINATE.value:
                    self._terminated = True
                    self._termination_reason = response.guard_reason
                return AgentResult(
                    message=response_message,
                    stop_reason=response.stop_reason,
                    turns=turn,
                    guard_action=response.guard_action,
                    guard_reason=response.guard_reason,
                    stop_details=response.stop_details,
                )

            if not requested_tools:
                if self._interrupt_requested:
                    await self._flush_interrupt_context()
                    continue

                return AgentResult(
                    message=response_message,
                    stop_reason=response.stop_reason,
                    turns=turn,
                    guard_action=response.guard_action,
                    guard_reason=response.guard_reason,
                    stop_details=response.stop_details,
                )

            tool_execution = await self._execute_requested_tools(requested_tools)
            await self._context.record_tool_results(tool_execution.tool_results)
            await self._flush_after_tool_context()

            if self._interrupt_requested or tool_execution.interrupted:
                await self._flush_interrupt_context()
                continue

            if self._should_return_continue_as_new():
                continuation_budget = self._context_token_budget(tool_schemas)
                return AgentResult(
                    message=response_message,
                    stop_reason=response.stop_reason,
                    turns=turn,
                    continuation_state=AgentState(
                        context_snapshot=await self._context.continuation_context_snapshot(
                            continuation_budget,
                        ),
                        turns=turn,
                        llm_guard_state=dict(self._llm_guard_state),
                    ),
                    guard_action=response.guard_action,
                    guard_reason=response.guard_reason,
                    stop_details=response.stop_details,
                )

        return AgentResult(
            message=text_message(
                "assistant",
                f"Stopped after reaching max_turns={max_turns}.",
            ),
            stop_reason=self._provider.stop_reason_for_max_turns(),
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
        return visible_user_message_text(messages[index])

    def restore_idle_state(self, state: AgentState) -> None:
        self._context.restore(state.context_snapshot)
        self._context_initialized = True
        self._llm_guard_state = dict(state.llm_guard_state)

    def context_snapshot(self) -> ContextSnapshot:
        if not self._context_initialized:
            return {"version": 2, "messages": []}
        return self._context.full_context_snapshot()

    async def compacted_state(self) -> AgentState:
        tool_schemas = self._tools.tool_schemas(self._tool_names)
        return AgentState(
            context_snapshot=await self._context.continuation_context_snapshot(
                self._context_token_budget(tool_schemas),
            ),
            turns=0,
            llm_guard_state=dict(self._llm_guard_state),
        )

    async def _call_provider(
        self, tool_schemas: list[dict[str, Any]]
    ) -> ProviderResponse | None:
        self._provider_call_sequence += 1
        tool_params = [dict(tool) for tool in tool_schemas]
        # Guards see the FULL durable history (un-windowed) so they can inspect
        # and mutate the whole conversation.
        guard_request = self._provider.create_request(
            system_prompt=self._system_prompt,
            model=self._model,
            max_tokens=self._max_tokens,
            context_token_limit=self._max_context_tokens,
            tools=tool_params,
            chat_history=await self._context.full_messages(),
            stream_id=self._stream_id,
            stream_sequence=self._provider_call_sequence,
            stream_attempt=1,
        )

        pre_guard_execution = await self._llm_guards.execute_pre(
            request=self._provider.request_to_dict(guard_request),
            state=self._llm_guard_state,
            stream_id=self._stream_id,
            activity_options=self._llm_guard_activity_options,
        )
        guarded = self._provider.request_from_dict(pre_guard_execution.request)
        if pre_guard_execution.halted:
            self._llm_guard_state = pre_guard_execution.state
            return self._provider.response_from_guard_execution(
                pre_guard_execution,
                model=guarded.model,
            )

        # Persist the (possibly censored) full history: pre-guard mutations are
        # durable. Windowing/clearing below is transport-only and never stored.
        await self._context.replace_messages(
            self._provider.request_chat_history(guarded)
        )

        try:
            response = await self._call_provider_with_context_recovery(
                guarded,
                tool_params,
            )
            if response is None:
                return None

            post_guard_execution = await self._llm_guards.execute_post(
                request=self._provider.request_to_dict(
                    self._provider.replace_request_chat_history(
                        guarded,
                        chat_history=await self._context.full_messages(),
                    )
                ),
                response=self._provider.response_to_dict(response),
                state=pre_guard_execution.state,
                stream_id=self._stream_id,
                activity_options=self._llm_guard_activity_options,
            )
            if post_guard_execution.halted:
                self._llm_guard_state = post_guard_execution.state
                return self._provider.response_from_guard_execution(
                    post_guard_execution,
                    model=response.model,
                )

            self._llm_guard_state = post_guard_execution.state
            return self._provider.response_from_dict(
                post_guard_execution.response
                or self._provider.response_to_dict(response)
            )
        except BaseException as err:
            if self._interrupt_requested and is_cancelled_exception(err):
                return None
            raise

    async def _call_provider_with_context_recovery(
        self,
        guarded_request: ProviderRequest,
        tool_params: list[dict[str, Any]],
    ) -> ProviderResponse | None:
        context_budget = self._context_token_budget(tool_params)
        stream_attempt = 1
        context_overflow_retries = 0

        while True:
            request = self._provider.replace_request_chat_history(
                guarded_request,
                chat_history=await self._context.messages_for_model(context_budget),
            )
            request = self._provider.replace_request_stream_attempt(
                request,
                stream_attempt,
            )

            try:
                return await self._run_provider_activity(request)
            except BaseException as err:
                if self._interrupt_requested and is_cancelled_exception(err):
                    return None
                if (
                    context_overflow_retries < CONTEXT_OVERFLOW_RECOVERY_ATTEMPTS
                    and _is_context_window_exceeded(err)
                ):
                    context_overflow_retries += 1
                    stream_attempt += 1
                    context_budget = self._context_token_budget(
                        tool_params,
                        context_overflow_retry=True,
                    )
                    continue
                raise

    async def _run_provider_activity(
        self,
        request: ProviderRequest,
    ) -> ProviderResponse | None:
        provider_handle = start_provider_activity(self._provider, request)

        await workflow.wait_condition(
            lambda: self._interrupt_requested or provider_handle.done()
        )
        if self._interrupt_requested:
            await self._discard_interrupted_provider_call(provider_handle)
            return None

        return await provider_handle

    def _context_token_budget(
        self,
        tool_schemas: list[dict[str, Any]],
        *,
        context_overflow_retry: bool = False,
    ) -> ContextTokenBudget:
        reserved_input_tokens = self._provider.estimate_request_tokens(
            system_prompt=self._system_prompt,
            tools=tool_schemas,
        )
        safety_margin_tokens = self._context_safety_margin_tokens
        chars_per_token = self._context_chars_per_token
        if context_overflow_retry:
            safety_margin_tokens = max(
                safety_margin_tokens * CONTEXT_OVERFLOW_RETRY_SAFETY_MARGIN_MULTIPLIER,
                CONTEXT_OVERFLOW_RETRY_MIN_SAFETY_MARGIN_TOKENS,
            )
            max_safety_margin = (
                self._max_context_tokens
                - self._max_tokens
                - reserved_input_tokens
                - 1
            )
            safety_margin_tokens = min(
                safety_margin_tokens,
                max(0, max_safety_margin),
            )
            chars_per_token = min(
                chars_per_token,
                CONTEXT_OVERFLOW_RETRY_CHARS_PER_TOKEN,
            )

        return ContextTokenBudget(
            max_context_tokens=self._max_context_tokens,
            reserved_output_tokens=self._max_tokens,
            reserved_input_tokens=reserved_input_tokens,
            safety_margin_tokens=safety_margin_tokens,
            chars_per_token=chars_per_token,
        )

    async def _discard_interrupted_provider_call(
        self, provider_handle: workflow.ActivityHandle[ProviderResponse]
    ) -> None:
        if not provider_handle.done():
            provider_handle.cancel()

        try:
            await provider_handle
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
                "The in-progress assistant response or tool execution was "
                "interrupted by the user. Discard any uncommitted partial "
                "assistant output and use this new context before continuing."
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
        info = workflow.info()
        return (
            self._continue_as_new_policy.enabled
            and (
                info.is_continue_as_new_suggested()
                or info.is_target_worker_deployment_version_changed()
            )
        )

    async def _execute_tool(
        self,
        tool_name: str,
        *,
        tool_call_id: str | None = None,
        **kwargs: Any,
    ) -> ToolResult:
        if self._tool_names is not None and tool_name not in self._tool_names:
            return ToolResult(
                payload={"error": f"Tool is not available to this agent: {tool_name}"},
                error=True,
            )
        if tool_name not in self._tools.tool_names():
            return ToolResult(
                payload={"error": f"Unknown tool requested: {tool_name}"},
                error=True,
            )
        return await self._tools.execute_tool(
            tool_name,
            kwargs,
            stream_id=self._stream_id,
            tool_call_id=tool_call_id,
            activity_options=self._activity_options,
        )

    async def _execute_requested_tools(
        self, tool_use_blocks: list[dict[str, Any]]
    ) -> "ToolExecutionResult":
        tool_tasks = [
            asyncio.create_task(self._execute_requested_tool(block))
            for block in tool_use_blocks
        ]

        try:
            await workflow.wait_condition(
                lambda: self._interrupt_requested
                or all(task.done() for task in tool_tasks)
            )

            interrupted = self._interrupt_requested
            if interrupted:
                for task in tool_tasks:
                    if not task.done():
                        task.cancel()

            # Preserve the tool-use contract: every requested tool receives a
            # result block, even when interruption cancels it.
            tool_results: list[dict[str, Any]] = []
            for block, task in zip(tool_use_blocks, tool_tasks):
                try:
                    tool_results.append(await task)
                except BaseException as err:
                    if interrupted and is_cancelled_exception(err):
                        tool_results.append(self._interrupted_tool_result(block))
                        continue
                    raise

            return ToolExecutionResult(
                tool_results=tool_results,
                interrupted=interrupted,
            )
        except BaseException:
            for task in tool_tasks:
                if not task.done():
                    task.cancel()
            raise

    async def _execute_requested_tool(
        self, block: dict[str, Any]
    ) -> dict[str, Any]:
        tool_name = cast(str, block["name"])
        tool_input = cast(dict[str, Any], block["input"])
        tool_use_id = cast(str, block["id"])

        try:
            result = await self._execute_tool(
                tool_name,
                tool_call_id=tool_use_id,
                **tool_input,
            )
        except UserFacingToolError as err:
            result = ToolResult(
                payload=err.to_tool_payload(),
                error=True,
            )
        except Exception as err:
            if is_cancelled_exception(err):
                raise
            result = ToolResult(
                payload=_tool_exception_payload(tool_name, err),
                error=True,
            )

        return tool_result_block(
            tool_use_id=tool_use_id,
            content=json_content(result.payload),
            is_error=result.error,
        )

    def _interrupted_tool_result(
        self,
        block: dict[str, Any],
    ) -> dict[str, Any]:
        return tool_result_block(
            tool_use_id=cast(str, block["id"]),
            content=json_content(
                {
                    "interrupted": True,
                    "message": (
                        "Tool execution was interrupted by the user before it "
                        "completed."
                    ),
                }
            ),
            is_error=True,
        )

    def _terminated_result(self, *, turns: int) -> AgentResult:
        return AgentResult(
            message=text_message("assistant", "The agent was terminated."),
            stop_reason="refusal",
            turns=turns,
            guard_action=LlmGuardAction.TERMINATE.value,
            guard_reason=self._termination_reason,
        )


@dataclass
class ToolExecutionResult:
    tool_results: list[dict]
    interrupted: bool = False


def _tool_exception_payload(tool_name: str, err: Exception) -> dict[str, Any]:
    causes = [_exception_summary(cause) for cause in _exception_chain(err)]
    root = causes[-1] if causes else _exception_summary(err)
    return {
        "error": root["message"] or "Tool execution failed.",
        "type": "ToolExecutionFailed",
        "tool_name": tool_name,
        "exception_type": type(err).__name__,
        "cause_type": root["type"],
        "message": (
            "The tool failed after any configured retries. Treat this as the "
            "tool result and continue if possible."
        ),
        "causes": causes,
    }


def _is_context_window_exceeded(err: BaseException) -> bool:
    return any(
        isinstance(cause, ApplicationError)
        and cause.type == CONTEXT_WINDOW_EXCEEDED_ERROR_TYPE
        for cause in _exception_chain(err)
    )


def _exception_chain(err: BaseException) -> list[BaseException]:
    chain: list[BaseException] = []
    seen: set[int] = set()
    current: BaseException | None = err
    while current is not None and id(current) not in seen and len(chain) < 6:
        chain.append(current)
        seen.add(id(current))
        next_err = current.__cause__ or current.__context__
        current = next_err if next_err is not current else None
    return chain


def _exception_summary(err: BaseException) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "type": type(err).__name__,
        "message": _exception_message(err),
    }
    if isinstance(err, ApplicationError):
        summary["application_type"] = err.type
        summary["non_retryable"] = err.non_retryable
    if isinstance(err, ActivityError):
        summary["activity_type"] = err.activity_type
        summary["activity_id"] = err.activity_id
        summary["retry_state"] = (
            err.retry_state.name if err.retry_state is not None else None
        )
    return summary


def _exception_message(err: BaseException) -> str:
    message = getattr(err, "message", None)
    if isinstance(message, str) and message:
        return message
    return str(err)


@dataclass
class AgentState:
    context_snapshot: ContextSnapshot
    turns: int
    llm_guard_state: dict = field(default_factory=dict)


@dataclass
class AgentResult:
    message: dict
    stop_reason: AgentStopReason | None
    turns: int
    continuation_state: AgentState | None = None
    guard_action: str | None = None
    guard_reason: str | None = None
    effective_user_prompt: str | None = None
    stop_details: dict | None = None

    @property
    def needs_continue_as_new(self) -> bool:
        return self.continuation_state is not None

    @property
    def terminated(self) -> bool:
        return self.guard_action == LlmGuardAction.TERMINATE.value


def _formatted_control_message(
    *,
    tag: str,
    description: str,
    message: str,
) -> str:
    return f"<{tag}>\n{description}\n\n{message}\n</{tag}>"
