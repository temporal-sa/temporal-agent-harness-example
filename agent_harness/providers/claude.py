from __future__ import annotations

import asyncio
import os
from collections.abc import Iterable
from contextlib import suppress
from dataclasses import dataclass, replace
from datetime import timedelta
from typing import Any, Literal, cast

from anthropic import APIStatusError, AsyncAnthropic
from temporalio import activity
from temporalio.exceptions import ApplicationError

from ..activity_options import ActivityOptions
from ..agent import (
    Agent,
    ContinueAsNewPolicy,
)
from ..context_manager import (
    ContextManagerFactory,
    DEFAULT_CHARS_PER_TOKEN,
    DEFAULT_CONTEXT_SAFETY_MARGIN_TOKENS,
    DEFAULT_MAX_CONTEXT_TOKENS,
)
from ..llm_guards import LlmGuardExecution, LlmGuardFn
from ..messages import (
    AgentBlock,
    AgentMessage,
    CONTEXT_COMPACTION_MARKER,
    CONTEXT_COMPACTION_MARKER_TEXT,
    ToolUseBlock,
    message as agent_message,
    message_text,
    normalize_message,
    provider_block,
    text_block,
    tool_use_block,
)
from ..sliding_window_context_manager import estimate_token_count
from ..streaming import AgentStreamWriter
from ..tools import ToolSet
from .errors import (
    counted_context_window_message,
    counted_tokens_exceed_context,
    status_error_is_context_window_exceeded,
)
from ._shared import (
    copy_mapping as _copy_mapping,
    copy_optional_mapping as _copy_optional_mapping,
    guard_response_dict,
    json_preview as _json_preview,
    mapping_list as _mapping_list,
    needs_refusal_fallback,
    non_retryable_http_status,
    object_to_dict as _object_to_dict,
    optional_object_to_dict as _optional_object_to_dict,
    provider_data as _shared_provider_data,
    provider_metadata as _shared_provider_metadata,
    refusal_fallback_text as _shared_refusal_fallback_text,
)
from .interface import (
    CONTEXT_WINDOW_EXCEEDED_ERROR_TYPE,
    AgentProvider,
    ProviderRequest,
    ProviderResponse,
)

ClaudeStopReason = Literal[
    "end_turn", "max_tokens", "stop_sequence", "tool_use", "pause_turn", "refusal"
]
ClaudeThinkingDisplay = Literal["summarized", "omitted"]
ClaudeThinkingMode = Literal["enabled", "adaptive"]
ClaudeThinkingEffort = Literal["low", "medium", "high", "xhigh", "max"]

DEFAULT_CLAUDE_ACTIVITY_OPTIONS = ActivityOptions(
    start_to_close_timeout=timedelta(minutes=20),
    heartbeat_timeout=timedelta(seconds=10),
)
CLAUDE_HEARTBEAT_INTERVAL_SECONDS = 5
FINE_GRAINED_TOOL_STREAMING_BETA = "fine-grained-tool-streaming-2025-05-14"
DEFAULT_THINKING_BUDGET_TOKENS = 4_096
MIN_THINKING_BUDGET_TOKENS = 1_024
PROVIDER_REFUSAL_FALLBACK = (
    "Claude refused this turn. Anthropic did not return a provider-generated "
    "refusal message, so the app stopped this agent run instead of retrying "
    "the same request."
)
_anthropic_clients: dict[tuple[str, str, str], AsyncAnthropic] = {}


@dataclass(frozen=True)
class ClaudeThinkingConfig:
    enabled: bool = False
    mode: ClaudeThinkingMode = "enabled"
    budget_tokens: int = DEFAULT_THINKING_BUDGET_TOKENS
    effort: ClaudeThinkingEffort | None = None
    display: ClaudeThinkingDisplay | None = None


@dataclass
class ClaudeRequest:
    system_prompt: str
    model: str
    max_tokens: int
    context_token_limit: int | None
    tools: list[dict]
    chat_history: list[dict]
    thinking: dict | None = None
    output_config: dict | None = None
    stream_id: str | None = None
    stream_sequence: int | None = None
    stream_attempt: int | None = None
    stream_agent: dict[str, str | None] | None = None


@dataclass
class ClaudeResponse:
    id: str
    model: str
    message: dict
    stop_reason: ClaudeStopReason | None
    stop_sequence: str | None
    usage: dict
    guard_action: str | None = None
    guard_reason: str | None = None
    stop_details: dict | None = None


class ClaudeProvider(AgentProvider):
    def __init__(
        self,
        *,
        thinking: ClaudeThinkingConfig | None = None,
        activity_options: ActivityOptions | None = None,
        context_chars_per_token: float = DEFAULT_CHARS_PER_TOKEN,
    ):
        self._thinking = thinking
        self._activity_options = activity_options or DEFAULT_CLAUDE_ACTIVITY_OPTIONS
        self._context_chars_per_token = context_chars_per_token

    @property
    def name(self) -> str:
        return "claude"

    @property
    def activity(self) -> Any:
        return call_agent_api

    @property
    def activity_options(self) -> ActivityOptions:
        return self._activity_options

    def estimate_request_tokens(
        self,
        *,
        system_prompt: str,
        tools: list[dict[str, Any]],
    ) -> int:
        return estimate_token_count(
            {"system": system_prompt, "tools": tools},
            chars_per_token=self._context_chars_per_token,
        )

    def create_request(
        self,
        *,
        system_prompt: str,
        model: str,
        max_tokens: int,
        context_token_limit: int | None,
        tools: list[dict[str, Any]],
        chat_history: list[AgentMessage],
        stream_id: str | None,
        stream_sequence: int | None,
        stream_attempt: int | None,
        stream_agent: dict[str, str | None] | None = None,
    ) -> "ClaudeRequest":
        return ClaudeRequest(
            system_prompt=system_prompt,
            model=model,
            max_tokens=max_tokens,
            context_token_limit=context_token_limit,
            tools=tools,
            chat_history=_agent_messages_to_claude_messages(chat_history),
            stream_id=stream_id,
            stream_sequence=stream_sequence,
            stream_attempt=stream_attempt,
            stream_agent=stream_agent,
            **_thinking_request_params(self._thinking, max_tokens=max_tokens),
        )

    def request_chat_history(self, request: ProviderRequest) -> list[AgentMessage]:
        return _claude_messages_to_agent_messages(cast(ClaudeRequest, request).chat_history)

    def replace_request_chat_history(
        self,
        request: ProviderRequest,
        chat_history: list[AgentMessage],
    ) -> "ClaudeRequest":
        return replace(
            cast(ClaudeRequest, request),
            chat_history=_agent_messages_to_claude_messages(chat_history),
        )

    def replace_request_stream_attempt(
        self,
        request: ProviderRequest,
        stream_attempt: int | None,
    ) -> "ClaudeRequest":
        return replace(cast(ClaudeRequest, request), stream_attempt=stream_attempt)

    def request_to_dict(self, request: ProviderRequest) -> dict[str, Any]:
        return _claude_request_to_dict(cast(ClaudeRequest, request))

    def request_from_dict(self, request: dict[str, Any]) -> "ClaudeRequest":
        return _claude_request_from_dict(request)

    def response_to_dict(self, response: ProviderResponse) -> dict[str, Any]:
        return _claude_response_to_dict(cast(ClaudeResponse, response))

    def response_from_dict(self, response: dict[str, Any]) -> "ClaudeResponse":
        return _claude_response_from_dict(response)

    def response_from_guard_execution(
        self,
        execution: LlmGuardExecution,
        *,
        model: str,
    ) -> "ClaudeResponse":
        return _claude_response_from_guard_execution(execution, model=model)

    def response_with_visible_refusal(
        self,
        response: ProviderResponse,
    ) -> "ClaudeResponse":
        return _response_with_visible_refusal(cast(ClaudeResponse, response))

    def response_message(self, response: ProviderResponse) -> AgentMessage:
        return _claude_message_to_agent_message(cast(ClaudeResponse, response).message)


class ClaudeAgent(Agent):
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
        stream_agent: dict[str, str | None] | None = None,
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
        super().__init__(
            system_prompt,
            tools,
            provider=ClaudeProvider(
                thinking=thinking,
                activity_options=claude_activity_options,
                context_chars_per_token=context_chars_per_token,
            ),
            model=model,
            max_tokens=max_tokens,
            tool_names=tool_names,
            stream_id=stream_id,
            stream_agent=stream_agent,
            activity_options=activity_options,
            llm_guard_activity_options=llm_guard_activity_options,
            pre_llm_guards=pre_llm_guards,
            post_llm_guards=post_llm_guards,
            context_manager_factory=context_manager_factory,
            max_context_tokens=max_context_tokens,
            context_safety_margin_tokens=context_safety_margin_tokens,
            context_chars_per_token=context_chars_per_token,
            continue_as_new_policy=continue_as_new_policy,
        )


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
        "context_token_limit": request.context_token_limit,
        "thinking": _copy_optional_mapping(request.thinking),
        "tools": [_copy_mapping(tool) for tool in request.tools],
        "chat_history": [_copy_mapping(message) for message in request.chat_history],
        "stream_id": request.stream_id,
        "stream_sequence": request.stream_sequence,
        "stream_attempt": request.stream_attempt,
        "stream_agent": _copy_optional_mapping(request.stream_agent),
        "output_config": _copy_optional_mapping(request.output_config),
    }


def _claude_request_from_dict(request: dict[str, Any]) -> ClaudeRequest:
    return ClaudeRequest(
        system_prompt=cast(str, request["system_prompt"]),
        model=cast(str, request["model"]),
        max_tokens=cast(int, request["max_tokens"]),
        context_token_limit=cast(int | None, request.get("context_token_limit")),
        thinking=_copy_optional_mapping(request.get("thinking")),
        output_config=_copy_optional_mapping(request.get("output_config")),
        tools=_mapping_list(request.get("tools", [])),
        chat_history=_mapping_list(request.get("chat_history", [])),
        stream_id=cast(str | None, request.get("stream_id")),
        stream_sequence=cast(int | None, request.get("stream_sequence")),
        stream_attempt=cast(int | None, request.get("stream_attempt")),
        stream_agent=cast(
            dict[str, str | None] | None,
            _copy_optional_mapping(request.get("stream_agent")),
        ),
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
        "stop_details": _copy_optional_mapping(response.stop_details),
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
        stop_details=_copy_optional_mapping(response.get("stop_details")),
    )


def _anthropic_client_kwargs() -> dict[str, str]:
    base_url = os.environ.get("ANTHROPIC_BASE_URL", "").strip()
    if not base_url:
        return {}
    return {"base_url": base_url}


def _anthropic_client() -> AsyncAnthropic:
    kwargs = _anthropic_client_kwargs()
    key = (
        os.environ.get("ANTHROPIC_API_KEY", ""),
        os.environ.get("ANTHROPIC_AUTH_TOKEN", ""),
        kwargs.get("base_url", ""),
    )
    client = _anthropic_clients.get(key)
    if client is None:
        client = AsyncAnthropic(max_retries=0, **kwargs)
        _anthropic_clients[key] = client
    return client


async def close_anthropic_clients() -> None:
    clients = list(_anthropic_clients.values())
    _anthropic_clients.clear()
    for client in clients:
        with suppress(Exception):
            await client.close()


def _claude_response_from_guard_execution(
    execution: LlmGuardExecution,
    *,
    model: str,
) -> ClaudeResponse:
    return _claude_response_from_dict(
        guard_response_dict(
            execution,
            model=model,
            message={
                "role": "assistant",
                "content": "The response was blocked by an LLM guard.",
            },
            stop_reason="refusal",
        )
    )


@activity.defn(name="call_agent_api")
async def call_agent_api(request: ClaudeRequest) -> ClaudeResponse:
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

    try:
        client = _anthropic_client()
        if _should_count_request_tokens(request):
            await _raise_if_claude_context_too_large(
                client,
                request,
                create_params,
            )
        response = await _stream_claude_message(
            client,
            create_params,
            stream_id=request.stream_id,
            stream_sequence=request.stream_sequence,
            stream_attempt=request.stream_attempt,
            stream_agent=request.stream_agent,
        )
    except APIStatusError as err:
        await AgentStreamWriter.for_provider(
            stream_id=request.stream_id,
            provider="claude",
            attempt=request.stream_attempt,
            agent=request.stream_agent,
        ).agent_failed(
            sequence=request.stream_sequence,
            model=request.model,
            error=err,
        )
        if _anthropic_error_is_context_window_exceeded(err):
            raise ApplicationError(
                str(err),
                type=CONTEXT_WINDOW_EXCEEDED_ERROR_TYPE,
                non_retryable=True,
            ) from err
        if non_retryable_http_status(err.status_code):
            raise ApplicationError(
                str(err),
                type=err.__class__.__name__,
                non_retryable=True,
            ) from err
        raise
    except BaseException as err:
        if not isinstance(err, asyncio.CancelledError):
            await AgentStreamWriter.for_provider(
                stream_id=request.stream_id,
                provider="claude",
                attempt=request.stream_attempt,
                agent=request.stream_agent,
            ).agent_failed(
                sequence=request.stream_sequence,
                model=request.model,
                error=err,
            )
        raise

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
        stop_details=_optional_object_to_dict(getattr(response, "stop_details", None)),
    )


def _anthropic_error_is_context_window_exceeded(err: APIStatusError) -> bool:
    return status_error_is_context_window_exceeded(err.status_code, str(err))


def _should_count_request_tokens(request: ClaudeRequest) -> bool:
    return bool(
        request.context_token_limit is not None
        and request.stream_attempt is not None
        and request.stream_attempt > 1
    )


async def _raise_if_claude_context_too_large(
    client: AsyncAnthropic,
    request: ClaudeRequest,
    create_params: dict[str, Any],
) -> None:
    count_params: dict[str, Any] = {
        "model": create_params["model"],
        "messages": create_params["messages"],
        "system": create_params["system"],
    }
    if request.tools:
        count_params["tools"] = create_params["tools"]
    if request.thinking is not None:
        count_params["thinking"] = create_params["thinking"]
    if request.output_config is not None:
        count_params["output_config"] = create_params["output_config"]

    token_count = await client.messages.count_tokens(**count_params)
    input_tokens = int(token_count.input_tokens)
    if not counted_tokens_exceed_context(
        input_tokens=input_tokens,
        max_output_tokens=request.max_tokens,
        context_token_limit=request.context_token_limit,
    ):
        return

    raise ApplicationError(
        counted_context_window_message(
            provider="Claude",
            input_tokens=input_tokens,
            max_output_tokens=request.max_tokens,
            context_token_limit=cast(int, request.context_token_limit),
        ),
        type=CONTEXT_WINDOW_EXCEEDED_ERROR_TYPE,
        non_retryable=True,
    )


async def _stream_claude_message(
    client: AsyncAnthropic,
    create_params: dict[str, Any],
    *,
    stream_id: str | None,
    stream_sequence: int | None,
    stream_attempt: int | None,
    stream_agent: dict[str, str | None] | None,
) -> Any:
    stream = AgentStreamWriter.for_provider(
        stream_id=stream_id,
        provider="claude",
        attempt=stream_attempt,
        agent=stream_agent,
    )
    heartbeat_state = _ClaudeHeartbeatState(sequence=stream_sequence)
    activity.heartbeat(heartbeat_state.payload("starting"))
    heartbeat_task = asyncio.create_task(_heartbeat_claude_stream(heartbeat_state))
    await stream.agent_started(
        sequence=stream_sequence,
        model=cast(str | None, create_params.get("model")),
    )

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
                        await stream.agent_cancelled(sequence=stream_sequence)
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

    stop_details = _optional_object_to_dict(getattr(response, "stop_details", None))
    await stream.agent_completed(
        id=response.id,
        model=response.model,
        sequence=stream_sequence,
        stop_reason=response.stop_reason,
        stop_details=stop_details,
        text=_response_display_text(
            response.stop_reason,
            response.content,
            stop_details=stop_details,
        ),
        usage=response.usage.to_dict(),
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
            "kind": "agent_stream",
            "provider": "claude",
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
    stream: AgentStreamWriter,
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
            await stream.thinking_started(
                sequence=stream_sequence,
                content_block_index=block_index,
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
            await stream.tool_input_started(
                sequence=stream_sequence,
                content_block_index=block_index,
                tool_use_id=state.tool_use_id,
                tool_name=state.tool_name,
                tool_type=state.tool_type,
            )
        return

    if event_type == "content_block_delta":
        delta = _object_to_dict(getattr(event, "delta", None))
        delta_type = delta.get("type")
        if delta_type == "text_delta":
            text = delta.get("text")
            if isinstance(text, str) and text:
                await stream.text_delta(sequence=stream_sequence, text=text)
            return

        if delta_type == "refusal_delta":
            text = delta.get("refusal")
            if isinstance(text, str) and text:
                await stream.text_delta(sequence=stream_sequence, text=text)
            return

        if delta_type == "thinking_delta":
            thinking = delta.get("thinking")
            if isinstance(thinking, str) and thinking:
                await stream.thinking_delta(
                    sequence=stream_sequence,
                    thinking=thinking,
                )
            return

        if delta_type == "input_json_delta":
            block_index = cast(int, getattr(event, "index"))
            state = tool_input_blocks.get(block_index)
            partial_json = delta.get("partial_json")
            if state is None or not isinstance(partial_json, str):
                return

            state.partial_json += partial_json
            await stream.tool_input_delta(
                sequence=stream_sequence,
                content_block_index=block_index,
                tool_use_id=state.tool_use_id,
                tool_name=state.tool_name,
                tool_type=state.tool_type,
                partial_json=partial_json,
            )
        return

    if event_type == "content_block_stop":
        block_index = cast(int, getattr(event, "index"))
        state = tool_input_blocks.pop(block_index, None)
        if state is None:
            return

        block = _object_to_dict(getattr(event, "content_block", None))
        input_value = block.get("input", state.partial_json)
        await stream.tool_input_completed(
            sequence=stream_sequence,
            content_block_index=block_index,
            tool_use_id=state.tool_use_id,
            tool_name=state.tool_name,
            tool_type=state.tool_type,
            input=input_value,
            input_preview=_json_preview(input_value),
        )


def _agent_messages_to_claude_messages(
    messages: list[AgentMessage],
) -> list[dict[str, Any]]:
    return [_agent_message_to_claude_message(message) for message in messages]


def _claude_messages_to_agent_messages(
    messages: list[dict[str, Any]],
) -> list[AgentMessage]:
    return [_claude_message_to_agent_message(message) for message in messages]


def _agent_message_to_claude_message(message: AgentMessage) -> dict[str, Any]:
    normalized = normalize_message(message)
    content = normalized["content"]
    if isinstance(content, str):
        claude_content: str | list[dict[str, Any]] = content
    else:
        claude_content = [
            block
            for block in (
                _agent_block_to_claude_block(block)
                for block in content
            )
            if block is not None
        ]

    return {
        "role": normalized["role"],
        "content": claude_content,
    }


def _claude_message_to_agent_message(message: dict[str, Any]) -> AgentMessage:
    role = message.get("role")
    if role not in ("user", "assistant"):
        raise ValueError(f"Invalid Claude message role: {role}")

    content = message.get("content")
    if isinstance(content, str):
        return agent_message(cast(Literal["user", "assistant"], role), content)
    if not isinstance(content, list):
        raise ValueError("Claude message content must be a string or list")

    blocks = [_claude_block_to_agent_block(block) for block in content]
    return agent_message(cast(Literal["user", "assistant"], role), blocks)


def _agent_block_to_claude_block(block: AgentBlock) -> dict[str, Any] | None:
    block_type = block.get("type")
    if block_type == "text":
        return {"type": "text", "text": str(block.get("text") or "")}
    if block_type == CONTEXT_COMPACTION_MARKER:
        return {
            "type": "text",
            "text": str(block.get("text") or CONTEXT_COMPACTION_MARKER_TEXT),
        }
    if block_type == "tool_use":
        return _agent_tool_use_to_claude_block(cast(ToolUseBlock, block))
    if block_type == "tool_result":
        return _agent_tool_result_to_claude_block(block)
    if block_type == "provider" and block.get("provider") == "claude":
        data = block.get("data")
        return _copy_mapping(data) if isinstance(data, dict) else None
    if block_type == "refusal":
        refusal = block.get("refusal")
        return {
            "type": "text",
            "text": refusal if isinstance(refusal, str) else "",
        }
    return None


def _agent_tool_use_to_claude_block(block: ToolUseBlock) -> dict[str, Any]:
    provider_data = _provider_data(block)
    if provider_data is not None:
        claude_block = provider_data
    else:
        claude_block = {"type": "tool_use"}
    claude_block["type"] = "tool_use"
    claude_block["id"] = cast(str, block["id"])
    claude_block["name"] = cast(str, block["name"])
    claude_block["input"] = _copy_mapping(block.get("input", {}))
    return claude_block


def _agent_tool_result_to_claude_block(block: AgentBlock) -> dict[str, Any]:
    provider_data = _provider_data(block)
    if provider_data is not None:
        claude_block = provider_data
    else:
        claude_block = {"type": "tool_result"}
    claude_block["type"] = "tool_result"
    claude_block["tool_use_id"] = cast(str, block["tool_use_id"])
    claude_block["content"] = block.get("content", "")
    claude_block["is_error"] = bool(block.get("is_error", False))
    return claude_block


def _claude_block_to_agent_block(block: Any) -> AgentBlock:
    block_dict = _block_to_dict(block)
    block_type = block_dict.get("type")
    if block_type == "text":
        return text_block(str(block_dict.get("text") or ""))
    if block_type == "tool_use":
        return tool_use_block(
            tool_use_id=cast(str, block_dict["id"]),
            name=cast(str, block_dict["name"]),
            input=_copy_mapping(block_dict.get("input", {})),
            provider=_provider_metadata("tool_use", block_dict),
        )
    if block_type == "tool_result":
        return {
            "type": "tool_result",
            "tool_use_id": block_dict.get("tool_use_id"),
            "content": block_dict.get("content", ""),
            "is_error": bool(block_dict.get("is_error", False)),
            "provider": _provider_metadata("tool_result", block_dict),
        }
    if block_type == "refusal":
        refusal = block_dict.get("refusal")
        return text_block(refusal if isinstance(refusal, str) else "")
    return provider_block(
        provider="claude",
        provider_type=str(block_type or "unknown"),
        data=block_dict,
    )


def _provider_metadata(
    provider_type: str,
    data: dict[str, Any],
) -> dict[str, Any]:
    return _shared_provider_metadata(
        provider="claude",
        provider_type=provider_type,
        data=data,
    )


def _provider_data(block: dict[str, Any]) -> dict[str, Any] | None:
    return _shared_provider_data(block, provider="claude")


def _claude_message_text(message: dict[str, Any]) -> str:
    return message_text(_claude_message_to_agent_message(message))


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


def _response_with_visible_refusal(response: ClaudeResponse) -> ClaudeResponse:
    if not _needs_refusal_fallback(response):
        return response

    return replace(
        response,
        message={
            "role": response.message.get("role", "assistant"),
            "content": _refusal_fallback_text(response.stop_details),
        },
    )


def _needs_refusal_fallback(response: ClaudeResponse) -> bool:
    return needs_refusal_fallback(
        stop_reason=response.stop_reason,
        refusal_stop_reasons={"refusal"},
        guard_action=response.guard_action,
        response_text=_claude_message_text(response.message),
    )


def _response_display_text(
    stop_reason: ClaudeStopReason | None,
    content: Any,
    *,
    stop_details: dict[str, Any] | None,
) -> str:
    text = _text_from_content_blocks(content).strip()
    if text:
        return text
    if stop_reason == "refusal":
        return _refusal_fallback_text(stop_details)
    return ""


def _refusal_fallback_text(stop_details: dict[str, Any] | None = None) -> str:
    return _shared_refusal_fallback_text(
        fallback=PROVIDER_REFUSAL_FALLBACK,
        stop_details=stop_details,
        details_text=_refusal_details_text(stop_details),
    )


def _refusal_details_text(stop_details: dict[str, Any] | None) -> str:
    if not stop_details:
        return ""
    parts: list[str] = []
    category = stop_details.get("category")
    if isinstance(category, str) and category:
        parts.append(f"Refusal category: {category}.")
    explanation = stop_details.get("explanation")
    if isinstance(explanation, str) and explanation:
        parts.append(explanation)
    return " ".join(parts)


def _text_from_content_blocks(content: Any) -> str:
    text_parts: list[str] = []
    for block in content:
        block_dict = _block_to_dict(block)
        text = _text_from_block_dict(block_dict)
        if text:
            text_parts.append(text)
    return "\n".join(text_parts)


def _text_from_block_dict(block: Any) -> str:
    if not isinstance(block, dict):
        return ""
    if block.get("type") == "text":
        text = block.get("text")
        return text if isinstance(text, str) else ""
    if block.get("type") == "refusal":
        refusal = block.get("refusal")
        return refusal if isinstance(refusal, str) else ""
    return ""
