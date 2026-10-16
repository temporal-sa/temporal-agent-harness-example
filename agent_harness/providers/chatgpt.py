from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Iterable
from contextlib import suppress
from dataclasses import dataclass, replace
from datetime import timedelta
from typing import Any, Literal, Mapping, cast

from temporalio import activity
from temporalio.exceptions import ApplicationError

from ..activity_options import ActivityOptions
from ..agent import Agent, ContinueAsNewPolicy
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
from .errors import status_error_is_context_window_exceeded
from ._shared import (
    copy_mapping as _copy_mapping,
    copy_optional_mapping as _copy_optional_mapping,
    guard_response_dict,
    json_object as _json_object,
    json_preview as _json_preview,
    mapping_list as _mapping_list,
    needs_refusal_fallback,
    non_retryable_http_status,
    object_to_dict as _object_to_dict,
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

ChatGPTStopReason = str
ChatGPTReasoningEffort = Literal["minimal", "low", "medium", "high"]
ChatGPTReasoningSummary = Literal["auto", "concise", "detailed"]

DEFAULT_CHATGPT_ACTIVITY_OPTIONS = ActivityOptions(
    start_to_close_timeout=timedelta(minutes=20),
    heartbeat_timeout=timedelta(seconds=10),
)
CHATGPT_HEARTBEAT_INTERVAL_SECONDS = 5
OPENAI_REFUSAL_STOP_REASONS = {"failed", "incomplete"}
PROVIDER_REFUSAL_FALLBACK = (
    "ChatGPT refused or failed this turn. OpenAI did not return a "
    "provider-generated refusal message, so the app stopped this agent run "
    "instead of retrying the same request."
)
_chatgpt_clients: dict[str, Any] = {}


def _chatgpt_client(api_key: str) -> Any:
    client = _chatgpt_clients.get(api_key)
    if client is None:
        from openai import AsyncOpenAI

        client = AsyncOpenAI(api_key=api_key, max_retries=0)
        _chatgpt_clients[api_key] = client
    return client


async def close_chatgpt_clients() -> None:
    clients = list(_chatgpt_clients.values())
    _chatgpt_clients.clear()
    for client in clients:
        with suppress(Exception):
            await client.close()


@dataclass(frozen=True)
class ChatGPTReasoningConfig:
    effort: ChatGPTReasoningEffort | None = None
    summary: ChatGPTReasoningSummary | None = None


@dataclass
class ChatGPTRequest:
    system_prompt: str
    model: str
    max_tokens: int
    context_token_limit: int | None
    tools: list[dict]
    chat_history: list[dict]
    stream_id: str | None = None
    stream_sequence: int | None = None
    stream_attempt: int | None = None
    stream_agent: dict[str, str | None] | None = None
    reasoning: dict | None = None


@dataclass
class ChatGPTResponse:
    id: str
    model: str
    message: dict
    stop_reason: ChatGPTStopReason | None
    stop_sequence: str | None
    usage: dict
    guard_action: str | None = None
    guard_reason: str | None = None
    stop_details: dict | None = None


class ChatGPTProvider(AgentProvider):
    def __init__(
        self,
        *,
        reasoning: ChatGPTReasoningConfig | None = None,
        activity_options: ActivityOptions | None = None,
        context_chars_per_token: float = DEFAULT_CHARS_PER_TOKEN,
    ):
        self._reasoning = reasoning
        self._activity_options = activity_options or DEFAULT_CHATGPT_ACTIVITY_OPTIONS
        self._context_chars_per_token = context_chars_per_token

    @property
    def name(self) -> str:
        return "chatgpt"

    @property
    def activity(self) -> Any:
        return call_chatgpt

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
            {"instructions": system_prompt, "tools": tools},
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
    ) -> ChatGPTRequest:
        return ChatGPTRequest(
            system_prompt=system_prompt,
            model=model,
            max_tokens=max_tokens,
            context_token_limit=context_token_limit,
            tools=_chatgpt_tools_from_agent_tools(tools),
            chat_history=_agent_messages_to_chatgpt_input(chat_history),
            stream_id=stream_id,
            stream_sequence=stream_sequence,
            stream_attempt=stream_attempt,
            stream_agent=stream_agent,
            reasoning=_reasoning_config_to_openai(self._reasoning),
        )

    def request_chat_history(self, request: ProviderRequest) -> list[AgentMessage]:
        return _chatgpt_input_to_agent_messages(cast(ChatGPTRequest, request).chat_history)

    def replace_request_chat_history(
        self,
        request: ProviderRequest,
        chat_history: list[AgentMessage],
    ) -> ChatGPTRequest:
        return replace(
            cast(ChatGPTRequest, request),
            chat_history=_agent_messages_to_chatgpt_input(chat_history),
        )

    def replace_request_stream_attempt(
        self,
        request: ProviderRequest,
        stream_attempt: int | None,
    ) -> ChatGPTRequest:
        return replace(cast(ChatGPTRequest, request), stream_attempt=stream_attempt)

    def request_to_dict(self, request: ProviderRequest) -> dict[str, Any]:
        return _chatgpt_request_to_dict(cast(ChatGPTRequest, request))

    def request_from_dict(self, request: dict[str, Any]) -> ChatGPTRequest:
        return _chatgpt_request_from_dict(request)

    def response_to_dict(self, response: ProviderResponse) -> dict[str, Any]:
        return _chatgpt_response_to_dict(cast(ChatGPTResponse, response))

    def response_from_dict(self, response: dict[str, Any]) -> ChatGPTResponse:
        return _chatgpt_response_from_dict(response)

    def response_from_guard_execution(
        self,
        execution: LlmGuardExecution,
        *,
        model: str,
    ) -> ChatGPTResponse:
        return _chatgpt_response_from_guard_execution(execution, model=model)

    def response_with_visible_refusal(
        self,
        response: ProviderResponse,
    ) -> ChatGPTResponse:
        return _response_with_visible_refusal(cast(ChatGPTResponse, response))

    def response_message(self, response: ProviderResponse) -> AgentMessage:
        return _chatgpt_message_to_agent_message(cast(ChatGPTResponse, response).message)


class ChatGPTAgent(Agent):
    def __init__(
        self,
        system_prompt: str,
        tools: ToolSet,
        *,
        model: str,
        max_tokens: int = 4096,
        reasoning: ChatGPTReasoningConfig | None = None,
        tool_names: list[str] | None = None,
        stream_id: str | None = None,
        stream_agent: dict[str, str | None] | None = None,
        activity_options: ActivityOptions | None = None,
        chatgpt_activity_options: ActivityOptions | None = None,
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
            provider=ChatGPTProvider(
                reasoning=reasoning,
                activity_options=chatgpt_activity_options,
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
    item_id: str
    output_index: int
    tool_use_id: str | None = None
    tool_name: str | None = None
    partial_json: str = ""


@dataclass
class _ChatGPTStreamState:
    sequence: int | None
    response_id: str | None = None
    model: str | None = None
    text_parts: list[str] | None = None
    reasoning_parts: list[str] | None = None
    output_items: list[dict[str, Any]] | None = None
    output_item_indices: dict[int, int] | None = None
    usage: dict[str, Any] | None = None
    stop_reason: str | None = None
    stop_details: dict[str, Any] | None = None
    phase: str = "starting"
    events: int = 0

    def __post_init__(self) -> None:
        if self.text_parts is None:
            self.text_parts = []
        if self.reasoning_parts is None:
            self.reasoning_parts = []
        if self.output_items is None:
            self.output_items = []
        if self.output_item_indices is None:
            self.output_item_indices = {}
        if self.usage is None:
            self.usage = {}

    def message(self) -> dict[str, Any]:
        if not self.output_items and self.text_parts:
            return {
                "role": "assistant",
                "content": [
                    {
                        "type": "message",
                        "role": "assistant",
                        "content": [
                            {
                                "type": "output_text",
                                "text": "".join(self.text_parts),
                                "annotations": [],
                            }
                        ],
                        "status": "completed",
                    }
                ],
            }
        return {
            "role": "assistant",
            "content": [_copy_mapping(item) for item in self.output_items or []],
        }


@dataclass
class _ChatGPTHeartbeatState:
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
            "provider": "chatgpt",
            "heartbeat_reason": heartbeat_reason,
            "sequence": self.sequence,
            "phase": self.phase,
            "events": self.events,
            "last_event_type": self.last_event_type,
            "stop_reason": self.stop_reason,
        }


def _chatgpt_request_to_dict(request: ChatGPTRequest) -> dict[str, Any]:
    return {
        "system_prompt": request.system_prompt,
        "model": request.model,
        "max_tokens": request.max_tokens,
        "context_token_limit": request.context_token_limit,
        "tools": [_copy_mapping(tool) for tool in request.tools],
        "chat_history": [_copy_mapping(message) for message in request.chat_history],
        "stream_id": request.stream_id,
        "stream_sequence": request.stream_sequence,
        "stream_attempt": request.stream_attempt,
        "stream_agent": _copy_optional_mapping(request.stream_agent),
        "reasoning": _copy_optional_mapping(request.reasoning),
    }


def _chatgpt_request_from_dict(request: dict[str, Any]) -> ChatGPTRequest:
    return ChatGPTRequest(
        system_prompt=cast(str, request["system_prompt"]),
        model=cast(str, request["model"]),
        max_tokens=cast(int, request["max_tokens"]),
        context_token_limit=cast(int | None, request.get("context_token_limit")),
        tools=_mapping_list(request.get("tools", [])),
        chat_history=_mapping_list(request.get("chat_history", [])),
        stream_id=cast(str | None, request.get("stream_id")),
        stream_sequence=cast(int | None, request.get("stream_sequence")),
        stream_attempt=cast(int | None, request.get("stream_attempt")),
        stream_agent=cast(
            dict[str, str | None] | None,
            _copy_optional_mapping(request.get("stream_agent")),
        ),
        reasoning=_copy_optional_mapping(request.get("reasoning")),
    )


def _chatgpt_response_to_dict(response: ChatGPTResponse) -> dict[str, Any]:
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


def _chatgpt_response_from_dict(response: dict[str, Any]) -> ChatGPTResponse:
    return ChatGPTResponse(
        id=cast(str, response["id"]),
        model=cast(str, response["model"]),
        message=_chatgpt_response_message_from_value(response["message"]),
        stop_reason=cast(str | None, response.get("stop_reason")),
        stop_sequence=cast(str | None, response.get("stop_sequence")),
        usage=_copy_mapping(response.get("usage", {})),
        guard_action=cast(str | None, response.get("guard_action")),
        guard_reason=cast(str | None, response.get("guard_reason")),
        stop_details=_copy_optional_mapping(response.get("stop_details")),
    )


def _chatgpt_response_from_guard_execution(
    execution: LlmGuardExecution,
    *,
    model: str,
) -> ChatGPTResponse:
    return _chatgpt_response_from_dict(
        guard_response_dict(
            execution,
            model=model,
            message={
                "role": "assistant",
                "content": [
                    {
                        "type": "message",
                        "role": "assistant",
                        "content": [
                            {
                                "type": "output_text",
                                "text": "The response was blocked by an LLM guard.",
                                "annotations": [],
                            }
                        ],
                        "status": "completed",
                    }
                ],
            },
            stop_reason="failed",
        )
    )


@activity.defn
async def call_chatgpt(request: ChatGPTRequest) -> ChatGPTResponse:
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        error = ApplicationError(
            "OPENAI_API_KEY must be set to call ChatGPT",
            type="MissingApiKey",
            non_retryable=True,
        )
        await AgentStreamWriter.for_provider(
            stream_id=request.stream_id,
            provider="chatgpt",
            attempt=request.stream_attempt,
            agent=request.stream_agent,
        ).agent_failed(
            sequence=request.stream_sequence,
            model=request.model,
            error=error,
        )
        raise error

    create_params: dict[str, Any] = {
        "model": request.model,
        "instructions": request.system_prompt or None,
        "input": request.chat_history,
        "max_output_tokens": request.max_tokens,
        "stream": True,
        "store": False,
    }
    if request.tools:
        create_params["tools"] = request.tools
        create_params["tool_choice"] = "auto"
        create_params["parallel_tool_calls"] = True
    if request.reasoning is not None:
        create_params["reasoning"] = request.reasoning

    try:
        from openai import APIStatusError

        state = await _stream_chatgpt_response(
            _chatgpt_client(api_key),
            create_params,
            stream_id=request.stream_id,
            stream_sequence=request.stream_sequence,
            stream_attempt=request.stream_attempt,
            stream_agent=request.stream_agent,
        )
    except APIStatusError as err:
        await AgentStreamWriter.for_provider(
            stream_id=request.stream_id,
            provider="chatgpt",
            attempt=request.stream_attempt,
            agent=request.stream_agent,
        ).agent_failed(
            sequence=request.stream_sequence,
            model=request.model,
            error=err,
        )
        if _openai_error_is_context_window_exceeded(err):
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
                provider="chatgpt",
                attempt=request.stream_attempt,
                agent=request.stream_agent,
            ).agent_failed(
                sequence=request.stream_sequence,
                model=request.model,
                error=err,
            )
        raise

    return ChatGPTResponse(
        id=state.response_id or f"chatgpt:{request.stream_sequence or 0}",
        model=state.model or request.model,
        message=state.message(),
        stop_reason=state.stop_reason,
        stop_sequence=None,
        usage=state.usage or {},
        stop_details=state.stop_details,
    )


async def _stream_chatgpt_response(
    client: Any,
    create_params: dict[str, Any],
    *,
    stream_id: str | None,
    stream_sequence: int | None,
    stream_attempt: int | None,
    stream_agent: dict[str, str | None] | None,
) -> _ChatGPTStreamState:
    stream = AgentStreamWriter.for_provider(
        stream_id=stream_id,
        provider="chatgpt",
        attempt=stream_attempt,
        agent=stream_agent,
    )
    stream_state = _ChatGPTStreamState(sequence=stream_sequence)
    heartbeat_state = _ChatGPTHeartbeatState(sequence=stream_sequence)
    activity.heartbeat(heartbeat_state.payload("starting"))
    heartbeat_task = asyncio.create_task(_heartbeat_chatgpt_stream(heartbeat_state))

    await stream.agent_started(
        sequence=stream_sequence,
        model=cast(str | None, create_params.get("model")),
    )

    try:
        response_stream = await client.responses.create(**create_params)
        heartbeat_state.phase = "streaming"
        cancel_task = asyncio.create_task(activity.wait_for_cancelled())
        event_iterator = response_stream.__aiter__()
        tool_input_blocks: dict[str, _ToolInputStreamState] = {}
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
                stream_state.events += 1
                activity.heartbeat(heartbeat_state.payload("event"))
                await _record_chatgpt_stream_event(
                    stream=stream,
                    event=event,
                    state=stream_state,
                    tool_input_blocks=tool_input_blocks,
                )
        finally:
            cancel_task.cancel()
            with suppress(asyncio.CancelledError):
                await cancel_task
    finally:
        heartbeat_task.cancel()
        with suppress(asyncio.CancelledError):
            await heartbeat_task

    await stream.agent_completed(
        id=stream_state.response_id,
        model=stream_state.model,
        sequence=stream_sequence,
        stop_reason=stream_state.stop_reason,
        stop_details=stream_state.stop_details,
        text="".join(stream_state.text_parts or []),
        usage=stream_state.usage or {},
    )
    heartbeat_state.phase = "complete"
    heartbeat_state.stop_reason = stream_state.stop_reason
    activity.heartbeat(heartbeat_state.payload("complete"))
    return stream_state


async def _heartbeat_chatgpt_stream(state: _ChatGPTHeartbeatState) -> None:
    while True:
        await asyncio.sleep(CHATGPT_HEARTBEAT_INTERVAL_SECONDS)
        activity.heartbeat(state.payload("timer"))


async def _record_chatgpt_stream_event(
    *,
    stream: AgentStreamWriter,
    event: Any,
    state: _ChatGPTStreamState,
    tool_input_blocks: dict[str, _ToolInputStreamState],
) -> None:
    event_type = getattr(event, "type", None)
    if event_type == "response.output_text.delta":
        text = cast(str | None, getattr(event, "delta", None))
        if text:
            state.text_parts.append(text)
            await stream.text_delta(sequence=state.sequence, text=text)
        return

    if event_type == "response.refusal.delta":
        text = cast(str | None, getattr(event, "delta", None))
        if text:
            state.text_parts.append(text)
            await stream.text_delta(sequence=state.sequence, text=text)
        return

    if event_type in (
        "response.reasoning_text.delta",
        "response.reasoning_summary_text.delta",
    ):
        thinking = cast(str | None, getattr(event, "delta", None))
        if thinking:
            if not state.reasoning_parts:
                await stream.thinking_started(
                    sequence=state.sequence,
                    content_block_index=0,
                )
            state.reasoning_parts.append(thinking)
            await stream.thinking_delta(sequence=state.sequence, thinking=thinking)
        return

    if event_type == "response.output_item.added":
        item = _object_to_dict(getattr(event, "item", None))
        output_index = cast(int, getattr(event, "output_index", 0))
        _upsert_output_item(state, output_index=output_index, item=item)
        if item.get("type") != "function_call":
            return
        item_id = cast(str, item.get("id") or "")
        state_key = item_id or str(output_index)
        tool_state = _ToolInputStreamState(
            item_id=state_key,
            output_index=output_index,
            tool_use_id=cast(str | None, item.get("call_id")),
            tool_name=cast(str | None, item.get("name")),
            partial_json=cast(str, item.get("arguments") or ""),
        )
        tool_input_blocks[state_key] = tool_state
        tool_input_blocks[str(output_index)] = tool_state
        await stream.tool_input_started(
            sequence=state.sequence,
            content_block_index=output_index,
            tool_use_id=tool_state.tool_use_id,
            tool_name=tool_state.tool_name,
            tool_type="function_call",
        )
        return

    if event_type == "response.output_item.done":
        item = _object_to_dict(getattr(event, "item", None))
        output_index = cast(int, getattr(event, "output_index", 0))
        _upsert_output_item(state, output_index=output_index, item=item)
        return

    if event_type == "response.function_call_arguments.delta":
        item_id = cast(str, getattr(event, "item_id", ""))
        output_index = cast(int, getattr(event, "output_index", 0))
        state_key = item_id or str(output_index)
        tool_state = _tool_input_state(
            tool_input_blocks,
            item_id=state_key,
            output_index=output_index,
        )
        partial_json = cast(str | None, getattr(event, "delta", None))
        if not partial_json:
            return
        tool_state.partial_json += partial_json
        await stream.tool_input_delta(
            sequence=state.sequence,
            content_block_index=tool_state.output_index,
            tool_use_id=tool_state.tool_use_id,
            tool_name=tool_state.tool_name,
            tool_type="function_call",
            partial_json=partial_json,
        )
        return

    if event_type == "response.function_call_arguments.done":
        item_id = cast(str, getattr(event, "item_id", ""))
        output_index = cast(int, getattr(event, "output_index", 0))
        state_key = item_id or str(output_index)
        tool_state = _tool_input_state(
            tool_input_blocks,
            item_id=state_key,
            output_index=output_index,
        )
        tool_state.tool_name = (
            cast(str | None, getattr(event, "name", None))
            or tool_state.tool_name
        )
        arguments = cast(str | None, getattr(event, "arguments", None))
        if arguments is not None:
            tool_state.partial_json = arguments
            _merge_function_call_arguments(
                state,
                output_index=output_index,
                arguments=arguments,
            )
        input_value = _json_object(tool_state.partial_json)
        await stream.tool_input_completed(
            sequence=state.sequence,
            content_block_index=tool_state.output_index,
            tool_use_id=tool_state.tool_use_id,
            tool_name=tool_state.tool_name,
            tool_type="function_call",
            input=input_value,
            input_preview=_json_preview(input_value),
        )
        return

    if event_type == "response.completed":
        response = getattr(event, "response", None)
        _record_final_openai_response(response, state)
        return

    if event_type in ("response.failed", "response.incomplete"):
        response = getattr(event, "response", None)
        _record_final_openai_response(response, state)
        return

    if event_type == "error":
        message = cast(str | None, getattr(event, "message", None))
        raise ApplicationError(
            message or "OpenAI stream error",
            type=cast(str | None, getattr(event, "code", None)) or "OpenAIStreamError",
            non_retryable=True,
        )


def _upsert_output_item(
    state: _ChatGPTStreamState,
    *,
    output_index: int,
    item: dict[str, Any],
) -> None:
    if not item:
        return
    if state.output_items is None:
        state.output_items = []
    if state.output_item_indices is None:
        state.output_item_indices = {}

    existing_index = state.output_item_indices.get(output_index)
    if existing_index is None:
        state.output_item_indices[output_index] = len(state.output_items)
        state.output_items.append(_copy_mapping(item))
        return

    state.output_items[existing_index] = _copy_mapping(item)


def _merge_function_call_arguments(
    state: _ChatGPTStreamState,
    *,
    output_index: int,
    arguments: str,
) -> None:
    if state.output_items is None or state.output_item_indices is None:
        return
    existing_index = state.output_item_indices.get(output_index)
    if existing_index is None:
        return
    item = state.output_items[existing_index]
    if item.get("type") == "function_call":
        item["arguments"] = arguments


def _tool_input_state(
    tool_input_blocks: dict[str, _ToolInputStreamState],
    *,
    item_id: str,
    output_index: int,
) -> _ToolInputStreamState:
    output_key = str(output_index)
    tool_state = tool_input_blocks.get(item_id) or tool_input_blocks.get(output_key)
    if tool_state is None:
        tool_state = _ToolInputStreamState(
            item_id=item_id,
            output_index=output_index,
        )
    tool_input_blocks[item_id] = tool_state
    tool_input_blocks[output_key] = tool_state
    return tool_state


def _record_final_openai_response(response: Any, state: _ChatGPTStreamState) -> None:
    response_dict = _object_to_dict(response)
    state.response_id = cast(str | None, response_dict.get("id")) or state.response_id
    model = response_dict.get("model")
    if isinstance(model, str):
        state.model = model
    status = response_dict.get("status")
    if isinstance(status, str):
        state.stop_reason = status
    usage = response_dict.get("usage")
    if isinstance(usage, dict):
        state.usage = _copy_mapping(usage)
    stop_details: dict[str, Any] = {}
    error = response_dict.get("error")
    if isinstance(error, dict):
        stop_details["error"] = _copy_mapping(error)
    incomplete_details = response_dict.get("incomplete_details")
    if isinstance(incomplete_details, dict):
        stop_details["incomplete_details"] = _copy_mapping(incomplete_details)
    if stop_details:
        state.stop_details = stop_details
    output = response_dict.get("output")
    if isinstance(output, list):
        state.output_items = [_copy_mapping(item) for item in output]
        state.output_item_indices = {
            index: index for index in range(len(state.output_items))
        }


def _openai_error_is_context_window_exceeded(err: Any) -> bool:
    return status_error_is_context_window_exceeded(
        cast(int | None, getattr(err, "status_code", None)),
        str(err),
    )


def _agent_messages_to_chatgpt_input(
    messages: list[AgentMessage],
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for message in messages:
        items.extend(_agent_message_to_chatgpt_input_items(message))
    return items


def _chatgpt_input_to_agent_messages(
    items: list[dict[str, Any]],
) -> list[AgentMessage]:
    messages: list[AgentMessage] = []
    for item in items:
        message = _chatgpt_input_item_to_agent_message(item)
        if message is not None:
            messages.append(message)
    return messages


def _agent_message_to_chatgpt_input_items(
    message: AgentMessage,
) -> list[dict[str, Any]]:
    normalized = normalize_message(message)
    role = cast(Literal["user", "assistant"], normalized["role"])
    content = normalized["content"]
    if isinstance(content, str):
        return [{"role": role, "content": content}]

    items: list[dict[str, Any]] = []
    text_parts: list[str] = []

    def flush_text() -> None:
        if not text_parts:
            return
        items.append({"role": role, "content": "\n".join(text_parts)})
        text_parts.clear()

    for block in content:
        block_type = block.get("type")
        if block_type == "text":
            text_parts.append(str(block.get("text") or ""))
            continue
        if block_type == CONTEXT_COMPACTION_MARKER:
            text_parts.append(str(block.get("text") or CONTEXT_COMPACTION_MARKER_TEXT))
            continue
        if block_type == "refusal":
            refusal = block.get("refusal")
            text_parts.append(refusal if isinstance(refusal, str) else "")
            continue

        flush_text()
        if block_type == "tool_use":
            items.append(_agent_tool_use_to_chatgpt_item(cast(ToolUseBlock, block)))
            continue
        if block_type == "tool_result":
            items.append(_agent_tool_result_to_chatgpt_item(block))
            continue
        if block_type == "provider" and block.get("provider") == "chatgpt":
            data = block.get("data")
            if isinstance(data, dict):
                items.append(_copy_mapping(data))

    flush_text()
    return items


def _agent_tool_use_to_chatgpt_item(block: ToolUseBlock) -> dict[str, Any]:
    provider_data = _provider_data(block)
    if provider_data is not None:
        item = _copy_mapping(provider_data)
    else:
        item = {"type": "function_call"}
    item["type"] = "function_call"
    item["call_id"] = cast(str, block["id"])
    item["name"] = cast(str, block["name"])
    item["arguments"] = json.dumps(block.get("input", {}), sort_keys=True)
    item["status"] = "completed"
    return item


def _agent_tool_result_to_chatgpt_item(block: AgentBlock) -> dict[str, Any]:
    provider_data = _provider_data(block)
    if provider_data is not None:
        item = _copy_mapping(provider_data)
    else:
        item = {"type": "function_call_output"}
    item["type"] = "function_call_output"
    item["call_id"] = cast(str, block["tool_use_id"])
    item["output"] = str(block.get("content", ""))
    item["status"] = "completed"
    return item


def _chatgpt_input_item_to_agent_message(item: dict[str, Any]) -> AgentMessage | None:
    item_type = item.get("type")
    if item_type == "function_call":
        return agent_message("assistant", [_chatgpt_function_call_to_agent_block(item, 0)])
    if item_type == "function_call_output":
        return agent_message("user", [_chatgpt_function_output_to_agent_block(item)])

    role = item.get("role")
    if role not in ("user", "assistant"):
        return None
    content = item.get("content")
    if isinstance(content, str):
        return agent_message(cast(Literal["user", "assistant"], role), content)
    if isinstance(content, list):
        blocks = [
            _chatgpt_content_part_to_agent_block(part)
            for part in content
            if isinstance(part, dict)
        ]
        return agent_message(cast(Literal["user", "assistant"], role), blocks)
    return agent_message(cast(Literal["user", "assistant"], role), "")


def _chatgpt_message_to_agent_message(message: dict[str, Any]) -> AgentMessage:
    content = message.get("content")
    if isinstance(content, str):
        return agent_message("assistant", content)
    if not isinstance(content, list):
        return agent_message("assistant", "")

    blocks: list[AgentBlock] = []
    for index, item in enumerate(content):
        item_dict = _copy_mapping(item)
        item_type = item_dict.get("type")
        if item_type == "message":
            blocks.extend(_message_item_to_agent_blocks(item_dict))
            continue
        if item_type == "function_call":
            blocks.append(_chatgpt_function_call_to_agent_block(item_dict, index))
            continue
        if item_type == "function_call_output":
            blocks.append(_chatgpt_function_output_to_agent_block(item_dict))
            continue
        if item_type in ("reasoning", "reasoning_summary"):
            blocks.append(
                provider_block(
                    provider="chatgpt",
                    provider_type=cast(str, item_type),
                    data=item_dict,
                )
            )
            continue
        blocks.append(
            provider_block(
                provider="chatgpt",
                provider_type=str(item_type or "unknown"),
                data=item_dict,
            )
        )

    return agent_message("assistant", blocks)


def _message_item_to_agent_blocks(item: Mapping[str, Any]) -> list[AgentBlock]:
    content = item.get("content")
    if isinstance(content, str):
        return [text_block(content)]
    if not isinstance(content, list):
        return []
    return [
        _chatgpt_content_part_to_agent_block(part)
        for part in content
        if isinstance(part, dict)
    ]


def _chatgpt_content_part_to_agent_block(part: Mapping[str, Any]) -> AgentBlock:
    part_dict = _copy_mapping(part)
    part_type = part_dict.get("type")
    if part_type in ("output_text", "input_text"):
        return text_block(str(part_dict.get("text") or ""))
    if part_type == "refusal":
        refusal = part_dict.get("refusal")
        return text_block(refusal if isinstance(refusal, str) else "")
    return provider_block(
        provider="chatgpt",
        provider_type=str(part_type or "unknown"),
        data=part_dict,
    )


def _chatgpt_function_call_to_agent_block(
    item: Mapping[str, Any],
    index: int,
) -> AgentBlock:
    item_dict = _copy_mapping(item)
    return tool_use_block(
        tool_use_id=_chatgpt_tool_use_id(item_dict, index),
        name=cast(str, item_dict.get("name") or ""),
        input=_json_object(cast(str, item_dict.get("arguments") or "")),
        provider=_provider_metadata("function_call", item_dict),
    )


def _chatgpt_function_output_to_agent_block(item: Mapping[str, Any]) -> AgentBlock:
    item_dict = _copy_mapping(item)
    return {
        "type": "tool_result",
        "tool_use_id": item_dict.get("call_id"),
        "content": item_dict.get("output", ""),
        "is_error": False,
        "provider": _provider_metadata("function_call_output", item_dict),
    }


def _chatgpt_response_message_from_value(value: Any) -> dict[str, Any]:
    message = _copy_mapping(value)
    if "content" in message:
        return message
    return {
        "role": "assistant",
        "content": [
            {
                "type": "message",
                "role": "assistant",
                "content": [
                    {
                        "type": "output_text",
                        "text": str(message.get("text") or ""),
                        "annotations": [],
                    }
                ],
                "status": "completed",
            }
        ],
    }


def _chatgpt_tool_use_id(item: Mapping[str, Any], index: int) -> str:
    call_id = item.get("call_id")
    if isinstance(call_id, str) and call_id:
        return call_id
    item_id = item.get("id")
    if isinstance(item_id, str) and item_id:
        return item_id
    name = item.get("name")
    return f"chatgpt-tool-{index}-{name or 'unknown'}"


def _chatgpt_tools_from_agent_tools(tools: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for tool in tools:
        name = tool.get("name")
        if not isinstance(name, str) or not name:
            continue
        function_tool: dict[str, Any] = {
            "type": "function",
            "name": name,
            "parameters": _copy_mapping(tool.get("input_schema", {})),
            "strict": False,
        }
        description = tool.get("description")
        if isinstance(description, str):
            function_tool["description"] = description
        result.append(function_tool)
    return result


def _reasoning_config_to_openai(
    reasoning: ChatGPTReasoningConfig | None,
) -> dict[str, Any] | None:
    if reasoning is None:
        return None
    result: dict[str, Any] = {}
    if reasoning.effort is not None:
        result["effort"] = reasoning.effort
    if reasoning.summary is not None:
        result["summary"] = reasoning.summary
    return result or None


def _provider_metadata(
    provider_type: str,
    data: dict[str, Any],
) -> dict[str, Any]:
    return _shared_provider_metadata(
        provider="chatgpt",
        provider_type=provider_type,
        data=data,
    )


def _provider_data(block: Mapping[str, Any]) -> dict[str, Any] | None:
    return _shared_provider_data(block, provider="chatgpt")


def _response_with_visible_refusal(response: ChatGPTResponse) -> ChatGPTResponse:
    if not _needs_refusal_fallback(response):
        return response
    return replace(
        response,
        message={
            "role": response.message.get("role", "assistant"),
            "content": [
                {
                    "type": "message",
                    "role": "assistant",
                    "content": [
                        {
                            "type": "output_text",
                            "text": _refusal_fallback_text(response.stop_details),
                            "annotations": [],
                        }
                    ],
                    "status": "completed",
                }
            ],
        },
    )


def _needs_refusal_fallback(response: ChatGPTResponse) -> bool:
    return needs_refusal_fallback(
        stop_reason=response.stop_reason,
        refusal_stop_reasons=OPENAI_REFUSAL_STOP_REASONS,
        guard_action=response.guard_action,
        response_text=message_text(_chatgpt_message_to_agent_message(response.message)),
    )


def _refusal_fallback_text(stop_details: dict[str, Any] | None = None) -> str:
    return _shared_refusal_fallback_text(
        fallback=PROVIDER_REFUSAL_FALLBACK,
        stop_details=stop_details,
        details_text=_refusal_details_text(stop_details),
    )


def _refusal_details_text(stop_details: dict[str, Any] | None) -> str:
    if not stop_details:
        return ""
    error = stop_details.get("error")
    if isinstance(error, dict):
        message = error.get("message")
        if isinstance(message, str) and message:
            return message
    incomplete = stop_details.get("incomplete_details")
    if isinstance(incomplete, dict):
        reason = incomplete.get("reason")
        if isinstance(reason, str) and reason:
            return f"Incomplete reason: {reason}."
    return ""
