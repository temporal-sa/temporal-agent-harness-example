from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Iterable
from contextlib import suppress
from dataclasses import dataclass, replace
from datetime import timedelta
from typing import Any, Literal, Mapping, cast

from google import genai
from google.genai import errors as genai_errors
from google.genai import types as genai_types
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
    object_to_dict as _model_to_dict,
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

GeminiStopReason = str
GeminiThinkingLevel = Literal["minimal", "low", "medium", "high"]

DEFAULT_GEMINI_ACTIVITY_OPTIONS = ActivityOptions(
    start_to_close_timeout=timedelta(minutes=20),
    heartbeat_timeout=timedelta(seconds=10),
)
GEMINI_HEARTBEAT_INTERVAL_SECONDS = 5
GEMINI_REFUSAL_STOP_REASONS = {
    "SAFETY",
    "RECITATION",
    "BLOCKLIST",
    "PROHIBITED_CONTENT",
    "SPII",
}
PROVIDER_REFUSAL_FALLBACK = (
    "Gemini refused this turn. Google did not return a provider-generated "
    "refusal message, so the app stopped this agent run instead of retrying "
    "the same request."
)
_gemini_clients: dict[str, genai.Client] = {}


def _gemini_client(api_key: str) -> genai.Client:
    client = _gemini_clients.get(api_key)
    if client is None:
        client = genai.Client(
            api_key=api_key,
            http_options=_gemini_http_options(),
        )
        _gemini_clients[api_key] = client
    return client


async def close_gemini_clients() -> None:
    clients = list(_gemini_clients.values())
    _gemini_clients.clear()
    for client in clients:
        with suppress(Exception):
            await client.aio.aclose()


@dataclass(frozen=True)
class GeminiThinkingConfig:
    include_thoughts: bool = False
    thinking_budget: int | None = None
    thinking_level: GeminiThinkingLevel | None = None


@dataclass
class GeminiRequest:
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
    thinking_config: dict | None = None


@dataclass
class GeminiResponse:
    id: str
    model: str
    message: dict
    stop_reason: GeminiStopReason | None
    stop_sequence: str | None
    usage: dict
    guard_action: str | None = None
    guard_reason: str | None = None
    stop_details: dict | None = None


class GeminiProvider(AgentProvider):
    def __init__(
        self,
        *,
        thinking: GeminiThinkingConfig | None = None,
        activity_options: ActivityOptions | None = None,
        context_chars_per_token: float = DEFAULT_CHARS_PER_TOKEN,
    ):
        self._thinking = thinking
        self._activity_options = activity_options or DEFAULT_GEMINI_ACTIVITY_OPTIONS
        self._context_chars_per_token = context_chars_per_token

    @property
    def name(self) -> str:
        return "gemini"

    @property
    def activity(self) -> Any:
        return call_gemini

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
    ) -> GeminiRequest:
        return GeminiRequest(
            system_prompt=system_prompt,
            model=model,
            max_tokens=max_tokens,
            context_token_limit=context_token_limit,
            tools=_gemini_tools_from_agent_tools(tools),
            chat_history=_agent_messages_to_gemini_contents(chat_history),
            stream_id=stream_id,
            stream_sequence=stream_sequence,
            stream_attempt=stream_attempt,
            stream_agent=stream_agent,
            thinking_config=_thinking_config_to_gemini(self._thinking),
        )

    def request_chat_history(self, request: ProviderRequest) -> list[AgentMessage]:
        return _gemini_contents_to_agent_messages(cast(GeminiRequest, request).chat_history)

    def replace_request_chat_history(
        self,
        request: ProviderRequest,
        chat_history: list[AgentMessage],
    ) -> GeminiRequest:
        return replace(
            cast(GeminiRequest, request),
            chat_history=_agent_messages_to_gemini_contents(chat_history),
        )

    def replace_request_stream_attempt(
        self,
        request: ProviderRequest,
        stream_attempt: int | None,
    ) -> GeminiRequest:
        return replace(cast(GeminiRequest, request), stream_attempt=stream_attempt)

    def request_to_dict(self, request: ProviderRequest) -> dict[str, Any]:
        return _gemini_request_to_dict(cast(GeminiRequest, request))

    def request_from_dict(self, request: dict[str, Any]) -> GeminiRequest:
        return _gemini_request_from_dict(request)

    def response_to_dict(self, response: ProviderResponse) -> dict[str, Any]:
        return _gemini_response_to_dict(cast(GeminiResponse, response))

    def response_from_dict(self, response: dict[str, Any]) -> GeminiResponse:
        return _gemini_response_from_dict(response)

    def response_from_guard_execution(
        self,
        execution: LlmGuardExecution,
        *,
        model: str,
    ) -> GeminiResponse:
        return _gemini_response_from_guard_execution(execution, model=model)

    def response_with_visible_refusal(
        self,
        response: ProviderResponse,
    ) -> GeminiResponse:
        return _response_with_visible_refusal(cast(GeminiResponse, response))

    def response_message(self, response: ProviderResponse) -> AgentMessage:
        return _gemini_content_to_agent_message(cast(GeminiResponse, response).message)


class GeminiAgent(Agent):
    def __init__(
        self,
        system_prompt: str,
        tools: ToolSet,
        *,
        model: str,
        max_tokens: int = 4096,
        thinking: GeminiThinkingConfig | None = None,
        tool_names: list[str] | None = None,
        stream_id: str | None = None,
        stream_agent: dict[str, str | None] | None = None,
        activity_options: ActivityOptions | None = None,
        gemini_activity_options: ActivityOptions | None = None,
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
            provider=GeminiProvider(
                thinking=thinking,
                activity_options=gemini_activity_options,
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
class _GeminiStreamState:
    sequence: int | None
    response_id: str | None = None
    model: str | None = None
    text_parts: list[str] | None = None
    thought_parts: list[str] | None = None
    function_calls: list[dict[str, Any]] | None = None
    usage: dict[str, Any] | None = None
    stop_reason: str | None = None
    stop_details: dict[str, Any] | None = None
    phase: str = "starting"
    events: int = 0

    def __post_init__(self) -> None:
        if self.text_parts is None:
            self.text_parts = []
        if self.thought_parts is None:
            self.thought_parts = []
        if self.function_calls is None:
            self.function_calls = []
        if self.usage is None:
            self.usage = {}

    def message(self) -> dict[str, Any]:
        blocks: list[dict[str, Any]] = []
        text = "".join(self.text_parts or [])
        if text:
            blocks.append({"type": "text", "text": text})
        for function_call in self.function_calls or []:
            blocks.append({"function_call": _copy_mapping(function_call)})
        if self.thought_parts:
            blocks.append(
                {
                    "thought": True,
                    "text": "".join(self.thought_parts),
                }
            )
        return {"role": "model", "parts": blocks}


@dataclass
class _GeminiHeartbeatState:
    sequence: int | None
    phase: str = "starting"
    events: int = 0
    stop_reason: str | None = None

    def payload(self, heartbeat_reason: str) -> dict[str, Any]:
        return {
            "kind": "agent_stream",
            "provider": "gemini",
            "heartbeat_reason": heartbeat_reason,
            "sequence": self.sequence,
            "phase": self.phase,
            "events": self.events,
            "stop_reason": self.stop_reason,
        }


def _gemini_request_to_dict(request: GeminiRequest) -> dict[str, Any]:
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
        "thinking_config": _copy_optional_mapping(request.thinking_config),
    }


def _gemini_request_from_dict(request: dict[str, Any]) -> GeminiRequest:
    return GeminiRequest(
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
        thinking_config=_copy_optional_mapping(request.get("thinking_config")),
    )


def _gemini_response_to_dict(response: GeminiResponse) -> dict[str, Any]:
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


def _gemini_response_from_dict(response: dict[str, Any]) -> GeminiResponse:
    return GeminiResponse(
        id=cast(str, response["id"]),
        model=cast(str, response["model"]),
        message=_gemini_response_message_from_value(response["message"]),
        stop_reason=cast(str | None, response.get("stop_reason")),
        stop_sequence=cast(str | None, response.get("stop_sequence")),
        usage=_copy_mapping(response.get("usage", {})),
        guard_action=cast(str | None, response.get("guard_action")),
        guard_reason=cast(str | None, response.get("guard_reason")),
        stop_details=_copy_optional_mapping(response.get("stop_details")),
    )


def _gemini_response_from_guard_execution(
    execution: LlmGuardExecution,
    *,
    model: str,
) -> GeminiResponse:
    return _gemini_response_from_dict(
        guard_response_dict(
            execution,
            model=model,
            message={
                "role": "model",
                "parts": [
                    {
                        "text": "The response was blocked by an LLM guard.",
                    }
                ],
            },
            stop_reason="SAFETY",
        )
    )


@activity.defn
async def call_gemini(request: GeminiRequest) -> GeminiResponse:
    api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        error = ApplicationError(
            "GEMINI_API_KEY or GOOGLE_API_KEY must be set to call Gemini",
            type="MissingApiKey",
            non_retryable=True,
        )
        await AgentStreamWriter.for_provider(
            stream_id=request.stream_id,
            provider="gemini",
            attempt=request.stream_attempt,
            agent=request.stream_agent,
        ).agent_failed(
            sequence=request.stream_sequence,
            model=request.model,
            error=error,
        )
        raise error

    config = _gemini_generate_content_config(request)
    contents = [
        genai_types.Content.model_validate(content)
        for content in request.chat_history
    ]

    try:
        client = _gemini_client(api_key).aio
        if _should_count_request_tokens(request):
            await _raise_if_gemini_context_too_large(
                client,
                request,
                contents,
                config,
            )
        state = await _stream_gemini_message(
            client,
            model=request.model,
            contents=contents,
            config=config,
            stream_id=request.stream_id,
            stream_sequence=request.stream_sequence,
            stream_attempt=request.stream_attempt,
            stream_agent=request.stream_agent,
        )
    except genai_errors.APIError as err:
        await AgentStreamWriter.for_provider(
            stream_id=request.stream_id,
            provider="gemini",
            attempt=request.stream_attempt,
            agent=request.stream_agent,
        ).agent_failed(
            sequence=request.stream_sequence,
            model=request.model,
            error=err,
        )
        if _google_error_is_context_window_exceeded(err):
            raise ApplicationError(
                str(err),
                type=CONTEXT_WINDOW_EXCEEDED_ERROR_TYPE,
                non_retryable=True,
            ) from err
        if non_retryable_http_status(
            cast(int | None, getattr(err, "code", None))
        ):
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
                provider="gemini",
                attempt=request.stream_attempt,
                agent=request.stream_agent,
            ).agent_failed(
                sequence=request.stream_sequence,
                model=request.model,
                error=err,
            )
        raise

    return GeminiResponse(
        id=state.response_id or f"gemini:{request.stream_sequence or 0}",
        model=state.model or request.model,
        message=state.message(),
        stop_reason=state.stop_reason,
        stop_sequence=None,
        usage=state.usage or {},
        stop_details=state.stop_details,
    )


def _gemini_http_options() -> genai_types.HttpOptions:
    return genai_types.HttpOptions(
        retry_options=genai_types.HttpRetryOptions(attempts=1),
    )


def _gemini_generate_content_config(
    request: GeminiRequest,
) -> genai_types.GenerateContentConfig:
    tools = [genai_types.Tool.model_validate(tool) for tool in request.tools]
    tool_config = (
        genai_types.ToolConfig(
            function_calling_config=genai_types.FunctionCallingConfig(
                mode=genai_types.FunctionCallingConfigMode.AUTO,
            )
        )
        if tools
        else None
    )
    return genai_types.GenerateContentConfig(
        system_instruction=request.system_prompt or None,
        max_output_tokens=request.max_tokens,
        tools=tools or None,
        tool_config=tool_config,
        # Keep the harness responsible for executing tools and feeding results
        # back into the workflow history.
        automatic_function_calling=genai_types.AutomaticFunctionCallingConfig(
            disable=True,
        ),
        thinking_config=(
            genai_types.ThinkingConfig.model_validate(request.thinking_config)
            if request.thinking_config is not None
            else None
        ),
    )


def _should_count_request_tokens(request: GeminiRequest) -> bool:
    return bool(
        request.context_token_limit is not None
        and request.stream_attempt is not None
        and request.stream_attempt > 1
    )


async def _raise_if_gemini_context_too_large(
    client: Any,
    request: GeminiRequest,
    contents: list[genai_types.Content],
    config: genai_types.GenerateContentConfig,
) -> None:
    count_config = genai_types.CountTokensConfig(
        system_instruction=config.system_instruction,
        tools=config.tools,
    )
    token_count = await client.models.count_tokens(
        model=request.model,
        contents=contents,
        config=count_config,
    )
    input_tokens = int(token_count.total_tokens or 0)
    if not counted_tokens_exceed_context(
        input_tokens=input_tokens,
        max_output_tokens=request.max_tokens,
        context_token_limit=request.context_token_limit,
    ):
        return

    raise ApplicationError(
        counted_context_window_message(
            provider="Gemini",
            input_tokens=input_tokens,
            max_output_tokens=request.max_tokens,
            context_token_limit=cast(int, request.context_token_limit),
        ),
        type=CONTEXT_WINDOW_EXCEEDED_ERROR_TYPE,
        non_retryable=True,
    )


async def _stream_gemini_message(
    client: Any,
    *,
    model: str,
    contents: list[genai_types.Content],
    config: genai_types.GenerateContentConfig,
    stream_id: str | None,
    stream_sequence: int | None,
    stream_attempt: int | None,
    stream_agent: dict[str, str | None] | None,
) -> _GeminiStreamState:
    stream = AgentStreamWriter.for_provider(
        stream_id=stream_id,
        provider="gemini",
        attempt=stream_attempt,
        agent=stream_agent,
    )
    stream_state = _GeminiStreamState(sequence=stream_sequence, model=model)
    heartbeat_state = _GeminiHeartbeatState(sequence=stream_sequence)
    activity.heartbeat(heartbeat_state.payload("starting"))
    heartbeat_task = asyncio.create_task(_heartbeat_gemini_stream(heartbeat_state))

    await stream.agent_started(sequence=stream_sequence, model=model)

    try:
        response_stream = await client.models.generate_content_stream(
            model=model,
            contents=contents,
            config=config,
        )
        heartbeat_state.phase = "streaming"
        cancel_task = asyncio.create_task(activity.wait_for_cancelled())
        chunk_iterator = response_stream.__aiter__()
        try:
            while True:
                next_chunk_task = asyncio.create_task(anext(chunk_iterator))
                done, _pending = await asyncio.wait(
                    {next_chunk_task, cancel_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )

                if cancel_task in done:
                    heartbeat_state.phase = "cancelled"
                    activity.heartbeat(heartbeat_state.payload("cancelled"))
                    next_chunk_task.cancel()
                    with suppress(asyncio.CancelledError):
                        await next_chunk_task
                    await stream.agent_cancelled(sequence=stream_sequence)
                    raise asyncio.CancelledError()

                try:
                    chunk = next_chunk_task.result()
                except StopAsyncIteration:
                    break

                heartbeat_state.events += 1
                stream_state.events += 1
                activity.heartbeat(heartbeat_state.payload("event"))
                await _record_gemini_chunk(
                    stream=stream,
                    chunk=chunk,
                    state=stream_state,
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
        model=stream_state.model or model,
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


async def _heartbeat_gemini_stream(state: _GeminiHeartbeatState) -> None:
    while True:
        await asyncio.sleep(GEMINI_HEARTBEAT_INTERVAL_SECONDS)
        activity.heartbeat(state.payload("timer"))


async def _record_gemini_chunk(
    *,
    stream: AgentStreamWriter,
    chunk: genai_types.GenerateContentResponse,
    state: _GeminiStreamState,
) -> None:
    state.response_id = chunk.response_id or state.response_id
    state.model = chunk.model_version or state.model
    if chunk.usage_metadata is not None:
        state.usage = _model_to_dict(chunk.usage_metadata)

    if chunk.prompt_feedback is not None:
        state.stop_details = _model_to_dict(chunk.prompt_feedback)

    if not chunk.candidates:
        return

    candidate = chunk.candidates[0]
    if candidate.finish_reason is not None:
        state.stop_reason = _enum_value(candidate.finish_reason)

    if candidate.safety_ratings:
        stop_details = dict(state.stop_details or {})
        stop_details["safety_ratings"] = [
            _model_to_dict(rating) for rating in candidate.safety_ratings
        ]
        state.stop_details = stop_details

    if candidate.content is None or not candidate.content.parts:
        return

    for part in candidate.content.parts:
        await _record_gemini_part(stream=stream, part=part, state=state)


async def _record_gemini_part(
    *,
    stream: AgentStreamWriter,
    part: genai_types.Part,
    state: _GeminiStreamState,
) -> None:
    if part.text:
        if part.thought is True:
            if not state.thought_parts:
                await stream.thinking_started(
                    sequence=state.sequence,
                    content_block_index=0,
                )
            state.thought_parts.append(part.text)
            await stream.thinking_delta(
                sequence=state.sequence,
                thinking=part.text,
            )
            return

        state.text_parts.append(part.text)
        await stream.text_delta(sequence=state.sequence, text=part.text)
        return

    if part.function_call is not None:
        normalized = _normalize_gemini_function_call(
            _model_to_dict(part.function_call)
        )
        state.function_calls.append(normalized)
        function_index = len(state.function_calls) - 1
        input_value = _copy_mapping(normalized.get("args", {}))
        await stream.tool_input_started(
            sequence=state.sequence,
            content_block_index=function_index,
            tool_use_id=_gemini_tool_use_id(normalized, function_index),
            tool_name=normalized.get("name"),
            tool_type="function_call",
        )
        await stream.tool_input_completed(
            sequence=state.sequence,
            content_block_index=function_index,
            tool_use_id=_gemini_tool_use_id(normalized, function_index),
            tool_name=normalized.get("name"),
            tool_type="function_call",
            input=input_value,
            input_preview=_json_preview(input_value),
        )


def _google_error_is_context_window_exceeded(err: Any) -> bool:
    return status_error_is_context_window_exceeded(
        cast(int | None, getattr(err, "code", None)),
        str(err),
    )


def _agent_messages_to_gemini_contents(
    messages: list[AgentMessage],
) -> list[dict[str, Any]]:
    tool_names_by_id: dict[str, str] = {}
    contents: list[dict[str, Any]] = []
    for message in messages:
        content = _agent_message_to_gemini_content(message, tool_names_by_id)
        if content["parts"]:
            contents.append(content)
    return contents


def _gemini_contents_to_agent_messages(
    contents: list[dict[str, Any]],
) -> list[AgentMessage]:
    return [_gemini_content_to_agent_message(content) for content in contents]


def _agent_message_to_gemini_content(
    message: AgentMessage,
    tool_names_by_id: dict[str, str],
) -> dict[str, Any]:
    normalized = normalize_message(message)
    role = "model" if normalized["role"] == "assistant" else "user"
    content = normalized["content"]
    if isinstance(content, str):
        parts = [{"text": content}]
    else:
        parts = [
            part
            for block in content
            for part in _agent_block_to_gemini_parts(block, tool_names_by_id)
        ]
    return {"role": role, "parts": parts}


def _agent_block_to_gemini_parts(
    block: AgentBlock,
    tool_names_by_id: dict[str, str],
) -> list[dict[str, Any]]:
    block_type = block.get("type")
    if block_type == "text":
        return [{"text": str(block.get("text") or "")}]
    if block_type == CONTEXT_COMPACTION_MARKER:
        return [{"text": str(block.get("text") or CONTEXT_COMPACTION_MARKER_TEXT)}]
    if block_type == "tool_use":
        function_call = _agent_tool_use_to_gemini_function_call(cast(ToolUseBlock, block))
        tool_id = _gemini_function_call_id(function_call)
        tool_name = function_call.get("name")
        if tool_id and isinstance(tool_name, str):
            tool_names_by_id[tool_id] = tool_name
        return [{"function_call": function_call}]
    if block_type == "tool_result":
        return [_agent_tool_result_to_gemini_function_response(block, tool_names_by_id)]
    if block_type == "provider" and block.get("provider") == "gemini":
        data = block.get("data")
        return [_copy_mapping(data)] if isinstance(data, dict) else []
    if block_type == "refusal":
        refusal = block.get("refusal")
        return [{"text": refusal if isinstance(refusal, str) else ""}]
    return []


def _agent_tool_use_to_gemini_function_call(block: ToolUseBlock) -> dict[str, Any]:
    provider_data = _provider_data(block)
    if provider_data is not None:
        function_call = _copy_mapping(provider_data.get("function_call", provider_data))
    else:
        function_call = {}
    function_call["name"] = cast(str, block["name"])
    function_call["args"] = _copy_mapping(block.get("input", {}))
    function_call["id"] = cast(str, block["id"])
    return function_call


def _agent_tool_result_to_gemini_function_response(
    block: AgentBlock,
    tool_names_by_id: dict[str, str],
) -> dict[str, Any]:
    provider_data = _provider_data(block)
    if provider_data is not None:
        response = _copy_mapping(provider_data.get("function_response", provider_data))
    else:
        tool_use_id = cast(str, block.get("tool_use_id") or "")
        response = {
            "name": tool_names_by_id.get(tool_use_id, tool_use_id),
            "id": tool_use_id,
        }

    content = block.get("content", "")
    response["response"] = _tool_result_response_payload(
        content,
        bool(block.get("is_error", False)),
    )
    return {"function_response": response}


def _tool_result_response_payload(content: Any, is_error: bool) -> dict[str, Any]:
    payload: Any = content
    if isinstance(content, str):
        with suppress(json.JSONDecodeError):
            payload = json.loads(content)
    result = {"result": payload}
    if is_error:
        result["is_error"] = True
    return result


def _gemini_content_to_agent_message(content: dict[str, Any]) -> AgentMessage:
    role = content.get("role")
    agent_role: Literal["user", "assistant"] = (
        "assistant" if role in ("model", "assistant") else "user"
    )
    parts = content.get("parts")
    if isinstance(parts, str):
        return agent_message(agent_role, parts)
    if not isinstance(parts, list):
        return agent_message(agent_role, "")
    return agent_message(
        agent_role,
        [_gemini_part_to_agent_block(part, index) for index, part in enumerate(parts)],
    )


def _gemini_part_to_agent_block(part: Any, index: int) -> AgentBlock:
    part_dict = _model_to_dict(part)
    if part_dict.get("thought") is True:
        return provider_block(
            provider="gemini",
            provider_type="thought",
            data=part_dict,
        )

    text = part_dict.get("text")
    if isinstance(text, str):
        return text_block(text)

    function_call = part_dict.get("function_call") or part_dict.get("functionCall")
    if isinstance(function_call, dict):
        normalized = _normalize_gemini_function_call(function_call)
        return tool_use_block(
            tool_use_id=_gemini_tool_use_id(normalized, index),
            name=cast(str, normalized.get("name") or ""),
            input=_copy_mapping(normalized.get("args", {})),
            provider=_provider_metadata("function_call", {"function_call": normalized}),
        )

    function_response = part_dict.get("function_response") or part_dict.get("functionResponse")
    if isinstance(function_response, dict):
        response = _normalize_gemini_function_response(function_response)
        payload = response.get("response", {})
        return {
            "type": "tool_result",
            "tool_use_id": _gemini_tool_use_id(response, index),
            "content": json.dumps(payload),
            "is_error": bool(isinstance(payload, dict) and payload.get("is_error")),
            "provider": _provider_metadata(
                "function_response",
                {"function_response": response},
            ),
        }

    return provider_block(
        provider="gemini",
        provider_type=str(next(iter(part_dict.keys()), "unknown")),
        data=part_dict,
    )


def _gemini_response_message_from_value(value: Any) -> dict[str, Any]:
    message = _copy_mapping(value)
    if "parts" in message:
        return message

    role = message.get("role")
    agent_role: Literal["user", "assistant"] = (
        "user" if role == "user" else "assistant"
    )
    return _agent_message_to_gemini_content(
        {"role": agent_role, "content": message.get("content", "")},
        {},
    )


def _normalize_gemini_function_call(value: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "name": value.get("name"),
        "args": _copy_mapping(value.get("args", {})),
        "id": value.get("id"),
    }


def _normalize_gemini_function_response(value: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "name": value.get("name"),
        "response": _copy_mapping(value.get("response", {})),
        "id": value.get("id"),
    }


def _gemini_function_call_id(function_call: Mapping[str, Any]) -> str | None:
    function_id = function_call.get("id")
    return function_id if isinstance(function_id, str) and function_id else None


def _gemini_tool_use_id(value: Mapping[str, Any], index: int) -> str:
    function_id = _gemini_function_call_id(value)
    if function_id:
        return function_id
    name = value.get("name")
    return f"gemini-tool-{index}-{name or 'unknown'}"


def _provider_metadata(
    provider_type: str,
    data: dict[str, Any],
) -> dict[str, Any]:
    return _shared_provider_metadata(
        provider="gemini",
        provider_type=provider_type,
        data=data,
    )


def _provider_data(block: Mapping[str, Any]) -> dict[str, Any] | None:
    return _shared_provider_data(block, provider="gemini")


def _gemini_tools_from_agent_tools(tools: list[dict[str, Any]]) -> list[dict[str, Any]]:
    declarations: list[dict[str, Any]] = []
    for tool in tools:
        name = tool.get("name")
        if not isinstance(name, str) or not name:
            continue
        declaration: dict[str, Any] = {"name": name}
        description = tool.get("description")
        if isinstance(description, str):
            declaration["description"] = description
        input_schema = tool.get("input_schema")
        if isinstance(input_schema, dict):
            declaration["parameters_json_schema"] = _schema_for_gemini(input_schema)
        declarations.append(declaration)

    if not declarations:
        return []
    return [{"function_declarations": declarations}]


def _schema_for_gemini(schema: Mapping[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in schema.items():
        if key in {"title", "default", "$defs", "additionalProperties"}:
            continue
        if key == "anyOf" and isinstance(value, list):
            non_null = [
                item
                for item in value
                if not (isinstance(item, dict) and item.get("type") == "null")
            ]
            if len(non_null) == 1 and isinstance(non_null[0], dict):
                result.update(_schema_for_gemini(non_null[0]))
            continue
        if key == "properties" and isinstance(value, dict):
            result[key] = {
                str(prop_name): _schema_for_gemini(prop_schema)
                for prop_name, prop_schema in value.items()
                if isinstance(prop_schema, dict)
            }
            continue
        if key == "items" and isinstance(value, dict):
            result[key] = _schema_for_gemini(value)
            continue
        result[key] = value
    return result


def _thinking_config_to_gemini(
    thinking: GeminiThinkingConfig | None,
) -> dict[str, Any] | None:
    if thinking is None:
        return None
    config: dict[str, Any] = {"include_thoughts": thinking.include_thoughts}
    if thinking.thinking_budget is not None:
        config["thinking_budget"] = thinking.thinking_budget
    if thinking.thinking_level is not None:
        config["thinking_level"] = thinking.thinking_level
    return config


def _response_with_visible_refusal(response: GeminiResponse) -> GeminiResponse:
    if not _needs_refusal_fallback(response):
        return response
    return replace(
        response,
        message={
            "role": response.message.get("role", "model"),
            "parts": [{"text": _refusal_fallback_text(response.stop_details)}],
        },
    )


def _needs_refusal_fallback(response: GeminiResponse) -> bool:
    return needs_refusal_fallback(
        stop_reason=response.stop_reason,
        refusal_stop_reasons=GEMINI_REFUSAL_STOP_REASONS,
        guard_action=response.guard_action,
        response_text=message_text(_gemini_content_to_agent_message(response.message)),
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
    block_reason = stop_details.get("block_reason") or stop_details.get("blockReason")
    if isinstance(block_reason, str) and block_reason:
        return f"Block reason: {block_reason}."
    return ""


def _enum_value(value: Any) -> str:
    raw_value = getattr(value, "value", value)
    return raw_value if isinstance(raw_value, str) else str(raw_value)
