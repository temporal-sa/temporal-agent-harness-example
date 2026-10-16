from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Protocol

from temporalio import workflow

from agent_harness.activity_options import ActivityOptions
from agent_harness.llm_guards import LlmGuardExecution
from agent_harness.messages import AgentMessage

ProviderActivity = Callable[..., Any]
ProviderStopReason = str
CONTEXT_WINDOW_EXCEEDED_ERROR_TYPE = "ContextWindowExceeded"


@dataclass
class ProviderRequest:
    system_prompt: str
    model: str
    max_tokens: int
    tools: list[dict]
    chat_history: list[dict]
    context_token_limit: int | None = None
    stream_id: str | None = None
    stream_sequence: int | None = None
    stream_attempt: int | None = None
    stream_agent: dict[str, str | None] | None = None


@dataclass
class ProviderResponse:
    id: str
    model: str
    message: dict
    stop_reason: ProviderStopReason | None
    stop_sequence: str | None
    usage: dict
    guard_action: str | None = None
    guard_reason: str | None = None
    stop_details: dict | None = None


class AgentProvider(Protocol):
    """Provider-specific adapter used by the provider-neutral agent loop."""

    @property
    def name(self) -> str:
        pass

    @property
    def activity(self) -> ProviderActivity:
        pass

    @property
    def activity_options(self) -> ActivityOptions:
        pass

    def estimate_request_tokens(
        self,
        *,
        system_prompt: str,
        tools: list[dict[str, Any]],
    ) -> int:
        pass

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
    ) -> ProviderRequest:
        pass

    def request_chat_history(self, request: ProviderRequest) -> list[AgentMessage]:
        pass

    def replace_request_chat_history(
        self,
        request: ProviderRequest,
        chat_history: list[AgentMessage],
    ) -> ProviderRequest:
        pass

    def replace_request_stream_attempt(
        self,
        request: ProviderRequest,
        stream_attempt: int | None,
    ) -> ProviderRequest:
        pass

    def request_to_dict(self, request: ProviderRequest) -> dict[str, Any]:
        pass

    def request_from_dict(self, request: dict[str, Any]) -> ProviderRequest:
        pass

    def response_to_dict(self, response: ProviderResponse) -> dict[str, Any]:
        pass

    def response_from_dict(self, response: dict[str, Any]) -> ProviderResponse:
        pass

    def response_from_guard_execution(
        self,
        execution: LlmGuardExecution,
        *,
        model: str,
    ) -> ProviderResponse:
        pass

    def response_with_visible_refusal(
        self,
        response: ProviderResponse,
    ) -> ProviderResponse:
        pass

    def response_message(self, response: ProviderResponse) -> AgentMessage:
        pass


def start_provider_activity(
    provider: AgentProvider,
    request: ProviderRequest,
) -> workflow.ActivityHandle[ProviderResponse]:
    return workflow.start_activity(
        provider.activity,
        request,
        summary=provider.name,
        **provider.activity_options.to_execute_activity_kwargs(),
    )
