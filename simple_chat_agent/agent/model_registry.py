"""Model registry for the coding agent: a static allowlist per provider.

Worker-only. Building a model constructs a provider client, which reads API keys from the
environment, so the API process never imports this module. Models are registered with the
TemporalAgent at worker start and selected per run by registry key ``provider:model``.
Provider SDK retries are off everywhere: every retry is a visible Temporal activity attempt.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass

from pydantic_ai.models import Model
from pydantic_ai.settings import ModelSettings

from simple_chat_agent.agent.contracts import ModelChoice, Provider
from simple_chat_agent.agent.harness_ext import HeartbeatingModel

log = logging.getLogger(__name__)

# Default allowlist (PRD S6). Override per environment with AGENT_MODELS_<PROVIDER>, a
# comma-separated list whose first entry is that provider's default.
DEFAULT_ALLOWLIST: dict[Provider, tuple[str, ...]] = {
    "anthropic": ("claude-sonnet-5-5", "claude-haiku-5-5", "claude-opus-5-5"),
    "openai": ("gpt-5.5", "gpt-5.4-mini"),
    "google": ("gemini-3.5-flash", "gemini-3.1-pro-preview"),
}

_KEY_ENV: dict[Provider, tuple[str, ...]] = {
    "anthropic": ("ANTHROPIC_API_KEY",),
    "openai": ("OPENAI_API_KEY",),
    "google": ("GEMINI_API_KEY", "GOOGLE_API_KEY"),
}


@dataclass(frozen=True)
class ProviderModels:
    provider: Provider
    models: tuple[str, ...]

    @property
    def default(self) -> str:
        return self.models[0]


def allowlist() -> dict[Provider, ProviderModels]:
    """The allowed models per provider, for providers whose key is configured."""
    result: dict[Provider, ProviderModels] = {}
    for provider, defaults in DEFAULT_ALLOWLIST.items():
        if not any(os.environ.get(name) for name in _KEY_ENV[provider]):
            continue
        override = os.environ.get(f"AGENT_MODELS_{provider.upper()}", "")
        names = tuple(n.strip() for n in override.split(",") if n.strip()) or defaults
        result[provider] = ProviderModels(provider, names)
    return result


def is_allowed(choice: ModelChoice) -> bool:
    entry = allowlist().get(choice.provider)
    return entry is not None and choice.model in entry.models


def _build(provider: Provider, name: str) -> Model:
    if provider == "anthropic":
        from anthropic import AsyncAnthropic
        from pydantic_ai.models.anthropic import AnthropicModel
        from pydantic_ai.providers.anthropic import AnthropicProvider

        client = AsyncAnthropic(max_retries=0)
        return AnthropicModel(name, provider=AnthropicProvider(anthropic_client=client))
    if provider == "openai":
        from openai import AsyncOpenAI
        from pydantic_ai.models.openai import OpenAIResponsesModel
        from pydantic_ai.providers.openai import OpenAIProvider

        client = AsyncOpenAI(max_retries=0)
        return OpenAIResponsesModel(name, provider=OpenAIProvider(openai_client=client))
    if provider == "google":
        from google import genai
        from google.genai.types import HttpOptions, HttpRetryOptions
        from pydantic_ai.models.google import GoogleModel
        from pydantic_ai.providers.google import GoogleProvider

        key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
        client = genai.Client(
            api_key=key,
            http_options=HttpOptions(retry_options=HttpRetryOptions(attempts=1)),
        )
        return GoogleModel(name, provider=GoogleProvider(client=client))
    raise ValueError(f"unknown provider {provider!r}")


def build_registry() -> dict[str, Model]:
    """Every allowed model, wrapped so its activity heartbeats, keyed ``provider:model``."""
    registry: dict[str, Model] = {}
    for provider, entry in allowlist().items():
        for name in entry.models:
            registry[f"{provider}:{name}"] = HeartbeatingModel(_build(provider, name))
    if not registry:
        raise RuntimeError(
            "No LLM provider keys are configured. Set ANTHROPIC_API_KEY, OPENAI_API_KEY or "
            "GEMINI_API_KEY (locally: scripts/dev-secrets.sh)."
        )
    log.info("model registry: %s", ", ".join(sorted(registry)))
    return registry


def settings_for(choice: ModelChoice, *, session_id: str) -> ModelSettings:
    """Per-provider settings: thinking where supported, and prompt caching.

    Anthropic caching is explicit (cache breakpoints on instructions, tools and the moving
    message tail). OpenAI and Gemini cache implicitly; OpenAI gets a per-session cache key so
    repeated prefixes from one session route to the same cache.
    """
    effort = choice.effort or "medium"
    if choice.provider == "anthropic":
        from pydantic_ai.models.anthropic import AnthropicModelSettings

        # Claude 5.x requires adaptive thinking with an effort level; Pydantic AI 2.14.1's
        # profiles predate these models, so the unified `thinking` setting would send the
        # rejected `enabled` form.
        return AnthropicModelSettings(
            max_tokens=16_000,
            anthropic_thinking={"type": "adaptive"},
            anthropic_effort=effort,
            anthropic_cache_instructions=True,
            anthropic_cache_tool_definitions=True,
            anthropic_cache=True,
        )
    if choice.provider == "openai":
        from pydantic_ai.models.openai import OpenAIResponsesModelSettings

        return OpenAIResponsesModelSettings(
            max_tokens=16_000,
            thinking=effort,
            openai_reasoning_summary="auto",
            openai_prompt_cache_key=session_id or "coding-agent",
        )
    return ModelSettings(max_tokens=16_000, thinking=effort)
