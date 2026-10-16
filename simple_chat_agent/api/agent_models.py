"""The coding agent's model catalog for the API (PRD S6): static, no provider calls, no keys.

Mirrors ``simple_chat_agent.agent.model_registry`` (``DEFAULT_ALLOWLIST`` plus the
``AGENT_MODELS_<PROVIDER>`` overrides) without importing it: that module is worker-only and
pulls in the provider model builders. ``tests/test_agent_routes.py`` pins the two lists
together. Token limits come from ``simple_chat_agent.agent.context.MODEL_LIMITS``, which
imports cleanly without keys.

The worker re-checks every choice against the models it can actually call, so this list only
decides what the picker offers and what ``POST sessions`` accepts.
"""

from __future__ import annotations

import os
from typing import Any

from simple_chat_agent.agent.context import model_limits
from simple_chat_agent.agent.contracts import ModelChoice, Posture, Provider

# Keep in step with model_registry.DEFAULT_ALLOWLIST (first entry is the provider default).
DEFAULT_ALLOWLIST: dict[Provider, tuple[str, ...]] = {
    "anthropic": ("claude-sonnet-5-5", "claude-haiku-5-5", "claude-opus-5-5"),
    "openai": ("gpt-5.5", "gpt-5.4-mini"),
    "google": ("gemini-3.5-flash", "gemini-3.1-pro-preview"),
}

PROVIDER_LABELS: dict[Provider, str] = {"anthropic": "Anthropic", "openai": "OpenAI", "google": "Google"}

DISPLAY_NAMES: dict[str, str] = {
    "claude-sonnet-5-5": "Claude Sonnet 5.5",
    "claude-haiku-5-5": "Claude Haiku 5.5",
    "claude-opus-5-5": "Claude Opus 5.5",
    "gpt-5.5": "GPT-5.5",
    "gpt-5.4-mini": "GPT-5.4 mini",
    "gemini-3.5-flash": "Gemini 3.5 Flash",
    "gemini-3.1-pro-preview": "Gemini 3.1 Pro (preview)",
}

# "fast" models are the ones subagents use (runtime.FAST_MODELS); everything else is "mid"
# unless it is a provider's top model.
TIERS: dict[str, str] = {
    "claude-haiku-5-5": "fast",
    "gpt-5.4-mini": "fast",
    "gemini-3.5-flash": "fast",
    "claude-opus-5-5": "top",
    "gemini-3.1-pro-preview": "top",
}

EFFORT_OPTIONS: tuple[str, ...] = ("low", "medium", "high")
DEFAULT_EFFORT = "medium"  # model_registry.settings_for: ``choice.effort or "medium"``

POSTURES: dict[Posture, tuple[str, str]] = {
    "ask_everything": ("Ask for everything", "Every tool call waits for you."),
    "trust_safe": ("Trust safe tools", "Read-only tools run; everything else waits for you."),
    "auto": ("Auto mode", "Read-only tools run; a judge decides the rest and asks you when unsure."),
    "never_ask": ("Never ask", "Every call runs without asking. Local development only."),
}
DEFAULT_POSTURE: Posture = "trust_safe"

DEMOS: dict[str, str] = {"refund_service": "Refund service (planted bug)"}


def allowlist() -> dict[Provider, tuple[str, ...]]:
    """Allowed model ids per provider, overrides applied. The API holds no LLM keys, so every
    provider is listed (``model_registry.allowlist(require_keys=False)``)."""
    result: dict[Provider, tuple[str, ...]] = {}
    for provider, defaults in DEFAULT_ALLOWLIST.items():
        override = os.environ.get(f"AGENT_MODELS_{provider.upper()}", "")
        result[provider] = tuple(n.strip() for n in override.split(",") if n.strip()) or defaults
    return result


def is_allowed(choice: ModelChoice) -> bool:
    return choice.model in allowlist().get(choice.provider, ())


def default_model() -> ModelChoice:
    lists = allowlist()
    provider: Provider = "anthropic" if lists.get("anthropic") else next(iter(lists))
    return ModelChoice(provider=provider, model=lists[provider][0])


def postures_for(environment: str) -> list[Posture]:
    """``never_ask`` is offered only outside the catalog."""
    return [p for p in POSTURES if p != "never_ask" or environment == "local"]


def _model_entry(provider: Provider, model: str) -> dict[str, Any]:
    max_input, max_output = model_limits(f"{provider}:{model}")
    return {
        "provider": provider,
        "id": model,
        "registry_key": f"{provider}:{model}",
        "display_name": DISPLAY_NAMES.get(model, model),
        "tier": TIERS.get(model, "mid"),
        "max_input_tokens": max_input,
        "max_output_tokens": max_output,
        "effort_options": list(EFFORT_OPTIONS),
        "default_effort": DEFAULT_EFFORT,
    }


def catalog(*, environment: str, research_tools: list[str], judge: str) -> dict[str, Any]:
    """The ``GET /api/agent/config`` body."""
    lists = allowlist()
    providers = [
        {
            "provider": provider,
            "label": PROVIDER_LABELS[provider],
            "default_model": models[0],
            "models": [_model_entry(provider, m) for m in models],
        }
        for provider, models in lists.items()
    ]
    default = default_model()
    return {
        "environment": environment,
        "default_model": default.model_dump(mode="json", exclude_none=True),
        "providers": providers,
        "models": [m for p in providers for m in p["models"]],
        "effort_options": list(EFFORT_OPTIONS),
        "default_effort": DEFAULT_EFFORT,
        "postures": [
            {"id": p, "label": POSTURES[p][0], "description": POSTURES[p][1]} for p in postures_for(environment)
        ],
        "default_posture": DEFAULT_POSTURE,
        "research_tools": research_tools,
        "judge": judge,
        "demos": [{"id": k, "label": v} for k, v in DEMOS.items()],
        "model_source": "allowlist",
    }
