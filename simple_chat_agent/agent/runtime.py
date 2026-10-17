"""The durable Pydantic AI agent shared by every CodingAgent run.

Built once at import, as Pydantic AI's Temporal integration requires: its activities (model
request and stream) are registered on the worker through ``AgentPlugin(CODING_AGENT)``. Each
run supplies its own runner (through deps), tools (per-run toolset) and model (registry key).
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from pydantic_ai import Agent
from pydantic_ai.messages import NativeToolReturnPart
from temporalio.common import RetryPolicy
from temporalio.workflow import ActivityConfig

from temporal_agent_harness.ai_sdks.pydantic_ai_harness import HarnessDeps

from simple_chat_agent.agent import harness_ext
from simple_chat_agent.agent.contracts import MAX_WEB_SOURCES, ModelChoice, WebSource
from simple_chat_agent.agent.model_registry import build_registry, settings_for
from simple_chat_agent.agent.tools.research import RESEARCH_TOOLS  # noqa: F401 - re-exported

AGENT_NAME = "coding-agent"


class CodingDeps(HarnessDeps):
    """Run deps: the harness runner and stream context, plus who the run is for. The ids
    travel into the model activity, where spend caps are checked and recorded."""

    session_id: str = ""
    owner_id: str = ""


# The cheaper model of each provider, used for subagents.
FAST_MODELS = {
    "anthropic": "claude-haiku-5-5",
    "openai": "gpt-5.4-mini",
    "google": "gemini-3.5-flash",
}

# PRD A19 and §5 rule 8: heartbeats detect a dead worker in ~10 s; at most 4 attempts.
MODEL_ACTIVITY = ActivityConfig(
    start_to_close_timeout=timedelta(minutes=15),
    schedule_to_close_timeout=timedelta(minutes=30),
    heartbeat_timeout=timedelta(seconds=10),
    retry_policy=RetryPolicy(
        initial_interval=timedelta(seconds=2),
        backoff_coefficient=2.0,
        maximum_interval=timedelta(seconds=60),
        maximum_attempts=4,
        non_retryable_error_types=["ProviderRejected"],
    ),
)

MODELS = build_registry()
ALLOWED_KEYS = frozenset(MODELS)
_DEFAULT_PROVIDER, _, _DEFAULT_NAME = next(iter(MODELS)).partition(":")
DEFAULT_MODEL = ModelChoice(provider=_DEFAULT_PROVIDER, model=_DEFAULT_NAME)  # type: ignore[arg-type]

CODING_AGENT = harness_ext.QuietTemporalAgent(
    Agent(
        MODELS[next(iter(MODELS))],
        # Instructions are passed per run: the coding agent and its subagents share this
        # durable agent but not its prompt.
        deps_type=CodingDeps,
    ),
    name=AGENT_NAME,
    models=MODELS,
    event_stream_handler=harness_ext.coding_event_stream_handler,
    model_activity_config=MODEL_ACTIVITY,
)


def fast_model_for(choice: ModelChoice) -> ModelChoice:
    """The provider's fast model if this worker can call it, else the parent's model."""
    fast = ModelChoice(provider=choice.provider, model=FAST_MODELS[choice.provider], effort="low")
    return fast if is_allowed_model(fast) else choice


def is_allowed_model(choice: ModelChoice) -> bool:
    return choice.registry_key in ALLOWED_KEYS


def model_settings(choice: ModelChoice, *, session_id: str, web_search: bool = False):
    return settings_for(choice, session_id=session_id, web_search=web_search)


# -- Provider web search (P4 4.5) -----------------------------------------------------------

# Providers whose models take Pydantic AI's built-in WebSearchTool (Anthropic, OpenAI
# Responses, Gemini). A session on another provider gets no search even with the setting on.
WEB_SEARCH_PROVIDERS = frozenset({"anthropic", "openai", "google"})


def web_search_capabilities(choice: ModelChoice, *, enabled: bool) -> list[Any]:
    """The per-run capabilities that turn on the provider's built-in web search: none when
    the setting is off, so no search can happen."""
    if not enabled or choice.provider not in WEB_SEARCH_PROVIDERS:
        return []
    from pydantic_ai.capabilities import WebSearch

    return [WebSearch()]


def _source_items(content: Any) -> list[dict[str, Any]]:
    """Every dict with a URL in a web-search result, whatever the provider's shape: Anthropic
    (a list of results with url/title), OpenAI ({"sources": [{"url": ...}]}), Gemini (a list of
    grounding chunks with uri/title/domain)."""
    found: list[dict[str, Any]] = []

    def walk(value: Any, depth: int = 0) -> None:
        if depth > 4:
            return
        if isinstance(value, dict):
            if isinstance(value.get("url") or value.get("uri"), str):
                found.append(value)
                return
            for v in value.values():
                walk(v, depth + 1)
        elif isinstance(value, list):
            for v in value:
                walk(v, depth + 1)

    walk(content)
    return found


def web_sources(messages: list[Any]) -> list[WebSource]:
    """The web pages the provider's search returned in ``messages`` (Pydantic AI
    ``NativeToolReturnPart`` with tool name ``web_search``), in order, deduplicated by URL, at
    most MAX_WEB_SOURCES. Only http(s) URLs; titles fall back to the domain or the URL."""
    sources: list[WebSource] = []
    seen: set[str] = set()
    for message in messages:
        for part in getattr(message, "parts", []):
            if not isinstance(part, NativeToolReturnPart) or part.tool_name != "web_search":
                continue
            for item in _source_items(getattr(part, "content", None)):
                url = str(item.get("url") or item.get("uri") or "").strip()
                if not url.lower().startswith(("https://", "http://")) or url in seen or len(url) > 2000:
                    continue
                seen.add(url)
                title = str(item.get("title") or item.get("domain") or url).strip()[:300]
                sources.append(WebSource(title=title, url=url))
                if len(sources) >= MAX_WEB_SOURCES:
                    return sources
    return sources
