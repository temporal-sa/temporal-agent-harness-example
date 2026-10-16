"""The durable Pydantic AI agent shared by every CodingAgent run.

Built once at import, as Pydantic AI's Temporal integration requires: its activities (model
request and stream) are registered on the worker through ``AgentPlugin(CODING_AGENT)``. Each
run supplies its own runner (through deps), tools (per-run toolset) and model (registry key).
"""

from __future__ import annotations

from datetime import timedelta

from pydantic_ai import Agent
from temporalio.common import RetryPolicy
from temporalio.workflow import ActivityConfig

from temporal_agent_harness.ai_sdks.pydantic_ai_harness import HarnessDeps

from simple_chat_agent.agent import harness_ext
from simple_chat_agent.agent.contracts import ModelChoice
from simple_chat_agent.agent.model_registry import build_registry, settings_for
from simple_chat_agent.agent.tools.research import RESEARCH_TOOLS, research_tools_available

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

# Research tools whose backends this worker has configured. Decided once at import, from the
# worker's environment, so workflow code reads a constant (deterministic per deployment).
ENABLED_RESEARCH_TOOLS = tuple(research_tools_available())

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


def model_settings(choice: ModelChoice, *, session_id: str):
    return settings_for(choice, session_id=session_id)
