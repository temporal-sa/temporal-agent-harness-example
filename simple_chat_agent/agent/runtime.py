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
from simple_chat_agent.agent.prompts import CODING_AGENT_INSTRUCTIONS

AGENT_NAME = "coding-agent"

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
        instructions=CODING_AGENT_INSTRUCTIONS,
        deps_type=HarnessDeps,
    ),
    name=AGENT_NAME,
    models=MODELS,
    event_stream_handler=harness_ext.coding_event_stream_handler,
    model_activity_config=MODEL_ACTIVITY,
)


def is_allowed_model(choice: ModelChoice) -> bool:
    return choice.registry_key in ALLOWED_KEYS


def model_settings(choice: ModelChoice, *, session_id: str):
    return settings_for(choice, session_id=session_id)
