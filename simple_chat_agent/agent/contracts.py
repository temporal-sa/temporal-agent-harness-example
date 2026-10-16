"""Wire contracts for the coding agent.

Imported by the worker, the API and tests. Keep this module free of provider SDKs and of
anything that needs a worker (no Pydantic AI, no model registry), so the API can import it
without LLM keys.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field
from temporalio.contrib.workflow_streams import WorkflowStreamState

AGENT_WORKFLOW_TYPE = "CodingAgent"

Provider = Literal["anthropic", "openai", "google"]
Posture = Literal["ask_everything", "trust_safe", "auto", "never_ask"]

# Signal names of the front door (PRD S8). Signals land in history with no worker running;
# the agent admits them into the harness runner when it next runs.
ENQUEUE_MESSAGE_SIGNAL = "enqueue_message"
ENQUEUE_APPROVAL_SIGNAL = "enqueue_approval"

# Bumped whenever CarriedState changes shape. Every field keeps a default so a newer
# version can read input written by an older one (PINNED + continue-as-new, PRD §6).
CARRIED_STATE_VERSION = 1


class ModelChoice(BaseModel):
    """A provider plus a model id from that provider's allowlist."""

    provider: Provider = "anthropic"
    model: str = "claude-sonnet-5-5"
    effort: Literal["low", "medium", "high"] | None = None

    @property
    def registry_key(self) -> str:
        return f"{self.provider}:{self.model}"


class FrontDoorMessage(BaseModel):
    """Payload of the enqueue_message signal."""

    request_id: str
    type: str
    payload: dict[str, Any] = Field(default_factory=dict)


class CarriedState(BaseModel):
    """Everything a run hands to the next one when it continues as new.

    The conversation is opaque JSON here so this module stays free of Pydantic AI; the
    workflow validates it on the way in.
    """

    state_version: int = CARRIED_STATE_VERSION
    conversation: list[dict[str, Any]] = Field(default_factory=list)
    stream_state: WorkflowStreamState | None = None
    turn_counter: int = 0
    recent_request_ids: list[str] = Field(default_factory=list)
    model: ModelChoice = Field(default_factory=ModelChoice)
    pending_model: ModelChoice | None = None
    # Observable state (plan, changes, workspace) as JSON, restored into the new run.
    observable: dict[str, dict[str, Any]] = Field(default_factory=dict)
    steer_queue: list[str] = Field(default_factory=list)
    # Front-door messages that arrived during the handoff; the next run admits them first.
    pending_messages: list[FrontDoorMessage] = Field(default_factory=list)
    # The remembered allow-list ("always allow") survives through AgentConfig.approval_policy,
    # which the workflow rebuilds from the live runner policy before continuing as new.


class CodingAgentInit(BaseModel):
    """The agent's init data: what a session starts with, plus anything carried across runs."""

    owner_id: str = ""
    session_id: str = ""
    model: ModelChoice = Field(default_factory=ModelChoice)
    posture: Posture = "trust_safe"
    # Where the session runs (catalog rules are stricter) and which auto-mode judge it uses.
    # Decided by the API at session start and carried as data, never read from the
    # environment in workflow code.
    environment: Literal["catalog", "local"] = "catalog"
    judge: Literal["llm", "jev"] = "llm"
    # Research tools this session may offer, decided once by whoever starts the session
    # (from its configured backends) and carried as data, so replay never depends on a
    # worker's environment.
    research_tools: list[str] = Field(default_factory=list)
    workspace_id: str = ""
    depth: int = 0
    system_prompt: str | None = None
    carried: CarriedState | None = None
    # Test hooks: force continue-as-new at the next idle point (P0 step 0.5), and offer
    # test-only tools such as wait_seconds.
    force_continue_as_new_after_turns: int | None = None
    test_tools: bool = False
    # Test hook: a low history-event cap to exercise the checkpoint (P1 step 1.8a).
    test_history_event_cap: int | None = None


class ChatMessage(BaseModel):
    """A user message to the coding agent."""

    text: str


class ChatReply(BaseModel):
    """The agent's final answer for a turn."""

    text: str
    interrupted: bool = False
    # Set when the turn stopped early to keep the workflow inside Temporal's history limits
    # (PRD A20). The agent continues as new; the user says "continue" to carry on.
    checkpoint: str | None = None
    # Set when the turn stopped on a known, explainable failure (spend cap, provider
    # rejection, context overflow) rather than an unexpected error.
    error: str | None = None


class SetModel(BaseModel):
    """Switch provider and/or model. Applies from the next turn."""

    model: ModelChoice


class SteerMessage(BaseModel):
    """Guidance for the turn in progress, injected before the agent's next model call."""

    text: str


class SetPosture(BaseModel):
    """Change how tool calls are approved. Applies to new calls; pending ones stay with you."""

    posture: Posture


class InterruptRequest(BaseModel):
    """Stop the current turn. Pending approvals in the turn are declined."""

    reason: str = "Stopped by the user."


class Ack(BaseModel):
    """Acknowledgement for control operations."""

    ok: bool = True
    detail: str = ""


class FrontDoorApproval(BaseModel):
    """Payload of the enqueue_approval signal."""

    request_id: str
    tool_id: str
    approved: bool
    reason: str | None = None
    remember: bool = False
