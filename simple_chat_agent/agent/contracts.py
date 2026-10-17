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
# The answer to a browser callback tool (ask_user, pick_files; PRD A12).
ENQUEUE_CALLBACK_SIGNAL = "enqueue_callback_result"
# The Redis bridge reports the highest stream offset it has stored, once per turn end. The
# agent then trims only what the bridge holds when it continues as new (P2 decision 4).
STREAM_ACKED_SIGNAL = "stream_acked"

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
    stream_acked: int = 0
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
    research_tools: list[str] = Field(default_factory=lambda: ["fetch_url"])
    workspace_id: str = ""
    depth: int = 0
    # MCP tools from the owner's registered servers (PRD A8). Set by the API at session start,
    # only for local sessions (D7); off for sessions that predate it.
    mcp: bool = False
    system_prompt: str | None = None
    carried: CarriedState | None = None
    # Test hooks: force continue-as-new at the next idle point (P0 step 0.5), and offer
    # test-only tools such as wait_seconds.
    force_continue_as_new_after_turns: int | None = None
    test_tools: bool = False
    # Test hook: a low history-event cap to exercise the checkpoint (P1 step 1.8a).
    test_history_event_cap: int | None = None


MAX_CHAT_ATTACHMENTS = 20


class ChatAttachment(BaseModel):
    """A file the user attached to a chat message (uploaded first with
    ``POST /api/agent/sessions/{id}/attachments``, which returns its id)."""

    attachment_id: str = Field(min_length=1, max_length=200)
    name: str = Field(default="", max_length=500)
    size: int | None = Field(default=None, ge=0)
    content_type: str | None = Field(default=None, max_length=200)


class ChatMessage(BaseModel):
    """A user message to the coding agent."""

    text: str
    # Defaulted: messages from before attachments existed (and carried, deferred ones) still
    # validate.
    attachments: list[ChatAttachment] = Field(default_factory=list, max_length=MAX_CHAT_ATTACHMENTS)


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


class SettingsAck(Ack):
    """Acknowledgement of ``set_model`` / ``set_posture`` with the value the agent now holds:
    the new one when ``ok``, the unchanged one when it refused. The bridge copies it into the
    session list, so the list never shows a setting the agent refused."""

    setting: Literal["model", "posture"]
    # set_model: the model for the next turn (the current one if the change was refused).
    model: ModelChoice | None = None
    posture: Posture | None = None


class ForgetAlwaysAllowed(BaseModel):
    """Forget the "always allow" approvals given in this session (no fields)."""


class AlwaysAllowedForgotten(Ack):
    """The tools that were always allowed and now ask again (as the posture says)."""

    forgotten: list[str] = Field(default_factory=list)


class StreamAck(BaseModel):
    """Payload of the stream_acked signal."""

    offset: int


class FrontDoorCallbackResult(BaseModel):
    """Payload of the enqueue_callback_result signal: the browser's answer to a pending
    callback tool call. Exactly one of ``result`` / ``error`` is meaningful."""

    request_id: str
    tool_id: str
    result: Any = None
    error: str | None = None


# -- Browser callback tools (PRD A12) --------------------------------------------------------

# pick_files limits, enforced by the upload route (size, count, names) and again by the agent
# (count and path shape) when the answer arrives.
PICK_MAX_FILES = 10
PICK_MAX_FILE_BYTES = 5 * 1024 * 1024  # the executor's per-write cap
PICK_MAX_NAME_CHARS = 100
PICK_UPLOAD_DIR = "uploads"
ASK_MAX_OPTIONS = 6


class ChoiceOption(BaseModel):
    """One choice card."""

    id: str = Field(description="Short stable id for this option, e.g. 'a' or 'keep'.")
    label: str = Field(description="What the card says.")
    description: str = Field("", description="One optional line under the label.")


class AskUserAnswer(BaseModel):
    """The option the user chose (one of the offered ids)."""

    option_id: str


class PickedFiles(BaseModel):
    """Workspace paths of the files the user chose, uploaded under ``uploads/``."""

    paths: list[str] = Field(default_factory=list)


class FrontDoorApproval(BaseModel):
    """Payload of the enqueue_approval signal."""

    request_id: str
    tool_id: str
    approved: bool
    reason: str | None = None
    remember: bool = False


# -- Subagent message models (key-free, so the API can describe subagent interfaces) --


class Question(BaseModel):
    """A question about the codebase for the explorer to answer from the files."""

    question: str = Field(description="What to find out. Be specific.")
    paths: list[str] = Field(default_factory=list, description="Where to start looking, if known.")


class Evidence(BaseModel):
    """One place in the code that supports the answer."""

    path: str
    line: int | None = None
    note: str


class Findings(BaseModel):
    """The explorer's answer, with the code that supports it."""

    answer: str
    evidence: list[Evidence] = Field(default_factory=list)


class ReviewRequest(BaseModel):
    """Ask the reviewer to review the workspace's changes: the uncommitted diff, or the last
    commit when nothing is uncommitted."""

    focus: str = Field("", description="What to pay most attention to, if anything.")


class Issue(BaseModel):
    """One problem the reviewer found."""

    severity: str = Field(description="blocker, major or minor")
    path: str
    line: int | None = None
    comment: str


class Review(BaseModel):
    """The reviewer's verdict on the diff."""

    verdict: str = Field(description="approve or request_changes")
    summary: str
    issues: list[Issue] = Field(default_factory=list)
