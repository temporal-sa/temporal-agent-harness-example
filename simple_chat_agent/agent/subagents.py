"""Read-only subagents the coding agent can delegate to (PRD A10).

Each is its own harness agent workflow with a typed contract, started as a child of the
coding agent through ``agent.subagent_toolset``. They share the parent's workspace but only
get read-only tools, so they never need approvals. Release 1 limits: depth 1 (children get no
subagent tools), at most two running at once (enforced by the parent), the provider's fast
model, and usage charged to the parent session.
"""

from __future__ import annotations

from temporalio import workflow
from temporalio.contrib.workflow_streams import WorkflowStream

with workflow.unsafe.imports_passed_through():
    from pydantic import BaseModel, Field
    from pydantic_ai.messages import ModelMessage

    from temporal_agent_harness.harness import agent
    from temporal_agent_harness.harness.agent_protocol import AgentConfig, ToolApprovalPolicy
    from temporal_agent_harness.harness.agent_workflow import AgentWorkflowRunner

    from simple_chat_agent.agent import harness_ext
    from simple_chat_agent.agent.contracts import (  # message models live in contracts (key-free)
        Evidence,
        Findings,
        Issue,
        ModelChoice,
        Question,
        Review,
        ReviewRequest,
    )
    from simple_chat_agent.agent.runtime import CODING_AGENT, CodingDeps, fast_model_for, model_settings
    from simple_chat_agent.agent.tools.workspace import WORKSPACE_TOOLS

READ_ONLY_TOOL_NAMES = frozenset({"read_file", "list_files", "search_code", "git_status", "git_diff"})
MAX_CONCURRENT_SUBAGENTS = 2


class SubagentInit(BaseModel):
    """What a subagent starts with: the parent's workspace and identity, and its model."""

    workspace_id: str
    owner_id: str = ""
    session_id: str = ""
    model: ModelChoice = Field(default_factory=ModelChoice)


EXPLORER_INSTRUCTIONS = """\
You are a code explorer helping another engineer. Answer the question by reading the
workspace with your read-only tools. Cite the files and lines that support your answer. Be
brief and factual; don't propose changes."""

REVIEWER_INSTRUCTIONS = """\
You are a careful code reviewer. Inspect the uncommitted diff (git_diff) and the surrounding
code, then judge whether the change is correct and complete. Report concrete issues with
file and line; approve only if you would merge it."""


def _read_only_tools() -> list:
    return [t for t in WORKSPACE_TOOLS if getattr(t, "__name__", "") in READ_ONLY_TOOL_NAMES]


class _ReadOnlyAgent:
    """Shared plumbing: a runner, a history, and a typed model run over read-only tools."""

    instructions: str = ""

    def _setup(self, config: AgentConfig, data: SubagentInit, stream: WorkflowStream) -> None:
        self._data = data
        self._runner = AgentWorkflowRunner(
            config,
            stream=stream,
            approval_policy_default=ToolApprovalPolicy.allow_inherently_safe(),
        )
        self._history: list[ModelMessage] = []

    async def _ask(self, prompt: str, output_type: type[BaseModel]) -> BaseModel:
        toolset = harness_ext.per_run_toolset(
            _read_only_tools(), id="workspace-ro", injections={"workspace_id": self._data.workspace_id}
        )
        result = await CODING_AGENT.run(
            prompt,
            output_type=output_type,
            instructions=self.instructions,
            deps=CodingDeps(
                runner=self._runner, session_id=self._data.session_id, owner_id=self._data.owner_id
            ),
            message_history=self._history,
            model=self._data.model.registry_key,
            model_settings=model_settings(self._data.model, session_id=self._data.session_id),
            toolsets=[toolset],
        )
        self._history = result.all_messages()
        return result.output


@agent.defn(name="ExplorerAgent")
class ExplorerAgent(_ReadOnlyAgent):
    """Answers questions about the codebase from the files."""

    instructions = EXPLORER_INSTRUCTIONS

    @agent.init
    def __init__(self, config: AgentConfig, data: SubagentInit) -> None:
        # The harness requires the stream to be built directly in @workflow.init.
        self._setup(config, data, WorkflowStream())

    @agent.accepts
    async def investigate(self, msg: Question) -> Findings:
        """Investigate a question about the code and report findings with evidence."""
        where = f"\nStart with: {', '.join(msg.paths)}" if msg.paths else ""
        return await self._ask(msg.question + where, Findings)  # type: ignore[return-value]


@agent.defn(name="ReviewerAgent")
class ReviewerAgent(_ReadOnlyAgent):
    """Reviews the workspace's uncommitted changes."""

    instructions = REVIEWER_INSTRUCTIONS

    @agent.init
    def __init__(self, config: AgentConfig, data: SubagentInit) -> None:
        self._setup(config, data, WorkflowStream())

    @agent.accepts
    async def review(self, msg: ReviewRequest) -> Review:
        """Review the current diff and return a verdict with concrete issues."""
        focus = f" Focus on: {msg.focus}" if msg.focus else ""
        return await self._ask(f"Review the current uncommitted diff.{focus}", Review)  # type: ignore[return-value]


SUBAGENT_WORKFLOWS = (ExplorerAgent, ReviewerAgent)


def subagent_tools(*, task_queue: str, init: SubagentInit) -> list:
    """The parent's tools for starting, using and stopping explorers and reviewers."""

    def data() -> SubagentInit:
        return init.model_copy(update={"model": fast_model_for(init.model)})

    return [
        *agent.subagent_toolset(ExplorerAgent, key="explorer", task_queue=task_queue, init_data=data),
        *agent.subagent_toolset(ReviewerAgent, key="reviewer", task_queue=task_queue, init_data=data),
    ]
