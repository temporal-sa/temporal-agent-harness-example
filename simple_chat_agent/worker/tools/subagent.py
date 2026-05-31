from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass, field
from typing import Any, cast

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from claude_harness.claude_agent import ClaudeAgent, ContinueAsNewPolicy
    from claude_harness.mcp import HttpMcpProvider
    from claude_harness.mcp_types import HttpMcpServerConfig
    from claude_harness.tool_types import ToolType
    from claude_harness.tools import ToolContext, ToolResult, ToolSet, tool
    from simple_chat_agent import TASK_QUEUE
    from simple_chat_agent.worker.tools.approval import (
        ApprovalDecision,
        ChildToolApprovalRequest,
        MutatingToolApprovalProvider,
    )
    from simple_chat_agent.worker.tools.artifacts import ArtifactProvider
    from simple_chat_agent.worker.tools.fetch_url import fetch_url
    from simple_chat_agent.worker.tools.github import GitHubProvider
    from simple_chat_agent.worker.tools.python_sandbox import python_sandbox


CREATE_SUBAGENT_TOOL = "create_subagent"
_DISALLOWED_SUBAGENT_TOOLS = frozenset({CREATE_SUBAGENT_TOOL})
_DEFAULT_SUBAGENT_MAX_TURNS = 8
_MAX_SUBAGENT_MAX_TURNS = 12
_DEFAULT_SUBAGENT_MAX_TOKENS = 16_000
_MAX_SUBAGENT_MAX_TOKENS = 32_000


@dataclass
class SubagentRequest:
    system_prompt: str
    task: str
    model: str
    max_tokens: int = _DEFAULT_SUBAGENT_MAX_TOKENS
    max_turns: int = _DEFAULT_SUBAGENT_MAX_TURNS
    tool_names: list[str] = field(default_factory=list)
    denied_tool_names: list[str] = field(default_factory=list)
    parent_workflow_id: str | None = None
    user_ref: str | None = None
    conversation_id: str | None = None
    github_connection_id: str | None = None
    mcp_servers: list[HttpMcpServerConfig] = field(default_factory=list)
    stream_id: str | None = None


@dataclass
class SubagentResponse:
    text: str
    stop_reason: str | None
    turns: int
    model: str
    tool_names: list[str]
    denied_tool_names: list[str]


class SubagentProvider:
    def __init__(
        self,
        *,
        default_model: Callable[[], str],
        user_ref: Callable[[], str | None],
        conversation_id: Callable[[], str | None],
        github_connection_id: Callable[[], str | None],
        mcp_servers: Callable[[], list[HttpMcpServerConfig]] | None = None,
    ) -> None:
        self._default_model = default_model
        self._user_ref = user_ref
        self._conversation_id = conversation_id
        self._github_connection_id = github_connection_id
        self._mcp_servers = mcp_servers or (lambda: [])

    @tool(
        name=CREATE_SUBAGENT_TOOL,
        description=(
            "Create a child Claude agent for a delegated task. Pass an explicit "
            "subset of tool_names for the child to use. The child inherits this "
            "chat's streaming sideband. Mutating delegated tools request "
            "approval through this parent chat. Recursive subagents are not "
            "delegated."
        ),
        tool_type=ToolType.READ,
    )
    async def create_subagent(
        self,
        ctx: ToolContext,
        system_prompt: str,
        task: str,
        tool_names: list[str] | None = None,
        model: str | None = None,
        max_tokens: int = _DEFAULT_SUBAGENT_MAX_TOKENS,
        max_turns: int = _DEFAULT_SUBAGENT_MAX_TURNS,
    ) -> ToolResult:
        if not system_prompt.strip():
            return ToolResult(
                payload={"error": "system_prompt is required."},
                error=True,
            )
        if not task.strip():
            return ToolResult(payload={"error": "task is required."}, error=True)

        available_tool_names = [
            name
            for name in ctx.tool_names()
            if name not in _DISALLOWED_SUBAGENT_TOOLS
        ]
        requested_tool_names = _dedupe(tool_names or [])
        granted_tool_names, denied_tool_names = _split_requested_tools(
            requested_tool_names,
            available_tool_names,
        )
        max_tokens = max(1_024, min(max_tokens, _MAX_SUBAGENT_MAX_TOKENS))
        max_turns = max(1, min(max_turns, _MAX_SUBAGENT_MAX_TURNS))
        child_workflow_id = (
            f"{workflow.info().workflow_id}-subagent-{workflow.uuid4()}"
        )

        result = await workflow.execute_child_workflow(
            SubagentWorkflow.run,
            SubagentRequest(
                system_prompt=system_prompt,
                task=task,
                model=model or self._default_model(),
                max_tokens=max_tokens,
                max_turns=max_turns,
                tool_names=granted_tool_names,
                denied_tool_names=denied_tool_names,
                parent_workflow_id=workflow.info().workflow_id,
                user_ref=self._user_ref(),
                conversation_id=self._conversation_id(),
                github_connection_id=self._github_connection_id(),
                mcp_servers=list(self._mcp_servers()),
                stream_id=ctx.stream_id,
            ),
            id=child_workflow_id,
            task_queue=TASK_QUEUE,
            static_summary=f"{CREATE_SUBAGENT_TOOL}:run",
        )

        return ToolResult(payload=asdict(result), error=False)


@workflow.defn
class SubagentWorkflow:
    def __init__(self) -> None:
        self._approval_decisions: dict[str, ApprovalDecision] = {}
        self._approval_counter = 0
        self._parent_workflow_id: str | None = None

    @workflow.signal
    async def resolve_delegated_approval(
        self,
        approval_id: str,
        decision: str,
    ) -> None:
        if decision in ("allow", "always_allow", "deny"):
            self._approval_decisions[approval_id] = cast(ApprovalDecision, decision)

    @workflow.run
    async def run(self, request: SubagentRequest) -> SubagentResponse:
        parent_workflow_id = _parent_workflow_id(request)
        self._parent_workflow_id = parent_workflow_id
        tools = _build_subagent_tools(
            parent_workflow_id=parent_workflow_id or workflow.info().workflow_id,
            user_ref=request.user_ref,
            conversation_id=request.conversation_id,
            github_connection_id=request.github_connection_id,
            mcp_servers=request.mcp_servers,
            request_mutating_tool_approval=self._request_parent_tool_approval,
        )
        tool_names, denied_tool_names = _split_requested_tools(
            _dedupe(request.tool_names),
            [
                name
                for name in tools.tool_names()
                if name not in _DISALLOWED_SUBAGENT_TOOLS
            ],
        )
        denied_tool_names = _dedupe([*request.denied_tool_names, *denied_tool_names])
        agent = ClaudeAgent(
            request.system_prompt,
            tools,
            model=request.model,
            max_tokens=request.max_tokens,
            tool_names=tool_names,
            stream_id=request.stream_id,
            continue_as_new_policy=ContinueAsNewPolicy(enabled=False),
        )
        result = await agent.run(request.task, max_turns=request.max_turns)
        return SubagentResponse(
            text=_assistant_text(result.message),
            stop_reason=result.stop_reason,
            turns=result.turns,
            model=request.model,
            tool_names=tool_names,
            denied_tool_names=denied_tool_names,
        )

    async def _request_parent_tool_approval(
        self,
        tool_name: str,
        tool_args: dict[str, Any],
    ) -> ApprovalDecision:
        if self._parent_workflow_id is None:
            return "deny"

        self._approval_counter += 1
        approval_id = f"child-approval-{self._approval_counter}"
        parent = workflow.get_external_workflow_handle(self._parent_workflow_id)
        await parent.signal(
            "request_child_approval",
            ChildToolApprovalRequest(
                child_workflow_id=workflow.info().workflow_id,
                child_approval_id=approval_id,
                tool_name=tool_name,
                tool_args=dict(tool_args),
            ),
        )
        await workflow.wait_condition(
            lambda: approval_id in self._approval_decisions
        )
        return self._approval_decisions.pop(approval_id)


def _build_subagent_tools(
    parent_workflow_id: str,
    user_ref: str | None,
    conversation_id: str | None,
    github_connection_id: str | None,
    mcp_servers: list[HttpMcpServerConfig],
    request_mutating_tool_approval: Callable[
        [str, dict[str, Any]], Awaitable[ApprovalDecision]
    ]
    | None = None,
) -> ToolSet:
    tools = ToolSet()
    tools.add_provider(MutatingToolApprovalProvider(request_mutating_tool_approval))
    tools.add_provider(
        ArtifactProvider(
            user_ref=lambda: user_ref,
            conversation_id=lambda: conversation_id,
            workflow_id=lambda: parent_workflow_id,
        )
    )
    tools.add_tool(fetch_url, python_sandbox)
    tools.add_provider(
        GitHubProvider(lambda: github_connection_id),
        exclude_tools=_DISALLOWED_SUBAGENT_TOOLS,
    )
    for server in mcp_servers:
        tools.add_mcp_provider(HttpMcpProvider(server))
    return tools


def _parent_workflow_id(request: SubagentRequest) -> str | None:
    if request.parent_workflow_id is not None:
        return request.parent_workflow_id
    parent = workflow.info().parent
    if parent is None:
        return None
    return parent.workflow_id


def _split_requested_tools(
    requested_tool_names: list[str],
    available_tool_names: list[str],
) -> tuple[list[str], list[str]]:
    available = set(available_tool_names)
    granted = [name for name in requested_tool_names if name in available]
    denied = [name for name in requested_tool_names if name not in available]
    return granted, denied


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


def _assistant_text(message: dict[str, Any]) -> str:
    content = message["content"]
    if isinstance(content, str):
        return content

    text_parts: list[str] = []
    for block in content:
        block_dict = _block_dict(block)
        if block_dict.get("type") == "text":
            text = block_dict.get("text")
            if isinstance(text, str):
                text_parts.append(text)

    return "\n".join(text_parts)


def _block_dict(block: Any) -> dict[str, Any]:
    if isinstance(block, dict):
        return cast(dict[str, Any], block)
    if hasattr(block, "to_dict"):
        return cast(dict[str, Any], block.to_dict())
    return {}
