from __future__ import annotations

from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import asdict, dataclass, field
from typing import Any, cast

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from agent_harness.agent import AgentState
    from agent_harness.mcp import HttpMcpProvider
    from agent_harness.mcp_types import HttpMcpServerConfig
    from agent_harness.providers.claude import ClaudeAgent, ClaudeThinkingConfig
    from agent_harness.tool_types import ToolType
    from agent_harness.tools import ToolContext, ToolResult, ToolSet, tool
    from simple_chat_agent import TASK_QUEUE
    from simple_chat_agent.worker.tools.approval import (
        TOOL_APPROVAL_TIMEOUT,
        ApprovalDecision,
        ChildToolApprovalRequest,
        MutatingToolApprovalProvider,
    )
    from simple_chat_agent.worker.tools.artifacts import ArtifactProvider
    from simple_chat_agent.worker.tools.fetch_url import fetch_url
    from simple_chat_agent.worker.tools.github import GitHubProvider
    from simple_chat_agent.worker.tools.research import ResearchProvider
    from simple_chat_agent.worker.tools.workspace_shell import WorkspaceShellProvider


CREATE_SUBAGENT_TOOL = "create_subagent"
_DEFAULT_SUBAGENT_MAX_TOKENS = 16_000
_MAX_SUBAGENT_MAX_TOKENS = 32_000


@dataclass
class SubagentRequest:
    system_prompt: str
    task: str
    model: str
    thinking: ClaudeThinkingConfig | None = None
    max_tokens: int = _DEFAULT_SUBAGENT_MAX_TOKENS
    tool_names: list[str] = field(default_factory=list)
    denied_tool_names: list[str] = field(default_factory=list)
    # The root chat workflow remains the authority for approvals and artifacts.
    parent_workflow_id: str | None = None
    # Runtime lineage separately records the subagent that delegated this task.
    immediate_parent_workflow_id: str | None = None
    parent_tool_call_id: str | None = None
    user_ref: str | None = None
    conversation_id: str | None = None
    github_connection_id: str | None = None
    mcp_servers: list[HttpMcpServerConfig] = field(default_factory=list)
    stream_id: str | None = None
    reference_time: str = ""
    agent_state: AgentState | None = None
    approval_counter: int = 0


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
        thinking: Callable[[], ClaudeThinkingConfig | None] | None = None,
        user_ref: Callable[[], str | None],
        conversation_id: Callable[[], str | None],
        github_connection_id: Callable[[], str | None],
        reference_time: Callable[[], str | None] | None = None,
        mcp_servers: Callable[[], list[HttpMcpServerConfig]] | None = None,
        root_workflow_id: Callable[[], str | None] | None = None,
    ) -> None:
        self._default_model = default_model
        self._thinking = thinking or (lambda: None)
        self._user_ref = user_ref
        self._conversation_id = conversation_id
        self._github_connection_id = github_connection_id
        self._reference_time = reference_time or (lambda: None)
        self._mcp_servers = mcp_servers or (list)
        self._root_workflow_id = root_workflow_id or (lambda: None)

    @tool(
        name=CREATE_SUBAGENT_TOOL,
        description=(
            "Proactively delegate one independent, substantive workstream to a "
            "child research agent. Use this whenever a request has separable "
            "research topics, comparisons, sources, regions, time periods, or "
            "report sections. For parallel work, call create_subagent multiple "
            "times in the same assistant turn—one focused call per independent "
            "workstream; those calls run concurrently. Do not research clearly "
            "separable workstreams serially yourself. Each child returns findings "
            "for you to cross-check and synthesize. Omit tool_names to grant all "
            "currently available tools, or pass a focused subset. Every child "
            "always receives create_subagent and may recursively delegate its own "
            "independent workstreams. "
            "Mutating tools still request approval through the parent chat. Avoid "
            "delegation only for trivial, single-step work. Children have no turn "
            "or delegation-depth limit."
        ),
        tool_type=ToolType.READ,
    )
    async def create_subagent(
        self,
        ctx: ToolContext,
        task: str,
        system_prompt: str = (
            "You are a focused research subagent. Investigate the delegated "
            "workstream thoroughly, use the available tools, cross-check "
            "important claims, distinguish evidence from inference, cite sources "
            "when available, recursively delegate independent substantive "
            "subproblems when useful, and return concise findings for the parent "
            "agent to synthesize."
        ),
        tool_names: list[str] | None = None,
        model: str | None = None,
        max_tokens: int = _DEFAULT_SUBAGENT_MAX_TOKENS,
    ) -> ToolResult:
        if not system_prompt.strip():
            return ToolResult(
                payload={"error": "system_prompt is required."},
                error=True,
            )
        if not task.strip():
            return ToolResult(payload={"error": "task is required."}, error=True)

        available_tool_names = ctx.tool_names()
        current_workflow_id = workflow.info().workflow_id
        root_workflow_id = self._root_workflow_id() or current_workflow_id
        child_workflow_id = f"{root_workflow_id}-subagent-{workflow.uuid4()}"
        requested_tool_names = (
            available_tool_names
            if tool_names is None
            else _dedupe([*tool_names, CREATE_SUBAGENT_TOOL])
        )
        granted_tool_names, denied_tool_names = _split_requested_tools(
            requested_tool_names,
            available_tool_names,
        )
        max_tokens = max(1_024, min(max_tokens, _MAX_SUBAGENT_MAX_TOKENS))

        result = await workflow.execute_child_workflow(
            SubagentWorkflow.run,
            SubagentRequest(
                system_prompt=system_prompt,
                task=task,
                model=model or self._default_model(),
                thinking=self._thinking(),
                max_tokens=max_tokens,
                tool_names=granted_tool_names,
                denied_tool_names=denied_tool_names,
                parent_workflow_id=root_workflow_id,
                immediate_parent_workflow_id=current_workflow_id,
                parent_tool_call_id=ctx.tool_call_id,
                user_ref=self._user_ref(),
                conversation_id=self._conversation_id(),
                github_connection_id=self._github_connection_id(),
                mcp_servers=list(self._mcp_servers()),
                stream_id=ctx.stream_id,
                reference_time=self._reference_time() or "",
            ),
            id=child_workflow_id,
            task_queue=TASK_QUEUE,
            static_summary=f"{CREATE_SUBAGENT_TOOL}:run",
            search_attributes=workflow.info().typed_search_attributes,
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
        if decision in ("allow", "always_allow", "deny", "expired", "cancelled"):
            self._approval_decisions[approval_id] = cast(ApprovalDecision, decision)

    @workflow.run
    async def run(self, request: SubagentRequest) -> SubagentResponse:
        parent_workflow_id = _parent_workflow_id(request)
        immediate_parent_workflow_id = _immediate_parent_workflow_id(request)
        self._parent_workflow_id = parent_workflow_id
        self._approval_counter = request.approval_counter
        tools = _build_subagent_tools(
            parent_workflow_id=parent_workflow_id or workflow.info().workflow_id,
            user_ref=request.user_ref,
            conversation_id=request.conversation_id,
            github_connection_id=request.github_connection_id,
            mcp_servers=request.mcp_servers,
            default_model=request.model,
            thinking=request.thinking,
            reference_time=request.reference_time,
            request_mutating_tool_approval=self._request_parent_tool_approval,
        )
        tool_names, denied_tool_names = _split_requested_tools(
            _dedupe([*request.tool_names, CREATE_SUBAGENT_TOOL]),
            tools.tool_names(),
        )
        denied_tool_names = _dedupe([*request.denied_tool_names, *denied_tool_names])
        agent = ClaudeAgent(
            request.system_prompt,
            tools,
            model=request.model,
            max_tokens=request.max_tokens,
            thinking=request.thinking,
            tool_names=tool_names,
            stream_id=request.stream_id,
            stream_agent={
                "id": workflow.info().workflow_id,
                "parent_id": immediate_parent_workflow_id,
                "kind": "subagent",
                "label": request.task,
                "parent_tool_call_id": request.parent_tool_call_id,
            },
        )
        if request.agent_state is None:
            result = await agent.run(
                request.task,
                reference_time=request.reference_time,
            )
        else:
            result = await agent.run(state=request.agent_state)

        if result.needs_continue_as_new:
            workflow.continue_as_new(
                SubagentRequest(
                    system_prompt=request.system_prompt,
                    task=request.task,
                    model=request.model,
                    thinking=request.thinking,
                    max_tokens=request.max_tokens,
                    tool_names=tool_names,
                    denied_tool_names=denied_tool_names,
                    parent_workflow_id=parent_workflow_id,
                    immediate_parent_workflow_id=immediate_parent_workflow_id,
                    parent_tool_call_id=request.parent_tool_call_id,
                    user_ref=request.user_ref,
                    conversation_id=request.conversation_id,
                    github_connection_id=request.github_connection_id,
                    mcp_servers=list(request.mcp_servers),
                    stream_id=request.stream_id,
                    reference_time=request.reference_time,
                    agent_state=result.continuation_state,
                    approval_counter=self._approval_counter,
                ),
                initial_versioning_behavior=(
                    workflow.ContinueAsNewVersioningBehavior.AUTO_UPGRADE
                ),
            )

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
        try:
            await workflow.wait_condition(
                lambda: approval_id in self._approval_decisions,
                timeout=TOOL_APPROVAL_TIMEOUT,
                timeout_summary=f"delegated-approval:{approval_id}",
            )
        except TimeoutError:
            with suppress(Exception):
                await parent.signal(
                    "expire_child_approval",
                    workflow.info().workflow_id,
                    approval_id,
                )
            return "expired"
        return self._approval_decisions.pop(approval_id)


def _build_subagent_tools(
    parent_workflow_id: str,
    user_ref: str | None,
    conversation_id: str | None,
    github_connection_id: str | None,
    mcp_servers: list[HttpMcpServerConfig],
    default_model: str,
    thinking: ClaudeThinkingConfig | None,
    reference_time: str,
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
    tools.add_tool(fetch_url)
    tools.add_provider(
        WorkspaceShellProvider(workspace_id=lambda: workflow.info().workflow_id)
    )
    tools.add_provider(ResearchProvider())
    tools.add_provider(GitHubProvider(lambda: github_connection_id))
    tools.add_provider(
        SubagentProvider(
            default_model=lambda: default_model,
            thinking=lambda: thinking,
            user_ref=lambda: user_ref,
            conversation_id=lambda: conversation_id,
            github_connection_id=lambda: github_connection_id,
            reference_time=lambda: reference_time,
            mcp_servers=lambda: list(mcp_servers),
            root_workflow_id=lambda: parent_workflow_id,
        )
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


def _immediate_parent_workflow_id(request: SubagentRequest) -> str | None:
    if request.immediate_parent_workflow_id is not None:
        return request.immediate_parent_workflow_id
    parent = workflow.info().parent
    if parent is not None:
        return parent.workflow_id
    return request.parent_workflow_id


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
        text = _block_text(block_dict)
        if text:
            text_parts.append(text)

    return "\n".join(text_parts)


def _block_text(block: dict[str, Any]) -> str:
    if block.get("type") == "text":
        text = block.get("text")
        return text if isinstance(text, str) else ""
    if block.get("type") == "refusal":
        refusal = block.get("refusal")
        return refusal if isinstance(refusal, str) else ""
    return ""


def _block_dict(block: Any) -> dict[str, Any]:
    if isinstance(block, dict):
        return cast(dict[str, Any], block)
    if hasattr(block, "to_dict"):
        return cast(dict[str, Any], block.to_dict())
    return {}
