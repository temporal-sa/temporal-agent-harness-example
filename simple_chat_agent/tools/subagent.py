from __future__ import annotations

from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from typing import Any, cast

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from claude_harness.claude_agent import ClaudeAgent, ContinueAsNewPolicy
    from claude_harness.mcp import HttpMcpProvider
    from claude_harness.mcp_types import HttpMcpServerConfig
    from claude_harness.streaming import StreamContext
    from claude_harness.tool_types import ToolType
    from claude_harness.tools import ToolContext, ToolResult, ToolSet, tool
    from simple_chat_agent import TASK_QUEUE
    from simple_chat_agent.tools.fetch_url import fetch_url
    from simple_chat_agent.tools.github import GitHubProvider


CREATE_SUBAGENT_TOOL = "create_subagent"
_DISALLOWED_SUBAGENT_TOOLS = frozenset(
    {
        CREATE_SUBAGENT_TOOL,
        "create_artifact",
        "github_open_issue",
        "python_sandbox",
    }
)
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
        github_connection_id: Callable[[], str | None],
        mcp_servers: Callable[[], list[HttpMcpServerConfig]] | None = None,
    ) -> None:
        self._default_model = default_model
        self._github_connection_id = github_connection_id
        self._mcp_servers = mcp_servers or (lambda: [])

    @tool(
        name=CREATE_SUBAGENT_TOOL,
        description=(
            "Create a child Claude agent for a delegated task. Pass an explicit "
            "subset of tool_names for the child to use. The child inherits this "
            "chat's streaming sideband. Mutating tools and recursive subagents "
            "are not delegated."
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

        await ctx.activity(
            _emit_subagent_event,
            step="start",
            args={
                "kind": "subagent_start",
                "payload": {
                    "workflow_id": child_workflow_id,
                    "model": model or self._default_model(),
                    "tool_names": granted_tool_names,
                    "denied_tool_names": denied_tool_names,
                    "task_preview": task[:280],
                },
            },
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
                github_connection_id=self._github_connection_id(),
                mcp_servers=list(self._mcp_servers()),
                stream_id=ctx.stream_id,
            ),
            id=child_workflow_id,
            task_queue=TASK_QUEUE,
            static_summary=f"{CREATE_SUBAGENT_TOOL}:run",
        )

        await ctx.activity(
            _emit_subagent_event,
            step="complete",
            args={
                "kind": "subagent_complete",
                "payload": {
                    "workflow_id": child_workflow_id,
                    "stop_reason": result.stop_reason,
                    "turns": result.turns,
                },
            },
        )

        return ToolResult(payload=asdict(result), error=False)


@workflow.defn
class SubagentWorkflow:
    @workflow.run
    async def run(self, request: SubagentRequest) -> SubagentResponse:
        tools = _build_subagent_tools(
            request.github_connection_id,
            request.mcp_servers,
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


async def _emit_subagent_event(
    kind: str,
    payload: dict[str, Any],
    *,
    stream: StreamContext,
) -> dict[str, str]:
    await stream.emit(payload, kind=kind)
    return {"status": "emitted"}


def _build_subagent_tools(
    github_connection_id: str | None,
    mcp_servers: list[HttpMcpServerConfig],
) -> ToolSet:
    tools = ToolSet()
    tools.add_tool(fetch_url)
    tools.add_provider(GitHubProvider(lambda: github_connection_id))
    for server in mcp_servers:
        tools.add_mcp_provider(HttpMcpProvider(server))
    return tools


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
