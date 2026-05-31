from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable
from dataclasses import replace
from typing import Any, Literal

from claude_harness.mcp import HttpMcpProvider
from claude_harness.mcp_types import HttpMcpServerConfig
from claude_harness.tools import ToolResult, ToolSet

from .approval import MutatingToolApprovalProvider
from .artifacts import ArtifactProvider, CREATE_ARTIFACT_TOOL
from .fetch_url import fetch_url
from .github import GitHubProvider
from .python_sandbox import python_sandbox
from .subagent import CREATE_SUBAGENT_TOOL, SubagentProvider

ApprovalDecision = Literal["allow", "always_allow", "deny"]
ApprovalRequest = Callable[[str, dict[str, Any]], Awaitable[ApprovalDecision]]

FETCH_URL_TOOL = "fetch_url"
PYTHON_SANDBOX_TOOL = "python_sandbox"
GITHUB_TOOL_NAMES = [
    "github_authenticated_user",
    "github_list_repositories",
    "github_list_issues",
    "github_open_issue",
]


class AppToolSet(ToolSet):
    def __init__(
        self,
        available_tool_names: Callable[[], Iterable[str]],
        *,
        mcp_servers: Callable[[], Iterable[HttpMcpServerConfig]] | None = None,
    ) -> None:
        super().__init__()
        self._available_tool_names = available_tool_names
        self._mcp_servers = mcp_servers or (lambda: ())
        self._mcp_tool_sources: dict[str, tuple[str, str, str | None, str]] = {}

    def tool_names(self) -> list[str]:
        self._sync_mcp_tools()
        available = self._available_set()
        return [name for name in super().tool_names() if name in available]

    def tool_schemas(self, names: Iterable[str] | None = None) -> list[Any]:
        self._sync_mcp_tools()
        available = self._available_set()
        requested = set(names) if names is not None else set(super().tool_names())
        visible = [
            name for name in super().tool_names() if name in available & requested
        ]
        return super().tool_schemas(visible)

    async def execute_tool(
        self,
        name: str,
        args: dict | None = None,
        **kwargs,
    ) -> ToolResult:
        self._sync_mcp_tools()
        if name not in self._available_set():
            return ToolResult(
                payload={
                    "error": f"Tool is not available in this chat: {name}",
                },
                error=True,
            )
        return await super().execute_tool(name, args, **kwargs)

    def _available_set(self) -> set[str]:
        return set(self._available_tool_names())

    def _sync_mcp_tools(self) -> None:
        desired_tools: dict[
            str,
            tuple[HttpMcpServerConfig, Any, tuple[str, str, str | None, str]],
        ] = {}
        for server in self._mcp_servers():
            if not server.enabled:
                continue
            for tool in server.tools:
                public_name = tool.public_name or server.public_tool_name(tool.name)
                if public_name in desired_tools:
                    continue
                desired_tools[public_name] = (
                    server,
                    tool,
                    (server.server_id, server.server_url, server.auth_ref, tool.name),
                )

        for name in list(self._mcp_tool_sources):
            if name not in desired_tools:
                self._mcp_tool_sources.pop(name, None)
                self._tool_registry.pop(name, None)

        for name, (server, tool, source) in desired_tools.items():
            if self._mcp_tool_sources.get(name) == source and name in self._tool_registry:
                continue
            if name in self._tool_registry and name not in self._mcp_tool_sources:
                continue

            self._tool_registry.pop(name, None)
            self.add_mcp_provider(HttpMcpProvider(replace(server, tools=[tool])))
            self._mcp_tool_sources[name] = source


def build_tools(
    *,
    available_tool_names: Callable[[], Iterable[str]],
    user_ref: Callable[[], str | None],
    conversation_id: Callable[[], str | None],
    workflow_id: Callable[[], str],
    github_connection_id: Callable[[], str | None],
    mcp_servers: Callable[[], Iterable[HttpMcpServerConfig]] | None = None,
    default_model: Callable[[], str],
    request_mutating_tool_approval: ApprovalRequest | None = None,
) -> ToolSet:
    tools = AppToolSet(
        available_tool_names,
        mcp_servers=mcp_servers,
    )
    tools.add_provider(MutatingToolApprovalProvider(request_mutating_tool_approval))
    tools.add_provider(
        ArtifactProvider(
            user_ref=user_ref,
            conversation_id=conversation_id,
            workflow_id=workflow_id,
        )
    )
    tools.add_tool(fetch_url, python_sandbox)
    tools.add_provider(GitHubProvider(github_connection_id))
    tools.add_provider(
        SubagentProvider(
            default_model=default_model,
            user_ref=user_ref,
            conversation_id=conversation_id,
            github_connection_id=github_connection_id,
            mcp_servers=lambda: list(
                mcp_servers() if mcp_servers is not None else ()
            ),
        )
    )
    return tools


def tool_names_for_connections(
    *,
    github_connection_id: str | None,
    mcp_servers: Iterable[HttpMcpServerConfig] | None = None,
) -> list[str]:
    names = [
        FETCH_URL_TOOL,
        PYTHON_SANDBOX_TOOL,
        CREATE_ARTIFACT_TOOL,
        CREATE_SUBAGENT_TOOL,
    ]
    if github_connection_id is not None:
        names.extend(GITHUB_TOOL_NAMES)
    for server in mcp_servers or ():
        names.extend(HttpMcpProvider(server).tool_names())
    return names
