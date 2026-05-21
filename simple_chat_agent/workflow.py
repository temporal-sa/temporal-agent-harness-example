from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, cast

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from claude_harness.mcp_types import HttpMcpServerConfig
    from claude_harness.claude_agent import (
        ClaudeAgent,
        ClaudeAgentResult,
        ClaudeAgentState,
        SteeringMode,
    )
    from simple_chat_agent.tools import build_tools, tool_names_for_connections


ChatRole = Literal["user", "assistant", "system"]
ApprovalDecision = Literal["allow", "always_allow", "deny"]
DEFAULT_MAX_TOKENS = 64_000


@dataclass
class ChatMessage:
    role: ChatRole
    content: str


@dataclass
class QueuedChatMessage:
    content: str
    transcript_index: int


@dataclass
class PendingApproval:
    approval_id: str
    tool_name: str
    tool_args: dict[str, Any]
    summary: str
    memory_key: str


@dataclass
class SimpleChatInput:
    user_ref: str = "local-user"
    conversation_id: str = "local-conversation"
    system_prompt: str = "You are a concise test chatbot."
    model: str = "claude-sonnet-4-5"
    max_tokens: int = DEFAULT_MAX_TOKENS
    max_turns: int = 20
    stream_id: str | None = None
    available_tool_names: list[str] = field(default_factory=list)
    github_connection_id: str | None = None
    mcp_servers: list[HttpMcpServerConfig] = field(default_factory=list)
    agent_state: ClaudeAgentState | None = None
    transcript: list[ChatMessage] = field(default_factory=list)
    pending_messages: list[QueuedChatMessage] = field(default_factory=list)
    active_message_index: int | None = None
    approval_memory: list[str] = field(default_factory=list)
    approval_counter: int = 0


@dataclass
class SimpleChatState:
    status: str
    pending_messages: int
    user_ref: str | None = None
    conversation_id: str | None = None
    available_tool_names: list[str] = field(default_factory=list)
    github_connected: bool = False
    mcp_servers: list[HttpMcpServerConfig] = field(default_factory=list)
    pending_approvals: list[PendingApproval] = field(default_factory=list)
    active_message_index: int | None = None
    queued_message_indices: list[int] = field(default_factory=list)
    transcript: list[ChatMessage] = field(default_factory=list)


@workflow.defn
class SimpleChatWorkflow:
    def __init__(self) -> None:
        self._pending_messages: list[QueuedChatMessage] = []
        self._transcript: list[ChatMessage] = []
        self._status = "starting"
        self._agent: ClaudeAgent | None = None
        self._active_message_index: int | None = None
        self._user_ref: str | None = None
        self._conversation_id: str | None = None
        self._available_tool_names: set[str] = set()
        self._github_connection_id: str | None = None
        self._mcp_servers: list[HttpMcpServerConfig] = []
        self._pending_approvals: dict[str, PendingApproval] = {}
        self._approval_decisions: dict[str, ApprovalDecision] = {}
        self._approval_memory: set[str] = set()
        self._approval_counter = 0
        self._delete_requested = False

    @workflow.signal
    async def chat(self, message: str) -> None:
        self._enqueue_chat(message)

    @workflow.signal
    async def delete(self) -> None:
        self._delete_requested = True

    @workflow.signal
    async def steer(self, message: str, mode: str = "immediate") -> None:
        if mode not in ("immediate", "after_next_tool_result"):
            self._transcript.append(
                ChatMessage(role="system", content=f"Unknown steering mode: {mode}")
            )
            return

        if self._agent is None:
            self._transcript.append(
                ChatMessage(
                    role="system",
                    content="Agent is not ready yet; steering was ignored.",
                )
            )
            return

        self._agent.steer(message, mode=cast(SteeringMode, mode))
        label = (
            "after the next tool result"
            if mode == "after_next_tool_result"
            else "before the next Claude call"
        )
        self._transcript.append(
            ChatMessage(
                role="system",
                content=f"Steering queued {label}: {message}",
            )
        )

    @workflow.signal
    async def interrupt(self, message: str) -> None:
        if self._agent is None or self._status != "responding":
            self._enqueue_chat(message)
            return

        self._agent.interrupt(message)
        self._transcript.append(
            ChatMessage(
                role="system",
                content=f"Interrupt sent; Claude will continue with: {message}",
            )
        )

    @workflow.signal
    async def update_tool_connections(
        self,
        available_tool_names: list[str],
        github_connection_id: str | None = None,
        mcp_servers: list[HttpMcpServerConfig] | None = None,
    ) -> None:
        self._available_tool_names = set(available_tool_names)
        self._github_connection_id = github_connection_id
        if mcp_servers is not None:
            self._mcp_servers = list(mcp_servers)
        github_status = "connected" if github_connection_id else "disconnected"
        mcp_status = f"{len(self._mcp_servers)} MCP server(s)"
        self._transcript.append(
            ChatMessage(
                role="system",
                content=(
                    f"Tool availability updated. GitHub is {github_status}; "
                    f"{mcp_status} configured."
                ),
            )
        )

    @workflow.signal
    async def resolve_approval(
        self,
        approval_id: str,
        decision: str,
    ) -> None:
        if decision not in ("allow", "always_allow", "deny"):
            self._transcript.append(
                ChatMessage(
                    role="system",
                    content=f"Unknown approval decision: {decision}",
                )
            )
            return
        if approval_id not in self._pending_approvals:
            self._transcript.append(
                ChatMessage(
                    role="system",
                    content=f"Approval is no longer pending: {approval_id}",
                )
            )
            return

        self._approval_decisions[approval_id] = cast(ApprovalDecision, decision)

    @workflow.query
    def state(self) -> SimpleChatState:
        return SimpleChatState(
            status=self._status,
            pending_messages=len(self._pending_messages),
            user_ref=self._user_ref,
            conversation_id=self._conversation_id,
            available_tool_names=sorted(self._available_tool_names),
            github_connected=self._github_connection_id is not None,
            mcp_servers=list(self._mcp_servers),
            pending_approvals=list(self._pending_approvals.values()),
            active_message_index=self._active_message_index,
            queued_message_indices=[
                message.transcript_index for message in self._pending_messages
            ],
            transcript=list(self._transcript),
        )

    @workflow.query
    def transcript(self) -> list[ChatMessage]:
        return list(self._transcript)

    @workflow.run
    async def run(self, chat_input: SimpleChatInput) -> None:
        self._transcript = list(chat_input.transcript)
        self._pending_messages = list(chat_input.pending_messages)
        self._active_message_index = chat_input.active_message_index
        self._approval_memory = set(chat_input.approval_memory)
        self._approval_counter = chat_input.approval_counter
        self._user_ref = chat_input.user_ref
        self._conversation_id = chat_input.conversation_id
        self._github_connection_id = chat_input.github_connection_id
        self._mcp_servers = list(chat_input.mcp_servers)
        self._available_tool_names = set(
            chat_input.available_tool_names
            or tool_names_for_connections(
                github_connection_id=chat_input.github_connection_id,
                mcp_servers=chat_input.mcp_servers,
            )
        )
        tools = build_tools(
            available_tool_names=lambda: self._available_tool_names,
            user_ref=lambda: self._user_ref,
            conversation_id=lambda: self._conversation_id,
            workflow_id=lambda: workflow.info().workflow_id,
            github_connection_id=lambda: self._github_connection_id,
            mcp_servers=lambda: self._mcp_servers,
            default_model=lambda: chat_input.model,
            request_mutating_tool_approval=self._request_tool_approval,
        )
        self._agent = ClaudeAgent(
            chat_input.system_prompt,
            tools,
            model=chat_input.model,
            max_tokens=chat_input.max_tokens,
            stream_id=chat_input.stream_id or workflow.info().workflow_id,
        )
        self._status = "idle"
        resume_agent_state = chat_input.agent_state

        while True:
            if resume_agent_state is None:
                await workflow.wait_condition(
                    lambda: self._delete_requested or len(self._pending_messages) > 0
                )
                if self._delete_requested:
                    return
                queued_message = self._pending_messages.pop(0)
                message = queued_message.content
                self._active_message_index = queued_message.transcript_index
            else:
                message = None
            self._status = "responding"
            try:
                result = await self._run_agent_turn(
                    message=message,
                    state=resume_agent_state,
                    max_turns=chat_input.max_turns,
                )
                resume_agent_state = None
                if result.needs_continue_as_new:
                    workflow.continue_as_new(
                        self._continue_as_new_input(
                            chat_input,
                            result.continuation_state,
                        )
                    )
                self._transcript.append(
                    ChatMessage(
                        role="assistant",
                        content=_assistant_text(result.message),
                    )
                )
            except Exception as err:
                self._transcript.append(
                    ChatMessage(
                        role="system",
                        content=f"{type(err).__name__}: {err}",
                    )
                )
            finally:
                self._active_message_index = None
                self._status = "idle"

    def _enqueue_chat(self, message: str) -> None:
        transcript_index = len(self._transcript)
        self._transcript.append(ChatMessage(role="user", content=message))
        self._pending_messages.append(
            QueuedChatMessage(
                content=message,
                transcript_index=transcript_index,
            )
        )

    async def _run_agent_turn(
        self,
        *,
        message: str | None,
        state: ClaudeAgentState | None,
        max_turns: int,
    ) -> ClaudeAgentResult:
        if self._agent is None:
            raise RuntimeError("Agent has not been initialized")
        if state is not None:
            return await self._agent.run(state=state, max_turns=max_turns)
        return await self._agent.run(message, max_turns=max_turns)

    async def _request_tool_approval(
        self,
        tool_name: str,
        tool_args: dict[str, Any],
    ) -> ApprovalDecision:
        memory_key = _approval_memory_key(tool_name, tool_args)
        if memory_key in self._approval_memory:
            return "allow"

        self._approval_counter += 1
        approval_id = f"approval-{self._approval_counter}"
        self._pending_approvals[approval_id] = PendingApproval(
            approval_id=approval_id,
            tool_name=tool_name,
            tool_args=dict(tool_args),
            summary=_approval_summary(tool_name, tool_args),
            memory_key=memory_key,
        )

        await workflow.wait_condition(
            lambda: approval_id in self._approval_decisions
        )
        decision = self._approval_decisions.pop(approval_id)
        self._pending_approvals.pop(approval_id, None)

        if decision == "always_allow":
            self._approval_memory.add(memory_key)

        return decision

    def _continue_as_new_input(
        self,
        chat_input: SimpleChatInput,
        agent_state: ClaudeAgentState | None,
    ) -> SimpleChatInput:
        return SimpleChatInput(
            user_ref=self._user_ref or chat_input.user_ref,
            conversation_id=self._conversation_id or chat_input.conversation_id,
            system_prompt=chat_input.system_prompt,
            model=chat_input.model,
            max_tokens=chat_input.max_tokens,
            max_turns=chat_input.max_turns,
            stream_id=chat_input.stream_id,
            available_tool_names=sorted(self._available_tool_names),
            github_connection_id=self._github_connection_id,
            mcp_servers=list(self._mcp_servers),
            agent_state=agent_state,
            transcript=list(self._transcript),
            pending_messages=list(self._pending_messages),
            active_message_index=self._active_message_index,
            approval_memory=sorted(self._approval_memory),
            approval_counter=self._approval_counter,
        )


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


def _approval_memory_key(tool_name: str, tool_args: dict[str, Any]) -> str:
    owner = tool_args.get("owner")
    repo = tool_args.get("repo")
    if isinstance(owner, str) and isinstance(repo, str):
        return f"{tool_name}:{owner}/{repo}"
    return tool_name


def _approval_summary(tool_name: str, tool_args: dict[str, Any]) -> str:
    if tool_name == "github_open_issue":
        owner = tool_args.get("owner")
        repo = tool_args.get("repo")
        title = tool_args.get("title")
        if isinstance(owner, str) and isinstance(repo, str):
            if isinstance(title, str) and title:
                return f"Open GitHub issue in {owner}/{repo}: {title}"
            return f"Open GitHub issue in {owner}/{repo}"
    if tool_name == "python_sandbox":
        code = tool_args.get("code")
        if isinstance(code, str) and code.strip():
            preview = " ".join(code.strip().split())
            if len(preview) > 96:
                preview = f"{preview[:93]}..."
            return f"Execute Python sandbox code: {preview}"
        return "Execute Python sandbox code"
    if tool_name == "create_artifact":
        name = tool_args.get("name")
        if isinstance(name, str) and name.strip():
            return f"Create artifact: {name.strip()}"
        return "Create artifact"
    return f"Run mutating tool: {tool_name}"
