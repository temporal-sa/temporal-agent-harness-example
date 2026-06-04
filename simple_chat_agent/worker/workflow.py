from __future__ import annotations

import asyncio
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from typing import Any, Literal, cast

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from agent_harness.attachments import AttachmentRef
    from agent_harness.agent import AgentResult, AgentState, SteeringMode
    from agent_harness.context_manager import (
        DEFAULT_MAX_CONTEXT_TOKENS,
        ContextSnapshot,
    )
    from agent_harness.errors import UserFacingAgentError
    from agent_harness.mcp_types import HttpMcpServerConfig
    from agent_harness.messages import (
        CONTEXT_COMPACTION_MARKER,
        message_text,
        normalize_message,
        tool_use_blocks,
        visible_user_message_text,
    )
    from agent_harness.providers.claude import (
        ClaudeAgent,
        ClaudeThinkingConfig,
    )
    from simple_chat_agent.worker.tools import build_tools, tool_names_for_connections
    from simple_chat_agent.worker.tools.approval import (
        ApprovalDecision,
        ChildToolApprovalRequest,
        TOOL_APPROVAL_TIMEOUT,
    )
    from simple_chat_agent.worker.good_place_guards import (
        good_place_post_guard,
        good_place_pre_guard,
    )


ChatRole = Literal["user", "assistant", "system"]
DEFAULT_MAX_TOKENS = 32_000
TRANSCRIPT_DELTA_BUFFER_LIMIT = 80
TRANSCRIPT_QUERY_DEFAULT_MAX_BYTES = 512_000
TRANSCRIPT_QUERY_MIN_MAX_BYTES = 16_384
TRANSCRIPT_QUERY_HARD_MAX_BYTES = 1_000_000
TRANSCRIPT_QUERY_BASE_OVERHEAD_BYTES = 2_048
TRANSCRIPT_QUERY_MESSAGE_OVERHEAD_BYTES = 512
TRANSCRIPT_TRUNCATION_NOTICE = (
    "\n\n[Transcript message truncated to keep the workflow query response bounded.]"
)
CHAT_WORKFLOW_RUN_TTL = timedelta(days=15)


@dataclass
class ChatMessage:
    role: ChatRole
    content: str
    attachments: list[AttachmentRef] = field(default_factory=list)


@dataclass
class TranscriptDelta:
    revision: int
    index: int
    message: ChatMessage


@dataclass
class TranscriptDeltaResult:
    from_revision: int
    to_revision: int
    deltas: list[TranscriptDelta]
    needs_snapshot: bool = False
    transcript_length: int = 0
    status: str = "idle"
    pending_messages: int = 0
    active_message_index: int | None = None
    state_revision: int = 0
    limited: bool = False
    byte_limit: int = TRANSCRIPT_QUERY_DEFAULT_MAX_BYTES
    estimated_bytes: int = 0


@dataclass
class QueuedChatMessage:
    content: str
    transcript_index: int
    attachments: list[AttachmentRef] = field(default_factory=list)
    settle_after_revision: int = 0
    available_tool_names: list[str] | None = None
    github_connection_id: str | None = None
    mcp_servers: list[HttpMcpServerConfig] | None = None


@dataclass
class ChatSignalRequest:
    message: str
    attachments: list[AttachmentRef] = field(default_factory=list)
    after_revision: int = 0
    available_tool_names: list[str] = field(default_factory=list)
    github_connection_id: str | None = None
    mcp_servers: list[HttpMcpServerConfig] = field(default_factory=list)


@dataclass
class PendingApproval:
    approval_id: str
    tool_name: str
    tool_args: dict
    summary: str
    memory_key: str
    expires_at: str | None = None
    requesting_workflow_id: str | None = None
    requesting_approval_id: str | None = None


@dataclass
class SimpleChatInput:
    """Durable chat workflow state carried across Continue-As-New.

    `agent_state` is hot resume state for a Continue-As-New that happened
    mid-agent-run. When present, the next run resumes the agent loop without a
    new user message.

    `agent_context_state` is idle context state for a workflow between turns.
    It restores the compacted model-visible context before the next user
    message starts. Both fields use the same AgentState shape, but they
    represent different points in the workflow state machine.
    """

    user_ref: str = "local-user"
    conversation_id: str = "local-conversation"
    system_prompt: str = "You are a concise test chatbot."
    model: str = "claude-sonnet-4-5"
    max_tokens: int = DEFAULT_MAX_TOKENS
    max_context_tokens: int = DEFAULT_MAX_CONTEXT_TOKENS
    thinking: ClaudeThinkingConfig | None = None
    max_turns: int = 20
    stream_id: str | None = None
    available_tool_names: list[str] = field(default_factory=list)
    github_connection_id: str | None = None
    mcp_servers: list[HttpMcpServerConfig] = field(default_factory=list)
    agent_state: AgentState | None = None
    agent_context_state: AgentState | None = None
    pending_messages: list[QueuedChatMessage] = field(default_factory=list)
    active_message_index: int | None = None
    active_settle_after_revision: int = 0
    approval_memory: list[str] = field(default_factory=list)
    approval_counter: int = 0
    good_place_censor: bool = False
    state_revision: int = 0
    transcript_revision: int = 0
    transcript_deltas: list[TranscriptDelta] = field(default_factory=list)
    last_touched_at: str = ""


@dataclass
class SimpleChatState:
    status: str
    pending_messages: int
    user_ref: str | None = None
    conversation_id: str | None = None
    model: str | None = None
    thinking: ClaudeThinkingConfig | None = None
    available_tool_names: list[str] = field(default_factory=list)
    github_connected: bool = False
    mcp_servers: list[HttpMcpServerConfig] = field(default_factory=list)
    pending_approvals: list[PendingApproval] = field(default_factory=list)
    active_message_index: int | None = None
    queued_message_indices: list[int] = field(default_factory=list)
    transcript: list[ChatMessage] = field(default_factory=list)
    transcript_length: int = 0
    state_revision: int = 0
    transcript_revision: int = 0


@dataclass
class TranscriptPage:
    messages: list[ChatMessage]
    start: int
    end: int
    total: int
    transcript_revision: int
    limited: bool = False
    byte_limit: int = TRANSCRIPT_QUERY_DEFAULT_MAX_BYTES
    estimated_bytes: int = 0


@dataclass
class SimpleChatSnapshot:
    state: SimpleChatState
    transcript_page: TranscriptPage


@workflow.defn
class SimpleChatWorkflow:
    def __init__(self) -> None:
        self._pending_messages: list[QueuedChatMessage] = []
        self._status = "starting"
        self._agent: ClaudeAgent | None = None
        self._agent_context_state: AgentState | None = None
        self._active_message_index: int | None = None
        self._active_message: QueuedChatMessage | None = None
        self._user_ref: str | None = None
        self._conversation_id: str | None = None
        self._model: str | None = None
        self._thinking: ClaudeThinkingConfig | None = None
        self._available_tool_names: set[str] = set()
        self._github_connection_id: str | None = None
        self._mcp_servers: list[HttpMcpServerConfig] = []
        self._pending_approvals: dict[str, PendingApproval] = {}
        self._approval_decisions: dict[str, ApprovalDecision] = {}
        self._approval_memory: set[str] = set()
        self._approval_counter = 0
        self._delete_requested = False
        self._stream_id: str | None = None
        self._state_revision = 0
        self._transcript_revision = 0
        self._transcript_deltas: list[TranscriptDelta] = []
        self._run_started_at: datetime | None = None
        self._last_touched_at: datetime | None = None
        self._touched_this_run = False
        self._active_settle_after_revision = 0

    @workflow.signal
    async def chat(self, request: ChatSignalRequest) -> None:
        self._touch()
        transcript_index = self._enqueue_chat(
            request.message,
            attachments=request.attachments,
            settle_after_revision=request.after_revision,
            available_tool_names=request.available_tool_names,
            github_connection_id=request.github_connection_id,
            mcp_servers=request.mcp_servers,
        )
        self._record_transcript_change(
            transcript_index,
            _chat_message_for_queue(self._pending_messages[-1]),
        )
        self._record_state_change()

    @workflow.signal
    async def delete(self) -> None:
        self._delete_requested = True
        self._record_state_change()

    @workflow.signal
    async def steer(
        self,
        message: str,
        mode: str = "immediate",
        attachments: list[AttachmentRef] | None = None,
    ) -> None:
        self._touch()
        if attachments:
            transcript_index = self._enqueue_chat(message, attachments=attachments)
            self._record_transcript_change(
                transcript_index,
                _chat_message_for_queue(self._pending_messages[-1]),
            )
            self._record_state_change()
            return
        if mode not in ("immediate", "after_next_tool_result"):
            self._record_system_message(f"Unknown steering mode: {mode}")
            return

        if self._agent is None:
            self._record_system_message(
                "Agent is not ready yet; steering was ignored."
            )
            return

        self._agent.steer(message, mode=cast(SteeringMode, mode))
        label = (
            "after the next tool result"
            if mode == "after_next_tool_result"
            else "before the next provider call"
        )
        self._record_system_message(f"Steering queued {label}: {message}")

    @workflow.signal
    async def interrupt(self, message: str) -> None:
        self._touch()
        if self._agent is None or self._status != "responding":
            transcript_index = self._enqueue_chat(message)
            self._record_transcript_change(
                transcript_index,
                _chat_message_for_queue(self._pending_messages[-1]),
            )
            self._record_state_change()
            return

        self._agent.interrupt(message)
        self._record_system_message(
            f"Interrupt sent; provider will continue with: {message}"
        )

    @workflow.signal
    async def resolve_approval(
        self,
        approval_id: str,
        decision: str,
    ) -> None:
        self._touch()
        if decision not in ("allow", "always_allow", "deny"):
            self._record_system_message(f"Unknown approval decision: {decision}")
            return
        approval = self._pending_approvals.get(approval_id)
        if approval is None:
            self._record_system_message(
                f"Approval is no longer pending: {approval_id}"
            )
            return

        if (
            approval.requesting_workflow_id is not None
            and approval.requesting_approval_id is not None
        ):
            self._pending_approvals.pop(approval_id, None)
            if decision == "always_allow":
                self._approval_memory.add(approval.memory_key)
            self._record_state_change()
            await self._signal_child_approval(
                workflow_id=approval.requesting_workflow_id,
                approval_id=approval.requesting_approval_id,
                decision=cast(ApprovalDecision, decision),
            )
            return

        self._approval_decisions[approval_id] = cast(ApprovalDecision, decision)

    @workflow.signal
    async def expire_child_approval(
        self,
        child_workflow_id: str,
        child_approval_id: str,
    ) -> None:
        for approval_id, approval in list(self._pending_approvals.items()):
            if (
                approval.requesting_workflow_id == child_workflow_id
                and approval.requesting_approval_id == child_approval_id
            ):
                self._pending_approvals.pop(approval_id, None)
                self._record_state_change()
                return

    @workflow.signal
    async def request_child_approval(
        self,
        request: ChildToolApprovalRequest,
    ) -> None:
        memory_key = _approval_memory_key(request.tool_name, request.tool_args)
        if memory_key in self._approval_memory:
            await self._signal_child_approval(
                workflow_id=request.child_workflow_id,
                approval_id=request.child_approval_id,
                decision="allow",
            )
            return

        for approval in self._pending_approvals.values():
            if (
                approval.requesting_workflow_id == request.child_workflow_id
                and approval.requesting_approval_id == request.child_approval_id
            ):
                return

        self._approval_counter += 1
        approval_id = f"approval-{self._approval_counter}"
        summary = _approval_summary(request.tool_name, request.tool_args)
        self._pending_approvals[approval_id] = PendingApproval(
            approval_id=approval_id,
            tool_name=request.tool_name,
            tool_args=dict(request.tool_args),
            summary=f"Subagent requested: {summary}",
            memory_key=memory_key,
            expires_at=_approval_expires_at(),
            requesting_workflow_id=request.child_workflow_id,
            requesting_approval_id=request.child_approval_id,
        )
        self._record_state_change()

    @workflow.query
    def state(self) -> SimpleChatState:
        return self._state(include_transcript=False)

    @workflow.query
    def snapshot(
        self,
        limit: int = 60,
        max_bytes: int = TRANSCRIPT_QUERY_DEFAULT_MAX_BYTES,
    ) -> SimpleChatSnapshot:
        return SimpleChatSnapshot(
            state=self._state(include_transcript=False),
            transcript_page=self._transcript_page(
                before=None,
                limit=limit,
                max_bytes=max_bytes,
            ),
        )

    @workflow.query
    def transcript_page(
        self,
        before: int | None = None,
        limit: int = 60,
        max_bytes: int = TRANSCRIPT_QUERY_DEFAULT_MAX_BYTES,
    ) -> TranscriptPage:
        return self._transcript_page(
            before=before,
            limit=limit,
            max_bytes=max_bytes,
        )

    @workflow.query
    def transcript_deltas_since(
        self,
        after_revision: int = 0,
        max_bytes: int = TRANSCRIPT_QUERY_DEFAULT_MAX_BYTES,
    ) -> TranscriptDeltaResult:
        after_revision = max(0, int(after_revision or 0))
        byte_limit = _transcript_byte_limit(max_bytes)
        if after_revision >= self._transcript_revision:
            return self._transcript_delta_result(
                after_revision=after_revision,
                deltas=[],
                needs_snapshot=False,
                byte_limit=byte_limit,
            )

        if not self._transcript_deltas:
            return self._transcript_delta_result(
                after_revision=after_revision,
                deltas=[],
                needs_snapshot=True,
                byte_limit=byte_limit,
            )

        oldest_revision = self._transcript_deltas[0].revision
        if after_revision < oldest_revision - 1:
            return self._transcript_delta_result(
                after_revision=after_revision,
                deltas=[],
                needs_snapshot=True,
                byte_limit=byte_limit,
            )

        deltas = [
            delta
            for delta in self._transcript_deltas
            if delta.revision > after_revision
        ]
        delta_bytes = _transcript_deltas_query_bytes(deltas)
        if delta_bytes > byte_limit:
            return self._transcript_delta_result(
                after_revision=after_revision,
                deltas=[],
                needs_snapshot=True,
                byte_limit=byte_limit,
                limited=True,
                estimated_bytes=byte_limit,
            )

        return self._transcript_delta_result(
            after_revision=after_revision,
            deltas=deltas,
            needs_snapshot=False,
            byte_limit=byte_limit,
        )

    def _state(self, *, include_transcript: bool) -> SimpleChatState:
        transcript = self._rendered_transcript()
        return SimpleChatState(
            status=self._status,
            pending_messages=len(self._pending_messages),
            user_ref=self._user_ref,
            conversation_id=self._conversation_id,
            model=self._model,
            thinking=self._thinking,
            available_tool_names=sorted(self._available_tool_names),
            github_connected=self._github_connection_id is not None,
            mcp_servers=list(self._mcp_servers),
            pending_approvals=list(self._pending_approvals.values()),
            active_message_index=self._active_message_index,
            queued_message_indices=[
                message.transcript_index for message in self._pending_messages
            ],
            transcript=list(transcript) if include_transcript else [],
            transcript_length=len(transcript),
            state_revision=self._state_revision,
            transcript_revision=self._transcript_revision,
        )

    def _transcript_page(
        self,
        *,
        before: int | None,
        limit: int,
        max_bytes: int,
    ) -> TranscriptPage:
        transcript = self._rendered_transcript()
        total = len(transcript)
        page_limit = max(1, min(int(limit or 60), 200))
        byte_limit = _transcript_byte_limit(max_bytes)
        end = total if before is None else max(0, min(int(before), total))
        lower_bound = max(0, end - page_limit)
        messages: list[ChatMessage] = []
        estimated_bytes = TRANSCRIPT_QUERY_BASE_OVERHEAD_BYTES
        limited = False
        start = end
        for index in range(end - 1, lower_bound - 1, -1):
            message = transcript[index]
            message_bytes = _chat_message_query_bytes(message)
            if estimated_bytes + message_bytes <= byte_limit:
                messages.insert(0, message)
                estimated_bytes += message_bytes
                start = index
                continue
            limited = True
            if not messages:
                available = max(
                    0,
                    byte_limit
                    - estimated_bytes
                    - TRANSCRIPT_QUERY_MESSAGE_OVERHEAD_BYTES,
                )
                truncated = _truncate_chat_message_for_query(message, available)
                messages.insert(0, truncated)
                estimated_bytes += _chat_message_query_bytes(truncated)
                start = index
            break
        return TranscriptPage(
            messages=messages,
            start=start,
            end=end,
            total=total,
            transcript_revision=self._transcript_revision,
            limited=limited,
            byte_limit=byte_limit,
            estimated_bytes=min(estimated_bytes, byte_limit),
        )

    def _transcript_delta_result(
        self,
        *,
        after_revision: int,
        deltas: list[TranscriptDelta],
        needs_snapshot: bool,
        byte_limit: int,
        limited: bool = False,
        estimated_bytes: int | None = None,
    ) -> TranscriptDeltaResult:
        if estimated_bytes is None:
            estimated_bytes = _transcript_deltas_query_bytes(deltas)
        return TranscriptDeltaResult(
            from_revision=after_revision,
            to_revision=self._transcript_revision,
            deltas=deltas,
            needs_snapshot=needs_snapshot,
            transcript_length=len(self._rendered_transcript()),
            status=self._status,
            pending_messages=len(self._pending_messages),
            active_message_index=self._active_message_index,
            state_revision=self._state_revision,
            limited=limited,
            byte_limit=byte_limit,
            estimated_bytes=min(estimated_bytes, byte_limit),
        )

    @workflow.run
    async def run(self, chat_input: SimpleChatInput) -> None:
        self._run_started_at = workflow.now()
        self._last_touched_at = _parse_datetime(chat_input.last_touched_at)
        if self._last_touched_at is None:
            self._last_touched_at = self._run_started_at
            self._touched_this_run = True
        else:
            self._touched_this_run = self._last_touched_at > self._run_started_at
        self._pending_messages = list(chat_input.pending_messages)
        if self._pending_messages or chat_input.agent_state is not None:
            self._last_touched_at = self._run_started_at
            self._touched_this_run = True
        self._active_message_index = chat_input.active_message_index
        self._active_settle_after_revision = chat_input.active_settle_after_revision
        self._active_message = None
        self._agent_context_state = chat_input.agent_context_state or chat_input.agent_state
        self._approval_memory = set(chat_input.approval_memory)
        self._approval_counter = chat_input.approval_counter
        self._state_revision = chat_input.state_revision
        self._transcript_revision = chat_input.transcript_revision
        self._transcript_deltas = list(chat_input.transcript_deltas)
        self._user_ref = chat_input.user_ref
        self._conversation_id = chat_input.conversation_id
        self._model = chat_input.model
        self._thinking = chat_input.thinking
        self._github_connection_id = chat_input.github_connection_id
        self._mcp_servers = list(chat_input.mcp_servers)
        self._stream_id = chat_input.stream_id or workflow.info().workflow_id
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
            max_context_tokens=chat_input.max_context_tokens,
            thinking=chat_input.thinking,
            stream_id=self._stream_id,
            pre_llm_guards=[good_place_pre_guard] if chat_input.good_place_censor else None,
            post_llm_guards=[good_place_post_guard] if chat_input.good_place_censor else None,
        )
        # Idle state restores the compacted context before waiting for a new
        # user message. Hot resume state is passed into the agent run below.
        if chat_input.agent_context_state is not None and chat_input.agent_state is None:
            self._agent.restore_idle_state(chat_input.agent_context_state)
        self._status = "idle"
        self._record_state_change()
        resume_agent_state = chat_input.agent_state

        while True:
            settle_after_revision = self._active_settle_after_revision
            if resume_agent_state is None:
                if (
                    workflow.all_handlers_finished()
                    and self._runtime_checkpoint_requested()
                ):
                    await self._continue_as_new(chat_input, None)
                if self._checkpoint_due() and workflow.all_handlers_finished():
                    if self._touched_this_run:
                        await self._continue_as_new(chat_input, None)
                    return
                try:
                    await workflow.wait_condition(
                        lambda: self._delete_requested
                        or len(self._pending_messages) > 0
                        or (
                            workflow.all_handlers_finished()
                            and self._runtime_checkpoint_requested()
                        ),
                        timeout=self._time_until_checkpoint(),
                    )
                except asyncio.TimeoutError:
                    pass
                if self._delete_requested:
                    return
                if (
                    workflow.all_handlers_finished()
                    and self._runtime_checkpoint_requested()
                ):
                    await self._continue_as_new(chat_input, None)
                if self._checkpoint_due() and workflow.all_handlers_finished():
                    if self._touched_this_run:
                        await self._continue_as_new(chat_input, None)
                    return
                if not self._pending_messages:
                    continue
                queued_message = self._pending_messages.pop(0)
                message = queued_message.content
                attachments = queued_message.attachments
                self._active_message_index = queued_message.transcript_index
                self._active_message = queued_message
                settle_after_revision = queued_message.settle_after_revision
                self._active_settle_after_revision = settle_after_revision
                self._apply_queued_tool_config(queued_message)
            else:
                message = None
                attachments = []
            self._status = "responding"
            self._record_state_change()
            should_emit_settled = False
            continue_as_new_state: AgentState | None = None
            try:
                result = await self._run_agent_turn(
                    message=message,
                    attachments=attachments,
                    state=resume_agent_state,
                    max_turns=chat_input.max_turns,
                )
                resume_agent_state = None
                if (
                    chat_input.good_place_censor
                    and self._active_message_index is not None
                ):
                    effective = await self._agent.effective_user_prompt()
                    if effective is not None:
                        self._record_transcript_change(
                            self._active_message_index,
                            ChatMessage(
                                role="user",
                                content=effective,
                                attachments=list(
                                    self._active_message.attachments
                                    if self._active_message is not None
                                    else []
                                ),
                            ),
                        )
                if result.needs_continue_as_new:
                    continue_as_new_state = result.continuation_state
                    self._agent_context_state = continue_as_new_state
                else:
                    self._agent_context_state = await self._agent.compacted_state()
                self._record_latest_rendered_message_change()
                should_emit_settled = True
            except UserFacingAgentError as err:
                self._record_system_message(err.message)
                should_emit_settled = True
            finally:
                self._active_message_index = None
                self._active_message = None
                self._active_settle_after_revision = 0
                self._status = "idle"
                self._record_state_change()
                if should_emit_settled:
                    await self._emit_turn_settled(settle_after_revision)
            if continue_as_new_state is not None:
                await self._continue_as_new(chat_input, continue_as_new_state)

    def _touch(self) -> None:
        self._last_touched_at = workflow.now()
        self._touched_this_run = True

    def _checkpoint_due(self) -> bool:
        if self._run_started_at is None:
            return False
        return workflow.now() >= self._run_started_at + CHAT_WORKFLOW_RUN_TTL

    def _runtime_checkpoint_requested(self) -> bool:
        info = workflow.info()
        return (
            info.is_continue_as_new_suggested()
            or info.is_target_worker_deployment_version_changed()
        )

    def _time_until_checkpoint(self) -> timedelta:
        if self._run_started_at is None:
            return CHAT_WORKFLOW_RUN_TTL
        remaining = (self._run_started_at + CHAT_WORKFLOW_RUN_TTL) - workflow.now()
        return max(remaining, timedelta())

    def _rendered_transcript(self) -> list[ChatMessage]:
        snapshot = self._context_snapshot()
        messages = snapshot.get("messages")
        if not isinstance(messages, list):
            transcript: list[ChatMessage] = []
        else:
            transcript = [
                rendered
                for message in messages
                if (rendered := _chat_message_from_agent_message(message)) is not None
            ]

        if self._active_message is not None:
            _overlay_transcript_message(
                transcript,
                self._active_message.transcript_index,
                _chat_message_for_queue(self._active_message),
            )
        for pending in self._pending_messages:
            _overlay_transcript_message(
                transcript,
                pending.transcript_index,
                _chat_message_for_queue(pending),
            )
        return transcript

    def _context_snapshot(self) -> ContextSnapshot:
        if self._agent is not None:
            snapshot = self._agent.context_snapshot()
            messages = snapshot.get("messages")
            if isinstance(messages, list) and messages:
                return snapshot
        if self._agent_context_state is not None:
            return self._agent_context_state.context_snapshot
        return {"version": 2, "messages": []}

    def _next_transcript_index(self) -> int:
        return len(self._rendered_transcript())

    def _enqueue_chat(
        self,
        message: str,
        *,
        attachments: list[AttachmentRef] | None = None,
        settle_after_revision: int = 0,
        available_tool_names: list[str] | None = None,
        github_connection_id: str | None = None,
        mcp_servers: list[HttpMcpServerConfig] | None = None,
    ) -> int:
        attachment_refs = list(attachments or [])
        transcript_index = self._next_transcript_index()
        self._pending_messages.append(
            QueuedChatMessage(
                content=message,
                transcript_index=transcript_index,
                attachments=attachment_refs,
                settle_after_revision=max(0, int(settle_after_revision or 0)),
                available_tool_names=(
                    list(available_tool_names)
                    if available_tool_names is not None
                    else None
                ),
                github_connection_id=github_connection_id,
                mcp_servers=list(mcp_servers) if mcp_servers is not None else None,
            )
        )
        return transcript_index

    def _apply_queued_tool_config(self, queued_message: QueuedChatMessage) -> None:
        if queued_message.available_tool_names is None:
            return
        self._available_tool_names = set(queued_message.available_tool_names)
        self._github_connection_id = queued_message.github_connection_id
        self._mcp_servers = list(queued_message.mcp_servers or [])

    async def _emit_turn_settled(self, after_revision: int) -> None:
        if not self._stream_id:
            return
        after_revision = max(0, int(after_revision or 0))
        result = self.transcript_deltas_since(
            after_revision,
            TRANSCRIPT_QUERY_DEFAULT_MAX_BYTES,
        )
        payload = _transcript_delta_result_payload(result)
        payload["settled"] = not _transcript_delta_result_still_live(payload)
        await workflow.execute_activity(
            "simple_chat_agent.emit_turn_settled",
            {
                "stream_id": self._stream_id,
                "workflow_id": workflow.info().workflow_id,
                "idempotency_key": (
                    f"{workflow.info().workflow_id}:"
                    f"{self._transcript_revision}:"
                    f"{self._state_revision}:"
                    f"{after_revision}"
                ),
                "result": payload,
            },
            start_to_close_timeout=timedelta(seconds=15),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=1),
                maximum_interval=timedelta(seconds=30),
            ),
        )

    async def _run_agent_turn(
        self,
        *,
        message: str | None,
        attachments: list[AttachmentRef] | None,
        state: AgentState | None,
        max_turns: int,
    ) -> AgentResult:
        if self._agent is None:
            raise RuntimeError("Agent has not been initialized")
        if state is not None:
            return await self._agent.run(state=state, max_turns=max_turns)
        return await self._agent.run(
            message,
            attachments=list(attachments or []),
            max_turns=max_turns,
        )

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
            expires_at=_approval_expires_at(),
        )
        self._record_state_change()

        try:
            await workflow.wait_condition(
                lambda: approval_id in self._approval_decisions
                or self._approval_wait_cancelled(),
                timeout=TOOL_APPROVAL_TIMEOUT,
                timeout_summary=f"approval:{approval_id}",
            )
        except asyncio.TimeoutError:
            decision: ApprovalDecision = "expired"
        else:
            if approval_id in self._approval_decisions:
                decision = self._approval_decisions.pop(approval_id)
            else:
                decision = "cancelled"
        self._pending_approvals.pop(approval_id, None)
        self._record_state_change()

        if decision == "always_allow":
            self._approval_memory.add(memory_key)

        return decision

    def _approval_wait_cancelled(self) -> bool:
        return self._delete_requested or (
            self._agent is not None and self._agent.interrupt_requested
        )

    async def _signal_child_approval(
        self,
        *,
        workflow_id: str,
        approval_id: str,
        decision: ApprovalDecision,
    ) -> None:
        try:
            await workflow.get_external_workflow_handle(workflow_id).signal(
                "resolve_delegated_approval",
                args=[approval_id, decision],
            )
        except Exception as err:
            self._record_system_message(
                "Could not deliver approval decision to subagent "
                f"{workflow_id}: {type(err).__name__}: {err}"
            )

    def _record_state_change(self) -> None:
        self._state_revision += 1

    def _record_transcript_change(
        self,
        index: int | None = None,
        message: ChatMessage | None = None,
    ) -> None:
        self._transcript_revision += 1
        if index is None or index < 0 or message is None:
            return
        self._transcript_deltas.append(
            TranscriptDelta(
                revision=self._transcript_revision,
                index=index,
                message=message,
            )
        )
        if len(self._transcript_deltas) > TRANSCRIPT_DELTA_BUFFER_LIMIT:
            self._transcript_deltas = self._transcript_deltas[
                -TRANSCRIPT_DELTA_BUFFER_LIMIT:
            ]

    def _record_system_message(self, content: str) -> None:
        index = self._next_transcript_index()
        self._record_transcript_change(
            index,
            ChatMessage(role="system", content=content),
        )

    def _record_latest_rendered_message_change(self) -> None:
        transcript = self._rendered_transcript()
        if not transcript:
            self._record_transcript_change()
            return
        self._record_transcript_change(len(transcript) - 1, transcript[-1])

    async def _continue_as_new_input(
        self,
        chat_input: SimpleChatInput,
        agent_state: AgentState | None,
    ) -> SimpleChatInput:
        agent_context_state = None
        if agent_state is None and self._agent is not None:
            agent_context_state = await self._agent.compacted_state()
        return SimpleChatInput(
            user_ref=self._user_ref or chat_input.user_ref,
            conversation_id=self._conversation_id or chat_input.conversation_id,
            system_prompt=chat_input.system_prompt,
            model=chat_input.model,
            max_tokens=chat_input.max_tokens,
            max_context_tokens=chat_input.max_context_tokens,
            thinking=chat_input.thinking,
            max_turns=chat_input.max_turns,
            stream_id=chat_input.stream_id,
            available_tool_names=sorted(self._available_tool_names),
            github_connection_id=self._github_connection_id,
            mcp_servers=list(self._mcp_servers),
            agent_state=agent_state,
            agent_context_state=agent_context_state,
            pending_messages=list(self._pending_messages),
            active_message_index=self._active_message_index,
            active_settle_after_revision=self._active_settle_after_revision,
            approval_memory=sorted(self._approval_memory),
            approval_counter=self._approval_counter,
            good_place_censor=chat_input.good_place_censor,
            state_revision=self._state_revision,
            transcript_revision=self._transcript_revision + 1,
            transcript_deltas=[],
            last_touched_at=(
                self._last_touched_at.isoformat()
                if self._last_touched_at is not None
                else chat_input.last_touched_at
            ),
        )

    async def _continue_as_new(
        self,
        chat_input: SimpleChatInput,
        agent_state: AgentState | None,
    ) -> None:
        workflow.continue_as_new(
            await self._continue_as_new_input(chat_input, agent_state),
            initial_versioning_behavior=(
                workflow.ContinueAsNewVersioningBehavior.AUTO_UPGRADE
            ),
        )


def _transcript_delta_result_payload(result: TranscriptDeltaResult) -> dict[str, Any]:
    return {
        "from_revision": result.from_revision,
        "to_revision": result.to_revision,
        "needs_snapshot": result.needs_snapshot,
        "transcript_length": result.transcript_length,
        "status": result.status,
        "pending_messages": result.pending_messages,
        "active_message_index": result.active_message_index,
        "state_revision": result.state_revision,
        "limited": result.limited,
        "byte_limit": result.byte_limit,
        "estimated_bytes": result.estimated_bytes,
        "deltas": [
            {
                "revision": delta.revision,
                "index": delta.index,
                "message": asdict(delta.message),
            }
            for delta in result.deltas
        ],
    }


def _transcript_delta_result_still_live(result: dict[str, Any]) -> bool:
    if result.get("status") in {"responding", "starting"}:
        return True
    try:
        if int(result.get("pending_messages") or 0) > 0:
            return True
    except (TypeError, ValueError):
        return True
    return result.get("active_message_index") is not None


_ATTACHMENT_LINE_RE = re.compile(
    r"^\s*\d+\.\s+(?P<name>.*?)\s+\("
    r"attachment_id=(?P<attachment_id>[^,)]*),\s+"
    r"mime_type=(?P<mime_type>[^,)]*),\s+"
    r"kind=(?P<content_kind>[^,)]*),\s+"
    r"size_bytes=(?P<size_bytes>\d+)\)"
)


def _chat_message_from_agent_message(value: Any) -> ChatMessage | None:
    try:
        agent_message = normalize_message(value)
    except ValueError:
        return None

    content = agent_message["content"]
    if isinstance(content, list) and any(
        _block_dict(block).get("type") == CONTEXT_COMPACTION_MARKER
        for block in content
    ):
        text = message_text(agent_message).strip()
        return ChatMessage(role="system", content=text) if text else None

    if agent_message["role"] == "user":
        text = visible_user_message_text(agent_message).strip()
        attachments = _attachments_from_agent_message(agent_message)
        if not text and not attachments:
            return None
        return ChatMessage(role="user", content=text, attachments=attachments)

    if tool_use_blocks(agent_message):
        return None

    text = _assistant_text(agent_message).strip()
    if not text:
        return None
    return ChatMessage(role="assistant", content=text)


def _attachments_from_agent_message(message: dict[str, Any]) -> list[AttachmentRef]:
    content = message.get("content")
    if isinstance(content, str):
        return _attachments_from_text(content)
    if not isinstance(content, list):
        return []

    attachments: list[AttachmentRef] = []
    for block in content:
        text = _block_text(_block_dict(block))
        if text.lstrip().startswith("<attachments>"):
            attachments.extend(_attachments_from_text(text))
    return attachments


def _attachments_from_text(text: str) -> list[AttachmentRef]:
    if "<attachments>" not in text:
        return []

    attachments: list[AttachmentRef] = []
    for line in text.splitlines():
        match = _ATTACHMENT_LINE_RE.match(line)
        if not match:
            continue
        groups = match.groupdict()
        attachments.append(
            AttachmentRef(
                attachment_id=groups["attachment_id"],
                name=groups["name"],
                mime_type=groups["mime_type"],
                size_bytes=int(groups["size_bytes"]),
                content_kind=cast(Any, groups["content_kind"]),
            )
        )
    return attachments


def _overlay_transcript_message(
    transcript: list[ChatMessage],
    index: int,
    message: ChatMessage,
) -> None:
    if index < 0:
        return
    if index < len(transcript):
        transcript[index] = message
        return
    transcript.append(message)


def _chat_message_for_queue(message: QueuedChatMessage) -> ChatMessage:
    return ChatMessage(
        role="user",
        content=message.content,
        attachments=list(message.attachments),
    )


def _transcript_byte_limit(max_bytes: int) -> int:
    try:
        requested = int(max_bytes)
    except (TypeError, ValueError):
        requested = TRANSCRIPT_QUERY_DEFAULT_MAX_BYTES
    return max(
        TRANSCRIPT_QUERY_MIN_MAX_BYTES,
        min(requested, TRANSCRIPT_QUERY_HARD_MAX_BYTES),
    )


def _transcript_deltas_query_bytes(deltas: list[TranscriptDelta]) -> int:
    total = TRANSCRIPT_QUERY_BASE_OVERHEAD_BYTES
    for delta in deltas:
        total += TRANSCRIPT_QUERY_MESSAGE_OVERHEAD_BYTES
        total += _text_bytes(str(delta.revision))
        total += _text_bytes(str(delta.index))
        total += _chat_message_query_bytes(delta.message)
    return total


def _chat_message_query_bytes(message: ChatMessage) -> int:
    total = TRANSCRIPT_QUERY_MESSAGE_OVERHEAD_BYTES
    total += _text_bytes(message.role)
    total += _text_bytes(message.content)
    for attachment in message.attachments:
        total += TRANSCRIPT_QUERY_MESSAGE_OVERHEAD_BYTES
        total += _text_bytes(attachment.attachment_id)
        total += _text_bytes(attachment.name)
        total += _text_bytes(attachment.mime_type)
        total += _text_bytes(str(attachment.size_bytes))
        total += _text_bytes(attachment.content_kind)
        total += _text_bytes(attachment.source)
        total += _text_bytes(attachment.text_preview)
        total += _text_bytes(str(attachment.text_chars or ""))
        total += _text_bytes(attachment.expires_at)
        for key, value in attachment.metadata.items():
            total += _text_bytes(str(key))
            total += _text_bytes(str(value))
    return total


def _truncate_chat_message_for_query(
    message: ChatMessage,
    available_content_bytes: int,
) -> ChatMessage:
    notice_bytes = _text_bytes(TRANSCRIPT_TRUNCATION_NOTICE)
    content_budget = max(0, available_content_bytes - notice_bytes)
    return ChatMessage(
        role=message.role,
        content=_truncate_text_bytes(message.content, content_budget)
        + TRANSCRIPT_TRUNCATION_NOTICE,
        attachments=list(message.attachments),
    )


def _truncate_text_bytes(text: str, max_bytes: int) -> str:
    if max_bytes <= 0:
        return ""
    if _text_bytes(text) <= max_bytes:
        return text
    encoded = text.encode("utf-8")[:max_bytes]
    return encoded.decode("utf-8", errors="ignore")


def _text_bytes(text: str) -> int:
    return len(text.encode("utf-8"))


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


def _parse_datetime(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


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


def _approval_expires_at() -> str:
    return (workflow.now() + TOOL_APPROVAL_TIMEOUT).isoformat()
