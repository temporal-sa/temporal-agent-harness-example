from __future__ import annotations

import asyncio
import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Literal

from temporalio import workflow
from temporalio.common import (
    SearchAttributeKey,
    SearchAttributePair,
    TypedSearchAttributes,
)
from temporalio.exceptions import ApplicationError
from temporalio.workflow import ParentClosePolicy

with workflow.unsafe.imports_passed_through():
    from agent_harness.context_manager import DEFAULT_MAX_CONTEXT_TOKENS
    from agent_harness.mcp_types import HttpMcpServerConfig
    from agent_harness.providers.claude import ClaudeThinkingConfig
    from simple_chat_agent import TASK_QUEUE
    from simple_chat_agent.worker.workflow import (
        QueuedChatMessage,
        SimpleChatInput,
        SimpleChatWorkflow,
    )


CHAT_REGISTRY_PREFIX = "simple-chat-user-"
ChatStatus = Literal["active", "deleting"]
USER_WORKFLOW_RUN_TTL = timedelta(days=15)


@dataclass
class UserChatsInput:
    user_id: str
    # Creator's email and the (registered) search-attribute name to tag both this
    # workflow and its child chat workflows with. Resolved by the web layer (the
    # deterministic boundary) so the workflow never reads the environment. Empty
    # name disables the search attribute (e.g. local dev without it registered).
    user_email: str = ""
    search_attr_name: str = ""
    # Registry state carried across continue-as-new so the new run keeps tracking
    # every chat and MCP server. Empty on a first start.
    chats: list[ChatRecord] = field(default_factory=list)
    mcp_servers: list[HttpMcpServerConfig] = field(default_factory=list)
    last_touched_at: str = ""


def user_email_search_attributes(
    *,
    search_attr_name: str,
    user_email: str,
) -> TypedSearchAttributes | None:
    """Typed search attributes carrying the creator email, or None when disabled.

    Applied via start options (start_workflow / start_child_workflow) so there is
    no extra workflow action/history cost.
    """
    if not search_attr_name or not user_email:
        return None
    key = SearchAttributeKey.for_keyword(search_attr_name)
    return TypedSearchAttributes([SearchAttributePair(key, user_email)])


@dataclass
class CreateChatRequest:
    system_prompt: str
    model: str
    max_tokens: int
    max_context_tokens: int = DEFAULT_MAX_CONTEXT_TOKENS
    thinking: ClaudeThinkingConfig | None = None
    initial_message: str | None = None
    available_tool_names: list[str] = field(default_factory=list)
    github_connection_id: str | None = None
    mcp_servers: list[HttpMcpServerConfig] = field(default_factory=list)
    good_place_censor: bool = False
    task_queue: str = TASK_QUEUE
    reference_time: str = ""


@dataclass
class TouchChatRequest:
    workflow_id: str
    title: str | None = None


@dataclass
class ChatRecord:
    workflow_id: str
    run_id: str
    title: str
    status: ChatStatus
    created_at: str
    updated_at: str
    task_queue: str = TASK_QUEUE


@dataclass
class UpdateMcpServerRequest:
    server: HttpMcpServerConfig


@dataclass
class DeleteMcpServerRequest:
    server_id: str


def user_chats_workflow_id(user_id: str) -> str:
    # Called only from the web layer (never inside workflow code), so reading the
    # environment here is safe. The prefix isolates a test stack's registries
    # from prod in the shared namespace (empty in prod).
    import os

    digest = hashlib.sha256(user_id.encode("utf-8")).hexdigest()[:32]
    prefix = os.environ.get("SIMPLE_CHAT_WORKFLOW_PREFIX", "")
    return f"{prefix}{CHAT_REGISTRY_PREFIX}{digest}"


@workflow.defn
class UserChatsWorkflow:
    def __init__(self) -> None:
        self._user_id = ""
        self._user_email = ""
        self._search_attr_name = ""
        self._chats: dict[str, ChatRecord] = {}
        self._mcp_servers: dict[str, HttpMcpServerConfig] = {}
        self._run_started_at: datetime | None = None
        self._last_touched_at: datetime | None = None
        self._touched_this_run = False

    @workflow.run
    async def run(self, request: UserChatsInput) -> None:
        self._run_started_at = workflow.now()
        self._last_touched_at = _parse_datetime(request.last_touched_at)
        if self._last_touched_at is None:
            self._last_touched_at = self._run_started_at
            self._touched_this_run = True
        else:
            self._touched_this_run = self._last_touched_at > self._run_started_at
        self._user_id = request.user_id
        self._user_email = request.user_email
        self._search_attr_name = request.search_attr_name
        # Restore registry state (carried across continue-as-new). Empty on a
        # first start; the chat/MCP updates that follow repopulate it on replay.
        self._chats = {chat.workflow_id: chat for chat in request.chats}
        self._mcp_servers = {server.server_id: server for server in request.mcp_servers}

        # This entity workflow receives updates on every chat create/touch/forget
        # and MCP change. Checkpoint active runs before claim-checked
        # payload lifecycle can expire; if no update touched this run for a full
        # TTL, close the registry and its tracked chat workflows.
        while True:
            try:
                await workflow.wait_condition(
                    lambda: (
                        (
                            workflow.info().is_continue_as_new_suggested()
                            or self._checkpoint_due()
                        )
                        and workflow.all_handlers_finished()
                    ),
                    timeout=self._time_until_checkpoint(),
                )
            except asyncio.TimeoutError:
                pass
            if not workflow.all_handlers_finished():
                continue
            if workflow.info().is_continue_as_new_suggested():
                workflow.continue_as_new(
                    self._continue_as_new_input(),
                    initial_versioning_behavior=(
                        workflow.ContinueAsNewVersioningBehavior.AUTO_UPGRADE
                    ),
                )
            if self._checkpoint_due():
                if self._touched_this_run:
                    workflow.continue_as_new(
                        self._continue_as_new_input(),
                        initial_versioning_behavior=(
                            workflow.ContinueAsNewVersioningBehavior.AUTO_UPGRADE
                        ),
                    )
                await self._delete_all_chats()
                return

    @workflow.update
    async def create_chat(self, request: CreateChatRequest) -> ChatRecord:
        self._touch()
        workflow_id = f"simple-chat-{workflow.uuid4()}"
        task_queue = request.task_queue or TASK_QUEUE
        initial_message = (
            request.initial_message.strip()
            if request.initial_message is not None
            else ""
        )
        pending_messages = (
            [
                QueuedChatMessage(
                    content=initial_message,
                    transcript_index=0,
                    reference_time=request.reference_time,
                )
            ]
            if initial_message
            else []
        )
        handle = await workflow.start_child_workflow(
            SimpleChatWorkflow.run,
            SimpleChatInput(
                user_ref=self._user_id,
                conversation_id=workflow_id,
                system_prompt=request.system_prompt,
                model=request.model,
                max_tokens=request.max_tokens,
                max_context_tokens=request.max_context_tokens,
                thinking=request.thinking,
                stream_id=workflow_id,
                available_tool_names=list(request.available_tool_names),
                github_connection_id=request.github_connection_id,
                mcp_servers=list(request.mcp_servers),
                pending_messages=pending_messages,
                good_place_censor=request.good_place_censor,
                last_touched_at=workflow.now().isoformat(),
            ),
            id=workflow_id,
            task_queue=task_queue,
            parent_close_policy=ParentClosePolicy.ABANDON,
            static_summary="simple chat session",
            search_attributes=user_email_search_attributes(
                search_attr_name=self._search_attr_name,
                user_email=self._user_email,
            ),
        )
        now = workflow.now().isoformat()
        record = ChatRecord(
            workflow_id=workflow_id,
            run_id=handle.first_execution_run_id or "",
            title=_conversation_title(initial_message),
            status="active",
            created_at=now,
            updated_at=now,
            task_queue=task_queue,
        )
        self._chats[workflow_id] = record
        return record

    @workflow.update
    async def touch_chat(self, request: TouchChatRequest) -> ChatRecord | None:
        return self._touch_chat(request)

    @workflow.signal
    def touch_chat_signal(self, request: TouchChatRequest) -> None:
        self._touch_chat(request)

    def _touch_chat(self, request: TouchChatRequest) -> ChatRecord | None:
        self._touch()
        record = self._chats.get(request.workflow_id)
        if record is None:
            return None

        updated = ChatRecord(
            workflow_id=record.workflow_id,
            run_id=record.run_id,
            title=request.title or record.title,
            status=record.status,
            created_at=record.created_at,
            updated_at=workflow.now().isoformat(),
            task_queue=record.task_queue,
        )
        self._chats[request.workflow_id] = updated
        return updated

    @workflow.update
    async def forget_chat(self, workflow_id: str) -> None:
        self._touch()
        self._chats.pop(workflow_id, None)

    @workflow.update
    async def upsert_mcp_server(
        self, request: UpdateMcpServerRequest
    ) -> list[HttpMcpServerConfig]:
        self._touch()
        self._mcp_servers[request.server.server_id] = request.server
        return self.list_mcp_servers()

    @workflow.update
    async def delete_mcp_server(
        self, request: DeleteMcpServerRequest
    ) -> list[HttpMcpServerConfig]:
        self._touch()
        self._mcp_servers.pop(request.server_id, None)
        return self.list_mcp_servers()

    @workflow.update
    async def delete_chat(self, workflow_id: str) -> None:
        self._touch()
        record = self._chats.get(workflow_id)
        if record is None:
            return

        self._chats[workflow_id] = ChatRecord(
            workflow_id=record.workflow_id,
            run_id=record.run_id,
            title=record.title,
            status="deleting",
            created_at=record.created_at,
            updated_at=workflow.now().isoformat(),
            task_queue=record.task_queue,
        )

        handle = workflow.get_external_workflow_handle(workflow_id)
        try:
            await handle.signal(SimpleChatWorkflow.delete)
            await handle.cancel()
        except Exception as err:
            if not _is_missing_external_workflow(err):
                raise

        self._chats.pop(workflow_id, None)


    @workflow.query
    def list_chats(self) -> list[ChatRecord]:
        return sorted(
            self._chats.values(),
            key=lambda chat: chat.updated_at,
            reverse=True,
        )

    @workflow.query
    def has_chat(self, workflow_id: str) -> bool:
        return workflow_id in self._chats


    @workflow.query
    def list_mcp_servers(self) -> list[HttpMcpServerConfig]:
        return sorted(self._mcp_servers.values(), key=lambda server: server.label)

    def _touch(self) -> None:
        self._last_touched_at = workflow.now()
        self._touched_this_run = True

    def _checkpoint_due(self) -> bool:
        if self._run_started_at is None:
            return False
        return workflow.now() >= self._run_started_at + USER_WORKFLOW_RUN_TTL

    def _time_until_checkpoint(self) -> timedelta:
        if self._run_started_at is None:
            return USER_WORKFLOW_RUN_TTL
        remaining = (self._run_started_at + USER_WORKFLOW_RUN_TTL) - workflow.now()
        return max(remaining, timedelta())

    def _continue_as_new_input(self) -> UserChatsInput:
        return UserChatsInput(
            user_id=self._user_id,
            user_email=self._user_email,
            search_attr_name=self._search_attr_name,
            chats=list(self._chats.values()),
            mcp_servers=list(self._mcp_servers.values()),
            last_touched_at=(
                self._last_touched_at.isoformat()
                if self._last_touched_at is not None
                else ""
            ),
        )

    async def _delete_all_chats(self) -> None:
        for record in list(self._chats.values()):
            if record.status != "active":
                continue
            handle = workflow.get_external_workflow_handle(record.workflow_id)
            try:
                await handle.signal(SimpleChatWorkflow.delete)
                await handle.cancel()
            except Exception as err:
                if not _is_missing_external_workflow(err):
                    raise
        self._chats.clear()


def _parse_datetime(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _conversation_title(message: str) -> str:
    normalized = " ".join(message.split())
    if not normalized:
        return "New chat"
    if len(normalized) <= 64:
        return normalized
    return f"{normalized[:61]}..."


def _is_missing_external_workflow(err: Exception) -> bool:
    if not isinstance(err, ApplicationError):
        return False
    error_type = (err.type or "").lower()
    message = str(err).lower()
    return (
        "notfound" in error_type
        or "not_found" in error_type
        or "not found" in message
        or "already completed" in message
        or "not running" in message
    )
