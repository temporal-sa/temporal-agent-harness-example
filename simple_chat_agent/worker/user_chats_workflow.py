from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Literal

from temporalio import workflow
from temporalio.common import (
    SearchAttributeKey,
    SearchAttributePair,
    TypedSearchAttributes,
)
from temporalio.workflow import ParentClosePolicy

with workflow.unsafe.imports_passed_through():
    from claude_harness.claude_agent import ClaudeThinkingConfig
    from claude_harness.mcp_types import HttpMcpServerConfig
    from simple_chat_agent import TASK_QUEUE
    from simple_chat_agent.worker.workflow import (
        ChatMessage,
        QueuedChatMessage,
        SimpleChatInput,
        SimpleChatWorkflow,
    )


CHAT_REGISTRY_PREFIX = "simple-chat-user-"
ChatStatus = Literal["active", "deleting"]


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
    max_turns: int
    thinking: ClaudeThinkingConfig | None = None
    initial_message: str | None = None
    available_tool_names: list[str] = field(default_factory=list)
    github_connection_id: str | None = None
    mcp_servers: list[HttpMcpServerConfig] = field(default_factory=list)
    good_place_censor: bool = False


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


@dataclass
class UpdateMcpServerRequest:
    server: HttpMcpServerConfig
    available_tool_names: list[str]
    github_connection_id: str | None = None


@dataclass
class DeleteMcpServerRequest:
    server_id: str
    available_tool_names: list[str]
    github_connection_id: str | None = None


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

    @workflow.run
    async def run(self, request: UserChatsInput) -> None:
        self._user_id = request.user_id
        self._user_email = request.user_email
        self._search_attr_name = request.search_attr_name
        # Restore registry state (carried across continue-as-new). Empty on a
        # first start; the chat/MCP updates that follow repopulate it on replay.
        self._chats = {chat.workflow_id: chat for chat in request.chats}
        self._mcp_servers = {
            server.server_id: server for server in request.mcp_servers
        }

        # This entity workflow receives an update on every chat create/touch/
        # forget and MCP change, so its history grows unbounded. Continue-as-new
        # when the server suggests it AND no update handler is mid-flight (so we
        # never drop an in-flight update), carrying the full registry state
        # forward. Child chat workflows are unaffected (ParentClosePolicy.ABANDON
        # + external-handle lifecycle ops).
        await workflow.wait_condition(
            lambda: workflow.info().is_continue_as_new_suggested()
            and workflow.all_handlers_finished()
        )
        workflow.continue_as_new(
            UserChatsInput(
                user_id=self._user_id,
                user_email=self._user_email,
                search_attr_name=self._search_attr_name,
                chats=list(self._chats.values()),
                mcp_servers=list(self._mcp_servers.values()),
            )
        )

    @workflow.update
    async def create_chat(self, request: CreateChatRequest) -> ChatRecord:
        workflow_id = f"simple-chat-{workflow.uuid4()}"
        initial_message = (
            request.initial_message.strip()
            if request.initial_message is not None
            else ""
        )
        transcript = (
            [ChatMessage(role="user", content=initial_message)]
            if initial_message
            else []
        )
        pending_messages = (
            [QueuedChatMessage(content=initial_message, transcript_index=0)]
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
                thinking=request.thinking,
                max_turns=request.max_turns,
                stream_id=workflow_id,
                available_tool_names=list(request.available_tool_names),
                github_connection_id=request.github_connection_id,
                mcp_servers=list(request.mcp_servers),
                transcript=transcript,
                pending_messages=pending_messages,
                good_place_censor=request.good_place_censor,
            ),
            id=workflow_id,
            task_queue=TASK_QUEUE,
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
        )
        self._chats[workflow_id] = record
        return record

    @workflow.update
    async def touch_chat(self, request: TouchChatRequest) -> ChatRecord | None:
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
        )
        self._chats[request.workflow_id] = updated
        return updated

    @workflow.update
    async def forget_chat(self, workflow_id: str) -> None:
        self._chats.pop(workflow_id, None)

    @workflow.update
    async def upsert_mcp_server(
        self, request: UpdateMcpServerRequest
    ) -> list[HttpMcpServerConfig]:
        self._mcp_servers[request.server.server_id] = request.server
        await self._broadcast_tool_connections(
            request.available_tool_names,
            request.github_connection_id,
        )
        return self.list_mcp_servers()

    @workflow.update
    async def delete_mcp_server(
        self, request: DeleteMcpServerRequest
    ) -> list[HttpMcpServerConfig]:
        self._mcp_servers.pop(request.server_id, None)
        await self._broadcast_tool_connections(
            request.available_tool_names,
            request.github_connection_id,
        )
        return self.list_mcp_servers()

    @workflow.update
    async def delete_chat(self, workflow_id: str) -> None:
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
        )

        handle = workflow.get_external_workflow_handle(workflow_id)
        try:
            await handle.signal(SimpleChatWorkflow.delete)
            await handle.cancel()
        except Exception:
            pass

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

    async def _broadcast_tool_connections(
        self,
        available_tool_names: list[str],
        github_connection_id: str | None,
    ) -> None:
        mcp_servers = self.list_mcp_servers()
        for record in self._chats.values():
            if record.status != "active":
                continue
            handle = workflow.get_external_workflow_handle(record.workflow_id)
            try:
                await handle.signal(
                    SimpleChatWorkflow.update_tool_connections,
                    args=[
                        available_tool_names,
                        github_connection_id,
                        mcp_servers,
                    ],
                )
            except Exception:
                pass


def _conversation_title(message: str) -> str:
    normalized = " ".join(message.split())
    if not normalized:
        return "New chat"
    if len(normalized) <= 64:
        return normalized
    return f"{normalized[:61]}..."
