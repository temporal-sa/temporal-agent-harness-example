from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Literal

from temporalio import workflow
from temporalio.workflow import ParentClosePolicy

with workflow.unsafe.imports_passed_through():
    from claude_harness.mcp_types import HttpMcpServerConfig
    from simple_chat_agent import TASK_QUEUE
    from simple_chat_agent.workflow import SimpleChatInput, SimpleChatWorkflow


CHAT_REGISTRY_PREFIX = "simple-chat-user-"
ChatStatus = Literal["active", "deleting"]


@dataclass
class UserChatsInput:
    user_id: str


@dataclass
class CreateChatRequest:
    system_prompt: str
    model: str
    max_tokens: int
    max_turns: int
    available_tool_names: list[str] = field(default_factory=list)
    github_connection_id: str | None = None
    mcp_servers: list[HttpMcpServerConfig] = field(default_factory=list)


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
    digest = hashlib.sha256(user_id.encode("utf-8")).hexdigest()[:32]
    return f"{CHAT_REGISTRY_PREFIX}{digest}"


@workflow.defn
class UserChatsWorkflow:
    def __init__(self) -> None:
        self._user_id = ""
        self._chats: dict[str, ChatRecord] = {}
        self._mcp_servers: dict[str, HttpMcpServerConfig] = {}

    @workflow.run
    async def run(self, request: UserChatsInput) -> None:
        self._user_id = request.user_id
        await workflow.wait_condition(lambda: False)

    @workflow.update
    async def create_chat(self, request: CreateChatRequest) -> ChatRecord:
        workflow_id = f"simple-chat-{workflow.uuid4()}"
        handle = await workflow.start_child_workflow(
            SimpleChatWorkflow.run,
            SimpleChatInput(
                user_ref=self._user_id,
                conversation_id=workflow_id,
                system_prompt=request.system_prompt,
                model=request.model,
                max_tokens=request.max_tokens,
                max_turns=request.max_turns,
                stream_id=workflow_id,
                available_tool_names=list(request.available_tool_names),
                github_connection_id=request.github_connection_id,
                mcp_servers=list(request.mcp_servers),
            ),
            id=workflow_id,
            task_queue=TASK_QUEUE,
            parent_close_policy=ParentClosePolicy.ABANDON,
            static_summary="simple chat session",
        )
        now = workflow.now().isoformat()
        record = ChatRecord(
            workflow_id=workflow_id,
            run_id=handle.first_execution_run_id or "",
            title="New chat",
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
