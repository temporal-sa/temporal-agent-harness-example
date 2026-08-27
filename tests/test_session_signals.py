from __future__ import annotations

import os
import unittest
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from fastapi import HTTPException
from temporalio.api.enums.v1 import EventType
from temporalio.client import Client
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from simple_chat_agent.api.auth import AuthenticatedUser
from simple_chat_agent.api.main import (
    _require_conversation_owner,
    _signal_workflow,
    _touch_conversation,
)
from simple_chat_agent.worker.user_chats_workflow import (
    CreateChatRequest,
    TouchChatRequest,
    UserChatsInput,
    UserChatsWorkflow,
    user_chats_workflow_id,
)
from simple_chat_agent.worker.workflow import SimpleChatWorkflow
from simple_chat_agent.worker.workflow_runner import agent_harness_workflow_runner


class SessionSignalTests(unittest.IsolatedAsyncioTestCase):
    async def test_owner_authorization_uses_temporal_description(self) -> None:
        user = AuthenticatedUser(
            user_id="user_google_123",
            username="owner@example.com",
        )
        workflow_id = "simple-chat-123"
        handle = Mock()
        handle.describe = AsyncMock(
            return_value=SimpleNamespace(
                workflow_type=SimpleChatWorkflow.__name__,
                parent_id=user_chats_workflow_id(user.user_id),
            )
        )

        with (
            patch(
                "simple_chat_agent.api.main._current_user",
                return_value=user,
            ),
            patch(
                "simple_chat_agent.api.main._handle",
                return_value=handle,
            ) as workflow_handle,
        ):
            actual = await _require_conversation_owner(Mock(), workflow_id)

        self.assertIs(actual, user)
        workflow_handle.assert_called_once_with(workflow_id)
        handle.describe.assert_awaited_once_with()

    async def test_owner_authorization_rejects_another_users_chat(self) -> None:
        user = AuthenticatedUser(
            user_id="user_google_123",
            username="owner@example.com",
        )
        handle = Mock()
        handle.describe = AsyncMock(
            return_value=SimpleNamespace(
                workflow_type=SimpleChatWorkflow.__name__,
                parent_id=user_chats_workflow_id("user_google_someone_else"),
            )
        )

        with (
            patch(
                "simple_chat_agent.api.main._current_user",
                return_value=user,
            ),
            patch(
                "simple_chat_agent.api.main._handle",
                return_value=handle,
            ),
            self.assertRaises(HTTPException) as raised,
        ):
            await _require_conversation_owner(Mock(), "simple-chat-other")

        self.assertEqual(raised.exception.status_code, 404)

    async def test_owner_authorization_rejects_another_workflow_type(self) -> None:
        user = AuthenticatedUser(
            user_id="user_google_123",
            username="owner@example.com",
        )
        handle = Mock()
        handle.describe = AsyncMock(
            return_value=SimpleNamespace(
                workflow_type="SubagentWorkflow",
                parent_id=user_chats_workflow_id(user.user_id),
            )
        )

        with (
            patch(
                "simple_chat_agent.api.main._current_user",
                return_value=user,
            ),
            patch(
                "simple_chat_agent.api.main._handle",
                return_value=handle,
            ),
            self.assertRaises(HTTPException) as raised,
        ):
            await _require_conversation_owner(Mock(), "simple-chat-subagent")

        self.assertEqual(raised.exception.status_code, 404)

    async def test_chat_signal_targets_workflow_id_without_run_id(self) -> None:
        handle = Mock()
        handle.signal = AsyncMock()

        with patch(
            "simple_chat_agent.api.main._handle",
            return_value=handle,
        ) as workflow_handle:
            await _signal_workflow(
                Mock(),
                "simple-chat-123",
                SimpleChatWorkflow.resolve_approval,
                args=["approval-1", "allow"],
            )

        workflow_handle.assert_called_once_with("simple-chat-123")
        handle.signal.assert_awaited_once_with(
            SimpleChatWorkflow.resolve_approval,
            args=["approval-1", "allow"],
        )

    async def test_conversation_touch_is_a_registry_signal(self) -> None:
        user_id = "user_google_123"
        registry = Mock()
        registry.signal = AsyncMock()

        with patch(
            "simple_chat_agent.api.main._handle",
            return_value=registry,
        ) as workflow_handle:
            await _touch_conversation(
                user_id,
                "simple-chat-123",
                title="Updated title",
                user_email="owner@example.com",
            )

        workflow_handle.assert_called_once_with(user_chats_workflow_id(user_id))
        registry.signal.assert_awaited_once_with(
            UserChatsWorkflow.touch_chat_signal,
            TouchChatRequest(
                workflow_id="simple-chat-123",
                title="Updated title",
            ),
        )

    async def test_approval_and_touch_signals_land_while_worker_is_stopped(
        self,
    ) -> None:
        user = AuthenticatedUser(
            user_id="user_google_worker_outage",
            username="owner@example.com",
        )
        task_queue = f"session-signals-{uuid.uuid4()}"
        workflow_prefix = f"test-{uuid.uuid4()}-"

        with patch.dict(
            os.environ,
            {"SIMPLE_CHAT_WORKFLOW_PREFIX": workflow_prefix},
        ):
            registry_id = user_chats_workflow_id(user.user_id)
            async with await WorkflowEnvironment.start_local() as env:
                async with Worker(
                    env.client,
                    task_queue=task_queue,
                    workflows=[UserChatsWorkflow, SimpleChatWorkflow],
                    workflow_runner=agent_harness_workflow_runner(),
                ):
                    registry = await env.client.start_workflow(
                        UserChatsWorkflow.run,
                        UserChatsInput(user_id=user.user_id),
                        id=registry_id,
                        task_queue=task_queue,
                    )
                    chat = await registry.execute_update(
                        UserChatsWorkflow.create_chat,
                        CreateChatRequest(
                            system_prompt="Test assistant",
                            model="claude-sonnet-4-5",
                            max_tokens=1_000,
                            task_queue=task_queue,
                        ),
                    )

                # No worker is running below. Describe and Signal are both
                # handled by Temporal itself.
                with (
                    patch(
                        "simple_chat_agent.api.main._current_user",
                        return_value=user,
                    ),
                    patch(
                        "simple_chat_agent.api.main._client",
                        return_value=env.client,
                    ),
                ):
                    owner = await _require_conversation_owner(
                        Mock(),
                        chat.workflow_id,
                    )
                    await _signal_workflow(
                        Mock(),
                        chat.workflow_id,
                        SimpleChatWorkflow.resolve_approval,
                        args=["approval-1", "allow"],
                    )
                    await _touch_conversation(
                        user.user_id,
                        chat.workflow_id,
                        user_email=user.username,
                    )

                self.assertIs(owner, user)
                chat_signals = await _signal_names(env.client, chat.workflow_id)
                registry_signals = await _signal_names(env.client, registry_id)
                self.assertIn("resolve_approval", chat_signals)
                self.assertIn("touch_chat_signal", registry_signals)


async def _signal_names(client: Client, workflow_id: str) -> list[str]:
    history = await client.get_workflow_handle(workflow_id).fetch_history()
    return [
        event.workflow_execution_signaled_event_attributes.signal_name
        for event in history.events
        if event.event_type == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
    ]


if __name__ == "__main__":
    unittest.main()
