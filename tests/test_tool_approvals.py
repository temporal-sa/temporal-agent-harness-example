from __future__ import annotations

import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from simple_chat_agent.worker.workflow import PendingApproval, SimpleChatWorkflow


class ToolApprovalTests(unittest.IsolatedAsyncioTestCase):
    async def test_always_allow_wakes_every_waiting_main_approval_in_scope(
        self,
    ) -> None:
        chat = SimpleChatWorkflow()

        async def wait_until_true(predicate, **_kwargs) -> None:
            while not predicate():
                await asyncio.sleep(0)

        with (
            patch(
                "simple_chat_agent.worker.workflow.workflow.wait_condition",
                side_effect=wait_until_true,
            ),
            patch(
                "simple_chat_agent.worker.workflow._approval_expires_at",
                return_value="2026-07-29T21:00:00+00:00",
            ),
            patch(
                "simple_chat_agent.worker.workflow.workflow.now",
                return_value=None,
            ),
        ):
            first = asyncio.create_task(
                chat._request_tool_approval("python_sandbox", {"code": "print(1)"})
            )
            second = asyncio.create_task(
                chat._request_tool_approval("python_sandbox", {"code": "print(2)"})
            )
            while len(chat._pending_approvals) < 2:
                await asyncio.sleep(0)

            await chat.resolve_approval("approval-1", "always_allow")
            decisions = await asyncio.gather(first, second)

        self.assertEqual(decisions, ["always_allow", "allow"])
        self.assertEqual(chat._approval_memory, {"python_sandbox"})
        self.assertEqual(chat._pending_approvals, {})

    async def test_always_allow_fulfills_every_waiting_child_in_scope(
        self,
    ) -> None:
        chat = SimpleChatWorkflow()
        chat._pending_approvals = {
            "approval-1": PendingApproval(
                approval_id="approval-1",
                tool_name="python_sandbox",
                tool_args={"code": "print(1)"},
                summary="Execute Python sandbox code",
                memory_key="python_sandbox",
                requesting_workflow_id="child-1",
                requesting_approval_id="child-approval-1",
            ),
            "approval-2": PendingApproval(
                approval_id="approval-2",
                tool_name="python_sandbox",
                tool_args={"code": "print(2)"},
                summary="Execute Python sandbox code",
                memory_key="python_sandbox",
                requesting_workflow_id="child-2",
                requesting_approval_id="child-approval-1",
            ),
            "approval-3": PendingApproval(
                approval_id="approval-3",
                tool_name="github_open_issue",
                tool_args={"owner": "temporalio", "repo": "sdk-python"},
                summary="Open GitHub issue",
                memory_key="github_open_issue:temporalio/sdk-python",
                requesting_workflow_id="child-3",
                requesting_approval_id="child-approval-1",
            ),
        }
        chat._signal_child_approval = AsyncMock()

        with patch(
            "simple_chat_agent.worker.workflow.workflow.now",
            return_value=None,
        ):
            await chat.resolve_approval("approval-1", "always_allow")

        self.assertEqual(chat._approval_memory, {"python_sandbox"})
        self.assertEqual(list(chat._pending_approvals), ["approval-3"])
        self.assertEqual(chat._signal_child_approval.await_count, 2)
        chat._signal_child_approval.assert_any_await(
            workflow_id="child-1",
            approval_id="child-approval-1",
            decision="allow",
        )
        chat._signal_child_approval.assert_any_await(
            workflow_id="child-2",
            approval_id="child-approval-1",
            decision="allow",
        )


if __name__ == "__main__":
    unittest.main()
