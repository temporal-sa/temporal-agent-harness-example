from __future__ import annotations

import unittest
import uuid
from unittest.mock import AsyncMock, Mock, patch

from temporalio import activity, workflow
from temporalio.common import (
    SearchAttributeKey,
    SearchAttributePair,
    TypedSearchAttributes,
)
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from agent_harness.messages import tool_use_block
from agent_harness.providers.claude import (
    ClaudeRequest,
    ClaudeResponse,
    ClaudeThinkingConfig,
)
from agent_harness.tools import ToolSet
from simple_chat_agent import TASK_QUEUE
from simple_chat_agent.worker.tools.subagent import (
    CREATE_SUBAGENT_TOOL,
    MAX_SUBAGENT_DELEGATION_DEPTH,
    SubagentProvider,
    SubagentRequest,
    SubagentResponse,
    SubagentWorkflow,
)
from simple_chat_agent.worker.workflow_runner import agent_harness_workflow_runner


@activity.defn(name="call_agent_api")
async def complete_subagent_call(_request: ClaudeRequest) -> ClaudeResponse:
    return ClaudeResponse(
        id="test-response",
        model="claude-sonnet-4-5",
        message={
            "role": "assistant",
            "content": [{"type": "text", "text": "Research complete."}],
        },
        stop_reason="end_turn",
        stop_sequence=None,
        usage={},
    )


@activity.defn(name="call_agent_api")
async def recursively_delegate_subagent_call(
    request: ClaudeRequest,
) -> ClaudeResponse:
    tool_names = {tool["name"] for tool in request.tools}
    if CREATE_SUBAGENT_TOOL not in tool_names:
        text = "create_subagent was not available."
        stop_reason = "end_turn"
        content: list[dict[str, object]] = [{"type": "text", "text": text}]
    elif request.chat_history[0]["content"] == "Investigate the nested task.":
        stop_reason = "end_turn"
        content = [{"type": "text", "text": "Nested research complete."}]
    else:
        tool_results = [
            block
            for message in request.chat_history
            for block in (
                message["content"] if isinstance(message["content"], list) else []
            )
            if block.get("type") == "tool_result"
        ]
        if not tool_results:
            stop_reason = "tool_use"
            content = [
                tool_use_block(
                    tool_use_id="create-grandchild-1",
                    name=CREATE_SUBAGENT_TOOL,
                    input={"task": "Investigate the nested task."},
                )
            ]
        else:
            nested_result = str(tool_results[-1].get("content", ""))
            stop_reason = "end_turn"
            text = (
                "Parent synthesis complete."
                if "Nested research complete." in nested_result
                else "Nested delegation failed."
            )
            content = [{"type": "text", "text": text}]

    return ClaudeResponse(
        id="test-recursive-response",
        model="claude-sonnet-4-5",
        message={"role": "assistant", "content": content},
        stop_reason=stop_reason,
        stop_sequence=None,
        usage={},
    )


class SubagentSearchAttributeTests(unittest.IsolatedAsyncioTestCase):
    async def test_subagent_workflow_is_valid_in_the_temporal_sandbox(self) -> None:
        agent_harness_workflow_runner().prepare_workflow(
            workflow._Definition.must_from_class(SubagentWorkflow)
        )

    async def test_subagent_builds_workspace_tools_in_the_temporal_sandbox(
        self,
    ) -> None:
        task_queue = f"subagent-sandbox-{uuid.uuid4()}"
        async with (
            await WorkflowEnvironment.start_local() as env,
            Worker(
                env.client,
                task_queue=task_queue,
                workflows=[SubagentWorkflow],
                activities=[complete_subagent_call],
                workflow_runner=agent_harness_workflow_runner(),
            ),
        ):
            result = await env.client.execute_workflow(
                SubagentWorkflow.run,
                SubagentRequest(
                    system_prompt="Complete the test task.",
                    task="Verify tool registration.",
                    model="claude-sonnet-4-5",
                    tool_names=["workspace_shell", "python_sandbox"],
                ),
                id=f"subagent-sandbox-{uuid.uuid4()}",
                task_queue=task_queue,
            )

        self.assertEqual(result.text, "Research complete.")
        self.assertIn("workspace_shell", result.tool_names)
        self.assertIn("python_sandbox", result.tool_names)
        self.assertIn(CREATE_SUBAGENT_TOOL, result.tool_names)

    async def test_child_workflow_inherits_parent_search_attributes(self) -> None:
        user_email = SearchAttributeKey.for_keyword("UserEmail")
        parent_search_attributes = TypedSearchAttributes(
            [SearchAttributePair(user_email, "researcher@example.com")]
        )
        workflow_info = Mock(
            workflow_id="simple-chat-parent",
            typed_search_attributes=parent_search_attributes,
        )
        ctx = Mock(
            stream_id="simple-chat-parent",
            tool_call_id="create-subagent-call-1",
        )
        ctx.tool_names.return_value = ["research", CREATE_SUBAGENT_TOOL]
        thinking = ClaudeThinkingConfig(
            enabled=True,
            mode="adaptive",
            effort="high",
            display="summarized",
        )
        provider = SubagentProvider(
            default_model=lambda: "claude-sonnet-4-5",
            thinking=lambda: thinking,
            user_ref=lambda: "user-123",
            conversation_id=lambda: "simple-chat-parent",
            github_connection_id=lambda: None,
            reference_time=lambda: "2026-07-29T18:42:00Z",
        )
        child_result = SubagentResponse(
            text="Research complete.",
            stop_reason="end_turn",
            turns=3,
            model="claude-sonnet-4-5",
            tool_names=["research"],
            denied_tool_names=[],
        )

        with (
            patch(
                "simple_chat_agent.worker.tools.subagent.workflow.info",
                return_value=workflow_info,
            ),
            patch(
                "simple_chat_agent.worker.tools.subagent.workflow.uuid4",
                return_value="child-id",
            ),
            patch(
                "simple_chat_agent.worker.tools.subagent.workflow."
                "execute_child_workflow",
                new=AsyncMock(return_value=child_result),
            ) as execute_child,
        ):
            result = await provider.create_subagent(
                ctx,
                task="Investigate the question.",
            )

        self.assertIs(
            execute_child.await_args.kwargs["search_attributes"],
            parent_search_attributes,
        )
        child_request = execute_child.await_args.args[1]
        self.assertEqual(
            child_request.tool_names,
            ["research", CREATE_SUBAGENT_TOOL],
        )
        self.assertEqual(child_request.parent_workflow_id, "simple-chat-parent")
        self.assertEqual(
            child_request.immediate_parent_workflow_id,
            "simple-chat-parent",
        )
        self.assertIs(child_request.thinking, thinking)
        self.assertEqual(child_request.reference_time, "2026-07-29T18:42:00Z")
        self.assertEqual(
            child_request.parent_tool_call_id,
            "create-subagent-call-1",
        )
        self.assertEqual(child_request.delegation_depth, 1)
        self.assertIn("focused research subagent", child_request.system_prompt)
        self.assertFalse(result.error)

    async def test_subagent_can_delegate_to_another_subagent(self) -> None:
        async with (
            await WorkflowEnvironment.start_local() as env,
            Worker(
                env.client,
                task_queue=TASK_QUEUE,
                workflows=[SubagentWorkflow],
                activities=[recursively_delegate_subagent_call],
                workflow_runner=agent_harness_workflow_runner(),
            ),
        ):
            result = await env.client.execute_workflow(
                SubagentWorkflow.run,
                SubagentRequest(
                    system_prompt="Delegate the nested task, then synthesize it.",
                    task="Investigate the parent task.",
                    model="claude-sonnet-4-5",
                    parent_workflow_id="simple-chat-main",
                    immediate_parent_workflow_id="simple-chat-main",
                ),
                id=f"simple-chat-main-subagent-{uuid.uuid4()}",
                task_queue=TASK_QUEUE,
            )

        self.assertEqual(result.text, "Parent synthesis complete.")
        self.assertIn(CREATE_SUBAGENT_TOOL, result.tool_names)

    async def test_recursive_child_stays_anchored_to_main_workflow(self) -> None:
        workflow_info = Mock(
            workflow_id="simple-chat-main-subagent-parent",
            typed_search_attributes=TypedSearchAttributes([]),
        )
        ctx = Mock(
            stream_id="simple-chat-main",
            tool_call_id="create-grandchild-call-1",
        )
        ctx.tool_names.return_value = ["research", CREATE_SUBAGENT_TOOL]
        provider = SubagentProvider(
            default_model=lambda: "claude-sonnet-4-5",
            user_ref=lambda: "user-123",
            conversation_id=lambda: "simple-chat-main",
            github_connection_id=lambda: None,
            root_workflow_id=lambda: "simple-chat-main",
            delegation_depth=lambda: 1,
        )
        child_result = SubagentResponse(
            text="Nested research complete.",
            stop_reason="end_turn",
            turns=2,
            model="claude-sonnet-4-5",
            tool_names=["research", CREATE_SUBAGENT_TOOL],
            denied_tool_names=[],
        )

        with (
            patch(
                "simple_chat_agent.worker.tools.subagent.workflow.info",
                return_value=workflow_info,
            ),
            patch(
                "simple_chat_agent.worker.tools.subagent.workflow.uuid4",
                return_value="grandchild-id",
            ),
            patch(
                "simple_chat_agent.worker.tools.subagent.workflow."
                "execute_child_workflow",
                new=AsyncMock(return_value=child_result),
            ) as execute_child,
        ):
            result = await provider.create_subagent(
                ctx,
                task="Investigate a nested workstream.",
                tool_names=["research"],
            )

        child_request = execute_child.await_args.args[1]
        self.assertEqual(
            execute_child.await_args.kwargs["id"],
            "simple-chat-main-subagent-grandchild-id",
        )
        self.assertEqual(child_request.parent_workflow_id, "simple-chat-main")
        self.assertEqual(
            child_request.immediate_parent_workflow_id,
            "simple-chat-main-subagent-parent",
        )
        self.assertEqual(
            child_request.tool_names,
            ["research", CREATE_SUBAGENT_TOOL],
        )
        self.assertEqual(child_request.delegation_depth, 2)
        self.assertFalse(result.error)

    async def test_depth_ten_child_does_not_receive_create_subagent(self) -> None:
        workflow_info = Mock(
            workflow_id="simple-chat-main-subagent-depth-9",
            typed_search_attributes=TypedSearchAttributes([]),
        )
        ctx = Mock(
            stream_id="simple-chat-main",
            tool_call_id="create-depth-10-call",
        )
        ctx.tool_names.return_value = ["research", CREATE_SUBAGENT_TOOL]
        provider = SubagentProvider(
            default_model=lambda: "claude-sonnet-4-5",
            user_ref=lambda: "user-123",
            conversation_id=lambda: "simple-chat-main",
            github_connection_id=lambda: None,
            root_workflow_id=lambda: "simple-chat-main",
            delegation_depth=lambda: MAX_SUBAGENT_DELEGATION_DEPTH - 1,
        )
        child_result = SubagentResponse(
            text="Depth-10 research complete.",
            stop_reason="end_turn",
            turns=1,
            model="claude-sonnet-4-5",
            tool_names=["research"],
            denied_tool_names=[],
        )

        with (
            patch(
                "simple_chat_agent.worker.tools.subagent.workflow.info",
                return_value=workflow_info,
            ),
            patch(
                "simple_chat_agent.worker.tools.subagent.workflow.uuid4",
                return_value="depth-10",
            ),
            patch(
                "simple_chat_agent.worker.tools.subagent.workflow."
                "execute_child_workflow",
                new=AsyncMock(return_value=child_result),
            ) as execute_child,
        ):
            result = await provider.create_subagent(
                ctx,
                task="Complete the final delegated workstream.",
                tool_names=["research", CREATE_SUBAGENT_TOOL],
            )

        child_request = execute_child.await_args.args[1]
        self.assertEqual(
            child_request.delegation_depth,
            MAX_SUBAGENT_DELEGATION_DEPTH,
        )
        self.assertEqual(child_request.tool_names, ["research"])
        self.assertNotIn(CREATE_SUBAGENT_TOOL, child_request.denied_tool_names)
        self.assertFalse(result.error)

    async def test_workflow_enforces_delegation_depth_boundary(self) -> None:
        task_queue = f"subagent-depth-{uuid.uuid4()}"
        async with (
            await WorkflowEnvironment.start_local() as env,
            Worker(
                env.client,
                task_queue=task_queue,
                workflows=[SubagentWorkflow],
                activities=[complete_subagent_call],
                workflow_runner=agent_harness_workflow_runner(),
            ),
        ):
            depth_nine = await env.client.execute_workflow(
                SubagentWorkflow.run,
                SubagentRequest(
                    system_prompt="Complete the test task.",
                    task="Verify depth-nine tools.",
                    model="claude-sonnet-4-5",
                    tool_names=["research", CREATE_SUBAGENT_TOOL],
                    delegation_depth=MAX_SUBAGENT_DELEGATION_DEPTH - 1,
                ),
                id=f"subagent-depth-9-{uuid.uuid4()}",
                task_queue=task_queue,
            )
            depth_ten = await env.client.execute_workflow(
                SubagentWorkflow.run,
                SubagentRequest(
                    system_prompt="Complete the test task.",
                    task="Verify depth-ten tools.",
                    model="claude-sonnet-4-5",
                    tool_names=["research", CREATE_SUBAGENT_TOOL],
                    delegation_depth=MAX_SUBAGENT_DELEGATION_DEPTH,
                ),
                id=f"subagent-depth-10-{uuid.uuid4()}",
                task_queue=task_queue,
            )

        self.assertIn(CREATE_SUBAGENT_TOOL, depth_nine.tool_names)
        self.assertNotIn(CREATE_SUBAGENT_TOOL, depth_ten.tool_names)
        self.assertNotIn(CREATE_SUBAGENT_TOOL, depth_ten.denied_tool_names)

    def test_tool_schema_encourages_selective_delegation_with_one_required_input(
        self,
    ) -> None:
        provider = SubagentProvider(
            default_model=lambda: "claude-sonnet-4-5",
            user_ref=lambda: "user-123",
            conversation_id=lambda: "simple-chat-parent",
            github_connection_id=lambda: None,
        )
        tools = ToolSet(providers=[provider])
        schema = next(
            tool
            for tool in tools.tool_schemas()
            if tool["name"] == CREATE_SUBAGENT_TOOL
        )

        self.assertEqual(schema["input_schema"]["required"], ["task"])
        self.assertIn("Delegate only when", schema["description"])
        self.assertIn("Prefer completing focused work yourself", schema["description"])
        self.assertIn("smallest number of subagents necessary", schema["description"])
        self.assertIn(
            "Recursive delegation should be exceptional", schema["description"]
        )
        self.assertIn("same assistant turn", schema["description"])
        self.assertIn(
            "depth-10 agents do not receive create_subagent", schema["description"]
        )
        self.assertNotIn("Proactively delegate", schema["description"])


if __name__ == "__main__":
    unittest.main()
