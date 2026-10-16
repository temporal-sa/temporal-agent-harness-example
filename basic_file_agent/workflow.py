from __future__ import annotations

from dataclasses import dataclass

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from agent_harness.agent import AgentState
    from agent_harness.guards import GuardPolicy
    from agent_harness.messages import message_text
    from agent_harness.providers.claude import ClaudeAgent
    from agent_harness.tools import ToolSet
    from basic_file_agent.tool_types import BasicFileToolType
    from basic_file_agent.tools import read_file, write_file


DEFAULT_MODEL = "claude-sonnet-4-5"
DEFAULT_MAX_TOKENS = 4_096
DEFAULT_INSTRUCTIONS = (
    "You are a small file-editing agent. Use read_file when you need to inspect "
    "existing workspace files. Use write_file when the user asks you to create "
    "or update a file. Keep your final response concise."
)


@dataclass
class BasicFileAgentRequest:
    prompt: str
    instructions: str = DEFAULT_INSTRUCTIONS
    model: str = DEFAULT_MODEL
    max_tokens: int = DEFAULT_MAX_TOKENS
    agent_state: AgentState | None = None


@dataclass
class BasicFileAgentResult:
    message: str
    stop_reason: str | None
    turns: int


@workflow.defn
class BasicFileAgentWorkflow:
    @workflow.run
    async def run(self, request: BasicFileAgentRequest) -> BasicFileAgentResult:
        tools = ToolSet(
            guard_policy=GuardPolicy.require_pre(BasicFileToolType.WRITE_FILE),
            tools=[read_file, write_file],
        )

        agent = ClaudeAgent(
            request.instructions,
            tools,
            model=request.model,
            max_tokens=request.max_tokens,
            stream_id=workflow.info().workflow_id,
            stream_agent={
                "id": workflow.info().workflow_id,
                "parent_id": None,
                "kind": "main",
                "label": "Main agent",
            },
        )

        if request.agent_state is None:
            result = await agent.run(request.prompt)
        else:
            result = await agent.run(state=request.agent_state)

        if result.needs_continue_as_new:
            workflow.continue_as_new(
                BasicFileAgentRequest(
                    prompt=request.prompt,
                    instructions=request.instructions,
                    model=request.model,
                    max_tokens=request.max_tokens,
                    agent_state=result.continuation_state,
                ),
                initial_versioning_behavior=(
                    workflow.ContinueAsNewVersioningBehavior.AUTO_UPGRADE
                ),
            )

        return BasicFileAgentResult(
            message=message_text(result.message),
            stop_reason=result.stop_reason,
            turns=result.turns,
        )
