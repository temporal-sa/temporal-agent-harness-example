from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Literal, Mapping, cast

from anthropic import AsyncAnthropic
from anthropic.types import MessageParam, ToolParam, ToolResultBlockParam
from temporalio import activity, workflow

from .tools import ToolResult, ToolSet

ClaudeStopReason = Literal[
    "end_turn", "max_tokens", "stop_sequence", "tool_use", "pause_turn", "refusal"
]


class ClaudeAgent:
    def __init__(
        self,
        system_prompt: str,
        tools: ToolSet,
        *,
        model: str,
        max_tokens: int = 4096,
        tool_names: list[str] | None = None,
        stream_id: str | None = None,
    ):
        self._system_prompt = system_prompt
        self._tools = tools
        self._model = model
        self._max_tokens = max_tokens
        self._tool_names = tool_names
        self._stream_id = stream_id

    async def run(self, user_prompt: str, *, max_turns: int = 20) -> ClaudeAgentResult:
        chat_history: list[MessageParam] = [
            MessageParam(role="user", content=user_prompt)
        ]
        tool_schemas = self._tools.tool_schemas(self._tool_names)

        for turn in range(1, max_turns + 1):
            response = await workflow.execute_activity(
                call_claude,
                ClaudeRequest(
                    system_prompt=self._system_prompt,
                    model=self._model,
                    max_tokens=self._max_tokens,
                    tools=tool_schemas,
                    chat_history=chat_history,
                ),
                start_to_close_timeout=timedelta(minutes=2),
                summary="claude",
            )

            if response.stop_reason != "tool_use":
                return ClaudeAgentResult(
                    message=response.message,
                    stop_reason=response.stop_reason,
                    turns=turn,
                )

            chat_history.append(response.message)
            tool_results = await self._execute_requested_tools(response.message)
            chat_history.append(MessageParam(role="user", content=tool_results))

        return ClaudeAgentResult(
            message=MessageParam(
                role="assistant",
                content=f"Stopped after reaching max_turns={max_turns}.",
            ),
            stop_reason="max_tokens",
            turns=max_turns,
        )

    async def _execute_tool(self, tool_name: str, **kwargs: Any) -> ToolResult:
        if self._tool_names is not None and tool_name not in self._tool_names:
            return ToolResult(
                payload={"error": f"Tool is not available to this agent: {tool_name}"},
                error=True,
            )
        return await self._tools.execute_tool(
            tool_name, kwargs, stream_id=self._stream_id
        )

    async def _execute_requested_tools(
        self, message: MessageParam
    ) -> list[ToolResultBlockParam]:
        return await asyncio.gather(
            *[
                self._execute_requested_tool(block)
                for block in _tool_use_blocks(message)
            ]
        )

    async def _execute_requested_tool(
        self, block: dict[str, Any]
    ) -> ToolResultBlockParam:
        tool_name = cast(str, block["name"])
        tool_input = cast(dict[str, Any], block["input"])
        tool_use_id = cast(str, block["id"])

        try:
            result = await self._execute_tool(tool_name, **tool_input)
        except Exception as err:
            result = ToolResult(
                payload={"error": str(err), "type": type(err).__name__},
                error=True,
            )

        return ToolResultBlockParam(
            type="tool_result",
            tool_use_id=tool_use_id,
            content=json.dumps(result.payload),
            is_error=result.error,
        )


@dataclass
class ClaudeAgentResult:
    message: MessageParam
    stop_reason: ClaudeStopReason | None
    turns: int


@dataclass
class ClaudeRequest:
    system_prompt: str
    model: str
    max_tokens: int
    tools: list[ToolParam]
    chat_history: list[MessageParam]


@dataclass
class ClaudeResponse:
    id: str
    model: str
    message: MessageParam
    stop_reason: ClaudeStopReason | None
    stop_sequence: str | None
    usage: dict[str, Any]


@activity.defn
async def call_claude(request: ClaudeRequest) -> ClaudeResponse:
    async with AsyncAnthropic() as client:
        response = await client.messages.create(
            model=request.model,
            max_tokens=request.max_tokens,
            system=request.system_prompt,
            messages=request.chat_history,
            tools=request.tools,
        )

    return ClaudeResponse(
        id=response.id,
        model=response.model,
        message=MessageParam(
            role=response.role,
            content=response.content,
        ),
        stop_reason=response.stop_reason,
        stop_sequence=response.stop_sequence,
        usage=response.usage.to_dict(),
    )


def _tool_use_blocks(message: MessageParam) -> list[dict[str, Any]]:
    content = message["content"]
    if isinstance(content, str):
        return []

    blocks: list[dict[str, Any]] = []
    for block in content:
        block_dict = (
            dict(cast(Mapping[str, Any], block))
            if isinstance(block, dict)
            else block.to_dict()
        )
        if block_dict.get("type") == "tool_use":
            blocks.append(block_dict)
    return blocks
