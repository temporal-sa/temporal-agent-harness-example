"""Start an agent, send it one prompt, print its reply:
python -m basic_file_agent.start_workflow "Create notes/hello.md ..."
"""

from __future__ import annotations

import argparse
import asyncio
import uuid

from temporal_agent_harness.harness.agent_client import AgentClient
from temporal_agent_harness.harness.agent_protocol import AgentConfig, MessageHandlerEnd, ToolStartEvent

from basic_file_agent import TASK_QUEUE, WORKFLOW_TYPE, connect


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("prompt")
    return parser.parse_args()


async def main() -> None:
    prompt = _parse_args().prompt
    client = await connect()
    workflow_id = f"basic-file-agent-{uuid.uuid4().hex[:8]}"
    await client.start_workflow(WORKFLOW_TYPE, AgentConfig(), id=workflow_id, task_queue=TASK_QUEUE)
    turn = await AgentClient(client, workflow_id).send_message(
        "chat", {"text": prompt}, on_item=lambda item, _offset: item
    )
    async for item in turn:
        if isinstance(item, Exception):  # AgentTurnError / AgentTurnTimeout
            raise item
        if isinstance(item.event, ToolStartEvent):
            print(f"[tool] {item.event.tool_name}")
        elif isinstance(item.event, MessageHandlerEnd):
            print(item.event.output["text"])
    # One prompt per run here; the agent itself would keep taking messages until closed.
    await client.get_workflow_handle(workflow_id).signal("close")


if __name__ == "__main__":
    asyncio.run(main())
