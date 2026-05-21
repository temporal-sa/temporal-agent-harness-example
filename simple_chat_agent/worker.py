from __future__ import annotations

import asyncio

from temporalio.client import Client
from temporalio.worker import Worker

from claude_harness.claude_agent import call_claude
from claude_harness.guards import run_guard_activity
from claude_harness.mcp import (
    configure_mcp_auth_resolver,
    configure_mcp_http_auth_resolver,
)
from claude_harness.streaming import configure_stream_sink
from claude_harness.tools import run_tool_activity
from simple_chat_agent import TASK_QUEUE
from simple_chat_agent.env import load_dotenv
from simple_chat_agent.external_storage import simple_chat_data_converter
from simple_chat_agent.mcp_auth import resolve_mcp_auth_headers, resolve_mcp_http_auth
from simple_chat_agent.streaming import JsonlStreamSink
from simple_chat_agent.tools.subagent import SubagentWorkflow
from simple_chat_agent.user_chats_workflow import UserChatsWorkflow
from simple_chat_agent.workflow import SimpleChatWorkflow


async def main() -> None:
    load_dotenv()
    configure_stream_sink(JsonlStreamSink())
    configure_mcp_auth_resolver(resolve_mcp_auth_headers)
    configure_mcp_http_auth_resolver(resolve_mcp_http_auth)
    client = await Client.connect(
        "localhost:7233",
        data_converter=simple_chat_data_converter(),
    )

    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[SimpleChatWorkflow, UserChatsWorkflow, SubagentWorkflow],
        activities=[call_claude, run_tool_activity, run_guard_activity],
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
