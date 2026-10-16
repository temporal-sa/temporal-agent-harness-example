"""A minimal file agent on temporal-agent-harness and Pydantic AI. See README.md."""

from __future__ import annotations

import os

from pydantic_ai.durable_exec.temporal import PydanticAIPlugin
from temporal_agent_harness.plugin import AgentHarnessPlugin
from temporalio.client import Client

TASK_QUEUE = "basic-file-agent"
WORKFLOW_TYPE = "BasicFileAgent"


async def connect(tools: tuple = ()) -> Client:
    """A client with the converters the harness needs. The harness plugin goes last."""
    return await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "127.0.0.1:7244"),
        namespace=os.environ.get("TEMPORAL_NAMESPACE", "default"),
        plugins=[PydanticAIPlugin(), AgentHarnessPlugin(tools=list(tools))],
    )
