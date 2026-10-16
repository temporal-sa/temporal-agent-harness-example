"""A standalone worker for the coding agent, for local development and P0 crash tests.

    .venv/bin/python -m simple_chat_agent.agent.dev_worker --task-queue p0

Connects to TEMPORAL_TEST_ADDRESS (default 127.0.0.1:7244). The production worker entrypoint
(simple_chat_agent.worker.main) adopts the same wiring in P1.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import socket
from datetime import timedelta

from pydantic_ai.durable_exec.temporal import AgentPlugin, PydanticAIPlugin
from temporalio.client import Client
from temporalio.worker import Worker

from temporal_agent_harness.plugin import AgentHarnessPlugin

from simple_chat_agent.agent.harness_ext import payload_storage

from simple_chat_agent.agent.runtime import CODING_AGENT
from simple_chat_agent.agent.tools.workspace import TEST_TOOLS, WORKSPACE_TOOLS
from simple_chat_agent.agent.workflow import CodingAgent


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--task-queue", required=True)
    parser.add_argument("--identity", default=f"dev-worker-{socket.gethostname()}-{os.getpid()}")
    args = parser.parse_args()
    client = await Client.connect(
        os.environ.get("TEMPORAL_TEST_ADDRESS", "127.0.0.1:7244"),
        namespace=os.environ.get("TEMPORAL_NAMESPACE", "default"),
        plugins=[PydanticAIPlugin(), AgentHarnessPlugin(
            tools=[*WORKSPACE_TOOLS, *TEST_TOOLS], large_payload_offload=payload_storage()
        )],
    )
    worker = Worker(
        client,
        task_queue=args.task_queue,
        workflows=[CodingAgent],
        plugins=[AgentPlugin(CODING_AGENT)],
        identity=args.identity,
        max_heartbeat_throttle_interval=timedelta(seconds=1),
        default_heartbeat_throttle_interval=timedelta(seconds=1),
    )
    print(f"worker ready identity={args.identity} task_queue={args.task_queue}", flush=True)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
