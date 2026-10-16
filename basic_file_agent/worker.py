from __future__ import annotations

import asyncio
import os

from temporalio.client import Client
from temporalio.worker import Worker

from agent_harness.guards import run_guard_activity
from agent_harness.providers.claude import call_agent_api
from agent_harness.streaming import emit_stream_event_activity
from agent_harness.tools import run_tool_activity
from basic_file_agent import TASK_QUEUE
from basic_file_agent.env import env_flag, load_dotenv
from basic_file_agent.workflow import BasicFileAgentWorkflow


async def main() -> None:
    load_dotenv()

    client_config = {
        "namespace": os.environ.get("TEMPORAL_NAMESPACE", "default"),
        "tls": env_flag("TEMPORAL_TLS", default=False),
    }
    if os.environ.get("TEMPORAL_API_KEY"):
        client_config["api_key"] = os.environ["TEMPORAL_API_KEY"]

    client = await Client.connect(
        os.environ.get("TEMPORAL_ENDPOINT", "localhost:7233"),
        **client_config,
    )

    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[BasicFileAgentWorkflow],
        activities=[
            call_agent_api,
            run_tool_activity,
            run_guard_activity,
            emit_stream_event_activity,
        ],
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
