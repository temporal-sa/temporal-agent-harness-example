from __future__ import annotations

import argparse
import asyncio
import json
import os
from uuid import uuid4

from temporalio.client import Client

from basic_file_agent import TASK_QUEUE
from basic_file_agent.env import env_flag, load_dotenv
from basic_file_agent.workflow import (
    BasicFileAgentRequest,
    BasicFileAgentWorkflow,
)


async def main() -> None:
    load_dotenv()
    args = _parse_args()

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
    result = await client.execute_workflow(
        BasicFileAgentWorkflow.run,
        BasicFileAgentRequest(
            prompt=args.prompt,
            instructions=args.instructions,
            model=args.model,
        ),
        id=args.workflow_id or f"basic-file-agent-{uuid4()}",
        task_queue=TASK_QUEUE,
    )
    print(json.dumps(result, default=lambda value: vars(value), indent=2))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the basic file agent workflow.")
    parser.add_argument("prompt", help="Task prompt for the agent.")
    parser.add_argument(
        "--instructions",
        default=BasicFileAgentRequest.instructions,
        help="System instructions for the agent.",
    )
    parser.add_argument(
        "--model",
        default=BasicFileAgentRequest.model,
        help="Claude model name.",
    )
    parser.add_argument("--workflow-id", default="", help="Optional workflow id.")
    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(main())
