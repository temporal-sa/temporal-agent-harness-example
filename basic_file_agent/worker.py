"""Run the worker: python -m basic_file_agent.worker"""

from __future__ import annotations

import asyncio

from pydantic_ai.durable_exec.temporal import AgentPlugin
from temporalio.worker import Worker

from basic_file_agent import TASK_QUEUE, connect
from basic_file_agent.tools import TOOLS
from basic_file_agent.workflow import FILE_AGENT, BasicFileAgent


async def main() -> None:
    # AgentHarnessPlugin(tools=TOOLS) registers the tool activities; AgentPlugin the model's.
    client = await connect(tools=TOOLS)
    worker = Worker(client, task_queue=TASK_QUEUE, workflows=[BasicFileAgent], plugins=[AgentPlugin(FILE_AGENT)])
    print(f"Worker listening on task queue {TASK_QUEUE!r}")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
