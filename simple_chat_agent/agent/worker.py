"""Production worker for the coding agent (PRD step 1.10).

    python -m simple_chat_agent.agent.worker

Hosts CodingAgent, its subagents, and every activity they schedule. Worker Versioning is on
by default (PINNED; workflows move to a new version only by continuing as new with
AUTO_UPGRADE). Payloads over 256 KB are offloaded (S3 in the catalog). The codec server and
the graceful drain match the previous worker. P5 points the catalog entrypoint
(simple_chat_agent.worker.main) here.
"""

from __future__ import annotations

import asyncio
import os
import signal
from contextlib import suppress
from datetime import timedelta

import uvicorn
from pydantic_ai.durable_exec.temporal import AgentPlugin, PydanticAIPlugin
from temporalio.client import Client
from temporalio.common import VersioningBehavior, WorkerDeploymentVersion
from temporalio.worker import Worker, WorkerDeploymentConfig

from temporal_agent_harness.plugin import AgentHarnessPlugin

from simple_chat_agent.agent.approvals import APPROVAL_ACTIVITIES, typesafe_plugin_if_configured
from simple_chat_agent.agent.harness_ext import payload_storage
from simple_chat_agent.agent.runtime import CODING_AGENT
from simple_chat_agent.agent.sessions import CodingSessions
from simple_chat_agent.agent.subagents import SUBAGENT_WORKFLOWS
from simple_chat_agent.agent.tools.artifacts import ARTIFACT_TOOLS
from simple_chat_agent.agent.tools.research import RESEARCH_TOOLS
from simple_chat_agent.agent.tools.workspace import TEST_TOOLS, WORKSPACE_ACTIVITIES, WORKSPACE_TOOLS
from simple_chat_agent.agent.workflow import CodingAgent
from simple_chat_agent.common.env import load_dotenv
from simple_chat_agent.worker.codec_server import (
    codec_server_enabled,
    codec_server_host,
    codec_server_port,
    codec_server_url,
    create_codec_app,
)
from simple_chat_agent.worker.shutdown import worker_graceful_shutdown_timeout

# A new task queue and deployment name for the rewrite (PRD §6 cutover runbook): nothing on
# the old SimpleChatWorkflow queue can continue-as-new into this code.
DEFAULT_TASK_QUEUE = "coding-agent"


def task_queue() -> str:
    return os.environ.get("CODING_AGENT_TASK_QUEUE", DEFAULT_TASK_QUEUE)


async def connect() -> Client:
    plugins: list = [PydanticAIPlugin()]
    typesafe = typesafe_plugin_if_configured()
    if typesafe is not None:
        plugins.append(typesafe)
    # The harness plugin goes last, so it keeps the Pydantic converter the others installed.
    plugins.append(
        AgentHarnessPlugin(
            tools=[
                *WORKSPACE_TOOLS,
                *RESEARCH_TOOLS,
                *ARTIFACT_TOOLS,
                # Test-only tools are registered only when explicitly enabled.
                *(TEST_TOOLS if os.environ.get("AGENT_ENABLE_TEST_TOOLS") == "1" else ()),
            ],
            large_payload_offload=payload_storage(),
        )
    )
    options: dict = {
        "namespace": os.environ.get("TEMPORAL_NAMESPACE", "default"),
        "tls": os.environ.get("TEMPORAL_TLS", "false").lower() in ("true", "1"),
        "plugins": plugins,
    }
    if os.environ.get("TEMPORAL_API_KEY"):
        options["api_key"] = os.environ["TEMPORAL_API_KEY"]
    address = os.environ.get("TEMPORAL_ADDRESS", os.environ.get("TEMPORAL_ENDPOINT", "localhost:7233"))
    return await Client.connect(address, **options)


def deployment_config(queue: str) -> WorkerDeploymentConfig | None:
    if os.environ.get("CODING_AGENT_WORKER_VERSIONING_ENABLED", "true").strip().lower() not in (
        "1",
        "true",
        "yes",
        "on",
    ):
        return None
    build_id = os.environ.get("TEMPORAL_WORKER_BUILD_ID", os.environ.get("CODING_AGENT_BUILD_ID", "")).strip()
    if not build_id:
        raise RuntimeError(
            "Worker Versioning is on but no build id is set (TEMPORAL_WORKER_BUILD_ID). "
            "Derive it from the source and uv.lock (PRD §6), never reuse one for new code."
        )
    name = os.environ.get("TEMPORAL_DEPLOYMENT_NAME", os.environ.get("CODING_AGENT_DEPLOYMENT_NAME", queue)).strip()
    return WorkerDeploymentConfig(
        version=WorkerDeploymentVersion(deployment_name=name, build_id=build_id),
        use_worker_versioning=True,
        default_versioning_behavior=VersioningBehavior.PINNED,
    )


def build_worker(client: Client, queue: str) -> Worker:
    return Worker(
        client,
        task_queue=queue,
        workflows=[CodingAgent, CodingSessions, *SUBAGENT_WORKFLOWS],
        activities=[*WORKSPACE_ACTIVITIES, *APPROVAL_ACTIVITIES],
        plugins=[AgentPlugin(CODING_AGENT)],
        deployment_config=deployment_config(queue),
        graceful_shutdown_timeout=worker_graceful_shutdown_timeout(),
        # Heartbeats reach the server at least once a second, so a dead worker is noticed
        # within the 10-15 s heartbeat timeouts and an interrupt reaches its activity fast.
        max_heartbeat_throttle_interval=timedelta(seconds=1),
        default_heartbeat_throttle_interval=timedelta(seconds=1),
    )


async def main() -> None:
    load_dotenv()
    client = await connect()
    queue = task_queue()
    worker = build_worker(client, queue)
    print(f"coding-agent worker: task_queue={queue} versioning={'on' if deployment_config(queue) else 'off'}", flush=True)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError, RuntimeError):
            loop.add_signal_handler(sig, stop.set)

    tasks = [asyncio.create_task(worker.run(), name="worker")]
    codec: uvicorn.Server | None = None
    if codec_server_enabled():
        codec = uvicorn.Server(
            uvicorn.Config(
                create_codec_app(client.data_converter),
                host=codec_server_host(),
                port=codec_server_port(),
                log_level="info",
            )
        )
        print(f"codec server on {codec_server_url()}", flush=True)
        tasks.append(asyncio.create_task(codec.serve(), name="codec"))

    stopper = asyncio.create_task(stop.wait(), name="stop")
    done, _ = await asyncio.wait([*tasks, stopper], return_when=asyncio.FIRST_COMPLETED)
    # Drain: the worker finishes in-flight activities (up to the graceful timeout).
    print("shutting down; draining in-flight activities", flush=True)
    await worker.shutdown()
    if codec is not None:
        codec.should_exit = True
    for task in tasks:
        with suppress(asyncio.CancelledError, Exception):
            await task
    stopper.cancel()
    for task in done:
        if task is not stopper and task.exception():
            raise task.exception()  # type: ignore[misc]


if __name__ == "__main__":
    asyncio.run(main())
