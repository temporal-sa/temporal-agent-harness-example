from __future__ import annotations

import asyncio
import os
import signal
from contextlib import suppress

import uvicorn
from temporalio.client import Client
from temporalio.common import VersioningBehavior, WorkerDeploymentVersion
from temporalio.worker import Worker, WorkerDeploymentConfig

from agent_harness.guards import run_guard_activity
from agent_harness.mcp import (
    configure_mcp_auth_resolver,
    configure_mcp_http_auth_resolver,
)
from agent_harness.providers.claude import call_agent_api, close_anthropic_clients
from agent_harness.streaming import (
    configure_stream_sink,
    emit_stream_event_activity,
    emit_stream_events_activity,
)
from agent_harness.tools import run_tool_activity
from simple_chat_agent import TASK_QUEUE
from simple_chat_agent.common.env import load_dotenv
from simple_chat_agent.common.external_storage import simple_chat_data_converter
from simple_chat_agent.common.mcp_auth import (
    resolve_mcp_auth_headers,
    resolve_mcp_http_auth,
)
from simple_chat_agent.common.streaming import configured_stream_sink
from simple_chat_agent.worker.codec_server import (
    codec_server_enabled,
    codec_server_host,
    codec_server_port,
    codec_server_url,
    create_codec_app,
)
from simple_chat_agent.worker.demo_workspace_activities import (
    configure_demo_workspace,
    crash_demo_workspace,
    create_demo_workspace_namespace,
    delete_demo_workspace,
    deploy_demo_workspace_workloads,
    provision_demo_workspace,
    purge_demo_workspace_payloads,
    resolve_demo_workspace_images,
    wait_demo_workspace_deployment,
)
from simple_chat_agent.worker.demo_workspace_workflow import DemoWorkspaceWorkflow
from simple_chat_agent.worker.good_place_guards import good_place_post_guard
from simple_chat_agent.worker.shutdown import (
    WORKER_GRACEFUL_SHUTDOWN_SECONDS_ENV,
    worker_graceful_shutdown_seconds,
    worker_graceful_shutdown_timeout,
)
from simple_chat_agent.worker.streaming_activities import emit_turn_settled
from simple_chat_agent.worker.tools.subagent import SubagentWorkflow
from simple_chat_agent.worker.user_chats_workflow import UserChatsWorkflow
from simple_chat_agent.worker.workflow import SimpleChatWorkflow
from simple_chat_agent.worker.workflow_runner import agent_harness_workflow_runner

WORKER_VERSION_ENV = "SIMPLE_CHAT_WORKER_VERSION"
WORKER_VERSIONING_ENABLED_ENV = "SIMPLE_CHAT_WORKER_VERSIONING_ENABLED"
WORKER_DEPLOYMENT_NAME_ENV = "SIMPLE_CHAT_WORKER_DEPLOYMENT_NAME"
DEFAULT_WORKER_VERSION = "1.0.0"


async def main() -> None:
    load_dotenv()
    stream_sink = configured_stream_sink()
    start_stream_sink = getattr(stream_sink, "start", None)
    if start_stream_sink is not None:
        await start_stream_sink()
    configure_stream_sink(
        stream_sink,
        llm_guard=(
            good_place_post_guard
            if os.environ.get("SIMPLE_CHAT_GOOD_PLACE", "1").lower()
            in ("1", "true", "yes")
            else None
        ),
    )
    configure_mcp_auth_resolver(resolve_mcp_auth_headers)
    configure_mcp_http_auth_resolver(resolve_mcp_http_auth)
    data_converter = simple_chat_data_converter()

    client_config = {
        "namespace": os.environ.get("TEMPORAL_NAMESPACE", "default"),
        "data_converter": data_converter,
        "tls": os.environ.get("TEMPORAL_TLS", "false").lower() in ["true", "1"],
    }
    if os.environ.get("TEMPORAL_API_KEY"):
        client_config["api_key"] = os.environ.get("TEMPORAL_API_KEY")

    client = await Client.connect(
        os.environ.get("TEMPORAL_ENDPOINT", "localhost:7233"), **client_config
    )

    deployment_config = _worker_deployment_config(TASK_QUEUE)
    if deployment_config:
        print(
            "Temporal worker versioning enabled: "
            f"deployment={deployment_config.version.deployment_name} "
            f"build_id={deployment_config.version.build_id} "
            f"behavior={deployment_config.default_versioning_behavior.name}"
        )

    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[
            SimpleChatWorkflow,
            UserChatsWorkflow,
            SubagentWorkflow,
            DemoWorkspaceWorkflow,
        ],
        activities=[
            call_agent_api,
            run_tool_activity,
            run_guard_activity,
            emit_stream_event_activity,
            emit_stream_events_activity,
            emit_turn_settled,
            provision_demo_workspace,
            resolve_demo_workspace_images,
            create_demo_workspace_namespace,
            configure_demo_workspace,
            deploy_demo_workspace_workloads,
            wait_demo_workspace_deployment,
            crash_demo_workspace,
            delete_demo_workspace,
            purge_demo_workspace_payloads,
        ],
        workflow_runner=agent_harness_workflow_runner(),
        deployment_config=deployment_config,
        graceful_shutdown_timeout=worker_graceful_shutdown_timeout(),
    )
    print(
        "Temporal worker graceful shutdown enabled: "
        f"timeout_seconds={worker_graceful_shutdown_seconds()} "
        f"env={WORKER_GRACEFUL_SHUTDOWN_SECONDS_ENV}"
    )
    try:
        if not codec_server_enabled():
            await _run_worker_until_stopped(worker)
            return

        codec_server = uvicorn.Server(
            uvicorn.Config(
                create_codec_app(data_converter),
                host=codec_server_host(),
                port=codec_server_port(),
                log_level="info",
            )
        )
        print(f"Temporal Web codec server listening on {codec_server_url()}")
        await _run_worker_and_codec_server(worker, codec_server)
    finally:
        await close_anthropic_clients()
        close = getattr(stream_sink, "close", None)
        if close is not None:
            await close()


async def _run_worker_and_codec_server(
    worker: Worker,
    codec_server: uvicorn.Server,
) -> None:
    shutdown_requested = asyncio.Event()
    loop = asyncio.get_running_loop()
    registered_signals = _register_shutdown_signal_handlers(
        loop,
        shutdown_requested,
    )
    worker_task = asyncio.create_task(worker.run(), name="temporal-worker")
    codec_task = asyncio.create_task(codec_server.serve(), name="codec-server")
    shutdown_task = asyncio.create_task(
        shutdown_requested.wait(),
        name="worker-shutdown-signal",
    )
    tasks = [worker_task, codec_task, shutdown_task]
    try:
        try:
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        except asyncio.CancelledError:
            await _stop_worker_and_codec_server(
                worker,
                worker_task,
                codec_server,
                codec_task,
            )
            raise

        results = await _stop_worker_and_codec_server(
            worker,
            worker_task,
            codec_server,
            codec_task,
        )
    finally:
        shutdown_task.cancel()
        with suppress(asyncio.CancelledError):
            await shutdown_task
        _remove_shutdown_signal_handlers(loop, registered_signals)

    for result in results:
        if isinstance(result, BaseException):
            raise result


async def _run_worker_until_stopped(worker: Worker) -> None:
    shutdown_requested = asyncio.Event()
    loop = asyncio.get_running_loop()
    registered_signals = _register_shutdown_signal_handlers(
        loop,
        shutdown_requested,
    )

    worker_task = asyncio.create_task(worker.run(), name="temporal-worker")
    shutdown_task = asyncio.create_task(
        shutdown_requested.wait(),
        name="worker-shutdown-signal",
    )
    try:
        done, _ = await asyncio.wait(
            [worker_task, shutdown_task],
            return_when=asyncio.FIRST_COMPLETED,
        )
        if shutdown_task in done and not worker_task.done():
            print("Worker shutdown signal received; draining in-flight activities")
            await worker.shutdown()
        await worker_task
    except asyncio.CancelledError:
        if not worker_task.done():
            await worker.shutdown()
        with suppress(asyncio.CancelledError):
            await worker_task
        raise
    finally:
        shutdown_task.cancel()
        with suppress(asyncio.CancelledError):
            await shutdown_task
        _remove_shutdown_signal_handlers(loop, registered_signals)


async def _stop_worker_and_codec_server(
    worker: Worker,
    worker_task: asyncio.Task[None],
    codec_server: uvicorn.Server,
    codec_task: asyncio.Task[None],
) -> list[BaseException | None]:
    if not codec_task.done():
        codec_server.should_exit = True
    if not worker_task.done():
        print("Worker process stopping; draining in-flight activities")
        await worker.shutdown()
    return await asyncio.gather(worker_task, codec_task, return_exceptions=True)


def _register_shutdown_signal_handlers(
    loop: asyncio.AbstractEventLoop,
    shutdown_requested: asyncio.Event,
) -> list[signal.Signals]:
    registered_signals: list[signal.Signals] = []
    for shutdown_signal in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(shutdown_signal, shutdown_requested.set)
        except (NotImplementedError, RuntimeError):
            continue
        registered_signals.append(shutdown_signal)
    return registered_signals


def _remove_shutdown_signal_handlers(
    loop: asyncio.AbstractEventLoop,
    registered_signals: list[signal.Signals],
) -> None:
    for shutdown_signal in registered_signals:
        loop.remove_signal_handler(shutdown_signal)


def _worker_deployment_config(task_queue: str) -> WorkerDeploymentConfig | None:
    if not _env_flag(WORKER_VERSIONING_ENABLED_ENV, default=True):
        print("Temporal worker versioning disabled")
        return None

    build_id = os.environ.get(WORKER_VERSION_ENV, DEFAULT_WORKER_VERSION).strip()
    deployment_name = os.environ.get(WORKER_DEPLOYMENT_NAME_ENV, task_queue).strip()
    if not build_id:
        raise RuntimeError(f"{WORKER_VERSION_ENV} must not be empty")
    if not deployment_name:
        raise RuntimeError(f"{WORKER_DEPLOYMENT_NAME_ENV} must not be empty")

    return WorkerDeploymentConfig(
        version=WorkerDeploymentVersion(
            deployment_name=deployment_name,
            build_id=build_id,
        ),
        use_worker_versioning=True,
        default_versioning_behavior=VersioningBehavior.PINNED,
    )


def _env_flag(name: str, *, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


if __name__ == "__main__":
    asyncio.run(main())
