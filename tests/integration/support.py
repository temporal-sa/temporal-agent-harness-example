"""Shared helpers for the P0 integration tests.

These tests talk to a real Temporal dev server and real LLM providers. They run only when
``RUN_INTEGRATION=1`` and the keys are present (``scripts/dev-secrets.sh`` writes them to
.env.local). Point them at a server with ``TEMPORAL_TEST_ADDRESS`` (default 127.0.0.1:7244).
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]


def load_env_local() -> None:
    env = ROOT / ".env.local"
    if not env.exists():
        return
    for line in env.read_text().splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())


load_env_local()

INTEGRATION = os.environ.get("RUN_INTEGRATION") == "1"
ADDRESS = os.environ.get("TEMPORAL_TEST_ADDRESS", "127.0.0.1:7244")


def skip_reason() -> str | None:
    if not INTEGRATION:
        return "set RUN_INTEGRATION=1 to run integration tests"
    if not os.environ.get("ANTHROPIC_API_KEY"):
        return "ANTHROPIC_API_KEY not set (run scripts/dev-secrets.sh)"
    return None


async def connect() -> Any:
    from pydantic_ai.durable_exec.temporal import PydanticAIPlugin
    from temporalio.client import Client

    from temporal_agent_harness.plugin import AgentHarnessPlugin

    from simple_chat_agent.agent.harness_ext import payload_storage

    from simple_chat_agent.agent.tools.artifacts import ARTIFACT_TOOLS
    from simple_chat_agent.agent.tools.research import RESEARCH_TOOLS
    from simple_chat_agent.agent.tools.workspace import TEST_TOOLS, WORKSPACE_TOOLS

    return await Client.connect(
        ADDRESS,
        namespace="default",
        plugins=[PydanticAIPlugin(), AgentHarnessPlugin(
            tools=[*WORKSPACE_TOOLS, *TEST_TOOLS, *RESEARCH_TOOLS, *ARTIFACT_TOOLS], large_payload_offload=payload_storage()
        )],
    )


@contextlib.asynccontextmanager
async def running_worker(client: Any, task_queue: str) -> AsyncIterator[Any]:
    from pydantic_ai.durable_exec.temporal import AgentPlugin
    from temporalio.worker import Worker

    from simple_chat_agent.agent.runtime import CODING_AGENT
    from simple_chat_agent.agent.sessions import CodingSessions
    from simple_chat_agent.agent.subagents import SUBAGENT_WORKFLOWS
    from simple_chat_agent.agent.workflow import CodingAgent

    from simple_chat_agent.agent.approvals import APPROVAL_ACTIVITIES
    from simple_chat_agent.agent.tools.workspace import WORKSPACE_ACTIVITIES

    worker = Worker(
        client,
        task_queue=task_queue,
        workflows=[CodingAgent, CodingSessions, *SUBAGENT_WORKFLOWS],
        activities=[*WORKSPACE_ACTIVITIES, *APPROVAL_ACTIVITIES],
        plugins=[AgentPlugin(CODING_AGENT)],
        max_heartbeat_throttle_interval=timedelta(seconds=1),
        default_heartbeat_throttle_interval=timedelta(seconds=1),
    )
    async with worker:
        yield worker


async def start_session(client: Any, task_queue: str, **init: Any) -> Any:
    from temporal_agent_harness.harness.agent_protocol import AgentConfig

    from simple_chat_agent.agent.contracts import AGENT_WORKFLOW_TYPE, CodingAgentInit

    workflow_id = f"p0-{uuid.uuid4().hex[:10]}"
    init.setdefault("environment", "local")
    init.setdefault("owner_id", "test-user")
    if "research_tools" not in init:
        from simple_chat_agent.agent.tools.research import research_tools_available

        init["research_tools"] = [t.__name__ for t in research_tools_available()]
    data = CodingAgentInit(session_id=workflow_id, workspace_id=workflow_id, **init)
    return await client.start_workflow(
        AGENT_WORKFLOW_TYPE,
        args=[AgentConfig(), data],
        id=workflow_id,
        task_queue=task_queue,
    )


async def send(handle: Any, op: str, payload: dict[str, Any]) -> str:
    from simple_chat_agent.agent.contracts import ENQUEUE_MESSAGE_SIGNAL, FrontDoorMessage

    request_id = uuid.uuid4().hex
    await handle.signal(
        ENQUEUE_MESSAGE_SIGNAL,
        FrontDoorMessage(request_id=request_id, type=op, payload=payload),
    )
    return request_id


async def approve(handle: Any, tool_id: str, approved: bool = True, remember: bool = False) -> None:
    from simple_chat_agent.agent.contracts import ENQUEUE_APPROVAL_SIGNAL, FrontDoorApproval

    await handle.signal(
        ENQUEUE_APPROVAL_SIGNAL,
        FrontDoorApproval(
            request_id=uuid.uuid4().hex, tool_id=tool_id, approved=approved, remember=remember
        ),
    )


@dataclass
class Collected:
    events: list[Any] = field(default_factory=list)
    # Local monotonic arrival time of each event, parallel to ``events``.
    arrivals: list[float] = field(default_factory=list)

    def of(self, kind: str) -> list[Any]:
        return [e for e in self.events if e.event.type.value == kind]

    def text(self, turn_number: int | None = None) -> str:
        return "".join(
            e.event.text
            for e in self.of("reply_delta")
            if turn_number is None or e.turn_number == turn_number
        )

    def turn_ends(self) -> int:
        return len(self.of("turn_end"))


class EventCollector:
    """Subscribes to the agent's turn_events stream in the background."""

    def __init__(self, client: Any, workflow_id: str) -> None:
        self._client = client
        self._workflow_id = workflow_id
        self.collected = Collected()
        self._task: asyncio.Task[None] | None = None

    async def __aenter__(self) -> EventCollector:
        from temporalio.contrib.workflow_streams import WorkflowStreamClient

        from temporal_agent_harness.harness.agent_protocol import AgentEvent

        stream = WorkflowStreamClient.create(self._client, self._workflow_id)

        async def pump() -> None:
            async for item in stream.subscribe(
                topics=["turn_events"], result_type=AgentEvent, poll_cooldown=timedelta(milliseconds=200)
            ):
                self.collected.events.append(item.data)
                self.collected.arrivals.append(asyncio.get_running_loop().time())

        self._task = asyncio.create_task(pump())
        return self

    async def __aexit__(self, *exc: object) -> None:
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._task

    async def wait_for(self, predicate: Any, timeout: float = 180.0) -> None:
        deadline = asyncio.get_running_loop().time() + timeout
        while not predicate(self.collected):
            if asyncio.get_running_loop().time() > deadline:
                kinds = [e.event.type.value for e in self.collected.events][-30:]
                raise AssertionError(f"timed out; last events: {kinds}")
            if self._task and self._task.done():
                exc = self._task.exception()
                raise AssertionError(f"stream subscription ended: {exc!r}")
            await asyncio.sleep(0.2)
