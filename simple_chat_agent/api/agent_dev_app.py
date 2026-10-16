"""A minimal API app serving only the agent routes, for P0 tests and local experiments.

    .venv/bin/python -m simple_chat_agent.api.agent_dev_app --port 8090 --task-queue p0

Auth is a test stand-in: the ``X-Test-User`` header names the user. P2 mounts the same router
in the real API (simple_chat_agent.api.main) behind catalog / local auth.
"""

from __future__ import annotations

import argparse
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from pydantic_ai.durable_exec.temporal import PydanticAIPlugin
from temporalio.client import Client

from temporal_agent_harness.plugin import AgentHarnessPlugin

from simple_chat_agent.agent.harness_ext import payload_storage
from simple_chat_agent.api.agent_bridge import Bridge
from simple_chat_agent.api.agent_routes import create_agent_router
from simple_chat_agent.api.agent_store import AgentStore


@dataclass
class TestUser:
    user_id: str


def header_user(request: Request) -> TestUser:
    user = request.headers.get("X-Test-User")
    if not user:
        raise HTTPException(status_code=401, detail="X-Test-User required")
    return TestUser(user)


def build_app(*, task_queue: str, temporal_address: str) -> FastAPI:
    state: dict[str, object] = {}

    async def temporal() -> Client:
        if "client" not in state:
            state["client"] = await Client.connect(
                temporal_address,
                namespace=os.environ.get("TEMPORAL_NAMESPACE", "default"),
                plugins=[PydanticAIPlugin(), AgentHarnessPlugin(large_payload_offload=payload_storage())],
            )
        return state["client"]  # type: ignore[return-value]

    store = AgentStore.from_env()

    async def bridge() -> Bridge:
        if "bridge" not in state:
            state["bridge"] = Bridge(await temporal(), store)
        return state["bridge"]  # type: ignore[return-value]

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        yield
        if "bridge" in state:
            await state["bridge"].aclose()  # type: ignore[union-attr]

    app = FastAPI(lifespan=lifespan)
    app.include_router(
        create_agent_router(
            temporal=temporal,
            store=store,
            bridge=bridge,
            current_user=header_user,
            task_queue=task_queue,
        ),
        prefix="/api/agent",
    )
    return app


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8090)
    parser.add_argument("--task-queue", required=True)
    args = parser.parse_args()
    app = build_app(
        task_queue=args.task_queue,
        temporal_address=os.environ.get("TEMPORAL_TEST_ADDRESS", "127.0.0.1:7244"),
    )
    uvicorn.run(app, host="127.0.0.1", port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
