"""P1 step 1.12: history budget for a 30-tool Sonnet turn.

    RUN_INTEGRATION=1 .venv/bin/python -m tests.integration.p1_budget
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid

import httpx

from tests.integration import support

PROMPT = (
    "Read every file src/m0.py through src/m29.py, ONE AT A TIME with read_file (30 separate "
    "calls, no batching), then write a summary of about 400 words of what the modules do."
)


async def seed(workspace_id: str) -> None:
    url = os.environ["WORKSPACE_EXECUTOR_URL"].rstrip("/")
    async with httpx.AsyncClient() as client:
        for i in range(30):
            body = "\n".join(f"def step_{i}_{j}(x):\n    return x * {j} + {i}\n" for j in range(25))
            r = await client.post(
                f"{url}/files/write",
                json={"workspace_id": workspace_id, "path": f"src/m{i}.py", "content": body, "create_parents": True},
                headers={"Authorization": f"Bearer {os.environ['WORKSPACE_EXECUTOR_TOKEN']}"},
            )
            r.raise_for_status()


async def main() -> None:
    client = await support.connect()
    queue = f"p1b-{uuid.uuid4().hex[:6]}"
    async with support.running_worker(client, queue):
        h = await support.start_session(
            client, queue, model={"provider": "anthropic", "model": "claude-sonnet-5-5", "effort": "low"}, posture="never_ask"
        )
        await seed(h.id)
        async with support.EventCollector(client, h.id) as ev:
            await support.send(h, "chat", {"text": PROMPT})
            await ev.wait_for(lambda c: c.turn_ends() >= 1, timeout=900)
            tools = len(ev.collected.of("tool_end"))
            models = len(ev.collected.of("model_interaction_ended"))
        desc = await h.describe()
        info = desc.raw_description.workflow_execution_info
        print(json.dumps({
            "tool_calls": tools, "model_calls": models,
            "history_events": info.history_length, "history_bytes": info.history_size_bytes,
            "under_10240_events": info.history_length < 10240, "under_10MB": info.history_size_bytes < 10 * 1024 * 1024,
        }))
        await h.signal("close")


if __name__ == "__main__":
    asyncio.run(main())
