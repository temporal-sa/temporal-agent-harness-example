"""P0 step 0.9b: measure workflow history and Action-relevant events for one realistic turn
at several stream batch intervals.

    RUN_INTEGRATION=1 .venv/bin/python -m tests.integration.p0_budget

The turn reads 10 files (approval-free) and writes a ~600-word summary. One subscriber reads
the stream throughout, as the Redis bridge will.
"""

from __future__ import annotations

import asyncio
import collections
import json
import os
import subprocess
import sys
import uuid

import httpx

from tests.integration import support

PROMPT = (
    "Read every file src/m0.py through src/m9.py (all ten), then write a summary of about "
    "600 words describing what each module does. Do not modify anything."
)


async def seed(workspace_id: str) -> None:
    url = os.environ.get("WORKSPACE_EXECUTOR_URL", "http://127.0.0.1:8082")
    token = os.environ["WORKSPACE_EXECUTOR_TOKEN"]
    async with httpx.AsyncClient() as client:
        for i in range(10):
            body = "\n".join(
                f"def step_{i}_{j}(x):\n    '''Step {j} of module {i}.'''\n    return x * {j} + {i}\n"
                for j in range(12)
            )
            r = await client.post(
                f"{url}/files/write",
                json={"workspace_id": workspace_id, "path": f"src/m{i}.py", "content": body, "create_parents": True},
                headers={"Authorization": f"Bearer {token}"},
            )
            r.raise_for_status()


async def measure(batch_ms: int) -> dict:
    client = await support.connect()
    task_queue = f"p0b-{uuid.uuid4().hex[:6]}"
    env = {**os.environ, "AGENT_STREAM_BATCH_MS": str(batch_ms)}
    worker = subprocess.Popen(
        [sys.executable, "-m", "simple_chat_agent.agent.dev_worker", "--task-queue", task_queue],
        cwd=str(support.ROOT), env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    try:
        await asyncio.sleep(4)
        handle = await support.start_session(
            client, task_queue, model={"provider": "anthropic", "model": "claude-haiku-5-5", "effort": "low"}
        )
        await seed(handle.id)
        async with support.EventCollector(client, handle.id) as events:
            await support.send(handle, "chat", {"text": PROMPT})
            await events.wait_for(lambda c: c.turn_ends() >= 1, timeout=300)
            stream_events = len(events.collected.events)
            tools = len(events.collected.of("tool_end"))
            words = len(events.collected.text().split())
        desc = await handle.describe()
        kinds: collections.Counter[str] = collections.Counter()
        async for event in handle.fetch_history_events():
            kinds[event.WhichOneof("attributes")] += 1
        await handle.signal("close")
        return {
            "batch_ms": batch_ms,
            "history_events": desc.raw_description.workflow_execution_info.history_length,
            "history_bytes": desc.raw_description.workflow_execution_info.history_size_bytes,
            "signals": kinds["workflow_execution_signaled_event_attributes"],
            "updates_accepted": kinds["workflow_execution_update_accepted_event_attributes"],
            "activities": kinds["activity_task_scheduled_event_attributes"],
            "workflow_tasks": kinds["workflow_task_completed_event_attributes"],
            "stream_events": stream_events,
            "tool_calls": tools,
            "reply_words": words,
        }
    finally:
        worker.kill()


async def main() -> None:
    reason = support.skip_reason()
    if reason:
        sys.exit(reason)
    results = []
    for batch in (50, 250, 500):
        result = await measure(batch)
        print(json.dumps(result), flush=True)
        results.append(result)


if __name__ == "__main__":
    asyncio.run(main())
