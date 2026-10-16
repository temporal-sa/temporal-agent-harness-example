"""P0 smoke: one Claude turn with no tools, end to end. Run directly:

    RUN_INTEGRATION=1 .venv/bin/python -m tests.integration.p0_smoke
"""

from __future__ import annotations

import asyncio
import sys
import uuid

from tests.integration import support


async def main() -> None:
    reason = support.skip_reason()
    if reason:
        sys.exit(reason)
    client = await support.connect()
    task_queue = f"p0-smoke-{uuid.uuid4().hex[:6]}"
    async with support.running_worker(client, task_queue):
        handle = await support.start_session(client, task_queue)
        async with support.EventCollector(client, handle.id) as events:
            await support.send(handle, "chat", {"text": "Reply with exactly: hello from the harness"})
            await events.wait_for(lambda c: c.turn_ends() >= 1)
            kinds = [e.event.type.value for e in events.collected.events]
            print("events:", kinds)
            print("text:", events.collected.text())
            ended = events.collected.of("model_interaction_ended")
            print("usage:", ended[-1].event.usage if ended else None)
        await handle.signal("close")


if __name__ == "__main__":
    asyncio.run(main())
