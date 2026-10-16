"""Record CodingAgent histories for the replay test (P1 step 1.13).

    RUN_INTEGRATION=1 .venv/bin/python -m tests.integration.record_histories

Writes tests/fixtures/histories/<scenario>.json. Re-record whenever workflow code changes on
purpose: a recorded history must replay on the SAME build. Cross-version replay failures are
expected under PINNED and are never a reason to add patching (PRD §6).
"""

from __future__ import annotations

import asyncio
import json
import uuid
from pathlib import Path

from tests.integration import support

OUT = support.ROOT / "tests" / "fixtures" / "histories"
HAIKU = {"provider": "anthropic", "model": "claude-haiku-5-5", "effort": "low"}


async def _save(client, workflow_id: str, name: str, *, run_id: str | None = None) -> None:
    handle = client.get_workflow_handle(workflow_id, run_id=run_id)
    history = await handle.fetch_history()
    OUT.mkdir(parents=True, exist_ok=True)
    path = OUT / f"{name}.json"
    path.write_text(json.dumps(json.loads(history.to_json()), indent=1))
    print(f"recorded {path.relative_to(support.ROOT)} ({len(history.events)} events)")


async def main() -> None:
    client = await support.connect()
    queue = f"rec-{uuid.uuid4().hex[:6]}"
    async with support.running_worker(client, queue):
        # 1. A tool turn with an approval, then a provider switch.
        h = await support.start_session(client, queue, model=HAIKU, posture="trust_safe")
        async with support.EventCollector(client, h.id) as ev:
            await support.send(h, "chat", {"text": "Create notes/a.txt containing 'alpha', then read it back."})
            await ev.wait_for(lambda c: c.of("tool_approval_requested"))
            await support.approve(h, ev.collected.of("tool_approval_requested")[0].event.tool_id, remember=True)
            await ev.wait_for(lambda c: c.turn_ends() >= 1)
            await support.send(h, "set_model", {"model": {"provider": "openai", "model": "gpt-5.4-mini"}})
            await support.send(h, "chat", {"text": "Use update_plan for a one-step plan marked done, then say ok."})
            await ev.wait_for(lambda c: c.turn_ends() >= 3, timeout=180)
        await h.signal("close")
        await h.result()
        await _save(client, h.id, "tool_turn_approval_switch")

        # 2. An interrupt during a tool activity.
        h = await support.start_session(client, queue, model=HAIKU, posture="never_ask", test_tools=True)
        async with support.EventCollector(client, h.id) as ev:
            await support.send(h, "chat", {"text": "Call wait_seconds with seconds=30, then reply finished."})
            await ev.wait_for(lambda c: any(e.event.tool_name == "wait_seconds" for e in c.of("tool_start")))
            await support.send(h, "interrupt", {})
            await ev.wait_for(lambda c: c.turn_ends() >= 1)
        await h.signal("close")
        await h.result()
        await _save(client, h.id, "interrupt_during_tool")

        # 3. A forced continue-as-new: record the first run (ends in continue-as-new).
        h = await support.start_session(client, queue, model=HAIKU, posture="never_ask", force_continue_as_new_after_turns=1)
        first = (await h.describe()).run_id
        async with support.EventCollector(client, h.id) as ev:
            await support.send(h, "chat", {"text": "Create c.txt containing 'gamma'."})
            await ev.wait_for(lambda c: c.turn_ends() >= 1)
            for _ in range(40):
                if (await h.describe()).run_id != first:
                    break
                await asyncio.sleep(0.5)
        await _save(client, h.id, "continue_as_new_first_run", run_id=first)
        await h.signal("close")


if __name__ == "__main__":
    asyncio.run(main())
