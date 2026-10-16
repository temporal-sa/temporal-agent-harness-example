"""P0 steps 0.2 and 0.3: a real coding-agent turn with tools and approval, then a provider switch.

Needs a Temporal dev server, the executor and LLM keys. Run with:

    RUN_INTEGRATION=1 .venv/bin/python -m unittest tests.integration.test_p0_agent -v
"""

from __future__ import annotations

import unittest
import uuid

from tests.integration import support


def chat_replies(c) -> list[str]:
    """Final texts of completed chat turns (set_model replies carry `ok`, not `text`)."""
    return [
        e.event.output["text"]
        for e in c.of("message_handler_end")
        if "text" in (e.event.output or {})
    ]


@unittest.skipIf(support.skip_reason(), support.skip_reason() or "")
class AgentTurnTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.client = await support.connect()
        self.task_queue = f"p0-{uuid.uuid4().hex[:6]}"

    async def _approve_pending(self, handle, events, *, count: int = 1) -> None:
        seen: set[str] = set()
        while len(seen) < count:
            await events.wait_for(
                lambda c: any(
                    e.event.tool_id not in seen for e in c.of("tool_approval_requested")
                )
            )
            for e in events.collected.of("tool_approval_requested"):
                if e.event.tool_id not in seen:
                    seen.add(e.event.tool_id)
                    await support.approve(handle, e.event.tool_id)

    async def test_tool_turn_then_switch_providers(self) -> None:
        async with support.running_worker(self.client, self.task_queue):
            handle = await support.start_session(self.client, self.task_queue)
            async with support.EventCollector(self.client, handle.id) as events:
                # 0.2: Claude writes (approval-gated) then reads (safe) a file.
                await support.send(
                    handle,
                    "chat",
                    {
                        "text": "Create the file notes/hello.txt containing exactly the word "
                        "'durable', then read it back and tell me what it contains."
                    },
                )
                await self._approve_pending(handle, events)
                await events.wait_for(lambda c: c.turn_ends() >= 1)
                c = events.collected
                tools = [(e.event.type.value, e.event.tool_name) for e in c.events if e.event.type.value in ("tool_start", "tool_end")]
                self.assertIn(("tool_end", "write_file"), tools)
                self.assertIn(("tool_end", "read_file"), tools)
                self.assertEqual(
                    [e.event.tool_name for e in c.of("tool_approval_requested")], ["write_file"]
                )
                usages = [e.event.usage for e in c.of("model_interaction_ended")]
                self.assertTrue(usages and all(u is not None and u.output_tokens for u in usages), usages)
                self.assertIn("durable", c.text(turn_number=1).lower())

                # 0.3: switch provider; each new provider answers about the earlier tool result.
                for replies, (provider, model) in enumerate(
                    [("openai", "gpt-5.4-mini"), ("google", "gemini-3.5-flash")], start=2
                ):
                    await support.send(
                        handle, "set_model", {"model": {"provider": provider, "model": model}}
                    )
                    await support.send(
                        handle,
                        "chat",
                        {
                            "text": "Without using any tools: what single word did the file you "
                            "created earlier contain? Answer with just the word."
                        },
                    )
                    await events.wait_for(
                        lambda c, n=replies: len(chat_replies(c)) >= n
                        or bool(c.of("message_handler_error")),
                        timeout=180,
                    )
                    self.assertFalse(events.collected.of("message_handler_error"))
                    self.assertIn("durable", chat_replies(events.collected)[-1].lower())
                    models = [e.event.model or "" for e in events.collected.of("model_interaction_started")]
                    # Providers may serve an alias (gemini-3.5-flash answered as gemini-3.6-flash).
                    family = {"openai": "gpt", "google": "gemini"}[provider]
                    self.assertTrue(models[-1].startswith(family), models)
            await handle.signal("close")


if __name__ == "__main__":
    unittest.main()
