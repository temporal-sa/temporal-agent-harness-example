"""P2 lead checks: session manager (2.3) and ack-bounded truncation (2.2f).

    RUN_INTEGRATION=1 .venv/bin/python -m unittest tests.integration.test_p2_lead -v
"""

from __future__ import annotations

import asyncio
import unittest
import uuid

from tests.integration import support

HAIKU = {"provider": "anthropic", "model": "claude-haiku-5-5", "effort": "low"}


@unittest.skipIf(support.skip_reason(), support.skip_reason() or "")
class P2Lead(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.client = await support.connect()
        self.task_queue = f"p2l-{uuid.uuid4().hex[:6]}"

    async def test_2_3_sessions_survive_continue_as_new(self) -> None:
        from simple_chat_agent.agent.sessions import (
            CodingSessions,
            RenameRequest,
            SessionRecord,
            SessionsState,
            sessions_workflow_id,
        )

        async with support.running_worker(self.client, self.task_queue):
            owner = uuid.uuid4().hex[:32]
            h = await self.client.start_workflow(
                CodingSessions.run, SessionsState(owner_digest=owner),
                id=sessions_workflow_id(owner), task_queue=self.task_queue,
            )
            await h.execute_update(CodingSessions.register_session, SessionRecord(session_id="s1", title="One"))
            await h.execute_update(CodingSessions.register_session, SessionRecord(session_id="s2", title="Two"))
            await h.execute_update(CodingSessions.rename_session, RenameRequest(session_id="s1", title="Renamed"))
            first = (await h.describe()).run_id
            await h.execute_update(CodingSessions.compact)
            for _ in range(40):
                if (await h.describe()).run_id != first:
                    break
                await asyncio.sleep(0.25)
            h2 = self.client.get_workflow_handle(h.id)
            self.assertNotEqual((await h2.describe()).run_id, first)
            listed = await h2.query(CodingSessions.list_sessions)
            self.assertEqual([(s.session_id, s.title) for s in listed], [("s2", "Two"), ("s1", "Renamed")])
            await h2.terminate()

    async def test_2_2f_truncation_respects_the_ack(self) -> None:
        from simple_chat_agent.agent.contracts import STREAM_ACKED_SIGNAL, StreamAck

        async with support.running_worker(self.client, self.task_queue):
            h = await support.start_session(
                self.client, self.task_queue, model=HAIKU, posture="never_ask", force_continue_as_new_after_turns=2
            )
            async with support.EventCollector(self.client, h.id) as ev:
                await support.send(h, "chat", {"text": "Reply with exactly: one"})
                await ev.wait_for(lambda c: c.turn_ends() >= 1)
                stats = await h.query("history_stats")
                acked = stats["stream_head"] - 5  # pretend the bridge stored all but the last 5
                await h.signal(STREAM_ACKED_SIGNAL, StreamAck(offset=acked))
                first = (await h.describe()).run_id
                # A longer second turn, so the head moves well past acked + 50.
                await support.send(h, "chat", {"text": "One at a time, create files t1.txt through t8.txt each containing its number, then say done."})
                await ev.wait_for(lambda c: c.turn_ends() >= 2)
                for _ in range(40):
                    if (await h.describe()).run_id != first:
                        break
                    await asyncio.sleep(0.25)
                after = await self.client.get_workflow_handle(h.id).query("history_stats")
                # The bridge acknowledged `acked`, and the head moved > 50 past it, so the new run
                # keeps exactly what came after the ack.
                self.assertGreater(after["stream_head"], acked + 60)
                self.assertEqual(after["stream_base"], acked)
                self.assertEqual(after["stream_acked"], acked)
            await h.signal("close")


if __name__ == "__main__":
    unittest.main()
