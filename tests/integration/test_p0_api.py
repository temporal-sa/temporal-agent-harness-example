"""P0 step 0.7: Signal front door with workers down, Redis-backed attach/status, ownership.

    RUN_INTEGRATION=1 SIMPLE_CHAT_REDIS_URL=redis://127.0.0.1:6390/0 \
        .venv/bin/python -m unittest tests.integration.test_p0_api -v
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import socket
import time
import unittest
import uuid

import httpx
import uvicorn

from tests.integration import support


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class SseReader:
    """Reads /attach frames in the background and keeps them."""

    def __init__(self, base: str, user: str, session: str, from_offset: int = 0) -> None:
        self.frames: list[tuple[str, dict]] = []
        self._args = (base, user, session, from_offset)
        self._task: asyncio.Task | None = None
        self.ended = False

    async def __aenter__(self) -> "SseReader":
        self._task = asyncio.create_task(self._run())
        return self

    async def __aexit__(self, *exc: object) -> None:
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._task

    async def _run(self) -> None:
        base, user, session, from_offset = self._args
        async with httpx.AsyncClient(timeout=None) as client:
            while True:
                async with client.stream(
                    "GET", f"{base}/attach", params={"session_id": session, "from_offset": from_offset},
                    headers={"X-Test-User": user},
                ) as response:
                    response.raise_for_status()
                    event = None
                    async for line in response.aiter_lines():
                        if line.startswith("event: "):
                            event = line[7:]
                        elif line.startswith("data: ") and event:
                            data = json.loads(line[6:])
                            self.frames.append((event, data))
                            from_offset = max(from_offset, data["resume_offset"])
                            event = None
                self.ended = True
                await asyncio.sleep(0.5)  # re-attach like the client does when work may arrive

    def of(self, kind: str) -> list[dict]:
        return [d for e, d in self.frames if e == kind]

    async def wait_for(self, predicate, timeout: float = 180) -> None:
        deadline = time.monotonic() + timeout
        while not predicate(self):
            if time.monotonic() > deadline:
                raise AssertionError(f"timed out; last: {[e for e, _ in self.frames][-25:]}")
            await asyncio.sleep(0.2)


@unittest.skipIf(support.skip_reason(), support.skip_reason() or "")
class FrontDoorTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        os.environ.setdefault("SIMPLE_CHAT_REDIS_URL", "redis://127.0.0.1:6390/0")
        os.environ["AGENT_REDIS_PREFIX"] = f"aah-test-{uuid.uuid4().hex[:6]}"
        from simple_chat_agent.api.agent_dev_app import build_app

        self.task_queue = f"p0a-{uuid.uuid4().hex[:6]}"
        self.client = await support.connect()
        port = free_port()
        app = build_app(task_queue=self.task_queue, temporal_address=support.ADDRESS)
        self.server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning"))
        self.server_task = asyncio.create_task(self.server.serve())
        while not self.server.started:
            await asyncio.sleep(0.05)
        self.base = f"http://127.0.0.1:{port}/api/agent"
        self.http = httpx.AsyncClient(timeout=30)

    async def asyncTearDown(self) -> None:
        await self.http.aclose()
        self.server.should_exit = True
        await self.server_task

    def h(self, user: str) -> dict:
        return {"X-Test-User": user}

    async def test_workers_down_sends_land_and_apply_once(self) -> None:
        # No worker is running. Create a session and send a message.
        r = await self.http.post(f"{self.base}/sessions", json={"model": {"provider": "anthropic", "model": "claude-haiku-5-5", "effort": "low"}}, headers=self.h("alice"))
        r.raise_for_status()
        session = r.json()["session_id"]
        t0 = time.monotonic()
        r = await self.http.post(
            f"{self.base}/messages",
            json={"session_id": session, "message": {"type": "chat", "payload": {"text": "Create notes/q.txt containing 'queued'."}}},
            headers=self.h("alice"),
        )
        r.raise_for_status()
        message_id = r.json()["message_id"]
        print(f"\n[0.7] messages returned in {time.monotonic() - t0:.2f}s with no worker running")
        signaled = await self._signal_count(session)
        self.assertEqual(signaled, 1, "the message is in history with no worker running")

        # Status answers from Redis with no worker.
        t0 = time.monotonic()
        st = (await self.http.get(f"{self.base}/status/{session}", headers=self.h("alice"))).json()
        self.assertLess(time.monotonic() - t0, 0.2)
        self.assertIn(message_id, st["pending_turns"])

        # Ownership: bob gets 404 on every route.
        for method, path, body in [
            ("GET", f"/status/{session}", None),
            ("GET", f"/workflow-status/{session}", None),
            ("GET", "/attach", None),
            ("POST", "/messages", {"session_id": session, "message": {"type": "chat", "payload": {"text": "hi"}}}),
            ("POST", "/approve", {"session_id": session, "tool_id": "x", "approved": True}),
            ("POST", f"/sessions/{session}/close", None),
        ]:
            params = {"session_id": session} if path == "/attach" else None
            resp = await self.http.request(method, f"{self.base}{path}", json=body, params=params, headers=self.h("bob"))
            self.assertEqual(resp.status_code, 404, (method, path, resp.text))

        async with SseReader(self.base, "alice", session) as sse:
            # Start a worker: the queued message is admitted under the id the API chose.
            async with support.running_worker(self.client, self.task_queue):
                await sse.wait_for(lambda s: s.of("tool_approval_requested"))
                accepted = sse.of("message_accepted")
                self.assertEqual(accepted[0]["message_id"], message_id)
                tool_id = sse.of("tool_approval_requested")[0]["tool_id"]
            # Worker is gone again. Approve now: the approval lands as a Signal.
            before = await self._signal_count(session)
            r = await self.http.post(f"{self.base}/approve", json={"session_id": session, "tool_id": tool_id, "approved": True}, headers=self.h("alice"))
            r.raise_for_status()
            self.assertEqual(await self._signal_count(session), before + 1)
            # A fresh attach while no worker runs still replays the whole transcript from Redis.
            async with SseReader(self.base, "alice", session) as late:
                await late.wait_for(lambda s: s.of("tool_approval_requested"), timeout=10)
            # Worker returns: the approval applies exactly once and the turn finishes.
            async with support.running_worker(self.client, self.task_queue):
                await sse.wait_for(lambda s: s.of("turn_end"))
            resolved = sse.of("tool_approval_resolved")
            self.assertEqual([d["approved"] for d in resolved], [True])
            self.assertEqual(len([d for d in sse.of("tool_end") if d["tool_name"] == "write_file"]), 1)
            # Resume offsets never go backwards in what the client saw.
            offsets = [d["resume_offset"] for _, d in sse.frames]
            self.assertEqual(offsets, sorted(offsets))

    async def _signal_count(self, session: str) -> int:
        handle = self.client.get_workflow_handle(session)
        n = 0
        async for event in handle.fetch_history_events():
            if event.HasField("workflow_execution_signaled_event_attributes"):
                n += 1
        return n


if __name__ == "__main__":
    unittest.main()
