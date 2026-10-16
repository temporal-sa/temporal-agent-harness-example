"""P2 validation on the REAL API app (catalog-header auth): 2.1b, 2.1c, 2.2b, 2.4.

    RUN_INTEGRATION=1 .venv/bin/python -m unittest tests.integration.test_p2_real_app -v

Runs `simple_chat_agent.api.main:app` under uvicorn and a dev worker as subprocesses, so the
worker can be stopped to show the workers-down behaviour.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import socket
import subprocess
import sys
import time
import unittest
import uuid

import httpx

from tests.integration import support

SCRATCH = "/private/tmp/claude-501/-Users-mjones-Source-temporal-demo-advanced-ai-agent/b2c50836-5f17-4dd1-aed5-333caa44f1a8/scratchpad"


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def user(name: str) -> dict[str, str]:
    return {
        "X-Temporal-Auth-Subject": name,
        "X-Temporal-Auth-Email": f"{name}@example.test",
        "X-Requested-With": "p2-test",
    }


async def read_sse(client: httpx.AsyncClient, url: str, headers: dict, *, until, timeout: float = 120) -> list[tuple[str, dict]]:
    frames: list[tuple[str, dict]] = []
    deadline = time.monotonic() + timeout
    async with client.stream("GET", url, headers=headers, timeout=None) as response:
        response.raise_for_status()
        event = None
        async for line in response.aiter_lines():
            if line.startswith("event: "):
                event = line[7:]
            elif line.startswith("data: ") and event:
                frames.append((event, json.loads(line[6:])))
                event = None
                if until(frames):
                    break
            if time.monotonic() > deadline:
                raise AssertionError(f"timed out; last: {[e for e, _ in frames][-20:]}")
    return frames


@unittest.skipIf(support.skip_reason(), support.skip_reason() or "")
class RealAppTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.tq = f"p2r-{uuid.uuid4().hex[:6]}"
        self.port = free_port()
        self.env = {
            **os.environ,
            "SIMPLE_CHAT_AUTH_MODE": "catalog",
            "CODING_AGENT_TASK_QUEUE": self.tq,
            "AGENT_ENVIRONMENT": "local",
            "CONSOLE_ENABLED": "1",
            "SIMPLE_CHAT_REDIS_URL": "redis://127.0.0.1:6390/0",
            "AGENT_REDIS_PREFIX": f"aah-p2r-{uuid.uuid4().hex[:6]}",
            "TEMPORAL_ADDRESS": support.ADDRESS,
            "SIMPLE_CHAT_DB_PATH": f"{SCRATCH}/p2r-{self.tq}.db",
            "SIMPLE_CHAT_ARTIFACT_DIR": f"{SCRATCH}/p2r-artifacts",
            "SIMPLE_CHAT_CODEC_SERVER_ENABLED": "0",
        }
        self.api = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "simple_chat_agent.api.main:app", "--port", str(self.port), "--log-level", "warning"],
            cwd=str(support.ROOT), env=self.env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        self.worker: subprocess.Popen | None = None
        self.base = f"http://127.0.0.1:{self.port}"
        self.http = httpx.AsyncClient(base_url=self.base, timeout=60)
        for _ in range(100):
            try:
                if (await self.http.get("/healthz")).status_code < 500:
                    break
            except httpx.HTTPError:
                pass
            await asyncio.sleep(0.2)

    async def asyncTearDown(self) -> None:
        await self.http.aclose()
        for p in (self.api, self.worker):
            if p is not None and p.poll() is None:
                p.send_signal(signal.SIGTERM)
                try:
                    p.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    p.kill()

    def start_worker(self) -> None:
        self.worker = subprocess.Popen(
            [sys.executable, "-m", "simple_chat_agent.agent.dev_worker", "--task-queue", self.tq],
            cwd=str(support.ROOT), env=self.env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )

    def stop_worker(self) -> None:
        assert self.worker is not None
        self.worker.kill()
        self.worker.wait()
        self.worker = None

    async def create(self, who: str, **body) -> dict:
        body.setdefault("model", {"provider": "anthropic", "model": "claude-haiku-5-5", "effort": "low"})
        body.setdefault("posture", "never_ask")
        r = await self.http.post("/api/agent/sessions", json=body, headers=user(who))
        self.assertEqual(r.status_code, 200, r.text)
        return r.json()

    async def test_real_app_end_to_end(self) -> None:
        self.start_worker()
        await asyncio.sleep(5)

        # 2.1c demo seed: the repo is there before the first message.
        session = await self.create("alice", demo="refund_service", title="Refund bug")
        sid = session["session_id"]
        self.assertGreaterEqual(session["seed"]["files"], 10, session)
        from tests.integration.test_p1_validation import read_workspace_file

        self.assertIsNotNone(await read_workspace_file(sid, "refund_service/refunds.py"))

        # One turn through the real API, read over the real attach SSE.
        r = await self.http.post("/api/agent/messages", json={"session_id": sid, "message": {"type": "chat", "payload": {"text": "Reply with exactly: ready"}}}, headers=user("alice"))
        self.assertEqual(r.status_code, 200, r.text)
        frames = await read_sse(self.http, f"/api/agent/attach?session_id={sid}&from_offset=0", user("alice"), until=lambda f: any(e == "turn_end" for e, _ in f))
        last_offset = max(d["resume_offset"] for _, d in frames)

        # 2.1b isolation: bob sees nothing of alice's session anywhere.
        for path in (
            f"/api/agent/status/{sid}", f"/api/agent/workflow-status/{sid}", f"/api/agent/agent-interface/{sid}",
            f"/api/agent/sessions/{sid}", f"/api/agent/attach?session_id={sid}", f"/api/agent/sessions/{sid}/logs/run_command:abc",
            f"/console/api/status/{sid}", f"/console/api/attach?session_id={sid}", f"/console/api/agent-interface/{sid}",
        ):
            resp = await self.http.get(path, headers=user("bob"))
            self.assertEqual(resp.status_code, 404, (path, resp.status_code, resp.text[:200]))
        for path, body in (
            ("/api/agent/messages", {"session_id": sid, "message": {"type": "chat", "payload": {"text": "hi"}}}),
            ("/api/agent/approve", {"session_id": sid, "tool_id": "x", "approved": True}),
            (f"/api/agent/sessions/{sid}/close", None),
            ("/console/api/approve", {"session_id": sid, "tool_id": "x", "approved": True}),
        ):
            resp = await self.http.post(path, json=body, headers=user("bob"))
            self.assertEqual(resp.status_code, 404, (path, resp.status_code, resp.text[:200]))
        bob_list = (await self.http.get("/api/agent/sessions", headers=user("bob"))).json()
        self.assertNotIn(sid, json.dumps(bob_list))
        self.assertNotIn(sid, json.dumps((await self.http.get("/console/api/sessions", headers=user("bob"))).json()))

        # 2.4 console: the owner opens the deep link and the console API answers.
        page = await self.http.get(f"/console/?s={sid}", headers=user("alice"))
        self.assertEqual(page.status_code, 200)
        self.assertIn("<html", page.text.lower())
        self.assertIn(sid, json.dumps((await self.http.get("/console/api/sessions", headers=user("alice"))).json()))
        self.assertEqual((await self.http.get(f"/console/api/status/{sid}", headers=user("alice"))).status_code, 200)
        cross = await self.http.post("/api/agent/messages", json={"session_id": sid, "message": {"type": "chat", "payload": {"text": "x"}}}, headers={**user("alice"), "X-Requested-With": "", "Origin": "https://evil.example"})
        self.assertEqual(cross.status_code, 403)

        # 2.2b workers down: transcript renders from Redis, status is fast, a send lands.
        self.stop_worker()
        replay = await read_sse(self.http, f"/api/agent/attach?session_id={sid}&from_offset=0", user("alice"), until=lambda f: max((d["resume_offset"] for _, d in f), default=0) >= last_offset, timeout=20)
        self.assertTrue(any(e == "turn_end" for e, _ in replay))
        t0 = time.monotonic()
        st = await self.http.get(f"/api/agent/status/{sid}", headers=user("alice"))
        self.assertLess(time.monotonic() - t0, 0.2)
        self.assertEqual(st.status_code, 200)
        t0 = time.monotonic()
        r = await self.http.post("/api/agent/messages", json={"session_id": sid, "message": {"type": "chat", "payload": {"text": "Reply with exactly: back"}}}, headers=user("alice"))
        self.assertEqual(r.status_code, 200, r.text)
        print(f"\n[2.2b] send with no worker: {time.monotonic() - t0:.2f}s")
        message_id = r.json()["message_id"]
        self.assertIn(message_id, json.dumps((await self.http.get(f"/api/agent/status/{sid}", headers=user("alice"))).json()))

        # The worker returns: the message applies exactly once.
        self.start_worker()
        frames2 = await read_sse(
            self.http, f"/api/agent/attach?session_id={sid}&from_offset={last_offset}", user("alice"),
            # Frames at the cursor are replayed (B1), so wait for the new message's own turn.
            until=lambda f: any(
                e == "turn_end" and i > next((j for j, (e2, d2) in enumerate(f) if e2 == "message_accepted" and d2.get("message_id") == message_id), len(f))
                for i, (e, _) in enumerate(f)
            ),
            timeout=120,
        )
        accepted = [d for e, d in frames2 if e == "message_accepted" and d.get("message_id") == message_id]
        self.assertEqual(len(accepted), 1)


if __name__ == "__main__":
    unittest.main()
