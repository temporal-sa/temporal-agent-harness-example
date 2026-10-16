"""The harness console at /console/ (P2 step 2.4, CTO-6): unchanged static assets, owner-scoped
console API, refusals, no CORS, and the CONSOLE_ENABLED flag.

Uses the real Redis at TEST_AGENT_REDIS_URL under a unique key prefix and the in-memory
Temporal fake from test_agent_routes. Skipped when Redis isn't reachable.
"""

from __future__ import annotations

import os
import unittest
import uuid
from typing import Any
from unittest import mock

import redis.asyncio as aredis
from fastapi import FastAPI
from fastapi.testclient import TestClient

from simple_chat_agent.agent.contracts import AGENT_WORKFLOW_TYPE, ENQUEUE_MESSAGE_SIGNAL
from simple_chat_agent.api import console
from simple_chat_agent.api.agent_routes import AgentApi, SessionDirectory, create_agent_router
from simple_chat_agent.api.agent_store import AgentStore
from tests.test_agent_routes import (
    REDIS_UP,
    REDIS_URL,
    A,
    B,
    FakeBridge,
    FakeTemporal,
    FakeWorkflow,
    header_user,
)

DIST = console.console_dist()
FOREIGN_ORIGIN = "https://evil.example.com"


def console_routes(sid: str) -> list[tuple[str, str, dict[str, Any] | None, dict[str, Any] | None]]:
    """Every console API route that names a session, as the console calls it."""
    return [
        ("GET", "/attach", None, {"session_id": sid, "from_offset": 0}),
        ("POST", "/messages", {"session_id": sid, "message": {"type": "chat", "payload": {"text": "hi"}}}, None),
        ("POST", "/approve", {"session_id": sid, "tool_id": "t", "approved": True}, None),
        ("POST", "/callback-result", {"session_id": sid, "tool_id": "t", "result": 1}, None),
        ("GET", f"/status/{sid}", None, None),
        ("GET", f"/workflow-status/{sid}", None, None),
        ("POST", f"/sessions/{sid}/close", None, None),
        ("GET", f"/agent-interface/{sid}", None, None),
        # A forged mount source naming the session's workspace.
        (
            "POST",
            "/files/view",
            {
                "source": {"name": "workspace", "config": {"workspace_id": sid}, "task_queue": "tq-test"},
                "path": "/README.md",
            },
            None,
        ),
        ("POST", "/files/okf-graph", {"source": {"name": "okf", "config": {"workspace_id": sid}}}, None),
    ]


ALL_API_ROUTES: list[tuple[str, str, dict[str, Any] | None, dict[str, Any] | None]] = [
    ("GET", "/agents", None, None),
    ("GET", "/sessions", None, None),
    ("POST", "/sessions", {"agent_workflow_type": AGENT_WORKFLOW_TYPE}, None),
    *console_routes("coding-agent-0000000000000000"),
]


@unittest.skipUnless(REDIS_UP, f"Redis not reachable at {REDIS_URL}")
@unittest.skipIf(DIST is None, "harness console assets not installed")
class ConsoleTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self.prefix = f"aah-test-console-{uuid.uuid4().hex[:8]}"
        self.env = mock.patch.dict(
            os.environ,
            {"AGENT_REDIS_PREFIX": self.prefix, "AGENT_ENVIRONMENT": "catalog", "AGENT_RESEARCH_TOOLS": "fetch_url"},
        )
        self.env.start()
        for name in ("AGENT_MODELS_ANTHROPIC", "AGENT_MODELS_OPENAI", "AGENT_MODELS_GOOGLE"):
            os.environ.pop(name, None)
        self.temporal = FakeTemporal()
        self.bridge = FakeBridge()

        async def temporal() -> FakeTemporal:
            return self.temporal

        async def bridge() -> FakeBridge:
            return self.bridge

        self.redis = aredis.from_url(REDIS_URL, decode_responses=True)
        self.store = AgentStore(self.redis)
        self.directory = SessionDirectory(self.redis, temporal=temporal, task_queue="tq-test", sync_workflow=False)

        def agent_router() -> Any:
            return create_agent_router(
                temporal=temporal,  # type: ignore[arg-type]
                store=self.store,
                bridge=bridge,  # type: ignore[arg-type]
                current_user=header_user,
                task_queue="tq-test",
                directory=self.directory,
            )

        app = FastAPI()
        app.include_router(agent_router(), prefix="/api/agent")
        console_api = console.create_console_router(
            agent_router=agent_router(),
            store=self.store,
            directory=self.directory,
            temporal=temporal,
            current_user=header_user,
            task_queue="tq-test",
        )
        assert DIST is not None
        console.mount_console_routes(app, console_api, DIST)
        self.client = TestClient(app)
        self.client.__enter__()

    def tearDown(self) -> None:
        async def cleanup() -> None:
            keys = [k async for k in self.redis.scan_iter(f"{self.prefix}:*")]
            if keys:
                await self.redis.delete(*keys)
            await self.redis.aclose()

        self.client.portal.call(cleanup)  # type: ignore[union-attr]
        self.client.__exit__(None, None, None)
        self.env.stop()

    def create(self, headers: dict[str, str] = A, **body: Any) -> str:
        r = self.client.post("/api/agent/sessions", json=body, headers=headers)
        self.assertEqual(r.status_code, 200, r.text)
        return r.json()["workflow_id"]

    def call(self, method: str, path: str, body: Any, params: Any, headers: dict[str, str]) -> Any:
        return self.client.request(method, f"/console/api{path}", json=body, params=params, headers=headers)


class StaticTests(ConsoleTestBase):
    def test_index_and_assets_served_unchanged(self) -> None:
        assert DIST is not None
        r = self.client.get("/console/")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.text, (DIST / "index.html").read_text())
        self.assertIn("text/html", r.headers["content-type"])
        # The page loads its bundle relative to itself, so /console/assets/... must resolve.
        for asset in sorted((DIST / "assets").iterdir()):
            with self.subTest(asset=asset.name):
                self.assertIn(f"./assets/{asset.name}", r.text)
                a = self.client.get(f"/console/assets/{asset.name}")
                self.assertEqual(a.status_code, 200)
                self.assertEqual(a.content, asset.read_bytes())
        self.assertEqual(self.client.get("/console/favicon.svg").status_code, 200)

    def test_bare_path_redirects_keeping_the_deep_link(self) -> None:
        r = self.client.get("/console?s=coding-agent-abc", follow_redirects=False)
        self.assertIn(r.status_code, (302, 307))
        self.assertEqual(r.headers["location"], "console/?s=coding-agent-abc")
        r = self.client.get("/console/?s=coding-agent-abc")
        self.assertEqual(r.status_code, 200)

    def test_bundle_calls_relative_api_paths(self) -> None:
        # Guards the premise of the whole mount: the console builds `api/<path>` relative to
        # its page (httpClient.apiPath), so it lands on /console/api/... and not /api/....
        assert DIST is not None
        bundle = next((DIST / "assets").glob("index-*.js")).read_text()
        self.assertIn("api/", bundle)
        self.assertNotIn('"/api/', bundle)


class AuthTests(ConsoleTestBase):
    def test_every_api_route_requires_auth(self) -> None:
        for method, path, body, params in ALL_API_ROUTES:
            with self.subTest(method=method, path=path):
                r = self.call(method, path, body, params, {})
                self.assertEqual(r.status_code, 401, r.text)
                self.assertEqual(r.json()["error"], "unauthorized")
        self.assertEqual(self.temporal.describes, [])


class OwnershipTests(ConsoleTestBase):
    def assert_all_404(self, sid: str, headers: dict[str, str]) -> None:
        for method, path, body, params in console_routes(sid):
            with self.subTest(method=method, path=path, sid=sid):
                r = self.call(method, path, body, params, headers)
                self.assertEqual(r.status_code, 404, r.text)
                self.assertEqual(r.json()["error"], "not_found")

    def test_foreign_session_and_subagents_are_404(self) -> None:
        sid = self.create(A)
        sub = f"{sid}-subagent-{uuid.uuid4().hex[:8]}"
        self.temporal.workflows[sub] = FakeWorkflow("ExplorerAgent", parent_id=sid)
        self.assert_all_404(sid, B)
        self.assert_all_404(sub, B)
        # Nothing reached Temporal or the bridge for bob.
        self.assertEqual(self.temporal.signals, [])
        self.assertEqual((self.bridge.woken, self.bridge.read), ([], []))

    def test_unknown_id_is_404(self) -> None:
        self.assert_all_404(f"coding-agent-{uuid.uuid4().hex[:16]}", A)

    def test_owner_reaches_delegated_routes(self) -> None:
        sid = self.create(A)
        sub = f"{sid}-subagent-1"
        self.temporal.workflows[sub] = FakeWorkflow("ReviewerAgent", parent_id=sid)
        for wid in (sid, sub):
            with self.subTest(wid=wid):
                self.assertEqual(self.client.get(f"/console/api/status/{wid}", headers=A).status_code, 200)
                r = self.client.get(f"/console/api/workflow-status/{wid}", headers=A)
                self.assertEqual(r.status_code, 200)
                self.assertEqual(r.json()["execution_status"], "RUNNING")
                r = self.client.get("/console/api/attach", params={"session_id": wid}, headers=A)
                self.assertEqual(r.status_code, 200, r.text)
                self.assertTrue(r.headers["content-type"].startswith("text/event-stream"))
        r = self.client.get(f"/console/api/agent-interface/{sid}", headers=A)
        self.assertEqual(r.status_code, 200)
        self.assertIn("chat", [f["name"] for f in r.json()])
        r = self.client.post(
            "/console/api/messages",
            json={"session_id": sid, "message": {"type": "chat", "payload": {"text": "hi"}}},
            headers=A,
        )
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["disposition"], "queued")
        self.assertEqual([s[:2] for s in self.temporal.signals], [(sid, ENQUEUE_MESSAGE_SIGNAL)])
        r = self.client.post(f"/console/api/sessions/{sid}/close", headers=A)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(self.temporal.signals[-1][:2], (sid, "close"))

    def test_files_view_is_404_even_for_the_owner(self) -> None:
        sid = self.create(A)
        for path in ("/files/view", "/files/okf-graph"):
            body = {"source": {"name": "w", "config": {"workspace_id": sid}, "task_queue": "tq-test"}, "path": "/x"}
            r = self.client.post(f"/console/api{path}", json=body, headers=A)
            self.assertEqual(r.status_code, 404, r.text)
            self.assertEqual(r.json()["error"], "not_found")


class SessionListTests(ConsoleTestBase):
    def test_list_holds_only_the_callers_sessions(self) -> None:
        a1 = self.create(A, title="one")
        a2 = self.create(A, title="two")
        b1 = self.create(B, title="bob's")
        # A workflow in the namespace that isn't in anyone's list (what the packaged app
        # would "discover" through Visibility) never shows up.
        self.temporal.workflows["coding-agent-stray"] = FakeWorkflow(AGENT_WORKFLOW_TYPE)
        r = self.client.get("/console/api/sessions", headers=A)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.headers["cache-control"], "no-store")
        rows = r.json()
        # Oldest first, like the harness session manager.
        self.assertEqual([s["workflow_id"] for s in rows], [a1, a2])
        for row in rows:
            self.assertEqual(row["agent_workflow_type"], AGENT_WORKFLOW_TYPE)
            self.assertEqual(row["execution_status"], "RUNNING")
            self.assertFalse(row["closed"])
            # Non-null, so the console opens a ?s= deep link even when a newer session exists.
            self.assertEqual(row["initial_user_message"], row["label"])
        self.assertEqual([s["workflow_id"] for s in self.client.get("/console/api/sessions", headers=B).json()], [b1])

    def test_create_is_refused(self) -> None:
        for body in ({"agent_workflow_type": AGENT_WORKFLOW_TYPE}, {"agent_workflow_type": AGENT_WORKFLOW_TYPE, "data": {}}):
            r = self.client.post("/console/api/sessions", json=body, headers=A)
            self.assertEqual(r.status_code, 403, r.text)
            self.assertEqual(r.json()["error"], "forbidden")
        self.assertEqual(self.temporal.starts, [])
        self.assertEqual(self.client.get("/console/api/sessions", headers=A).json(), [])

    def test_agents_is_coding_agent_only_and_never_boot_started(self) -> None:
        r = self.client.get("/console/api/agents", headers=A)
        self.assertEqual(r.status_code, 200, r.text)
        agents = r.json()["agents"]
        self.assertEqual([a["workflow_type"] for a in agents], [AGENT_WORKFLOW_TYPE])
        # Required init data: the console's boot never auto-creates a session.
        self.assertTrue(agents[0]["init_data"]["required"])
        # The fake has no workflow service: readiness degrades to "unknown", not a 500.
        self.assertEqual(agents[0]["worker"]["status"], "unknown")

    def test_other_api_paths_are_404(self) -> None:
        for method, path in (("POST", "/chat"), ("GET", "/states"), ("GET", "/sessions/x/logs/y"), ("DELETE", "/sessions/x")):
            with self.subTest(path=path):
                r = self.client.request(method, f"/console/api{path}", headers=A)
                self.assertEqual(r.status_code, 404, r.text)


class CorsTests(ConsoleTestBase):
    def test_no_cors_for_a_foreign_origin(self) -> None:
        sid = self.create(A)
        h = {**A, "Origin": FOREIGN_ORIGIN}
        for method, path, params in (
            ("GET", "/console/", None),
            ("GET", "/console/api/sessions", None),
            ("GET", f"/console/api/status/{sid}", None),
            ("GET", "/console/api/attach", {"session_id": sid}),
        ):
            with self.subTest(path=path):
                r = self.client.request(method, path, params=params, headers=h)
                self.assertNotIn("access-control-allow-origin", r.headers)
                self.assertNotIn("access-control-allow-credentials", r.headers)
        r = self.client.options(
            "/console/api/messages",
            headers={"Origin": FOREIGN_ORIGIN, "Access-Control-Request-Method": "POST"},
        )
        self.assertNotIn("access-control-allow-origin", r.headers)
        self.assertNotEqual(r.status_code, 200)


class FlagTests(unittest.TestCase):
    def test_flag_defaults(self) -> None:
        cases = [
            ({"AGENT_ENVIRONMENT": "local"}, True),
            ({"AGENT_ENVIRONMENT": "catalog"}, False),
            ({}, False),  # the default environment is the catalog
            ({"AGENT_ENVIRONMENT": "catalog", "CONSOLE_ENABLED": "true"}, True),
            ({"AGENT_ENVIRONMENT": "local", "CONSOLE_ENABLED": "false"}, False),
            ({"AGENT_ENVIRONMENT": "local", "CONSOLE_ENABLED": "0"}, False),
            ({"AGENT_ENVIRONMENT": "local", "CONSOLE_ENABLED": ""}, True),
        ]
        for env, expected in cases:
            with self.subTest(env=env), mock.patch.dict(os.environ, env, clear=False):
                for name in ("AGENT_ENVIRONMENT", "CONSOLE_ENABLED"):
                    if name not in env:
                        os.environ.pop(name, None)
                self.assertEqual(console.console_enabled(), expected)

    @unittest.skipIf(DIST is None, "harness console assets not installed")
    def test_mount_follows_the_flag(self) -> None:
        for env, mounted in (
            ({"AGENT_ENVIRONMENT": "catalog", "CONSOLE_ENABLED": ""}, False),
            ({"AGENT_ENVIRONMENT": "catalog", "CONSOLE_ENABLED": "1"}, True),
            ({"AGENT_ENVIRONMENT": "local", "CONSOLE_ENABLED": ""}, True),
        ):
            with self.subTest(env=env), mock.patch.dict(os.environ, env):
                app = FastAPI()
                api = AgentApi(current_user=header_user, task_queue="tq-test")
                self.assertEqual(console.mount_console(app, api), mounted)
                client = TestClient(app)
                self.assertEqual(client.get("/console/").status_code, 200 if mounted else 404)
                r = client.get("/console/api/sessions")
                self.assertEqual(r.status_code, 401 if mounted else 404)


@unittest.skipUnless(REDIS_UP, f"Redis not reachable at {REDIS_URL}")
@unittest.skipIf(DIST is None, "harness console assets not installed")
class RealAuthTests(unittest.TestCase):
    """The console behind the real app's auth (``main._current_user``), local and catalog."""

    def test_local_cookie_and_catalog_headers(self) -> None:
        from simple_chat_agent.api import main
        from simple_chat_agent.api.auth import SESSION_COOKIE, create_session_token, user_from_local_username

        prefix = f"aah-test-console-main-{uuid.uuid4().hex[:6]}"
        env = {"AGENT_REDIS_PREFIX": prefix, "CONSOLE_ENABLED": "1", "SIMPLE_CHAT_REDIS_URL": REDIS_URL}
        with mock.patch.dict(os.environ, env):
            app = FastAPI()
            api = AgentApi(current_user=main._current_user, task_queue="tq-test")
            self.assertTrue(console.mount_console(app, api))
            # One client (one event loop) for both modes: the store's Redis pool is per loop.
            with TestClient(app) as client:
                with mock.patch.dict(os.environ, {"SIMPLE_CHAT_AUTH_MODE": "local"}):
                    r = client.get("/console/api/sessions")
                    self.assertEqual(r.status_code, 401)
                    self.assertEqual(r.json()["error"], "unauthorized")
                    client.cookies.set(SESSION_COOKIE, create_session_token(user_from_local_username("demo")))
                    r = client.get("/console/api/sessions")
                    self.assertEqual(r.status_code, 200, r.text)
                    self.assertEqual(r.json(), [])
                    client.cookies.clear()
                with mock.patch.dict(os.environ, {"SIMPLE_CHAT_AUTH_MODE": "catalog"}):
                    self.assertEqual(client.get("/console/api/sessions").status_code, 401)
                    r = client.get(
                        "/console/api/sessions",
                        headers={"x-temporal-auth-subject": "sub-1", "x-temporal-auth-email": "a@example.com"},
                    )
                    self.assertEqual(r.status_code, 200, r.text)
                client.portal.call(api.store.r.aclose)  # type: ignore[union-attr]

    def test_real_app_mounts_console_when_enabled(self) -> None:
        from simple_chat_agent.api import main

        paths = {getattr(r, "path", None) for r in main.app.routes}
        if main.app.state.console_enabled:
            self.assertIn("/console/api/sessions", paths)
            self.assertIn("/console/", paths)
        else:
            self.assertFalse(any(str(p).startswith("/console") for p in paths if p))
        # The packaged harness app is never mounted (its /api/agents), and nothing adds CORS.
        self.assertNotIn("/api/agents", paths)
        self.assertNotIn("CORSMiddleware", [m.cls.__name__ for m in main.app.user_middleware])


if __name__ == "__main__":
    unittest.main()
