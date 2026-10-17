"""Browser callback tools (PRD A12): answer checks, front-door admission, routes."""

from __future__ import annotations

import asyncio
import json
import os
import unittest
from types import SimpleNamespace
from typing import Any
from unittest import mock

os.environ.setdefault("ANTHROPIC_API_KEY", "unit-test-not-used")

from temporalio.exceptions import ApplicationError  # noqa: E402

from simple_chat_agent.agent import approvals, harness_ext  # noqa: E402
from simple_chat_agent.agent.contracts import ENQUEUE_CALLBACK_SIGNAL, PICK_MAX_FILES  # noqa: E402
from simple_chat_agent.agent.tools import callbacks  # noqa: E402
from simple_chat_agent.api import agent_routes  # noqa: E402
from simple_chat_agent.api.agent_telemetry import rejected_frame  # noqa: E402

from tests.test_agent_routes import A, B, AgentRoutesTestBase  # noqa: E402

ASK = {"question": "Which?", "options": [{"id": "keep", "label": "Keep"}, {"id": "drop", "label": "Drop"}]}


class CheckTests(unittest.TestCase):
    def test_ask_user_only_offered_ids(self) -> None:
        callbacks.check_callback_result("ask_user", ASK, {"option_id": "drop"})
        for bad in ({"option_id": "other"}, {"option_id": None}, {}):
            with self.assertRaises(ApplicationError) as cm:
                callbacks.check_callback_result("ask_user", ASK, bad)
            self.assertEqual(cm.exception.type, "InvalidCallbackResult")

    def test_pick_files_paths_and_count(self) -> None:
        tool_input = {"prompt": "CSV please", "max_files": 2}
        callbacks.check_callback_result("pick_files", tool_input, {"paths": ["uploads/a.csv", "uploads/b-1.csv"]})
        callbacks.check_callback_result("pick_files", tool_input, {"paths": []})
        for bad in (
            {"paths": ["uploads/a", "uploads/b", "uploads/c"]},
            {"paths": ["uploads/a", "uploads/a"]},
            {"paths": ["src/app.py"]},
            {"paths": ["uploads/../x"]},
            {"paths": ["uploads/.env"]},
            {"paths": "uploads/a"},
        ):
            with self.subTest(bad=bad), self.assertRaises(ApplicationError):
                callbacks.check_callback_result("pick_files", tool_input, bad)
        self.assertEqual(callbacks.pick_limit({"max_files": 99}), PICK_MAX_FILES)

    def test_tools_are_safe_callbacks(self) -> None:
        for tool in callbacks.CALLBACK_TOOLS:
            self.assertTrue(getattr(tool, "__agent_tool__", False))
            self.assertEqual(approvals.risk_class_for(tool.__name__), approvals.READ_ONLY)


class _Entry(SimpleNamespace):
    pass


class _Runner:
    """The parts of AgentWorkflowRunner the callback helpers touch."""

    def __init__(self) -> None:
        self.entries = {"t1": _Entry(tool_id="t1", tool_name="ask_user", tool_input=ASK, turn_id="turn-1", pending=True)}
        self.handled: list[Any] = []
        self.published: list[str] = []
        self._status = SimpleNamespace(
            callback_entry=lambda tid: self.entries.get(tid),
            pending_callbacks=lambda: [SimpleNamespace(tool_id=e.tool_id) for e in self.entries.values() if e.pending],
            resolve_callback=self._resolve,
        )

    def _resolve(self, tool_id: str, *, outcome: str, result: Any, error: str | None) -> None:
        self.entries[tool_id].pending = False
        self.entries[tool_id].outcome = (outcome, error)

    def _validate_provide_callback_result(self, result: Any) -> None:
        entry = self.entries.get(result.tool_id)
        if entry is None:
            raise ApplicationError("no pending callback", type="UnknownCallback")

    async def _handle_provide_callback_result(self, result: Any) -> None:
        self.handled.append(result)

    def _publish_callback_resolved(self, tool_id: str) -> None:
        self.published.append(tool_id)


class AdmissionTests(unittest.TestCase):
    def test_admits_an_offered_answer_and_refuses_others(self) -> None:
        runner = _Runner()
        check = callbacks.check_callback_result
        asyncio.run(harness_ext.admit_callback_result(runner, "t1", {"option_id": "keep"}, None, check))  # type: ignore[arg-type]
        self.assertEqual(runner.handled[0].result, {"option_id": "keep"})
        with self.assertRaises(ApplicationError) as cm:
            asyncio.run(harness_ext.admit_callback_result(runner, "t1", {"option_id": "nope"}, None, check))  # type: ignore[arg-type]
        self.assertEqual(harness_ext.describe_rejection(cm.exception)[0], "invalid_answer")
        self.assertIn("not one of the offered options", harness_ext.describe_rejection(cm.exception)[1])
        # A declined question (error) needs no offered id.
        asyncio.run(harness_ext.admit_callback_result(runner, "t1", None, "No thanks", check))  # type: ignore[arg-type]
        self.assertEqual(len(runner.handled), 2)
        with self.assertRaises(ApplicationError) as cm:
            asyncio.run(harness_ext.admit_callback_result(runner, "zz", {"option_id": "keep"}, None, check))  # type: ignore[arg-type]
        self.assertEqual(harness_ext.describe_rejection(cm.exception)[0], "unknown_callback")

    def test_interrupt_fails_pending_callbacks_of_the_turn(self) -> None:
        runner = _Runner()
        self.assertEqual(harness_ext.fail_pending_callbacks(runner, turn_id="other", reason="x"), 0)  # type: ignore[arg-type]
        self.assertEqual(harness_ext.fail_pending_callbacks(runner, turn_id="turn-1", reason="Stopped."), 1)  # type: ignore[arg-type]
        self.assertEqual(runner.entries["t1"].outcome, ("error", "Stopped."))
        self.assertEqual(runner.published, ["t1"])

    def test_rejection_frame_keeps_callback_kind(self) -> None:
        detail = {"kind": "callback", "request_id": "r1", "tool_id": "t1", "code": "invalid_answer", "reason": "no"}
        _, frame = rejected_frame({"message_id": "x_rejected:r1", "message": json.dumps(detail)})
        self.assertEqual((frame["kind"], frame["tool_id"], frame["code"]), ("callback", "t1", "invalid_answer"))


class UploadNameTests(unittest.TestCase):
    def test_names(self) -> None:
        self.assertEqual(agent_routes.upload_file_name("../../etc/passwd"), "passwd")
        self.assertEqual(agent_routes.upload_file_name("C:\\a\\My Report (1).csv"), "My_Report_1_.csv")
        self.assertEqual(agent_routes.upload_file_name(".."), "file")
        long = agent_routes.upload_file_name("a" * 300 + ".txt")
        self.assertEqual((len(long), long[-4:]), (100, ".txt"))
        for raw in ("x.csv", ".env", "-rf", "a b.txt"):
            self.assertRegex(f"uploads/{agent_routes.upload_file_name(raw)}", callbacks.UPLOAD_PATH)


class CallbackRouteTests(AgentRoutesTestBase):
    def setUp(self) -> None:
        self.written: dict[tuple[str, str], bytes] = {}

        async def write(workspace_id: str, path: str, content: bytes) -> None:
            if (workspace_id, path) in self.written:
                raise FileExistsError(path)
            self.written[(workspace_id, path)] = content

        patcher = mock.patch.object(agent_routes, "_default_write_upload", write)
        patcher.start()
        self.addCleanup(patcher.stop)
        super().setUp()

    def upload(self, sid: str, name: str, data: bytes = b"a,b\n", tool_id: str = "toolu_1", headers: dict[str, str] = A) -> Any:
        return self.client.post(
            f"/api/agent/sessions/{sid}/callbacks/{tool_id}/files", files={"file": (name, data, "text/csv")}, headers=headers
        )

    def test_pick_files_upload_then_answer(self) -> None:
        sid = self.create()["session_id"]
        r1, r2 = self.upload(sid, "data.csv"), self.upload(sid, "data.csv")
        self.assertEqual((r1.status_code, r2.status_code), (200, 200), r1.text)
        self.assertEqual((r1.json()["path"], r2.json()["path"]), ("uploads/data.csv", "uploads/data-1.csv"))
        self.assertEqual(self.written[(sid, "uploads/data.csv")], b"a,b\n")
        # Only paths this call's picker uploaded.
        bad = self.client.post(
            "/api/agent/callback-result",
            json={"session_id": sid, "tool_id": "toolu_1", "result": {"paths": ["uploads/other.csv"]}},
            headers=A,
        )
        self.assertEqual(bad.status_code, 422, bad.text)
        other_call = self.client.post(
            "/api/agent/callback-result",
            json={"session_id": sid, "tool_id": "toolu_2", "result": {"paths": ["uploads/data.csv"]}},
            headers=A,
        )
        self.assertEqual(other_call.status_code, 422)
        ok = self.client.post(
            "/api/agent/callback-result",
            json={"session_id": sid, "tool_id": "toolu_1", "result": {"paths": ["uploads/data.csv", "uploads/data-1.csv"]}},
            headers=A,
        )
        self.assertEqual(ok.status_code, 200, ok.text)
        wid, name, payload = self.temporal.signals[-1]
        self.assertEqual((wid, name, payload.tool_id), (sid, ENQUEUE_CALLBACK_SIGNAL, "toolu_1"))
        self.assertEqual(payload.result, {"paths": ["uploads/data.csv", "uploads/data-1.csv"]})

    def test_upload_limits_and_owner(self) -> None:
        sid = self.create()["session_id"]
        self.assertEqual(self.upload(sid, "a.csv", headers=B).status_code, 404)
        self.assertEqual(self.upload(sid, "a.csv", tool_id="bad id!").status_code, 422)
        with mock.patch.object(agent_routes, "PICK_MAX_FILE_BYTES", 3):
            self.assertEqual(self.upload(sid, "big.csv", b"abcd").status_code, 413)
        with mock.patch.object(agent_routes, "PICK_MAX_FILES", 2):
            self.assertEqual(self.upload(sid, "a.csv").status_code, 200)
            self.assertEqual(self.upload(sid, "b.csv").status_code, 200)
            self.assertEqual(self.upload(sid, "c.csv").status_code, 409)

    def test_ask_user_answer_and_decline_go_through_the_front_door(self) -> None:
        sid = self.create()["session_id"]
        r = self.client.post(
            "/api/agent/callback-result",
            json={"session_id": sid, "tool_id": "toolu_9", "result": {"option_id": "keep"}, "request_id": "req-1"},
            headers=A,
        )
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["request_id"], "req-1")
        self.assertEqual(self.temporal.signals[-1][2].result, {"option_id": "keep"})
        r = self.client.post(
            "/api/agent/callback-result",
            json={"session_id": sid, "tool_id": "toolu_9", "error": "Not now\n"},
            headers=A,
        )
        self.assertEqual(self.temporal.signals[-1][2].error, "Not now")
        self.assertEqual(
            self.client.post(
                "/api/agent/callback-result", json={"session_id": sid, "tool_id": "t", "result": {"option_id": "x"}}, headers=B
            ).status_code,
            404,
        )


if __name__ == "__main__":
    unittest.main()
