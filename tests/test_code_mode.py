"""Code Mode (PRD A11): limits, nested ids, the one script approval, the /workspace backend."""

from __future__ import annotations

import asyncio
import os
import unittest
from pathlib import PurePosixPath
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, patch

os.environ.setdefault("ANTHROPIC_API_KEY", "unit-test-not-used")

from temporalio.exceptions import ApplicationError  # noqa: E402

from temporal_agent_harness.harness.code_mode import code_mode_type_check  # noqa: E402
from temporal_agent_harness.harness.code_mode.driver import CodeModeDriver  # noqa: E402
from temporal_agent_harness.harness.code_mode.vfs import FileOp  # noqa: E402

from simple_chat_agent.agent import approvals, harness_ext  # noqa: E402
from simple_chat_agent.agent.tools import code_mode  # noqa: E402


class TypeCheckTests(unittest.TestCase):
    def test_tool_shape_and_type_error_reaches_the_model(self) -> None:
        tool = code_mode.build_code_mode_tool("coding-agent-abc")
        self.assertEqual(tool.__name__, "run_script")
        self.assertIn("at most 16 host-function calls", tool.__doc__)
        self.assertIn("`/workspace` (read-only)", tool.__doc__)
        ok = "import asyncio\nasync def main():\n    return await read_file(path='a.py')\nasyncio.run(main())\n"
        self.assertIsNone(asyncio.run(code_mode_type_check(tool, ok)))
        bad = "import asyncio\nasync def main():\n    return await read_file(path=1)\nasyncio.run(main())\n"
        self.assertIsNotNone(asyncio.run(code_mode_type_check(tool, bad)))

    def test_risk_classes(self) -> None:
        for name in ("run_script", "fs_stat", "fs_read", "fs_list", "fs_seed"):
            self.assertEqual(approvals.risk_class_for(name), approvals.READ_ONLY)
        self.assertFalse(approvals.remember_allowed(approvals.risk_class_for("run_script")))


class _Runner:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []

    def _policy_auto_approves(self, tool_name: str, *, inherently_safe: bool) -> bool:
        return inherently_safe

    async def run_tool(self, call_id: str, tool: Any, /, *args: Any, **kwargs: Any) -> Any:
        self.calls.append((call_id, harness_ext._APPROVED_SCRIPT_TOOLS.get()))
        return self._policy_auto_approves("fs_read", inherently_safe=False)


class ScriptApprovalTests(unittest.TestCase):
    def test_host_calls_nest_and_skip_their_gate_only_inside_the_script(self) -> None:
        runner = _Runner()
        harness_ext.allow_approved_script_calls(runner)  # type: ignore[arg-type]
        harness_ext.allow_approved_script_calls(runner)  # idempotent
        # Outside a script: the policy answers.
        self.assertFalse(runner._policy_auto_approves("fs_read", inherently_safe=False))
        script = harness_ext._ScriptRunner(runner, "toolu_1", frozenset({"fs_read"}))  # type: ignore[arg-type]

        async def two() -> list[Any]:
            return [await script.run_tool("random", object()), await script.run_tool("random", object())]

        self.assertEqual(asyncio.run(two()), [True, True])
        self.assertEqual([c[0] for c in runner.calls], ["toolu_1/1", "toolu_1/2"])
        self.assertEqual(runner.calls[0][1], frozenset({"fs_read"}))
        self.assertIsNone(harness_ext._APPROVED_SCRIPT_TOOLS.get())
        # A tool the script doesn't expose still goes to the policy.
        token = harness_ext._APPROVED_SCRIPT_TOOLS.set(frozenset({"fs_read"}))
        try:
            self.assertFalse(runner._policy_auto_approves("write_file", inherently_safe=False))
        finally:
            harness_ext._APPROVED_SCRIPT_TOOLS.reset(token)


class LimitTests(unittest.TestCase):
    def _driver(self, limits: harness_ext.ScriptLimits) -> Any:
        cls = harness_ext._limited_driver_class()
        return cls(object(), {}, {}, injections={}, stubs=None, limits=limits)

    def test_host_call_cap_and_concurrency(self) -> None:
        driver = self._driver(harness_ext.ScriptLimits(max_host_calls=16, max_file_ops=2, concurrency=4))
        stepper = SimpleNamespace(Sleep=type("Sleep", (), {}))
        state = {"now": 0, "peak": 0}

        async def fake(self: Any, call: Any, stepper: Any) -> str:
            state["now"] += 1
            state["peak"] = max(state["peak"], state["now"])
            await asyncio.sleep(0.01)
            state["now"] -= 1
            return "ok"

        async def run() -> list[Any]:
            calls = [SimpleNamespace(function_name="read_file") for _ in range(17)]
            return await asyncio.gather(*(driver._run_call(c, stepper) for c in calls), return_exceptions=True)

        with patch.object(CodeModeDriver, "_run_call", fake):
            outcomes = asyncio.run(run())
        self.assertEqual(outcomes[:16], ["ok"] * 16)
        self.assertIsInstance(outcomes[16], RuntimeError)
        self.assertIn("at most 16 host-function calls", str(outcomes[16]))
        self.assertEqual(state["peak"], 4)

    def test_file_ops_have_their_own_cap(self) -> None:
        driver = self._driver(harness_ext.ScriptLimits(max_host_calls=1, max_file_ops=2, concurrency=4))
        stepper = SimpleNamespace(Sleep=type("Sleep", (), {}))
        op = FileOp.__new__(FileOp)

        async def run() -> list[Any]:
            return [await driver._run_call(op, stepper) for _ in range(2)]

        with patch.object(CodeModeDriver, "_run_call", AsyncMock(return_value="ok")):
            self.assertEqual(asyncio.run(run()), ["ok", "ok"])
            with self.assertRaises(RuntimeError):
                asyncio.run(driver._run_call(op, stepper))


class WorkspaceFSTests(unittest.TestCase):
    def _fs(self, workspace: str = "coding-agent-abc", caller: str | None = "coding-agent-abc") -> Any:
        with patch.object(code_mode.activity, "in_activity", return_value=caller is not None), patch.object(
            code_mode.activity, "info", return_value=SimpleNamespace(workflow_id=caller)
        ):
            return code_mode.ExecutorWorkspaceFS(code_mode.WorkspaceFSConfig(workspace_id=workspace))

    def test_only_the_calling_sessions_workspace(self) -> None:
        self._fs()
        with self.assertRaises(ValueError):
            self._fs(caller="coding-agent-other")
        with self.assertRaises(ValueError):
            self._fs(caller=None)
        with self.assertRaises(ValueError):
            self._fs(workspace="../x", caller="../x")

    def test_stat_read_list(self) -> None:
        fs = self._fs()
        replies = {
            "stat": {"type": "directory", "size": 64, "mtime": 1.5},
            "read": {"content": "aGk=", "encoding": "base64"},
            "list": {"entries": [{"name": "a.py"}, {"name": "src"}]},
        }

        async def files_call(op: str, body: dict[str, Any]) -> dict[str, Any]:
            self.assertEqual(body["workspace_id"], "coding-agent-abc")
            return replies[op]

        with patch.object(code_mode, "_files_call", files_call):
            st = asyncio.run(fs.stat(PurePosixPath("src")))
            self.assertTrue(st.is_dir)
            self.assertEqual(asyncio.run(fs.read(PurePosixPath("b.bin"))), b"hi")
            self.assertEqual(asyncio.run(fs.list(PurePosixPath("."))), ["a.py", "src"])

    def test_not_found(self) -> None:
        fs = self._fs()

        async def missing(op: str, body: dict[str, Any]) -> dict[str, Any]:
            raise ApplicationError("nope", type="Workspace_not_found")

        with patch.object(code_mode, "_files_call", missing):
            self.assertIsNone(asyncio.run(fs.stat(PurePosixPath("x"))))
            with self.assertRaises(FileNotFoundError):
                asyncio.run(fs.read(PurePosixPath("x")))

    def test_activities_registered(self) -> None:
        from temporalio.activity import _Definition

        names = {_Definition.must_from_callable(a).name for a in code_mode.CODE_MODE_ACTIVITIES}
        self.assertIn("vfs.executor-workspace.read", names)
        self.assertNotIn("vfs.executor-workspace.write", names)


if __name__ == "__main__":
    unittest.main()
