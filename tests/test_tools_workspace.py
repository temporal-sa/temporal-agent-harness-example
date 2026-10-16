"""Workspace tools (P1 step 1.1) and executor job reattach.

- Rendering and bounding of tool results (pure).
- Executor /execute job semantics: attach, replay, bounded buffer, orphan grace, cancel,
  lost-job detection, lock serialization. Bubblewrap is stubbed with plain bash, so these
  run on macOS.
- run_command_activity and the /execute-backed tools against an in-process executor
  (uvicorn in a thread, bwrap stubbed), including a crash-and-reattach retry.
- A live suite against a real executor container, skipped unless
  WORKSPACE_EXECUTOR_LIVE_URL is set (see simple_chat_agent/worker/sandbox/README.md).
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import os
import shlex
import socket
import tempfile
import threading
import time
import unittest
import uuid
from pathlib import Path
from typing import Any
from unittest.mock import patch

import uvicorn
from fastapi import HTTPException
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment

from simple_chat_agent.agent.tools import workspace as ws
from simple_chat_agent.worker.sandbox import executor as ex
from temporal_agent_harness.harness import agent
from temporal_agent_harness.harness.stream_context import TurnStreamContext

TOKEN = "test-executor-token"


# -- Pure rendering and bounding -------------------------------------------------------------


class BoundingTests(unittest.TestCase):
    def test_head_tail_keeps_both_ends_within_limit(self) -> None:
        text = "HEAD" + "x" * 20_000 + "TAIL"
        bounded, cut = ws._bound_head_tail(text, 1_000)
        self.assertTrue(cut)
        self.assertLessEqual(len(bounded), 1_000)
        self.assertTrue(bounded.startswith("HEAD"))
        self.assertTrue(bounded.endswith("TAIL"))
        self.assertIn("chars omitted", bounded)

    def test_output_budget_gives_unused_room_to_the_other_stream(self) -> None:
        out, err, cut = ws.bounded_output("o" * 20_000, "short error")
        self.assertTrue(cut)
        self.assertEqual(err, "short error")
        self.assertLessEqual(len(out) + len(err), ws.MAX_COMMAND_OUTPUT_CHARS)
        self.assertGreater(len(out), ws.MAX_COMMAND_OUTPUT_CHARS - 100)

    def test_small_output_is_untouched(self) -> None:
        self.assertEqual(ws.bounded_output("a\n", "b\n"), ("a\n", "b\n", False))

    def test_unified_diff_counts_and_bounds(self) -> None:
        before = "".join(f"line {i}\n" for i in range(2_000))
        after = before.replace("line 5\n", "line five\n").replace("line 1999\n", "")
        diff, added, removed, cut = ws.unified_diff("a.py", before, after)
        self.assertEqual((added, removed), (1, 2))
        self.assertIn("+line five", diff)
        self.assertFalse(cut)

        big_after = "".join(f"new {i}\n" for i in range(2_000))
        diff, added, removed, cut = ws.unified_diff("a.py", before, big_after)
        self.assertTrue(cut)
        self.assertEqual((added, removed), (2_000, 2_000))  # counts cover the whole diff
        self.assertLessEqual(len(diff), ws.MAX_DIFF_CHARS + 80)

    def test_created_file_diff(self) -> None:
        diff, added, removed, _ = ws.unified_diff("n.txt", "", "a\nb")
        self.assertEqual((added, removed), (2, 0))
        self.assertIn("No newline at end of file", diff)

    def test_relative_path_check(self) -> None:
        self.assertEqual(ws._check_relative_path("src/a.py"), "src/a.py")
        for bad in ("/etc/passwd", "../x", "a/../../b"):
            with self.assertRaises(ApplicationError):
                ws._check_relative_path(bad)

    def test_execution_id_is_stable_and_valid(self) -> None:
        one = ws._execution_id("run_command", "wf-1", "toolu_abc")
        self.assertEqual(one, ws._execution_id("run_command", "wf-1", "toolu_abc"))
        self.assertNotEqual(one, ws._execution_id("run_command", "wf-2", "toolu_abc"))
        self.assertRegex(one, ex._SAFE_EXECUTION_ID.pattern)


class ResultRenderingTests(unittest.TestCase):
    def test_command_result_from_executor_payload(self) -> None:
        payload = {
            "exit_code": 1,
            "stdout": "o" * 50_000,
            "stderr": "Traceback\nAssertionError: boom\n",
            "elapsed_seconds": 3.25,
            "files_changed": {
                "created": ["new.py", ".git/index"],
                "modified": ["src/app.py"],
                "deleted": [],
            },
            "error": "Command exited with code 1.",
            "type": "WorkspaceShellProcessError",
        }
        result = ws.command_result("pytest -q", payload, log_ref="run_command:abc")
        self.assertEqual(result.exit_code, 1)
        self.assertTrue(result.truncated)
        self.assertEqual(
            [(c.path, c.status) for c in result.changed_files],
            [("src/app.py", "modified"), ("new.py", "created")],
        )
        text = str(result)
        self.assertIn("$ pytest -q", text)
        self.assertIn("exit 1 · 3.2s", text)
        self.assertIn("AssertionError: boom", text)
        self.assertIn("M src/app.py, A new.py", text)
        self.assertIn("full log: run_command:abc", text)
        self.assertLess(len(text), ws.MAX_COMMAND_OUTPUT_CHARS + 600)
        # The JSON form (what a structured consumer sees) is bounded too.
        self.assertLess(len(result.model_dump_json()), ws.MAX_COMMAND_OUTPUT_CHARS + 1_500)

    def test_command_result_timeout_and_reattach(self) -> None:
        result = ws.command_result(
            "sleep 99",
            {"exit_code": None, "timed_out": True, "elapsed_seconds": 5, "error": "timed out"},
            log_ref="x",
            attempt=2,
            reattached=True,
        )
        self.assertIn("exit ? · 5.0s · timed out · reattached (attempt 2)", str(result))

    def test_changed_files_are_capped(self) -> None:
        names = [f"f{i}.txt" for i in range(300)]
        result = ws.command_result(
            "touch", {"exit_code": 0, "files_changed": {"created": names}}, log_ref="x"
        )
        self.assertEqual(len(result.changed_files), ws.MAX_CHANGED_FILES)
        self.assertEqual(result.changed_files_total, 300)
        self.assertIn("(+280 more)", str(result))

    def test_edit_and_write_results(self) -> None:
        edit = ws.EditResult(path="a.py", added_lines=1, removed_lines=1, diff="@@ -1 +1 @@\n-a\n+b\n")
        self.assertTrue(str(edit).startswith("Edited a.py (+1 -1)."))
        write = ws.WriteResult(
            path="b.py", created=True, bytes_written=10, added_lines=2, removed_lines=0, diff=""
        )
        self.assertEqual(str(write), "Created b.py (10 bytes, +2 -0).")

    def test_list_files_text(self) -> None:
        result = ws.ListFilesResult(
            path=".",
            recursive=False,
            entries=[ws.FileEntry(path="src", type="directory"), ws.FileEntry(path="a.py", type="file", size=3)],
        )
        self.assertEqual(str(result), "src/\na.py  (3 B)")

    def test_search_output_parsing(self) -> None:
        lines = [
            '{"type":"begin","data":{"path":{"text":"./src/a.py"}}}',
            '{"type":"match","data":{"path":{"text":"./src/a.py"},"lines":{"text":"def refund():\\n"},"line_number":7}}',
            '{"type":"match","data":{"path":{"text":"b.py"},"lines":{"text":"' + "x" * 500 + '"},"line_number":1}}',
            '{"type":"match","data":{"path":{"text":"c.py"},"lines":{"text":"z"},"line_number":2}}',
            '{"type":"mat',  # cut mid-line by the executor
        ]
        matches, truncated = ws.parse_search_output("\n".join(lines), 2)
        self.assertEqual(matches[0], ws.SearchMatch(path="src/a.py", line=7, text="def refund():"))
        self.assertEqual(len(matches[1].text), ws.MAX_MATCH_TEXT_CHARS + 1)
        self.assertTrue(truncated)
        self.assertIn("src/a.py:7: def refund():", str(ws.SearchResult(pattern="refund", matches=matches)))

    def test_search_command_quotes_everything(self) -> None:
        command = ws.search_command("a'; rm -rf / #", "src dir", "*.py", 10)
        self.assertEqual(
            shlex.split(command)[-4:], ["--regexp", "a'; rm -rf / #", "--", "src dir"]
        )

    def test_git_status_parsing(self) -> None:
        result = ws.parse_git_status(
            "## main...origin/main [ahead 1]\n M src/app.py\nR  old.py -> new.py\n?? notes.txt\n"
        )
        self.assertEqual(result.branch, "main")
        self.assertFalse(result.clean)
        self.assertEqual(result.entries[1].original_path, "old.py")
        self.assertIn("?? notes.txt", str(result))
        self.assertTrue(ws.parse_git_status("## No commits yet on main\n").clean)

    def test_git_diff_parsing_and_bounding(self) -> None:
        stdout = f"3\t1\tsrc/app.py\n-\t-\tlogo.png\n\n{ws._SEPARATOR}\n" + "+x\n" * 10_000
        result = ws.parse_git_diff(stdout, staged=False)
        self.assertEqual(result.files[0], ws.GitDiffFile(path="src/app.py", added=3, removed=1))
        self.assertIsNone(result.files[1].added)
        self.assertTrue(result.truncated)
        self.assertLess(len(result.diff), ws.MAX_GIT_DIFF_CHARS + 100)

    def test_git_commit_parsing(self) -> None:
        result = ws.parse_git_commit(
            {
                "exit_code": 0,
                "stdout": "abc123def4567890\nFix refund rounding\n 2 files changed, 5 insertions(+), 1 deletion(-)\n",
            }
        )
        self.assertEqual((result.files_changed, result.insertions, result.deletions), (2, 5, 1))
        self.assertIn("Committed abc123def456 'Fix refund rounding'", str(result))
        nothing = ws.parse_git_commit({"exit_code": 3, "stdout": "nothing to commit\n"})
        self.assertFalse(nothing.committed)
        with self.assertRaises(ApplicationError):
            ws.parse_git_commit({"exit_code": 128, "stderr": "fatal: not a git repository"})

    def test_progress_coalescer_batches_and_caps(self) -> None:
        published: list[str] = []
        coalescer = ws._ProgressCoalescer(published.append, flush_seconds=60, flush_chars=10, max_chars=25)
        for _ in range(3):
            coalescer.add("abcd")
        self.assertEqual(published, ["abcdabcdabcd"])
        coalescer.add("x" * 20)  # crosses the 25-char budget
        coalescer.add("more")
        coalescer.flush()
        self.assertEqual("".join(published).count("x"), 13)
        self.assertIn("live output limit", published[-1])


class ToolDeclarationTests(unittest.TestCase):
    def test_exports(self) -> None:
        names = [tool.__name__ for tool in ws.WORKSPACE_TOOLS]
        self.assertEqual(
            names,
            ["list_files", "read_file", "search_code", "write_file", "edit_file",
             "run_command", "git_status", "git_diff", "git_commit"],
        )
        self.assertEqual(ws.WORKSPACE_ACTIVITIES, [ws.run_command_activity])
        self.assertNotIn("push", " ".join(names))
        # run_command is inline (no activity body); the rest are activity tools.
        self.assertFalse(getattr(ws.run_command, "__agent_activity_tool__", False))
        for tool in ws.WORKSPACE_TOOLS:
            if tool is not ws.run_command:
                agent.tool_activity(tool)

    def test_workspace_id_is_hidden_from_the_model(self) -> None:
        from temporal_agent_harness.ai_sdks.pydantic_ai_harness import _tool_schema

        for tool in ws.WORKSPACE_TOOLS:
            _, _, schema = _tool_schema(tool)
            self.assertNotIn("workspace_id", schema.get("properties", {}), tool.__name__)


# -- Executor jobs (in-process, bwrap stubbed) -----------------------------------------------


def _stub_sandbox_command(request: ex.ExecuteRequest, workspace_dir: Path) -> list[str]:
    cwd = workspace_dir / Path(ex._normalize_cwd(request.cwd)).relative_to("/workspace")
    script = f"cd {shlex.quote(str(cwd))} && {request.command}"
    # The launcher env has a minimal PATH; give the stub the host's so rg and git resolve.
    return ["env", f"PATH={os.environ.get('PATH', '')}", "bash", "--noprofile", "--norc",
            "-o", "pipefail", "-c", script]


class _ExecutorEnv:
    """Fresh executor root, token, stubbed sandbox and fresh loop-bound globals."""

    def setup_executor(self, case: unittest.TestCase, **env: str) -> Path:
        temp_dir = tempfile.TemporaryDirectory()
        case.addCleanup(temp_dir.cleanup)
        patches = [
            patch.dict(
                "os.environ",
                {"WORKSPACE_EXECUTOR_ROOT": temp_dir.name, "WORKSPACE_EXECUTOR_TOKEN": TOKEN, **env},
            ),
            patch.object(ex, "_sandbox_command", _stub_sandbox_command),
            patch.object(ex, "_workspace_locks", {}),
            patch.object(ex, "_workspace_locks_guard", asyncio.Lock()),
            patch.object(ex, "_concurrency", asyncio.Semaphore(4)),
            patch.object(ex, "_jobs", {}),
        ]
        for patcher in patches:
            patcher.start()
            case.addCleanup(patcher.stop)
        return Path(temp_dir.name)

    @staticmethod
    def request(command: str, execution_id: str = "job:1", workspace: str = "ws-1", **kw: Any) -> ex.ExecuteRequest:
        return ex.ExecuteRequest(workspace_id=workspace, execution_id=execution_id, command=command, **kw)

    @staticmethod
    def workspace(workspace: str = "ws-1") -> Path:
        path = ex._workspace_dir(ex._workspace_key(workspace))
        path.mkdir(parents=True, exist_ok=True)
        return path


async def _collect(events: Any, *, until_text: str | None = None) -> list[dict[str, Any]]:
    seen: list[dict[str, Any]] = []
    async for event in events:
        seen.append(event)
        if until_text and until_text in str(event.get("payload", {}).get("text", "")):
            break
    return seen


def _text(events: list[dict[str, Any]]) -> str:
    return "".join(
        e["payload"].get("text", "") for e in events if e["kind"] in ("stdout", "stderr")
    )


class ExecutorJobTests(_ExecutorEnv, unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.setup_executor(self)

    async def test_concurrent_request_with_same_id_attaches_and_runs_once(self) -> None:
        ws_dir = self.workspace()
        command = "echo run >> runs.txt; echo hi; sleep 0.6; echo done"
        first = ex._execute_events(self.request(command))
        head = await _collect(first, until_text="hi")
        second = await _collect(ex._execute_events(self.request(command)))
        rest = await _collect(first)

        self.assertFalse(head[0]["payload"]["attached"])
        self.assertTrue(second[0]["payload"]["attached"])
        self.assertEqual(_text(second), "hi\ndone\n")  # replayed "hi", then live "done"
        self.assertEqual(_text(head + rest), "hi\ndone\n")
        self.assertEqual(second[-1]["payload"]["exit_code"], 0)
        self.assertEqual(second[-1]["payload"], rest[-1]["payload"])
        self.assertEqual((ws_dir / "runs.txt").read_text(), "run\n")
        self.assertEqual(ex._jobs, {})

    async def test_disconnect_leaves_job_running_for_reattach(self) -> None:
        ws_dir = self.workspace()
        command = "echo run >> runs.txt; echo hi; sleep 0.6; echo done"
        first = ex._execute_events(self.request(command))
        await _collect(first, until_text="hi")
        await first.aclose()  # the worker died
        self.assertEqual(len(ex._jobs), 1)

        again = await _collect(ex._execute_events(self.request(command)))
        self.assertTrue(again[0]["payload"]["attached"])
        self.assertEqual(again[-1]["payload"]["exit_code"], 0)
        self.assertEqual(_text(again), "hi\ndone\n")
        self.assertEqual((ws_dir / "runs.txt").read_text(), "run\n")

    async def test_finished_job_returns_cached_result_without_rerunning(self) -> None:
        ws_dir = self.workspace()
        command = "echo run >> runs.txt; echo hi"
        await _collect(ex._execute_events(self.request(command)))
        cached = await _collect(ex._execute_events(self.request(command)))
        self.assertTrue(cached[-1]["payload"]["cached"])
        self.assertEqual(cached[-1]["payload"]["stdout"], "hi\n")
        self.assertEqual((ws_dir / "runs.txt").read_text(), "run\n")

    async def test_finished_job_replays_its_output_to_a_late_client(self) -> None:
        # Regression (1.1c): attempt 2 of a crashed run_command often arrives after the
        # command finished. It must still get the output, not just the result.
        self.workspace()
        command = "echo hi; sleep 0.1; echo finished"
        live = await _collect(ex._execute_events(self.request(command)))
        late = await _collect(ex._execute_events(self.request(command)))
        self.assertTrue(late[0]["payload"]["attached"])
        self.assertTrue(late[0]["payload"]["cached"])
        self.assertEqual(_text(late), "hi\nfinished\n")
        self.assertEqual(
            [e["seq"] for e in late if "seq" in e], [e["seq"] for e in live if "seq" in e]
        )
        self.assertEqual(late[-1]["kind"], "result")

    async def test_finished_job_replay_marks_dropped_events(self) -> None:
        self.workspace()
        command = "for i in $(seq 1 10); do echo line$i; sleep 0.01; done"
        with patch.object(ex, "JOB_REPLAY_BUFFER_EVENTS", 3):
            await _collect(ex._execute_events(self.request(command)))
        late = await _collect(ex._execute_events(self.request(command)))
        self.assertEqual(late[1]["kind"], "dropped")
        self.assertEqual(late[1]["payload"]["next_seq"], late[2]["seq"])
        self.assertEqual(_text(late), "line8\nline9\nline10\n")

    async def test_events_carry_increasing_seq(self) -> None:
        self.workspace()
        events = await _collect(ex._execute_events(self.request("echo a; sleep 0.1; echo b >&2")))
        seqs = [e["seq"] for e in events if e["kind"] in ("stdout", "stderr")]
        self.assertEqual(seqs, sorted(seqs))
        self.assertEqual(len(seqs), 2)

    async def test_different_command_with_live_id_is_rejected(self) -> None:
        self.workspace()
        first = ex._execute_events(self.request("echo hi; sleep 0.5"))
        await _collect(first, until_text="hi")
        with self.assertRaises(HTTPException) as caught:
            ex._execute_events(self.request("echo other"))
        self.assertEqual(caught.exception.status_code, 409)
        await _collect(first)

    async def test_replay_buffer_drops_oldest_with_a_marker(self) -> None:
        self.workspace()
        command = "for i in $(seq 1 30); do echo line$i; sleep 0.01; done; sleep 0.3; echo end"
        with patch.object(ex, "JOB_REPLAY_BUFFER_EVENTS", 5):
            first = ex._execute_events(self.request(command))
            await _collect(first, until_text="line30")
            late = await _collect(ex._execute_events(self.request(command)))
            await _collect(first)
        kinds = [e["kind"] for e in late]
        self.assertEqual(kinds[1], "dropped")
        self.assertGreater(late[1]["payload"]["dropped_events"], 0)
        self.assertIn("line30", _text(late))
        self.assertNotIn("line1\n", _text(late))
        self.assertEqual(late[-1]["kind"], "result")
        self.assertIn("line1\n", late[-1]["payload"]["stdout"])  # the result is never dropped

    async def test_job_with_no_client_is_stopped_after_grace(self) -> None:
        self.workspace()
        with patch.dict("os.environ", {"WORKSPACE_EXECUTOR_ORPHAN_GRACE_SECONDS": "1"}):
            first = ex._execute_events(self.request("echo hi; sleep 30"))
            await _collect(first, until_text="hi")
            await first.aclose()
            job = next(iter(ex._jobs.values()))
            await asyncio.wait_for(job.task, 10)
        self.assertEqual(job.result["type"], "WorkspaceShellAbandoned")
        cached = await _collect(ex._execute_events(self.request("echo hi; sleep 30")))
        self.assertEqual(cached[-1]["payload"]["type"], "WorkspaceShellAbandoned")

    async def test_cancel_stops_running_and_queued_jobs(self) -> None:
        self.workspace()
        running = ex._execute_events(self.request("echo hi; sleep 30", "job:run"))
        await _collect(running, until_text="hi")
        queued = ex._execute_events(self.request("echo never", "job:queued"))
        queued_start = await anext(queued)
        self.assertEqual(queued_start["payload"]["state"], "queued")

        for execution_id in ("job:queued", "job:run"):
            response = await ex.execute_cancel(
                ex.ExecuteCancelRequest(workspace_id="ws-1", execution_id=execution_id),
                authorization=f"Bearer {TOKEN}",
            )
            self.assertEqual(response["state"], "cancelling")
        run_events = await asyncio.wait_for(_collect(running), 10)
        queued_events = await asyncio.wait_for(_collect(queued), 10)
        self.assertEqual(run_events[-1]["payload"]["type"], "WorkspaceShellCancelled")
        self.assertEqual(queued_events[-1]["payload"]["type"], "WorkspaceShellCancelled")
        self.assertNotIn("never", _text(queued_events))
        unknown = await ex.execute_cancel(
            ex.ExecuteCancelRequest(workspace_id="ws-1", execution_id="job:none"),
            authorization=f"Bearer {TOKEN}",
        )
        self.assertEqual(unknown["state"], "not_running")

    async def test_lost_job_after_executor_restart_is_not_rerun(self) -> None:
        ws_dir = self.workspace()
        state = ex._state_path(ex._workspace_key("ws-1"), "job:lost")
        ex._mark_started(state)  # started under a previous executor process, no result
        events = await _collect(ex._execute_events(self.request("echo run >> runs.txt", "job:lost")))
        self.assertEqual(events[-1]["payload"]["type"], "WorkspaceJobLost")
        self.assertFalse((ws_dir / "runs.txt").exists())

    async def test_commands_in_one_workspace_still_serialize(self) -> None:
        ws_dir = self.workspace()
        first = ex._execute_events(self.request("echo a-start >> log; sleep 0.4; echo a-end >> log", "job:a"))
        second = ex._execute_events(self.request("echo b >> log", "job:b"))
        await asyncio.gather(_collect(first), _collect(second))
        self.assertEqual((ws_dir / "log").read_text().split(), ["a-start", "a-end", "b"])


# -- Tools against an in-process executor server ---------------------------------------------


class _ServerThread:
    def __init__(self) -> None:
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        self.port = sock.getsockname()[1]
        sock.close()
        self.server = uvicorn.Server(
            uvicorn.Config(ex.app, host="127.0.0.1", port=self.port, log_level="warning", lifespan="off")
        )
        self.thread = threading.Thread(target=self.server.run, daemon=True)

    def __enter__(self) -> str:
        self.thread.start()
        deadline = time.monotonic() + 10
        while not self.server.started:
            if time.monotonic() > deadline:
                raise RuntimeError("executor test server did not start")
            time.sleep(0.02)
        return f"http://127.0.0.1:{self.port}"

    def __exit__(self, *exc: Any) -> None:
        self.server.should_exit = True
        self.thread.join(10)


class _FakePublisher:
    def __init__(self) -> None:
        self.events: list[tuple[float, Any]] = []

    def publish(self, event: Any) -> None:
        self.events.append((time.monotonic(), event))

    @property
    def text(self) -> str:
        return "".join(e.progress_delta for _, e in self.events)


def _tool_ctx(tool_id: str) -> agent.AgentToolContext:
    return agent.AgentToolContext(
        stream_context=TurnStreamContext(turn_id="turn-1", turn_number=1, agent_id="a1"),
        tool_id=tool_id,
    )


class _ToolServerEnv(_ExecutorEnv):
    def start_server(self, case: unittest.TestCase) -> None:
        self.setup_executor(case)
        # Loop-bound globals are first used on the server thread's loop.
        server = _ServerThread()
        url = server.__enter__()
        case.addCleanup(server.__exit__, None, None, None)
        env = patch.dict("os.environ", {"WORKSPACE_EXECUTOR_URL": url})
        env.start()
        case.addCleanup(env.stop)
        self.publisher = _FakePublisher()

        @contextlib.asynccontextmanager
        async def fake_publisher(context: Any, *, batch_interval: Any = None) -> Any:
            yield self.publisher

        pub = patch.object(ws.AgentWorkflowRunner, "publisher_from_activity", fake_publisher)
        pub.start()
        case.addCleanup(pub.stop)

    def env(self, attempt: int = 1, heartbeat_details: list[Any] | None = None, activity_id: str = "act-1") -> ActivityEnvironment:
        environment = ActivityEnvironment()
        environment.info = dataclasses.replace(
            environment.info,
            workflow_id="wf-1",
            activity_id=activity_id,
            attempt=attempt,
            heartbeat_details=heartbeat_details or [],
        )
        return environment


class RunCommandActivityTests(_ToolServerEnv, unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.start_server(self)
        self.ws_dir = self.workspace()

    def input(self, command: str, tool_id: str = "toolu_1", **kw: Any) -> ws.RunCommandInput:
        return ws.RunCommandInput(workspace_id="ws-1", command=command, tool_ctx=_tool_ctx(tool_id), **kw)

    async def test_streams_progress_and_returns_bounded_result(self) -> None:
        heartbeats: list[Any] = []
        environment = self.env()
        environment.on_heartbeat = lambda *d: heartbeats.append(d)
        started = time.monotonic()
        result = await environment.run(
            ws.run_command_activity,
            self.input("echo hi; sleep 1.5; echo done; echo oops >&2; touch made.txt"),
        )
        self.assertEqual(result.exit_code, 0)
        self.assertIn("hi\ndone", result.stdout_tail)
        self.assertEqual(result.stderr_tail, "oops\n")
        self.assertEqual([(c.path, c.status) for c in result.changed_files], [("made.txt", "created")])
        self.assertEqual(result.log_ref, ws._execution_id("run_command", "wf-1", "toolu_1"))
        # "hi" was published well before the command finished (within 1 s of output).
        first_at, first = self.publisher.events[0]
        self.assertEqual(first.progress_delta, "hi\n")
        self.assertEqual(first.tool_id, "toolu_1")
        self.assertLess(first_at - started, 1.0)
        self.assertEqual(self.publisher.text, "hi\ndone\noops\n")
        self.assertTrue(heartbeats)
        self.assertEqual(heartbeats[-1][0]["execution_id"], result.log_ref)

    async def test_nonzero_exit_is_a_result_not_an_error(self) -> None:
        result = await self.env().run(ws.run_command_activity, self.input("echo bad >&2; exit 3"))
        self.assertEqual(result.exit_code, 3)
        self.assertIn("exit 3", str(result))

    async def test_output_is_bounded(self) -> None:
        result = await self.env().run(
            ws.run_command_activity, self.input("python3 -c \"print('x' * 60000)\"; echo LAST")
        )
        self.assertTrue(result.truncated)
        self.assertTrue(result.stdout_tail.rstrip().endswith("LAST"))
        self.assertLessEqual(len(result.stdout_tail) + len(result.stderr_tail), ws.MAX_COMMAND_OUTPUT_CHARS)

    async def test_retry_after_worker_crash_reattaches_and_runs_once(self) -> None:
        command = "echo run >> runs.txt; echo hi; sleep 1.5; echo done"
        heartbeats: list[Any] = []
        first_env = self.env()
        first_env.on_heartbeat = lambda *d: heartbeats.append(d)
        # A crashed worker sends no cancel; only the stream disappears.
        with patch.object(ws, "_cancel_execution", self._no_cancel):
            attempt1 = asyncio.create_task(first_env.run(ws.run_command_activity, self.input(command)))
            while "hi" not in self.publisher.text:
                await asyncio.sleep(0.02)
            await asyncio.sleep(1.1)  # let a heartbeat record the delivered seq
            attempt1.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await attempt1
        self.assertEqual(len(ex._jobs), 1)  # still running in the executor
        delivered = len(self.publisher.events)

        result = await self.env(attempt=2, heartbeat_details=[heartbeats[-1][0]]).run(
            ws.run_command_activity, self.input(command)
        )
        self.assertEqual(result.exit_code, 0)
        self.assertTrue(result.reattached)
        self.assertEqual(result.attempt, 2)
        self.assertEqual(result.stdout_tail, "hi\ndone\n")
        self.assertEqual((self.ws_dir / "runs.txt").read_text(), "run\n")  # one start marker
        retried = "".join(e.progress_delta for _, e in self.publisher.events[delivered:])
        self.assertIn("reattaching", retried)
        self.assertIn("done", retried)
        self.assertNotIn("hi\n", retried)  # not re-published

    async def test_retry_after_the_command_finished_publishes_the_missed_output(self) -> None:
        # Regression (1.1c): worker A dies mid-command; by the time attempt 2 runs, the
        # command has finished. Attempt 2 must publish the output produced after the
        # crash ("finished"), not only the reattach note, and must not re-publish "hi".
        command = "echo run >> runs.txt; echo hi; sleep 0.8; echo finished"
        heartbeats: list[Any] = []
        first_env = self.env()
        first_env.on_heartbeat = lambda *d: heartbeats.append(d)
        with patch.object(ws, "_cancel_execution", self._no_cancel):
            attempt1 = asyncio.create_task(first_env.run(ws.run_command_activity, self.input(command)))
            while not any(d[0].get("seq", -1) >= 0 for d in heartbeats):
                await asyncio.sleep(0.02)
            attempt1.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await attempt1
        deadline = time.monotonic() + 10
        while ex._jobs and time.monotonic() < deadline:
            await asyncio.sleep(0.05)
        self.assertEqual(ex._jobs, {})  # finished before the retry
        delivered = len(self.publisher.events)

        result = await self.env(attempt=2, heartbeat_details=[heartbeats[-1][0]]).run(
            ws.run_command_activity, self.input(command)
        )
        retried = "".join(e.progress_delta for _, e in self.publisher.events[delivered:])
        self.assertIn("reattaching", retried)
        self.assertIn("finished\n", retried)
        self.assertNotIn("hi\n", retried)
        self.assertNotIn("dropped", retried)
        self.assertTrue(result.reattached)
        self.assertEqual(result.stdout_tail, "hi\nfinished\n")
        self.assertEqual((self.ws_dir / "runs.txt").read_text(), "run\n")

    async def test_retry_without_heartbeat_replays_everything_from_a_finished_job(self) -> None:
        command = "echo hi; echo finished"
        await self.env().run(ws.run_command_activity, self.input(command))
        delivered = len(self.publisher.events)
        await self.env(attempt=2).run(ws.run_command_activity, self.input(command))
        retried = "".join(e.progress_delta for _, e in self.publisher.events[delivered:])
        self.assertIn("hi\nfinished\n", retried)

    async def test_cancelled_activity_cancels_the_job(self) -> None:
        task = asyncio.create_task(self.env().run(ws.run_command_activity, self.input("echo hi; sleep 30")))
        while "hi" not in self.publisher.text:
            await asyncio.sleep(0.02)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        deadline = time.monotonic() + 10
        while ex._jobs and time.monotonic() < deadline:
            await asyncio.sleep(0.05)
        self.assertEqual(ex._jobs, {})

    async def test_unreachable_executor_is_retryable(self) -> None:
        with patch.dict("os.environ", {"WORKSPACE_EXECUTOR_URL": "http://127.0.0.1:9"}):
            with self.assertRaises(ApplicationError) as caught:
                await self.env().run(ws.run_command_activity, self.input("echo hi"))
        self.assertEqual(caught.exception.type, "ExecutorUnavailable")
        self.assertFalse(caught.exception.non_retryable)

    async def test_bad_cwd_is_not_retryable(self) -> None:
        with self.assertRaises(ApplicationError) as caught:
            await self.env().run(ws.run_command_activity, self.input("pwd", cwd="/etc"))
        self.assertTrue(caught.exception.non_retryable)
        self.assertEqual(caught.exception.type, "InvalidCommand")

    @staticmethod
    async def _no_cancel(*_: Any) -> None:
        return None


def _activity_body(tool: Any) -> Any:
    return agent.tool_activity(tool)


class ExecToolsTests(_ToolServerEnv, unittest.IsolatedAsyncioTestCase):
    """search_code and git tools through /execute. bash stands in for bwrap, so these need
    rg and git on the host; skipped where missing."""

    async def asyncSetUp(self) -> None:
        self.start_server(self)
        self.ws_dir = self.workspace()

    async def call(self, tool: Any, *args: Any, activity_id: str | None = None) -> Any:
        environment = self.env(activity_id=activity_id or uuid.uuid4().hex)
        return await environment.run(_activity_body(tool), *args, _tool_ctx("t"))

    @unittest.skipUnless(__import__("shutil").which("rg"), "ripgrep not installed")
    async def test_search_code(self) -> None:
        (self.ws_dir / "src").mkdir()
        (self.ws_dir / "src" / "refund.py").write_text("def refund(amount):\n    return round(amount)\n")
        (self.ws_dir / "notes.md").write_text("refund policy\n")
        result = await self.call(ws.search_code, "refund", "ws-1", ".", "*.py", 50)
        self.assertEqual([(m.path, m.line) for m in result.matches], [("src/refund.py", 1)])
        none = await self.call(ws.search_code, "zzz_nothing", "ws-1", ".", None, 50)
        self.assertEqual(str(none), "No matches for 'zzz_nothing'.")

    @unittest.skipUnless(__import__("shutil").which("git"), "git not installed")
    async def test_git_status_diff_commit(self) -> None:
        import subprocess

        subprocess.run(["git", "init", "-q", "-b", "main"], cwd=self.ws_dir, check=True)
        (self.ws_dir / "app.py").write_text("x = 1\n")
        hook = self.ws_dir / ".git" / "hooks" / "pre-commit"
        hook.write_text("#!/bin/sh\ntouch HOOK_RAN\n")
        hook.chmod(0o755)

        status = await self.call(ws.git_status, "ws-1")
        self.assertEqual(status.branch, "main")
        self.assertEqual([(e.status, e.path) for e in status.entries], [("??", "app.py")])

        with self.assertRaises(ApplicationError):
            await self.call(ws.git_commit, "   ", "ws-1", True)
        commit = await self.call(ws.git_commit, "Add app", "ws-1", True, activity_id="commit-1")
        self.assertTrue(commit.committed, commit)
        self.assertEqual(commit.summary, "Add app")
        self.assertFalse((self.ws_dir / "HOOK_RAN").exists())  # hooks never run
        # A retry of the same tool call returns the first result; no second commit.
        again = await self.call(ws.git_commit, "Add app", "ws-1", True, activity_id="commit-1")
        self.assertEqual(again.commit, commit.commit)

        (self.ws_dir / "app.py").write_text("x = 2\n")
        diff = await self.call(ws.git_diff, "ws-1", None, False)
        self.assertEqual(diff.files, [ws.GitDiffFile(path="app.py", added=1, removed=1)])
        self.assertIn("+x = 2", diff.diff)
        staged = await self.call(ws.git_diff, "ws-1", None, True)
        self.assertEqual(str(staged), "No staged changes.")
        nothing = await self.call(ws.git_commit, "noop", "ws-1", False)
        self.assertFalse(nothing.committed)


class FileToolsTests(_ToolServerEnv, unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.start_server(self)
        self.ws_dir = self.workspace()

    async def call(self, tool: Any, *args: Any, attempt: int = 1) -> Any:
        return await self.env(attempt=attempt).run(_activity_body(tool), *args, _tool_ctx("t"))

    async def test_write_reports_counts_and_diff(self) -> None:
        created = await self.call(ws.write_file, "pkg/a.py", "a\nb\n", "ws-1")
        self.assertTrue(created.created)
        self.assertEqual((created.added_lines, created.removed_lines), (2, 0))
        changed = await self.call(ws.write_file, "pkg/a.py", "a\nc\n", "ws-1")
        self.assertFalse(changed.created)
        self.assertEqual((changed.added_lines, changed.removed_lines), (1, 1))
        self.assertIn("-b\n+c\n", changed.diff)

    async def test_edit_exact_unique_match(self) -> None:
        (self.ws_dir / "a.py").write_text("one\ntwo\nthree\ntwo\n")
        result = await self.call(ws.edit_file, "a.py", "one\n", "ONE\n", "ws-1")
        self.assertEqual((result.added_lines, result.removed_lines), (1, 1))
        self.assertEqual((self.ws_dir / "a.py").read_text(), "ONE\ntwo\nthree\ntwo\n")
        with self.assertRaises(ApplicationError) as caught:
            await self.call(ws.edit_file, "a.py", "two\n", "2\n", "ws-1")
        self.assertTrue(caught.exception.non_retryable)
        self.assertIn("more than once", str(caught.exception))
        with self.assertRaises(ApplicationError):
            await self.call(ws.edit_file, "a.py", "", "x", "ws-1")

    async def test_edit_retry_after_lost_response_reports_already_applied(self) -> None:
        (self.ws_dir / "a.py").write_text("x = 1\n")
        await self.call(ws.edit_file, "a.py", "x = 1", "x = 2", "ws-1")
        retried = await self.call(ws.edit_file, "a.py", "x = 1", "x = 2", "ws-1", attempt=2)
        self.assertTrue(retried.already_applied)
        self.assertEqual((retried.added_lines, retried.removed_lines), (1, 1))

    async def test_list_files_flat_and_recursive(self) -> None:
        (self.ws_dir / "src").mkdir()
        (self.ws_dir / "src" / "a.py").write_text("x")
        (self.ws_dir / ".git").mkdir()
        (self.ws_dir / ".git" / "HEAD").write_text("x")
        flat = await self.call(ws.list_files, "ws-1", ".", False)
        self.assertEqual([e.path for e in flat.entries], [".git", "src"])
        deep = await self.call(ws.list_files, "ws-1", ".", True)
        self.assertEqual([e.path for e in deep.entries], [".git", "src", "src/a.py"])
        with self.assertRaises(ApplicationError):
            await self.call(ws.list_files, "ws-1", "../other", False)


# -- Live executor (Docker) ------------------------------------------------------------------


@unittest.skipUnless(
    os.environ.get("WORKSPACE_EXECUTOR_LIVE_URL"),
    "set WORKSPACE_EXECUTOR_LIVE_URL (and WORKSPACE_EXECUTOR_TOKEN) to run against a live executor",
)
class LiveExecutorTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        env = patch.dict("os.environ", {"WORKSPACE_EXECUTOR_URL": os.environ["WORKSPACE_EXECUTOR_LIVE_URL"]})
        env.start()
        self.addCleanup(env.stop)
        self.workspace_id = f"live-{uuid.uuid4().hex[:8]}"
        self.publisher = _FakePublisher()

        @contextlib.asynccontextmanager
        async def fake_publisher(context: Any, *, batch_interval: Any = None) -> Any:
            yield self.publisher

        pub = patch.object(ws.AgentWorkflowRunner, "publisher_from_activity", fake_publisher)
        pub.start()
        self.addCleanup(pub.stop)

    def body(self, command: str, execution_id: str) -> dict[str, Any]:
        return {"workspace_id": self.workspace_id, "execution_id": execution_id, "command": command,
                "cwd": "/workspace", "timeout_seconds": 60, "network": False}

    async def test_run_command_streams(self) -> None:
        environment = ActivityEnvironment()
        environment.info = dataclasses.replace(environment.info, workflow_id=self.workspace_id)
        started = time.monotonic()
        result = await environment.run(
            ws.run_command_activity,
            ws.RunCommandInput(workspace_id=self.workspace_id, command="echo hi; sleep 2; echo done",
                               tool_ctx=_tool_ctx("live-1")),
        )
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(result.stdout_tail, "hi\ndone\n")
        first_at, first = self.publisher.events[0]
        last_at, _ = self.publisher.events[-1]
        self.assertEqual(first.progress_delta, "hi\n")
        self.assertLess(first_at - started, 1.0)
        self.assertGreater(last_at - first_at, 1.5)  # "done" arrived live, ~2 s later

    async def test_concurrent_same_execution_id_reattaches(self) -> None:
        execution_id = f"live:{uuid.uuid4().hex[:12]}"
        command = "echo run >> runs.txt; echo hi; sleep 2; echo done"
        first_events: list[tuple[str, dict[str, Any]]] = []

        async def first() -> tuple[dict[str, Any], bool]:
            return await ws._stream_execute(
                self.body(command, execution_id), on_event=lambda k, p: first_events.append((k, p))
            )

        task = asyncio.create_task(first())
        while not any("hi" in str(p.get("text")) for _, p in first_events):
            await asyncio.sleep(0.05)
        second_events: list[tuple[str, dict[str, Any]]] = []
        second, attached = await ws._stream_execute(
            self.body(command, execution_id), on_event=lambda k, p: second_events.append((k, p))
        )
        first_result, first_attached = await task
        self.assertTrue(attached)
        self.assertFalse(first_attached)
        self.assertEqual("".join(str(p.get("text")) for k, p in second_events if k == "stdout"), "hi\ndone\n")
        self.assertEqual(second["stdout"], first_result["stdout"])
        count, _ = await ws._stream_execute(self.body("wc -l < runs.txt", f"live:{uuid.uuid4().hex[:12]}"))
        self.assertEqual(count["stdout"].strip(), "1")


if __name__ == "__main__":
    unittest.main()
