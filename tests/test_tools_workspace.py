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
from temporalio.activity import ActivityCancellationDetails
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


class SandboxEnvTests(unittest.TestCase):
    def test_bwrap_command_sets_git_identity_and_it_is_configurable(self) -> None:
        request = ex.ExecuteRequest(workspace_id="w", execution_id="e:1", command="true")
        with patch.dict("os.environ", {"WORKSPACE_EXECUTOR_GIT_NAME": "Bot", "WORKSPACE_EXECUTOR_GIT_EMAIL": "bot@x.invalid"}):
            args = ex._sandbox_command(request, Path("/w"))
        pairs = {args[i + 1]: args[i + 2] for i, arg in enumerate(args) if arg == "--setenv"}
        for name in ("GIT_AUTHOR_NAME", "GIT_COMMITTER_NAME"):
            self.assertEqual(pairs[name], "Bot")
        for name in ("GIT_AUTHOR_EMAIL", "GIT_COMMITTER_EMAIL"):
            self.assertEqual(pairs[name], "bot@x.invalid")
        self.assertEqual(pairs["HOME"], "/workspace")
        self.assertEqual(ex._sandbox_env()["GIT_AUTHOR_NAME"], "Coding Agent")


class BusyMessageTests(unittest.TestCase):
    def test_message_matches_what_holds_the_workspace(self) -> None:
        # N7.
        def job(state: str, execution_id: str) -> Any:
            j = ex._Job(workspace_key="k", execution_id=execution_id, fingerprint="f")
            j.state = state  # type: ignore[assignment]
            return j

        cases = [
            ([], "Another file change is in progress"),
            ([job("queued", "q1")], "1 command is queued in the workspace"),
            ([job("queued", "q1"), job("queued", "q2")], "2 commands are queued"),
            ([job("running", "r"), job("queued", "q1")], "A command is running in the workspace and 1 more is queued"),
        ]
        for jobs, expected in cases:
            with self.subTest(expected), patch.object(ex, "_jobs", {("k", j.execution_id): j for j in jobs}):
                message = ex._busy_message("k")
                self.assertIn(expected, message)
                self.assertIn("nothing was written", message)
        with patch.object(ex, "_jobs", {("other", "r"): job("running", "r")}):
            self.assertIn("Another file change", ex._busy_message("k"))


class GitHardeningUnitTests(unittest.TestCase):
    def test_git_version_self_test(self) -> None:
        import subprocess as sp

        for output, ok in (("git version 2.39.5\n", False), ("git version 2.40.0\n", True),
                           ("git version 2.47.3\n", True), ("garbage", False)):
            with self.subTest(output), patch.object(ex.shutil, "which", return_value="/usr/bin/git"), \
                    patch.object(ex.subprocess, "run", return_value=sp.CompletedProcess([], 0, output, "")):
                error = ex._git_version_self_test()
                self.assertEqual(error is None, ok, error)
        with patch.object(ex.shutil, "which", return_value=None):
            self.assertIn("not installed", ex._git_version_self_test() or "")

    def test_overrides_use_git_config_count_not_dash_c(self) -> None:
        prelude = ws.GIT_PRELUDE
        self.assertNotIn(" -c ", prelude)
        self.assertIn(f"GIT_CONFIG_COUNT={len(ws.GIT_CONFIG_OVERRIDES)}", prelude)
        keys = dict(ws.GIT_CONFIG_OVERRIDES)
        for key, value in (("maintenance.auto", "false"), ("gc.auto", "0"), ("core.fsmonitor", "false"),
                           ("core.hooksPath", "/dev/null"), ("commit.gpgsign", "false")):
            self.assertEqual(keys[key], value)
        for command in (ws.git_status_command(), ws.git_diff_command(None, False), ws.git_commit_command(True)):
            self.assertTrue(command.startswith(prelude))
            self.assertIn("safe_git_preflight", command)

    def test_sandbox_env_keeps_xdg_config_out_of_the_workspace(self) -> None:
        # N5.
        self.assertFalse(ex._sandbox_env()["XDG_CONFIG_HOME"].startswith("/workspace"))


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

    def test_git_commit_is_shell_class(self) -> None:
        import inspect

        source = inspect.getsource(ws)
        decorator = source[: source.index("async def git_commit(")].rsplit("@agent.", 1)[1]
        self.assertIn('auto_approval_criteria="shell"', decorator)
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
    # Mirror the sandbox environment: its variables (git identity included), HOME = the
    # workspace, and the host PATH so rg and git resolve.
    sandbox_env = {**ex._sandbox_env(), "HOME": str(workspace_dir), "PATH": os.environ.get("PATH", "")}
    return ["env", "-i", *[f"{k}={v}" for k, v in sandbox_env.items()],
            "bash", "--noprofile", "--norc", "-o", "pipefail", "-c", script]


# B1 scenarios. Each planted program appends a word to .git/PWNED when git runs it.
GIT_BASE_REPO = r"""
set -e
git init -q -b main
for f in a b c d; do echo 1 > $f.txt; done
git add -A
git commit -q --no-verify -m init
for f in a b c d; do echo 2 > $f.txt; done
"""
# Repo config filters chosen by .gitattributes and by .git/info/attributes, including a
# long-running `process` filter.
PLANT_REPO_FILTERS = r"""
git config filter.repo.clean 'sh -c "echo repo >> .git/PWNED; cat"'
git config filter.info.clean 'sh -c "echo info >> .git/PWNED; cat"'
git config filter.proc.process 'sh -c "echo proc >> .git/PWNED; cat"'
printf 'b.txt filter=repo\n' > .gitattributes
mkdir -p .git/info
printf 'c.txt filter=info\nd.txt filter=proc\n' > .git/info/attributes
"""
# N1: a driver name containing "=" (defeated `-c filter.<name>.clean=` overrides).
PLANT_EQUALS_FILTER = r"""
git config 'filter.a=b.clean' 'sh -c "echo equals >> .git/PWNED; cat"'
mkdir -p .git/info
printf '* filter=a=b\n' > .git/info/attributes
"""
# N2: a gc hook, run by `git commit` -> `git maintenance run --auto` -> gc.
PLANT_GC_HOOK = r"""
git config gc.auto 1
git config gc.recentObjectsHook 'sh -c "echo gc >> .git/PWNED"'
"""
PLANT_INCLUDES = r"""
printf '[filter "inc"]\n\tclean = "sh -c \\"echo inc >> .git/PWNED; cat\\""\n' > evil.cfg
git config include.path ../evil.cfg
git config 'includeIf.gitdir:/.path' ../evil.cfg
"""
# $HOME/.gitconfig (HOME is /workspace in the sandbox) + .gitattributes.
PLANT_HOME_FILTER = r"""
git config -f .gitconfig filter.home.clean 'sh -c "echo home >> .git/PWNED; cat"'
printf 'a.txt filter=home\n' > .gitattributes
"""
# N5: git's XDG global config under the workspace.
PLANT_XDG_FILTER = r"""
mkdir -p .config/git
git config -f .config/git/config filter.xdg.clean 'sh -c "echo xdg >> .git/PWNED; cat"'
printf 'a.txt filter=xdg\n' > .config/git/attributes
"""
# Runs every applicable clean filter deterministically (no index state involved).
HASH_ALL = "for f in a b c d; do git hash-object --path=$f.txt $f.txt >/dev/null 2>&1; done; true"
PWNED = "cat .git/PWNED 2>/dev/null; rm -f .git/PWNED; true"


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

    async def test_cached_result_with_a_different_command_is_rejected(self) -> None:
        # M2: an execution_id reused for a different command must not get another
        # command's cached result.
        self.workspace()
        await _collect(ex._execute_events(self.request("echo one", "job:reused")))
        with self.assertRaises(HTTPException) as caught:
            ex._execute_events(self.request("echo two", "job:reused"))
        self.assertEqual(caught.exception.status_code, 409)
        same = await _collect(ex._execute_events(self.request("echo one", "job:reused")))
        self.assertEqual(same[-1]["payload"]["stdout"], "one\n")
        self.assertNotIn("fingerprint", same[-1]["payload"])

    async def test_queued_calls_in_one_workspace_do_not_starve_other_workspaces(self) -> None:
        # M4: two global slots. Session A runs a long command and queues three more;
        # session B's command must run right away, not wait for A.
        with patch.object(ex, "_concurrency", asyncio.Semaphore(2)):
            self.workspace("ws-a")
            self.workspace("ws-b")
            long_a = ex._execute_events(self.request("echo hi; sleep 2", "a:long", "ws-a"))
            await _collect(long_a, until_text="hi")
            queued = [
                asyncio.ensure_future(_collect(ex._execute_events(self.request("true", f"a:q{i}", "ws-a"))))
                for i in range(3)
            ]
            await asyncio.sleep(0.1)
            started = time.monotonic()
            events = await asyncio.wait_for(
                _collect(ex._execute_events(self.request("echo b", "b:1", "ws-b"))), 5
            )
            self.assertEqual(events[-1]["payload"]["stdout"], "b\n")
            self.assertLess(time.monotonic() - started, 1.0)
            await _collect(long_a)
            await asyncio.gather(*queued)

    async def test_every_command_has_a_git_identity(self) -> None:
        # Scripted-task finding: `git commit` from run_command exited 128 (no identity).
        ws_dir = self.workspace()
        events = await _collect(ex._execute_events(self.request(
            "git init -q -b main && echo x > a.txt && git add a.txt && git commit -q -m first "
            "&& git log -1 --format='%an <%ae> / %cn <%ce>'"
        )))
        result = events[-1]["payload"]
        self.assertEqual(result["exit_code"], 0, result)
        self.assertEqual(
            result["stdout"].strip(),
            "Coding Agent <agent@coding-agent.invalid> / Coding Agent <agent@coding-agent.invalid>",
        )

    async def test_state_gc_removes_old_files_but_keeps_live_and_recent_ones(self) -> None:
        # Minor 8.
        self.workspace()
        await _collect(ex._execute_events(self.request("echo old", "job:old")))
        await _collect(ex._execute_events(self.request("echo new", "job:new")))
        live = ex._execute_events(self.request("echo hi; sleep 1", "job:live"))
        await _collect(live, until_text="hi")
        key = ex._workspace_key("ws-1")
        old_state = ex._state_path(key, "job:old")
        old_files = [old_state, ex._events_path(old_state), ex._started_marker(old_state)]
        live_marker = ex._started_marker(ex._state_path(key, "job:live"))
        ancient = time.time() - 3 * 24 * 3600
        for path in [*old_files, live_marker]:
            os.utime(path, (ancient, ancient))

        removed = ex._sweep_state()
        self.assertEqual(removed, 3)
        self.assertFalse(any(path.exists() for path in old_files))
        self.assertTrue(live_marker.exists())
        self.assertTrue(ex._state_path(key, "job:new").exists())
        await _collect(live)

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
        self.assertEqual(
            result.log_ref,
            ws._execution_id("run_command", "wf-1", environment.info.workflow_run_id, "toolu_1"),
        )
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

    async def _start_then_cancel(self, cancel: Any, tool_id: str = "toolu_1") -> None:
        environment = self.env()
        task = asyncio.create_task(
            environment.run(ws.run_command_activity, self.input("echo hi; sleep 30", tool_id))
        )
        while "hi" not in self.publisher.text:
            await asyncio.sleep(0.02)
        cancel(environment, task)
        with contextlib.suppress(asyncio.CancelledError):
            await task

    async def _wait_jobs_empty(self, seconds: float) -> bool:
        deadline = time.monotonic() + seconds
        while ex._jobs and time.monotonic() < deadline:
            await asyncio.sleep(0.05)
        return not ex._jobs

    async def test_workflow_requested_cancel_cancels_the_job(self) -> None:
        await self._start_then_cancel(lambda environment, task: environment.cancel())
        self.assertTrue(await self._wait_jobs_empty(10))

    async def test_other_cancellations_leave_the_job_for_reattach(self) -> None:
        # Minor 5: worker shutdown, a heartbeat-timed-out zombie attempt, or a bare
        # asyncio cancel must not kill the job a later attempt reattaches to.
        cases = {
            "timed_out": lambda env, task: env.cancel(ActivityCancellationDetails(timed_out=True)),
            "worker_shutdown": lambda env, task: env.cancel(
                ActivityCancellationDetails(worker_shutdown=True)
            ),
            "bare": lambda env, task: task.cancel(),
        }
        for name, cancel in cases.items():
            with self.subTest(name):
                self.publisher.events.clear()
                await self._start_then_cancel(cancel, tool_id=f"toolu_{name}")
                await asyncio.sleep(0.3)
                self.assertEqual(len(ex._jobs), 1, name)
                job = next(iter(ex._jobs.values()))
                await ex.execute_cancel(
                    ex.ExecuteCancelRequest(workspace_id="ws-1", execution_id=job.execution_id),
                    authorization=f"Bearer {TOKEN}",
                )
                self.assertTrue(await self._wait_jobs_empty(10))

    async def test_execution_ids_differ_across_runs_and_survive_retries(self) -> None:
        # M2: activity ids restart at 1 after continue-as-new; the run id separates them.
        async def ids() -> str:
            return ws._activity_execution_id("git_status")

        first = self.env()
        first.info = dataclasses.replace(first.info, workflow_run_id="run-1", activity_id="1")
        retry = self.env(attempt=3)
        retry.info = dataclasses.replace(retry.info, workflow_run_id="run-1", activity_id="1")
        after_can = self.env()
        after_can.info = dataclasses.replace(after_can.info, workflow_run_id="run-2", activity_id="1")
        self.assertEqual(await first.run(ids), await retry.run(ids))
        self.assertNotEqual(await first.run(ids), await after_can.run(ids))

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



class _GitSafetyScenarios:
    """B1 regression scenarios, run in-process (bwrap stubbed) and live (Docker).

    Subclasses provide ``sh`` (a run_command-style /execute in the scenario's workspace,
    returning the executor result) and ``tool`` (call a git tool's activity body).
    """

    wid: str

    async def sh(self, command: str) -> dict[str, Any]:
        raise NotImplementedError

    async def tool(self, tool: Any, *args: Any) -> Any:
        raise NotImplementedError

    async def setup_repo(self, *scripts: str) -> None:
        result = await self.sh("\n".join((GIT_BASE_REPO, *scripts)))
        self.assertEqual(result["exit_code"], 0, result)  # type: ignore[attr-defined]

    async def pwned(self) -> list[str]:
        return (await self.sh(PWNED))["stdout"].split()

    async def assert_git_tools_refuse(self, *keys: str) -> None:
        calls = ((ws.git_status, (self.wid,)), (ws.git_diff, (self.wid, None, False)),
                 (ws.git_commit, ("Update", self.wid, True)))
        for tool, args in calls:
            with self.assertRaises(ApplicationError) as caught:  # type: ignore[attr-defined]
                await self.tool(tool, *args)
            error = caught.exception
            self.assertEqual(error.type, "GitConfigRefused", error)  # type: ignore[attr-defined]
            self.assertTrue(error.non_retryable)  # type: ignore[attr-defined]
            for key in keys:
                self.assertIn(key, str(error))  # type: ignore[attr-defined]
        self.assertEqual(await self.pwned(), [])  # type: ignore[attr-defined]

    async def assert_git_tools_work(self) -> None:
        status = await self.tool(ws.git_status, self.wid)
        self.assertIsNone(status.error, status)  # type: ignore[attr-defined]
        self.assertFalse(status.clean)  # type: ignore[attr-defined]
        diff = await self.tool(ws.git_diff, self.wid, None, False)
        self.assertIsNone(diff.error, diff)  # type: ignore[attr-defined]
        commit = await self.tool(ws.git_commit, "Update", self.wid, True)
        self.assertTrue(commit.committed, commit)  # type: ignore[attr-defined]

    async def test_repo_config_filters_are_refused(self) -> None:
        await self.setup_repo(PLANT_REPO_FILTERS)
        await self.sh(HASH_ALL)
        self.assertEqual(sorted(await self.pwned()), ["info", "proc", "repo"])  # live vectors
        await self.assert_git_tools_refuse(
            "filter.repo.clean", "filter.info.clean", "filter.proc.process",
            ".git/info/attributes is not empty",
        )

    async def test_filter_name_containing_equals_is_refused(self) -> None:
        # N1.
        await self.setup_repo(PLANT_EQUALS_FILTER)
        await self.sh(HASH_ALL)
        self.assertIn("equals", await self.pwned())
        await self.assert_git_tools_refuse("filter.a=b.clean", ".git/info/attributes")

    async def test_gc_hook_is_refused_on_commit(self) -> None:
        # N2.
        await self.setup_repo(PLANT_GC_HOOK)
        await self.assert_git_tools_refuse("gc.auto", "gc.recentobjectshook")

    async def test_include_and_include_if_are_refused(self) -> None:
        await self.setup_repo(PLANT_INCLUDES)
        await self.assert_git_tools_refuse("include.path", "includeif.gitdir:/.path")

    async def test_home_gitconfig_filter_is_ignored(self) -> None:
        await self.setup_repo(PLANT_HOME_FILTER)
        await self.sh(HASH_ALL)
        self.assertEqual(await self.pwned(), ["home"])  # plain git (HOME=/workspace) runs it
        await self.assert_git_tools_work()
        self.assertEqual(await self.pwned(), [])  # type: ignore[attr-defined]

    async def test_xdg_config_under_the_workspace_is_ignored(self) -> None:
        # N5: neither run_command's plain git nor the git tools read
        # /workspace/.config/git, because XDG_CONFIG_HOME is not under the workspace.
        await self.setup_repo(PLANT_XDG_FILTER)
        await self.sh("export XDG_CONFIG_HOME=$PWD/.config; " + HASH_ALL)
        self.assertEqual(await self.pwned(), ["xdg"])  # the vector itself is live
        await self.sh(HASH_ALL)
        self.assertEqual(await self.pwned(), [])  # type: ignore[attr-defined]
        await self.assert_git_tools_work()
        self.assertEqual(await self.pwned(), [])  # type: ignore[attr-defined]

    async def test_fresh_init_repo_works(self) -> None:
        await self.setup_repo("git config user.name Someone")
        await self.assert_git_tools_work()

    async def test_cloned_repo_works(self) -> None:
        upstream = await self.sh(
            "set -e; up=$(mktemp -d); git -C $up init -q -b main; "
            "echo 1 > $up/a.txt; git -C $up add -A; git -C $up commit -q -m up; "
            "git clone -q $up . ; echo 2 > a.txt; git config --local --list --name-only"
        )
        self.assertEqual(upstream["exit_code"], 0, upstream)  # type: ignore[attr-defined]
        self.assertIn("remote.origin.url", upstream["stdout"])  # type: ignore[attr-defined]
        self.assertIn("branch.main.merge", upstream["stdout"])  # type: ignore[attr-defined]
        await self.assert_git_tools_work()


@unittest.skipUnless(__import__("shutil").which("git"), "git not installed")
class GitSafetyInProcessTests(_GitSafetyScenarios, _ToolServerEnv, unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.start_server(self)
        self.wid = "ws-1"
        self.workspace()

    async def sh(self, command: str) -> dict[str, Any]:
        result, _ = await ws._stream_execute(
            {"workspace_id": self.wid, "execution_id": f"sh:{uuid.uuid4().hex}", "command": command,
             "cwd": "/workspace", "timeout_seconds": 60, "network": False}
        )
        return result

    async def tool(self, tool: Any, *args: Any) -> Any:
        environment = self.env(activity_id=uuid.uuid4().hex)
        return await environment.run(_activity_body(tool), *args, _tool_ctx("t"))


class ExecutionLogTests(_ToolServerEnv, unittest.IsolatedAsyncioTestCase):
    """POST /execute/log and read_execution_log (P2 step 2.5, PRD S7)."""

    async def asyncSetUp(self) -> None:
        self.start_server(self)
        self.workspace("ws-1")
        self.workspace("ws-2")

    async def run_job(self, command: str, execution_id: str, workspace: str = "ws-1") -> dict[str, Any]:
        result, _ = await ws._stream_execute(
            {"workspace_id": workspace, "execution_id": execution_id, "command": command,
             "cwd": "/workspace", "timeout_seconds": 60, "network": False}
        )
        return result

    async def test_finished_job_streams_and_interleaving(self) -> None:
        await self.run_job("echo out1; sleep 0.05; echo err1 >&2; sleep 0.05; echo out2; exit 3", "log:fin")
        both = await ws.read_execution_log("ws-1", "log:fin")
        self.assertEqual(both["text"], "out1\nerr1\nout2\n")
        self.assertTrue(both["interleaved"])
        self.assertTrue(both["finished"])
        self.assertEqual(both["exit_code"], 3)
        self.assertIsNone(both["next_offset"])
        self.assertFalse(both["truncated_at_capture"])
        out = await ws.read_execution_log("ws-1", "log:fin", stream="stdout")
        err = await ws.read_execution_log("ws-1", "log:fin", stream="stderr")
        self.assertEqual((out["text"], err["text"]), ("out1\nout2\n", "err1\n"))

    async def test_both_falls_back_to_sections_when_the_replay_buffer_dropped_events(self) -> None:
        with patch.object(ex, "JOB_REPLAY_BUFFER_EVENTS", 2):
            await self.run_job(
                "for i in 1 2 3 4 5; do echo o$i; sleep 0.02; done; echo e >&2", "log:dropped"
            )
        both = await ws.read_execution_log("ws-1", "log:dropped")
        self.assertFalse(both["interleaved"])
        self.assertEqual(both["text"], "o1\no2\no3\no4\no5\n\n[stderr]\ne\n")

    async def test_running_job_returns_output_so_far_then_the_rest(self) -> None:
        job = asyncio.ensure_future(self.run_job("echo first; sleep 1.5; echo second", "log:run"))
        deadline = time.monotonic() + 5
        page: dict[str, Any] = {}
        while time.monotonic() < deadline:
            with contextlib.suppress(ws.ExecutionLogNotFound):  # until the job registers
                page = await ws.read_execution_log("ws-1", "log:run")
                if page["text"]:
                    break
            await asyncio.sleep(0.05)
        self.assertEqual(page["text"], "first\n")
        self.assertFalse(page["finished"])
        self.assertIsNone(page["exit_code"])
        self.assertEqual(page["next_offset"], 6)
        await job
        rest = await ws.read_execution_log("ws-1", "log:run", offset=page["next_offset"])
        self.assertEqual(rest["text"], "second\n")
        self.assertTrue(rest["finished"])
        self.assertEqual(rest["exit_code"], 0)
        self.assertIsNone(rest["next_offset"])

    async def test_ids_are_scoped_to_the_workspace(self) -> None:
        await self.run_job("echo secret", "log:scoped", workspace="ws-1")
        self.assertEqual((await ws.read_execution_log("ws-1", "log:scoped"))["text"], "secret\n")
        for workspace_id, execution_id in (("ws-2", "log:scoped"), ("ws-1", "log:unknown"),
                                           ("ws-1", "../../etc/passwd")):
            with self.subTest(workspace_id=workspace_id, execution_id=execution_id):
                with self.assertRaises(ws.ExecutionLogNotFound):
                    await ws.read_execution_log(workspace_id, execution_id)
        # A running job is scoped the same way.
        job = asyncio.ensure_future(self.run_job("echo hi; sleep 1", "log:live-scoped"))
        await asyncio.sleep(0.3)
        with self.assertRaises(ws.ExecutionLogNotFound):
            await ws.read_execution_log("ws-2", "log:live-scoped")
        await job

    async def test_multibyte_output_split_across_reads_is_not_mangled(self) -> None:
        # 4096-byte reads split 3-byte characters; the capture and stream must stay intact.
        result = await self.run_job("python3 -c \"import sys; sys.stdout.write('€' * 5000)\"", "log:utf8")
        self.assertEqual(result["stdout"], "€" * 5000)
        log = await ws.read_execution_log("ws-1", "log:utf8")
        self.assertEqual(log["text"], "€" * 5000)
        self.assertTrue(log["interleaved"])

    async def test_paging_by_chars_and_bytes(self) -> None:
        await self.run_job("python3 -c \"print('x' * 999)\"", "log:page")
        pages, offset = [], 0
        while offset is not None:
            page = await ws.read_execution_log("ws-1", "log:page", stream="stdout", offset=offset, limit=300)
            self.assertLessEqual(len(page["text"]), 300)
            pages.append(page["text"])
            offset = page["next_offset"]
        self.assertEqual("".join(pages), "x" * 999 + "\n")
        self.assertEqual(len(pages), 4)
        past_end = await ws.read_execution_log("ws-1", "log:page", stream="stdout", offset=5000)
        self.assertEqual((past_end["text"], past_end["next_offset"]), ("", None))

        # 3-byte characters: a full-size page is cut at 256 KB of UTF-8, not 256K chars.
        await self.run_job("python3 -c \"print('€' * 99000)\"", "log:bytes")
        first = await ws.read_execution_log("ws-1", "log:bytes", stream="stdout")
        self.assertLessEqual(len(first["text"].encode()), ex.MAX_LOG_RESPONSE_BYTES)
        self.assertEqual(first["next_offset"], len(first["text"]))
        second = await ws.read_execution_log("ws-1", "log:bytes", stream="stdout", offset=first["next_offset"])
        self.assertEqual(first["text"] + second["text"], "€" * 99000 + "\n")
        self.assertIsNone(second["next_offset"])

    async def test_auth_is_required(self) -> None:
        import httpx

        await self.run_job("echo hi", "log:auth")
        url = os.environ["WORKSPACE_EXECUTOR_URL"] + "/execute/log"
        body = {"workspace_id": "ws-1", "execution_id": "log:auth"}
        async with httpx.AsyncClient() as client:
            for headers in ({}, {"Authorization": "Bearer wrong"}, {"Authorization": TOKEN}):
                with self.subTest(headers=headers):
                    response = await client.post(url, json=body, headers=headers)
                    self.assertEqual(response.status_code, 401)
                    self.assertNotIn("hi", response.text)
        # (The in-process server shares os.environ, so swap the client's header instead.)
        with patch.object(ws, "_executor_headers", lambda: {"Authorization": "Bearer wrong"}):
            with self.assertRaises(ws.ExecutorRequestError):
                await ws.read_execution_log("ws-1", "log:auth")

    async def test_unreachable_executor(self) -> None:
        with patch.dict("os.environ", {"WORKSPACE_EXECUTOR_URL": "http://127.0.0.1:9"}):
            with self.assertRaises(ws.ExecutorRequestError):
                await ws.read_execution_log("ws-1", "log:x")


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

    async def test_git_control_files_are_protected(self) -> None:
        # B1: /files must not let a model plant git config, hooks or attributes.
        (self.ws_dir / ".git").mkdir()
        (self.ws_dir / ".git" / "HEAD").write_text("ref: refs/heads/main\n")
        (self.ws_dir / "ok.txt").write_text("x")
        refused = [
            (ws.write_file, (".git/config", "[core]\n", "ws-1")),
            (ws.write_file, (".GIT/hooks/pre-commit", "#!/bin/sh\n", "ws-1")),
            (ws.write_file, (".git/info/attributes", "* filter=x\n", "ws-1")),
            (ws.write_file, (".gitconfig", "[filter \"x\"]\n", "ws-1")),
            (ws.edit_file, (".git/HEAD", "main", "evil", "ws-1")),
            (ws.write_file, (".config/git/config", "[filter \"x\"]\n", "ws-1")),
            (ws.write_file, (".Config/Git/attributes", "* filter=x\n", "ws-1")),
        ]
        for tool, args in refused:
            with self.subTest(args[0]):
                with self.assertRaises(ApplicationError) as caught:
                    await self.call(tool, *args)
                self.assertTrue(caught.exception.non_retryable)
                self.assertEqual(caught.exception.type, "Workspace_protected_path")
        self.assertFalse((self.ws_dir / ".git" / "config").exists())
        self.assertFalse((self.ws_dir / ".gitconfig").exists())
        # Raw endpoints: mkdir, delete, rename in and out of .git.
        import httpx

        url = os.environ["WORKSPACE_EXECUTOR_URL"]
        headers = {"Authorization": f"Bearer {TOKEN}"}
        async with httpx.AsyncClient() as client:
            for op, body in [
                ("mkdir", {"path": ".git/hooks"}),
                ("delete", {"path": ".git/HEAD"}),
                ("rename", {"source": "ok.txt", "destination": ".git/hooks/post-checkout"}),
                ("rename", {"source": ".git/HEAD", "destination": "HEAD"}),
                ("rename", {"source": "ok.txt", "destination": ".gitconfig"}),
            ]:
                response = await client.post(f"{url}/files/{op}", json={"workspace_id": "ws-1", **body}, headers=headers)
                self.assertEqual(response.status_code, 403, (op, body, response.text))
                self.assertEqual(response.json()["code"], "protected_path")
        # Look-alikes are fine.
        await self.call(ws.write_file, ".github/workflows/ci.yml", "on: push\n", "ws-1")
        await self.call(ws.write_file, "src/.gitconfig", "x\n", "ws-1")
        await self.call(ws.write_file, ".gitignore", "*.pyc\n", "ws-1")
        await self.call(ws.write_file, ".config/app.toml", "x = 1\n", "ws-1")

    async def test_write_behind_a_running_command_fails_fast_and_does_not_land(self) -> None:
        # Minor 7: the outcome is truthful: a definite, non-retryable "busy" and no write.
        running = asyncio.ensure_future(ws._stream_execute(
            {"workspace_id": "ws-1", "execution_id": "busy:1", "command": "sleep 1.5",
             "cwd": "/workspace", "timeout_seconds": 60, "network": False}
        ))
        await asyncio.sleep(0.3)
        with patch.object(ws, "MUTATION_BUSY_TIMEOUT_SECONDS", 0.3):
            with self.assertRaises(ApplicationError) as caught:
                await self.call(ws.write_file, "late.txt", "x", "ws-1")
        self.assertEqual(caught.exception.type, "Workspace_workspace_busy")
        self.assertTrue(caught.exception.non_retryable)
        await running
        await asyncio.sleep(0.2)
        self.assertFalse((self.ws_dir / "late.txt").exists())
        # Once the command is done, the same write goes through.
        result = await self.call(ws.write_file, "late.txt", "x", "ws-1")
        self.assertTrue(result.created)

    async def test_zero_busy_timeout_writes_when_the_workspace_is_free(self) -> None:
        # N6: busy_timeout=0 means "only if free right now", not "always busy".
        with patch.object(ws, "MUTATION_BUSY_TIMEOUT_SECONDS", 0):
            result = await self.call(ws.write_file, "now.txt", "x", "ws-1")
            self.assertTrue(result.created)
            edited = await self.call(ws.edit_file, "now.txt", "x", "y", "ws-1")
            self.assertEqual(edited.added_lines, 1)
            running = asyncio.ensure_future(ws._stream_execute(
                {"workspace_id": "ws-1", "execution_id": "busy:0", "command": "sleep 1",
                 "cwd": "/workspace", "timeout_seconds": 60, "network": False}
            ))
            await asyncio.sleep(0.3)
            with self.assertRaises(ApplicationError) as caught:
                await self.call(ws.write_file, "later.txt", "x", "ws-1")
            self.assertEqual(caught.exception.type, "Workspace_workspace_busy")
            self.assertIn("A command is running", str(caught.exception))
            await running

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


_LIVE = unittest.skipUnless(
    os.environ.get("WORKSPACE_EXECUTOR_LIVE_URL"),
    "set WORKSPACE_EXECUTOR_LIVE_URL (and WORKSPACE_EXECUTOR_TOKEN) to run against a live executor",
)


@_LIVE
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

    async def _tool(self, tool: Any, *args: Any) -> Any:
        environment = ActivityEnvironment()
        environment.info = dataclasses.replace(
            environment.info, workflow_id=self.workspace_id, activity_id=uuid.uuid4().hex
        )
        return await environment.run(agent.tool_activity(tool), *args, _tool_ctx("live"))

    async def test_execution_log_for_a_run_command_result(self) -> None:
        environment = ActivityEnvironment()
        environment.info = dataclasses.replace(environment.info, workflow_id=self.workspace_id)
        task = asyncio.ensure_future(environment.run(
            ws.run_command_activity,
            ws.RunCommandInput(workspace_id=self.workspace_id, tool_ctx=_tool_ctx("live-log"),
                               command="echo first; echo oops >&2; sleep 1.5; echo second"),
        ))
        while "first" not in self.publisher.text:
            await asyncio.sleep(0.05)
        log_ref = ws._execution_id("run_command", self.workspace_id, environment.info.workflow_run_id, "live-log")
        running = await ws.read_execution_log(self.workspace_id, log_ref)
        self.assertFalse(running["finished"])
        self.assertIn("first\n", running["text"])
        result = await task
        self.assertEqual(result.log_ref, log_ref)
        done = await ws.read_execution_log(self.workspace_id, result.log_ref)
        self.assertTrue(done["finished"])
        self.assertEqual(done["text"], "first\noops\nsecond\n")
        self.assertEqual(done["exit_code"], 0)
        with self.assertRaises(ws.ExecutionLogNotFound):
            await ws.read_execution_log(f"other-{self.workspace_id}", result.log_ref)

    async def test_plain_git_commit_from_a_command_has_an_identity(self) -> None:
        result, _ = await ws._stream_execute(self.body(
            "git init -q -b main && echo x > a.txt && git add a.txt && git commit -q -m first "
            "&& git log -1 --format='%an <%ae>'",
            f"live:{uuid.uuid4().hex[:12]}",
        ))
        self.assertEqual(result["exit_code"], 0, result)
        self.assertEqual(result["stdout"].strip(), "Coding Agent <agent@coding-agent.invalid>")

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


@_LIVE
class LiveGitSafetyTests(_GitSafetyScenarios, LiveExecutorTests):
    """The B1 scenarios in real Bubblewrap (git 2.47 in the image)."""

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.wid = self.workspace_id

    async def sh(self, command: str) -> dict[str, Any]:
        result, _ = await ws._stream_execute(self.body(command, f"live:{uuid.uuid4().hex[:12]}"))
        return result

    async def tool(self, tool: Any, *args: Any) -> Any:
        return await self._tool(tool, *args)


if __name__ == "__main__":
    unittest.main()
