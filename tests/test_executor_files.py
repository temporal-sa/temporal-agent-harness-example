from __future__ import annotations

import asyncio
import base64
import os
import socket
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx
from fastapi.testclient import TestClient

from simple_chat_agent.worker.sandbox import executor as sandbox_executor
from simple_chat_agent.worker.sandbox.executor import (
    MAX_FILE_READ_BYTES,
    MAX_FILE_WRITE_BYTES,
    MAX_LIST_ENTRIES,
    _credential_boundary_self_test,
    _workspace_dir,
    _workspace_key,
)

TOKEN = "test-executor-token"
AUTH = {"Authorization": f"Bearer {TOKEN}"}
WORKSPACE = "workflow-files-1"
SIBLING = "workflow-files-2"


class _ExecutorFilesTestBase(unittest.TestCase):
    def setUp(self) -> None:
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        env = patch.dict(
            "os.environ",
            {
                "WORKSPACE_EXECUTOR_ROOT": temp_dir.name,
                "WORKSPACE_EXECUTOR_TOKEN": TOKEN,
            },
        )
        env.start()
        self.addCleanup(env.stop)
        # Locks are bound to an event loop; give each test fresh ones.
        for name, value in (
            ("_workspace_locks", {}),
            ("_workspace_locks_guard", asyncio.Lock()),
        ):
            patcher = patch.object(sandbox_executor, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)

        self.workspace = _workspace_dir(_workspace_key(WORKSPACE))
        self.workspace.mkdir(parents=True)
        self.sibling = _workspace_dir(_workspace_key(SIBLING))
        self.sibling.mkdir(parents=True)
        (self.sibling / "secret.txt").write_text("sibling secret", encoding="utf-8")

        self.client = TestClient(sandbox_executor.app)
        self.client.__enter__()
        self.addCleanup(self.client.__exit__, None, None, None)

    def call(self, operation: str, **body: object) -> httpx.Response:
        body.setdefault("workspace_id", WORKSPACE)
        return self.client.post(f"/files/{operation}", json=body, headers=AUTH)

    def assertCode(self, response: httpx.Response, code: str) -> dict:
        payload = response.json()
        self.assertFalse(payload.get("ok", True), payload)
        self.assertEqual(payload["code"], code, payload)
        return payload


class ExecutorFilesAuthTests(_ExecutorFilesTestBase):
    def test_every_endpoint_requires_the_bearer_token(self) -> None:
        bodies = {
            "stat": {"path": "a"},
            "read": {"path": "a"},
            "list": {"path": "."},
            "write": {"path": "a", "content": "x"},
            "edit": {"path": "a", "old_string": "x", "new_string": "y"},
            "mkdir": {"path": "a"},
            "delete": {"path": "a"},
            "rename": {"source": "a", "destination": "b"},
        }
        for operation, body in bodies.items():
            body = {"workspace_id": WORKSPACE, **body}
            for headers in (
                {},
                {"Authorization": "Bearer wrong"},
                {"Authorization": TOKEN},
            ):
                with self.subTest(operation=operation, headers=headers):
                    response = self.client.post(
                        f"/files/{operation}", json=body, headers=headers
                    )
                    self.assertEqual(response.status_code, 401)
        self.assertFalse((self.workspace / "a").exists())

    def test_unconfigured_token_refuses_requests(self) -> None:
        with patch.dict("os.environ", {"WORKSPACE_EXECUTOR_TOKEN": ""}):
            response = self.call("list", path=".")
        self.assertEqual(response.status_code, 503)


class ExecutorFilesConfinementTests(_ExecutorFilesTestBase):
    def test_dotdot_traversal_is_refused(self) -> None:
        for path in (
            "..",
            "../",
            f"../{_workspace_key(SIBLING)}/secret.txt",
            "../../../../etc/passwd",
            "src/../../etc/passwd",
            "a/b/../../..",
        ):
            for operation in ("read", "stat"):
                with self.subTest(path=path, operation=operation):
                    response = self.call(operation, path=path)
                    self.assertIn(response.status_code, (400,))
                    self.assertIn(
                        response.json()["code"], ("outside_workspace", "invalid_path")
                    )
        response = self.call("write", path="../escape.txt", content="x")
        self.assertCode(response, "outside_workspace")
        self.assertFalse((self.workspace.parent / "escape.txt").exists())

    def test_absolute_paths_are_refused(self) -> None:
        for path in (
            "/etc/passwd",
            "/workspace/a.txt",
            str(self.sibling / "secret.txt"),
        ):
            with self.subTest(path=path):
                self.assertCode(self.call("read", path=path), "outside_workspace")
        self.assertCode(
            self.call("write", path="/tmp/escape.txt", content="x"), "outside_workspace"
        )
        self.assertCode(
            self.call("rename", source="a", destination="/tmp/x"), "outside_workspace"
        )

    def test_nul_bytes_and_empty_components_are_refused(self) -> None:
        for path in ("a\x00b", "a.txt\x00../../etc/passwd", "a//b", "a/", ""):
            with self.subTest(path=path):
                self.assertCode(self.call("read", path=path), "invalid_path")
        self.assertCode(self.call("write", path="x\x00y", content="z"), "invalid_path")

    def test_encoded_traversal_is_treated_as_a_literal_name(self) -> None:
        for path in (
            "%2e%2e/%2e%2e/etc/passwd",
            "..%2f..%2fetc%2fpasswd",
            "%2e%2e%2f%2e%2e%2fetc%2fpasswd",
            "..\\..\\etc\\passwd",
            "．．/．．/etc/passwd",
        ):
            with self.subTest(path=path):
                self.assertCode(self.call("read", path=path), "not_found")
        # Writing one creates a confined file with that literal name.
        response = self.call("write", path="..%2fescape", content="x")
        self.assertEqual(response.status_code, 200, response.json())
        self.assertTrue((self.workspace / "..%2fescape").is_file())
        self.assertFalse((self.workspace.parent / "escape").exists())

    def test_leaf_symlink_to_etc_passwd_is_refused(self) -> None:
        os.symlink("/etc/passwd", self.workspace / "passwd")
        for operation in ("read", "stat", "delete"):
            with self.subTest(operation=operation):
                self.assertCode(self.call(operation, path="passwd"), "is_symlink")
        self.assertCode(
            self.call("write", path="passwd", content="pwned"), "is_symlink"
        )
        self.assertCode(
            self.call("edit", path="passwd", old_string="root", new_string="x"),
            "is_symlink",
        )
        self.assertCode(
            self.call("rename", source="passwd", destination="p2"), "is_symlink"
        )
        (self.workspace / "other").write_text("x")
        self.assertCode(
            self.call("rename", source="other", destination="passwd", overwrite=True),
            "is_symlink",
        )
        self.assertTrue((self.workspace / "passwd").is_symlink())

    def test_mid_path_symlink_to_sibling_workspace_is_refused(self) -> None:
        os.symlink(self.sibling, self.workspace / "evil")
        os.symlink(f"../{_workspace_key(SIBLING)}", self.workspace / "evil_rel")
        for link in ("evil", "evil_rel"):
            with self.subTest(link=link):
                self.assertCode(
                    self.call("read", path=f"{link}/secret.txt"), "is_symlink"
                )
                self.assertCode(self.call("list", path=link), "is_symlink")
                self.assertCode(
                    self.call("write", path=f"{link}/planted.txt", content="x"),
                    "is_symlink",
                )
                self.assertCode(
                    self.call("delete", path=f"{link}/secret.txt"), "is_symlink"
                )
                self.assertCode(
                    self.call(
                        "write",
                        path=f"{link}/deep/planted.txt",
                        content="x",
                        create_parents=True,
                    ),
                    "is_symlink",
                )
        self.assertEqual(sorted(p.name for p in self.sibling.iterdir()), ["secret.txt"])
        self.assertEqual(
            (self.sibling / "secret.txt").read_text(encoding="utf-8"), "sibling secret"
        )

    def test_symlinked_workspace_root_is_refused(self) -> None:
        linked_id = "workflow-files-linked"
        linked = _workspace_dir(_workspace_key(linked_id))
        os.symlink(self.sibling, linked)
        response = self.call("read", workspace_id=linked_id, path="secret.txt")
        self.assertEqual(response.status_code, 500)
        self.assertNotIn("sibling secret", response.text)

    def test_hardlink_is_refused_for_write_and_edit(self) -> None:
        outside = self.sibling / "secret.txt"
        try:
            os.link(outside, self.workspace / "linked.txt")
        except OSError as err:
            self.skipTest(f"cannot create hard links here: {err}")
        payload = self.assertCode(
            self.call("write", path="linked.txt", content="overwritten"),
            "outside_workspace",
        )
        self.assertIn("2 hard links", payload["message"])
        self.assertCode(
            self.call(
                "edit", path="linked.txt", old_string="sibling", new_string="pwned"
            ),
            "outside_workspace",
        )
        self.assertEqual(outside.read_text(encoding="utf-8"), "sibling secret")

    def test_fifo_is_refused_for_read_and_write_without_blocking(self) -> None:
        if not hasattr(os, "mkfifo"):
            self.skipTest("platform cannot create FIFOs")
        try:
            os.mkfifo(self.workspace / "pipe")
        except OSError as err:
            self.skipTest(f"cannot create FIFOs here: {err}")
        started = time.monotonic()
        self.assertCode(self.call("read", path="pipe"), "not_regular_file")
        self.assertCode(
            self.call("write", path="pipe", content="x"), "not_regular_file"
        )
        self.assertCode(
            self.call("edit", path="pipe", old_string="a", new_string="b"),
            "not_regular_file",
        )
        self.assertLess(time.monotonic() - started, 5)
        stat = self.call("stat", path="pipe").json()
        self.assertEqual(stat["type"], "other")

    def test_unix_socket_is_refused_for_read(self) -> None:
        if not hasattr(socket, "AF_UNIX"):
            self.skipTest("platform has no AF_UNIX sockets")
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.addCleanup(server.close)
        # Bind by relative name: macOS temp paths exceed the sun_path limit.
        previous = os.getcwd()
        os.chdir(self.workspace)
        try:
            server.bind("s")
        finally:
            os.chdir(previous)
        self.assertCode(self.call("read", path="s"), "not_regular_file")

    def test_directory_is_not_readable_as_a_file(self) -> None:
        (self.workspace / "d").mkdir()
        self.assertCode(self.call("read", path="d"), "not_regular_file")
        self.assertCode(self.call("write", path="d", content="x"), "not_regular_file")


class ExecutorFilesOperationTests(_ExecutorFilesTestBase):
    def test_sparse_file_over_the_cap_is_too_large_but_a_range_read_works(
        self,
    ) -> None:
        big = self.workspace / "big.bin"
        size = MAX_FILE_READ_BYTES * 8
        with open(big, "wb") as handle:
            handle.truncate(size)
            handle.seek(size - 5)
            handle.write(b"tail!")

        payload = self.assertCode(self.call("read", path="big.bin"), "too_large")
        self.assertIn("run_command", payload["message"])
        self.assertEqual(payload["size"], size)
        self.assertCode(
            self.call("read", path="big.bin", length=MAX_FILE_READ_BYTES + 1),
            "too_large",
        )

        response = self.call("read", path="big.bin", offset=size - 105, length=100)
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["length"], 100)
        self.assertEqual(body["content"], "\x00" * 100)
        self.assertFalse(body["eof"])

        tail = self.call("read", path="big.bin", offset=size - 5).json()
        self.assertEqual(tail["content"], "tail!")
        self.assertTrue(tail["eof"])

        exact = self.call("read", path="big.bin", length=MAX_FILE_READ_BYTES).json()
        self.assertEqual(exact["length"], MAX_FILE_READ_BYTES)

    def test_binary_content_round_trips_as_base64(self) -> None:
        data = bytes(range(256))
        response = self.call(
            "write",
            path="bin/blob",
            content=base64.b64encode(data).decode(),
            encoding="base64",
            create_parents=True,
        )
        self.assertEqual(response.status_code, 200, response.json())
        body = self.call("read", path="bin/blob").json()
        self.assertEqual(body["encoding"], "base64")
        self.assertEqual(base64.b64decode(body["content"]), data)

    def test_write_cap_and_parent_creation(self) -> None:
        self.assertCode(
            self.call("write", path="a/b/c.txt", content="hello"), "not_found"
        )
        response = self.call(
            "write", path="a/b/c.txt", content="hello", create_parents=True
        )
        self.assertEqual(
            response.json(),
            {
                "ok": True,
                "path": "a/b/c.txt",
                "bytes_written": 5,
                "created": True,
            },
        )
        self.assertEqual((self.workspace / "a/b/c.txt").read_text(), "hello")

        self.assertCode(
            self.call("write", path="a/b/c.txt", content="again", overwrite=False),
            "exists",
        )
        self.assertCode(
            self.call(
                "write", path="huge.txt", content="x" * (MAX_FILE_WRITE_BYTES + 1)
            ),
            "too_large",
        )
        self.assertCode(
            self.call(
                "write",
                path="huge.bin",
                content=base64.b64encode(b"\0" * (MAX_FILE_WRITE_BYTES + 1)).decode(),
                encoding="base64",
            ),
            "too_large",
        )
        self.assertFalse((self.workspace / "huge.txt").exists())
        ok = self.call("write", path="max.txt", content="x" * MAX_FILE_WRITE_BYTES)
        self.assertEqual(ok.status_code, 200)

    def test_write_preserves_mode_and_leaves_no_temp_files(self) -> None:
        script = self.workspace / "run.sh"
        script.write_text("#!/bin/sh\n")
        script.chmod(0o755)
        self.call("write", path="run.sh", content="#!/bin/sh\necho hi\n")
        self.assertEqual(script.stat().st_mode & 0o777, 0o755)
        self.assertEqual(sorted(p.name for p in self.workspace.iterdir()), ["run.sh"])

    def test_edit_requires_exactly_one_match(self) -> None:
        target = self.workspace / "app.py"
        target.write_text("x = 1\ny = 1\n", encoding="utf-8")

        zero = self.assertCode(
            self.call("edit", path="app.py", old_string="z = 1", new_string="z = 2"),
            "match_count",
        )
        self.assertEqual(zero["match_count"], 0)
        self.assertIn("0 times", zero["message"])

        two = self.assertCode(
            self.call("edit", path="app.py", old_string="= 1", new_string="= 2"),
            "match_count",
        )
        self.assertEqual(two["match_count"], 2)
        self.assertIn("2 times", two["message"])
        self.assertEqual(target.read_text(), "x = 1\ny = 1\n")

        one = self.call("edit", path="app.py", old_string="y = 1", new_string="y = 2")
        self.assertEqual(one.status_code, 200, one.json())
        self.assertEqual(target.read_text(), "x = 1\ny = 2\n")

    def test_edit_counts_overlapping_matches(self) -> None:
        (self.workspace / "a.txt").write_text("aaa")
        payload = self.assertCode(
            self.call("edit", path="a.txt", old_string="aa", new_string="b"),
            "match_count",
        )
        self.assertEqual(payload["match_count"], 2)

    def test_list_is_one_level_with_types_and_sizes(self) -> None:
        (self.workspace / "src").mkdir()
        (self.workspace / "src" / "nested.py").write_text("n")
        (self.workspace / "README.md").write_text("hello")
        os.symlink("/etc/passwd", self.workspace / "link")
        body = self.call("list", path=".").json()
        self.assertEqual(
            body["entries"],
            [
                {"name": "README.md", "type": "file", "size": 5},
                {"name": "link", "type": "symlink", "size": None},
                {"name": "src", "type": "directory", "size": None},
            ],
        )
        self.assertFalse(body["truncated"])
        nested = self.call("list", path="src").json()
        self.assertEqual([e["name"] for e in nested["entries"]], ["nested.py"])
        self.assertCode(self.call("list", path="README.md"), "not_a_directory")
        self.assertCode(self.call("list", path="missing"), "not_found")

    def test_list_is_capped(self) -> None:
        many = self.workspace / "many"
        many.mkdir()
        for index in range(MAX_LIST_ENTRIES + 25):
            (many / f"f{index:05d}").touch()
        body = self.call("list", path="many").json()
        self.assertEqual(len(body["entries"]), MAX_LIST_ENTRIES)
        self.assertTrue(body["truncated"])

    def test_stat(self) -> None:
        (self.workspace / "f.txt").write_text("abc")
        body = self.call("stat", path="f.txt").json()
        self.assertEqual(
            (body["type"], body["size"], body["path"]), ("file", 3, "f.txt")
        )
        self.assertEqual(self.call("stat", path=".").json()["type"], "directory")
        self.assertEqual(self.call("stat", path="./f.txt").json()["path"], "f.txt")
        self.assertCode(self.call("stat", path="nope"), "not_found")
        self.assertEqual(self.call("stat", path="nope").status_code, 404)

    def test_mkdir_delete_and_rename(self) -> None:
        self.assertEqual(self.call("mkdir", path="d").json()["created"], True)
        self.assertCode(self.call("mkdir", path="d"), "exists")
        self.assertEqual(
            self.call("mkdir", path="d", parents=True).json()["created"], False
        )
        self.assertCode(self.call("mkdir", path="x/y"), "not_found")
        self.assertTrue(self.call("mkdir", path="x/y", parents=True).json()["ok"])

        (self.workspace / "d" / "f.txt").write_text("f")
        self.assertCode(self.call("delete", path="d"), "not_empty")
        self.assertCode(self.call("delete", path="."), "invalid_path")

        self.assertCode(
            self.call("rename", source="d/f.txt", destination="x/y"), "exists"
        )
        renamed = self.call("rename", source="d/f.txt", destination="g.txt")
        self.assertEqual(renamed.status_code, 200, renamed.json())
        self.assertEqual((self.workspace / "g.txt").read_text(), "f")
        (self.workspace / "h.txt").write_text("h")
        self.assertCode(
            self.call("rename", source="h.txt", destination="g.txt"), "exists"
        )
        self.assertTrue(
            self.call(
                "rename", source="h.txt", destination="g.txt", overwrite=True
            ).json()["ok"]
        )
        self.assertEqual((self.workspace / "g.txt").read_text(), "h")
        self.assertTrue(
            self.call(
                "rename",
                source="g.txt",
                destination="new/dir/g.txt",
                create_parents=True,
            ).json()["ok"]
        )

        self.assertEqual(self.call("delete", path="d").json()["type"], "directory")
        self.assertEqual(
            self.call("delete", path="new/dir/g.txt").json()["type"], "file"
        )
        self.assertCode(self.call("delete", path="new/dir/g.txt"), "not_found")
        self.assertFalse((self.workspace / "d").exists())

    def test_workspaces_are_isolated_by_id(self) -> None:
        self.call("write", path="mine.txt", content="mine")
        other = self.call("read", workspace_id=SIBLING, path="mine.txt")
        self.assertCode(other, "not_found")
        self.assertEqual(
            self.call("read", workspace_id=SIBLING, path="secret.txt").json()[
                "content"
            ],
            "sibling secret",
        )


class ExecutorFilesLockingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        env = patch.dict(
            "os.environ",
            {
                "WORKSPACE_EXECUTOR_ROOT": temp_dir.name,
                "WORKSPACE_EXECUTOR_TOKEN": TOKEN,
            },
        )
        env.start()
        self.addCleanup(env.stop)
        for name, value in (
            ("_workspace_locks", {}),
            ("_workspace_locks_guard", asyncio.Lock()),
        ):
            patcher = patch.object(sandbox_executor, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        workspace = _workspace_dir(_workspace_key(WORKSPACE))
        workspace.mkdir(parents=True)
        (workspace / "f.txt").write_text("hello")
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=sandbox_executor.app),
            base_url="http://executor",
            headers=AUTH,
        )
        self.addAsyncCleanup(self.client.aclose)

    async def test_reads_do_not_wait_for_a_running_command_but_writes_do(
        self,
    ) -> None:
        locks = await sandbox_executor._workspace_locks_for(_workspace_key(WORKSPACE))
        body = {"workspace_id": WORKSPACE}

        # Simulate a long /execute holding the workspace's exec lock.
        await locks.exec.acquire()
        try:
            started = time.monotonic()
            read = await asyncio.wait_for(
                self.client.post("/files/read", json={**body, "path": "f.txt"}),
                timeout=2,
            )
            self.assertEqual(read.json()["content"], "hello")
            self.assertLess(time.monotonic() - started, 0.5)

            write = asyncio.ensure_future(
                self.client.post(
                    "/files/write", json={**body, "path": "f.txt", "content": "new"}
                )
            )
            await asyncio.sleep(0.3)
            self.assertFalse(write.done())

            # A write queued behind the command does not hold up readers.
            read_again = await asyncio.wait_for(
                self.client.post("/files/stat", json={**body, "path": "f.txt"}),
                timeout=2,
            )
            self.assertEqual(read_again.json()["size"], 5)
        finally:
            locks.exec.release()

        response = await asyncio.wait_for(write, timeout=2)
        self.assertEqual(response.json()["bytes_written"], 3)

    async def test_mutations_are_serialized_with_each_other(self) -> None:
        body = {"workspace_id": WORKSPACE}
        responses = await asyncio.gather(
            *(
                self.client.post(
                    "/files/write",
                    json={**body, "path": f"n{index}.txt", "content": str(index)},
                )
                for index in range(20)
            ),
            *(
                self.client.post("/files/read", json={**body, "path": "f.txt"})
                for _ in range(20)
            ),
        )
        self.assertTrue(all(r.status_code == 200 for r in responses))


class ExecutorCredentialBoundaryTests(unittest.TestCase):
    def test_provider_keys_fail_the_self_test(self) -> None:
        for name in (
            "ANTHROPIC_API_KEY",
            "OPENAI_API_KEY",
            "GEMINI_API_KEY",
            "GOOGLE_API_KEY",
            "TYPESAFE_API_KEY",
        ):
            with self.subTest(name=name), patch.dict(
                "os.environ", {name: "secret"}, clear=True
            ):
                self.assertIn(name, _credential_boundary_self_test() or "")

    def test_mcp_oauth_and_github_token_names_fail_the_self_test(self) -> None:
        for name in (
            "SIMPLE_CHAT_MCP_SERVER_TOKEN",
            "GITHUB_OAUTH_CLIENT_SECRET",
            "my_oauth_secret",
            "GITHUB_TOKEN",
            "CI_GITHUB_TOKEN_RO",
        ):
            with self.subTest(name=name), patch.dict(
                "os.environ", {name: "secret"}, clear=True
            ):
                self.assertIn(name, _credential_boundary_self_test() or "")

    def test_clean_environment_passes_the_env_check(self) -> None:
        with (
            patch.dict(
                "os.environ",
                {"WORKSPACE_EXECUTOR_TOKEN": "t", "PATH": "/usr/bin"},
                clear=True,
            ),
            patch.object(Path, "exists", return_value=False),
            patch.object(sandbox_executor, "_metadata_status", return_value=None),
        ):
            self.assertIsNone(_credential_boundary_self_test())


if __name__ == "__main__":
    unittest.main()
