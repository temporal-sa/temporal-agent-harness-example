"""Artifact and attachment tools (P1 step 1.3) and the demo fixture loader (step 1.11)."""

from __future__ import annotations

import base64
import inspect
import json
import os
import re
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch

import httpx
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment

from simple_chat_agent.agent import fixtures
from simple_chat_agent.agent.tools import artifacts
from simple_chat_agent.agent.tools.artifacts import (
    ARTIFACT_TOOLS,
    ExecutorClient,
    create_artifact,
    create_artifact_impl,
    import_attachment_impl,
    import_attachment_to_workspace,
    read_attachment,
    read_attachment_impl,
)
from simple_chat_agent.common.attachments import USER_ATTACHMENT_SOURCE, create_user_attachment
from simple_chat_agent.common.store import AppStore
from temporal_agent_harness.harness.agent_workflow import tool_activity

OWNER = "user-1"
SESSION = "coding-agent-abc"
WORKSPACE = "coding-agent-abc"
PNG = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR" + b"\x00" * 32


class _StoreCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        env = patch.dict(os.environ)
        env.start()
        self.addCleanup(env.stop)
        for key in ("SIMPLE_CHAT_ARTIFACTS_TABLE", "SIMPLE_CHAT_S3_BUCKET", "SIMPLE_CHAT_DYNAMODB_TABLE"):
            os.environ.pop(key, None)
        self.tmp = Path(tmp.name)
        self.store = AppStore(str(self.tmp / "db.sqlite3"), artifact_dir=str(self.tmp / "artifacts"))

    def upload(self, name: str, content: bytes, *, owner: str = OWNER, session: str = SESSION, mime: str | None = None):
        return create_user_attachment(
            self.store,
            user_id=owner,
            conversation_id=session,
            workflow_id=session,
            name=name,
            content=content,
            mime_type=mime,
        )

    def assertToolError(self, err: ApplicationError, type_: str, *, retryable: bool = False) -> None:
        self.assertEqual(err.type, type_)
        self.assertEqual(err.non_retryable, not retryable)


class CreateArtifactTests(_StoreCase):
    async def create(self, **kwargs: Any):
        args: dict[str, Any] = {"store": self.store, "owner_id": OWNER, "session_id": SESSION}
        args.update(kwargs)
        return await create_artifact_impl(**args)

    async def test_text_artifact_is_stored_for_the_owner_and_session(self) -> None:
        result = await self.create(name="notes.md", content="# Hi\n", mime_type="text/markdown")

        self.assertEqual(result.name, "notes.md")
        self.assertEqual(result.mime_type, "text/markdown")
        self.assertEqual(result.size, 5)
        self.assertEqual(result.view_url, f"/api/artifacts/{result.artifact_id}")
        self.assertTrue(result.expires_at)
        record = self.store.get_artifact(user_id=OWNER, artifact_id=result.artifact_id)
        self.assertEqual(record.workflow_id, SESSION)
        self.assertEqual(record.conversation_id, SESSION)
        self.assertEqual(self.store.read_artifact_bytes(record), b"# Hi\n")
        self.assertIsNone(self.store.get_artifact(user_id="someone-else", artifact_id=result.artifact_id))

    async def test_str_carries_what_the_model_needs_without_the_body(self) -> None:
        result = await self.create(name="r.txt", content="x" * 5000)
        text = str(result)
        self.assertLess(len(text), 400)
        for part in ("r.txt", result.artifact_id, result.view_url, result.download_url, "5000 bytes"):
            self.assertIn(part, text)
        self.assertNotIn("xxxxx", text)

    async def test_base64_binary_round_trips(self) -> None:
        result = await self.create(
            name="pixel.png", content=base64.b64encode(PNG).decode(), mime_type="image/png", encoding="base64"
        )
        record = self.store.get_artifact(user_id=OWNER, artifact_id=result.artifact_id)
        self.assertEqual(self.store.read_artifact_bytes(record), PNG)
        self.assertEqual(result.size, len(PNG))

    async def test_validation_errors_are_non_retryable(self) -> None:
        cases = [
            {"name": "x.bin", "content": "not base64!!", "encoding": "base64"},
            {"name": "dir/x.txt", "content": "x"},
            {"name": "  ", "content": "x"},
            {"name": "x.txt", "content": "x", "mime_type": "not a mime"},
            {"name": "x.txt", "content": ""},
            {"name": "x.txt", "content": "x" * (artifacts.MAX_ARTIFACT_BYTES + 1)},
            {"name": "x.txt", "content": "x", "encoding": "rot13"},
        ]
        for case in cases:
            with self.subTest(case=case["name"], encoding=case.get("encoding")):
                with self.assertRaises(ApplicationError) as ctx:
                    await self.create(**case)
                self.assertToolError(ctx.exception, "ArtifactValidationError")

    async def test_missing_identity_is_refused(self) -> None:
        with self.assertRaises(ApplicationError) as ctx:
            await self.create(owner_id="", name="x.txt", content="x")
        self.assertToolError(ctx.exception, "MissingIdentity")

    async def test_same_idempotency_key_returns_the_same_artifact(self) -> None:
        first = await self.create(name="a.txt", content="one", idempotency_key="k1")
        second = await self.create(name="a.txt", content="one", idempotency_key="k1")
        self.assertEqual(first.artifact_id, second.artifact_id)
        self.assertEqual(len(self.store.list_artifacts(user_id=OWNER, workflow_id=SESSION)), 1)

    async def test_store_failure_is_retryable(self) -> None:
        def boom(**_: Any) -> None:
            raise OSError("disk unavailable")

        with patch.object(self.store, "create_artifact", side_effect=boom):
            with self.assertRaises(ApplicationError) as ctx:
                await self.create(name="a.txt", content="x")
        self.assertToolError(ctx.exception, "ArtifactStoreUnavailable", retryable=True)

    async def test_idempotency_key_is_stable_within_one_activity(self) -> None:
        async def body() -> tuple[str | None, str | None]:
            return artifacts._idempotency_key("create_artifact"), artifacts._idempotency_key("create_artifact")

        first, again = await ActivityEnvironment().run(body)
        self.assertIsNotNone(first)
        self.assertEqual(first, again)
        self.assertTrue(first.startswith("create_artifact:"))
        self.assertIsNone(artifacts._idempotency_key("create_artifact"))


class ReadAttachmentTests(_StoreCase):
    async def read(self, attachment_id: str, **kwargs: Any):
        args: dict[str, Any] = {"store": self.store, "owner_id": OWNER, "session_id": SESSION}
        args.update(kwargs)
        return await read_attachment_impl(attachment_id=attachment_id, **args)

    async def test_text_attachment_is_returned(self) -> None:
        upload = self.upload("notes.txt", b"hello attachment")
        result = await self.read(upload.artifact_id)

        self.assertTrue(result.content_available)
        self.assertEqual(result.content, "hello attachment")
        self.assertFalse(result.truncated)
        self.assertEqual(result.returned_chars, 16)
        self.assertEqual(result.size, 16)
        self.assertEqual(result.view_url, f"/api/attachments/{upload.artifact_id}")
        # M1: the model sees str(result), so the text must be in it, after a short header.
        text = str(result)
        header, _, body = text.partition("\n")
        self.assertEqual(body, "hello attachment")
        self.assertIn("notes.txt", header)
        self.assertIn(upload.artifact_id, header)

    async def test_reads_are_bounded(self) -> None:
        upload = self.upload("big.txt", b"a" * 60_000)

        short = await self.read(upload.artifact_id, max_chars=100)
        self.assertEqual(len(short.content), 100)
        self.assertTrue(short.truncated)
        self.assertEqual(short.original_chars, 60_000)

        self.assertIn("truncated", str(short))
        self.assertIn("a" * 100, str(short))
        self.assertNotIn("a" * 101, str(short))

        clamped = await self.read(upload.artifact_id, max_chars=10**9)
        self.assertEqual(len(clamped.content), artifacts.MAX_ATTACHMENT_READ_CHARS)
        self.assertTrue(clamped.truncated)
        # The model-facing text stays bounded: the content cap plus a short header.
        self.assertLess(len(str(clamped)), artifacts.MAX_ATTACHMENT_READ_CHARS + 600)

    async def test_binary_attachment_returns_metadata_only(self) -> None:
        upload = self.upload("pixel.png", PNG, mime="image/png")
        result = await self.read(upload.artifact_id)

        self.assertFalse(result.content_available)
        self.assertIsNone(result.content)
        self.assertEqual(result.content_kind, "image")
        self.assertIn("not a text-like file", result.reason)
        self.assertIn("binary", str(result))
        self.assertIn("import_attachment_to_workspace", str(result))

    async def test_text_stored_as_binary_is_sniffed(self) -> None:
        record = self.store.create_artifact(
            user_id=OWNER,
            conversation_id=SESSION,
            workflow_id=SESSION,
            name="data.bin",
            mime_type="application/octet-stream",
            content=b"plain text after all",
            metadata={"source": USER_ATTACHMENT_SOURCE, "content_kind": "binary"},
        )
        result = await self.read(record.artifact_id)
        self.assertTrue(result.content_available)
        self.assertEqual(result.content_kind, "text")
        self.assertEqual(result.mime_type, "text/plain")

    async def test_other_owner_session_or_generated_artifact_is_not_found(self) -> None:
        theirs = self.upload("notes.txt", b"theirs", owner="user-2")
        other_session = self.upload("notes.txt", b"other", session="coding-agent-other")
        generated = await create_artifact_impl(
            store=self.store, owner_id=OWNER, session_id=SESSION, name="gen.txt", content="x"
        )
        for attachment_id in (theirs.artifact_id, other_session.artifact_id, generated.artifact_id, "nope"):
            with self.subTest(attachment_id=attachment_id):
                with self.assertRaises(ApplicationError) as ctx:
                    await self.read(attachment_id)
                self.assertToolError(ctx.exception, "AttachmentNotFound")

    async def test_expired_attachment_is_refused(self) -> None:
        past = (datetime.now(UTC) - timedelta(days=1)).isoformat()
        record = self.store.create_artifact(
            user_id=OWNER,
            conversation_id=SESSION,
            workflow_id=SESSION,
            name="old.txt",
            mime_type="text/plain",
            content=b"old",
            metadata={"source": USER_ATTACHMENT_SOURCE, "content_kind": "text", "expires_at": past},
        )
        with self.assertRaises(ApplicationError) as ctx:
            await self.read(record.artifact_id)
        self.assertToolError(ctx.exception, "AttachmentExpired")
        self.assertIn("upload it again", str(ctx.exception))

    async def test_missing_bytes_are_reported(self) -> None:
        upload = self.upload("notes.txt", b"hello")
        Path(upload.path).unlink()
        with self.assertRaises(ApplicationError) as ctx:
            await self.read(upload.artifact_id)
        self.assertToolError(ctx.exception, "AttachmentUnavailable")

    async def test_missing_identity_is_refused(self) -> None:
        upload = self.upload("notes.txt", b"hello")
        with self.assertRaises(ApplicationError) as ctx:
            await self.read(upload.artifact_id, session_id="")
        self.assertToolError(ctx.exception, "MissingIdentity")


class _FakeExecutor:
    def __init__(self, *, exit_code: int = 0) -> None:
        self.writes: list[dict[str, Any]] = []
        self.commands: list[dict[str, Any]] = []
        self.exit_code = exit_code

    async def files(self, op: str, body: dict[str, Any]) -> dict[str, Any]:
        assert op == "write", op
        self.writes.append(body)
        raw = base64.b64decode(body["content"]) if body.get("encoding") == "base64" else body["content"].encode()
        return {"path": body["path"], "size": len(raw)}

    async def execute(self, **kwargs: Any) -> dict[str, Any]:
        self.commands.append(kwargs)
        return {"exit_code": self.exit_code, "stdout": "abc123def\n", "stderr": "fatal: nope" if self.exit_code else ""}


class ImportAttachmentTests(_StoreCase):
    async def run_import(self, attachment_id: str, path: str = "data/in.bin", **kwargs: Any):
        args: dict[str, Any] = {
            "store": self.store,
            "executor": self.executor,
            "owner_id": OWNER,
            "session_id": SESSION,
            "workspace_id": WORKSPACE,
        }
        args.update(kwargs)
        return await import_attachment_impl(attachment_id=attachment_id, path=path, **args)

    def setUp(self) -> None:
        super().setUp()
        self.executor = _FakeExecutor()

    async def test_bytes_are_copied_into_the_workspace(self) -> None:
        upload = self.upload("pixel.png", PNG, mime="image/png")
        result = await self.run_import(upload.artifact_id, "assets/pixel.png")

        [write] = self.executor.writes
        self.assertEqual(write["workspace_id"], WORKSPACE)
        self.assertEqual(write["path"], "assets/pixel.png")
        self.assertEqual(write["encoding"], "base64")
        self.assertTrue(write["create_parents"])
        self.assertEqual(base64.b64decode(write["content"]), PNG)
        self.assertEqual(result.size, len(PNG))
        for part in ("pixel.png", upload.artifact_id, f"{len(PNG)} bytes", "assets/pixel.png"):
            self.assertIn(part, str(result))

    async def test_ownership_is_checked_before_writing(self) -> None:
        theirs = self.upload("notes.txt", b"theirs", owner="user-2")
        other_session = self.upload("notes.txt", b"other", session="coding-agent-other")
        for attachment_id in (theirs.artifact_id, other_session.artifact_id):
            with self.subTest(attachment_id=attachment_id):
                with self.assertRaises(ApplicationError) as ctx:
                    await self.run_import(attachment_id)
                self.assertToolError(ctx.exception, "AttachmentNotFound")
        self.assertEqual(self.executor.writes, [])

    async def test_too_large_for_the_workspace(self) -> None:
        upload = self.upload("big.txt", b"a" * 100)
        with patch.object(artifacts, "MAX_IMPORT_BYTES", 99):
            with self.assertRaises(ApplicationError) as ctx:
                await self.run_import(upload.artifact_id)
        self.assertToolError(ctx.exception, "Workspace_too_large")
        self.assertEqual(self.executor.writes, [])

    async def test_missing_workspace_or_path(self) -> None:
        upload = self.upload("notes.txt", b"hi")
        with self.assertRaises(ApplicationError) as ctx:
            await self.run_import(upload.artifact_id, workspace_id="")
        self.assertToolError(ctx.exception, "MissingIdentity")
        with self.assertRaises(ApplicationError) as ctx:
            await self.run_import(upload.artifact_id, path=" ")
        self.assertToolError(ctx.exception, "Workspace_invalid_path")


class ExecutorClientTests(unittest.IsolatedAsyncioTestCase):
    def client(self, handler) -> ExecutorClient:
        return ExecutorClient(url="http://executor.test", token="t0k", transport=httpx.MockTransport(handler))

    async def test_files_write_posts_with_bearer_token(self) -> None:
        seen: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(200, json={"path": "a.txt", "size": 1})

        data = await self.client(handler).files("write", {"workspace_id": "w", "path": "a.txt", "content": "x"})
        self.assertEqual(data["size"], 1)
        self.assertEqual(seen[0].url.path, "/files/write")
        self.assertEqual(seen[0].headers["authorization"], "Bearer t0k")

    async def test_error_mapping(self) -> None:
        cases = [
            (httpx.Response(503, text="down"), "ExecutorUnavailable", False),
            (httpx.Response(400, json={"detail": {"code": "outside_workspace", "message": "no"}}), "Workspace_outside_workspace", True),
            (httpx.Response(409, json={"detail": {"code": "busy", "message": "later"}}), "Workspace_busy", False),
            (httpx.Response(400, text="<html>"), "ExecutorBadResponse", True),
        ]
        for response, type_, non_retryable in cases:
            with self.subTest(type_=type_):
                with self.assertRaises(ApplicationError) as ctx:
                    await self.client(lambda _r, resp=response: resp).files("write", {})
                self.assertEqual(ctx.exception.type, type_)
                self.assertEqual(ctx.exception.non_retryable, non_retryable)

    async def test_unreachable_is_retryable(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("refused", request=request)

        with self.assertRaises(ApplicationError) as ctx:
            await self.client(handler).files("write", {})
        self.assertEqual(ctx.exception.type, "ExecutorUnavailable")
        self.assertFalse(ctx.exception.non_retryable)

    async def test_execute_returns_the_result_event(self) -> None:
        bodies: list[dict[str, Any]] = []

        def handler(request: httpx.Request) -> httpx.Response:
            bodies.append(json.loads(request.content))
            lines = [
                {"kind": "start", "payload": {}},
                {"kind": "stdout", "payload": {"text": "abc\n"}},
                {"kind": "result", "payload": {"exit_code": 0, "stdout": "abc\n"}},
            ]
            return httpx.Response(200, content="\n".join(json.dumps(x) for x in lines) + "\n")

        result = await self.client(handler).execute(workspace_id="w", execution_id="e-1", command="echo abc")
        self.assertEqual(result, {"exit_code": 0, "stdout": "abc\n"})
        self.assertEqual(bodies[0]["execution_id"], "e-1")
        self.assertFalse(bodies[0]["network"])


class ToolDefinitionTests(unittest.TestCase):
    """Risk classes, identity injection and retry policies (PRD §5.8)."""

    def risk(self, tool) -> tuple[bool, str | None]:
        nonlocals = inspect.getclosurevars(tool).nonlocals
        return nonlocals["inherently_safe"], nonlocals["auto_approval_criteria"]

    def test_exported_tools(self) -> None:
        self.assertEqual(
            [t.__name__ for t in ARTIFACT_TOOLS],
            ["create_artifact", "read_attachment", "import_attachment_to_workspace"],
        )
        for tool in ARTIFACT_TOOLS:
            self.assertEqual(tool_activity(tool).__name__, tool.__name__)

    def test_risk_classes(self) -> None:
        self.assertEqual(self.risk(create_artifact), (False, "external_side_effect"))
        self.assertEqual(self.risk(read_attachment), (True, None))
        self.assertEqual(self.risk(import_attachment_to_workspace), (False, "workspace_edit"))

    def test_identity_is_never_model_facing(self) -> None:
        expected = {
            "create_artifact": {"name", "content", "mime_type", "encoding"},
            "read_attachment": {"attachment_id", "max_chars"},
            "import_attachment_to_workspace": {"attachment_id", "path"},
        }
        for tool in ARTIFACT_TOOLS:
            with self.subTest(tool=tool.__name__):
                self.assertEqual(set(inspect.signature(tool).parameters), expected[tool.__name__])

    def test_retry_policies_are_explicit(self) -> None:
        for config in (artifacts.STORE_READ_CONFIG, artifacts.STORE_WRITE_CONFIG, artifacts.IMPORT_CONFIG):
            self.assertIsNotNone(config["retry_policy"].maximum_attempts)
            self.assertGreater(config["retry_policy"].maximum_attempts, 0)
            self.assertIn("start_to_close_timeout", config)


class _FakeRunner:
    def __init__(self, result: Any) -> None:
        self.result = result
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def run_tool(self, tool_call_id: str, tool: Any, *, injections: Any = None, **kwargs: Any) -> Any:
        self.calls.append((tool_call_id, kwargs))
        return self.result


class ModelFacingRenderingTests(_StoreCase):
    """M1 regression: what the model receives goes through harness_ext.per_run_toolset."""

    async def model_sees(self, tool_name: str, result: Any, **kwargs: Any) -> Any:
        from types import SimpleNamespace

        from simple_chat_agent.agent import harness_ext

        runner = _FakeRunner(result)
        toolset = harness_ext.per_run_toolset(list(ARTIFACT_TOOLS), id="t", injections={})
        invoke = toolset.tools[tool_name].function
        with patch.object(harness_ext, "_runner_from_ctx", lambda ctx, tool_name: runner):
            seen = await invoke(SimpleNamespace(tool_call_id="call-1"), **kwargs)
        self.assertEqual(len(runner.calls), 1)
        return seen

    async def test_read_attachment_text_reaches_the_model(self) -> None:
        upload = self.upload("notes.txt", b"the secret ingredient is cardamom")
        result = await read_attachment_impl(
            store=self.store, owner_id=OWNER, session_id=SESSION, attachment_id=upload.artifact_id
        )
        seen = await self.model_sees("read_attachment", result, attachment_id=upload.artifact_id)
        self.assertIsInstance(seen, str)
        self.assertIn("the secret ingredient is cardamom", seen)

    async def test_binary_attachment_tells_the_model_how_to_proceed(self) -> None:
        upload = self.upload("pixel.png", PNG, mime="image/png")
        result = await read_attachment_impl(
            store=self.store, owner_id=OWNER, session_id=SESSION, attachment_id=upload.artifact_id
        )
        seen = await self.model_sees("read_attachment", result, attachment_id=upload.artifact_id)
        self.assertIn("binary", seen)
        self.assertIn("import_attachment_to_workspace", seen)

    async def test_create_and_import_results_reach_the_model_with_ids(self) -> None:
        created = await create_artifact_impl(
            store=self.store, owner_id=OWNER, session_id=SESSION, name="r.txt", content="x"
        )
        seen = await self.model_sees("create_artifact", created, name="r.txt", content="x")
        self.assertIn(created.artifact_id, seen)
        self.assertIn(created.view_url, seen)

        upload = self.upload("notes.txt", b"hi")
        imported = await import_attachment_impl(
            store=self.store,
            executor=_FakeExecutor(),
            owner_id=OWNER,
            session_id=SESSION,
            workspace_id=WORKSPACE,
            attachment_id=upload.artifact_id,
            path="in/notes.txt",
        )
        seen = await self.model_sees(
            "import_attachment_to_workspace", imported, attachment_id=upload.artifact_id, path="in/notes.txt"
        )
        self.assertIn("in/notes.txt", seen)

    def test_every_result_model_defines_its_own_str(self) -> None:
        from pydantic import BaseModel

        for model in (artifacts.ArtifactResult, artifacts.AttachmentReadResult, artifacts.AttachmentImportResult):
            with self.subTest(model=model.__name__):
                self.assertIsNot(model.__str__, BaseModel.__str__)


_REPO = Path(__file__).resolve().parents[1]
_OLD_HARNESS_IMPORT = re.compile(r"^\s*(?:from|import)\s+agent_harness\b", re.MULTILINE)
# Files still on the old harness, owned by others; each entry must be removed once its file
# is ported (the test fails if an entry no longer imports agent_harness).
_PENDING_OLD_HARNESS_IMPORTS = {"simple_chat_agent/common/streaming.py"}


class NoOldHarnessImportTests(unittest.TestCase):
    """Minor 16: the new agent and the shared code must not import the in-tree agent_harness
    package (P5 deletes it). ``temporal_agent_harness`` is the official one and is fine."""

    def offenders(self) -> set[str]:
        found = set()
        for folder in ("simple_chat_agent/agent", "simple_chat_agent/common"):
            for path in sorted((_REPO / folder).rglob("*.py")):
                if _OLD_HARNESS_IMPORT.search(path.read_text(encoding="utf-8")):
                    found.add(path.relative_to(_REPO).as_posix())
        return found

    def test_no_new_imports_of_the_old_harness(self) -> None:
        self.assertEqual(self.offenders() - _PENDING_OLD_HARNESS_IMPORTS, set())

    def test_pending_list_only_shrinks(self) -> None:
        self.assertEqual(_PENDING_OLD_HARNESS_IMPORTS - self.offenders(), set())

    def test_pattern_ignores_the_official_harness(self) -> None:
        self.assertIsNone(_OLD_HARNESS_IMPORT.search("from temporal_agent_harness.harness import agent"))
        self.assertIsNotNone(_OLD_HARNESS_IMPORT.search("    from agent_harness.attachments import X"))
        self.assertIsNotNone(_OLD_HARNESS_IMPORT.search("import agent_harness"))


class AttachmentRefCompatTests(unittest.TestCase):
    """The old API sends common's AttachmentRef over a signal and the old worker decodes it
    into agent_harness's; the field layout must match until P5 removes the old app."""

    def test_common_attachment_ref_matches_the_old_one(self) -> None:
        import dataclasses

        try:
            from agent_harness.attachments import AttachmentRef as OldRef
        except ImportError:  # pragma: no cover - after P5 deletes agent_harness
            self.skipTest("agent_harness is gone")
        from simple_chat_agent.common.attachments import AttachmentRef as NewRef

        def layout(cls: type) -> list[tuple[str, str, Any]]:
            return [
                (f.name, str(f.type), f.default if f.default is not dataclasses.MISSING else f.default_factory)
                for f in dataclasses.fields(cls)
            ]

        self.assertEqual(layout(NewRef), layout(OldRef))

    def test_old_app_import_paths_still_work(self) -> None:
        from simple_chat_agent.common.attachments import (  # noqa: F401
            AttachmentContentKind,
            AttachmentRef,
            attachment_dict,
            attachment_ref_from_artifact,
        )


REFUND_SERVICE_FILES = [
    ".gitignore",
    "pyproject.toml",
    "refund_service/__init__.py",
    "refund_service/models.py",
    "refund_service/money.py",
    "refund_service/refunds.py",
    "refund_service/settlement.py",
    "refund_service/store.py",
    "requirements.txt",
    "tests/conftest.py",
    "tests/test_money.py",
    "tests/test_refund.py",
    "tests/test_settlement.py",
]


class FixtureLoaderTests(unittest.IsolatedAsyncioTestCase):
    def test_refund_service_file_set_is_stable(self) -> None:
        files = fixtures.fixture_files("refund_service")
        self.assertEqual(list(files), REFUND_SERVICE_FILES)

    def test_planted_bug_is_still_there(self) -> None:
        # The demo depends on exactly this bug; see tests/fixtures/refund_service/README.md.
        refunds = fixtures.fixture_files("refund_service")["refund_service/refunds.py"]
        self.assertIn("amount = order.total_cents if amount_cents is None else amount_cents", refunds)
        self.assertIn(
            "def test_refund_rest_of_partially_refunded_order",
            fixtures.fixture_files("refund_service")["tests/test_refund.py"],
        )

    def test_readme_and_caches_are_not_seeded(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "demo"
            (root / "pkg" / "__pycache__").mkdir(parents=True)
            (root / "pkg" / "__pycache__" / "m.cpython-312.pyc").write_bytes(b"\x00")
            (root / "pkg" / "m.py").write_text("x = 1\n")
            (root / ".pytest_cache").mkdir()
            (root / ".pytest_cache" / "README.md").write_text("cache")
            (root / "README.md").write_text("the answer")
            with patch.dict(os.environ, {"AGENT_FIXTURES_DIR": tmp}):
                self.assertEqual(fixtures.fixture_files("demo"), {"pkg/m.py": "x = 1\n"})

    def test_bad_or_unknown_names(self) -> None:
        with self.assertRaises(ValueError):
            fixtures.fixture_files("../etc")
        with self.assertRaises(FileNotFoundError):
            fixtures.fixture_files("no_such_fixture")

    async def test_seed_writes_every_file_then_commits(self) -> None:
        executor = _FakeExecutor()
        result = await fixtures.seed_workspace("ws-1", "refund_service", executor=executor)

        self.assertEqual([w["path"] for w in executor.writes], REFUND_SERVICE_FILES)
        self.assertTrue(all(w["workspace_id"] == "ws-1" and w["create_parents"] for w in executor.writes))
        [command] = executor.commands
        self.assertEqual(command["workspace_id"], "ws-1")
        self.assertFalse(command["network"])
        for part in ("git init", "git add -A", "commit -q -m 'Initial import'", "git rev-parse HEAD"):
            self.assertIn(part, command["command"])
        self.assertRegex(command["execution_id"], r"^[A-Za-z0-9_.:-]{1,160}$")
        self.assertEqual(result.commit, "abc123def")
        self.assertEqual(result.files, REFUND_SERVICE_FILES)

    async def test_seed_reports_a_failed_git_step(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "fatal: nope"):
            await fixtures.seed_workspace("ws-1", "refund_service", executor=_FakeExecutor(exit_code=128))


if __name__ == "__main__":
    unittest.main()
