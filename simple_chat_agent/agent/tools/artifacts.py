"""Artifact and attachment tools on the harness (PRD A7).

Ported from ``simple_chat_agent/worker/tools/{artifacts,attachments}.py`` onto harness
activity tools over the existing ``AppStore`` (local disk + SQLite, or S3 + DynamoDB when
configured). Behaviour is unchanged: owner and session checks, size and MIME validation,
expiry handling, bounded text reads and metadata-only answers for binary files.

Identity (``owner_id``, ``session_id``, ``workspace_id``) is injected by the agent from its
own init data, never chosen by the model. ``session_id`` is the CodingAgent workflow id,
which is what uploads record as ``workflow_id``.

Errors the model can act on (validation, not found, expired) are non-retryable
``ApplicationError``s, so they reach the model as a tool failure and the turn continues. A
store or executor that is down is retryable, bounded by the tool's retry policy, after
which the model sees the error (PRD A7 "store down").
"""

# No `from __future__ import annotations`: the harness resolves tool type hints against its
# own module globals, so annotations must be real objects, not strings.

import asyncio
import base64
import json
import os
import re
from datetime import timedelta
from typing import Any, Literal

import httpx
from pydantic import BaseModel
from temporalio import activity
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError
from temporalio.workflow import ActivityConfig

from temporal_agent_harness.harness import agent

from simple_chat_agent.common.attachments import (
    artifact_is_user_attachment,
    decode_text_attachment,
    sniff_attachment_text_kind,
    text_like_content_kind,
)
from simple_chat_agent.common.store import (
    AppStore,
    ArtifactRecord,
    artifact_expires_at,
    artifact_is_expired,
)

MAX_ARTIFACT_BYTES = 2_000_000
MAX_ATTACHMENT_READ_CHARS = 50_000
DEFAULT_ATTACHMENT_READ_CHARS = 12_000
# The executor caps one /files/write at 5 MiB (decoded).
MAX_IMPORT_BYTES = 5 * 1024 * 1024

# Risk classes (PRD §5.8), stated once and used by the decorators below.
CREATE_ARTIFACT_CRITERIA = "external_side_effect"
IMPORT_ATTACHMENT_CRITERIA = "workspace_edit"

STORE_READ_CONFIG = ActivityConfig(
    start_to_close_timeout=timedelta(seconds=30),
    retry_policy=RetryPolicy(maximum_attempts=5, maximum_interval=timedelta(seconds=10)),
)
STORE_WRITE_CONFIG = ActivityConfig(
    start_to_close_timeout=timedelta(seconds=60),
    # Idempotent per activity (see _idempotency_key), so retries never duplicate.
    retry_policy=RetryPolicy(maximum_attempts=3, maximum_interval=timedelta(seconds=10)),
)
IMPORT_CONFIG = ActivityConfig(
    start_to_close_timeout=timedelta(seconds=60),
    # /files/write is an atomic overwrite of the same bytes, so a retry is harmless.
    retry_policy=RetryPolicy(maximum_attempts=3, maximum_interval=timedelta(seconds=10)),
)

ArtifactEncoding = Literal["text", "base64"]


# -- Results --------------------------------------------------------------------------


class ArtifactResult(BaseModel):
    artifact_id: str
    name: str
    mime_type: str
    size: int
    view_url: str
    download_url: str
    expires_at: str

    def __str__(self) -> str:
        return f"Created artifact {self.name} ({self.mime_type}, {self.size} bytes): {self.view_url}"


class AttachmentReadResult(BaseModel):
    attachment_id: str
    name: str
    mime_type: str
    size: int
    content_kind: str
    view_url: str
    content_available: bool
    content: str | None = None
    truncated: bool = False
    original_chars: int | None = None
    returned_chars: int = 0
    reason: str | None = None

    def __str__(self) -> str:
        head = f"{self.name} ({self.mime_type}, {self.size} bytes)"
        if not self.content_available:
            return f"{head}: no text content. {self.reason or ''}".rstrip()
        more = f", truncated from {self.original_chars}" if self.truncated else ""
        return f"{head}: {self.returned_chars} chars{more}"


class AttachmentImportResult(BaseModel):
    attachment_id: str
    name: str
    mime_type: str
    size: int
    path: str

    def __str__(self) -> str:
        return f"Imported {self.name} ({self.size} bytes) to {self.path}."


# -- Shared helpers -------------------------------------------------------------------


def _store() -> AppStore:
    """The AppStore for this worker. Tests patch this."""
    return AppStore()


async def _run_store(fn: Any, /, **kwargs: Any) -> Any:
    """Run a blocking AppStore call off the event loop. Store failures are retryable."""
    try:
        return await asyncio.to_thread(fn, **kwargs)
    except ApplicationError:
        raise
    except Exception as exc:
        raise ApplicationError(
            f"The artifact store isn't answering: {type(exc).__name__}: {exc}",
            type="ArtifactStoreUnavailable",
        ) from exc


def _idempotency_key(tool: str) -> str | None:
    """Stable across retries of one tool call: the activity id within its workflow run."""
    if not activity.in_activity():
        return None
    info = activity.info()
    return f"{tool}:{info.workflow_run_id}:{info.activity_id}"


def _not_found(attachment_id: str) -> ApplicationError:
    return ApplicationError(
        f"Attachment {attachment_id} not found.",
        type="AttachmentNotFound",
        non_retryable=True,
    )


async def _load_attachment(
    store: AppStore, *, owner_id: str, session_id: str, attachment_id: str
) -> ArtifactRecord:
    """The owner's user upload in this session, or AttachmentNotFound. Expired is an error."""
    if not owner_id or not session_id:
        raise ApplicationError(
            "Attachment identity context is not available.",
            type="MissingIdentity",
            non_retryable=True,
        )
    record = await _run_store(store.get_artifact, user_id=owner_id, artifact_id=attachment_id)
    if (
        record is None
        or not artifact_is_user_attachment(record)
        or record.workflow_id != session_id
    ):
        raise _not_found(attachment_id)
    if artifact_is_expired(record):
        raise ApplicationError(
            f"Attachment {record.name} has expired. It is past its retention window; "
            "ask the user to upload it again if the content is still needed.",
            type="AttachmentExpired",
            non_retryable=True,
        )
    return record


async def _read_bytes(store: AppStore, record: ArtifactRecord) -> bytes:
    try:
        return await asyncio.to_thread(store.read_artifact_bytes, record)
    except FileNotFoundError as exc:
        raise ApplicationError(
            f"Attachment {record.name} content is unavailable: the metadata exists but the "
            "stored bytes are gone (expired or deleted).",
            type="AttachmentUnavailable",
            non_retryable=True,
        ) from exc
    except Exception as exc:
        raise ApplicationError(
            f"The artifact store isn't answering: {type(exc).__name__}: {exc}",
            type="ArtifactStoreUnavailable",
        ) from exc


def _decode_content(content: str, encoding: str) -> bytes:
    if encoding == "text":
        return content.encode("utf-8")
    if encoding == "base64":
        try:
            return base64.b64decode(content, validate=True)
        except ValueError as err:
            raise ValueError("Artifact content is not valid base64.") from err
    raise ValueError(f"Unsupported artifact encoding: {encoding}")


def _validate_artifact(*, name: str, mime_type: str, content: bytes) -> None:
    if not name.strip():
        raise ValueError("Artifact name is required.")
    if "/" in name or "\\" in name:
        raise ValueError("Artifact name must be a file name, not a path.")
    if not re.fullmatch(r"[A-Za-z0-9.+_-]+/[A-Za-z0-9.+_-]+", mime_type):
        raise ValueError("Artifact mime_type must be a valid MIME type.")
    if not content:
        raise ValueError("Artifact content is empty.")
    if len(content) > MAX_ARTIFACT_BYTES:
        raise ValueError(f"Artifact is too large. Max bytes: {MAX_ARTIFACT_BYTES}.")


# -- Implementations (plain async functions, unit-tested directly) --------------------


async def create_artifact_impl(
    *,
    store: AppStore,
    owner_id: str,
    session_id: str,
    name: str,
    content: str,
    mime_type: str = "text/plain",
    encoding: str = "text",
    idempotency_key: str | None = None,
) -> ArtifactResult:
    if not owner_id or not session_id:
        raise ApplicationError(
            "Artifact identity context is not available.",
            type="MissingIdentity",
            non_retryable=True,
        )
    try:
        data = _decode_content(content, encoding)
        _validate_artifact(name=name, mime_type=mime_type, content=data)
    except ValueError as err:
        raise ApplicationError(str(err), type="ArtifactValidationError", non_retryable=True)
    record = await _run_store(
        store.create_artifact,
        user_id=owner_id,
        conversation_id=session_id,
        workflow_id=session_id,
        name=name,
        mime_type=mime_type,
        content=data,
        metadata={"encoding": encoding},
        idempotency_key=idempotency_key,
    )
    return ArtifactResult(
        artifact_id=record.artifact_id,
        name=record.name,
        mime_type=record.mime_type,
        size=record.size_bytes,
        view_url=f"/api/artifacts/{record.artifact_id}",
        download_url=f"/api/artifacts/{record.artifact_id}/download",
        expires_at=artifact_expires_at(record),
    )


async def read_attachment_impl(
    *,
    store: AppStore,
    owner_id: str,
    session_id: str,
    attachment_id: str,
    max_chars: int = DEFAULT_ATTACHMENT_READ_CHARS,
) -> AttachmentReadResult:
    limit = max(1, min(int(max_chars or DEFAULT_ATTACHMENT_READ_CHARS), MAX_ATTACHMENT_READ_CHARS))
    record = await _load_attachment(
        store, owner_id=owner_id, session_id=session_id, attachment_id=attachment_id
    )
    data = await _read_bytes(store, record)

    kind = record.metadata.get("content_kind")
    mime_type = record.mime_type
    if not text_like_content_kind(str(kind) if kind else None):
        detected_kind, detected_mime = sniff_attachment_text_kind(
            name=record.name, mime_type=record.mime_type, content=data
        )
        if text_like_content_kind(detected_kind):
            kind, mime_type = detected_kind, detected_mime

    base = {
        "attachment_id": record.artifact_id,
        "name": record.name,
        "mime_type": mime_type,
        "size": record.size_bytes,
        "content_kind": str(kind or "binary"),
        "view_url": f"/api/attachments/{record.artifact_id}",
    }
    if not text_like_content_kind(str(kind) if kind else None):
        return AttachmentReadResult(
            **base,
            content_available=False,
            reason=(
                "This attachment is not a text-like file. Provider-native inspection may be "
                "available for supported models; otherwise ask the user for a text version, "
                "or import it into the workspace with import_attachment_to_workspace."
            ),
        )
    text = decode_text_attachment(data, mime_type=mime_type)
    return AttachmentReadResult(
        **base,
        content_available=True,
        content=text[:limit],
        truncated=len(text) > limit,
        original_chars=len(text),
        returned_chars=min(len(text), limit),
    )


class ExecutorClient:
    """A small client for the sandbox executor (WORKSPACE_EXECUTOR_URL / _TOKEN).

    Separate from tools/workspace.py on purpose (file ownership); the error mapping is the
    same: unreachable or 5xx is retryable, a structured 4xx is a non-retryable tool error.
    """

    def __init__(
        self,
        url: str | None = None,
        token: str | None = None,
        timeout: float = 30.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ):
        self.url = (url or os.environ.get("WORKSPACE_EXECUTOR_URL", "http://127.0.0.1:8082")).rstrip("/")
        self.token = token if token is not None else os.environ.get("WORKSPACE_EXECUTOR_TOKEN", "")
        self.timeout = timeout
        self._transport = transport  # tests only

    def _client(self, timeout: float) -> httpx.AsyncClient:
        return httpx.AsyncClient(timeout=timeout, transport=self._transport)

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"}

    async def files(self, op: str, body: dict[str, Any]) -> dict[str, Any]:
        try:
            async with self._client(self.timeout) as client:
                response = await client.post(f"{self.url}/files/{op}", json=body, headers=self._headers())
        except httpx.HTTPError as exc:
            raise ApplicationError(f"The sandbox isn't answering: {exc}", type="ExecutorUnavailable")
        return _check_response(response, op)

    async def execute(
        self,
        *,
        workspace_id: str,
        execution_id: str,
        command: str,
        timeout_seconds: int = 60,
        network: bool = False,
    ) -> dict[str, Any]:
        """Run a command and return the final ``result`` payload of the NDJSON stream."""
        body = {
            "workspace_id": workspace_id,
            "execution_id": execution_id,
            "command": command,
            "timeout_seconds": timeout_seconds,
            "network": network,
        }
        result: dict[str, Any] | None = None
        try:
            async with self._client(self.timeout + timeout_seconds) as client:
                async with client.stream(
                    "POST", f"{self.url}/execute", json=body, headers=self._headers()
                ) as response:
                    if response.status_code >= 400:
                        await response.aread()
                        _check_response(response, "execute")
                    async for line in response.aiter_lines():
                        if not line.strip():
                            continue
                        event = json.loads(line)
                        if event.get("kind") == "result":
                            result = event.get("payload") or {}
        except httpx.HTTPError as exc:
            raise ApplicationError(f"The sandbox isn't answering: {exc}", type="ExecutorUnavailable")
        if result is None:
            raise ApplicationError("The sandbox ended the command without a result.", type="ExecutorUnavailable")
        return result


_PERMANENT_CODES = frozenset(
    {"not_found", "is_symlink", "outside_workspace", "too_large", "not_regular_file",
     "exists", "invalid_path", "not_a_directory", "invalid_request"}
)


def _check_response(response: httpx.Response, op: str) -> dict[str, Any]:
    if response.status_code >= 500:
        raise ApplicationError(f"The sandbox failed ({response.status_code}).", type="ExecutorUnavailable")
    try:
        data = response.json()
    except ValueError:
        raise ApplicationError(
            f"The sandbox returned an unreadable response ({response.status_code}).",
            type="ExecutorBadResponse",
            non_retryable=True,
        )
    if response.status_code >= 400:
        detail = data.get("detail", data) if isinstance(data, dict) else data
        code = detail.get("code") if isinstance(detail, dict) else None
        message = detail.get("message") if isinstance(detail, dict) else str(detail)
        raise ApplicationError(
            message or f"{op} failed",
            type=f"Workspace_{code or 'error'}",
            non_retryable=code in _PERMANENT_CODES or response.status_code in (401, 403, 404, 422),
        )
    return data


async def import_attachment_impl(
    *,
    store: AppStore,
    executor: ExecutorClient,
    owner_id: str,
    session_id: str,
    workspace_id: str,
    attachment_id: str,
    path: str,
) -> AttachmentImportResult:
    if not workspace_id:
        raise ApplicationError(
            "Workspace context is not available.", type="MissingIdentity", non_retryable=True
        )
    if not path.strip():
        raise ApplicationError("A destination path is required.", type="Workspace_invalid_path", non_retryable=True)
    record = await _load_attachment(
        store, owner_id=owner_id, session_id=session_id, attachment_id=attachment_id
    )
    if record.size_bytes > MAX_IMPORT_BYTES:
        raise ApplicationError(
            f"Attachment {record.name} is {record.size_bytes} bytes; the workspace accepts at "
            f"most {MAX_IMPORT_BYTES} bytes per file.",
            type="Workspace_too_large",
            non_retryable=True,
        )
    data = await _read_bytes(store, record)
    written = await executor.files(
        "write",
        {
            "workspace_id": workspace_id,
            "path": path,
            "content": base64.b64encode(data).decode("ascii"),
            "encoding": "base64",
            "create_parents": True,
        },
    )
    return AttachmentImportResult(
        attachment_id=record.artifact_id,
        name=record.name,
        mime_type=record.mime_type,
        size=int(written.get("size", len(data))),
        path=str(written.get("path") or path),
    )


# -- Harness tools --------------------------------------------------------------------


@agent.activity_tool_defn(
    activity_config=STORE_WRITE_CONFIG, auto_approval_criteria=CREATE_ARTIFACT_CRITERIA
)
async def create_artifact(
    name: str,
    content: str,
    owner_id: agent.Injected[str],
    session_id: agent.Injected[str],
    mime_type: str = "text/plain",
    encoding: ArtifactEncoding = "text",
) -> ArtifactResult:
    """Publish a file the user can view and download from the chat UI. Use this when the
    user asks you to write, save, export or create a user-facing file; files in the
    workspace are not shown to the user until published here.

    `name` is a file name only (no path). Pass text with encoding='text', or binary content
    as base64 with encoding='base64'. At most 2,000,000 bytes. HTML artifacts must be
    self-contained and work without JavaScript or network-loaded assets: the viewer
    disables both."""
    return await create_artifact_impl(
        store=_store(),
        owner_id=owner_id,
        session_id=session_id,
        name=name,
        content=content,
        mime_type=mime_type,
        encoding=encoding,
        idempotency_key=_idempotency_key("create_artifact"),
    )


@agent.activity_tool_defn(inherently_safe=True, activity_config=STORE_READ_CONFIG)
async def read_attachment(
    attachment_id: str,
    owner_id: agent.Injected[str],
    session_id: agent.Injected[str],
    max_chars: int = DEFAULT_ATTACHMENT_READ_CHARS,
) -> AttachmentReadResult:
    """Read the text of a file or paste block the user attached to this chat, by
    attachment_id. Returns at most `max_chars` characters (up to 50,000). For non-text
    files it returns metadata only."""
    return await read_attachment_impl(
        store=_store(),
        owner_id=owner_id,
        session_id=session_id,
        attachment_id=attachment_id,
        max_chars=max_chars,
    )


@agent.activity_tool_defn(
    activity_config=IMPORT_CONFIG, auto_approval_criteria=IMPORT_ATTACHMENT_CRITERIA
)
async def import_attachment_to_workspace(
    attachment_id: str,
    path: str,
    owner_id: agent.Injected[str],
    session_id: agent.Injected[str],
    workspace_id: agent.Injected[str],
) -> AttachmentImportResult:
    """Copy a file the user attached to this chat into the workspace, byte for byte, so you
    can work on it with the workspace tools. `path` is relative to the workspace root;
    parent directories are created and an existing file is overwritten."""
    return await import_attachment_impl(
        store=_store(),
        executor=ExecutorClient(),
        owner_id=owner_id,
        session_id=session_id,
        workspace_id=workspace_id,
        attachment_id=attachment_id,
        path=path,
    )


ARTIFACT_TOOLS = (create_artifact, read_attachment, import_attachment_to_workspace)
