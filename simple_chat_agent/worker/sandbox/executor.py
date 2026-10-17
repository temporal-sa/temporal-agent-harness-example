from __future__ import annotations

import asyncio
import base64
import binascii
import codecs
import errno
import hashlib
import hmac
import http.client
import json
import os
import re
import shutil
import signal
import secrets
import socket
import stat as stat_module
import subprocess
import time
from collections import deque
from collections.abc import AsyncIterator, Callable
from contextlib import ExitStack, asynccontextmanager, suppress
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any, Literal, TypeVar

import uvicorn
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, ConfigDict, Field

MAX_COMMAND_CHARS = 100_000
MAX_STDIN_CHARS = 1_000_000
MAX_TIMEOUT_SECONDS = 15 * 60
DEFAULT_TIMEOUT_SECONDS = 300
MAX_CAPTURE_CHARS = 100_000
MAX_STREAM_CHARS = 250_000
MAX_CHANGED_FILES = 2_000
READ_CHUNK_BYTES = 4096
PROGRESS_INTERVAL_SECONDS = 5
TERMINATE_GRACE_SECONDS = 2
WORKSPACE_MODE = 0o700
STATE_DIR_NAME = ".executor-state"
DEFAULT_LIVE_HEALTH_INTERVAL_SECONDS = 30.0
DEFAULT_MAX_ORPHAN_ZOMBIES = 8
# Reattach (P1 A5): a running job outlives its HTTP stream. These bound what each job
# keeps for a reattaching client, and how long it runs with nobody attached.
JOB_REPLAY_BUFFER_CHARS = 128_000
JOB_REPLAY_BUFFER_EVENTS = 2_048
DEFAULT_ORPHAN_GRACE_SECONDS = 120.0
# Per-execution state (result, replay events, started marker) older than this is swept.
# Longer than any activity's retry window, so idempotency is unaffected.
DEFAULT_STATE_TTL_SECONDS = 24 * 3600.0
STATE_GC_INTERVAL_SECONDS = 3600.0
_SAFE_EXECUTION_ID = re.compile(r"^[A-Za-z0-9_.:-]{1,160}$")

# /files/* limits (PRD §5 rule 5).
MAX_FILE_READ_BYTES = 1024 * 1024
MAX_FILE_WRITE_BYTES = 5 * 1024 * 1024
MAX_LIST_ENTRIES = 2_000
MAX_FILE_PATH_BYTES = 4096
# Request-body guard only: base64 of MAX_FILE_WRITE_BYTES fits comfortably.
# Anything between the decoded cap and this bound gets a structured too_large.
MAX_FILE_CONTENT_CHARS = 8 * 1024 * 1024
MAX_FILE_REQUEST_PATH_CHARS = 65_536
_T = TypeVar("_T")


class ExecuteRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    workspace_id: str = Field(min_length=1, max_length=512)
    execution_id: str = Field(min_length=1, max_length=200)
    command: str = Field(min_length=1, max_length=MAX_COMMAND_CHARS)
    cwd: str = Field(default="/workspace", max_length=1024)
    timeout_seconds: int = Field(
        default=DEFAULT_TIMEOUT_SECONDS,
        ge=1,
        le=MAX_TIMEOUT_SECONDS,
    )
    network: bool = True
    stdin: str | None = Field(default=None, max_length=MAX_STDIN_CHARS)


MAX_LOG_RESPONSE_BYTES = 256 * 1024


class ExecuteLogRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    workspace_id: str = Field(min_length=1, max_length=512)
    execution_id: str = Field(min_length=1, max_length=200)
    stream: Literal["stdout", "stderr", "both"] = "both"
    offset: int = Field(default=0, ge=0)
    limit: int = Field(default=MAX_LOG_RESPONSE_BYTES, ge=1, le=MAX_LOG_RESPONSE_BYTES)


class ExecuteCancelRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    workspace_id: str = Field(min_length=1, max_length=512)
    execution_id: str = Field(min_length=1, max_length=200)


class _FileRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    workspace_id: str = Field(min_length=1, max_length=512)


class FilePathRequest(_FileRequest):
    path: str = Field(max_length=MAX_FILE_REQUEST_PATH_CHARS)


class FileReadRequest(FilePathRequest):
    offset: int = Field(default=0, ge=0)
    length: int | None = Field(default=None, ge=0)


class FileListRequest(_FileRequest):
    path: str = Field(default=".", max_length=MAX_FILE_REQUEST_PATH_CHARS)


class _MutationRequest(BaseModel):
    # How long a mutation waits for a running command to finish before failing with
    # workspace_busy (nothing is written). None waits indefinitely.
    busy_timeout_seconds: float | None = Field(default=None, ge=0, le=600)


class FileWriteRequest(FilePathRequest, _MutationRequest):
    content: str = Field(max_length=MAX_FILE_CONTENT_CHARS)
    encoding: Literal["utf-8", "base64"] = "utf-8"
    create_parents: bool = False
    overwrite: bool = True


class FileEditRequest(FilePathRequest, _MutationRequest):
    old_string: str = Field(min_length=1, max_length=MAX_FILE_CONTENT_CHARS)
    new_string: str = Field(max_length=MAX_FILE_CONTENT_CHARS)


class FileMkdirRequest(FilePathRequest, _MutationRequest):
    parents: bool = False


class FileDeleteRequest(FilePathRequest, _MutationRequest):
    pass


class FileRenameRequest(_FileRequest, _MutationRequest):
    source: str = Field(max_length=MAX_FILE_REQUEST_PATH_CHARS)
    destination: str = Field(max_length=MAX_FILE_REQUEST_PATH_CHARS)
    overwrite: bool = False
    create_parents: bool = False


class _ReadWriteLock:
    """An asyncio readers/writer lock that prefers waiting writers."""

    def __init__(self) -> None:
        self._condition = asyncio.Condition()
        self._readers = 0
        self._writer = False
        self._waiting_writers = 0

    @asynccontextmanager
    async def shared(self) -> AsyncIterator[None]:
        async with self._condition:
            await self._condition.wait_for(
                lambda: not self._writer and self._waiting_writers == 0
            )
            self._readers += 1
        try:
            yield
        finally:
            async with self._condition:
                self._readers -= 1
                if self._readers == 0:
                    self._condition.notify_all()

    @asynccontextmanager
    async def exclusive(self) -> AsyncIterator[None]:
        async with self._condition:
            self._waiting_writers += 1
            try:
                await self._condition.wait_for(
                    lambda: not self._writer and self._readers == 0
                )
            finally:
                self._waiting_writers -= 1
                if not self._writer and self._waiting_writers == 0:
                    # A cancelled waiter may have been the only thing holding
                    # readers back.
                    self._condition.notify_all()
            self._writer = True
        try:
            yield
        finally:
            async with self._condition:
                self._writer = False
                self._condition.notify_all()


@dataclass
class _WorkspaceLocks:
    """Per-workspace locking for /execute and /files/*.

    Three kinds of access share one workspace:

    - ``/execute`` (may run for up to 15 minutes) takes ``exec``.
    - File mutations (write, edit, mkdir, delete, rename) take ``exec`` and
      then ``files.exclusive()``. They never interleave with a running
      command's writes or with each other, and readers never see a
      half-applied mutation.
    - File reads (stat, read, list) take ``files.shared()`` only.

    So the exclusive side is "exec + files.exclusive": writes block on a
    running command, but reads do not. A read during a command can observe
    the command's in-progress writes, exactly as another process on the box
    would; that is the trade-off for keeping read_file fast while a long
    pytest runs. A mutation waiting behind ``exec`` has not yet registered on
    ``files``, so it does not hold up readers; once it owns ``exec`` it waits
    only for in-flight reads, which are millisecond operations.

    Lock order is always ``exec`` -> ``_concurrency`` (jobs) and ``exec`` ->
    ``files`` (mutations), so there is no cycle. A job takes its workspace's
    ``exec`` before a global ``_concurrency`` slot, so jobs queued behind a
    long command in one workspace never hold slots other workspaces need. A command cannot keep mutating after its ``/execute`` returns:
    the sandbox has its own PID namespace and ``--die-with-parent``, and the
    process group is killed on timeout or cancellation.
    """

    exec: asyncio.Lock = field(default_factory=asyncio.Lock)
    files: _ReadWriteLock = field(default_factory=_ReadWriteLock)


@dataclass
class _Job:
    """One /execute job, which outlives the HTTP stream that started it.

    Lifecycle: ``queued`` (waiting for the workspace lock) -> ``running`` -> done
    (``result`` set). Stream events are kept in a bounded replay buffer so a client that
    reconnects with the same execution_id (an activity retried after a worker crash)
    replays what it missed and then follows live. The oldest events are dropped first;
    a subscriber that fell behind the buffer gets one ``dropped`` marker event. The final
    result is held outside the buffer and is never dropped.
    """

    workspace_key: str
    execution_id: str
    fingerprint: str
    state: Literal["queued", "running", "done"] = "queued"
    events: deque[tuple[int, dict[str, Any], int]] = field(default_factory=deque)
    buffered_chars: int = 0
    next_seq: int = 0
    result: dict[str, Any] | None = None
    subscribers: int = 0
    detached_at: float = field(default_factory=time.monotonic)
    launched: bool = False
    cancel_requested: asyncio.Event = field(default_factory=asyncio.Event)
    wakeup: asyncio.Event = field(default_factory=asyncio.Event)
    task: asyncio.Future[None] | None = None
    # The running process's captures, for /execute/log while the job runs.
    stdout_capture: "_CapturedOutput | None" = None
    stderr_capture: "_CapturedOutput | None" = None

    def append(self, event: dict[str, Any]) -> None:
        seq = self.next_seq
        self.next_seq += 1
        event = {**event, "seq": seq}
        payload = event.get("payload")
        text = payload.get("text") if isinstance(payload, dict) else None
        chars = (len(text) if isinstance(text, str) else 0) + 64
        self.events.append((seq, event, chars))
        self.buffered_chars += chars
        while len(self.events) > 1 and (
            self.buffered_chars > JOB_REPLAY_BUFFER_CHARS
            or len(self.events) > JOB_REPLAY_BUFFER_EVENTS
        ):
            _, _, dropped = self.events.popleft()
            self.buffered_chars -= dropped
        self._wake()

    def finish(self, result: dict[str, Any]) -> None:
        self.result = result
        self.state = "done"
        self._wake()

    def orphaned(self) -> bool:
        return (
            self.subscribers == 0
            and time.monotonic() - self.detached_at > _orphan_grace_seconds()
        )

    def _wake(self) -> None:
        # Broadcast: every subscriber waits on the event it captured before reading.
        previous, self.wakeup = self.wakeup, asyncio.Event()
        previous.set()


def _orphan_grace_seconds() -> float:
    return max(
        1.0,
        float(
            os.environ.get(
                "WORKSPACE_EXECUTOR_ORPHAN_GRACE_SECONDS",
                str(DEFAULT_ORPHAN_GRACE_SECONDS),
            )
        ),
    )


def _request_fingerprint(request: ExecuteRequest) -> str:
    material = json.dumps(
        [
            request.command,
            _normalize_cwd(request.cwd),
            request.timeout_seconds,
            request.network,
            request.stdin,
        ]
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


app = FastAPI(title="Agent workspace executor", docs_url=None, redoc_url=None)
_workspace_locks: dict[str, _WorkspaceLocks] = {}
# Live (queued or running) jobs by (workspace_key, execution_id).
_jobs: dict[tuple[str, str], _Job] = {}
_last_state_gc_at = 0.0
_workspace_locks_guard = asyncio.Lock()
_concurrency = asyncio.Semaphore(
    max(1, int(os.environ.get("WORKSPACE_EXECUTOR_MAX_CONCURRENT", "4")))
)
_self_test_error: str | None = None
_live_health_lock = asyncio.Lock()
_last_live_health_check_at = 0.0
_last_live_health_error: str | None = None
# Paths served without the bearer token.
_PUBLIC_PATHS = frozenset({"/healthz"})


@app.middleware("http")
async def _authorize_before_body(request: Request, call_next: Any) -> Any:
    """Check the bearer token before FastAPI reads or validates the request body (P5), so an
    unauthenticated caller gets 401 (or 503 when no token is configured) and never a 422
    that describes the body schema. The handlers' own ``_authorize`` calls stay as a second
    check."""
    if request.url.path not in _PUBLIC_PATHS:
        try:
            _authorize(request.headers.get("authorization"))
        except HTTPException as exc:
            return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})
    return await call_next(request)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    missing = [
        name for name in ("bwrap", "bash", "prlimit") if shutil.which(name) is None
    ]
    if missing:
        raise HTTPException(
            status_code=503,
            detail=f"Missing executor commands: {', '.join(missing)}",
        )
    if _self_test_error is not None:
        raise HTTPException(status_code=503, detail=_self_test_error)
    live_error = await _live_sandbox_health_error()
    if live_error is not None:
        raise HTTPException(status_code=503, detail=live_error)
    return {"status": "ok"}


async def _live_sandbox_health_error() -> str | None:
    global _last_live_health_check_at, _last_live_health_error

    zombie_count = _orphaned_zombie_count()
    max_zombies = max(
        0,
        int(
            os.environ.get(
                "WORKSPACE_EXECUTOR_MAX_ORPHAN_ZOMBIES",
                str(DEFAULT_MAX_ORPHAN_ZOMBIES),
            )
        ),
    )
    if zombie_count > max_zombies:
        return (
            "Workspace sandbox reaper is unhealthy: "
            f"found {zombie_count} orphaned zombie processes "
            f"(maximum {max_zombies})."
        )

    interval_seconds = max(
        1.0,
        float(
            os.environ.get(
                "WORKSPACE_EXECUTOR_LIVE_HEALTH_INTERVAL_SECONDS",
                str(DEFAULT_LIVE_HEALTH_INTERVAL_SECONDS),
            )
        ),
    )
    now = time.monotonic()
    if now - _last_live_health_check_at < interval_seconds:
        return _last_live_health_error

    async with _live_health_lock:
        now = time.monotonic()
        if now - _last_live_health_check_at < interval_seconds:
            return _last_live_health_error
        _last_live_health_error = await asyncio.to_thread(_sandbox_self_test)
        _last_live_health_check_at = time.monotonic()
        return _last_live_health_error


def _orphaned_zombie_count(proc_root: Path = Path("/proc")) -> int:
    if not proc_root.is_dir():
        return 0

    count = 0
    for status_path in proc_root.glob("[0-9]*/status"):
        try:
            status = status_path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        state = ""
        parent_pid = ""
        for line in status.splitlines():
            if line.startswith("State:"):
                state = line.removeprefix("State:").strip()
            elif line.startswith("PPid:"):
                parent_pid = line.removeprefix("PPid:").strip()
        if state.startswith("Z") and parent_pid == "1":
            count += 1
    return count


@app.post("/execute")
async def execute(
    request: ExecuteRequest,
    authorization: str | None = Header(default=None),
) -> StreamingResponse:
    _authorize(authorization)
    _normalize_cwd(request.cwd)
    if not request.command.strip():
        raise HTTPException(status_code=422, detail="Command is required.")
    if not _SAFE_EXECUTION_ID.fullmatch(request.execution_id):
        raise HTTPException(status_code=422, detail="Invalid execution_id.")

    events = _execute_events(request)
    return StreamingResponse(
        _encode_events(events),
        media_type="application/x-ndjson",
        headers={"Cache-Control": "no-store"},
    )


# --- /files/* ---------------------------------------------------------------
#
# These endpoints run in the executor process, outside Bubblewrap, so every
# path is confined by hand: it is split into components and walked one
# component at a time from an fd on the workspace root with dir_fd and
# O_NOFOLLOW. Symlinks are refused at every level, so nothing the sandboxed
# command creates can redirect the executor outside the workspace. All type
# checks use os.fstat on the opened fd.


class _FileOpError(Exception):
    _STATUS = {
        "not_found": 404,
        "exists": 409,
        "not_empty": 409,
        "match_count": 409,
        "workspace_busy": 409,
        "protected_path": 403,
        "too_large": 413,
        "io_error": 500,
    }

    def __init__(self, code: str, message: str, **extra: Any) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.extra = extra

    @property
    def status_code(self) -> int:
        return self._STATUS.get(self.code, 400)


@app.exception_handler(_FileOpError)
async def _file_op_error_handler(_: Request, err: _FileOpError) -> JSONResponse:
    return JSONResponse(
        status_code=err.status_code,
        content={"ok": False, "code": err.code, "message": err.message, **err.extra},
    )


@app.post("/files/stat")
async def files_stat(
    request: FilePathRequest, authorization: str | None = Header(default=None)
) -> dict[str, Any]:
    _authorize(authorization)
    return await _run_file_op(request.workspace_id, False, _op_stat, request)


@app.post("/files/read")
async def files_read(
    request: FileReadRequest, authorization: str | None = Header(default=None)
) -> dict[str, Any]:
    _authorize(authorization)
    return await _run_file_op(request.workspace_id, False, _op_read, request)


@app.post("/files/list")
async def files_list(
    request: FileListRequest, authorization: str | None = Header(default=None)
) -> dict[str, Any]:
    _authorize(authorization)
    return await _run_file_op(request.workspace_id, False, _op_list, request)


@app.post("/files/write")
async def files_write(
    request: FileWriteRequest, authorization: str | None = Header(default=None)
) -> dict[str, Any]:
    _authorize(authorization)
    _refuse_protected(request.path)
    data = _decode_write_content(request)
    return await _run_file_op(
        request.workspace_id,
        True,
        lambda root, req: _op_write(root, req, data),
        request,
        busy_timeout=request.busy_timeout_seconds,
    )


@app.post("/files/edit")
async def files_edit(
    request: FileEditRequest, authorization: str | None = Header(default=None)
) -> dict[str, Any]:
    _authorize(authorization)
    _refuse_protected(request.path)
    return await _run_file_op(
        request.workspace_id, True, _op_edit, request,
        busy_timeout=request.busy_timeout_seconds,
    )


@app.post("/files/mkdir")
async def files_mkdir(
    request: FileMkdirRequest, authorization: str | None = Header(default=None)
) -> dict[str, Any]:
    _authorize(authorization)
    _refuse_protected(request.path)
    return await _run_file_op(
        request.workspace_id, True, _op_mkdir, request,
        busy_timeout=request.busy_timeout_seconds,
    )


@app.post("/files/delete")
async def files_delete(
    request: FileDeleteRequest, authorization: str | None = Header(default=None)
) -> dict[str, Any]:
    _authorize(authorization)
    _refuse_protected(request.path)
    return await _run_file_op(
        request.workspace_id, True, _op_delete, request,
        busy_timeout=request.busy_timeout_seconds,
    )


@app.post("/files/rename")
async def files_rename(
    request: FileRenameRequest, authorization: str | None = Header(default=None)
) -> dict[str, Any]:
    _authorize(authorization)
    _refuse_protected(request.source, request.destination)
    return await _run_file_op(
        request.workspace_id, True, _op_rename, request,
        busy_timeout=request.busy_timeout_seconds,
    )


def _busy_message(workspace_key: str) -> str:
    states = [job.state for (key, _), job in list(_jobs.items()) if key == workspace_key]
    running, queued = states.count("running"), states.count("queued")
    if running:
        what = "A command is running in the workspace"
        if queued:
            what += f" and {queued} more {'is' if queued == 1 else 'are'} queued"
    elif queued:
        what = f"{queued} command{'s are' if queued > 1 else ' is'} queued in the workspace"
    else:
        what = "Another file change is in progress in the workspace"
    return (
        f"{what}; nothing was written. File changes wait for commands; retry when they "
        "finish."
    )


def _refuse_protected(*paths: str) -> None:
    """Refuse mutations of git's control files (B1).

    Anything under ``.git`` (config, hooks, info/attributes), a root-level
    ``.gitconfig`` and ``.config/git/`` (the sandbox HOME is /workspace, so these are
    git's global config locations) can make a later git call run workspace-chosen
    programs. Compared case-insensitively, because a macOS-hosted
    executor sits on a case-insensitive filesystem.
    """
    for path in paths:
        parts = _split_path(path, allow_root=True)
        if not parts:
            continue
        folded = [part.casefold() for part in parts]
        if (
            folded[0] == ".git"
            or folded == [".gitconfig"]
            or folded[:2] == [".config", "git"]
        ):
            raise _FileOpError(
                "protected_path",
                f"{_display(parts)} is git control data and can't be changed with "
                "file tools. Use the git tools for git operations.",
            )


async def _run_file_op(
    workspace_id: str,
    mutates: bool,
    operation: Callable[[int, _T], dict[str, Any]],
    request: _T,
    *,
    busy_timeout: float | None = None,
) -> dict[str, Any]:
    workspace_key = _workspace_key(workspace_id)
    workspace_dir = _workspace_dir(workspace_key)
    locks = await _workspace_locks_for(workspace_key)

    def run() -> dict[str, Any]:
        workspace_dir.mkdir(mode=WORKSPACE_MODE, parents=True, exist_ok=True)
        try:
            root_fd = os.open(workspace_dir, _DIR_FLAGS)
        except OSError as err:
            raise _FileOpError("io_error", "Workspace is unavailable.") from err
        try:
            return operation(root_fd, request)
        except _FileOpError:
            raise
        except OSError as err:
            raise _os_error(err, "") from err
        finally:
            os.close(root_fd)

    async def locked() -> dict[str, Any]:
        # Lock scheme: see _WorkspaceLocks.
        if mutates:
            if busy_timeout is None or not locks.exec.locked():
                # Uncontended: taken without suspending, so busy_timeout=0 means
                # "only if free right now", not "always busy".
                await locks.exec.acquire()
            else:
                try:
                    await asyncio.wait_for(locks.exec.acquire(), busy_timeout)
                except (asyncio.TimeoutError, TimeoutError):
                    # Nothing was written: the caller gets a definite outcome
                    # instead of a client timeout while the write lands later.
                    raise _FileOpError(
                        "workspace_busy", _busy_message(workspace_key)
                    ) from None
            try:
                async with locks.files.exclusive():
                    return await asyncio.to_thread(run)
            finally:
                locks.exec.release()
        async with locks.files.shared():
            return await asyncio.to_thread(run)

    # If the client disconnects, the worker thread keeps running; shield the
    # task so the locks are released only after the filesystem work is done.
    return await asyncio.shield(asyncio.ensure_future(locked()))


_DIR_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
# O_NONBLOCK: opening a FIFO for its fstat must never block.
_LEAF_FLAGS = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC


def _split_path(path: str, *, allow_root: bool) -> list[str]:
    """Split a workspace-relative path into components, or raise.

    Returns [] for the workspace root ("" or "."). "." components are skipped;
    they never leave the current directory.
    """
    if "\x00" in path:
        raise _FileOpError("invalid_path", "Path contains a NUL byte.")
    if len(path.encode("utf-8", errors="surrogatepass")) > MAX_FILE_PATH_BYTES:
        raise _FileOpError("invalid_path", "Path is too long.")
    if path.startswith("/"):
        raise _FileOpError(
            "outside_workspace",
            "Absolute paths are not allowed; use a path relative to the "
            "workspace root (for /workspace/src/app.py, use src/app.py).",
        )
    parts: list[str] = []
    if path not in ("", "."):
        for part in path.split("/"):
            if part == "":
                raise _FileOpError(
                    "invalid_path", f"Path {path!r} has an empty component."
                )
            if part == "..":
                raise _FileOpError(
                    "outside_workspace", "Path cannot contain '..' components."
                )
            if part != ".":
                parts.append(part)
    if not parts and not allow_root:
        raise _FileOpError(
            "invalid_path", "This operation needs a path below the root."
        )
    return parts


def _display(parts: list[str]) -> str:
    return "/".join(parts) or "."


def _os_error(err: OSError, shown: str) -> _FileOpError:
    where = f"{shown}: " if shown else ""
    reason = err.strerror or err.__class__.__name__
    code = {
        errno.ENOENT: "not_found",
        errno.EEXIST: "exists",
        errno.ENOTEMPTY: "not_empty",
        errno.ELOOP: "is_symlink",
        errno.ENOTDIR: "not_a_directory",
        errno.EISDIR: "not_regular_file",
        errno.ENAMETOOLONG: "invalid_path",
        errno.EINVAL: "invalid_path",
    }.get(err.errno or 0, "io_error")
    return _FileOpError(code, f"{where}{reason}.")


def _classify_open_failure(
    dir_fd: int, name: str, shown: str, err: OSError, *, want_dir: bool
) -> _FileOpError:
    """Explain why an O_NOFOLLOW open of one component failed.

    Uses fstatat(AT_SYMLINK_NOFOLLOW) on a single name relative to an fd we
    already hold, never a path string, and only to pick the error code.
    """
    if err.errno == errno.ENOENT:
        return _FileOpError("not_found", f"{shown} does not exist.")
    try:
        st = os.stat(name, dir_fd=dir_fd, follow_symlinks=False)
    except FileNotFoundError:
        return _FileOpError("not_found", f"{shown} does not exist.")
    except OSError:
        return _os_error(err, shown)
    if stat_module.S_ISLNK(st.st_mode):
        return _FileOpError(
            "is_symlink", f"{shown} is a symbolic link; symlinks are not followed."
        )
    if want_dir and not stat_module.S_ISDIR(st.st_mode):
        return _FileOpError("not_a_directory", f"{shown} is not a directory.")
    if not stat_module.S_ISREG(st.st_mode) and not stat_module.S_ISDIR(st.st_mode):
        return _FileOpError(
            "not_regular_file", f"{shown} is not a regular file or directory."
        )
    return _os_error(err, shown)


def _walk_dirs(
    root_fd: int, parts: list[str], stack: ExitStack, *, create: bool = False
) -> int:
    """Open each component as a real directory (no symlinks) and return the
    fd of the last one. The caller's ExitStack closes every fd."""
    fd = root_fd
    for index, name in enumerate(parts):
        shown = _display(parts[: index + 1])
        try:
            next_fd = os.open(name, _DIR_FLAGS, dir_fd=fd)
        except FileNotFoundError as err:
            if not create:
                raise _classify_open_failure(fd, name, shown, err, want_dir=True)
            with suppress(FileExistsError):
                os.mkdir(name, dir_fd=fd)
            try:
                next_fd = os.open(name, _DIR_FLAGS, dir_fd=fd)
            except OSError as retry_err:
                raise _classify_open_failure(
                    fd, name, shown, retry_err, want_dir=True
                ) from retry_err
        except OSError as err:
            raise _classify_open_failure(fd, name, shown, err, want_dir=True) from err
        stack.callback(os.close, next_fd)
        fd = next_fd
    return fd


def _open_leaf(
    dir_fd: int, name: str, shown: str, stack: ExitStack
) -> tuple[int, os.stat_result]:
    try:
        fd = os.open(name, _LEAF_FLAGS, dir_fd=dir_fd)
    except OSError as err:
        raise _classify_open_failure(dir_fd, name, shown, err, want_dir=False) from err
    stack.callback(os.close, fd)
    return fd, os.fstat(fd)


def _open_leaf_if_exists(
    dir_fd: int, name: str, shown: str, stack: ExitStack
) -> tuple[int, os.stat_result] | None:
    try:
        return _open_leaf(dir_fd, name, shown, stack)
    except _FileOpError as err:
        if err.code == "not_found":
            return None
        raise


def _require_regular(st: os.stat_result, shown: str, *, for_write: bool) -> None:
    if not stat_module.S_ISREG(st.st_mode):
        kind = (
            "a directory" if stat_module.S_ISDIR(st.st_mode) else "not a regular file"
        )
        raise _FileOpError("not_regular_file", f"{shown} is {kind}.")
    if for_write and st.st_nlink > 1:
        # Writes only. Package managers (pnpm, uv link-mode=hardlink) fill node_modules and
        # .venv with multiply-linked files, so refusing reads would break normal work. A
        # sandboxed process can only link files it can already reach, so reads gain little.
        raise _FileOpError(
            "outside_workspace",
            f"{shown} has {st.st_nlink} hard links; refusing to modify a file "
            "that may be shared with a path outside the workspace.",
        )


def _file_type(mode: int) -> str:
    if stat_module.S_ISREG(mode):
        return "file"
    if stat_module.S_ISDIR(mode):
        return "directory"
    if stat_module.S_ISLNK(mode):
        return "symlink"
    return "other"


def _stat_payload(shown: str, st: os.stat_result) -> dict[str, Any]:
    return {
        "path": shown,
        "type": _file_type(st.st_mode),
        "size": st.st_size,
        "mode": f"{stat_module.S_IMODE(st.st_mode):04o}",
        "mtime": st.st_mtime,
        "nlink": st.st_nlink,
    }


def _read_exact(fd: int, offset: int, length: int) -> bytes:
    chunks: list[bytes] = []
    remaining = length
    while remaining > 0:
        chunk = os.pread(fd, min(remaining, 256 * 1024), offset)
        if not chunk:
            break
        chunks.append(chunk)
        offset += len(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _encode_content(data: bytes) -> tuple[str, str]:
    try:
        return data.decode("utf-8"), "utf-8"
    except UnicodeDecodeError as exc:
        # A range read can end inside a multibyte character. If the only problem is a
        # truncated character in the last 3 bytes, the text is still text.
        if exc.start >= len(data) - 3 and exc.reason == "unexpected end of data":
            return data[: exc.start].decode("utf-8"), "utf-8"
        return base64.b64encode(data).decode("ascii"), "base64"


def _decode_write_content(request: FileWriteRequest) -> bytes:
    too_large = _FileOpError(
        "too_large",
        f"Writes are capped at {MAX_FILE_WRITE_BYTES} bytes per call.",
        limit=MAX_FILE_WRITE_BYTES,
    )
    if request.encoding == "base64":
        if len(request.content) // 4 * 3 > MAX_FILE_WRITE_BYTES + 3:
            raise too_large
        try:
            data = base64.b64decode(request.content, validate=True)
        except (binascii.Error, ValueError) as err:
            raise _FileOpError(
                "invalid_request", "content is not valid base64."
            ) from err
    else:
        if len(request.content) > MAX_FILE_WRITE_BYTES:
            raise too_large
        data = request.content.encode("utf-8", errors="surrogatepass")
    if len(data) > MAX_FILE_WRITE_BYTES:
        raise too_large
    return data


def _atomic_replace(dir_fd: int, name: str, data: bytes, mode: int) -> None:
    """Write to a fresh temp file in the same directory, then rename it over
    the target. The rename replaces the directory entry, so it never writes
    through an existing inode."""
    temp_name = f".executor-{secrets.token_hex(8)}.tmp"
    fd = os.open(
        temp_name,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
        0o600,
        dir_fd=dir_fd,
    )
    try:
        try:
            view = memoryview(data)
            while view:
                written = os.write(fd, view)
                view = view[written:]
            os.fchmod(fd, mode)
        finally:
            os.close(fd)
        os.replace(temp_name, name, src_dir_fd=dir_fd, dst_dir_fd=dir_fd)
    except BaseException:
        with suppress(OSError):
            os.unlink(temp_name, dir_fd=dir_fd)
        raise


def _op_stat(root_fd: int, request: FilePathRequest) -> dict[str, Any]:
    parts = _split_path(request.path, allow_root=True)
    if not parts:
        return {"ok": True, **_stat_payload(".", os.fstat(root_fd))}
    with ExitStack() as stack:
        parent = _walk_dirs(root_fd, parts[:-1], stack)
        _, st = _open_leaf(parent, parts[-1], _display(parts), stack)
        return {"ok": True, **_stat_payload(_display(parts), st)}


def _op_read(root_fd: int, request: FileReadRequest) -> dict[str, Any]:
    parts = _split_path(request.path, allow_root=False)
    shown = _display(parts)
    with ExitStack() as stack:
        parent = _walk_dirs(root_fd, parts[:-1], stack)
        fd, st = _open_leaf(parent, parts[-1], shown, stack)
        _require_regular(st, shown, for_write=False)
        size = st.st_size
        offset = min(request.offset, size)
        # Clamp to what the file holds before applying the cap, so a generous length on a
        # small file is fine.
        length = size - offset if request.length is None else min(request.length, size - offset)
        if length > MAX_FILE_READ_BYTES:
            raise _FileOpError(
                "too_large",
                f"{shown} is {size} bytes and reads are capped at "
                f"{MAX_FILE_READ_BYTES} bytes per call. Read a range with "
                "offset/length, or use run_command (for example head, sed -n, "
                "or rg) for large files.",
                size=size,
                limit=MAX_FILE_READ_BYTES,
            )
        data = _read_exact(fd, offset, length)
        # A model-chosen offset can land inside a multibyte character: skip up to 3 leading
        # continuation bytes and report the adjusted offset.
        lead = 0
        while offset > 0 and lead < 3 and lead < len(data) and 0x80 <= data[lead] <= 0xBF:
            lead += 1
        if lead:
            offset += lead
            data = data[lead:]
        content, encoding = _encode_content(data)
        # The whole range sat inside one character: read through to its end, a byte at a time.
        while encoding == "utf-8" and data and not content and len(data) < 4 and offset + len(data) < size:
            data += _read_exact(fd, offset + len(data), 1)
            content, encoding = _encode_content(data)
        # Bytes actually returned (a trailing partial character may have been held back).
        returned = len(content.encode("utf-8")) if encoding == "utf-8" else len(data)
        return {
            "ok": True,
            "path": shown,
            "size": size,
            "offset": offset,
            "length": returned,
            "eof": offset + returned >= size,
            "encoding": encoding,
            "content": content,
        }


def _op_list(root_fd: int, request: FileListRequest) -> dict[str, Any]:
    parts = _split_path(request.path, allow_root=True)
    entries: list[dict[str, Any]] = []
    truncated = False
    with ExitStack() as stack:
        dir_fd = _walk_dirs(root_fd, parts, stack)
        # scandir(fd) stats entries with fstatat(dir_fd, name, NOFOLLOW).
        with os.scandir(dir_fd) as iterator:
            for entry in iterator:
                if len(entries) >= MAX_LIST_ENTRIES:
                    truncated = True
                    break
                try:
                    st = entry.stat(follow_symlinks=False)
                except OSError:
                    continue
                kind = _file_type(st.st_mode)
                entries.append(
                    {
                        "name": entry.name,
                        "type": kind,
                        "size": st.st_size if kind == "file" else None,
                    }
                )
    entries.sort(key=lambda item: item["name"])
    return {
        "ok": True,
        "path": _display(parts),
        "entries": entries,
        "truncated": truncated,
        "limit": MAX_LIST_ENTRIES,
    }


def _op_write(root_fd: int, request: FileWriteRequest, data: bytes) -> dict[str, Any]:
    parts = _split_path(request.path, allow_root=False)
    shown = _display(parts)
    with ExitStack() as stack:
        parent = _walk_dirs(root_fd, parts[:-1], stack, create=request.create_parents)
        existing = _open_leaf_if_exists(parent, parts[-1], shown, stack)
        mode = 0o644
        if existing is not None:
            _, st = existing
            _require_regular(st, shown, for_write=True)
            if not request.overwrite:
                raise _FileOpError("exists", f"{shown} already exists.")
            mode = stat_module.S_IMODE(st.st_mode) & 0o777
        _atomic_replace(parent, parts[-1], data, mode)
    return {
        "ok": True,
        "path": shown,
        "bytes_written": len(data),
        "created": existing is None,
    }


def _count_matches(data: bytes, needle: bytes, *, limit: int = 2) -> int:
    """Count occurrences, including overlapping ones, stopping at ``limit`` (an edit only
    needs to know 0, 1 or "more than one")."""
    count = 0
    start = data.find(needle)
    while start != -1 and count < limit:
        count += 1
        start = data.find(needle, start + 1)
    return count


def _op_edit(root_fd: int, request: FileEditRequest) -> dict[str, Any]:
    parts = _split_path(request.path, allow_root=False)
    shown = _display(parts)
    old = request.old_string.encode("utf-8", errors="surrogatepass")
    new = request.new_string.encode("utf-8", errors="surrogatepass")
    with ExitStack() as stack:
        parent = _walk_dirs(root_fd, parts[:-1], stack)
        fd, st = _open_leaf(parent, parts[-1], shown, stack)
        _require_regular(st, shown, for_write=True)
        if st.st_size > MAX_FILE_WRITE_BYTES:
            raise _FileOpError(
                "too_large",
                f"{shown} is {st.st_size} bytes; edits are capped at "
                f"{MAX_FILE_WRITE_BYTES} bytes. Use run_command instead.",
                size=st.st_size,
                limit=MAX_FILE_WRITE_BYTES,
            )
        data = _read_exact(fd, 0, st.st_size)
        matches = _count_matches(data, old)
        if matches != 1:
            raise _FileOpError(
                "match_count",
                f"old_string must match exactly once in {shown}; it matched "
                + ("0 times." if matches == 0 else "more than once.")
                + (
                    " Include more surrounding context to make it unique."
                    if matches > 1
                    else ""
                ),
                match_count=matches,
            )
        updated = data.replace(old, new, 1)
        if len(updated) > MAX_FILE_WRITE_BYTES:
            raise _FileOpError(
                "too_large",
                f"The edited file would be {len(updated)} bytes; writes are "
                f"capped at {MAX_FILE_WRITE_BYTES} bytes.",
                limit=MAX_FILE_WRITE_BYTES,
            )
        _atomic_replace(
            parent, parts[-1], updated, stat_module.S_IMODE(st.st_mode) & 0o777
        )
    return {"ok": True, "path": shown, "bytes_written": len(updated)}


def _op_mkdir(root_fd: int, request: FileMkdirRequest) -> dict[str, Any]:
    parts = _split_path(request.path, allow_root=False)
    shown = _display(parts)
    with ExitStack() as stack:
        parent = _walk_dirs(root_fd, parts[:-1], stack, create=request.parents)
        try:
            os.mkdir(parts[-1], dir_fd=parent)
        except FileExistsError as err:
            if not request.parents:
                raise _FileOpError("exists", f"{shown} already exists.") from err
            # mkdir -p: fine if it is already a real directory.
            _walk_dirs(parent, parts[-1:], stack)
            return {"ok": True, "path": shown, "created": False}
    return {"ok": True, "path": shown, "created": True}


def _op_delete(root_fd: int, request: FileDeleteRequest) -> dict[str, Any]:
    parts = _split_path(request.path, allow_root=False)
    shown = _display(parts)
    with ExitStack() as stack:
        parent = _walk_dirs(root_fd, parts[:-1], stack)
        _, st = _open_leaf(parent, parts[-1], shown, stack)
        if stat_module.S_ISDIR(st.st_mode):
            try:
                os.rmdir(parts[-1], dir_fd=parent)
            except OSError as err:
                if err.errno in (errno.ENOTEMPTY, errno.EEXIST):
                    raise _FileOpError(
                        "not_empty", f"{shown} is a directory that is not empty."
                    ) from err
                raise _os_error(err, shown) from err
            return {"ok": True, "path": shown, "type": "directory"}
        if not stat_module.S_ISREG(st.st_mode):
            raise _FileOpError(
                "not_regular_file",
                f"{shown} is not a regular file or directory.",
            )
        os.unlink(parts[-1], dir_fd=parent)
    return {"ok": True, "path": shown, "type": "file"}


def _op_rename(root_fd: int, request: FileRenameRequest) -> dict[str, Any]:
    source = _split_path(request.source, allow_root=False)
    destination = _split_path(request.destination, allow_root=False)
    source_shown = _display(source)
    destination_shown = _display(destination)
    with ExitStack() as stack:
        source_parent = _walk_dirs(root_fd, source[:-1], stack)
        _, source_st = _open_leaf(source_parent, source[-1], source_shown, stack)
        if not (
            stat_module.S_ISREG(source_st.st_mode)
            or stat_module.S_ISDIR(source_st.st_mode)
        ):
            raise _FileOpError(
                "not_regular_file",
                f"{source_shown} is not a regular file or directory.",
            )
        destination_parent = _walk_dirs(
            root_fd, destination[:-1], stack, create=request.create_parents
        )
        existing = _open_leaf_if_exists(
            destination_parent, destination[-1], destination_shown, stack
        )
        if existing is not None:
            _, destination_st = existing
            replaceable = (
                request.overwrite
                and stat_module.S_ISREG(destination_st.st_mode)
                and stat_module.S_ISREG(source_st.st_mode)
            )
            if not replaceable:
                raise _FileOpError("exists", f"{destination_shown} already exists.")
        try:
            os.rename(
                source[-1],
                destination[-1],
                src_dir_fd=source_parent,
                dst_dir_fd=destination_parent,
            )
        except OSError as err:
            raise _os_error(err, f"{source_shown} -> {destination_shown}") from err
    return {
        "ok": True,
        "source": source_shown,
        "destination": destination_shown,
    }


def main() -> None:
    global _self_test_error
    _prepare_executor_root()
    global _last_state_gc_at
    with suppress(OSError):
        _sweep_state()
    _last_state_gc_at = time.monotonic()
    _self_test_error = (
        _credential_boundary_self_test() or _sandbox_self_test() or _git_version_self_test()
    )
    if _self_test_error is not None:
        print(_self_test_error, flush=True)
    uvicorn.run(
        app,
        host=os.environ.get("WORKSPACE_EXECUTOR_HOST", "0.0.0.0"),
        port=int(os.environ.get("WORKSPACE_EXECUTOR_PORT", "8082")),
        log_level=os.environ.get("WORKSPACE_EXECUTOR_LOG_LEVEL", "info"),
    )


def _execute_events(request: ExecuteRequest) -> AsyncIterator[dict[str, Any]]:
    """Open (or reattach to) the job for ``request.execution_id`` and stream its events.

    Not a generator itself: the lookup and job creation run synchronously, with no await
    between "is there a live job / cached result?" and "register a new job", so two
    concurrent requests with one execution_id can never start two processes. A mismatched
    request for a live execution_id raises HTTPException(409) here, before streaming.
    """
    _maybe_schedule_state_gc()
    workspace_key = _workspace_key(request.workspace_id)
    key = (workspace_key, request.execution_id)
    job = _jobs.get(key)
    if job is not None:
        if job.fingerprint != _request_fingerprint(request):
            raise HTTPException(
                status_code=409,
                detail="execution_id is already running a different command.",
            )
        return _subscribe(job, request, attached=True)

    state_path = _state_path(workspace_key, request.execution_id)
    cached = _load_cached_result(state_path)
    if cached is not None:
        stored = cached.pop("fingerprint", None)
        if stored is not None and stored != _request_fingerprint(request):
            raise HTTPException(
                status_code=409,
                detail="execution_id already ran a different command.",
            )
        cached["cached"] = True
        # A client reattaching after the job finished (a retried activity whose worker died
        # mid-run) still needs the output it missed, so the job's replay buffer is kept
        # with its result.
        return _replay_cached(
            request, workspace_key, cached, _load_cached_events(state_path)
        )
    if _started_marker(state_path).exists():
        # The process started under an executor that has since restarted: the job and its
        # result are gone. Running it again would run the command twice.
        return _replay_cached(
            request,
            workspace_key,
            {
                "workspace": workspace_key,
                "exit_code": None,
                "stdout": "",
                "stderr": "",
                "timed_out": False,
                "files_changed": {"created": [], "modified": [], "deleted": []},
                "error": (
                    "The sandbox restarted while this command was running, so its "
                    "result was lost. It may have partly run; check the workspace "
                    "before running it again."
                ),
                "type": "WorkspaceJobLost",
            },
        )

    job = _Job(
        workspace_key=workspace_key,
        execution_id=request.execution_id,
        fingerprint=_request_fingerprint(request),
    )
    _jobs[key] = job
    job.task = asyncio.ensure_future(_run_job(job, request, state_path))
    job.task.add_done_callback(lambda _: _settle_job(job, request))
    return _subscribe(job, request, attached=False)


def _settle_job(job: "_Job", request: ExecuteRequest) -> None:
    """Finish a job whose task never ran its own cleanup (cancelled before it started)."""
    if job.result is None:
        _jobs.pop((job.workspace_key, job.execution_id), None)
        job.finish(_stopped_payload(request, job, "WorkspaceShellCancelled"))


async def _replay_cached(
    request: ExecuteRequest,
    workspace_key: str,
    result: dict[str, Any],
    events: list[dict[str, Any]] | None = None,
) -> AsyncIterator[dict[str, Any]]:
    yield {
        "kind": "start",
        "payload": {
            **_start_payload(request, workspace_key),
            "execution_id": request.execution_id,
            "attached": True,
            "state": "done",
            "cached": True,
        },
    }
    events = events or []
    first = events[0].get("seq", 0) if events else 0
    if isinstance(first, int) and first > 0:
        yield _dropped_event(first, 0)
    for event in events:
        yield event
    yield {"kind": "result", "payload": result}


def _dropped_event(first: int, cursor: int) -> dict[str, Any]:
    return {
        "kind": "dropped",
        "payload": {
            "dropped_events": first - cursor,
            # The first seq still available: a client that already has everything
            # before it can ignore this marker.
            "next_seq": first,
            "message": (
                f"[{first - cursor} earlier output chunk(s) dropped from the replay buffer]"
            ),
        },
    }


async def _subscribe(
    job: "_Job", request: ExecuteRequest, *, attached: bool
) -> AsyncIterator[dict[str, Any]]:
    """Replay the job's buffered events, then follow it live until its result.

    A subscriber that disconnects (worker crash, cancelled activity) leaves the job
    running; a later request with the same execution_id reattaches. A job with no
    subscriber for the orphan grace period is stopped (see _run_job).
    """
    job.subscribers += 1
    try:
        yield {
            "kind": "start",
            "payload": {
                **_start_payload(request, job.workspace_key),
                "execution_id": job.execution_id,
                "attached": attached,
                "state": job.state,
                "next_seq": job.next_seq,
            },
        }
        cursor = 0
        while True:
            wakeup = job.wakeup
            first = job.events[0][0] if job.events else job.next_seq
            if cursor < first:
                yield _dropped_event(first, cursor)
                cursor = first
            for seq, event, _ in list(job.events):
                if seq >= cursor:
                    yield event
                    cursor = seq + 1
            if job.result is not None and cursor >= job.next_seq:
                yield {"kind": "result", "payload": job.result}
                return
            await wakeup.wait()
    finally:
        job.subscribers -= 1
        if job.subscribers == 0:
            job.detached_at = time.monotonic()


async def _run_job(job: "_Job", request: ExecuteRequest, state_path: Path) -> None:
    result: dict[str, Any] | None = None
    store = False
    try:
        locks = await _workspace_locks_for(job.workspace_key)
        # See _WorkspaceLocks: commands exclude file mutations, not file reads.
        async with locks.exec, _concurrency:
            if job.cancel_requested.is_set():
                result = _stopped_payload(request, job, "WorkspaceShellCancelled")
                store = True
            elif job.orphaned():
                result = _stopped_payload(request, job, "WorkspaceShellAbandoned")
                store = True
            else:
                job.state = "running"
                result, store = await _run_process(job, request, state_path)
    except asyncio.CancelledError:
        result = _stopped_payload(request, job, "WorkspaceShellCancelled")
        store = job.launched
        raise
    except Exception as err:  # noqa: BLE001 - reported to every subscriber
        result = {
            "error": f"Workspace sandbox failed to start: {err}",
            "type": "WorkspaceSandboxError",
            "workspace": job.workspace_key,
        }
        store = job.launched
    finally:
        if result is None:
            result = _stopped_payload(request, job, "WorkspaceSandboxError")
        if store:
            with suppress(OSError):
                # Events first: a request that finds the result also finds its output.
                _store_cached_events(state_path, [event for _, event, _ in job.events])
                _store_cached_result(
                    state_path, {**result, "fingerprint": job.fingerprint}
                )
        # Cache first, then unregister: a request that misses _jobs finds the cache.
        _jobs.pop((job.workspace_key, job.execution_id), None)
        job.finish(result)


async def _run_process(
    job: "_Job", request: ExecuteRequest, state_path: Path
) -> tuple[dict[str, Any], bool]:
    workspace_dir = _workspace_dir(job.workspace_key)
    workspace_dir.mkdir(mode=WORKSPACE_MODE, parents=True, exist_ok=True)
    workspace_dir.chmod(WORKSPACE_MODE)
    before = _workspace_snapshot(workspace_dir)

    command = _sandbox_command(request, workspace_dir)
    started_at = time.monotonic()
    process: asyncio.subprocess.Process | None = None
    stdout = _CapturedOutput(MAX_CAPTURE_CHARS)
    stderr = _CapturedOutput(MAX_CAPTURE_CHARS)
    job.stdout_capture, job.stderr_capture = stdout, stderr
    stream_budget = _StreamBudget(MAX_STREAM_CHARS)
    timed_out = False
    stop_type: str | None = None
    cancel_wait = asyncio.ensure_future(job.cancel_requested.wait())

    try:
        process = await asyncio.create_subprocess_exec(
            *command,
            stdin=(
                asyncio.subprocess.PIPE
                if request.stdin is not None
                else asyncio.subprocess.DEVNULL
            ),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=_launcher_env(),
            start_new_session=True,
        )
        job.launched = True
        _mark_started(state_path)
        if request.stdin is not None and process.stdin is not None:
            process.stdin.write(request.stdin.encode("utf-8"))
            await process.stdin.drain()
            process.stdin.close()

        readers = [
            asyncio.create_task(
                _read_output(process.stdout, "stdout", stdout, stream_budget, job)
            ),
            asyncio.create_task(
                _read_output(process.stderr, "stderr", stderr, stream_budget, job)
            ),
        ]
        wait_task = asyncio.create_task(process.wait())
        deadline = started_at + request.timeout_seconds
        next_progress = started_at + PROGRESS_INTERVAL_SECONDS

        while not wait_task.done():
            now = time.monotonic()
            if now >= deadline:
                timed_out = True
                await _terminate_process(process)
                break
            if job.cancel_requested.is_set():
                stop_type = "WorkspaceShellCancelled"
                await _terminate_process(process)
                break
            if job.orphaned():
                stop_type = "WorkspaceShellAbandoned"
                await _terminate_process(process)
                break
            await asyncio.wait(
                {wait_task, cancel_wait},
                timeout=max(0.05, min(1.0, deadline - now, next_progress - now)),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if time.monotonic() >= next_progress and not wait_task.done():
                job.append(
                    {
                        "kind": "progress",
                        "payload": {
                            "elapsed_seconds": round(time.monotonic() - started_at, 3),
                            "stdout_chars": stdout.original_chars,
                            "stderr_chars": stderr.original_chars,
                        },
                    }
                )
                next_progress = time.monotonic() + PROGRESS_INTERVAL_SECONDS

        if not wait_task.done():
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(wait_task, timeout=TERMINATE_GRACE_SECONDS)
        await asyncio.gather(*readers, return_exceptions=True)

        after = _workspace_snapshot(workspace_dir)
        result = _result_payload(
            request=request,
            workspace_key=job.workspace_key,
            exit_code=process.returncode,
            stdout=stdout,
            stderr=stderr,
            stream_budget=stream_budget,
            timed_out=timed_out,
            elapsed_seconds=time.monotonic() - started_at,
            before=before,
            after=after,
            git_branch=_git_branch(workspace_dir),
        )
        if stop_type is not None:
            result.update(_stop_error(stop_type))
        return result, True
    except BaseException:
        if process is not None:
            await _terminate_process(process)
        raise
    finally:
        cancel_wait.cancel()


def _stop_error(stop_type: str) -> dict[str, str]:
    grace = _orphan_grace_seconds()
    messages = {
        "WorkspaceShellCancelled": "Command was cancelled.",
        "WorkspaceShellAbandoned": (
            f"Command was stopped: no client was attached for {grace:g} seconds."
        ),
        "WorkspaceSandboxError": "Workspace sandbox stopped unexpectedly.",
    }
    return {"error": messages[stop_type], "type": stop_type}


def _stopped_payload(
    request: ExecuteRequest, job: "_Job", stop_type: str
) -> dict[str, Any]:
    return {
        "workspace": job.workspace_key,
        "cwd": _normalize_cwd(request.cwd),
        "command": request.command,
        "network": request.network,
        "exit_code": None,
        "stdout": "",
        "stderr": "",
        "timed_out": False,
        "elapsed_seconds": 0.0,
        "files_changed": {"created": [], "modified": [], "deleted": []},
        **_stop_error(stop_type),
    }


@app.post("/execute/log")
async def execute_log(
    request: ExecuteLogRequest,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    """Read a job's stored output by execution_id (PRD S7), paged.

    Scoped to the workspace: state lives under the workspace's own key, and live jobs are
    keyed by (workspace, execution_id), so an id from another workspace is a 404, the
    same as an unknown or expired one.

    - ``stream``: ``stdout`` and ``stderr`` are the captured streams, each up to
      MAX_CAPTURE_CHARS. ``both`` is the two interleaved in arrival order when the replay
      buffer still holds the whole run (``interleaved: true``); otherwise stdout followed
      by stderr under a ``[stderr]`` header.
    - ``offset``/``limit`` count characters. A response carries at most ``limit``
      characters and at most 256 KB of UTF-8. ``next_offset`` is where to continue; it is
      null once everything is read and the job has finished. While the job runs, read
      again from ``next_offset`` to follow it.
    """
    _authorize(authorization)
    if not _SAFE_EXECUTION_ID.fullmatch(request.execution_id):
        raise HTTPException(status_code=422, detail="Invalid execution_id.")
    workspace_key = _workspace_key(request.workspace_id)
    job = _jobs.get((workspace_key, request.execution_id))
    if job is not None and job.result is None:
        stdout = job.stdout_capture.text if job.stdout_capture else ""
        stderr = job.stderr_capture.text if job.stderr_capture else ""
        truncated = bool(
            (job.stdout_capture and job.stdout_capture.truncated)
            or (job.stderr_capture and job.stderr_capture.truncated)
        )
        events = [event for _, event, _ in list(job.events)]
        result: dict[str, Any] = {"exit_code": None}
        finished = False
    else:
        state_path = _state_path(workspace_key, request.execution_id)
        cached = _load_cached_result(state_path)
        if cached is None:
            raise HTTPException(
                status_code=404,
                detail={
                    "code": "not_found",
                    "message": "No output is retained for this execution in this workspace.",
                },
            )
        result = cached
        stdout = str(cached.get("stdout") or "")
        stderr = str(cached.get("stderr") or "")
        truncated = bool(cached.get("stdout_truncated") or cached.get("stderr_truncated"))
        events = _load_cached_events(state_path)
        finished = True

    interleaved = False
    if request.stream == "stdout":
        text = stdout
    elif request.stream == "stderr":
        text = stderr
    else:
        complete = (
            not truncated
            and (not events or events[0].get("seq") == 0)
            and not result.get("stream_truncated")
        )
        streamed = [e for e in events if e.get("kind") in ("stdout", "stderr")]
        if complete and (streamed or not (stdout or stderr)):
            text = "".join(str(e.get("payload", {}).get("text", "")) for e in streamed)
            interleaved = True
        else:
            text = stdout + (f"\n[stderr]\n{stderr}" if stderr else "")

    chunk = text[request.offset : request.offset + request.limit]
    encoded = chunk.encode("utf-8", errors="replace")
    if len(encoded) > MAX_LOG_RESPONSE_BYTES:
        chunk = encoded[:MAX_LOG_RESPONSE_BYTES].decode("utf-8", errors="ignore")
    end = min(request.offset, len(text)) + len(chunk)
    more = end < len(text)
    return {
        "ok": True,
        "execution_id": request.execution_id,
        "stream": request.stream,
        "offset": request.offset,
        "text": chunk,
        "total_chars": len(text),
        "next_offset": end if (more or not finished) else None,
        "finished": finished,
        "exit_code": result.get("exit_code"),
        "timed_out": bool(result.get("timed_out")),
        "error_type": result.get("type"),
        "truncated_at_capture": truncated,
        "interleaved": interleaved,
    }


@app.post("/execute/cancel")
async def execute_cancel(
    request: ExecuteCancelRequest,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    """Stop a running or queued job. Idempotent; unknown or finished ids are a no-op."""
    _authorize(authorization)
    job = _jobs.get((_workspace_key(request.workspace_id), request.execution_id))
    if job is None:
        return {"ok": True, "state": "not_running"}
    job.cancel_requested.set()
    if job.state == "queued" and job.task is not None:
        # Waiting on the workspace lock: nothing to terminate, just stop waiting.
        job.task.cancel()
    return {"ok": True, "state": "cancelling"}


async def _encode_events(events: AsyncIterator[dict[str, Any]]) -> AsyncIterator[bytes]:
    async for event in events:
        yield (json.dumps(event, default=str, separators=(",", ":")) + "\n").encode(
            "utf-8"
        )


async def _read_output(
    reader: asyncio.StreamReader | None,
    name: str,
    capture: "_CapturedOutput",
    stream_budget: "_StreamBudget",
    job: "_Job",
) -> None:
    if reader is None:
        return
    # Incremental: a multibyte character split across two reads must not become U+FFFD.
    decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
    while True:
        chunk = await reader.read(READ_CHUNK_BYTES)
        text = decoder.decode(chunk, final=not chunk)
        if not chunk and not text:
            return
        if not text:
            continue
        capture.append(text)
        streamed = stream_budget.take(text)
        if streamed:
            job.append(
                {
                    "kind": name,
                    "payload": {
                        "text": streamed,
                        "stdout_chars": (
                            capture.original_chars if name == "stdout" else None
                        ),
                        "stderr_chars": (
                            capture.original_chars if name == "stderr" else None
                        ),
                    },
                }
            )
        if not chunk:
            return


async def _terminate_process(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        return
    with suppress(ProcessLookupError):
        os.killpg(process.pid, signal.SIGTERM)
    try:
        await asyncio.wait_for(process.wait(), timeout=TERMINATE_GRACE_SECONDS)
        return
    except asyncio.TimeoutError:
        pass
    with suppress(ProcessLookupError):
        os.killpg(process.pid, signal.SIGKILL)
    with suppress(asyncio.TimeoutError):
        await asyncio.wait_for(process.wait(), timeout=TERMINATE_GRACE_SECONDS)


def _sandbox_command(request: ExecuteRequest, workspace_dir: Path) -> list[str]:
    cwd = _normalize_cwd(request.cwd)
    bwrap = shutil.which("bwrap") or "bwrap"
    bash = shutil.which("bash") or "/bin/bash"
    prlimit = shutil.which("prlimit") or "prlimit"
    address_space_bytes = int(
        os.environ.get("WORKSPACE_EXECUTOR_ADDRESS_SPACE_BYTES", str(768 * 1024 * 1024))
    )
    file_size_bytes = int(
        os.environ.get("WORKSPACE_EXECUTOR_FILE_SIZE_BYTES", str(256 * 1024 * 1024))
    )

    args = [
        prlimit,
        f"--cpu={request.timeout_seconds + 5}",
        f"--as={address_space_bytes}",
        "--nproc=256",
        "--nofile=1024",
        f"--fsize={file_size_bytes}",
        "--core=0",
        "--",
        bwrap,
        "--die-with-parent",
        "--new-session",
        "--unshare-all",
    ]
    if request.network:
        args.append("--share-net")
    args.extend(
        [
            "--ro-bind",
            "/",
            "/",
            # Mounting procfs is denied by the EKS/container LSM without making
            # this pod privileged. Mask the executor's /proc instead: exposing
            # it would let a command read PID 1's environment and recover the
            # executor bearer token.
            "--tmpfs",
            "/proc",
            "--dev",
            "/dev",
            "--tmpfs",
            "/tmp",
            "--tmpfs",
            "/run",
            "--bind",
            str(workspace_dir),
            "/workspace",
            # Hide the outer executor's state and all other agent workspaces.
            # The selected bind remains mounted at /workspace.
            "--tmpfs",
            str(_executor_root()),
            "--chdir",
            cwd,
            "--clearenv",
            *[
                arg
                for name, value in _sandbox_env().items()
                for arg in ("--setenv", name, value)
            ],
            "--",
            bash,
            "--noprofile",
            "--norc",
            "-o",
            "pipefail",
            "-lc",
            request.command,
        ]
    )
    return args


DEFAULT_GIT_NAME = "Coding Agent"
DEFAULT_GIT_EMAIL = "agent@coding-agent.invalid"


def _sandbox_env() -> dict[str, str]:
    """The whole environment of a sandboxed command (bwrap --clearenv).

    Includes a git identity, so a plain ``git commit`` from run_command works in a
    fresh repo without relying on a ``.gitconfig`` in ``HOME`` (which git tools ignore,
    see B1). Override with WORKSPACE_EXECUTOR_GIT_NAME / WORKSPACE_EXECUTOR_GIT_EMAIL.
    """
    name = os.environ.get("WORKSPACE_EXECUTOR_GIT_NAME") or DEFAULT_GIT_NAME
    email = os.environ.get("WORKSPACE_EXECUTOR_GIT_EMAIL") or DEFAULT_GIT_EMAIL
    return {
        "PATH": "/workspace/.venv/bin:/usr/local/bin:/usr/bin:/bin",
        "HOME": "/workspace",
        # Not under HOME: /workspace/.config/git/{config,attributes} would otherwise act as
        # git's global config for every command. /tmp is a per-command tmpfs.
        "XDG_CONFIG_HOME": "/tmp/.config",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "PIP_CACHE_DIR": "/workspace/.cache/pip",
        "PYTHONUNBUFFERED": "1",
        # No __pycache__ in the workspace: it would show up as changes (P5).
        "PYTHONDONTWRITEBYTECODE": "1",
        "GIT_AUTHOR_NAME": name,
        "GIT_AUTHOR_EMAIL": email,
        "GIT_COMMITTER_NAME": name,
        "GIT_COMMITTER_EMAIL": email,
    }


def _normalize_cwd(value: str) -> str:
    candidate = PurePosixPath(value or "/workspace")
    if not candidate.is_absolute():
        candidate = PurePosixPath("/workspace") / candidate
    if candidate == PurePosixPath("/workspace"):
        return "/workspace"
    try:
        relative = candidate.relative_to("/workspace")
    except ValueError as err:
        raise HTTPException(
            status_code=422,
            detail="cwd must be /workspace or a directory beneath it.",
        ) from err
    if ".." in relative.parts:
        raise HTTPException(status_code=422, detail="cwd cannot contain '..'.")
    return str(PurePosixPath("/workspace") / relative)


def _authorize(authorization: str | None) -> None:
    expected = os.environ.get("WORKSPACE_EXECUTOR_TOKEN", "")
    if not expected:
        raise HTTPException(
            status_code=503, detail="Executor authentication is not configured."
        )
    scheme, _, supplied = (authorization or "").partition(" ")
    if scheme.lower() != "bearer" or not hmac.compare_digest(supplied.encode("utf-8", "surrogateescape"), expected.encode("utf-8", "surrogateescape")):
        raise HTTPException(status_code=401, detail="Unauthorized")


def _prepare_executor_root() -> None:
    root = _executor_root()
    root.mkdir(mode=WORKSPACE_MODE, parents=True, exist_ok=True)
    # Kubernetes prepares mounted volume roots as root:fsGroup. The executor
    # can write through the group bit but, correctly, cannot chmod a directory
    # it does not own after all Linux capabilities have been dropped.
    if root.stat().st_uid == os.geteuid():
        root.chmod(WORKSPACE_MODE)
    (root / STATE_DIR_NAME).mkdir(mode=WORKSPACE_MODE, exist_ok=True)


def _executor_root() -> Path:
    return Path(os.environ.get("WORKSPACE_EXECUTOR_ROOT", "/workspaces")).resolve()


def _workspace_key(workspace_id: str) -> str:
    return hashlib.sha256(workspace_id.encode("utf-8")).hexdigest()[:40]


def _workspace_dir(workspace_key: str) -> Path:
    return _executor_root() / "sessions" / workspace_key


def _state_path(workspace_key: str, execution_id: str) -> Path:
    digest = hashlib.sha256(execution_id.encode("utf-8")).hexdigest()
    return _executor_root() / STATE_DIR_NAME / workspace_key / f"{digest}.json"


async def _workspace_locks_for(workspace_key: str) -> _WorkspaceLocks:
    async with _workspace_locks_guard:
        locks = _workspace_locks.get(workspace_key)
        if locks is None:
            locks = _workspace_locks[workspace_key] = _WorkspaceLocks()
        return locks


def _launcher_env() -> dict[str, str]:
    return {
        "HOME": "/tmp",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "PATH": "/usr/local/bin:/usr/bin:/bin",
    }


def _sandbox_self_test() -> str | None:
    workspace = _executor_root() / ".self-test"
    workspace.mkdir(mode=WORKSPACE_MODE, parents=True, exist_ok=True)
    request = ExecuteRequest(
        workspace_id="self-test",
        execution_id="workspace_shell:self-test",
        command="true",
        timeout_seconds=10,
        network=False,
    )
    try:
        completed = subprocess.run(
            _sandbox_command(request, workspace),
            stdin=subprocess.DEVNULL,
            capture_output=True,
            env=_launcher_env(),
            timeout=15,
            check=False,
        )
    except Exception as err:
        return f"Bubblewrap self-test could not start: {err}"
    if completed.returncode == 0:
        return None
    detail = completed.stderr.decode("utf-8", errors="replace").strip()
    return (
        "Bubblewrap self-test failed"
        f" with exit code {completed.returncode}: {detail or 'no stderr'}"
    )


MIN_GIT_VERSION = (2, 40)


def _git_version_self_test() -> str | None:
    """The worker's git tools need git >= 2.40 (--attr-source); /healthz fails otherwise."""
    git = shutil.which("git")
    if git is None:
        return "git is not installed in the executor image."
    try:
        completed = subprocess.run(
            [git, "--version"], capture_output=True, text=True, timeout=10, check=False,
            env=_launcher_env(),
        )
    except Exception as err:  # noqa: BLE001
        return f"git --version failed: {err}"
    version = _parse_git_version(completed.stdout)
    if version is None:
        return f"Could not read the git version from {completed.stdout.strip()!r}."
    if version < MIN_GIT_VERSION:
        return (
            f"git {'.'.join(map(str, version))} is too old; the git tools need "
            f"{'.'.join(map(str, MIN_GIT_VERSION))} or later."
        )
    return None


def _parse_git_version(text: str) -> tuple[int, int] | None:
    match = re.search(r"git version (\d+)\.(\d+)", text)
    return (int(match.group(1)), int(match.group(2))) if match else None


def _credential_boundary_self_test() -> str | None:
    credential_env_names = (
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_WEB_IDENTITY_TOKEN_FILE",
        "AWS_CONTAINER_CREDENTIALS_FULL_URI",
        "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
        # Model-provider and integration keys belong to the worker and API
        # only (PRD §5 rule 6). /files/* runs outside Bubblewrap, so the
        # executor must never hold them.
        "ANTHROPIC_API_KEY",
        "OPENAI_API_KEY",
        "GEMINI_API_KEY",
        "GOOGLE_API_KEY",
        "TYPESAFE_API_KEY",
    )
    # Any variable whose name contains one of these is treated as a credential.
    credential_env_name_markers = ("MCP_", "OAUTH", "GITHUB_TOKEN")
    exposed = [name for name in credential_env_names if os.environ.get(name)]
    exposed.extend(
        sorted(
            name
            for name, value in os.environ.items()
            if value
            and name not in exposed
            and any(marker in name.upper() for marker in credential_env_name_markers)
        )
    )
    if exposed:
        return f"Executor credential boundary failed: {', '.join(exposed)} is set."

    service_account_token = Path("/var/run/secrets/kubernetes.io/serviceaccount/token")
    if service_account_token.exists():
        return "Executor credential boundary failed: ServiceAccount token is mounted."

    probes = (
        (
            "GET",
            "/latest/meta-data/iam/security-credentials/",
            {},
            "IMDSv1 role credentials",
        ),
        (
            "PUT",
            "/latest/api/token",
            {"X-aws-ec2-metadata-token-ttl-seconds": "60"},
            "an IMDSv2 token",
        ),
    )
    for method, path, headers, description in probes:
        status = _metadata_status(method, path, headers)
        if status is not None:
            return (
                "Executor credential boundary failed: the Pod can reach the "
                f"metadata path for {description} (HTTP {status})."
            )

    kubernetes_host = os.environ.get("KUBERNETES_SERVICE_HOST")
    kubernetes_port = int(os.environ.get("KUBERNETES_SERVICE_PORT", "443"))
    if kubernetes_host and _tcp_endpoint_reachable(kubernetes_host, kubernetes_port):
        return "Executor credential boundary failed: Kubernetes API is reachable."
    return None


def _metadata_status(method: str, path: str, headers: dict[str, str]) -> int | None:
    timeout = float(
        os.environ.get("WORKSPACE_EXECUTOR_IMDS_PROBE_TIMEOUT_SECONDS", "1.5")
    )
    connection = http.client.HTTPConnection("169.254.169.254", timeout=timeout)
    try:
        connection.request(method, path, headers=headers)
        response = connection.getresponse()
        status = response.status
        response.read(1)
        return status
    except (OSError, TimeoutError, http.client.HTTPException):
        return None
    finally:
        connection.close()


def _tcp_endpoint_reachable(host: str, port: int) -> bool:
    timeout = float(
        os.environ.get("WORKSPACE_EXECUTOR_PRIVATE_PROBE_TIMEOUT_SECONDS", "1.5")
    )
    try:
        connection = socket.create_connection((host, port), timeout=timeout)
    except OSError:
        return False
    connection.close()
    return True


def _start_payload(request: ExecuteRequest, workspace_key: str) -> dict[str, Any]:
    return {
        "workspace": workspace_key,
        "cwd": _normalize_cwd(request.cwd),
        "timeout_seconds": request.timeout_seconds,
        "network": request.network,
        "command_chars": len(request.command),
    }


def _result_payload(
    *,
    request: ExecuteRequest,
    workspace_key: str,
    exit_code: int | None,
    stdout: "_CapturedOutput",
    stderr: "_CapturedOutput",
    stream_budget: "_StreamBudget",
    timed_out: bool,
    elapsed_seconds: float,
    before: dict[str, tuple[int, int]],
    after: dict[str, tuple[int, int]],
    git_branch: str | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "workspace": workspace_key,
        "cwd": _normalize_cwd(request.cwd),
        "command": request.command,
        "network": request.network,
        "exit_code": exit_code,
        "stdout": stdout.text,
        "stderr": stderr.text,
        "timed_out": timed_out,
        "elapsed_seconds": round(elapsed_seconds, 3),
        "files_changed": _changed_files(before, after),
        # The workspace repo's current branch after the command (None: not a repo, or
        # unknown). Read from .git/HEAD, so no git process runs for it.
        "git_branch": git_branch,
    }
    if stdout.truncated:
        result["stdout_truncated"] = True
    if stderr.truncated:
        result["stderr_truncated"] = True
    if stream_budget.truncated:
        result["stream_truncated"] = True
    if timed_out:
        result["error"] = f"Command timed out after {request.timeout_seconds} seconds."
        result["type"] = "WorkspaceShellTimeout"
    elif exit_code not in (0, None):
        result["error"] = f"Command exited with code {exit_code}."
        result["type"] = "WorkspaceShellProcessError"
    return result


GIT_HEAD_MAX_BYTES = 512
GIT_DETACHED_HEAD = "HEAD (no branch)"  # what `git status -b` prints for a detached HEAD
_GIT_HEAD_REF = re.compile(r"ref: refs/heads/([^\x00-\x1f\x7f]{1,250})")
_GIT_HEAD_SHA = re.compile(r"[0-9a-f]{40}|[0-9a-f]{64}")


def _git_branch(workspace_dir: Path) -> str | None:
    """The branch checked out in the workspace's root repo, read from ``.git/HEAD``.

    Returns the branch name, :data:`GIT_DETACHED_HEAD` for a detached HEAD, or None when
    the workspace root is not a repo or HEAD can't be read (a ``.git`` file or symlink,
    reftable storage, an unexpected format). Symlinks are never followed, and only a
    well-formed ref or object id is returned, so nothing else the workspace planted in
    HEAD can leak out.
    """
    flags = os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    try:
        git_fd = os.open(workspace_dir / ".git", flags | os.O_DIRECTORY)
    except OSError:
        return None
    try:
        head_fd = os.open("HEAD", flags | os.O_NONBLOCK, dir_fd=git_fd)
        try:
            if not stat_module.S_ISREG(os.fstat(head_fd).st_mode):
                return None
            data = os.read(head_fd, GIT_HEAD_MAX_BYTES)
        finally:
            os.close(head_fd)
    except OSError:
        return None
    finally:
        os.close(git_fd)
    text = data.decode("utf-8", errors="replace").strip()
    ref = _GIT_HEAD_REF.fullmatch(text)
    if ref:
        name = ref.group(1)
        return None if name == ".invalid" else name  # reftable stores HEAD elsewhere
    if _GIT_HEAD_SHA.fullmatch(text):
        return GIT_DETACHED_HEAD
    return None


def _workspace_snapshot(workspace_dir: Path) -> dict[str, tuple[int, int]]:
    snapshot: dict[str, tuple[int, int]] = {}
    if not workspace_dir.exists():
        return snapshot
    for path in workspace_dir.rglob("*"):
        if len(snapshot) >= MAX_CHANGED_FILES:
            break
        if path.is_symlink() or not path.is_file():
            continue
        with suppress(OSError):
            stat = path.stat()
            snapshot[str(path.relative_to(workspace_dir))] = (
                stat.st_size,
                stat.st_mtime_ns,
            )
    return snapshot


def _changed_files(
    before: dict[str, tuple[int, int]],
    after: dict[str, tuple[int, int]],
) -> dict[str, list[str]]:
    return {
        "created": sorted(after.keys() - before.keys()),
        "modified": sorted(
            name for name in after.keys() & before.keys() if after[name] != before[name]
        ),
        "deleted": sorted(before.keys() - after.keys()),
    }


def _load_cached_result(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _state_ttl_seconds() -> float:
    return max(
        60.0,
        float(
            os.environ.get(
                "WORKSPACE_EXECUTOR_STATE_TTL_SECONDS", str(DEFAULT_STATE_TTL_SECONDS)
            )
        ),
    )


def _sweep_state(now: float | None = None) -> int:
    """Delete per-execution state files older than the TTL; returns how many.

    Files of live jobs are kept whatever their age. Empty per-workspace directories
    are removed too.
    """
    root = _executor_root() / STATE_DIR_NAME
    if not root.is_dir():
        return 0
    cutoff = (time.time() if now is None else now) - _state_ttl_seconds()
    live = {
        _state_path(workspace_key, execution_id).stem
        for workspace_key, execution_id in list(_jobs)
    }
    removed = 0
    for directory in root.iterdir():
        if directory.is_symlink() or not directory.is_dir():
            continue
        for entry in directory.iterdir():
            if entry.name.split(".", 1)[0] in live:
                continue
            with suppress(OSError):
                if entry.is_file() and entry.lstat().st_mtime < cutoff:
                    entry.unlink()
                    removed += 1
        with suppress(OSError):
            directory.rmdir()  # only succeeds when empty
    return removed


def _maybe_schedule_state_gc() -> None:
    global _last_state_gc_at
    now = time.monotonic()
    if now - _last_state_gc_at < STATE_GC_INTERVAL_SECONDS:
        return
    _last_state_gc_at = now
    with suppress(RuntimeError):
        task = asyncio.get_running_loop().create_task(asyncio.to_thread(_sweep_state))
        _background_tasks.add(task)
        task.add_done_callback(_background_tasks.discard)


_background_tasks: set[asyncio.Future[Any]] = set()


def _started_marker(state_path: Path) -> Path:
    return state_path.with_suffix(".started")


def _mark_started(state_path: Path) -> None:
    marker = _started_marker(state_path)
    marker.parent.mkdir(mode=WORKSPACE_MODE, parents=True, exist_ok=True)
    marker.touch()


def _events_path(state_path: Path) -> Path:
    return state_path.with_suffix(".events.json")


def _store_cached_events(state_path: Path, events: list[dict[str, Any]]) -> None:
    # Bounded by the job's replay buffer (JOB_REPLAY_BUFFER_CHARS / _EVENTS).
    path = _events_path(state_path)
    path.parent.mkdir(mode=WORKSPACE_MODE, parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(events, default=str), encoding="utf-8")
    temporary.replace(path)


def _load_cached_events(state_path: Path) -> list[dict[str, Any]]:
    try:
        events = json.loads(_events_path(state_path).read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return []
    if not isinstance(events, list):
        return []
    return [event for event in events if isinstance(event, dict)]


def _store_cached_result(path: Path, result: dict[str, Any]) -> None:
    path.parent.mkdir(mode=WORKSPACE_MODE, parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(result, default=str), encoding="utf-8")
    temporary.replace(path)


class _CapturedOutput:
    def __init__(self, limit: int) -> None:
        self.limit = limit
        self._parts: list[str] = []
        self._chars = 0
        self.original_chars = 0
        self.truncated = False

    def append(self, text: str) -> None:
        self.original_chars += len(text)
        remaining = self.limit - self._chars
        if remaining <= 0:
            self.truncated = True
            return
        kept = text[:remaining]
        self._parts.append(kept)
        self._chars += len(kept)
        if len(kept) < len(text):
            self.truncated = True

    @property
    def text(self) -> str:
        return "".join(self._parts)


class _StreamBudget:
    def __init__(self, limit: int) -> None:
        self.limit = limit
        self.used = 0
        self.truncated = False

    def take(self, text: str) -> str:
        remaining = self.limit - self.used
        if remaining <= 0:
            self.truncated = True
            return ""
        kept = text[:remaining]
        self.used += len(kept)
        if len(kept) < len(text):
            self.truncated = True
        return kept


if __name__ == "__main__":
    main()
