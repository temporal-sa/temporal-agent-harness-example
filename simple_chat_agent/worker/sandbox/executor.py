from __future__ import annotations

import asyncio
import base64
import binascii
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


class FileWriteRequest(FilePathRequest):
    content: str = Field(max_length=MAX_FILE_CONTENT_CHARS)
    encoding: Literal["utf-8", "base64"] = "utf-8"
    create_parents: bool = False
    overwrite: bool = True


class FileEditRequest(FilePathRequest):
    old_string: str = Field(min_length=1, max_length=MAX_FILE_CONTENT_CHARS)
    new_string: str = Field(max_length=MAX_FILE_CONTENT_CHARS)


class FileMkdirRequest(FilePathRequest):
    parents: bool = False


class FileRenameRequest(_FileRequest):
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

    Lock order is always ``_concurrency`` -> ``exec`` -> ``files``, so there is
    no cycle. A command cannot keep mutating after its ``/execute`` returns:
    the sandbox has its own PID namespace and ``--die-with-parent``, and the
    process group is killed on timeout or cancellation.
    """

    exec: asyncio.Lock = field(default_factory=asyncio.Lock)
    files: _ReadWriteLock = field(default_factory=_ReadWriteLock)


app = FastAPI(title="Agent workspace executor", docs_url=None, redoc_url=None)
_workspace_locks: dict[str, _WorkspaceLocks] = {}
_workspace_locks_guard = asyncio.Lock()
_concurrency = asyncio.Semaphore(
    max(1, int(os.environ.get("WORKSPACE_EXECUTOR_MAX_CONCURRENT", "4")))
)
_self_test_error: str | None = None
_live_health_lock = asyncio.Lock()
_last_live_health_check_at = 0.0
_last_live_health_error: str | None = None


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

    return StreamingResponse(
        _encode_events(_execute_events(request)),
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
    data = _decode_write_content(request)
    return await _run_file_op(
        request.workspace_id,
        True,
        lambda root, req: _op_write(root, req, data),
        request,
    )


@app.post("/files/edit")
async def files_edit(
    request: FileEditRequest, authorization: str | None = Header(default=None)
) -> dict[str, Any]:
    _authorize(authorization)
    return await _run_file_op(request.workspace_id, True, _op_edit, request)


@app.post("/files/mkdir")
async def files_mkdir(
    request: FileMkdirRequest, authorization: str | None = Header(default=None)
) -> dict[str, Any]:
    _authorize(authorization)
    return await _run_file_op(request.workspace_id, True, _op_mkdir, request)


@app.post("/files/delete")
async def files_delete(
    request: FilePathRequest, authorization: str | None = Header(default=None)
) -> dict[str, Any]:
    _authorize(authorization)
    return await _run_file_op(request.workspace_id, True, _op_delete, request)


@app.post("/files/rename")
async def files_rename(
    request: FileRenameRequest, authorization: str | None = Header(default=None)
) -> dict[str, Any]:
    _authorize(authorization)
    return await _run_file_op(request.workspace_id, True, _op_rename, request)


async def _run_file_op(
    workspace_id: str,
    mutates: bool,
    operation: Callable[[int, _T], dict[str, Any]],
    request: _T,
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
            async with locks.exec, locks.files.exclusive():
                return await asyncio.to_thread(run)
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


def _op_delete(root_fd: int, request: FilePathRequest) -> dict[str, Any]:
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
    _self_test_error = _credential_boundary_self_test() or _sandbox_self_test()
    if _self_test_error is not None:
        print(_self_test_error, flush=True)
    uvicorn.run(
        app,
        host=os.environ.get("WORKSPACE_EXECUTOR_HOST", "0.0.0.0"),
        port=int(os.environ.get("WORKSPACE_EXECUTOR_PORT", "8082")),
        log_level=os.environ.get("WORKSPACE_EXECUTOR_LOG_LEVEL", "info"),
    )


async def _execute_events(request: ExecuteRequest) -> AsyncIterator[dict[str, Any]]:
    workspace_key = _workspace_key(request.workspace_id)
    workspace_dir = _workspace_dir(workspace_key)
    state_path = _state_path(workspace_key, request.execution_id)
    locks = await _workspace_locks_for(workspace_key)

    # See _WorkspaceLocks: commands exclude file mutations, not file reads.
    async with _concurrency, locks.exec:
        cached = _load_cached_result(state_path)
        if cached is not None:
            cached["cached"] = True
            yield {"kind": "start", "payload": _start_payload(request, workspace_key)}
            yield {"kind": "result", "payload": cached}
            return

        workspace_dir.mkdir(mode=WORKSPACE_MODE, parents=True, exist_ok=True)
        workspace_dir.chmod(WORKSPACE_MODE)
        before = _workspace_snapshot(workspace_dir)
        yield {"kind": "start", "payload": _start_payload(request, workspace_key)}

        command = _sandbox_command(request, workspace_dir)
        started_at = time.monotonic()
        process: asyncio.subprocess.Process | None = None
        stdout = _CapturedOutput(MAX_CAPTURE_CHARS)
        stderr = _CapturedOutput(MAX_CAPTURE_CHARS)
        stream_budget = _StreamBudget(MAX_STREAM_CHARS)
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        timed_out = False

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
            if request.stdin is not None and process.stdin is not None:
                process.stdin.write(request.stdin.encode("utf-8"))
                await process.stdin.drain()
                process.stdin.close()

            readers = [
                asyncio.create_task(
                    _read_output(process.stdout, "stdout", stdout, stream_budget, queue)
                ),
                asyncio.create_task(
                    _read_output(process.stderr, "stderr", stderr, stream_budget, queue)
                ),
            ]
            wait_task = asyncio.create_task(process.wait())
            deadline = started_at + request.timeout_seconds
            next_progress = started_at + PROGRESS_INTERVAL_SECONDS

            while not wait_task.done():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    timed_out = True
                    await _terminate_process(process)
                    break

                queue_wait = asyncio.create_task(queue.get())
                done, _ = await asyncio.wait(
                    {wait_task, queue_wait},
                    timeout=min(remaining, max(0.05, next_progress - time.monotonic())),
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if queue_wait in done:
                    yield queue_wait.result()
                else:
                    queue_wait.cancel()
                    with suppress(asyncio.CancelledError):
                        await queue_wait

                if time.monotonic() >= next_progress and not wait_task.done():
                    yield {
                        "kind": "progress",
                        "payload": {
                            "elapsed_seconds": round(time.monotonic() - started_at, 3),
                            "stdout_chars": stdout.original_chars,
                            "stderr_chars": stderr.original_chars,
                        },
                    }
                    next_progress = time.monotonic() + PROGRESS_INTERVAL_SECONDS

            if not wait_task.done():
                with suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(wait_task, timeout=TERMINATE_GRACE_SECONDS)
            await asyncio.gather(*readers, return_exceptions=True)
            while not queue.empty():
                yield queue.get_nowait()

            exit_code = process.returncode
            after = _workspace_snapshot(workspace_dir)
            result = _result_payload(
                request=request,
                workspace_key=workspace_key,
                exit_code=exit_code,
                stdout=stdout,
                stderr=stderr,
                stream_budget=stream_budget,
                timed_out=timed_out,
                elapsed_seconds=time.monotonic() - started_at,
                before=before,
                after=after,
            )
            _store_cached_result(state_path, result)
            yield {"kind": "result", "payload": result}
        except asyncio.CancelledError:
            if process is not None:
                await _terminate_process(process)
            raise
        except Exception as err:
            if process is not None:
                await _terminate_process(process)
            yield {
                "kind": "result",
                "payload": {
                    "error": f"Workspace sandbox failed to start: {err}",
                    "type": "WorkspaceSandboxError",
                    "workspace": workspace_key,
                },
            }


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
    queue: asyncio.Queue[dict[str, Any]],
) -> None:
    if reader is None:
        return
    while True:
        chunk = await reader.read(READ_CHUNK_BYTES)
        if not chunk:
            return
        text = chunk.decode("utf-8", errors="replace")
        capture.append(text)
        streamed = stream_budget.take(text)
        if streamed:
            await queue.put(
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
            "--setenv",
            "PATH",
            "/workspace/.venv/bin:/usr/local/bin:/usr/bin:/bin",
            "--setenv",
            "HOME",
            "/workspace",
            "--setenv",
            "LANG",
            "C.UTF-8",
            "--setenv",
            "LC_ALL",
            "C.UTF-8",
            "--setenv",
            "PIP_CACHE_DIR",
            "/workspace/.cache/pip",
            "--setenv",
            "PYTHONUNBUFFERED",
            "1",
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
    if scheme.lower() != "bearer" or not hmac.compare_digest(supplied, expected):
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
