from __future__ import annotations

import asyncio
import hashlib
import hmac
import http.client
import json
import os
import re
import shutil
import signal
import socket
import subprocess
import time
from collections.abc import AsyncIterator
from contextlib import suppress
from pathlib import Path, PurePosixPath
from typing import Any

import uvicorn
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import StreamingResponse
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


app = FastAPI(title="Agent workspace executor", docs_url=None, redoc_url=None)
_workspace_locks: dict[str, asyncio.Lock] = {}
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
    lock = await _workspace_lock(workspace_key)

    async with _concurrency, lock:
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


async def _workspace_lock(workspace_key: str) -> asyncio.Lock:
    async with _workspace_locks_guard:
        return _workspace_locks.setdefault(workspace_key, asyncio.Lock())


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
    )
    exposed = [name for name in credential_env_names if os.environ.get(name)]
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
