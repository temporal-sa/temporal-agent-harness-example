from __future__ import annotations

import asyncio
import ast
import json
import sys
import tempfile
import time
from contextlib import suppress
from pathlib import Path
from typing import Any

from claude_harness.streaming import StreamContext

MAX_CODE_CHARS = 100_000
LAMBDA_MAX_TIMEOUT_SECONDS = 15 * 60
SANDBOX_DEADLINE_BUFFER_SECONDS = 15
MAX_TIMEOUT_SECONDS = LAMBDA_MAX_TIMEOUT_SECONDS - SANDBOX_DEADLINE_BUFFER_SECONDS
DEFAULT_TIMEOUT_SECONDS = MAX_TIMEOUT_SECONDS
MAX_OUTPUT_CHARS = 50_000
MAX_STREAM_CHARS = 100_000
READ_CHUNK_BYTES = 4096
PROGRESS_INTERVAL_SECONDS = 5
PROCESS_POLL_INTERVAL_SECONDS = 0.05
STREAM_DRAIN_TIMEOUT_SECONDS = 2
PR_SET_DUMPABLE = 4
SUID_DUMP_DISABLE = 0
SENSITIVE_PARENT_ENV_NAMES = frozenset(
    {
        "AWS_ACCESS_KEY",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_LAMBDA_METADATA_API",
        "AWS_LAMBDA_METADATA_TOKEN",
        "AWS_LAMBDA_RUNTIME_API",
    }
)


class SandboxHardeningError(RuntimeError):
    pass


_HOST_PROCESS_HARDENED = False


async def execute_python_sandbox(
    code: str,
    timeout_seconds: int,
    *,
    stream: StreamContext,
) -> dict[str, Any]:
    timeout_seconds = max(1, min(timeout_seconds, MAX_TIMEOUT_SECONDS))
    await stream.emit(
        {
            "timeout_seconds": timeout_seconds,
            "max_timeout_seconds": MAX_TIMEOUT_SECONDS,
            "chars": len(code),
        },
        kind="python_sandbox_start",
    )

    try:
        prepared_code = _prepare_code(code)
    except ValueError as err:
        payload = {"error": str(err), "type": "ValidationError"}
        await stream.emit(payload, kind="python_sandbox_rejected")
        return payload

    return await _run_sandbox_process(prepared_code, timeout_seconds, stream)


def _prepare_code(code: str) -> str:
    if len(code) > MAX_CODE_CHARS:
        raise ValueError(f"Code is too large. Max chars: {MAX_CODE_CHARS}.")

    try:
        tree = ast.parse(code, mode="exec")
    except SyntaxError as err:
        raise ValueError(f"SyntaxError: {err}") from err

    if tree.body and isinstance(tree.body[-1], ast.Expr):
        tree.body[-1] = ast.Assign(
            targets=[ast.Name(id="_result", ctx=ast.Store())],
            value=tree.body[-1].value,
        )
        ast.fix_missing_locations(tree)

    return ast.unparse(tree)


async def _run_sandbox_process(
    code: str,
    timeout_seconds: int,
    stream: StreamContext,
) -> dict[str, Any]:
    try:
        harden_host_process_for_sandbox_children()
    except SandboxHardeningError as err:
        payload = {
            "error": str(err),
            "type": "SandboxHardeningError",
        }
        await stream.emit(payload, kind="python_sandbox_hardening_failed")
        return payload

    with tempfile.TemporaryDirectory(prefix="claude-python-sandbox-") as temp_dir:
        result_path = Path(temp_dir) / "sandbox-result.json"
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-u",
            "-c",
            _RUNNER,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=temp_dir,
            env=_runner_env(result_path),
            start_new_session=True,
        )
        await _write_runner_input(process, {"code": code})

        stdout = _CapturedOutput(MAX_OUTPUT_CHARS)
        stderr = _CapturedOutput(MAX_OUTPUT_CHARS)
        stream_budget = _StreamBudget(MAX_STREAM_CHARS)
        started_at = time.monotonic()
        stdout_task = asyncio.create_task(
            _read_process_stream(
                process.stdout,
                "stdout",
                stream,
                stdout,
                stream_budget,
            )
        )
        stderr_task = asyncio.create_task(
            _read_process_stream(
                process.stderr,
                "stderr",
                stream,
                stderr,
                stream_budget,
            )
        )
        progress_task = asyncio.create_task(
            _emit_progress(stream, started_at, timeout_seconds)
        )

        timed_out = False
        try:
            timed_out = await _wait_for_sandbox_completion(
                process,
                result_path,
                timeout_seconds,
            )
            if timed_out:
                _kill_sandbox_process_group(process.pid)
                with suppress(ProcessLookupError):
                    process.kill()
                await process.wait()
        finally:
            if process.returncode is None and result_path.exists():
                with suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(
                        process.wait(),
                        timeout=STREAM_DRAIN_TIMEOUT_SECONDS,
                    )
            _kill_sandbox_process_group(process.pid)
            if process.returncode is None:
                with suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(
                        process.wait(),
                        timeout=STREAM_DRAIN_TIMEOUT_SECONDS,
                    )
            progress_task.cancel()
            with suppress(asyncio.CancelledError):
                await progress_task

        try:
            await asyncio.wait_for(
                asyncio.gather(stdout_task, stderr_task),
                timeout=STREAM_DRAIN_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            for task in (stdout_task, stderr_task):
                task.cancel()
            with suppress(asyncio.CancelledError):
                await asyncio.gather(stdout_task, stderr_task)

        if timed_out:
            payload: dict[str, Any] = {
                "error": f"Python execution timed out after {timeout_seconds} seconds.",
                "type": "TimeoutExpired",
                "timed_out": True,
            }
            await stream.emit(payload, kind="python_sandbox_timeout")
        else:
            payload = _load_runner_result(result_path)

        payload["exit_code"] = process.returncode
        payload["stdout"] = stdout.text
        payload["stderr"] = stderr.text
        if stdout.truncated:
            payload["stdout_truncated"] = True
        if stderr.truncated:
            payload["stderr_truncated"] = True
        if stream_budget.truncated:
            payload["stream_truncated"] = True

        if process.returncode not in (0, None) and "error" not in payload:
            payload["error"] = f"Python process exited with code {process.returncode}."
            payload["type"] = "SandboxProcessError"

        await stream.emit(
            {
                "exit_code": process.returncode,
                "ok": process.returncode == 0 and "error" not in payload,
                "stdout_chars": len(payload["stdout"]),
                "stderr_chars": len(payload["stderr"]),
                "timed_out": timed_out,
            },
            kind="python_sandbox_complete",
        )
        return payload


async def _wait_for_sandbox_completion(
    process: asyncio.subprocess.Process,
    result_path: Path,
    timeout_seconds: int,
) -> bool:
    """Return True when execution timed out, False when it completed.

    asyncio's subprocess wait can remain pending while a grandchild process
    keeps stdout/stderr pipes open. The runner writes its result file after user
    code finishes, so the parent treats either direct process exit or result file
    creation as completion and then tears down the whole process group.
    """
    wait_task = asyncio.create_task(process.wait())
    deadline = time.monotonic() + timeout_seconds
    try:
        while True:
            if process.returncode is not None or wait_task.done() or result_path.exists():
                return False

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return True

            await asyncio.wait(
                {wait_task},
                timeout=min(PROCESS_POLL_INTERVAL_SECONDS, remaining),
            )
    finally:
        if not wait_task.done():
            wait_task.cancel()
            with suppress(asyncio.CancelledError):
                await wait_task


def harden_host_process_for_sandbox_children() -> None:
    """Prevent same-UID sandbox children from reading this process via /proc.

    Lambda injects execution-role credentials and metadata API tokens
    into the runtime process environment. The sandbox child gets a clean env, but
    without this Linux hardening it can still read the parent environment through
    /proc/<pid>/environ when both processes run as the same UID.
    """
    global _HOST_PROCESS_HARDENED

    if _HOST_PROCESS_HARDENED or not sys.platform.startswith("linux"):
        return

    try:
        _disable_current_process_dumpability()
    except SandboxHardeningError as dumpable_err:
        try:
            _scrub_sensitive_process_environment()
        except SandboxHardeningError as scrub_err:
            raise SandboxHardeningError(
                f"{dumpable_err} Environment scrub fallback also failed: {scrub_err}"
            ) from scrub_err

    _HOST_PROCESS_HARDENED = True


def _disable_current_process_dumpability() -> None:
    try:
        import ctypes
        import os
    except ImportError as err:  # pragma: no cover - ctypes is expected on Linux.
        raise SandboxHardeningError(
            f"Could not load Linux prctl support: {err}."
        ) from err

    libc = ctypes.CDLL(None, use_errno=True)
    result = libc.prctl(PR_SET_DUMPABLE, SUID_DUMP_DISABLE, 0, 0, 0)
    if result != 0:
        errno = ctypes.get_errno()
        raise SandboxHardeningError(
            "Could not disable parent process dumpability before sandbox spawn: "
            f"errno {errno} ({os.strerror(errno)})."
        )


def _scrub_sensitive_process_environment() -> None:
    """Best-effort fallback for Lambda runtimes that deny prctl.

    os.environ.pop() alone is not enough: /proc/<pid>/environ reads the original
    environment memory region. Overwriting the C environ strings first removes
    the sensitive bytes from that region before the variables are unset.
    """
    try:
        import ctypes
        import os
    except ImportError as err:  # pragma: no cover - ctypes/os exist on Linux.
        raise SandboxHardeningError(
            f"Could not load Linux environment scrub support: {err}."
        ) from err

    try:
        libc = ctypes.CDLL(None)
        environ = ctypes.POINTER(ctypes.c_void_p).in_dll(libc, "environ")
    except Exception as err:
        raise SandboxHardeningError(f"Could not access C environ: {err}.") from err

    index = 0
    try:
        while environ[index]:
            pointer = int(environ[index])
            raw = ctypes.string_at(pointer)
            name = raw.split(b"=", 1)[0].decode("utf-8", errors="ignore")
            if name in SENSITIVE_PARENT_ENV_NAMES:
                ctypes.memset(pointer, 0, len(raw))
            index += 1
    except Exception as err:
        raise SandboxHardeningError(
            f"Could not overwrite sensitive C environ entries: {err}."
        ) from err

    for name in SENSITIVE_PARENT_ENV_NAMES:
        os.environ.pop(name, None)


def _kill_sandbox_process_group(pid: int | None) -> None:
    if pid is None or not sys.platform.startswith(("linux", "darwin")):
        return

    try:
        import os
        import signal
    except ImportError:  # pragma: no cover - os/signal exist on supported POSIX.
        return

    with suppress(ProcessLookupError, PermissionError, OSError):
        os.killpg(pid, signal.SIGKILL)


def _runner_env(result_path: Path) -> dict[str, str]:
    # Do not inherit the worker/Lambda environment; the sandbox code should not
    # receive agent secrets, OAuth tokens, AWS credentials, or stream tokens.
    return {
        "PYTHONIOENCODING": "utf-8",
        "PYTHONUNBUFFERED": "1",
        "PYTHON_SANDBOX_RESULT_PATH": str(result_path),
    }


async def _write_runner_input(
    process: asyncio.subprocess.Process,
    payload: dict[str, Any],
) -> None:
    if process.stdin is None:
        return

    with suppress(BrokenPipeError, ConnectionResetError):
        process.stdin.write(json.dumps(payload).encode("utf-8"))
        await process.stdin.drain()
    process.stdin.close()
    with suppress(BrokenPipeError, ConnectionResetError):
        await process.stdin.wait_closed()


async def _read_process_stream(
    reader: asyncio.StreamReader | None,
    name: str,
    stream: StreamContext,
    capture: "_CapturedOutput",
    stream_budget: "_StreamBudget",
) -> None:
    if reader is None:
        return

    while True:
        chunk = await reader.read(READ_CHUNK_BYTES)
        if not chunk:
            return

        text = chunk.decode("utf-8", errors="replace")
        capture.append(text)
        emit_text, truncated_now = stream_budget.take(text)
        if emit_text:
            await stream.emit(
                {"stream": name, "text": emit_text},
                kind=f"python_sandbox_{name}",
            )
        if truncated_now:
            await stream.emit(
                {"max_stream_chars": MAX_STREAM_CHARS},
                kind="python_sandbox_stream_truncated",
            )


async def _emit_progress(
    stream: StreamContext,
    started_at: float,
    timeout_seconds: int,
) -> None:
    while True:
        await asyncio.sleep(PROGRESS_INTERVAL_SECONDS)
        elapsed_seconds = int(time.monotonic() - started_at)
        await stream.emit(
            {
                "elapsed_seconds": elapsed_seconds,
                "timeout_seconds": timeout_seconds,
            },
            kind="python_sandbox_progress",
        )


def _load_runner_result(result_path: Path) -> dict[str, Any]:
    if not result_path.exists():
        return {
            "error": "Sandbox runner did not return a result.",
            "type": "SandboxRunnerError",
        }

    try:
        payload = json.loads(result_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as err:
        return {
            "error": f"Sandbox runner returned invalid result data: {err}",
            "type": "SandboxRunnerError",
        }

    if not isinstance(payload, dict):
        return {
            "error": "Sandbox runner returned a non-object payload.",
            "type": "SandboxRunnerError",
        }
    return payload


class _CapturedOutput:
    def __init__(self, limit: int) -> None:
        self._limit = limit
        self._chunks: list[str] = []
        self._length = 0
        self.truncated = False

    @property
    def text(self) -> str:
        return "".join(self._chunks)

    def append(self, text: str) -> None:
        remaining = self._limit - self._length
        if remaining <= 0:
            self.truncated = True
            return

        if len(text) > remaining:
            self._chunks.append(text[:remaining])
            self._length += remaining
            self.truncated = True
            return

        self._chunks.append(text)
        self._length += len(text)


class _StreamBudget:
    def __init__(self, limit: int) -> None:
        self._remaining = limit
        self.truncated = False
        self._notice_sent = False

    def take(self, text: str) -> tuple[str, bool]:
        if self._remaining <= 0:
            return "", self._mark_truncated()

        if len(text) > self._remaining:
            emit_text = text[: self._remaining]
            self._remaining = 0
            return emit_text, self._mark_truncated()

        self._remaining -= len(text)
        return text, False

    def _mark_truncated(self) -> bool:
        self.truncated = True
        if self._notice_sent:
            return False
        self._notice_sent = True
        return True


_RUNNER = r"""
import json
import os
import sys
import traceback


def jsonable(value):
    try:
        json.dumps(value)
        return value
    except TypeError:
        return repr(value)


def write_result(payload):
    result_path = os.environ["PYTHON_SANDBOX_RESULT_PATH"]
    with open(result_path, "w", encoding="utf-8") as result_file:
        json.dump(payload, result_file)


payload = json.loads(sys.stdin.read())
code = payload["code"]
namespace = {
    "__name__": "__main__",
    "__file__": "<python_sandbox>",
}
exit_code = 0

try:
    exec(code, namespace, namespace)
    response = {
        "result": jsonable(namespace.get("_result", namespace.get("result"))),
    }
except BaseException as err:
    exit_code = 1
    response = {
        "error": str(err),
        "type": type(err).__name__,
        "traceback": traceback.format_exc(limit=20),
    }

try:
    write_result(response)
except BaseException as err:
    print(f"Failed to write sandbox result: {err}", file=sys.stderr)
    exit_code = 1

raise SystemExit(exit_code)
"""
