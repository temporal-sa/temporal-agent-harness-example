from __future__ import annotations

import json
import os
from collections.abc import AsyncIterator, Callable
from datetime import timedelta
from typing import Any

import httpx
from temporalio.common import RetryPolicy

from agent_harness.streaming import StreamContext
from agent_harness.tool_types import ToolType
from agent_harness.tools import RoutedActivityContext, ToolContext, ToolResult, tool

WORKSPACE_SHELL_TOOL = "workspace_shell"
LEGACY_PYTHON_SANDBOX_TOOL = "python_sandbox"
DEFAULT_TIMEOUT_SECONDS = 300
MAX_TIMEOUT_SECONDS = 15 * 60
ACTIVITY_TIMEOUT_BUFFER_SECONDS = 60
ACTIVITY_HEARTBEAT_TIMEOUT_SECONDS = 30
MAX_COMMAND_CHARS = 100_000
MAX_STDIN_CHARS = 1_000_000
WORKSPACE_SHELL_RETRY_POLICY = RetryPolicy(maximum_attempts=1)


class WorkspaceShellProvider:
    """Persistent, sandboxed command execution scoped to one agent workflow."""

    def __init__(self, *, workspace_id: Callable[[], str]) -> None:
        self._workspace_id = workspace_id

    @tool(
        name=WORKSPACE_SHELL_TOOL,
        description=(
            "Run an arbitrary Bash command in an isolated persistent workspace. "
            "The command runs through `bash -lc`, so pipes, redirects, heredocs, "
            "Python, curl, git, jq, and normal shell composition work. Only "
            "/workspace is writable, and files created there remain available to "
            "later workspace_shell calls by this agent. The sandbox has public "
            "HTTP/HTTPS internet access but cannot reach cluster, VPC, loopback, "
            "link-local, or cloud-metadata addresses. Use cwd='/workspace' or a "
            "directory beneath it. stdout and stderr stream while the command is "
            "running. Commands may have external side effects, so executions are "
            "not retried automatically."
        ),
        tool_type=ToolType.MUTATING,
        pre_guards=["mutating_tool_approval"],
    )
    async def workspace_shell(
        self,
        ctx: ToolContext,
        command: str,
        cwd: str = "/workspace",
        timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
        network: bool = True,
        stdin: str | None = None,
    ) -> ToolResult:
        payload = await _execute_workspace_command(
            ctx,
            workspace_id=self._workspace_id(),
            command=command,
            cwd=cwd,
            timeout_seconds=timeout_seconds,
            network=network,
            stdin=stdin,
        )
        return ToolResult(payload=payload, error="error" in payload)

    @tool(
        name=LEGACY_PYTHON_SANDBOX_TOOL,
        description=(
            "Compatibility alias for older conversations. Execute Python in the "
            "persistent isolated workspace. New calls should use workspace_shell."
        ),
        tool_type=ToolType.MUTATING,
        pre_guards=["mutating_tool_approval"],
    )
    async def python_sandbox(
        self,
        ctx: ToolContext,
        code: str,
        timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    ) -> ToolResult:
        payload = await _execute_workspace_command(
            ctx,
            workspace_id=self._workspace_id(),
            command="python -",
            cwd="/workspace",
            timeout_seconds=timeout_seconds,
            network=True,
            stdin=code,
        )
        return ToolResult(payload=payload, error="error" in payload)


async def _execute_workspace_command(
    ctx: ToolContext,
    *,
    workspace_id: str,
    command: str,
    cwd: str,
    timeout_seconds: int,
    network: bool,
    stdin: str | None,
) -> dict[str, Any]:
    normalized_timeout = max(1, min(int(timeout_seconds), MAX_TIMEOUT_SECONDS))
    return await ctx.activity(
        _run_workspace_shell_activity,
        args={
            "workspace_id": workspace_id,
            "execution_id": ctx.idempotency_key(command, cwd),
            "command": command,
            "cwd": cwd,
            "timeout_seconds": normalized_timeout,
            "network": network,
            "stdin": stdin,
        },
        schedule_to_start_timeout=timedelta(seconds=30),
        start_to_close_timeout=timedelta(
            seconds=normalized_timeout + ACTIVITY_TIMEOUT_BUFFER_SECONDS
        ),
        schedule_to_close_timeout=timedelta(
            seconds=normalized_timeout + ACTIVITY_TIMEOUT_BUFFER_SECONDS
        ),
        heartbeat_timeout=timedelta(seconds=ACTIVITY_HEARTBEAT_TIMEOUT_SECONDS),
        retry_policy=WORKSPACE_SHELL_RETRY_POLICY,
    )


async def _run_workspace_shell_activity(
    workspace_id: str,
    execution_id: str,
    command: str,
    cwd: str,
    timeout_seconds: int,
    network: bool,
    stdin: str | None,
    *,
    stream: StreamContext,
    activity_ctx: RoutedActivityContext,
) -> dict[str, Any]:
    try:
        request = _validated_request(
            workspace_id=workspace_id,
            execution_id=execution_id,
            command=command,
            cwd=cwd,
            timeout_seconds=timeout_seconds,
            network=network,
            stdin=stdin,
        )
    except ValueError as err:
        payload = {"error": str(err), "type": "WorkspaceShellValidationError"}
        await stream.emit(payload, kind="workspace_shell_rejected")
        return payload

    activity_ctx.heartbeat(
        {
            "phase": "connecting",
            "workspace_id": workspace_id,
            "timeout_seconds": request["timeout_seconds"],
        },
        force=True,
    )

    result: dict[str, Any] | None = None
    try:
        async for event in _executor_events(request):
            kind = str(event.get("kind") or "")
            payload = event.get("payload")
            if not isinstance(payload, dict):
                payload = {"value": payload}

            if kind == "result":
                result = payload
                continue

            stream_kind = f"workspace_shell_{kind or 'event'}"
            await stream.emit(payload, kind=stream_kind)
            activity_ctx.heartbeat(
                {
                    "phase": kind or "running",
                    "workspace_id": workspace_id,
                    "stdout_chars": payload.get("stdout_chars"),
                    "stderr_chars": payload.get("stderr_chars"),
                }
            )
    except Exception as err:
        payload = {
            "error": f"Workspace executor request failed: {err}",
            "type": "WorkspaceExecutorUnavailable",
        }
        await stream.emit(payload, kind="workspace_shell_error")
        activity_ctx.heartbeat({"phase": "failed", "type": payload["type"]})
        return payload

    if result is None:
        result = {
            "error": "Workspace executor closed the response without a result.",
            "type": "WorkspaceExecutorProtocolError",
        }
        await stream.emit(result, kind="workspace_shell_error")

    activity_ctx.heartbeat(
        {
            "phase": "complete",
            "workspace_id": workspace_id,
            "exit_code": result.get("exit_code"),
            "error": "error" in result,
        },
        force=True,
    )
    return result


def _validated_request(
    *,
    workspace_id: str,
    execution_id: str,
    command: str,
    cwd: str,
    timeout_seconds: int,
    network: bool,
    stdin: str | None,
) -> dict[str, Any]:
    if not workspace_id.strip():
        raise ValueError("Workspace identity is not available.")
    if not command.strip():
        raise ValueError("Command is required.")
    if len(command) > MAX_COMMAND_CHARS:
        raise ValueError(f"Command is too large. Max chars: {MAX_COMMAND_CHARS}.")
    if stdin is not None and len(stdin) > MAX_STDIN_CHARS:
        raise ValueError(f"stdin is too large. Max chars: {MAX_STDIN_CHARS}.")

    return {
        "workspace_id": workspace_id,
        "execution_id": execution_id,
        "command": command,
        "cwd": cwd,
        "timeout_seconds": max(1, min(int(timeout_seconds), MAX_TIMEOUT_SECONDS)),
        "network": bool(network),
        "stdin": stdin,
    }


async def _executor_events(request: dict[str, Any]) -> AsyncIterator[dict[str, Any]]:
    url = os.environ.get(
        "WORKSPACE_EXECUTOR_URL", "http://127.0.0.1:8082"
    ).rstrip("/")
    token = os.environ.get("WORKSPACE_EXECUTOR_TOKEN", "").strip()
    if not token:
        raise RuntimeError("WORKSPACE_EXECUTOR_TOKEN is not configured")

    timeout = httpx.Timeout(
        connect=10.0,
        read=float(request["timeout_seconds"] + ACTIVITY_TIMEOUT_BUFFER_SECONDS),
        write=30.0,
        pool=10.0,
    )
    async with httpx.AsyncClient(timeout=timeout) as client:
        async with client.stream(
            "POST",
            f"{url}/execute",
            headers={"Authorization": f"Bearer {token}"},
            json=request,
        ) as response:
            response.raise_for_status()
            async for line in response.aiter_lines():
                if not line.strip():
                    continue
                event = json.loads(line)
                if not isinstance(event, dict):
                    raise RuntimeError("executor returned a non-object event")
                yield event
