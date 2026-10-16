"""Workspace tools backed by the sandbox executor's /files endpoints (PRD A5, CTO-3).

Each tool is a harness activity tool: the harness gates it, publishes its lifecycle events,
and runs the body as a Temporal activity named after the tool. ``workspace_id`` is injected
by the agent from its own session, never chosen by the model.
"""

# No `from __future__ import annotations`: the harness resolves tool type hints against its
# own module globals, so annotations must be real objects, not strings.

import os
from datetime import timedelta
from typing import Any

import httpx
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError
from temporalio.workflow import ActivityConfig

from temporal_agent_harness.harness import agent

READ_CONFIG = ActivityConfig(
    start_to_close_timeout=timedelta(seconds=30),
    retry_policy=RetryPolicy(maximum_attempts=5, maximum_interval=timedelta(seconds=10)),
)
WRITE_CONFIG = ActivityConfig(
    start_to_close_timeout=timedelta(seconds=30),
    retry_policy=RetryPolicy(maximum_attempts=3, maximum_interval=timedelta(seconds=10)),
)

# Error codes from the executor that retrying cannot fix.
_PERMANENT_CODES = frozenset(
    {
        "not_found",
        "is_symlink",
        "outside_workspace",
        "too_large",
        "not_regular_file",
        "match_count",
        "exists",
        "invalid_path",
    }
)


async def _files_call(op: str, body: dict[str, Any]) -> dict[str, Any]:
    url = os.environ.get("WORKSPACE_EXECUTOR_URL", "http://127.0.0.1:8082").rstrip("/")
    token = os.environ.get("WORKSPACE_EXECUTOR_TOKEN", "")
    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.post(
                f"{url}/files/{op}",
                json=body,
                headers={"Authorization": f"Bearer {token}"},
            )
    except httpx.HTTPError as exc:
        # Unreachable executor: retryable, Temporal backs off.
        raise ApplicationError(f"The sandbox isn't answering: {exc}", type="ExecutorUnavailable")
    if response.status_code >= 500:
        raise ApplicationError(
            f"The sandbox failed ({response.status_code}).", type="ExecutorUnavailable"
        )
    data = response.json()
    if response.status_code >= 400:
        detail = data.get("detail", data) if isinstance(data, dict) else data
        code = detail.get("code") if isinstance(detail, dict) else None
        message = detail.get("message") if isinstance(detail, dict) else str(detail)
        # The model can act on these, so they come back as a normal tool failure.
        raise ApplicationError(
            message or f"{op} failed",
            type=f"Workspace_{code or 'error'}",
            non_retryable=code in _PERMANENT_CODES or response.status_code in (401, 403, 404, 422),
        )
    return data


@agent.activity_tool_defn(inherently_safe=True, activity_config=READ_CONFIG)
async def read_file(
    path: str,
    workspace_id: agent.Injected[str],
    offset: int = 0,
    length: int = 200_000,
) -> str:
    """Read a text file from the workspace. `path` is relative to the workspace root.
    Use `offset` and `length` (in bytes) to read part of a large file."""
    data = await _files_call(
        "read",
        {"workspace_id": workspace_id, "path": path, "offset": offset, "length": length},
    )
    if data.get("encoding") == "base64":
        return f"[{path} is a binary file of {data.get('size')} bytes; inspect it with run_command]"
    content = data.get("content", "")
    if data.get("eof") is False:
        size = data.get("size")
        content += f"\n\n[read {len(content)} bytes at offset {offset} of {size}; read more with offset]"
    return content


@agent.activity_tool_defn(activity_config=WRITE_CONFIG, auto_approval_criteria="workspace_edit")
async def write_file(path: str, content: str, workspace_id: agent.Injected[str]) -> str:
    """Create or overwrite a text file in the workspace. Parent directories are created.
    `path` is relative to the workspace root."""
    data = await _files_call(
        "write",
        {
            "workspace_id": workspace_id,
            "path": path,
            "content": content,
            "create_parents": True,
        },
    )
    return f"Wrote {data.get('size', len(content.encode()))} bytes to {path}."


WORKSPACE_TOOLS = (read_file, write_file)
