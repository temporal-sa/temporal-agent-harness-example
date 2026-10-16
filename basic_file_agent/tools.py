"""Two activity-backed file tools, confined to one workspace directory."""

from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path

from temporal_agent_harness.harness import agent
from temporalio.workflow import ActivityConfig

FILE_IO = ActivityConfig(start_to_close_timeout=timedelta(seconds=30))


def _resolve(path: str) -> Path:
    """The file's absolute path, refusing anything outside the workspace."""
    root = Path(os.environ.get("BASIC_FILE_AGENT_WORKSPACE", "./basic_file_agent_workspace")).resolve()
    resolved = (root / path).resolve()
    if not resolved.is_relative_to(root):
        raise ValueError(f"{path!r} is outside the workspace")
    return resolved


@agent.activity_tool_defn(inherently_safe=True, activity_config=FILE_IO)
async def read_file(path: str) -> str:
    """Read a UTF-8 text file. `path` is relative to the workspace root."""
    return _resolve(path).read_text(encoding="utf-8")


@agent.activity_tool_defn(activity_config=FILE_IO)
async def write_file(path: str, content: str) -> str:
    """Create or overwrite a UTF-8 text file. `path` is relative to the workspace root."""
    target = _resolve(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding="utf-8")
    return f"Wrote {len(content)} characters to {path}"


TOOLS = [read_file, write_file]
