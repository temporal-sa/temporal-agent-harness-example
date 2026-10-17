"""Code Mode for the coding agent (PRD A11).

``run_script`` runs a model-written Python script in the harness sandbox (Monty, stepped from
workflow code). The script can call the workspace's read tools as async host functions and read
the workspace as files under ``/workspace``. Built once per run in ``CodingAgent.__init__`` by
:func:`build_code_mode_tool`.

* **Limits** (``harness_ext.ScriptLimits``): at most 16 host-function calls and 64 file
  operations per script, at most 4 of them in flight at once. Over-limit calls raise
  ``RuntimeError`` inside the script.
* **Approval**: ``run_script`` itself is gated (never inherently safe; risk class
  ``read_only``, so the judge may approve it in auto mode). Approving it ("Approve script")
  approves its host calls once: they run without their own gate. Everything it can reach is
  read-only.
* **Events**: each host call and each ``fs_*`` call publishes ``tool_start``/``tool_end``
  (or ``tool_error``) with ``tool_id = "<run_script tool id>/<n>"``, n = 1, 2, ... in start
  order, so the UI nests them under the script row.
* **Type errors** come back to the model as the tool result ``"Script error (...)"`` before
  any host call runs.

``/workspace`` is :class:`ExecutorWorkspaceFS`, a read-only ``ActivityFileSystem`` over the
executor's ``/files/{stat,read,list}``. Its activities (``vfs.executor-workspace.*``) are
registered on the worker from :data:`CODE_MODE_ACTIVITIES`.
"""

# No `from __future__ import annotations`: the harness resolves type hints at runtime.

import base64
import errno
import os
import re
from pathlib import PurePosixPath
from typing import Any

from pydantic import BaseModel
from temporalio import activity
from temporalio.exceptions import ApplicationError

from temporal_agent_harness.harness.code_mode import ActivityFileSystem, FileStat, VFSMount

from simple_chat_agent.agent import harness_ext
from simple_chat_agent.agent.tools.workspace import _files_call, git_diff, git_status, list_files, read_file, search_code

CODE_MODE_TOOL_NAME = "run_script"
WORKSPACE_MOUNT = "/workspace"
SCRIPT_LIMITS = harness_ext.ScriptLimits(max_host_calls=16, max_file_ops=64, concurrency=4)
# The tools a script may call as host functions: the workspace's read-only tools.
HOST_TOOLS = (list_files, read_file, search_code, git_status, git_diff)
# Executor ids: what the API mints for sessions (coding-agent-<hex>) and tests use.
_WORKSPACE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$")

GUIDE = f"""
Limits: at most {SCRIPT_LIMITS.max_host_calls} host-function calls and \
{SCRIPT_LIMITS.max_file_ops} file operations per script, at most {SCRIPT_LIMITS.concurrency} \
running at once; a call over the limit raises RuntimeError. To read many files, read them under \
`{WORKSPACE_MOUNT}` (for example `pathlib.Path("{WORKSPACE_MOUNT}/src/app.py").read_text()`), \
not with read_file. List a directory with `Path.iterdir()` (there is no `glob`/`rglob`), and \
sort or compare paths as `str(p)`, not `Path` objects. `{WORKSPACE_MOUNT}` is read-only: change \
files with the normal tools. \
Return a compact result (counts, short excerpts), not whole files.
"""


class WorkspaceFSConfig(BaseModel):
    """Which workspace ``/workspace`` shows. Untrusted (an activity input): checked on use."""

    workspace_id: str


def _os_error(kind: type[OSError], code: int, path: PurePosixPath) -> OSError:
    return kind(code, os.strerror(code), str(path))


class ExecutorWorkspaceFS(ActivityFileSystem[WorkspaceFSConfig], name="executor-workspace"):
    """The session's workspace, read-only, through the executor's ``/files/*`` API.

    Scoped like every workspace tool (PRD §5 rule 5): the workspace is the calling workflow's
    own. A Code Mode mount runs on the lead agent, whose workspace id is its workflow id, so an
    activity whose config names any other workspace (or that runs outside a workflow, e.g. a
    standalone ``view`` call) is refused. The executor confines paths to the workspace."""

    def __init__(self, config: WorkspaceFSConfig) -> None:
        super().__init__(config)
        workspace_id = config.workspace_id
        if not _WORKSPACE_ID.fullmatch(workspace_id or ""):
            raise ValueError("invalid workspace id")
        caller = activity.info().workflow_id if activity.in_activity() else None
        if caller != workspace_id:
            raise ValueError("this filesystem serves only the calling session's own workspace")
        self._workspace_id = workspace_id

    async def _call(self, op: str, path: PurePosixPath, **extra: Any) -> dict[str, Any] | None:
        try:
            return await _files_call(op, {"workspace_id": self._workspace_id, "path": str(path), **extra})
        except ApplicationError as err:
            if err.type == "Workspace_not_found":
                return None
            if err.type in ("Workspace_protected_path", "Workspace_forbidden"):
                raise _os_error(PermissionError, errno.EACCES, path) from None
            raise

    async def stat(self, path: PurePosixPath) -> FileStat | None:
        data = await self._call("stat", path)
        if data is None:
            return None
        return FileStat(
            is_dir=data.get("type") == "directory",
            size=int(data.get("size") or 0),
            mtime=float(data.get("mtime") or 0.0),
        )

    async def read(self, path: PurePosixPath, offset: int = 0, length: int | None = None) -> bytes:
        data = await self._call("read", path, offset=offset, length=length)
        if data is None:
            raise _os_error(FileNotFoundError, errno.ENOENT, path)
        content = str(data.get("content", ""))
        return base64.b64decode(content) if data.get("encoding") == "base64" else content.encode("utf-8")

    async def list(self, path: PurePosixPath) -> list[str]:
        data = await self._call("list", path)
        if data is None:
            raise _os_error(FileNotFoundError, errno.ENOENT, path)
        return [str(e.get("name", "")) for e in data.get("entries", []) if e.get("name")]


# Registered on the worker next to the workspace tools.
CODE_MODE_ACTIVITIES = list(ExecutorWorkspaceFS.activities)


def build_code_mode_tool(workspace_id: str) -> Any:
    """The ``run_script`` tool for one session: the host tools with ``workspace_id`` injected,
    and ``/workspace`` mounted read-only. Build it once per workflow run (it renders stubs)."""
    mount = VFSMount(
        path=WORKSPACE_MOUNT,
        backend=ExecutorWorkspaceFS.backend(WorkspaceFSConfig(workspace_id=workspace_id)),
        read_only=True,
        description="the session's workspace (the repository you work in), read-only",
    )
    return harness_ext.limited_code_mode_tool(
        HOST_TOOLS,
        name=CODE_MODE_TOOL_NAME,
        injections={"workspace_id": workspace_id},
        mounts=[mount],
        limits=SCRIPT_LIMITS,
        guide=GUIDE,
    )
