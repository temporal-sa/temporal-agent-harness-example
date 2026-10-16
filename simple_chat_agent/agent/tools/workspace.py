"""Workspace tools backed by the sandbox executor (PRD A5, CTO-3).

File tools (``list_files``, ``read_file``, ``write_file``, ``edit_file``) call the executor's
``/files/*`` API. ``search_code`` and the git tools run short commands through ``/execute``
(inside Bubblewrap). Each is a harness activity tool: the harness gates it, publishes its
lifecycle events, and runs the body as a Temporal activity named after the tool.

``run_command`` is different: an inline ``@agent.tool_defn`` whose body runs our own
``run_command_activity`` (registered from :data:`WORKSPACE_ACTIVITIES`). That activity streams
stdout/stderr as ``tool_progress_delta`` events, heartbeats, and is idempotent per tool call: the
executor keys the job by an execution_id derived from the tool id, so a retry after a worker
crash reattaches to the running job (or reads its cached result) instead of starting it again.

``workspace_id`` is always injected by the agent from its own session, never chosen by the model.

Risk classes and retry policies (PRD §5.2, §5.8):

==============  ===================  ===========================================================
Tool            Class / criteria     Retry policy
==============  ===================  ===========================================================
list_files      read_only (safe)     5 attempts
read_file       read_only (safe)     5 attempts
search_code     read_only (safe)     5 attempts; same execution_id, so a retry reattaches
git_status      read_only (safe)     5 attempts; same execution_id
git_diff        read_only (safe)     5 attempts; same execution_id
write_file      workspace_edit       3 attempts; rewriting the same content is idempotent
edit_file       workspace_edit       3 attempts; a retry that finds the edit already applied
                                     reports it instead of failing
git_commit      shell                3 attempts; execution_id per tool call, so the executor
                                     returns the first attempt's cached result (one commit)
run_command     shell                retries only while the executor is unreachable before the
                                     job starts, and after a worker crash; every retry reuses
                                     the execution_id, so it reattaches and never re-runs
==============  ===================  ===========================================================
"""

# No `from __future__ import annotations`: the harness resolves tool type hints against its
# own module globals, so annotations must be real objects, not strings.

import asyncio
import difflib
import hashlib
import json
import os
import shlex
import time
from collections.abc import Callable
from contextlib import suppress
from datetime import timedelta
from typing import Any, Literal

import httpx
from pydantic import BaseModel, Field
from temporalio import activity, workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError
from temporalio.workflow import ActivityConfig

from temporal_agent_harness.harness import agent
from temporal_agent_harness.harness.agent_protocol.events import ToolProgressDelta
from temporal_agent_harness.harness.agent_workflow import AgentWorkflowRunner

# -- Limits ---------------------------------------------------------------------------------

MAX_DIFF_CHARS = 4_000
MAX_COMMAND_OUTPUT_CHARS = 8_000
MAX_GIT_DIFF_CHARS = 8_000
MAX_LIST_RESULTS = 500
MAX_LIST_DIRS = 100
MAX_LIST_TEXT_CHARS = 8_000
MAX_SEARCH_RESULTS = 200
MAX_MATCH_TEXT_CHARS = 300
MAX_CHANGED_FILES = 100
MAX_STATUS_ENTRIES = 200
MAX_COMMIT_MESSAGE_CHARS = 10_000
MAX_COMMAND_CHARS = 100_000
MAX_TIMEOUT_SECONDS = 15 * 60
# A write or edit waits this long for a running command to finish, then fails with a
# definite "workspace busy" (nothing written). Well inside the 20 s client timeout, so a
# client timeout can no longer report failure for a write that lands later.
MUTATION_BUSY_TIMEOUT_SECONDS = 10.0
# A job can wait for the workspace lock behind another command before it starts.
QUEUE_ALLOWANCE_SECONDS = 120
HEARTBEAT_TIMEOUT = timedelta(seconds=15)
HEARTBEAT_EVERY_SECONDS = 1.0  # the SDK throttles what reaches the server
TICK_SECONDS = 0.1  # how often a waiting stream checks for progress flushes and heartbeats
PROGRESS_BATCH_INTERVAL = timedelta(milliseconds=300)
PROGRESS_FLUSH_SECONDS = 0.25
PROGRESS_FLUSH_CHARS = 4_000
MAX_PROGRESS_CHARS = 250_000
STREAM_RECONNECTS = 3
LIST_SKIP_DIRS = frozenset(
    {".git", "node_modules", ".venv", "venv", "__pycache__", ".mypy_cache", ".pytest_cache",
     ".ruff_cache", ".cache", ".tox", "dist", "build"}
)
# The git tools are approved as read-only (status, diff) or without a shell review
# (commit), so no git call they make may run a program the workspace chose (B1). Git can
# run programs from many config keys (filters, diff drivers, hooks, fsmonitor, signing,
# gc/maintenance hooks, pagers, ...) and more appear in new git versions, so instead of
# denylisting them the git tools fail closed:
#
# 1. Preflight (`safe_git_preflight`, run once per tool call, under the workspace lock):
#    the repo-local config (`git config --local`, which does not follow includes) and
#    `config.worktree` may contain only GIT_CONFIG_ALLOWLIST keys; any other key,
#    including include.* / includeIf.*, refuses the call and names the keys. A non-empty
#    `.git/info/attributes` (which nothing can switch off) also refuses.
# 2. Environment: no global or system config or attributes (HOME and XDG_CONFIG_HOME under
#    /tmp, GIT_CONFIG_GLOBAL=/dev/null, GIT_CONFIG_NOSYSTEM=1, GIT_ATTR_NOSYSTEM=1), no
#    repository discovery above the workspace.
# 3. Overrides via GIT_CONFIG_COUNT/KEY_n/VALUE_n (never `-c k=v`, whose `=` splitting
#    a crafted name can abuse): hooks, fsmonitor, signing, auto-gc/maintenance off.
# 4. Attributes read from an empty tree (--attr-source, git >= 2.40), so a worktree
#    `.gitattributes` can name no driver; diffs also pass --no-ext-diff --no-textconv.
#
# The executor also refuses /files writes under `.git/`, `.config/git/` and to a root
# `.gitconfig`, and checks git >= 2.40 at startup.
#
# Why each allowlisted key is safe (none names a program, path to execute, or include):
#   core.repositoryformatversion, core.bare, core.logallrefupdates: written by `git init`;
#     repository layout flags.
#   core.filemode, core.ignorecase, core.precomposeunicode, core.symlinks: written by
#     `git init` per platform; filesystem capability flags.
#   core.autocrlf, core.eol, core.safecrlf: built-in line-ending conversion, no program.
#   user.name, user.email: commit identity strings.
#   remote.<name>.url, remote.<name>.fetch: written by `git clone`. Only fetch/push use
#     them, and the git tools never fetch or push.
#   branch.<name>.remote, .merge, .rebase: written by clone/checkout; upstream tracking.
#   init.defaultbranch: a branch name.
#   extensions.objectformat, extensions.refstorage: repository-format choices (sha256,
#     reftable) git refuses to open a repo without. extensions.worktreeconfig: enables
#     config.worktree, which the preflight checks too. Other extensions (partialclone,
#     which can trigger fetches) are refused.
GIT_CONFIG_ALLOWLIST = (
    "core.repositoryformatversion",
    "core.bare",
    "core.logallrefupdates",
    "core.filemode",
    "core.ignorecase",
    "core.precomposeunicode",
    "core.symlinks",
    "core.autocrlf",
    "core.eol",
    "core.safecrlf",
    "user.name",
    "user.email",
    "remote.*.url",
    "remote.*.fetch",
    "branch.*.remote",
    "branch.*.merge",
    "branch.*.rebase",
    "init.defaultbranch",
    "extensions.objectformat",
    "extensions.refstorage",
    "extensions.worktreeconfig",
)
GIT_CONFIG_OVERRIDES = (
    ("core.hooksPath", "/dev/null"),
    ("core.fsmonitor", "false"),
    ("core.attributesFile", "/dev/null"),
    ("core.pager", "cat"),
    ("core.quotePath", "false"),
    ("commit.gpgsign", "false"),
    ("tag.gpgsign", "false"),
    ("maintenance.auto", "false"),
    ("gc.auto", "0"),
    ("gc.autoDetach", "false"),
    ("color.ui", "false"),
    ("diff.ignoreSubmodules", "all"),
    ("status.submoduleSummary", "false"),
)
GIT_REFUSED_EXIT = 97
GIT_REFUSED_MARKER = "@@safe-git-refused@@"


def _git_prelude() -> str:
    patterns = "|".join(GIT_CONFIG_ALLOWLIST)
    overrides = [f"export GIT_CONFIG_COUNT={len(GIT_CONFIG_OVERRIDES)}"]
    for index, (key, value) in enumerate(GIT_CONFIG_OVERRIDES):
        overrides.append(
            f"export GIT_CONFIG_KEY_{index}={shlex.quote(key)} "
            f"GIT_CONFIG_VALUE_{index}={shlex.quote(value)}"
        )
    return "\n".join(
        [
            "export HOME=/tmp XDG_CONFIG_HOME=/tmp/.config GIT_CONFIG_GLOBAL=/dev/null "
            "GIT_CONFIG_NOSYSTEM=1 GIT_ATTR_NOSYSTEM=1 GIT_CEILING_DIRECTORIES=/ GIT_PAGER=cat",
            "unset GIT_DIR GIT_WORK_TREE GIT_CONFIG GIT_CONFIG_PARAMETERS",
            *overrides,
            r"""safe_git_refuse() {
  printf '%s\n' "__MARKER__" "$@" >&2
  exit __EXIT__
}
safe_git_preflight() {
  command git rev-parse --git-dir >/dev/null 2>&1 || return 0  # not a repo: git says so
  local keys key bad=() path
  keys=$(command git config --local --no-includes --list --name-only 2>/dev/null) \
    || safe_git_refuse "(the repo config could not be read)"
  path=$(command git rev-parse --git-path config.worktree)
  if [ -e "$path" ]; then
    keys+=$'\n'"$(command git config --file "$path" --no-includes --list --name-only 2>/dev/null \
      || echo '(config.worktree could not be read)')"
  fi
  while IFS= read -r key; do
    [ -n "$key" ] || continue
    case "$key" in
      __PATTERNS__) ;;
      *) bad+=("$key") ;;
    esac
  done <<< "$keys"
  path=$(command git rev-parse --git-path info/attributes)
  if [ -e "$path" ] && grep -q '[^[:space:]]' "$path" 2>/dev/null; then
    bad+=("(.git/info/attributes is not empty)")
  fi
  [ ${#bad[@]} -eq 0 ] || safe_git_refuse "${bad[@]}"
}
safe_git() {
  command git --no-pager --attr-source="$(command git hash-object -t tree /dev/null)" "$@"
}
safe_git_preflight""".replace("__MARKER__", GIT_REFUSED_MARKER)
            .replace("__EXIT__", str(GIT_REFUSED_EXIT))
            .replace("__PATTERNS__", patterns),
        ]
    )


GIT_PRELUDE = _git_prelude()
GIT = "safe_git"
_SEPARATOR = "@@@workspace-tool-separator-7f3a@@@"

# -- Activity configs -----------------------------------------------------------------------

READ_CONFIG = ActivityConfig(
    start_to_close_timeout=timedelta(seconds=30),
    retry_policy=RetryPolicy(maximum_attempts=5, maximum_interval=timedelta(seconds=10)),
)
WRITE_CONFIG = ActivityConfig(
    start_to_close_timeout=timedelta(seconds=30),
    retry_policy=RetryPolicy(maximum_attempts=3, maximum_interval=timedelta(seconds=10)),
)
# /execute-backed tools wait for the workspace lock behind a running command, so they get a
# long start-to-close and rely on heartbeats to notice a lost worker.
EXEC_READ_CONFIG = ActivityConfig(
    start_to_close_timeout=timedelta(seconds=MAX_TIMEOUT_SECONDS + QUEUE_ALLOWANCE_SECONDS),
    heartbeat_timeout=HEARTBEAT_TIMEOUT,
    retry_policy=RetryPolicy(maximum_attempts=5, maximum_interval=timedelta(seconds=10)),
)
EXEC_WRITE_CONFIG = ActivityConfig(
    start_to_close_timeout=timedelta(seconds=MAX_TIMEOUT_SECONDS + QUEUE_ALLOWANCE_SECONDS),
    heartbeat_timeout=HEARTBEAT_TIMEOUT,
    retry_policy=RetryPolicy(maximum_attempts=3, maximum_interval=timedelta(seconds=10)),
)
# run_command: "The sandbox isn't answering. Temporal will retry every 5s." Errors after the
# job started are raised non-retryable; the remaining retryable failures (unreachable before
# start, heartbeat timeout after a worker crash) all reattach by execution_id.
RUN_COMMAND_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    backoff_coefficient=1.0,
    maximum_attempts=6,
    non_retryable_error_types=["CommandFailed", "InvalidCommand"],
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
        "not_a_directory",
        "not_empty",
        "invalid_request",
        "protected_path",
        "workspace_busy",
    }
)


# -- Result models --------------------------------------------------------------------------


class ToolResult(BaseModel):
    """Base for structured tool results. ``str(result)`` is the compact model-facing text;
    the fields are for the UI (changed-files panel, diff view)."""

    def model_text(self) -> str:  # pragma: no cover - every subclass overrides
        return self.model_dump_json()

    def __str__(self) -> str:
        return self.model_text()


class FileEntry(BaseModel):
    path: str
    type: Literal["file", "directory", "symlink", "other"]
    size: int | None = None


class ListFilesResult(ToolResult):
    path: str
    recursive: bool
    entries: list[FileEntry]
    truncated: bool = False

    def model_text(self) -> str:
        if not self.entries:
            return f"{self.path}: (empty)"
        lines = []
        for entry in self.entries:
            suffix = "/" if entry.type == "directory" else ""
            size = f"  ({entry.size} B)" if entry.size is not None else ""
            lines.append(f"{entry.path}{suffix}{size}")
        text, cut = _bound_head(
            "\n".join(lines), MAX_LIST_TEXT_CHARS, "list truncated; list a subdirectory"
        )
        if self.truncated and not cut:
            text += f"\n[stopped at {len(self.entries)} entries; list a subdirectory]"
        return text


class WriteResult(ToolResult):
    path: str
    created: bool
    bytes_written: int
    added_lines: int
    removed_lines: int
    diff: str = Field(description=f"Unified diff, at most about {MAX_DIFF_CHARS} chars.")
    diff_truncated: bool = False

    def model_text(self) -> str:
        verb = "Created" if self.created else "Wrote"
        return (
            f"{verb} {self.path} ({self.bytes_written} bytes, "
            f"+{self.added_lines} -{self.removed_lines})."
        )


class EditResult(ToolResult):
    path: str
    added_lines: int
    removed_lines: int
    diff: str = Field(description=f"Unified diff, at most about {MAX_DIFF_CHARS} chars.")
    diff_truncated: bool = False
    already_applied: bool = False

    def model_text(self) -> str:
        head = f"Edited {self.path} (+{self.added_lines} -{self.removed_lines})"
        if self.already_applied:
            head += "; the edit had already been applied by an earlier attempt"
        return f"{head}.\n{self.diff}" if self.diff else f"{head}."


class SearchMatch(BaseModel):
    path: str
    line: int
    text: str


class SearchResult(ToolResult):
    pattern: str
    matches: list[SearchMatch]
    truncated: bool = False
    error: str | None = None

    def model_text(self) -> str:
        if self.error and not self.matches:
            return f"search_code failed: {self.error}"
        if not self.matches:
            return f"No matches for {self.pattern!r}."
        lines = [f"{m.path}:{m.line}: {m.text}" for m in self.matches]
        if self.truncated:
            lines.append(f"[stopped at {len(self.matches)} matches; narrow the search]")
        if self.error:
            lines.append(f"[rg: {self.error}]")
        return "\n".join(lines)


class ChangedFile(BaseModel):
    path: str
    status: Literal["created", "modified", "deleted"]


class CommandResult(ToolResult):
    command: str
    exit_code: int | None
    stdout_tail: str = Field(description="Head and tail of stdout, bounded.")
    stderr_tail: str = Field(description="Head and tail of stderr, bounded.")
    truncated: bool = False
    duration_s: float = 0.0
    timed_out: bool = False
    changed_files: list[ChangedFile] = Field(default_factory=list)
    changed_files_total: int = 0
    error: str | None = None
    error_type: str | None = None
    log_ref: str = Field(default="", description="Executor execution_id holding the full log.")
    attempt: int = 1
    reattached: bool = False

    def model_text(self) -> str:
        shown = self.command if len(self.command) <= 200 else self.command[:200] + "…"
        status = "exit ?" if self.exit_code is None else f"exit {self.exit_code}"
        parts = [f"$ {shown}", f"{status} · {self.duration_s:.1f}s"]
        if self.timed_out:
            parts[-1] += " · timed out"
        if self.reattached:
            parts[-1] += f" · reattached (attempt {self.attempt})"
        if self.error and not self.timed_out and self.error_type != "WorkspaceShellProcessError":
            parts.append(f"error: {self.error}")
        if self.stdout_tail:
            parts.append("stdout:\n" + self.stdout_tail.rstrip("\n"))
        if self.stderr_tail:
            parts.append("stderr:\n" + self.stderr_tail.rstrip("\n"))
        if self.truncated:
            parts.append(f"[output truncated; full log: {self.log_ref}]")
        if self.changed_files:
            marks = {"created": "A", "modified": "M", "deleted": "D"}
            listed = ", ".join(f"{marks[c.status]} {c.path}" for c in self.changed_files[:20])
            more = self.changed_files_total - min(20, len(self.changed_files))
            parts.append("changed: " + listed + (f" (+{more} more)" if more > 0 else ""))
        return "\n".join(parts)


class GitStatusEntry(BaseModel):
    path: str
    status: str = Field(description="Two-letter porcelain v1 code, e.g. ' M', '??', 'A '.")
    original_path: str | None = None


class GitStatusResult(ToolResult):
    branch: str | None = None
    entries: list[GitStatusEntry] = Field(default_factory=list)
    clean: bool = True
    truncated: bool = False
    error: str | None = None

    def model_text(self) -> str:
        if self.error:
            return f"git status failed: {self.error}"
        head = f"On branch {self.branch}" if self.branch else "git status"
        if self.clean:
            return f"{head}; working tree clean."
        lines = [head]
        for entry in self.entries:
            rename = f"{entry.original_path} -> " if entry.original_path else ""
            lines.append(f"{entry.status} {rename}{entry.path}")
        if self.truncated:
            lines.append(f"[showing {len(self.entries)} entries]")
        return "\n".join(lines)


class GitDiffFile(BaseModel):
    path: str
    added: int | None = Field(default=None, description="None for a binary file.")
    removed: int | None = None


class GitDiffResult(ToolResult):
    staged: bool
    files: list[GitDiffFile] = Field(default_factory=list)
    diff: str = ""
    truncated: bool = False
    error: str | None = None

    def model_text(self) -> str:
        if self.error:
            return f"git diff failed: {self.error}"
        if not self.files and not self.diff:
            return "No staged changes." if self.staged else "No unstaged changes."
        return self.diff or "\n".join(
            f"{f.path} +{f.added if f.added is not None else '?'} "
            f"-{f.removed if f.removed is not None else '?'}"
            for f in self.files
        )


class GitCommitResult(ToolResult):
    committed: bool
    commit: str | None = None
    summary: str = ""
    files_changed: int = 0
    insertions: int = 0
    deletions: int = 0
    output: str = ""

    def model_text(self) -> str:
        if not self.committed:
            return f"Nothing committed. {self.output}".strip()
        return (
            f"Committed {self.commit[:12] if self.commit else '?'} {self.summary!r} "
            f"({self.files_changed} files, +{self.insertions} -{self.deletions})."
        )


# -- Bounding and diff helpers --------------------------------------------------------------


def _bound_head(text: str, limit: int, note: str) -> tuple[str, bool]:
    if len(text) <= limit:
        return text, False
    cut = text.rfind("\n", 0, limit)
    cut = cut if cut > limit // 2 else limit
    return text[:cut] + f"\n[... {len(text) - cut} more chars; {note}]", True


def _bound_head_tail(text: str, limit: int, head_share: float = 0.25) -> tuple[str, bool]:
    """Keep the start and the end of ``text`` (errors usually sit at the end)."""
    if len(text) <= limit:
        return text, False
    marker_room = 48
    head = max(0, int((limit - marker_room) * head_share))
    tail = max(0, limit - marker_room - head)
    omitted = len(text) - head - tail
    return f"{text[:head]}\n[... {omitted} chars omitted ...]\n{text[len(text) - tail:]}", True


def _split_output_budget(stdout: str, stderr: str, total: int) -> tuple[int, int]:
    """Share ``total`` between the two streams, giving unused room to the other one."""
    half = total // 2
    if len(stdout) + len(stderr) <= total:
        return len(stdout), len(stderr)
    if len(stdout) < half:
        return len(stdout), total - len(stdout)
    if len(stderr) < half:
        return total - len(stderr), len(stderr)
    return half, total - half


def bounded_output(stdout: str, stderr: str, total: int = MAX_COMMAND_OUTPUT_CHARS) -> tuple[str, str, bool]:
    out_limit, err_limit = _split_output_budget(stdout, stderr, total)
    out, out_cut = _bound_head_tail(stdout, out_limit)
    err, err_cut = _bound_head_tail(stderr, err_limit)
    return out, err, out_cut or err_cut


def unified_diff(path: str, before: str, after: str, limit: int = MAX_DIFF_CHARS) -> tuple[str, int, int, bool]:
    """Return ``(diff, added_lines, removed_lines, truncated)``; counts cover the full diff."""
    lines = list(
        difflib.unified_diff(
            before.splitlines(keepends=True),
            after.splitlines(keepends=True),
            fromfile=f"a/{path}",
            tofile=f"b/{path}",
            n=3,
        )
    )
    added = sum(1 for line in lines if line.startswith("+") and not line.startswith("+++"))
    removed = sum(1 for line in lines if line.startswith("-") and not line.startswith("---"))
    text = "".join(line if line.endswith("\n") else line + "\n\\ No newline at end of file\n" for line in lines)
    bounded, cut = _bound_head(text, limit, "diff truncated")
    return bounded, added, removed, cut


def _check_relative_path(path: str, what: str = "path") -> str:
    path = (path or ".").strip()
    if path.startswith("/") or ".." in path.split("/") or "\x00" in path:
        raise ApplicationError(
            f"{what} must be relative to the workspace root and must not contain '..'.",
            type="InvalidArgument",
            non_retryable=True,
        )
    return path


def _execution_id(kind: str, *parts: str) -> str:
    """An executor execution_id (``^[A-Za-z0-9_.:-]{1,160}$``) stable across retries."""
    digest = hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()[:40]
    return f"{kind}:{digest}"


def _activity_execution_id(kind: str) -> str:
    # The run id matters: activity ids restart at 1 after continue-as-new. All three are
    # stable across retries of one activity.
    info = activity.info()
    return _execution_id(kind, info.workflow_id or "", info.workflow_run_id or "", info.activity_id)


# -- Executor client ------------------------------------------------------------------------


def _executor_url() -> str:
    return os.environ.get("WORKSPACE_EXECUTOR_URL", "http://127.0.0.1:8082").rstrip("/")


def _executor_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {os.environ.get('WORKSPACE_EXECUTOR_TOKEN', '')}"}


def _unavailable(detail: Any) -> ApplicationError:
    return ApplicationError(
        f"The sandbox isn't answering ({detail}). Temporal will retry every 5s.",
        type="ExecutorUnavailable",
    )


async def _files_call(op: str, body: dict[str, Any]) -> dict[str, Any]:
    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.post(
                f"{_executor_url()}/files/{op}", json=body, headers=_executor_headers()
            )
    except httpx.HTTPError as exc:
        # Unreachable executor: retryable, Temporal backs off.
        raise _unavailable(exc)
    if response.status_code >= 500:
        raise ApplicationError(
            f"The sandbox failed ({response.status_code}).", type="ExecutorUnavailable"
        )
    try:
        data = response.json()
    except ValueError:
        # A non-JSON answer from a misbehaving proxy or executor: not something to retry.
        raise ApplicationError(
            f"The sandbox returned an unreadable response ({response.status_code}).",
            type="ExecutorBadResponse",
            non_retryable=response.status_code < 500,
        )
    if response.status_code >= 400:
        detail = data.get("detail", data) if isinstance(data, dict) else data
        code = detail.get("code") if isinstance(detail, dict) else None
        message = detail.get("message") if isinstance(detail, dict) else str(detail)
        # The model can act on these, so they come back as a normal tool failure.
        raise ApplicationError(
            message or f"{op} failed",
            {"code": code},
            type=f"Workspace_{code or 'error'}",
            non_retryable=code in _PERMANENT_CODES or response.status_code in (401, 403, 404, 422),
        )
    return data


async def _read_text_if_exists(workspace_id: str, path: str) -> str | None:
    """Current text of ``path`` for diffing, or None when absent. Raises for other errors."""
    try:
        data = await _files_call("read", {"workspace_id": workspace_id, "path": path})
    except ApplicationError as err:
        if err.type == "Workspace_not_found":
            return None
        raise
    if data.get("encoding") == "base64":
        raise ApplicationError(
            f"{path} is a binary file; it can't be diffed or edited as text.",
            type="Workspace_binary",
            non_retryable=True,
        )
    return str(data.get("content", ""))


def _error_code(err: ApplicationError) -> str | None:
    details = err.details or ()
    if details and isinstance(details[0], dict):
        return details[0].get("code")
    return err.type.removeprefix("Workspace_") if err.type else None


async def _stream_execute(
    body: dict[str, Any],
    *,
    on_event: Callable[[str, dict[str, Any]], None] | None = None,
    on_tick: Callable[[], None] | None = None,
    resume_seq: int = -1,
    seq_sink: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], bool]:
    """POST /execute and follow its NDJSON stream to the result.

    Returns ``(result_payload, attached)``. ``on_event(kind, payload)`` sees every stream
    event after ``resume_seq`` (``stdout``/``stderr``/``progress``/``dropped``); ``on_tick``
    runs after each event and every :data:`TICK_SECONDS` while waiting, for heartbeats and
    progress flushes.

    Failures before the executor accepts the job raise a retryable ``ExecutorUnavailable``.
    If the stream breaks after that, this reconnects with the same execution_id (the
    executor reattaches) up to :data:`STREAM_RECONNECTS` times before giving up with a
    non-retryable ``CommandFailed``.
    """
    timeout = httpx.Timeout(connect=10.0, read=None, write=30.0, pool=10.0)
    accepted = False
    attached = False
    last_seq = resume_seq
    reconnects = 0
    while True:
        queue: asyncio.Queue[tuple[str, Any]] = asyncio.Queue()

        async def reader() -> None:
            try:
                async with httpx.AsyncClient(timeout=timeout) as client:
                    async with client.stream(
                        "POST", f"{_executor_url()}/execute", json=body, headers=_executor_headers()
                    ) as response:
                        if response.status_code >= 400:
                            await response.aread()
                            await queue.put(("http_error", response))
                            return
                        async for line in response.aiter_lines():
                            if line.strip():
                                await queue.put(("event", json.loads(line)))
                await queue.put(("eof", None))
            except Exception as exc:  # noqa: BLE001 - handed to the consumer
                await queue.put(("exception", exc))

        task = asyncio.create_task(reader())
        try:
            while True:
                try:
                    kind, item = await asyncio.wait_for(queue.get(), TICK_SECONDS)
                except asyncio.TimeoutError:
                    if on_tick:
                        on_tick()
                    continue
                if kind == "event":
                    event_kind = str(item.get("kind") or "")
                    payload = item.get("payload") if isinstance(item.get("payload"), dict) else {}
                    if event_kind == "start":
                        accepted = True
                        attached = attached or bool(payload.get("attached"))
                        continue
                    if event_kind == "result":
                        return payload, attached
                    if event_kind == "dropped" and isinstance(payload.get("next_seq"), int):
                        if payload["next_seq"] - 1 <= last_seq:
                            continue  # nothing this client is missing was dropped
                    seq = item.get("seq")
                    if isinstance(seq, int):
                        if seq <= last_seq:
                            continue  # already delivered before a reconnect or a retry
                        last_seq = seq
                        if seq_sink is not None:
                            seq_sink["seq"] = seq
                    if on_event:
                        on_event(event_kind, payload)
                    if on_tick:
                        on_tick()
                    continue
                if kind == "http_error":
                    _raise_http_error(item, accepted)
                # eof without a result, or a transport exception
                if not accepted:
                    raise _unavailable(item if kind == "exception" else "stream closed early")
                break
        finally:
            if not task.done():
                task.cancel()
                with suppress(asyncio.CancelledError, Exception):
                    await task
        reconnects += 1
        if reconnects > STREAM_RECONNECTS:
            raise ApplicationError(
                "Lost the connection to the sandbox while the command was running.",
                type="CommandFailed",
                non_retryable=True,
            )
        await asyncio.sleep(min(2.0, 0.5 * reconnects))


def _raise_http_error(response: httpx.Response, accepted: bool) -> None:
    status = response.status_code
    try:
        detail = response.json().get("detail")
    except ValueError:
        detail = response.text[:300]
    if status >= 500 and not accepted:
        raise _unavailable(f"HTTP {status}: {detail}")
    raise ApplicationError(
        f"The sandbox refused the command (HTTP {status}): {detail}",
        type="InvalidCommand" if status in (400, 409, 422) else "CommandFailed",
        non_retryable=True,
    )


class ExecutionLogNotFound(LookupError):
    """The executor holds no output for this execution id in this workspace (unknown id,
    another workspace's id, or expired by the state TTL)."""


class ExecutorRequestError(RuntimeError):
    """The executor was unreachable or answered with an unexpected error."""


async def read_execution_log(
    workspace_id: str,
    execution_id: str,
    *,
    stream: Literal["stdout", "stderr", "both"] = "both",
    offset: int = 0,
    limit: int = 262_144,
) -> dict[str, Any]:
    """Read a command's stored output (``CommandResult.log_ref``) from the executor (S7).

    A plain async function for the API process, not a tool: call it only after checking
    that the caller owns ``workspace_id``. The executor scopes ids to the workspace.

    Returns the executor's page: ``text``, ``offset``, ``next_offset`` (None when
    everything is read and the job finished), ``total_chars``, ``finished``, ``exit_code``,
    ``timed_out``, ``error_type``, ``truncated_at_capture`` and ``interleaved``.

    Raises ExecutionLogNotFound (show "Output no longer retained") or
    ExecutorRequestError.
    """
    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.post(
                f"{_executor_url()}/execute/log",
                json={
                    "workspace_id": workspace_id,
                    "execution_id": execution_id,
                    "stream": stream,
                    "offset": max(0, int(offset)),
                    "limit": max(1, min(int(limit), 262_144)),
                },
                headers=_executor_headers(),
            )
    except httpx.HTTPError as exc:
        raise ExecutorRequestError(f"The sandbox isn't answering: {exc}") from exc
    if response.status_code == 404:
        raise ExecutionLogNotFound(execution_id)
    if response.status_code == 422:
        # A malformed id cannot exist in any workspace.
        raise ExecutionLogNotFound(execution_id)
    if response.status_code >= 400:
        raise ExecutorRequestError(
            f"The sandbox refused the log request (HTTP {response.status_code})."
        )
    try:
        return dict(response.json())
    except ValueError as exc:
        raise ExecutorRequestError("The sandbox returned an unreadable log page.") from exc


async def _cancel_execution(workspace_id: str, execution_id: str) -> None:
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            await client.post(
                f"{_executor_url()}/execute/cancel",
                json={"workspace_id": workspace_id, "execution_id": execution_id},
                headers=_executor_headers(),
            )
    except httpx.HTTPError:
        # Best effort: the executor stops an unattended job after its orphan grace period.
        pass


def _cancel_was_requested() -> bool:
    details = activity.cancellation_details()
    return bool(
        details is not None
        and details.cancel_requested
        and not (details.timed_out or details.worker_shutdown)
    )


def _heartbeat_ticker(details: Callable[[], Any]) -> Callable[[], None]:
    last = [0.0]

    def tick() -> None:
        now = time.monotonic()
        if now - last[0] >= HEARTBEAT_EVERY_SECONDS:
            last[0] = now
            activity.heartbeat(details())

    return tick


async def _exec_collect(
    workspace_id: str,
    kind: str,
    command: str,
    *,
    timeout_seconds: int = 60,
    stdin: str | None = None,
    network: bool = False,
) -> dict[str, Any]:
    """Run a short command for a tool through /execute and return the executor result.

    The execution_id comes from the activity id, so a retried activity reattaches to (or
    reads the cached result of) the same job.
    """
    body = {
        "workspace_id": workspace_id,
        "execution_id": _activity_execution_id(kind),
        "command": command,
        "cwd": "/workspace",
        "timeout_seconds": timeout_seconds,
        "network": network,
        "stdin": stdin,
    }
    result, _ = await _stream_execute(body, on_tick=_heartbeat_ticker(lambda: {"phase": kind}))
    if result.get("type") == "WorkspaceSandboxError":
        raise _unavailable(result.get("error"))
    return result


# -- File tools -----------------------------------------------------------------------------


@agent.activity_tool_defn(inherently_safe=True, activity_config=READ_CONFIG)
async def list_files(
    workspace_id: agent.Injected[str],
    path: str = ".",
    recursive: bool = False,
) -> ListFilesResult:
    """List a workspace directory. `path` is relative to the workspace root. With
    `recursive=true`, walks subdirectories (skipping .git, node_modules, .venv and caches)
    up to 500 entries."""
    root = _check_relative_path(path)
    entries: list[FileEntry] = []
    pending = [root]
    visited = 0
    truncated = False
    while pending:
        directory = pending.pop(0)
        visited += 1
        data = await _files_call("list", {"workspace_id": workspace_id, "path": directory})
        truncated = truncated or bool(data.get("truncated"))
        for item in data.get("entries", []):
            name = item.get("name", "")
            full = name if directory in ("", ".") else f"{directory.rstrip('/')}/{name}"
            kind = item.get("type") if item.get("type") in ("file", "directory", "symlink") else "other"
            entries.append(FileEntry(path=full, type=kind, size=item.get("size")))
            if recursive and kind == "directory" and name not in LIST_SKIP_DIRS:
                pending.append(full)
            if len(entries) >= MAX_LIST_RESULTS:
                break
        if len(entries) >= MAX_LIST_RESULTS or (recursive and visited >= MAX_LIST_DIRS and pending):
            truncated = truncated or bool(pending) or len(entries) >= MAX_LIST_RESULTS
            break
    return ListFilesResult(path=root, recursive=recursive, entries=entries, truncated=truncated)


@agent.activity_tool_defn(inherently_safe=True, activity_config=READ_CONFIG)
async def read_file(
    path: str,
    workspace_id: agent.Injected[str],
    offset: int = 0,
    length: int = 64_000,
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
        size, got = data.get("size"), data.get("length", 0)
        content += (
            f"\n\n[showing bytes {offset}-{offset + got} of {size}; "
            f"continue with offset={offset + got}]"
        )
    return content


@agent.activity_tool_defn(activity_config=WRITE_CONFIG, auto_approval_criteria="workspace_edit")
async def write_file(path: str, content: str, workspace_id: agent.Injected[str]) -> WriteResult:
    """Create or overwrite a text file in the workspace. Parent directories are created.
    `path` is relative to the workspace root. Prefer edit_file for changes to an existing file."""
    try:
        before = await _read_text_if_exists(workspace_id, path)
    except ApplicationError as err:
        if _error_code(err) not in ("too_large", "binary"):
            raise
        before = ""  # too big or binary to diff: count the new text only
    data = await _files_call(
        "write",
        {
            "workspace_id": workspace_id,
            "path": path,
            "content": content,
            "create_parents": True,
            "busy_timeout_seconds": MUTATION_BUSY_TIMEOUT_SECONDS,
        },
    )
    shown = data.get("path", path)
    diff, added, removed, cut = unified_diff(shown, before or "", content)
    return WriteResult(
        path=shown,
        created=bool(data.get("created", before is None)),
        bytes_written=int(data.get("bytes_written", len(content.encode()))),
        added_lines=added,
        removed_lines=removed,
        diff=diff,
        diff_truncated=cut,
    )


@agent.activity_tool_defn(activity_config=WRITE_CONFIG, auto_approval_criteria="workspace_edit")
async def edit_file(
    path: str,
    old_string: str,
    new_string: str,
    workspace_id: agent.Injected[str],
) -> EditResult:
    """Replace `old_string` with `new_string` in a workspace file. `old_string` must match
    the file's text exactly (whitespace included) and occur exactly once; include enough
    surrounding lines to make it unique. `path` is relative to the workspace root."""
    if not old_string:
        raise ApplicationError(
            "old_string must not be empty; use write_file to create a file.",
            type="InvalidArgument",
            non_retryable=True,
        )
    if old_string == new_string:
        raise ApplicationError(
            "old_string and new_string are identical; nothing to change.",
            type="InvalidArgument",
            non_retryable=True,
        )
    before = await _read_text_if_exists(workspace_id, path)
    if before is None:
        raise ApplicationError(f"{path} does not exist.", type="Workspace_not_found", non_retryable=True)
    try:
        data = await _files_call(
            "edit",
            {
                "workspace_id": workspace_id,
                "path": path,
                "old_string": old_string,
                "new_string": new_string,
                "busy_timeout_seconds": MUTATION_BUSY_TIMEOUT_SECONDS,
            },
        )
    except ApplicationError as err:
        # A retry after an edit that landed but whose response was lost: old_string is
        # gone and new_string is there once. Report the earlier edit instead of failing.
        if (
            _error_code(err) == "match_count"
            and activity.info().attempt > 1
            and new_string
            and before.count(old_string) == 0
            and before.count(new_string) == 1
        ):
            previous = before.replace(new_string, old_string, 1)
            diff, added, removed, cut = unified_diff(path, previous, before)
            return EditResult(
                path=path, added_lines=added, removed_lines=removed, diff=diff,
                diff_truncated=cut, already_applied=True,
            )
        raise
    shown = data.get("path", path)
    after = before.replace(old_string, new_string, 1)
    diff, added, removed, cut = unified_diff(shown, before, after)
    return EditResult(path=shown, added_lines=added, removed_lines=removed, diff=diff, diff_truncated=cut)


# -- search_code ----------------------------------------------------------------------------


def search_command(pattern: str, path: str, glob: str | None, max_results: int) -> str:
    args = [
        "rg", "--json", "--max-count", str(max_results), "--max-filesize", "1M",
        "--hidden", "--glob", "!.git",
    ]
    if glob:
        args += ["--glob", glob]
    args += ["--regexp", pattern, "--", path]
    return " ".join(shlex.quote(a) for a in args)


def parse_search_output(stdout: str, max_results: int) -> tuple[list[SearchMatch], bool]:
    matches: list[SearchMatch] = []
    truncated = False
    for line in stdout.splitlines():
        try:
            event = json.loads(line)
        except ValueError:
            truncated = True  # the executor cut the output mid-line
            continue
        if event.get("type") != "match":
            continue
        if len(matches) >= max_results:
            truncated = True
            break
        data = event.get("data", {})
        file_path = (data.get("path") or {}).get("text") or "?"
        text = ((data.get("lines") or {}).get("text") or "").rstrip("\n")
        if len(text) > MAX_MATCH_TEXT_CHARS:
            text = text[:MAX_MATCH_TEXT_CHARS] + "…"
        matches.append(
            SearchMatch(path=file_path.removeprefix("./"), line=int(data.get("line_number") or 0), text=text)
        )
    return matches, truncated


@agent.activity_tool_defn(inherently_safe=True, activity_config=EXEC_READ_CONFIG)
async def search_code(
    pattern: str,
    workspace_id: agent.Injected[str],
    path: str = ".",
    glob: str | None = None,
    max_results: int = 50,
) -> SearchResult:
    """Search the workspace with ripgrep. `pattern` is a regular expression (Rust regex
    syntax). `path` limits the search to a file or directory; `glob` filters file names
    (for example "*.py" or "!tests/**"). Respects .gitignore. Returns up to `max_results`
    matching lines (at most 200)."""
    if not pattern:
        raise ApplicationError("pattern must not be empty.", type="InvalidArgument", non_retryable=True)
    target = _check_relative_path(path)
    limit = max(1, min(int(max_results), MAX_SEARCH_RESULTS))
    result = await _exec_collect(
        workspace_id, "search_code", search_command(pattern, target, glob, limit)
    )
    matches, cut = parse_search_output(str(result.get("stdout", "")), limit)
    exit_code = result.get("exit_code")
    error = None
    if exit_code not in (0, 1):
        error = (str(result.get("stderr", "")).strip() or str(result.get("error", "")))[:500]
        if exit_code == 127:
            error = "ripgrep (rg) is not installed in the sandbox."
    return SearchResult(
        pattern=pattern,
        matches=matches,
        truncated=cut or bool(result.get("stdout_truncated")),
        error=error or None,
    )


# -- run_command ----------------------------------------------------------------------------


class RunCommandInput(BaseModel):
    workspace_id: str
    command: str
    cwd: str = "."
    timeout_seconds: int = 300
    tool_ctx: agent.AgentToolContext


class _ProgressCoalescer:
    """Turn many small stdout/stderr chunks into fewer ``tool_progress_delta`` events."""

    def __init__(
        self,
        publish: Callable[[str], None],
        *,
        flush_seconds: float = PROGRESS_FLUSH_SECONDS,
        flush_chars: int = PROGRESS_FLUSH_CHARS,
        max_chars: int = MAX_PROGRESS_CHARS,
    ) -> None:
        self._publish = publish
        self._flush_seconds = flush_seconds
        self._flush_chars = flush_chars
        self._budget = max_chars
        self._pending: list[str] = []
        self._pending_chars = 0
        self._first_at = 0.0
        self._capped = False
        self.published_chars = 0

    def add(self, text: str) -> None:
        if not text:
            return
        if self._budget <= 0:
            if not self._capped:
                self._capped = True
                self._pending.append("\n[live output limit reached; the result has the tail]\n")
                self.flush()
            return
        text = text[: self._budget]
        self._budget -= len(text)
        if not self._pending:
            self._first_at = time.monotonic()
        self._pending.append(text)
        self._pending_chars += len(text)
        if self._pending_chars >= self._flush_chars:
            self.flush()

    def note(self, text: str) -> None:
        self._pending.append(text)
        self.flush()

    def maybe_flush(self) -> None:
        if self._pending and time.monotonic() - self._first_at >= self._flush_seconds:
            self.flush()

    def flush(self) -> None:
        if not self._pending:
            return
        text = "".join(self._pending)
        self._pending.clear()
        self._pending_chars = 0
        self.published_chars += len(text)
        self._publish(text)


def command_result(
    command: str,
    payload: dict[str, Any],
    *,
    log_ref: str,
    attempt: int = 1,
    reattached: bool = False,
) -> CommandResult:
    """Build the bounded CommandResult from an executor result payload."""
    stdout, stderr, cut = bounded_output(str(payload.get("stdout") or ""), str(payload.get("stderr") or ""))
    changed: list[ChangedFile] = []
    files = payload.get("files_changed") if isinstance(payload.get("files_changed"), dict) else {}
    for status in ("modified", "created", "deleted"):
        for name in files.get(status, []) or []:
            if name == ".git" or name.startswith(".git/"):
                continue
            changed.append(ChangedFile(path=str(name), status=status))  # type: ignore[arg-type]
    exit_code = payload.get("exit_code")
    return CommandResult(
        command=command,
        exit_code=exit_code if isinstance(exit_code, int) else None,
        stdout_tail=stdout,
        stderr_tail=stderr,
        truncated=cut or any(
            bool(payload.get(k)) for k in ("stdout_truncated", "stderr_truncated")
        ),
        duration_s=float(payload.get("elapsed_seconds") or 0.0),
        timed_out=bool(payload.get("timed_out")),
        changed_files=changed[:MAX_CHANGED_FILES],
        changed_files_total=len(changed),
        error=payload.get("error"),
        error_type=payload.get("type"),
        log_ref=log_ref,
        attempt=attempt,
        reattached=reattached,
    )


@activity.defn(name="workspace.run_command")
async def run_command_activity(request: RunCommandInput) -> CommandResult:
    """Run one shell command in the workspace sandbox, streaming its output to the turn."""
    info = activity.info()
    tool_ctx = request.tool_ctx
    execution_id = _execution_id(
        "run_command", info.workflow_id or "", info.workflow_run_id or "", tool_ctx.tool_id
    )
    resume_seq = -1
    if info.attempt > 1 and info.heartbeat_details:
        last = info.heartbeat_details[-1]
        if isinstance(last, dict) and isinstance(last.get("seq"), int):
            resume_seq = last["seq"]
    body = {
        "workspace_id": request.workspace_id,
        "execution_id": execution_id,
        "command": request.command,
        "cwd": request.cwd or ".",
        "timeout_seconds": max(1, min(int(request.timeout_seconds), MAX_TIMEOUT_SECONDS)),
        "network": True,
    }
    state = {"seq": resume_seq, "execution_id": execution_id}

    async with AgentWorkflowRunner.publisher_from_activity(
        tool_ctx.stream_context, batch_interval=PROGRESS_BATCH_INTERVAL
    ) as pub:

        def publish(text: str) -> None:
            pub.publish(
                ToolProgressDelta(tool_id=tool_ctx.tool_id, tool_name="run_command", progress_delta=text)
            )

        progress = _ProgressCoalescer(publish)
        beat = _heartbeat_ticker(lambda: dict(state))
        if info.attempt > 1:
            progress.note(f"\n[worker restarted; reattaching to the command (attempt {info.attempt})]\n")

        def on_event(kind: str, payload: dict[str, Any]) -> None:
            if kind in ("stdout", "stderr"):
                progress.add(str(payload.get("text") or ""))
            elif kind == "dropped":
                progress.note(f"\n{payload.get('message') or '[earlier output dropped]'}\n")

        def on_tick() -> None:
            progress.maybe_flush()
            beat()

        try:
            # The heartbeat carries the last delivered seq, so a retry skips what this
            # attempt already published.
            result, attached = await _stream_execute(
                body,
                on_event=on_event,
                on_tick=on_tick,
                resume_seq=resume_seq,
                seq_sink=state,
            )
        except asyncio.CancelledError:
            # Stop the job only when the workflow asked (user interrupt). A worker shutdown
            # or a zombie attempt whose heartbeat timed out must leave it running: the
            # next attempt reattaches to it.
            if _cancel_was_requested():
                await asyncio.shield(_cancel_execution(request.workspace_id, execution_id))
            raise
        finally:
            progress.flush()

    if result.get("type") == "WorkspaceSandboxError":
        raise ApplicationError(str(result.get("error")), type="CommandFailed", non_retryable=True)
    return command_result(
        request.command,
        result,
        log_ref=execution_id,
        attempt=info.attempt,
        reattached=attached or info.attempt > 1,
    )


@agent.tool_defn(auto_approval_criteria="shell")
async def run_command(
    command: str,
    workspace_id: agent.Injected[str],
    cwd: str = ".",
    timeout_seconds: int = 300,
) -> CommandResult:
    """Run a bash command in the workspace sandbox (cwd is relative to the workspace root).
    Output streams live; the result has the exit code, the head and tail of stdout and
    stderr, and the files the command changed. `timeout_seconds` is at most 900. Use it to
    run tests, builds and scripts; prefer the file and git tools for reading and editing."""
    if not command.strip():
        raise ApplicationError("command must not be empty.", type="InvalidCommand", non_retryable=True)
    if len(command) > MAX_COMMAND_CHARS:
        raise ApplicationError(
            f"command is longer than {MAX_COMMAND_CHARS} chars.", type="InvalidCommand", non_retryable=True
        )
    timeout = max(1, min(int(timeout_seconds), MAX_TIMEOUT_SECONDS))
    return await workflow.execute_activity(
        run_command_activity,
        RunCommandInput(
            workspace_id=workspace_id,
            command=command,
            cwd=_check_relative_path(cwd, "cwd"),
            timeout_seconds=timeout,
            tool_ctx=agent.AgentToolContext.for_current_tool_id(),
        ),
        start_to_close_timeout=timedelta(seconds=timeout + QUEUE_ALLOWANCE_SECONDS),
        heartbeat_timeout=HEARTBEAT_TIMEOUT,
        retry_policy=RUN_COMMAND_RETRY_POLICY,
        summary="run_command",
    )


# -- git ------------------------------------------------------------------------------------


def parse_git_status(stdout: str) -> GitStatusResult:
    branch = None
    entries: list[GitStatusEntry] = []
    truncated = False
    for line in stdout.splitlines():
        if line.startswith("## "):
            branch = line[3:].split("...")[0].strip()
            if branch.startswith("No commits yet on "):
                branch = branch.removeprefix("No commits yet on ")
            continue
        if len(line) < 4:
            continue
        if len(entries) >= MAX_STATUS_ENTRIES:
            truncated = True
            break
        code, rest = line[:2], line[3:]
        original = None
        if " -> " in rest and code[0] in "RC":
            original, rest = rest.split(" -> ", 1)
        entries.append(GitStatusEntry(path=rest, status=code, original_path=original))
    return GitStatusResult(branch=branch, entries=entries, clean=not entries, truncated=truncated)


def _raise_if_git_refused(result: dict[str, Any]) -> None:
    """Turn the preflight's refusal into a non-retryable error naming the keys."""
    stderr = str(result.get("stderr") or "")
    if result.get("exit_code") != GIT_REFUSED_EXIT or GIT_REFUSED_MARKER not in stderr:
        return
    items = [
        line.strip()
        for line in stderr.split(GIT_REFUSED_MARKER, 1)[1].splitlines()
        if line.strip()
    ]
    shown = ", ".join(items[:20]) + (f" (+{len(items) - 20} more)" if len(items) > 20 else "")
    raise ApplicationError(
        "The repo config contains settings the agent's git tools won't run with: "
        f"{shown}. Inspect them with run_command (for example `git config --local --list`).",
        {"keys": items[:100]},
        type="GitConfigRefused",
        non_retryable=True,
    )


def _git_error(result: dict[str, Any]) -> str | None:
    _raise_if_git_refused(result)
    if result.get("exit_code") == 0:
        return None
    stderr = str(result.get("stderr") or "").strip()
    return (stderr or str(result.get("error") or "git failed"))[:500]


@agent.activity_tool_defn(inherently_safe=True, activity_config=EXEC_READ_CONFIG)
async def git_status(workspace_id: agent.Injected[str]) -> GitStatusResult:
    """Show the workspace's git branch and changed, staged and untracked files."""
    result = await _exec_collect(workspace_id, "git_status", git_status_command())
    error = _git_error(result)
    if error:
        return GitStatusResult(error=error, clean=False)
    return parse_git_status(str(result.get("stdout") or ""))


def git_status_command() -> str:
    return f"{GIT_PRELUDE}\n{GIT} status --porcelain=v1 -b --ignore-submodules=all"


def git_diff_command(path: str | None, staged: bool) -> str:
    base = (
        f"{GIT} diff --no-ext-diff --no-textconv --no-color --ignore-submodules=all"
        + (" --cached" if staged else "")
    )
    scope = f" -- {shlex.quote(path)}" if path else ""
    return (
        f"{GIT_PRELUDE}\n"
        f"{base} --numstat{scope} && printf '\\n%s\\n' {_SEPARATOR} && {base}{scope}"
    )


def parse_git_diff(stdout: str, staged: bool, limit: int = MAX_GIT_DIFF_CHARS) -> GitDiffResult:
    numstat, _, diff = stdout.partition(f"\n{_SEPARATOR}\n")
    files: list[GitDiffFile] = []
    for line in numstat.splitlines():
        parts = line.split("\t", 2)
        if len(parts) != 3:
            continue
        added, removed, name = parts
        files.append(
            GitDiffFile(
                path=name,
                added=int(added) if added.isdigit() else None,
                removed=int(removed) if removed.isdigit() else None,
            )
        )
    bounded, cut = _bound_head(diff, limit, "diff truncated; pass `path` to see one file")
    return GitDiffResult(staged=staged, files=files, diff=bounded, truncated=cut)


@agent.activity_tool_defn(inherently_safe=True, activity_config=EXEC_READ_CONFIG)
async def git_diff(
    workspace_id: agent.Injected[str],
    path: str | None = None,
    staged: bool = False,
) -> GitDiffResult:
    """Show the git diff of the workspace: unstaged changes, or staged ones with
    `staged=true`. `path` limits it to one file or directory. Untracked files are not
    included (see git_status)."""
    scope = _check_relative_path(path) if path else None
    result = await _exec_collect(workspace_id, "git_diff", git_diff_command(scope, staged))
    error = _git_error(result)
    if error:
        return GitDiffResult(staged=staged, error=error)
    parsed = parse_git_diff(str(result.get("stdout") or ""), staged)
    if result.get("stdout_truncated"):
        parsed.truncated = True
    return parsed


def git_commit_command(add_all: bool) -> str:
    # The message arrives on stdin (`-F -`), so it is never shell-quoted.
    # The identity comes from the executor's GIT_AUTHOR_*/GIT_COMMITTER_* environment.
    return "\n".join(
        [
            GIT_PRELUDE,
            "set -e",
            *([f"{GIT} add -A"] if add_all else []),
            f'if {GIT} diff --cached --quiet --ignore-submodules=all; then echo "nothing to commit"; exit 3; fi',
            f"{GIT} commit --no-verify -q -F -",
            f"{GIT} log -1 --format='%H%n%s'",
            f"{GIT} show --no-ext-diff --no-textconv --shortstat --format= HEAD",
        ]
    )


def parse_git_commit(result: dict[str, Any]) -> GitCommitResult:
    stdout = str(result.get("stdout") or "")
    if result.get("exit_code") == 3 and "nothing to commit" in stdout:
        return GitCommitResult(committed=False, output="There were no changes to commit.")
    if result.get("exit_code") != 0:
        raise ApplicationError(
            f"git commit failed: {_git_error(result)}", type="GitCommitFailed", non_retryable=True
        )
    lines = [line for line in stdout.splitlines() if line.strip()]
    sha = lines[0].strip() if lines else None
    summary = lines[1].strip() if len(lines) > 1 else ""
    stat = lines[-1] if len(lines) > 2 else ""
    numbers = {"file": 0, "insertion": 0, "deletion": 0}
    for chunk in stat.split(","):
        words = chunk.strip().split(" ")
        if len(words) >= 2 and words[0].isdigit():
            for key in numbers:
                if words[1].startswith(key):
                    numbers[key] = int(words[0])
    return GitCommitResult(
        committed=True,
        commit=sha,
        summary=summary,
        files_changed=numbers["file"],
        insertions=numbers["insertion"],
        deletions=numbers["deletion"],
        output=_bound_head(stdout, 2_000, "output truncated")[0],
    )


# "shell", not "workspace_edit": a commit always goes to a human in the catalog, and
# "always allow" never applies to it.
@agent.activity_tool_defn(activity_config=EXEC_WRITE_CONFIG, auto_approval_criteria="shell")
async def git_commit(
    message: str,
    workspace_id: agent.Injected[str],
    add_all: bool = True,
) -> GitCommitResult:
    """Commit the workspace's changes to its local git repository. With `add_all=true`
    (default) every change, including untracked files, is staged first; otherwise only
    what is already staged is committed. Local only: nothing is pushed."""
    if not message or not message.strip():
        raise ApplicationError("A commit message is required.", type="InvalidArgument", non_retryable=True)
    if len(message) > MAX_COMMIT_MESSAGE_CHARS:
        raise ApplicationError(
            f"The commit message is longer than {MAX_COMMIT_MESSAGE_CHARS} chars.",
            type="InvalidArgument",
            non_retryable=True,
        )
    result = await _exec_collect(
        workspace_id, "git_commit", git_commit_command(add_all), stdin=message.strip() + "\n"
    )
    _raise_if_git_refused(result)
    return parse_git_commit(result)


WORKSPACE_TOOLS = (
    list_files,
    read_file,
    search_code,
    write_file,
    edit_file,
    run_command,
    git_status,
    git_diff,
    git_commit,
)
# Activities the worker must register itself: AgentHarnessPlugin(tools=...) registers the
# activity tools' bodies but skips inline tools such as run_command.
WORKSPACE_ACTIVITIES = [run_command_activity]


# -- Test-only tool ---------------------------------------------------------------------
# Offered to the model only when a session sets CodingAgentInit.test_tools (integration
# tests of interrupt and crash handling need a tool that runs for a while).

WAIT_CONFIG = ActivityConfig(
    start_to_close_timeout=timedelta(minutes=5),
    heartbeat_timeout=timedelta(seconds=5),
    retry_policy=RetryPolicy(maximum_attempts=2),
)


@agent.activity_tool_defn(inherently_safe=True, activity_config=WAIT_CONFIG)
async def wait_seconds(seconds: int) -> str:
    """Wait for the given number of seconds (testing only)."""
    for _ in range(max(0, min(seconds, 240))):
        activity.heartbeat()
        await asyncio.sleep(1)
    return f"Waited {seconds} seconds."


TEST_TOOLS = (wait_seconds,)
