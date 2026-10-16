"""Observable state for the coding agent's product view (PRD A13).

Declared on the workflow class with ``agent.state(...)``; the harness publishes a snapshot and
then one JSON patch per change on the agent's event stream, so the UI's Plan and Changes
panels update live with no Queries.

* ``plan``: the agent's own checklist, written by the inline ``update_plan`` tool.
* ``changes``: files the agent touched and commits it made, folded from tool results.
* ``workspace``: the last command and its exit code.

No ``from __future__ import annotations``: tool annotations are read concretely by the harness.
"""

from typing import Any, Literal

from pydantic import BaseModel, Field

from temporal_agent_harness.harness import agent
from temporal_agent_harness.harness.state import HarnessState, StateRef

PlanStatus = Literal["pending", "in_progress", "done", "skipped"]
FileStatus = Literal["added", "modified", "deleted", "renamed"]

MAX_PLAN_ITEMS = 30
MAX_PLAN_TEXT = 300
MAX_CHANGED_FILES = 200
MAX_COMMITS = 50


class PlanItem(HarnessState):
    """One step of the agent's plan."""

    text: str
    status: PlanStatus = "pending"


class PlanState(HarnessState):
    """The agent's checklist for the current task."""

    goal: str = ""
    items: list[PlanItem] = Field(default_factory=list)


class ChangedFile(HarnessState):
    """A file the agent changed in its workspace this session."""

    path: str
    status: FileStatus = "modified"
    added: int = 0
    removed: int = 0
    tool_id: str = ""


class Commit(HarnessState):
    """A local commit the agent made."""

    hash: str
    message: str


class ChangesState(HarnessState):
    """Files changed and commits made, newest last."""

    files: list[ChangedFile] = Field(default_factory=list)
    commits: list[Commit] = Field(default_factory=list)


class WorkspaceState(HarnessState):
    """What the workspace's terminal did last."""

    last_command: str = ""
    last_exit_code: int | None = None
    commands_run: int = 0


# -- update_plan ------------------------------------------------------------------------


class PlanUpdate(BaseModel):
    """The full plan, replacing the previous one."""

    goal: str = Field("", description="One line saying what you are trying to achieve.")
    items: list[PlanItem] = Field(
        description="Every step, in order, with its current status. Send the whole list each time."
    )


class PlanReply(BaseModel):
    """Acknowledgement of a plan update."""

    summary: str

    def __str__(self) -> str:
        return self.summary


@agent.tool_defn(inherently_safe=True)
async def update_plan(update: PlanUpdate, plan: agent.Injected[StateRef[PlanState]]) -> PlanReply:
    """Write your plan for the current task as a checklist the user can watch.

    Call it at the start of a multi-step task, and again whenever a step starts, finishes or
    changes. Send the whole list each time; mark at most one step `in_progress`.
    """
    # Bounded: every plan change becomes a state patch in workflow history.
    items = [
        PlanItem(text=i.text.strip()[:MAX_PLAN_TEXT], status=i.status) for i in update.items[:MAX_PLAN_ITEMS]
    ]
    plan.set(PlanState(goal=update.goal.strip()[:MAX_PLAN_TEXT], items=items))
    done = sum(1 for i in items if i.status == "done")
    return PlanReply(summary=f"Plan updated: {done}/{len(items)} steps done.")


STATE_TOOLS = (update_plan,)


# -- folding tool results into state ----------------------------------------------------


def fold_tool_result(
    changes: StateRef[ChangesState],
    workspace: StateRef[WorkspaceState],
    *,
    tool_name: str,
    tool_id: str,
    args: dict[str, Any],
    result: Any,
) -> None:
    """Update the Changes and Workspace panels from one tool result.

    Duck-typed over the tool result models (see tools/workspace.py): anything with
    ``changed_files`` (run_command), ``path`` with ``added_lines``/``removed_lines``
    (write/edit), ``commit_hash`` (git_commit), or ``exit_code``.
    """
    touched: list[tuple[str, str, int, int]] = []
    if getattr(result, "path", None) and hasattr(result, "added_lines"):
        status = "added" if getattr(result, "created", False) else "modified"
        touched.append((result.path, status, result.added_lines or 0, result.removed_lines or 0))
    for item in getattr(result, "changed_files", None) or []:
        path = item.get("path") if isinstance(item, dict) else getattr(item, "path", None)
        status = item.get("status", "modified") if isinstance(item, dict) else getattr(item, "status", "modified")
        if path:
            touched.append((path, _normalize_status(status), 0, 0))

    if touched:
        with changes.mutate() as draft:
            for path, status, added, removed in touched:
                existing = next((f for f in draft.files if f.path == path), None)
                if existing is None:
                    draft.files.append(
                        ChangedFile(path=path, status=status, added=added, removed=removed, tool_id=tool_id)
                    )
                else:
                    existing.added += added
                    existing.removed += removed
                    existing.tool_id = tool_id
                    if status == "deleted" or existing.status != "added":
                        existing.status = status
            if len(draft.files) > MAX_CHANGED_FILES:
                del draft.files[: len(draft.files) - MAX_CHANGED_FILES]

    # GitCommitResult reports `committed` and the new hash as `commit`.
    commit_hash = getattr(result, "commit_hash", None) or (
        getattr(result, "commit", None) if getattr(result, "committed", False) else None
    )
    if commit_hash:
        with changes.mutate() as draft:
            message = getattr(result, "message", "") or args.get("message", "") or getattr(result, "summary", "")
            draft.commits.append(Commit(hash=str(commit_hash), message=str(message)[:300]))
            if len(draft.commits) > MAX_COMMITS:
                del draft.commits[: len(draft.commits) - MAX_COMMITS]

    if tool_name == "run_command" and hasattr(result, "exit_code"):
        with workspace.mutate() as draft:
            draft.last_command = str(args.get("command", ""))[:500]
            draft.last_exit_code = result.exit_code
            draft.commands_run += 1


def _normalize_status(status: str) -> str:
    s = (status or "").lower()
    if s.startswith("a") or s in ("new", "created", "untracked", "?"):
        return "added"
    if s.startswith("d"):
        return "deleted"
    if s.startswith("r"):
        return "renamed"
    return "modified"
