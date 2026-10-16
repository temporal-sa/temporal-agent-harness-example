"""CodingSessions: one per user, the session manager (P2 decision 1).

Holds the user's session list, titles and settings. It never starts or parents sessions:
the API starts each CodingAgent directly, so new sessions always run on the current worker
version, and records it here with a Signal (signal-with-start, so it lands while workers are
down). The API mirrors the list into Redis, so browsers never Query this workflow. It
continues as new on Temporal's suggestion, on a deployment version change (checked at least
hourly by the idle timer), or on the operator's `compact` Update.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from pydantic import BaseModel, Field

SESSIONS_WORKFLOW_TYPE = "CodingSessions"
IDLE_WAKE = timedelta(hours=1)
MAX_SESSIONS = 500


def sessions_workflow_id(owner_digest: str) -> str:
    return f"coding-sessions-{owner_digest}"


class SessionRecord(BaseModel):
    """One of the user's sessions."""

    session_id: str
    title: str = "New session"
    model: str = ""
    posture: str = ""
    created_at: str = ""
    archived: bool = False


class SessionsState(BaseModel):
    """Everything the manager carries across runs."""

    owner_digest: str
    sessions: list[SessionRecord] = Field(default_factory=list)
    settings: dict[str, str] = Field(default_factory=dict)


class RenameRequest(BaseModel):
    session_id: str
    title: str


class SessionUpdate(BaseModel):
    """The session's current model or posture, after the API accepted a change. ``None``
    leaves a field as it is."""

    session_id: str
    model: str | None = None
    posture: str | None = None


@workflow.defn(name=SESSIONS_WORKFLOW_TYPE)
class CodingSessions:
    @workflow.init
    def __init__(self, state: SessionsState) -> None:
        self._state = state
        self._compact_requested = False

    @workflow.run
    async def run(self, state: SessionsState) -> None:
        while True:
            try:
                await workflow.wait_condition(self._should_continue_as_new, timeout=IDLE_WAKE)
            except TimeoutError:
                pass
            if self._should_continue_as_new() or workflow.info().is_target_worker_deployment_version_changed():
                await workflow.wait_condition(workflow.all_handlers_finished)
                workflow.continue_as_new(
                    args=[self._state],
                    initial_versioning_behavior=workflow.ContinueAsNewVersioningBehavior.AUTO_UPGRADE,
                )

    def _should_continue_as_new(self) -> bool:
        info = workflow.info()
        return (
            self._compact_requested
            or info.is_continue_as_new_suggested()
            or info.is_target_worker_deployment_version_changed()
        )

    @workflow.update
    async def register_session(self, record: SessionRecord) -> list[SessionRecord]:
        """Add (or replace) a session; newest first, bounded."""
        rest = [s for s in self._state.sessions if s.session_id != record.session_id]
        self._state.sessions = [record, *rest][:MAX_SESSIONS]
        return self._state.sessions

    @workflow.update
    async def rename_session(self, request: RenameRequest) -> list[SessionRecord]:
        for s in self._state.sessions:
            if s.session_id == request.session_id:
                s.title = request.title.strip()[:200] or s.title
        return self._state.sessions

    @workflow.update
    async def update_session(self, request: SessionUpdate) -> list[SessionRecord]:
        for s in self._state.sessions:
            if s.session_id == request.session_id:
                if request.model:
                    s.model = request.model[:200]
                if request.posture:
                    s.posture = request.posture[:50]
        return self._state.sessions

    @workflow.update
    async def remove_session(self, session_id: str) -> list[SessionRecord]:
        self._state.sessions = [s for s in self._state.sessions if s.session_id != session_id]
        return self._state.sessions

    @workflow.update
    async def set_setting(self, item: dict[str, str]) -> dict[str, str]:
        self._state.settings.update({k: v[:2000] for k, v in item.items()})
        return self._state.settings

    # Signal versions of the mutations: the API sends these (with signal-with-start), so a
    # change lands in history even while no worker is running. The Updates stay for operators
    # and tests that want the resulting list back.

    @workflow.signal(name="register_session_signal")
    async def register_session_signal(self, record: SessionRecord) -> None:
        await self.register_session(record)

    @workflow.signal(name="rename_session_signal")
    async def rename_session_signal(self, request: RenameRequest) -> None:
        await self.rename_session(request)

    @workflow.signal(name="update_session_signal")
    async def update_session_signal(self, request: SessionUpdate) -> None:
        await self.update_session(request)

    @workflow.signal(name="remove_session_signal")
    async def remove_session_signal(self, session_id: str) -> None:
        await self.remove_session(session_id)

    @workflow.update
    async def compact(self) -> None:
        """Operator action: start a fresh run carrying the same state."""
        self._compact_requested = True

    @workflow.query
    def list_sessions(self) -> list[SessionRecord]:
        """For operators and tests. The API never calls it: a lost Redis list is rebuilt from
        Visibility and the sessions' memos, so no browser path depends on a worker."""
        return self._state.sessions
