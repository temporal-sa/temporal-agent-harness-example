"""The harness console at ``/console/``, backed by our owner-scoped routes (P2 step 2.4, CTO-6).

The console is the harness's packaged UI (``temporal_agent_harness/ui/dist``), served
unchanged. It calls ``api/<path>`` relative to its page, so from ``/console/`` every call
lands on ``/console/api/<path>``. Those routes are ours; the packaged app
(``create_agent_harness_app``) is never mounted: it lists every session in the namespace,
sends open CORS, starts its own session manager and takes the file viewer's config from the
request.

Route map (console path -> handler):

* ``attach``, ``messages``, ``approve``, ``callback-result``, ``status/{id}``,
  ``workflow-status/{id}``, ``sessions/{id}/close``, ``agent-interface/{id}``: the same
  handlers as ``/api/agent/...`` (a second instance of :func:`create_agent_router`, same
  store, directory and auth), so ownership, Redis-backed reads and error bodies are identical.
* ``GET sessions``: the caller's own list from the session directory, never a Visibility list.
* ``GET agents``: CodingAgent only, marked as needing init data so the console never starts
  a session by itself on boot.
* ``POST sessions``: refused (403). Sessions start from the app, which picks the model,
  posture and demo seed the console can't express.
* ``POST files/view``, ``POST files/okf-graph``: 404. There is no Code Mode VFS until P4, and
  the request's ``source`` config is never trusted.

Everything else under ``/console/api/`` (``chat`` included) is a 404. No CORS headers are
added; the console is same-origin.
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.routing import APIRoute
from fastapi.staticfiles import StaticFiles

from simple_chat_agent.agent.contracts import AGENT_WORKFLOW_TYPE
from simple_chat_agent.api.agent_routes import (
    NO_STORE,
    AgentApi,
    HarnessError,
    HarnessErrorRoute,
    SessionDirectory,
    _summary,
    agent_environment,
    not_found,
)
from simple_chat_agent.api.agent_store import AgentStore

log = logging.getLogger(__name__)

CONSOLE_PATH = "/console"
CONSOLE_API_PREFIX = f"{CONSOLE_PATH}/api"

# The agent routes the console calls and gets unchanged from the agent router.
DELEGATED_ROUTES: frozenset[tuple[str, str]] = frozenset(
    {
        ("GET", "/attach"),
        ("POST", "/messages"),
        ("POST", "/approve"),
        ("POST", "/callback-result"),
        ("GET", "/status/{session_id}"),
        ("GET", "/workflow-status/{session_id}"),
        ("POST", "/sessions/{session_id}/close"),
        ("GET", "/agent-interface/{session_id}"),
    }
)

CODING_AGENT_KEY = "coding"
CODING_AGENT_LABEL = "Coding agent"
CODING_AGENT_DESCRIPTION = "Works in your session's workspace: reads, edits and runs code, with your approval."
# The console starts a session for the first agent with no required init data when it has
# nothing to reopen (ui agentRun.initialize / initData.defaultBootAgent). Marking the init data
# required stops that boot-time create; the console shows "start one from New session", and
# that create is refused with a message pointing at the app.
_INIT_DATA = {
    "required": True,
    "schema": {
        "type": "object",
        "title": "Start from the app",
        "description": "Coding sessions are started from the main app. Open one there, then use its console link.",
        "properties": {},
    },
}


def console_enabled() -> bool:
    """``CONSOLE_ENABLED`` when set; otherwise on for ``AGENT_ENVIRONMENT=local`` and off in
    the catalog (the default environment)."""
    raw = (os.environ.get("CONSOLE_ENABLED") or "").strip().lower()
    if raw:
        return raw in {"1", "true", "yes", "on"}
    return agent_environment() == "local"


def console_dist() -> Path | None:
    from temporal_agent_harness.ui import packaged_ui_dist

    return packaged_ui_dist()


def create_console_router(
    *,
    agent_router: APIRouter,
    store: AgentStore,
    directory: SessionDirectory,
    temporal: Callable[[], Awaitable[Any]],
    current_user: Callable[[Request], Any],
    task_queue: str,
) -> APIRouter:
    """The console's API. Mount with ``prefix="/console/api"``.

    ``agent_router`` is a router from :func:`create_agent_router` built on the same
    ``store`` and ``directory``; only :data:`DELEGATED_ROUTES` are taken from it."""
    router = APIRouter(route_class=HarnessErrorRoute)

    def user_id(request: Request) -> str:
        return current_user(request).user_id

    @router.get("/agents")
    async def agents(uid: str = Depends(user_id)):
        return JSONResponse(
            {
                "agents": [
                    {
                        "key": CODING_AGENT_KEY,
                        "workflow_type": AGENT_WORKFLOW_TYPE,
                        "task_queue": task_queue,
                        "label": CODING_AGENT_LABEL,
                        "description": CODING_AGENT_DESCRIPTION,
                        "agent": None,
                        "init_data": _INIT_DATA,
                        "worker": await _worker_readiness(temporal, task_queue),
                    }
                ]
            },
            headers=NO_STORE,
        )

    @router.get("/sessions")
    async def list_sessions(uid: str = Depends(user_id)):
        # Oldest first, as the harness session manager lists them: the console's fallback
        # pick on boot is the *last* live session in the list.
        records = sorted(await directory.list(uid), key=lambda r: float(r.get("created_at") or 0))
        metas = await asyncio.gather(*(store.meta(r["session_id"]) for r in records))
        sessions = []
        for record, meta in zip(records, metas):
            summary = _summary(record, closed=meta.closed)
            # The console skips a ?s= / remembered session with no first-message preview when
            # a newer one exists (ui bootSession.chooseBootSession). We don't keep the first
            # message, so the title stands in; the console shows it in the same place.
            summary["initial_user_message"] = summary["label"]
            sessions.append(summary)
        return JSONResponse(sessions, headers=NO_STORE)

    @router.post("/sessions")
    async def create_session(uid: str = Depends(user_id)):
        raise HarnessError(
            403, "forbidden", "Start coding sessions from the app; the console opens sessions you already have."
        )

    @router.post("/files/view")
    async def view_file(uid: str = Depends(user_id)):
        # The body names a mount source (store config and task queue). It is never used:
        # there is no Code Mode VFS until P4, so no file belongs to any session yet.
        raise not_found("No such file.")

    @router.post("/files/okf-graph")
    async def okf_graph(uid: str = Depends(user_id)):
        raise not_found("No such file.")

    for route in agent_router.routes:
        if isinstance(route, APIRoute) and any((m, route.path) in DELEGATED_ROUTES for m in route.methods):
            router.routes.append(route)

    @router.api_route(
        "/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"], include_in_schema=False
    )
    async def unknown(path: str):
        # Everything the console API doesn't serve (the packaged app's ``chat`` included),
        # rather than falling through to the static mount.
        raise HarnessError(404, "not_found", "Not found.")

    return router


async def _worker_readiness(temporal: Callable[[], Awaitable[Any]], task_queue: str) -> dict[str, Any]:
    """The agent picker's readiness chip, from DescribeTaskQueue (no Query, no worker)."""
    from dataclasses import asdict

    from temporal_agent_harness.web.task_queue_status import describe_task_queue_workers

    try:
        workers = await describe_task_queue_workers(await temporal(), [task_queue])
        return asdict(workers.for_task_queue(task_queue))
    except Exception as exc:  # noqa: BLE001 - never fail the agent list over readiness
        return {"status": "unknown", "task_queue": task_queue, "poller_count": 0, "last_seen": None, "error": type(exc).__name__}


def mount_console_routes(app: FastAPI, console_api: APIRouter, dist: Path) -> None:
    """The console API at ``/console/api`` and the static console at ``/console/``. The
    static mount goes last so it can't shadow the API."""
    app.include_router(console_api, prefix=CONSOLE_API_PREFIX)
    index_html = (dist / "index.html").read_text()

    @app.get(CONSOLE_PATH, include_in_schema=False)
    async def console_redirect(request: Request):
        query = f"?{request.url.query}" if request.url.query else ""
        # Relative, so it also works behind a path prefix.
        return RedirectResponse(f"console/{query}")

    @app.get(f"{CONSOLE_PATH}/", include_in_schema=False)
    async def console_page():
        return HTMLResponse(index_html, headers={"Cache-Control": "no-cache"})

    app.mount(CONSOLE_PATH, StaticFiles(directory=dist), name="console")


def mount_console(app: FastAPI, agent_api: AgentApi, *, app_store: Callable[[], Any] | None = None) -> bool:
    """Mount the console on the real app when :func:`console_enabled`. Returns whether it
    was mounted."""
    if not console_enabled():
        return False
    dist = console_dist()
    if dist is None:
        log.warning("CONSOLE_ENABLED but the harness console assets aren't installed; /console is off")
        return False
    console_api = create_console_router(
        agent_router=agent_api.router(app_store=app_store),
        store=agent_api.store,
        directory=agent_api.directory,
        temporal=agent_api.temporal,
        current_user=agent_api.current_user,
        task_queue=agent_api.task_queue,
    )
    mount_console_routes(app, console_api, dist)
    return True
