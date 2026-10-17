"""The API and the product view (catalog entrypoint: python -m simple_chat_agent.api.main).

Serves the coding agent's routes at /api/agent, the built web app at / (P5; it was /app/), the
harness console at /console/, sign-in and identity (/api/me, /api/auth/*), artifact downloads
(/api/artifacts/*) and /healthz, all behind the catalog's identity headers or the local cookie.
"""

from __future__ import annotations

import json
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse, Response
from starlette.middleware.gzip import GZipMiddleware
from temporalio.client import Client

from simple_chat_agent import TASK_QUEUE
from simple_chat_agent.api.agent_routes import AgentApi
from simple_chat_agent.api.artifacts import create_artifacts_router
from simple_chat_agent.api.auth import (
    DEFAULT_SESSION_SECONDS,
    SESSION_COOKIE,
    AuthenticatedUser,
    AuthError,
    catalog_auth_enabled,
    create_session_token,
    session_secret_problem,
    user_from_catalog_headers,
    user_from_local_username,
    user_from_session_token,
)
from simple_chat_agent.api.console import mount_console
from simple_chat_agent.api.local_auth import local_auth_credentials_valid, local_auth_requested
from simple_chat_agent.api.temporal_ui import temporal_ui_user_workflows_url
from simple_chat_agent.common.env import load_dotenv
from simple_chat_agent.common.external_storage import simple_chat_data_converter
from simple_chat_agent.common.store import AppStore

load_dotenv()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    load_dotenv()
    if local_auth_requested() and not catalog_auth_enabled() and (problem := session_secret_problem()):
        # PRD §5.6: local sign-in refuses to start without a real signing secret.
        raise RuntimeError(f"Local sign-in is on but {problem} Set a random value of 32+ characters.")
    client_config: dict[str, Any] = {
        "namespace": os.environ.get("TEMPORAL_NAMESPACE", "default"),
        "data_converter": simple_chat_data_converter(),
        "tls": os.environ.get("TEMPORAL_TLS", "false").lower() in ["true", "1"],
    }
    if os.environ.get("TEMPORAL_API_KEY"):
        client_config["api_key"] = os.environ.get("TEMPORAL_API_KEY")

    # /healthz reports ready only once this connects; /api/me uses its namespace.
    app.state.temporal_client = await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", os.environ.get("TEMPORAL_ENDPOINT", "localhost:7233")), **client_config
    )
    app.state.store = AppStore()
    try:
        yield
    finally:
        await _agent_api.aclose()


app = FastAPI(lifespan=lifespan)
# Starlette excludes text/event-stream from gzip, so live SSE latency is not
# traded for buffering while large JSON state snapshots still compress.
app.add_middleware(GZipMiddleware, minimum_size=1024, compresslevel=6)


@app.get("/api/me")
async def me(request: Request) -> dict[str, Any]:
    user = _current_user(request)
    return {
        "user_id": user.user_id,
        "username": user.username,
        # Link to all of this user's workflows in the Temporal UI, filtered by
        # the UserEmail search attribute. None when the attribute is disabled.
        "temporal_ui_workflows_url": temporal_ui_user_workflows_url(
            user.username,
            namespace=_client().namespace,
            search_attr_name=_user_email_sa_name(),
        ),
    }


def _local_auth_route_enabled() -> bool:
    return local_auth_requested() and not catalog_auth_enabled()


@app.get("/api/auth/config")
async def auth_config() -> dict[str, Any]:
    return {
        "mode": "catalog" if catalog_auth_enabled() else "local",
        "local_enabled": _local_auth_route_enabled(),
    }


@app.get("/healthz")
async def health() -> dict[str, str]:
    _client()
    return {"status": "ok"}


if _local_auth_route_enabled():

    @app.post("/api/auth/local/login")
    async def local_auth_login(request: Request) -> Response:
        if not _local_auth_route_enabled():
            raise HTTPException(status_code=404, detail="Not found")
        try:
            body = await request.json()
        except json.JSONDecodeError:
            body = {}
        if not isinstance(body, dict):
            body = {}
        username = body.get("username")
        password = body.get("password")
        if not isinstance(username, str) or not isinstance(password, str):
            raise HTTPException(status_code=401, detail="Invalid local credentials")
        if not local_auth_credentials_valid(username, password):
            raise HTTPException(status_code=401, detail="Invalid local credentials")

        user = user_from_local_username(username)
        response = Response(
            content=json.dumps(
                {"status": "ok", "user_id": user.user_id, "username": user.username}
            ),
            media_type="application/json",
        )
        response.set_cookie(
            SESSION_COOKIE,
            create_session_token(user),
            max_age=DEFAULT_SESSION_SECONDS,
            httponly=True,
            samesite="lax",
        )
        return response


@app.post("/api/logout")
async def logout() -> Response:
    response = Response(
        content=json.dumps({"status": "ok"}),
        media_type="application/json",
    )
    response.delete_cookie(SESSION_COOKIE)
    return response


def _user_email_sa_name() -> str:
    return os.environ.get("SIMPLE_CHAT_USER_EMAIL_SEARCH_ATTR", "").strip()


def _client() -> Client:
    client = getattr(app.state, "temporal_client", None)
    if client is None:
        raise HTTPException(status_code=503, detail="Temporal client is not ready")
    return client


def _store() -> AppStore:
    store = getattr(app.state, "store", None)
    if store is None:
        raise HTTPException(status_code=503, detail="App store is not ready")
    return store


def _current_user(request: Request) -> AuthenticatedUser:
    if catalog_auth_enabled():
        try:
            return user_from_catalog_headers(request.headers, task_queue=TASK_QUEUE)
        except AuthError as err:
            raise HTTPException(status_code=401, detail=str(err)) from err
    token = request.cookies.get(SESSION_COOKIE)
    if token is None:
        raise HTTPException(status_code=401, detail="Not authenticated")
    try:
        return user_from_session_token(token)
    except AuthError as err:
        raise HTTPException(status_code=401, detail=str(err)) from err


# Artifacts the agent created (create_artifact), viewed and downloaded by their owner.
app.include_router(create_artifacts_router(store=_store, current_user=_current_user))

# The coding agent's routes (P2): the harness client's HTTP contract plus sessions, config,
# uploads and tool output, behind the same catalog / local auth as everything else.
_agent_api = AgentApi(current_user=lambda request: _current_user(request))
app.state.agent_api = _agent_api
app.include_router(_agent_api.router(app_store=_store), prefix="/api/agent")

# The product view: the Vite build in simple_chat_agent/web/dist at /, behind the same catalog /
# local auth as /api/agent. Unknown paths fall back to index.html (client routing); a missing
# asset under assets/ is a 404, and so is anything under the API's own prefixes. Without a build,
# / explains how to make one. The P3/P4 address /app/... redirects to the same path at /, so old
# links and bookmarks still land. The catch-all route is registered last (after the console).
_LEGACY_APP_PATH = "/app"
# Paths the SPA fallback must never answer with index.html.
_NOT_APP_PREFIXES = ("api/", "internal/", "console/", "static/")
_NOT_APP_PATHS = {"api", "internal", "console", "healthz"}
_APP_HEADERS = {
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Content-Security-Policy": "frame-ancestors 'none'",
    "Referrer-Policy": "same-origin",
}
# The app itself (not the 401 sign-in page, which has an inline script). Checked against the
# built app in headless Chromium: no violations.
_APP_CSP = (
    "default-src 'self'; script-src 'self'; object-src 'none'; base-uri 'none'; frame-src 'self'; "
    "img-src 'self' data:; style-src 'self' 'unsafe-inline'; frame-ancestors 'none'"
)
_APP_FILE_HEADERS = {**_APP_HEADERS, "Content-Security-Policy": _APP_CSP}


def _web_dist() -> Path:
    override = os.environ.get("SIMPLE_CHAT_WEB_DIST", "").strip()
    return Path(override) if override else Path(__file__).resolve().parent.parent / "web" / "dist"


def _app_page(status: int, title: str, body_html: str) -> HTMLResponse:
    page = (
        '<!doctype html><html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width, initial-scale=1">'
        f"<title>{title}</title><style>body{{font:16px/1.5 system-ui,sans-serif;max-width:36rem;"
        "margin:4rem auto;padding:0 1rem;color:#141414;background:#fff}code{background:#f2f2f2;"
        "padding:0 .25rem}input,button{font:inherit;padding:.5rem;margin:.25rem 0;width:100%;"
        "box-sizing:border-box}@media (prefers-color-scheme:dark){body{color:#f2f2f2;"
        "background:#141414}code{background:#2a2a2a}}</style></head>"
        f"<body><h1>{title}</h1>{body_html}</body></html>"
    )
    return HTMLResponse(page, status_code=status, headers={**_APP_HEADERS, "Cache-Control": "no-store"})


def _app_sign_in(request: Request) -> HTMLResponse:
    if not _local_auth_route_enabled():
        hint = (
            "<p>Your sign-in didn't reach the app. Reload the page; if it keeps happening, "
            "sign in again through the catalog.</p>"
            if catalog_auth_enabled()
            else "<p>Local sign-in is off. Set <code>SIMPLE_CHAT_LOCAL_AUTH_ENABLED=1</code> "
            "(with local credentials) and restart the API.</p>"
        )
        return _app_page(401, "Sign in to continue", hint)
    login = json.dumps(f"{request.scope.get('root_path', '')}/api/auth/local/login")
    form = (
        '<form id="f"><label>Username<input name="username" autocomplete="username" required></label>'
        '<label>Password<input name="password" type="password" autocomplete="current-password" required>'
        '</label><button type="submit">Sign in</button><p id="e" role="alert"></p></form>'
        "<script>document.getElementById('f').addEventListener('submit',async(ev)=>{ev.preventDefault();"
        "const d=new FormData(ev.target);const r=await fetch(" + login + ",{method:'POST',"
        "headers:{'Content-Type':'application/json','X-Requested-With':'fetch'},credentials:'same-origin',"
        "body:JSON.stringify({username:d.get('username'),password:d.get('password')})});"
        "if(r.ok){location.reload()}else{document.getElementById('e').textContent="
        "'That username and password didn\\u2019t match.'}});</script>"
    )
    return _app_page(401, "Sign in to continue", form)


# The harness console (P2 step 2.4): its unchanged static UI at /console/, talking to our own
# owner-scoped routes at /console/api/. CONSOLE_ENABLED; on by default only for
# AGENT_ENVIRONMENT=local. Mounted last so its static mount can't shadow other routes.
app.state.console_enabled = mount_console(app, _agent_api, app_store=_store)


@app.api_route(_LEGACY_APP_PATH, methods=["GET", "HEAD"], include_in_schema=False)
@app.api_route(_LEGACY_APP_PATH + "/{path:path}", methods=["GET", "HEAD"], include_in_schema=False)
async def legacy_app_redirect(request: Request, path: str = "") -> Response:
    # Strip leading slashes and backslashes so /app//evil.example can't become a
    # protocol-relative redirect off this origin.
    rest = path.lstrip("/\\")
    query = f"?{request.url.query}" if request.url.query else ""
    return RedirectResponse(f"{request.scope.get('root_path', '')}/{rest}{query}")


@app.api_route("/", methods=["GET", "HEAD"], include_in_schema=False)
@app.api_route("/{path:path}", methods=["GET", "HEAD"], include_in_schema=False)
async def app_page(request: Request, path: str = "") -> Response:
    not_found = Response("Not found", status_code=404, media_type="text/plain", headers=_APP_HEADERS)
    if path in _NOT_APP_PATHS or path.startswith(_NOT_APP_PREFIXES):
        return not_found
    try:
        _current_user(request)
    except HTTPException as err:
        if err.status_code != 401:
            raise
        return _app_sign_in(request)
    dist = _web_dist().resolve()
    index_html = dist / "index.html"
    if not index_html.is_file():
        return _app_page(
            503,
            "The product view isn't built yet",
            "<p>Run <code>make web</code> in the repo root, then reload. "
            "For live reload while you work, run <code>make web-dev</code> instead.</p>",
        )
    if "\x00" in path:
        return not_found
    try:
        target = (dist / path).resolve() if path else index_html
        is_file = target.is_relative_to(dist) and target.is_file()
    except (ValueError, OSError):
        return not_found
    if is_file:
        # Vite fingerprints everything under assets/, so those can be cached for good.
        cache = "public, max-age=31536000, immutable" if path.startswith("assets/") else "no-cache"
        return FileResponse(target, headers={**_APP_FILE_HEADERS, "Cache-Control": cache})
    if path.startswith("assets/") or not target.is_relative_to(dist):
        return not_found
    return FileResponse(index_html, headers={**_APP_FILE_HEADERS, "Cache-Control": "no-cache"})


def main() -> None:
    uvicorn.run(
        "simple_chat_agent.api.main:app",
        host=os.environ.get("SIMPLE_CHAT_API_HOST", "127.0.0.1"),
        port=8000,
        reload=os.environ.get("SIMPLE_CHAT_API_RELOAD", "false").lower() in {"1", "true"},
        proxy_headers=True,
        forwarded_allow_ips=os.environ.get("FORWARDED_ALLOW_IPS", "127.0.0.1"),
    )


if __name__ == "__main__":
    main()
