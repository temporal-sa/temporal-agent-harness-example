"""A tiny MCP server for local checks (P4 step 4.1): streamable HTTP on 127.0.0.1.

    .venv/bin/python -m tests.fixtures.mcp.echo_server --port 8765

The SSRF guard refuses loopback, so the worker and API that use it run locally with
``AGENT_ENVIRONMENT=local`` and ``AGENT_MCP_LOOPBACK_PORTS=8765``. ``--token`` makes it require
``Authorization: Bearer <token>`` (anything else gets 401, to exercise "Reconnect").
"""

from __future__ import annotations

import argparse

import uvicorn
from mcp.server.fastmcp import FastMCP
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

server = FastMCP("echo", stateless_http=True)


@server.tool()
def echo(text: str) -> str:
    """Return the text unchanged, prefixed with 'echo: '."""
    return f"echo: {text}"


@server.tool()
def add(a: int, b: int) -> int:
    """Add two integers."""
    return a + b


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--token", default="")
    args = parser.parse_args()
    app = server.streamable_http_app()
    if args.token:
        expected = f"Bearer {args.token}"

        async def require_token(request, call_next):  # type: ignore[no-untyped-def]
            if request.headers.get("authorization") != expected:
                return JSONResponse({"error": "unauthorized"}, status_code=401)
            return await call_next(request)

        app.add_middleware(BaseHTTPMiddleware, dispatch=require_token)
    uvicorn.run(app, host="127.0.0.1", port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
