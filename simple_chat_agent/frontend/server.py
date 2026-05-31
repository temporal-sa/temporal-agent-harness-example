from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles


STATIC_DIR = Path(__file__).resolve().parent / "static"
FRONTEND_INDEX = STATIC_DIR / "dist" / "index.html"

app = FastAPI()

# StaticFiles owns /static, including Vite's built assets under /static/dist.
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


def _index() -> FileResponse:
    return FileResponse(FRONTEND_INDEX)


@app.get("/")
@app.get("/index.html")
async def index() -> FileResponse:
    return _index()


@app.get("/favicon.ico")
async def favicon_ico() -> FileResponse:
    return FileResponse(STATIC_DIR / "favicon.ico")


@app.get("/favicon.svg")
async def favicon_svg() -> FileResponse:
    return FileResponse(STATIC_DIR / "favicon.svg")


@app.get("/{_path:path}")
async def spa_fallback(_path: str) -> FileResponse:
    return _index()
