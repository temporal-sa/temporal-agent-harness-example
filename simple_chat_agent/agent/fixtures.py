"""Demo fixture repos (P1 step 1.11, PRD §2.5).

``fixture_files(name)`` returns a fixture's seedable files; ``seed_workspace`` writes them
into a sandbox workspace through the executor ``/files/write`` API and makes an initial git
commit through ``/execute``, so the agent's ``git_diff`` / ``git_commit`` work from the start.

Fixtures live in ``tests/fixtures/<name>/``; set ``AGENT_FIXTURES_DIR`` to read them from
somewhere else (for example a copy baked into the worker image).
"""

from __future__ import annotations

import os
import re
import uuid
from pathlib import Path

from pydantic import BaseModel

from simple_chat_agent.agent.tools.artifacts import ExecutorClient

_DEFAULT_ROOT = Path(__file__).resolve().parents[2] / "tests" / "fixtures"
# Kept out of the seeded workspace: build/cache noise, and README.md, which documents the
# planted bug for the demo team and would hand the agent the answer.
_SKIP_DIRS = frozenset({"__pycache__", ".pytest_cache", ".venv", ".git", ".mypy_cache", ".ruff_cache"})
_SKIP_FILES = frozenset({"README.md", ".DS_Store"})
_SKIP_SUFFIXES = (".pyc", ".pyo")
_NAME = re.compile(r"^[A-Za-z0-9_-]+$")

SEED_AUTHOR = "Demo Seed"
SEED_EMAIL = "seed@example.invalid"


class SeedResult(BaseModel):
    workspace_id: str
    fixture: str
    files: list[str]
    commit: str


def fixtures_root() -> Path:
    override = os.environ.get("AGENT_FIXTURES_DIR", "").strip()
    return Path(override) if override else _DEFAULT_ROOT


def fixture_files(name: str) -> dict[str, str]:
    """Relative POSIX path -> UTF-8 text for every seedable file of fixture ``name``,
    sorted by path."""
    if not _NAME.fullmatch(name):
        raise ValueError(f"Invalid fixture name: {name!r}")
    root = fixtures_root() / name
    if not root.is_dir():
        raise FileNotFoundError(f"No fixture named {name!r} under {fixtures_root()}")
    files: dict[str, str] = {}
    for path in sorted(root.rglob("*")):
        rel = path.relative_to(root)
        if any(part in _SKIP_DIRS for part in rel.parts) or not path.is_file():
            continue
        if path.name in _SKIP_FILES or path.suffix in _SKIP_SUFFIXES:
            continue
        files[rel.as_posix()] = path.read_text(encoding="utf-8")
    return files


async def seed_workspace(
    workspace_id: str,
    name: str = "refund_service",
    *,
    executor: ExecutorClient | None = None,
    execution_id: str | None = None,
) -> SeedResult:
    """Write fixture ``name`` into ``workspace_id`` and commit it as "Initial import".

    Raises ``ApplicationError`` (from the executor client) when the sandbox refuses a write
    or is unreachable, and ``RuntimeError`` when the git step fails.
    """
    client = executor or ExecutorClient()
    files = fixture_files(name)
    for path, content in files.items():
        await client.files(
            "write",
            {
                "workspace_id": workspace_id,
                "path": path,
                "content": content,
                "create_parents": True,
            },
        )
    command = (
        "set -e; "
        "git init -q; "
        "git symbolic-ref HEAD refs/heads/main; "
        "git add -A; "
        f"git -c user.name='{SEED_AUTHOR}' -c user.email='{SEED_EMAIL}' "
        "commit -q -m 'Initial import'; "
        "git rev-parse HEAD"
    )
    result = await client.execute(
        workspace_id=workspace_id,
        execution_id=execution_id or f"seed-{name}-{uuid.uuid4().hex[:12]}",
        command=command,
        timeout_seconds=60,
        network=False,
    )
    if result.get("exit_code") != 0:
        detail = (result.get("stderr") or result.get("error") or "").strip()
        raise RuntimeError(f"Seeding git for {name!r} failed: {detail}")
    commit = (result.get("stdout") or "").strip().splitlines()
    return SeedResult(
        workspace_id=workspace_id,
        fixture=name,
        files=list(files),
        commit=commit[-1] if commit else "",
    )
