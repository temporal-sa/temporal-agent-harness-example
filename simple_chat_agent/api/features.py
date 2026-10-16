from __future__ import annotations

import os


def env_flag(name: str, *, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def github_tools_enabled() -> bool:
    return env_flag("SIMPLE_CHAT_GITHUB_TOOLS_ENABLED", default=True)
