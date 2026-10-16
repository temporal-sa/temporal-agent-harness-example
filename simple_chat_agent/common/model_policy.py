from __future__ import annotations

import re
from typing import Literal, TypeAlias

# Workflows cannot query Anthropic's model catalog deterministically, so the
# create_subagent schema advertises the currently supported choices explicitly.
SELECTABLE_ANTHROPIC_MODEL_IDS = (
    "claude-opus-4-8",
    "claude-sonnet-4-6",
    "claude-sonnet-4-5-20250929",
    "claude-haiku-4-5-20251001",
)
SelectableAnthropicModel: TypeAlias = Literal[
    "claude-opus-4-8",
    "claude-sonnet-4-6",
    "claude-sonnet-4-5-20250929",
    "claude-haiku-4-5-20251001",
]

_MODEL_ID = re.compile(
    r"^claude-(?P<family>opus|sonnet|haiku)-(?P<major>[0-9]+)-(?P<minor>[0-9]+)(?:-|$)",
    re.IGNORECASE,
)
_MAX_CLAUDE_4_MINOR = 8


def anthropic_model_is_allowed(model_id: str) -> bool:
    """Allow Claude 4 Sonnet/Haiku through 4.8, plus Opus 4.8 only."""

    match = _MODEL_ID.match(model_id.strip())
    if match is None:
        return False

    major = int(match.group("major"))
    minor = int(match.group("minor"))
    if major != 4 or minor > _MAX_CLAUDE_4_MINOR:
        return False
    if match.group("family").lower() == "opus":
        return minor == _MAX_CLAUDE_4_MINOR
    return True
