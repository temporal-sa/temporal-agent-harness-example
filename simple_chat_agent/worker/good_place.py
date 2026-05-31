"""The Good Place profanity censor.

Pure, dependency-free string substitution: swears become wholesome
near-homophones. Whole-word matching with case preservation; idempotent.
"""
from __future__ import annotations

import re
from typing import Any

# Lowercased word/phrase -> wholesome replacement. Derivatives are explicit
# because whole-word matching will not fire on a substring (e.g. \bfuck\b does
# not match "fucking").
_REPLACEMENTS: dict[str, str] = {
    "son of a bitch": "son of a bench",
    "motherfucker": "motherforker",
    "motherfuckers": "motherforkers",
    "bullshit": "bullshirt",
    "asshole": "ash-hole",
    "assholes": "ash-holes",
    "fucking": "forking",
    "fucked": "forked",
    "fucker": "forker",
    "fuckers": "forkers",
    "shitty": "shirty",
    "shits": "shirts",
    "bitches": "benches",
    "fuck": "fork",
    "shit": "shirt",
    "bitch": "bench",
    "damn": "dang",
    "ass": "ash",
}

# One case-insensitive regex; longest keys first so phrases/derivatives win
# over their shorter roots.
_PATTERN = re.compile(
    r"\b("
    + "|".join(re.escape(k) for k in sorted(_REPLACEMENTS, key=len, reverse=True))
    + r")\b",
    re.IGNORECASE,
)


def _apply_case(source: str, target: str) -> str:
    if source.isupper():
        return target.upper()
    if source[:1].isupper():
        return target[:1].upper() + target[1:]
    return target


def censor(text: str) -> str:
    def _replace(match: re.Match[str]) -> str:
        word = match.group(0)
        return _apply_case(word, _REPLACEMENTS[word.lower()])

    return _PATTERN.sub(_replace, text)


def censor_content(content: str | list[Any]) -> str | list[Any]:
    """Censor an Anthropic message ``content`` value.

    Strings are censored directly. For block lists, only ``text`` blocks are
    rewritten; every other block (tool_use, tool_result, image, ...) is passed
    through unchanged. Never mutates the input in place.
    """
    if isinstance(content, str):
        return censor(content)

    censored: list[Any] = []
    for block in content:
        if (
            isinstance(block, dict)
            and block.get("type") == "text"
            and isinstance(block.get("text"), str)
        ):
            new_block = dict(block)
            new_block["text"] = censor(block["text"])
            censored.append(new_block)
        else:
            censored.append(block)
    return censored
