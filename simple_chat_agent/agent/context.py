"""Context management for the coding agent (PRD A17): a pure history processor.

``fit_history`` takes a Pydantic AI message history and returns one that fits the model's
context window. It runs in Temporal workflow code (as a ``before_model_request`` capability),
so it is pure: no I/O, no tokenizer, no clock, no randomness, and it never mutates its input.
Token counts are a character-based estimate (``chars_per_token``, default 4.0).

Algorithm (ported from ``agent_harness/sliding_window_context_manager.py``):

1. The history is split into the *head* (the first request: system prompt and first user
   prompt) and *exchange units*: one ``ModelResponse`` plus the ``ModelRequest`` that follows
   it (the tool results for its calls, and/or the next user prompt). Keeping or dropping whole
   units keeps requests and responses alternating and every tool call next to its result.
2. Incomplete tool exchanges (a call without its result, or a result without its call) are
   dropped, except in the last unit, which is the request being sent now.
3. Sliding window: above ``window`` messages, the oldest units are dropped down to
   ``window * window_low_water`` messages (hysteresis, so we don't edit the prefix and lose
   the prompt cache on every request).
4. Token budget: above the budget, the oldest units are dropped down to
   ``budget * token_low_water``; then the largest tool results become a JSON stub
   ``{"truncated": true, "original_chars": n, "preview": ...}``; then the largest text (model
   text, user prompts, tool-call arguments) is truncated.
5. When messages were dropped, a compaction marker is appended to the head request. The
   marker carries the cumulative count, so re-applying the processor is a no-op.
6. When anything was edited, Anthropic thinking blocks are removed: their signatures bind the
   exact prefix that produced them, and accounts created on or after 2026-08-31 get a 400 for
   replaying a block after an edit. Other providers' reasoning is left alone (OpenAI function
   calls reference their reasoning item; Gemini signatures live on the call part itself).

Always kept: the head request, the last unit (the current request), the unit holding the
latest user prompt, every system prompt part (hoisted into the head if its unit is dropped),
and request ``instructions``.
"""

from __future__ import annotations

import dataclasses
import json
import math
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from pydantic_ai.capabilities import AbstractCapability
from pydantic_ai.exceptions import ModelHTTPError
from pydantic_ai.messages import (
    BaseToolCallPart,
    BinaryContent,
    CompactionPart,
    FilePart,
    ModelMessage,
    ModelRequest,
    ModelResponse,
    NativeToolCallPart,
    NativeToolReturnPart,
    RetryPromptPart,
    SystemPromptPart,
    TextContent,
    TextPart,
    ThinkingPart,
    ToolCallPart,
    ToolReturnPart,
    UserPromptPart,
    is_multi_modal_content,
)

__all__ = [
    "COMPACTION_MARKER_PREFIX",
    "DEFAULT_CHARS_PER_TOKEN",
    "DEFAULT_SAFETY_MARGIN",
    "DEFAULT_WINDOW",
    "MAX_FILL",
    "MODEL_LIMITS",
    "ContextBudget",
    "ContextFit",
    "budget_for",
    "context_overflow_error",
    "estimate_tokens",
    "fit_history",
    "fit_with",
    "history_processor",
    "is_context_overflow",
    "model_limits",
    "tighter",
]

DEFAULT_CHARS_PER_TOKEN = 4.0
DEFAULT_SAFETY_MARGIN = 4_000
DEFAULT_RESERVE_OUTPUT = 16_000  # model_registry.settings_for sets max_tokens=16_000
DEFAULT_WINDOW = 40
MAX_FILL = 0.85  # PRD A17: never fill more than 85% of the window (estimated)
WINDOW_LOW_WATER = 0.5
TOKEN_LOW_WATER = 0.8
PREVIEW_CHARS = 1_000
MIN_TRUNCATE_CHARS = 512  # smaller items are left alone; a stub would not save anything

# Estimates for content we can't measure in characters.
_MEDIA_TOKENS = 1_600  # an image or file URL, roughly one ~1.15 MP image
_PART_OVERHEAD_CHARS = 16  # role/type/id framing per part
_MESSAGE_OVERHEAD_CHARS = 16

COMPACTION_MARKER_PREFIX = "[Context compacted: "
_TEXT_TRUNCATED_PREFIX = "[Truncated to fit the context window; original "
_STRIP_THINKING_PROVIDERS = frozenset({"anthropic"})
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Model limits
# ---------------------------------------------------------------------------

# (max_input_tokens, max_output_tokens) per allowlisted model (model_registry.DEFAULT_ALLOWLIST).
# max_input_tokens is the most the prompt may hold; the fit subtracts the reserved output on
# top, so for providers whose window is shared (Anthropic, Gemini) the result is always valid.
# Sources (checked 2026-10-16):
# - Claude Opus 5.5 / Sonnet 5.5 / Haiku 5.5: 1M-token context window, 128K max output
#   (Anthropic model table, cached 2026-10-06; also served live by GET /v1/models as
#   max_input_tokens / max_tokens). Anthropic counts input + max_tokens against the window.
# - GPT-5.5 and GPT-5.4-mini: OpenAI's GPT-5 family documents a 400K context with up to 128K
#   output and 272K maximum input. Some newer GPT-5.x models are larger; we have not verified
#   either model's card, so we use the conservative GPT-5 figure (272K input, 128K output).
# - Gemini 3.5 Flash / Gemini 3.1 Pro Preview: Gemini 3-family limit of 1,048,576 input and
#   65,536 output tokens (Gemini model cards). Assumed unchanged for these versions.
MODEL_LIMITS: dict[str, tuple[int, int]] = {
    "anthropic:claude-opus-5-5": (1_000_000, 128_000),
    "anthropic:claude-sonnet-5-5": (1_000_000, 128_000),
    "anthropic:claude-haiku-5-5": (1_000_000, 128_000),
    "openai:gpt-5.5": (272_000, 128_000),
    "openai:gpt-5.4-mini": (272_000, 128_000),
    "google:gemini-3.5-flash": (1_048_576, 65_536),
    "google:gemini-3.1-pro-preview": (1_048_576, 65_536),
}
# Unknown model (e.g. an AGENT_MODELS_<PROVIDER> override): assume a 128K window and 16K output.
_UNKNOWN_LIMITS = (128_000, 16_000)


def model_limits(registry_key: str) -> tuple[int, int]:
    """``(max_input_tokens, max_output_tokens)`` for a ``provider:model`` registry key.

    A bare model name also works. Unknown models get a conservative 128K/16K."""
    if registry_key in MODEL_LIMITS:
        return MODEL_LIMITS[registry_key]
    for key, limits in MODEL_LIMITS.items():
        if key.partition(":")[2] == registry_key:
            return limits
    return _UNKNOWN_LIMITS


# ---------------------------------------------------------------------------
# Budget
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ContextBudget:
    """Everything ``fit_history`` needs to decide how much history fits."""

    max_input_tokens: int
    reserve_output_tokens: int = DEFAULT_RESERVE_OUTPUT
    system_and_tools_tokens: int = 0
    safety_margin: int = DEFAULT_SAFETY_MARGIN
    chars_per_token: float = DEFAULT_CHARS_PER_TOKEN
    window: int = DEFAULT_WINDOW
    max_fill: float = MAX_FILL

    def __post_init__(self) -> None:
        if self.max_input_tokens < 1:
            raise ValueError("max_input_tokens must be at least 1")
        if min(self.reserve_output_tokens, self.system_and_tools_tokens, self.safety_margin) < 0:
            raise ValueError("token reservations cannot be negative")
        if self.chars_per_token <= 0:
            raise ValueError("chars_per_token must be greater than 0")
        if self.window < 2:
            raise ValueError("window must be at least 2")
        if not 0 < self.max_fill <= 1:
            raise ValueError("max_fill must be in (0, 1]")

    @property
    def message_tokens(self) -> int:
        """Tokens available to the messages themselves (may be <= 0 for a hopeless budget)."""
        by_reservation = (
            self.max_input_tokens
            - self.reserve_output_tokens
            - self.system_and_tools_tokens
            - self.safety_margin
        )
        by_fill = math.floor(self.max_input_tokens * self.max_fill) - self.system_and_tools_tokens
        return min(by_reservation, by_fill)


def budget_for(
    registry_key: str,
    *,
    reserve_output_tokens: int = DEFAULT_RESERVE_OUTPUT,
    system_and_tools_tokens: int = 0,
    **overrides: Any,
) -> ContextBudget:
    """A budget for an allowlisted model, with the output reservation clamped to its limit."""
    max_input, max_output = model_limits(registry_key)
    return ContextBudget(
        max_input_tokens=max_input,
        reserve_output_tokens=min(reserve_output_tokens, max_output),
        system_and_tools_tokens=system_and_tools_tokens,
        **overrides,
    )


def tighter(budget: ContextBudget) -> ContextBudget:
    """The budget for the one overflow retry: 4x the safety margin (at least 16K tokens) and a
    more pessimistic estimate (at most 2.5 chars per token)."""
    return dataclasses.replace(
        budget,
        safety_margin=max(budget.safety_margin * 4, 16_000),
        chars_per_token=min(budget.chars_per_token, 2.5),
    )


# ---------------------------------------------------------------------------
# Estimation
# ---------------------------------------------------------------------------


def _json(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, default=str, separators=(",", ":"))
    except (TypeError, ValueError):
        return str(value)


def _content_cost(content: Any) -> tuple[int, int]:
    """(chars, media_tokens) of a prompt or tool-return content value."""
    if isinstance(content, str):
        return len(content), 0
    if isinstance(content, TextContent):
        return len(content.content), 0
    if is_multi_modal_content(content):
        if isinstance(content, BinaryContent) and not content.is_image:
            return len(content.data), 0
        return 0, _MEDIA_TOKENS
    if isinstance(content, (list, tuple)):
        chars = media = 0
        for item in content:
            c, m = _content_cost(item)
            chars += c
            media += m
        return chars, media
    return len(_json(content)), 0


def _part_cost(part: Any) -> tuple[int, int]:
    if isinstance(part, (SystemPromptPart, TextPart, ThinkingPart)):
        return len(part.content) + _PART_OVERHEAD_CHARS, 0
    if isinstance(part, (UserPromptPart, ToolReturnPart, NativeToolReturnPart)):
        chars, media = _content_cost(part.content)
        name = getattr(part, "tool_name", "") or ""
        return chars + len(name) + _PART_OVERHEAD_CHARS, media
    if isinstance(part, RetryPromptPart):
        content = part.content if isinstance(part.content, str) else _json(part.content)
        return len(content) + len(part.tool_name or "") + _PART_OVERHEAD_CHARS, 0
    if isinstance(part, BaseToolCallPart):
        args = part.args if isinstance(part.args, str) else _json(part.args or {})
        return len(args) + len(part.tool_name) + _PART_OVERHEAD_CHARS, 0
    if isinstance(part, FilePart):
        return _PART_OVERHEAD_CHARS, _MEDIA_TOKENS
    if isinstance(part, CompactionPart):
        return len(part.content or "") + _PART_OVERHEAD_CHARS, 0
    return _PART_OVERHEAD_CHARS, 0


def _message_tokens(message: ModelMessage, chars_per_token: float) -> int:
    # Request `instructions` are not counted here: only the last request's are sent, and the
    # caller accounts for them in system_and_tools_tokens.
    chars = _MESSAGE_OVERHEAD_CHARS
    media = 0
    for part in message.parts:
        c, m = _part_cost(part)
        chars += c
        media += m
    return math.ceil(chars / chars_per_token) + media


def estimate_tokens(
    messages: Sequence[ModelMessage], *, chars_per_token: float = DEFAULT_CHARS_PER_TOKEN
) -> int:
    """Estimated input tokens for these messages (character-based, deterministic)."""
    if chars_per_token <= 0:
        raise ValueError("chars_per_token must be greater than 0")
    return sum(_message_tokens(m, chars_per_token) for m in messages)


def estimate_text_tokens(text: str, *, chars_per_token: float = DEFAULT_CHARS_PER_TOKEN) -> int:
    return math.ceil(len(text) / chars_per_token)


# ---------------------------------------------------------------------------
# Structure helpers
# ---------------------------------------------------------------------------


def _is_marker(part: Any) -> bool:
    return (
        isinstance(part, UserPromptPart)
        and isinstance(part.content, str)
        and part.content.startswith(COMPACTION_MARKER_PREFIX)
    )


def _marker_count(part: UserPromptPart) -> int:
    digits = ""
    for ch in str(part.content)[len(COMPACTION_MARKER_PREFIX) :]:
        if not ch.isdigit():
            break
        digits += ch
    return int(digits) if digits else 0


def _marker_text(dropped: int) -> str:
    return (
        f"{COMPACTION_MARKER_PREFIX}{dropped} earlier messages were removed to fit the model's "
        "context window. Earlier tool results and details may be missing; re-read files or re-run "
        "commands rather than relying on memory of them.]"
    )


def _is_user_prompt(part: Any) -> bool:
    return isinstance(part, UserPromptPart) and not _is_marker(part)


def _call_ids(response: ModelResponse) -> set[str]:
    return {
        p.tool_call_id
        for p in response.parts
        if isinstance(p, ToolCallPart) and not isinstance(p, NativeToolCallPart)
    }


def _is_return(part: Any) -> bool:
    return isinstance(part, ToolReturnPart) or (
        isinstance(part, RetryPromptPart) and part.tool_name is not None
    )


def _return_ids(requests: Sequence[ModelMessage]) -> set[str]:
    return {p.tool_call_id for m in requests for p in m.parts if _is_return(p)}


def _replace_parts(message: Any, parts: list[Any]) -> Any:
    return dataclasses.replace(message, parts=parts)


@dataclass
class _Split:
    head: list[ModelMessage]  # first request plus any requests before the first response
    units: list[list[ModelMessage]]  # [ModelResponse, ModelRequest, ...]


def _split(messages: Sequence[ModelMessage]) -> _Split:
    head: list[ModelMessage] = []
    units: list[list[ModelMessage]] = []
    for message in messages:
        if isinstance(message, ModelResponse):
            units.append([message])
        elif units:
            units[-1].append(message)
        else:
            head.append(message)
    return _Split(head, units)


def _repair_units(split: _Split) -> tuple[_Split, int, bool]:
    """Drop incomplete tool exchanges outside the last unit.

    Returns the new split, the number of messages dropped, and whether anything changed."""
    changed = False
    dropped = 0
    # Results in the head have no call before them.
    head: list[ModelMessage] = []
    for message in split.head:
        if isinstance(message, ModelRequest) and any(_is_return(p) for p in message.parts):
            kept = [p for p in message.parts if not _is_return(p)]
            if kept:
                message = _replace_parts(message, kept)
                changed = True
        head.append(message)

    units: list[list[ModelMessage]] = []
    last = len(split.units) - 1
    for index, unit in enumerate(split.units):
        if index == last:
            units.append(unit)
            continue
        response, requests = unit[0], unit[1:]
        calls = _call_ids(response)  # type: ignore[arg-type]
        returns = _return_ids(requests)
        if calls == returns:
            units.append(unit)
            continue
        changed = True
        matched = calls & returns
        new_response = _replace_parts(
            response,
            [
                p
                for p in response.parts
                if not (isinstance(p, ToolCallPart) and not isinstance(p, NativeToolCallPart))
                or p.tool_call_id in matched
            ],
        )
        new_requests = [
            _replace_parts(m, [p for p in m.parts if not _is_return(p) or p.tool_call_id in matched])
            for m in requests
        ]
        if not new_response.parts or not new_requests or any(not m.parts for m in new_requests):
            dropped += len(unit)
            continue
        units.append([new_response, *new_requests])
    return _Split(head, units), dropped, changed


# ---------------------------------------------------------------------------
# Truncation
# ---------------------------------------------------------------------------


def _parse_stub(content: Any) -> tuple[int, str] | None:
    if not isinstance(content, str) or not content.startswith('{"truncated"'):
        return None
    try:
        data = json.loads(content)
    except ValueError:
        return None
    if isinstance(data, dict) and data.get("truncated") is True and "original_chars" in data:
        return int(data["original_chars"]), str(data.get("preview", ""))
    return None


def _stub(original_chars: int, preview: str) -> str:
    return json.dumps(
        {"truncated": True, "original_chars": original_chars, "preview": preview},
        ensure_ascii=False,
    )


def _tool_result_text(part: ToolReturnPart) -> str | None:
    """The tool result as text, or None when it carries files (left alone)."""
    content = part.content
    if isinstance(content, str):
        return content
    if is_multi_modal_content(content):
        return None
    if isinstance(content, (list, tuple)) and any(is_multi_modal_content(c) for c in content):
        return None
    return _json(content)


def _shrunk_preview(source: str, current_len: int, excess_chars: int) -> str | None:
    """A preview of ``source`` shorter than ``current_len``, or None if no progress is possible."""
    target = min(PREVIEW_CHARS, max(0, len(source) - excess_chars))
    if target >= current_len:
        target = current_len // 2
    if target >= current_len:
        return None
    return source[:target]


def _truncate_tool_result(part: ToolReturnPart, excess_chars: int) -> ToolReturnPart | None:
    text = _tool_result_text(part)
    if text is None:
        return None
    stub = _parse_stub(text)
    if stub is not None:
        original, preview = stub
        if not preview:
            return None
        new_preview = _shrunk_preview(preview, len(preview), excess_chars)
        if new_preview is None:
            return None
        return dataclasses.replace(part, content=_stub(original, new_preview))
    if len(text) < MIN_TRUNCATE_CHARS:
        return None
    new = _stub(len(text), text[: min(PREVIEW_CHARS, max(0, len(text) - excess_chars))])
    if len(new) >= len(text):
        return None
    return dataclasses.replace(part, content=new)


def _truncated_text(text: str, excess_chars: int) -> str | None:
    if text.startswith(_TEXT_TRUNCATED_PREFIX):
        header, sep, body = text.partition("]\n")
        if not sep or not body:
            return None
        new_body = _shrunk_preview(body, len(body), excess_chars)
        if new_body is None:
            return None
        return f"{header}]\n{new_body}"
    if len(text) < MIN_TRUNCATE_CHARS:
        return None
    body = text[: min(PREVIEW_CHARS, max(0, len(text) - excess_chars))]
    new = f"{_TEXT_TRUNCATED_PREFIX}{len(text)} chars]\n{body}"
    return new if len(new) < len(text) else None


def _truncate_text_part(part: Any, excess_chars: int) -> Any | None:
    if isinstance(part, TextPart) or (_is_user_prompt(part) and isinstance(part.content, str)):
        new = _truncated_text(part.content, excess_chars)
        return None if new is None else dataclasses.replace(part, content=new)
    if isinstance(part, ToolCallPart) and not isinstance(part, NativeToolCallPart):
        args = part.args if isinstance(part.args, str) else _json(part.args or {})
        stub = _parse_stub(args)
        if stub is not None:
            original, preview = stub
            new_preview = _shrunk_preview(preview, len(preview), excess_chars) if preview else None
            if new_preview is None:
                return None
            return dataclasses.replace(
                part, args={"truncated": True, "original_chars": original, "preview": new_preview}
            )
        if len(args) < MIN_TRUNCATE_CHARS:
            return None
        preview = args[: min(PREVIEW_CHARS, max(0, len(args) - excess_chars))]
        return dataclasses.replace(
            part, args={"truncated": True, "original_chars": len(args), "preview": preview}
        )
    return None


def _part_size(part: Any) -> int:
    return _part_cost(part)[0]


# ---------------------------------------------------------------------------
# The processor
# ---------------------------------------------------------------------------


def fit_with(messages: Sequence[ModelMessage], budget: ContextBudget) -> list[ModelMessage]:
    """Fit ``messages`` to ``budget``. Pure; never mutates its input; idempotent."""
    if not messages:
        return list(messages)
    cpt = budget.chars_per_token
    split = _split(messages)

    # 0. Take the existing compaction marker off the head; it is re-added at the end.
    prior_dropped = 0
    marker_timestamp = None
    head: list[ModelMessage] = []
    for message in split.head:
        markers = [p for p in message.parts if _is_marker(p)]
        if markers:
            prior_dropped = max(prior_dropped, *(_marker_count(p) for p in markers))
            marker_timestamp = markers[-1].timestamp
            message = _replace_parts(message, [p for p in message.parts if not _is_marker(p)])
        head.append(message)
    split = _Split(head, split.units)

    # 1. Incomplete tool exchanges.
    split, dropped, changed = _repair_units(split)
    units = split.units
    head = split.head

    # Units that are never dropped: the last one (the request being sent) and the one that
    # holds the latest user prompt (the task the model is working on).
    protected: set[int] = set()
    if units:
        protected.add(len(units) - 1)
        for index in range(len(units) - 1, -1, -1):
            if any(_is_user_prompt(p) for m in units[index][1:] for p in m.parts):
                protected.add(index)
                break
    alive = [True] * len(units)
    hoisted: list[SystemPromptPart] = []

    def drop(index: int) -> None:
        nonlocal dropped, changed
        alive[index] = False
        dropped += len(units[index])
        changed = True
        for m in units[index]:
            hoisted.extend(p for p in m.parts if isinstance(p, SystemPromptPart))

    def drop_oldest_until(done: Callable[[], bool]) -> None:
        for index in range(len(units)):
            if done():
                return
            if alive[index] and index not in protected:
                drop(index)

    # 2. Sliding window.
    def message_count() -> int:
        return len(head) + sum(len(u) for i, u in enumerate(units) if alive[i])

    if message_count() > budget.window:
        low = max(2, math.floor(budget.window * WINDOW_LOW_WATER))
        drop_oldest_until(lambda: message_count() <= low)

    # 3. Token budget.
    unit_tokens = [sum(_message_tokens(m, cpt) for m in u) for u in units]
    head_tokens = sum(_message_tokens(m, cpt) for m in head)
    marker_tokens = math.ceil((len(_marker_text(10**6)) + _PART_OVERHEAD_CHARS) / cpt)

    def total() -> int:
        live = sum(t for i, t in enumerate(unit_tokens) if alive[i])
        hoisted_tokens = math.ceil(sum(_part_size(p) for p in hoisted) / cpt)
        return head_tokens + live + hoisted_tokens + (marker_tokens if prior_dropped or dropped else 0)

    available = budget.message_tokens
    if total() > available:
        target = math.floor(available * TOKEN_LOW_WATER) if available > 0 else 0

        drop_oldest_until(lambda: total() <= target)

        if total() > target:
            # Work on the surviving messages, flattened, from here on.
            flat: list[ModelMessage] = [*head]
            for i, u in enumerate(units):
                if alive[i]:
                    flat.extend(u)
            tokens = [_message_tokens(m, cpt) for m in flat]
            extra = total() - sum(tokens)  # marker and hoisted parts

            def run_phase(candidate: Callable[[Any, Any], bool], shrink: Callable[[Any, int], Any]) -> None:
                nonlocal changed
                exhausted: set[tuple[int, int]] = set()
                while sum(tokens) + extra > target:
                    best: tuple[int, int, int] | None = None
                    for mi, message in enumerate(flat):
                        for pi, part in enumerate(message.parts):
                            if (mi, pi) in exhausted or not candidate(message, part):
                                continue
                            size = _part_size(part)
                            if best is None or size > best[2]:
                                best = (mi, pi, size)
                    if best is None:
                        return
                    mi, pi, _ = best
                    excess_chars = math.ceil((sum(tokens) + extra - target) * cpt)
                    new_part = shrink(flat[mi].parts[pi], excess_chars)
                    if new_part is None or _part_size(new_part) >= best[2]:
                        exhausted.add((mi, pi))
                        continue
                    parts = list(flat[mi].parts)
                    parts[pi] = new_part
                    flat[mi] = _replace_parts(flat[mi], parts)
                    tokens[mi] = _message_tokens(flat[mi], cpt)
                    changed = True

            # 3b. Largest tool results become stubs.
            run_phase(
                lambda m, p: isinstance(m, ModelRequest)
                and isinstance(p, ToolReturnPart)
                and type(p) is ToolReturnPart
                and p.tool_kind is None,
                _truncate_tool_result,
            )
            # 3c. Then the largest text: model text, user prompts, tool-call arguments.
            run_phase(
                lambda m, p: isinstance(p, (TextPart, UserPromptPart))
                or (isinstance(p, ToolCallPart) and type(p) is ToolCallPart and p.tool_kind is None),
                _truncate_text_part,
            )
            head = flat[: len(head)]
            rest = flat[len(head) :]
            units = [rest]  # already flattened; alive/protected no longer needed
            alive = [True]

    if not changed and not dropped:
        return list(messages)

    # 4. Reassemble: head (+ hoisted system prompts, + marker), then surviving units.
    output: list[ModelMessage] = []
    for i, u in enumerate(units):
        if alive[i]:
            output.extend(u)
    if head and isinstance(head[0], ModelRequest):
        first = head[0]
        parts = list(first.parts)
        if hoisted:
            n_system = 0
            while n_system < len(parts) and isinstance(parts[n_system], SystemPromptPart):
                n_system += 1
            parts[n_system:n_system] = hoisted
        total_dropped = prior_dropped + dropped
        if total_dropped:
            if marker_timestamp is None:
                stamps = [p.timestamp for p in parts if isinstance(p, UserPromptPart)]
                # Never the default (the wall clock): this runs in workflow code.
                marker_timestamp = (stamps[0] if stamps else first.timestamp) or _EPOCH
            parts.append(UserPromptPart(_marker_text(total_dropped), timestamp=marker_timestamp))
        head = [_replace_parts(first, parts), *head[1:]]
    output = [*head, *output]

    # 5. Edited history invalidates Anthropic thinking signatures: remove those blocks.
    return [_strip_thinking(m) for m in output]


def _strip_thinking(message: ModelMessage) -> ModelMessage:
    if not isinstance(message, ModelResponse):
        return message
    if (message.provider_name or "") not in _STRIP_THINKING_PROVIDERS:
        return message
    if not any(isinstance(p, ThinkingPart) for p in message.parts):
        return message
    parts = [p for p in message.parts if not isinstance(p, ThinkingPart)]
    if not parts:
        parts = [TextPart(content="(reasoning omitted)")]
    return _replace_parts(message, parts)


def fit_history(
    messages: Sequence[ModelMessage],
    *,
    max_input_tokens: int,
    reserve_output_tokens: int,
    system_and_tools_tokens: int,
    safety_margin: int,
    window: int = DEFAULT_WINDOW,
    chars_per_token: float = DEFAULT_CHARS_PER_TOKEN,
    max_fill: float = MAX_FILL,
) -> list[ModelMessage]:
    """Fit a message history to the model's context window. See the module docstring."""
    return fit_with(
        messages,
        ContextBudget(
            max_input_tokens=max_input_tokens,
            reserve_output_tokens=reserve_output_tokens,
            system_and_tools_tokens=system_and_tools_tokens,
            safety_margin=safety_margin,
            chars_per_token=chars_per_token,
            window=window,
            max_fill=max_fill,
        ),
    )


def history_processor(
    budget: ContextBudget,
) -> Callable[[list[ModelMessage]], Awaitable[list[ModelMessage]]]:
    """An async history processor for ``ProcessHistory(...)`` or ``history_processors=[...]``.

    Async so Pydantic AI never hands it to a thread executor. Uses the fixed
    ``budget.system_and_tools_tokens``; prefer ``ContextFit``, which measures them per request.
    """

    async def process(messages: list[ModelMessage]) -> list[ModelMessage]:
        return fit_with(messages, budget)

    return process


@dataclass
class ContextFit(AbstractCapability[Any]):
    """Capability that fits the history before every model request.

    Measures the request's instructions and tool definitions (system_and_tools_tokens) and
    reads ``max_tokens`` from the request's model settings (reserve_output_tokens), so only
    the model's window and the estimate settings are fixed per run.
    """

    budget: ContextBudget

    async def before_model_request(self, ctx: Any, request_context: Any) -> Any:
        request_context.messages = fit_with(
            request_context.messages, self.request_budget(request_context)
        )
        return request_context

    def request_budget(self, request_context: Any) -> ContextBudget:
        cpt = self.budget.chars_per_token
        params = request_context.model_request_parameters
        chars = 0
        messages = request_context.messages
        last = messages[-1] if messages else None
        if isinstance(last, ModelRequest) and last.instructions:
            chars += len(last.instructions)
        elif params.instruction_parts:
            chars += sum(len(p.content) for p in params.instruction_parts)
        for tool in [*params.function_tools, *params.output_tools]:
            chars += len(tool.name) + len(tool.description or "") + len(_json(tool.parameters_json_schema))
        settings = request_context.model_settings or {}
        reserve = settings.get("max_tokens") or self.budget.reserve_output_tokens
        return dataclasses.replace(
            self.budget,
            system_and_tools_tokens=self.budget.system_and_tools_tokens + math.ceil(chars / cpt),
            reserve_output_tokens=int(reserve),
        )

    @classmethod
    def get_serialization_name(cls) -> str | None:
        return None


# ---------------------------------------------------------------------------
# Overflow recognition
# ---------------------------------------------------------------------------

# Substrings (lower-cased) of provider context-window errors, as they appear in the error body
# Pydantic AI carries on ModelHTTPError.body:
# - Anthropic, 400 invalid_request_error: "prompt is too long: 215000 tokens > 200000 maximum";
#   "input length and `max_tokens` exceed context limit: 188000 + 21333 > 200000, ...".
#   413 request_too_large: "Request exceeds the maximum allowed number of bytes." (any 413)
# - OpenAI, 400 with code "context_length_exceeded": "This model's maximum context length is
#   128000 tokens. However, your messages resulted in ..." (Chat) / "Your input exceeds the
#   context window of this model. Please adjust your input and try again." (Responses).
# - Gemini, 400 INVALID_ARGUMENT: "The input token count (1200000) exceeds the maximum number of
#   tokens allowed (1048576)."
_OVERFLOW_MARKERS = (
    "prompt is too long",
    "exceed context limit",
    "context_length_exceeded",
    "maximum context length",
    "exceeds the context window",
    "context window exceeded",
    "input token count",
    "exceeds the maximum number of tokens allowed",
    "request_too_large",
    "too many input tokens",
)
CONTEXT_OVERFLOW_ERROR_TYPE = "ContextOverflow"


def _http_overflow(exc: ModelHTTPError) -> bool:
    if exc.status_code == 413:
        return True
    if exc.status_code != 400:
        return False
    return any(marker in _json(exc.body).lower() for marker in _OVERFLOW_MARKERS)


def is_context_overflow(exc: BaseException | None) -> bool:
    """True if ``exc`` (or anything in its cause chain) is a provider context-window error.

    Recognises the raw ``ModelHTTPError`` in the model activity, and, in the workflow, the
    ``ActivityError`` whose cause is an ``ApplicationError`` of type ``ContextOverflow`` (see
    ``context_overflow_error``)."""
    seen: set[int] = set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        if isinstance(exc, ModelHTTPError) and _http_overflow(exc):
            return True
        if getattr(exc, "type", None) == CONTEXT_OVERFLOW_ERROR_TYPE:
            return True
        cause = getattr(exc, "cause", None)  # temporalio FailureError
        exc = cause if isinstance(cause, BaseException) else exc.__cause__
    return False


def context_overflow_error(exc: ModelHTTPError) -> Exception:
    """The activity-side error for a context overflow: non-retryable, typed ``ContextOverflow``
    so the workflow can recognise it after the body is gone."""
    from temporalio.exceptions import ApplicationError

    return ApplicationError(
        "The conversation is too long for the model's context window.",
        {"status_code": exc.status_code, "model": exc.model_name},
        type=CONTEXT_OVERFLOW_ERROR_TYPE,
        non_retryable=True,
    )
