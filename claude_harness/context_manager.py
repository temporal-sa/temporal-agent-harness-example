from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Protocol, cast

from anthropic.types import MessageParam, ToolResultBlockParam

ContextSnapshot = dict[str, Any]
DEFAULT_MAX_CONTEXT_TOKENS = 200_000
DEFAULT_CONTEXT_SAFETY_MARGIN_TOKENS = 4_000
DEFAULT_CHARS_PER_TOKEN = 4.0


class ContextManager(Protocol):
    async def initialize(self, user_prompt: str) -> None:
        pass

    async def record_user_message(self, user_prompt: str) -> None:
        pass

    def restore(self, snapshot: ContextSnapshot) -> None:
        pass

    def snapshot(self) -> ContextSnapshot:
        pass

    async def messages_for_model(
        self,
        token_budget: ContextTokenBudget | None = None,
    ) -> list[MessageParam]:
        pass

    async def record_assistant_message(self, message: MessageParam) -> None:
        pass

    async def record_tool_results(
        self, tool_results: list[ToolResultBlockParam]
    ) -> None:
        pass


ContextManagerFactory = Callable[[], ContextManager]


@dataclass(frozen=True)
class ContextTokenBudget:
    max_context_tokens: int = DEFAULT_MAX_CONTEXT_TOKENS
    reserved_output_tokens: int = 4_096
    reserved_input_tokens: int = 0
    safety_margin_tokens: int = DEFAULT_CONTEXT_SAFETY_MARGIN_TOKENS
    chars_per_token: float = DEFAULT_CHARS_PER_TOKEN

    def __post_init__(self) -> None:
        if self.max_context_tokens < 1:
            raise ValueError("max_context_tokens must be at least 1")
        if self.reserved_output_tokens < 0:
            raise ValueError("reserved_output_tokens cannot be negative")
        if self.reserved_input_tokens < 0:
            raise ValueError("reserved_input_tokens cannot be negative")
        if self.safety_margin_tokens < 0:
            raise ValueError("safety_margin_tokens cannot be negative")
        if self.chars_per_token <= 0:
            raise ValueError("chars_per_token must be greater than 0")
        if self.input_token_budget < 1:
            raise ValueError(
                "Context token budget leaves no room for messages; reduce "
                "max_tokens, reserved input, or safety margin"
            )

    @property
    def input_token_budget(self) -> int:
        return (
            self.max_context_tokens
            - self.reserved_output_tokens
            - self.reserved_input_tokens
            - self.safety_margin_tokens
        )


@dataclass
class SlidingWindowContextManager:
    max_recent_messages: int = 20
    preserve_initial_user_message: bool = True
    clear_old_tool_results: bool = True
    max_tool_result_chars: int | None = None
    _messages: list[MessageParam] = field(default_factory=list, init=False)

    def __post_init__(self) -> None:
        if self.max_recent_messages < 2:
            raise ValueError("max_recent_messages must be at least 2")
        if (
            self.max_tool_result_chars is not None
            and self.max_tool_result_chars < 1
        ):
            raise ValueError("max_tool_result_chars must be at least 1")

    async def initialize(self, user_prompt: str) -> None:
        self._messages = []
        await self.record_user_message(user_prompt)

    async def record_user_message(self, user_prompt: str) -> None:
        self._messages.append(MessageParam(role="user", content=user_prompt))

    def restore(self, snapshot: ContextSnapshot) -> None:
        version = snapshot.get("version")
        if version != 1:
            raise ValueError(f"Unsupported context snapshot version: {version}")

        messages = snapshot.get("messages")
        if not isinstance(messages, list):
            raise ValueError("Context snapshot messages must be a list")

        self._messages = [_message_from_snapshot(message) for message in messages]

    def snapshot(self) -> ContextSnapshot:
        return {
            "version": 1,
            "messages": [_message_to_snapshot(message) for message in self._messages],
        }

    async def messages_for_model(
        self,
        token_budget: ContextTokenBudget | None = None,
    ) -> list[MessageParam]:
        selected = self._selected_messages()
        latest_tool_result_index = _latest_tool_result_index(selected)
        messages = [
            _message_for_model(
                message,
                clear_tool_results=index != latest_tool_result_index
                and self.clear_old_tool_results,
                max_tool_result_chars=None
                if index == latest_tool_result_index
                else self.max_tool_result_chars,
            )
            for index, message in enumerate(selected)
        ]
        if token_budget is None:
            return messages

        return _fit_messages_to_token_budget(
            messages,
            token_budget,
            preserve_first_message=self.preserve_initial_user_message,
        )

    async def record_assistant_message(self, message: MessageParam) -> None:
        self._messages.append(_normalize_message(message))

    async def record_tool_results(
        self, tool_results: list[ToolResultBlockParam]
    ) -> None:
        if not tool_results:
            return

        self._messages.append(
            _normalize_message(MessageParam(role="user", content=tool_results))
        )

    def _selected_messages(self) -> list[MessageParam]:
        if len(self._messages) <= self.max_recent_messages:
            return _drop_incomplete_tool_exchanges(self._messages)

        start_index = len(self._messages) - self.max_recent_messages
        selected = list(self._messages[start_index:])

        if self.preserve_initial_user_message and start_index > 0:
            selected = [self._messages[0], *selected]

        return _drop_incomplete_tool_exchanges(selected)


def _drop_incomplete_tool_exchanges(
    messages: list[MessageParam],
) -> list[MessageParam]:
    selected: list[MessageParam] = []
    index = 0

    while index < len(messages):
        message = messages[index]
        tool_use_ids = _tool_use_ids(message)

        if tool_use_ids:
            next_message = messages[index + 1] if index + 1 < len(messages) else None
            if next_message is None or not _has_tool_results_for(
                next_message,
                tool_use_ids,
            ):
                text_only_message = _without_tool_use_blocks(message)
                if text_only_message is not None:
                    selected.append(text_only_message)
                index += 1
                continue

            selected.append(message)
            selected.append(next_message)
            index += 2
            continue

        if _message_has_block_type(message, "tool_result"):
            index += 1
            continue

        selected.append(message)
        index += 1

    return selected


def _has_tool_results_for(
    message: MessageParam,
    tool_use_ids: set[str],
) -> bool:
    return tool_use_ids.issubset(_tool_result_ids(message))


def _tool_use_ids(message: MessageParam) -> set[str]:
    return _block_ids(message, "tool_use", "id")


def _tool_result_ids(message: MessageParam) -> set[str]:
    return _block_ids(message, "tool_result", "tool_use_id")


def _block_ids(
    message: MessageParam,
    block_type: str,
    id_key: str,
) -> set[str]:
    content = message["content"]
    if isinstance(content, str):
        return set()

    ids: set[str] = set()
    for block in content:
        block_dict = _block_as_mapping(block)
        if block_dict.get("type") == block_type:
            block_id = block_dict.get(id_key)
            if isinstance(block_id, str):
                ids.add(block_id)

    return ids


def _without_tool_use_blocks(message: MessageParam) -> MessageParam | None:
    content = message["content"]
    if isinstance(content, str):
        return message

    blocks = [
        _copy_block(block)
        for block in content
        if _block_as_mapping(block).get("type") != "tool_use"
    ]
    if not blocks:
        return None

    return MessageParam(role=message["role"], content=blocks)


def _latest_tool_result_index(messages: list[MessageParam]) -> int | None:
    for index in range(len(messages) - 1, -1, -1):
        if _message_has_block_type(messages[index], "tool_result"):
            return index

    return None


def _fit_messages_to_token_budget(
    messages: list[MessageParam],
    token_budget: ContextTokenBudget,
    *,
    preserve_first_message: bool,
) -> list[MessageParam]:
    if _estimated_tokens(messages, token_budget) <= token_budget.input_token_budget:
        return messages

    groups = _message_groups(messages)
    prefix: list[list[MessageParam]] = []
    if preserve_first_message and groups:
        prefix = [groups.pop(0)]

    while len(groups) > 1:
        candidate = _flatten_message_groups([*prefix, *groups])
        if _estimated_tokens(candidate, token_budget) <= token_budget.input_token_budget:
            return candidate
        groups.pop(0)

    compacted = _flatten_message_groups([*prefix, *groups])
    compacted = _truncate_tool_results_to_budget(compacted, token_budget)
    if _estimated_tokens(compacted, token_budget) <= token_budget.input_token_budget:
        return compacted

    return _truncate_text_to_budget(compacted, token_budget)


def _message_groups(messages: list[MessageParam]) -> list[list[MessageParam]]:
    groups: list[list[MessageParam]] = []
    index = 0

    while index < len(messages):
        message = messages[index]
        tool_use_ids = _tool_use_ids(message)
        if tool_use_ids and index + 1 < len(messages):
            next_message = messages[index + 1]
            if _has_tool_results_for(next_message, tool_use_ids):
                groups.append([message, next_message])
                index += 2
                continue

        groups.append([message])
        index += 1

    return groups


def _flatten_message_groups(
    groups: list[list[MessageParam]],
) -> list[MessageParam]:
    return [message for group in groups for message in group]


def _truncate_tool_results_to_budget(
    messages: list[MessageParam],
    token_budget: ContextTokenBudget,
) -> list[MessageParam]:
    compacted = [_normalize_message(message) for message in messages]

    while _estimated_tokens(compacted, token_budget) > token_budget.input_token_budget:
        location = _largest_tool_result_location(compacted)
        if location is None:
            return compacted

        message_index, block_index, original = location
        excess_tokens = (
            _estimated_tokens(compacted, token_budget)
            - token_budget.input_token_budget
        )
        excess_chars = math.ceil(excess_tokens * token_budget.chars_per_token)
        target_chars = max(0, min(len(original) - 1, len(original) - excess_chars - 500))
        if target_chars >= len(original):
            target_chars = max(0, len(original) // 2)

        truncated = _truncated_payload(
            original,
            preview_chars=target_chars,
            reason="Tool result truncated to fit model context budget.",
        )
        if len(truncated) >= len(original):
            return compacted

        _set_block_content(compacted[message_index], block_index, truncated)

    return compacted


def _truncate_text_to_budget(
    messages: list[MessageParam],
    token_budget: ContextTokenBudget,
) -> list[MessageParam]:
    compacted = [_normalize_message(message) for message in messages]

    while _estimated_tokens(compacted, token_budget) > token_budget.input_token_budget:
        location = _largest_text_location(compacted)
        if location is None:
            return compacted

        message_index, block_index, original = location
        excess_tokens = (
            _estimated_tokens(compacted, token_budget)
            - token_budget.input_token_budget
        )
        excess_chars = math.ceil(excess_tokens * token_budget.chars_per_token)
        target_chars = max(0, min(len(original) - 1, len(original) - excess_chars - 500))
        if target_chars >= len(original):
            target_chars = max(0, len(original) // 2)

        truncated_text = _truncated_text(
            original,
            preview_chars=target_chars,
            reason="Message text truncated to fit model context budget.",
        )
        if len(truncated_text) >= len(original):
            return compacted

        if block_index is None:
            compacted[message_index] = MessageParam(
                role=compacted[message_index]["role"],
                content=truncated_text,
            )
        else:
            _set_block_content(compacted[message_index], block_index, truncated_text)

    return compacted


def _largest_tool_result_location(
    messages: list[MessageParam],
) -> tuple[int, int, str] | None:
    largest: tuple[int, int, str] | None = None

    for message_index, message in enumerate(messages):
        content = message["content"]
        if isinstance(content, str):
            continue
        for block_index, block in enumerate(content):
            block_dict = _block_as_mapping(block)
            if block_dict.get("type") != "tool_result":
                continue
            block_content = block_dict.get("content")
            if not isinstance(block_content, str):
                continue
            if largest is None or len(block_content) > len(largest[2]):
                largest = (message_index, block_index, block_content)

    return largest


def _largest_text_location(
    messages: list[MessageParam],
) -> tuple[int, int | None, str] | None:
    largest: tuple[int, int | None, str] | None = None

    for message_index, message in enumerate(messages):
        content = message["content"]
        if isinstance(content, str):
            if largest is None or len(content) > len(largest[2]):
                largest = (message_index, None, content)
            continue

        for block_index, block in enumerate(content):
            block_dict = _block_as_mapping(block)
            if block_dict.get("type") != "text":
                continue
            text = block_dict.get("text")
            if not isinstance(text, str):
                continue
            if largest is None or len(text) > len(largest[2]):
                largest = (message_index, block_index, text)

    return largest


def _set_block_content(
    message: MessageParam,
    block_index: int,
    content: str,
) -> None:
    blocks = message["content"]
    if isinstance(blocks, str):
        raise TypeError("Cannot set block content on a string message")

    block = _copy_block(blocks[block_index])
    if not isinstance(block, dict):
        raise TypeError("Cannot set content on a non-dict block")
    if block.get("type") == "text":
        block["text"] = content
    else:
        block["content"] = content
    blocks[block_index] = block


def _truncated_payload(
    content: str,
    *,
    preview_chars: int,
    reason: str,
) -> str:
    return json.dumps(
        {
            "truncated": True,
            "reason": reason,
            "original_chars": len(content),
            "preview": content[:preview_chars],
        }
    )


def _truncated_text(
    content: str,
    *,
    preview_chars: int,
    reason: str,
) -> str:
    return (
        f"[{reason} Original chars: {len(content)}.]\n\n"
        f"{content[:preview_chars]}"
    )


def estimate_token_count(
    value: Any,
    *,
    chars_per_token: float = DEFAULT_CHARS_PER_TOKEN,
) -> int:
    if chars_per_token <= 0:
        raise ValueError("chars_per_token must be greater than 0")
    return math.ceil(len(json.dumps(value, separators=(",", ":"))) / chars_per_token)


def _estimated_tokens(
    messages: list[MessageParam],
    token_budget: ContextTokenBudget,
) -> int:
    return estimate_token_count(
        [_message_to_snapshot(message) for message in messages],
        chars_per_token=token_budget.chars_per_token,
    )


def _message_for_model(
    message: MessageParam,
    *,
    clear_tool_results: bool,
    max_tool_result_chars: int | None,
) -> MessageParam:
    content = message["content"]
    if isinstance(content, str):
        return MessageParam(role=message["role"], content=content)

    blocks = [
        _block_for_model(
            block,
            clear_tool_results=clear_tool_results,
            max_tool_result_chars=max_tool_result_chars,
        )
        for block in content
    ]
    return MessageParam(role=message["role"], content=blocks)


def _block_for_model(
    block: Any,
    *,
    clear_tool_results: bool,
    max_tool_result_chars: int | None,
) -> Any:
    block_copy = _copy_block(block)
    if not isinstance(block_copy, dict):
        return block_copy

    if block_copy.get("type") != "tool_result":
        return block_copy

    if clear_tool_results:
        return _cleared_tool_result_block(block_copy)

    if max_tool_result_chars is None:
        return block_copy

    content = block_copy.get("content")
    if isinstance(content, str) and len(content) > max_tool_result_chars:
        block_copy["content"] = json.dumps(
            {
                "truncated": True,
                "original_chars": len(content),
                "preview": content[:max_tool_result_chars],
            }
        )

    return block_copy


def _cleared_tool_result_block(block: dict[str, Any]) -> dict[str, Any]:
    tool_use_id = block.get("tool_use_id")
    block["content"] = json.dumps(
        {
            "cleared": True,
            "reason": "Older tool result omitted from model context.",
            "tool_use_id": tool_use_id,
            "tool_result_ref": None
            if tool_use_id is None
            else f"tool_result:{tool_use_id}",
            "original_chars": _content_length(block.get("content")),
        }
    )
    return block


def _content_length(content: Any) -> int | None:
    if isinstance(content, str):
        return len(content)
    if isinstance(content, list):
        return len(json.dumps(content))
    return None


def _normalize_message(message: MessageParam) -> MessageParam:
    return _message_from_snapshot(_message_to_snapshot(message))


def _message_to_snapshot(message: MessageParam) -> dict[str, Any]:
    content = message["content"]
    if isinstance(content, str):
        snapshot_content: str | list[Any] = content
    else:
        snapshot_content = [_block_to_snapshot(block) for block in content]

    return {
        "role": message["role"],
        "content": snapshot_content,
    }


def _message_from_snapshot(message: Any) -> MessageParam:
    if not isinstance(message, dict):
        raise ValueError("Context snapshot message must be a dict")

    role = message.get("role")
    if role not in ("user", "assistant"):
        raise ValueError(f"Invalid context snapshot role: {role}")

    content = message.get("content")
    if not isinstance(content, str) and not isinstance(content, list):
        raise ValueError("Context snapshot content must be a string or list")

    return MessageParam(role=role, content=content)


def _block_to_snapshot(block: Any) -> dict[str, Any]:
    block_dict = _block_as_mapping(block)
    return {key: value for key, value in block_dict.items()}


def _copy_block(block: Any) -> Any:
    if isinstance(block, dict):
        return dict(cast(Mapping[str, Any], block))
    return block


def _message_has_block_type(message: MessageParam, block_type: str) -> bool:
    content = message["content"]
    if isinstance(content, str):
        return False

    for block in content:
        block_dict = _block_as_mapping(block)
        if block_dict.get("type") == block_type:
            return True

    return False


def _block_as_mapping(block: Any) -> Mapping[str, Any]:
    if isinstance(block, dict):
        return cast(Mapping[str, Any], block)
    if hasattr(block, "to_dict"):
        return cast(Mapping[str, Any], block.to_dict())
    return {}
