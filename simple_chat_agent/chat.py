from __future__ import annotations

import asyncio
import contextlib
import json
import os
import shutil
import textwrap
from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4

from prompt_toolkit.application import Application
from prompt_toolkit.buffer import Buffer
from prompt_toolkit.formatted_text import StyleAndTextTuples
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.layout import HSplit, Layout, VSplit, Window
from prompt_toolkit.layout.controls import BufferControl, FormattedTextControl
from prompt_toolkit.layout.dimension import Dimension
from prompt_toolkit.styles import Style
from temporalio.client import Client

from simple_chat_agent import TASK_QUEUE
from simple_chat_agent.env import load_dotenv
from simple_chat_agent.external_storage import simple_chat_data_converter
from simple_chat_agent.streaming import stream_path
from simple_chat_agent.workflow import (
    ChatMessage,
    SimpleChatInput,
    SimpleChatState,
    SimpleChatWorkflow,
)


STYLE = Style.from_dict(
    {
        "title": "bold fg:#7aa2f7",
        "status": "fg:#9aa0a6",
        "help": "fg:#7dcfff",
        "user-bubble": "fg:#e5d7ff bg:#241b33",
        "assistant-bubble": "fg:#d7e4ff bg:#172330",
        "system-bubble": "fg:#f0c674 bg:#2b2416",
        "system": "fg:#e0af68",
        "pending-bubble": "fg:#b8a8d9 bg:#211a2d italic",
        "draft-bubble": "fg:#9aa7b8 bg:#172330 italic",
        "stream": "fg:#6e7681 italic",
        "prompt": "bold fg:#7aa2f7",
        "input": "fg:#ffffff",
    }
)

STATE_POLL_INTERVAL_SECONDS = 0.1
STREAM_POLL_INTERVAL_SECONDS = 0.01


@dataclass
class PendingSubmission:
    label: str
    content: str
    start_index: int


@dataclass
class ChatUi:
    workflow_id: str
    handle: Any
    status: str = "starting"
    pending_messages: int = 0
    active_message_index: int | None = None
    queued_message_indices: set[int] = field(default_factory=set)
    transcript: list[ChatMessage] = field(default_factory=list)
    pending_submissions: list[PendingSubmission] = field(default_factory=list)
    active_stream_text: str = ""
    stream_events: list[str] = field(default_factory=list)
    notices: list[str] = field(default_factory=list)
    input_buffer: Buffer = field(default_factory=lambda: Buffer(multiline=False))
    app: Application[Any] | None = None

    def build(self) -> Application[Any]:
        key_bindings = KeyBindings()
        body_control = FormattedTextControl(
            self.render_body,
            show_cursor=False,
        )

        @key_bindings.add("enter")
        def _submit(event: Any) -> None:
            text = self.input_buffer.text
            self.input_buffer.reset()
            asyncio.create_task(self.submit(text))

        @key_bindings.add("c-c")
        @key_bindings.add("c-d")
        def _exit(event: Any) -> None:
            event.app.exit()

        body = Window(
            content=body_control,
            wrap_lines=False,
            always_hide_cursor=True,
            height=Dimension(weight=1),
        )
        status = Window(
            content=FormattedTextControl(self.render_status),
            height=1,
            always_hide_cursor=True,
        )
        input_control = BufferControl(buffer=self.input_buffer)
        input_row = VSplit(
            [
                Window(
                    content=FormattedTextControl([("class:prompt", "> ")]),
                    width=2,
                    always_hide_cursor=True,
                ),
                Window(content=input_control, height=1, style="class:input"),
            ],
            height=1,
        )

        root = HSplit([body, status, input_row])
        self.app = Application(
            layout=Layout(root, focused_element=input_control),
            key_bindings=key_bindings,
            full_screen=True,
            mouse_support=True,
            style=STYLE,
            min_redraw_interval=0,
            max_render_postpone_time=0,
        )
        return self.app

    async def submit(self, user_message: str) -> None:
        stripped = user_message.strip()
        if not stripped:
            return

        try:
            keep_running = await _handle_input(self.handle, self, stripped)
        except Exception as err:
            self.add_notice(f"{type(err).__name__}: {err}")
            keep_running = True

        if not keep_running and self.app is not None:
            self.app.exit()

    def update_state(self, state: SimpleChatState) -> None:
        had_final_assistant = _has_new_assistant_message(
            self.transcript, state.transcript
        ) or (
            bool(self.active_stream_text)
            and state.transcript != self.transcript
            and bool(state.transcript)
            and state.transcript[-1].role == "assistant"
        )
        self.status = state.status
        self.pending_messages = state.pending_messages
        self.active_message_index = state.active_message_index
        self.queued_message_indices = set(state.queued_message_indices)
        self.transcript = list(state.transcript)
        self._reconcile_pending_submissions()

        if had_final_assistant:
            self.active_stream_text = ""

        self.invalidate()

    def add_stream_event(self, event: dict[str, Any]) -> None:
        kind = event.get("kind")
        payload = event.get("payload")

        if kind == "claude_start":
            self.active_stream_text = ""
        elif kind == "claude_text_delta" and isinstance(payload, dict):
            text = payload.get("text")
            if isinstance(text, str):
                self.active_stream_text += text
        elif kind == "claude_complete":
            pass
        else:
            self.stream_events.append(_format_tool_stream_event(event))
            self.stream_events = self.stream_events[-6:]

        self.invalidate()

    def add_notice(self, message: str) -> None:
        self.notices.append(message)
        self.notices = self.notices[-5:]
        self.invalidate()

    def add_pending_submission(self, label: str, content: str) -> PendingSubmission:
        pending = PendingSubmission(
            label=label,
            content=content,
            start_index=len(self.transcript),
        )
        self.pending_submissions.append(pending)
        self.invalidate()
        return pending

    def remove_pending_submission(self, pending: PendingSubmission) -> None:
        with contextlib.suppress(ValueError):
            self.pending_submissions.remove(pending)
        self.invalidate()

    def render_body(self) -> StyleAndTextTuples:
        header_chunks: StyleAndTextTuples = [
            ("class:title", f"Simple Chat Agent  {self.workflow_id}\n"),
            ("class:help", f"{_compact_help()}\n\n"),
        ]
        content_chunks: StyleAndTextTuples = []

        for notice in self.notices:
            content_chunks.append(("class:system", f"[local] {notice}\n"))

        if self.stream_events:
            for event in self.stream_events:
                content_chunks.append(("class:stream", f"{event}\n"))
            content_chunks.append(("", "\n"))

        start_index = max(0, len(self.transcript) - 30)
        for index, message in enumerate(
            self.transcript[start_index:],
            start=start_index,
        ):
            content_chunks.extend(
                _message_chunks(
                    message,
                    is_active=index == self.active_message_index,
                    is_queued=index in self.queued_message_indices,
                )
            )

        for pending in self.pending_submissions:
            content_chunks.extend(_pending_chunks(pending))

        if self.active_stream_text:
            content_chunks.extend(
                _bubble_chunks(
                    label="assistant",
                    content=self.active_stream_text,
                    style="class:draft-bubble",
                    indent=0,
                )
            )

        return _visible_tail_chunks(header_chunks, content_chunks)

    def render_status(self) -> StyleAndTextTuples:
        queued = (
            f" | queued messages: {self.pending_messages}"
            if self.pending_messages
            else ""
        )
        return [
            (
                "class:status",
                f"status: {self.status}{queued} | auto-following latest content",
            )
        ]

    def invalidate(self) -> None:
        if self.app is not None:
            self.app.invalidate()

    def _reconcile_pending_submissions(self) -> None:
        self.pending_submissions = [
            pending
            for pending in self.pending_submissions
            if not _pending_was_acknowledged(pending, self.transcript)
        ]


async def main() -> None:
    load_dotenv()
    client = await Client.connect(
        "localhost:7233",
        data_converter=simple_chat_data_converter(),
    )

    workflow_id = f"simple-chat-{uuid4()}"
    stream_path(workflow_id).unlink(missing_ok=True)
    handle = await client.start_workflow(
        SimpleChatWorkflow.run,
        SimpleChatInput(
            model=os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-5"),
            stream_id=workflow_id,
        ),
        id=workflow_id,
        task_queue=TASK_QUEUE,
    )

    ui = ChatUi(workflow_id=workflow_id, handle=handle)
    app = ui.build()
    stop = asyncio.Event()
    monitor_task = asyncio.create_task(_monitor_state(handle, ui, stop))
    stream_task = asyncio.create_task(_monitor_stream(workflow_id, ui, stop))

    try:
        await app.run_async()
    finally:
        stop.set()
        for task in (monitor_task, stream_task):
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task


async def _handle_input(handle: Any, ui: ChatUi, user_message: str) -> bool:
    command, _, rest = user_message.partition(" ")
    command = command.lower()
    rest = rest.strip()

    if command in {"/quit", "/exit"}:
        return False
    if command == "/help":
        ui.add_notice(_help_text())
        return True
    if command == "/status":
        ui.add_notice(_format_status(ui.status, ui.pending_messages))
        return True
    if command == "/queue":
        await _signal_with_required_text(
            ui,
            rest,
            lambda text: handle.signal(SimpleChatWorkflow.chat, text),
            "/queue <message>",
            pending_label="you",
        )
        return True
    if command == "/steer":
        await _signal_with_required_text(
            ui,
            rest,
            lambda text: handle.signal(
                SimpleChatWorkflow.steer,
                args=[text, "immediate"],
            ),
            "/steer <message>",
            pending_label="you (steering)",
        )
        return True
    if command == "/after-tool":
        await _signal_with_required_text(
            ui,
            rest,
            lambda text: handle.signal(
                SimpleChatWorkflow.steer,
                args=[text, "after_next_tool_result"],
            ),
            "/after-tool <message>",
            pending_label="you (after tool)",
        )
        return True
    if command == "/interrupt":
        await _signal_with_required_text(
            ui,
            rest,
            lambda text: handle.signal(SimpleChatWorkflow.interrupt, text),
            "/interrupt <message>",
            pending_label="you (interrupt)",
        )
        return True

    if ui.status == "responding":
        await _send_with_pending(
            ui,
            label="you (steering)",
            content=user_message,
            send=lambda: handle.signal(
                SimpleChatWorkflow.steer,
                args=[user_message, "immediate"],
            ),
        )
    else:
        await _send_with_pending(
            ui,
            label="you",
            content=user_message,
            send=lambda: handle.signal(SimpleChatWorkflow.chat, user_message),
        )
    return True


async def _signal_with_required_text(
    ui: ChatUi,
    text: str,
    send: Any,
    usage: str,
    *,
    pending_label: str,
) -> None:
    if not text:
        ui.add_notice(f"Usage: {usage}")
        return

    await _send_with_pending(
        ui,
        label=pending_label,
        content=text,
        send=lambda: send(text),
    )


async def _send_with_pending(
    ui: ChatUi,
    *,
    label: str,
    content: str,
    send: Any,
) -> None:
    pending = ui.add_pending_submission(label, content)
    try:
        await send()
    except Exception:
        ui.remove_pending_submission(pending)
        raise


async def _monitor_state(handle: Any, ui: ChatUi, stop: asyncio.Event) -> None:
    while not stop.is_set():
        try:
            state = await handle.query(SimpleChatWorkflow.state)
        except Exception as err:
            ui.add_notice(f"query failed: {type(err).__name__}: {err}")
            await asyncio.sleep(1)
            continue

        ui.update_state(state)
        await asyncio.sleep(STATE_POLL_INTERVAL_SECONDS)


async def _monitor_stream(stream_id: str, ui: ChatUi, stop: asyncio.Event) -> None:
    path = stream_path(stream_id)
    offset = 0

    while not stop.is_set():
        if path.exists():
            with path.open("r", encoding="utf-8") as stream:
                stream.seek(offset)
                lines = stream.readlines()
                offset = stream.tell()

            for line in lines:
                with contextlib.suppress(json.JSONDecodeError):
                    ui.add_stream_event(json.loads(line))

        await asyncio.sleep(STREAM_POLL_INTERVAL_SECONDS)


def _has_new_assistant_message(
    previous: list[ChatMessage], current: list[ChatMessage]
) -> bool:
    return any(message.role == "assistant" for message in current[len(previous) :])


def _message_chunks(
    message: ChatMessage,
    *,
    is_active: bool = False,
    is_queued: bool = False,
) -> StyleAndTextTuples:
    if message.role == "user":
        if is_active:
            return _bubble_chunks(
                label="you -> agent",
                content=f"{message.content}  (delivered)",
                style="class:pending-bubble",
                indent=4,
            )
        if is_queued:
            return _bubble_chunks(
                label="you",
                content=f"{message.content}  (queued)",
                style="class:pending-bubble",
                indent=4,
            )
        return _bubble_chunks(
            label="you",
            content=message.content,
            style="class:user-bubble",
            indent=4,
        )
    if message.role == "assistant":
        return _bubble_chunks(
            label="assistant",
            content=message.content,
            style="class:assistant-bubble",
            indent=0,
        )
    return _bubble_chunks(
        label="system",
        content=message.content,
        style="class:system-bubble",
        indent=1,
    )


def _pending_chunks(pending: PendingSubmission) -> StyleAndTextTuples:
    return _bubble_chunks(
        label=pending.label,
        content=f"{pending.content}  (sending)",
        style="class:pending-bubble",
        indent=4,
    )


def _bubble_chunks(
    *,
    label: str,
    content: str,
    style: str,
    indent: int,
) -> StyleAndTextTuples:
    terminal_width = shutil.get_terminal_size((100, 24)).columns
    bubble_width = min(110, max(44, terminal_width - indent - 8))
    prefix = f"{label}: "
    content_width = max(20, bubble_width - len(prefix) - 2)
    lines = _wrapped_lines(content, content_width)
    chunks: StyleAndTextTuples = []

    for index, line in enumerate(lines):
        line_prefix = prefix if index == 0 else " " * len(prefix)
        body = f" {line_prefix}{line.ljust(content_width)} "
        chunks.append(("", " " * indent))
        chunks.append((style, body))
        chunks.append(("", "\n"))

    chunks.append(("", "\n"))
    return chunks


def _wrapped_lines(content: str, width: int) -> list[str]:
    lines: list[str] = []
    paragraphs = content.splitlines() or [""]

    for paragraph in paragraphs:
        if paragraph == "":
            lines.append("")
            continue
        lines.extend(
            textwrap.wrap(
                paragraph,
                width=width,
                replace_whitespace=False,
                drop_whitespace=True,
                break_long_words=True,
                break_on_hyphens=False,
            )
            or [""]
        )

    return lines


def _pending_was_acknowledged(
    pending: PendingSubmission, transcript: list[ChatMessage]
) -> bool:
    for message in transcript[pending.start_index :]:
        if message.role == "user" and message.content == pending.content:
            return True
        if message.role == "system" and pending.content in message.content:
            return True

    return False


def _visible_tail_chunks(
    header_chunks: StyleAndTextTuples,
    content_chunks: StyleAndTextTuples,
) -> StyleAndTextTuples:
    terminal_height = shutil.get_terminal_size((100, 24)).lines
    body_height = max(6, terminal_height - 2)
    header_lines = _split_styled_lines(header_chunks)
    content_lines = _split_styled_lines(content_chunks)

    if len(header_lines) + len(content_lines) <= body_height:
        return _join_styled_lines([*header_lines, *content_lines])

    omitted = [
        (
            "class:stream",
            "... older content omitted; showing latest messages ...",
        )
    ]
    tail_height = max(1, body_height - len(header_lines) - 1)
    return _join_styled_lines([*header_lines, omitted, *content_lines[-tail_height:]])


def _split_styled_lines(chunks: StyleAndTextTuples) -> list[StyleAndTextTuples]:
    lines: list[StyleAndTextTuples] = [[]]

    for style, text in chunks:
        parts = text.split("\n")
        for index, part in enumerate(parts):
            if part:
                lines[-1].append((style, part))
            if index < len(parts) - 1:
                lines.append([])

    while len(lines) > 1 and not lines[-1]:
        lines.pop()

    return lines


def _join_styled_lines(lines: list[StyleAndTextTuples]) -> StyleAndTextTuples:
    chunks: StyleAndTextTuples = []
    for line in lines:
        chunks.extend(line)
        chunks.append(("", "\n"))
    return chunks


def _format_tool_stream_event(event: dict[str, Any]) -> str:
    kind = event.get("kind")
    payload = event.get("payload")
    tool_name = event.get("tool_name") or "tool"
    step = event.get("step")
    label = tool_name if step is None else f"{tool_name}:{step}"
    return f"[stream {label}] {kind}: {_short_json(payload)}"


def _short_json(payload: Any, max_chars: int = 180) -> str:
    text = json.dumps(payload, default=str)
    if len(text) <= max_chars:
        return text
    return f"{text[:max_chars]}..."


def _format_status(status: str, pending_messages: int) -> str:
    suffix = (
        f", queued messages: {pending_messages}" if pending_messages else ""
    )
    return f"status: {status}{suffix}"


def _compact_help() -> str:
    return (
        "Type to chat; while Claude is responding, plain text becomes steering. "
        "/after-tool, /interrupt, /queue, /status, /help, /quit"
    )


def _help_text() -> str:
    return "\n".join(
        [
            "Type normally to chat. If Claude is responding, normal text sends steering.",
            "/steer <message>       Add steering before the next Claude call.",
            "/after-tool <message>  Add steering after the next tool result.",
            "/interrupt <message>   Cancel the in-flight Claude call and continue with context.",
            "/queue <message>       Queue a normal chat message even while busy.",
            "/status                Show workflow status.",
            "/quit                  Exit.",
        ]
    )


if __name__ == "__main__":
    asyncio.run(main())
