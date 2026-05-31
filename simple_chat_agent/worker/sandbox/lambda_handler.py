from __future__ import annotations

import asyncio
import json
from dataclasses import asdict
from typing import Any
from urllib.request import Request, urlopen

from claude_harness.streaming import StreamContext, StreamEvent, configure_stream_sink
from simple_chat_agent.worker.sandbox.runtime import (
    MAX_TIMEOUT_SECONDS,
    SANDBOX_DEADLINE_BUFFER_SECONDS,
    execute_python_sandbox,
)


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    configure_stream_sink(_stream_sink_from_event(event))
    timeout_seconds = _requested_timeout(event, context)
    stream_data = event.get("stream") if isinstance(event.get("stream"), dict) else {}
    stream = StreamContext(
        stream_id=stream_data.get("stream_id"),
        tool_name=stream_data.get("tool_name"),
        step=stream_data.get("step"),
    )
    return asyncio.run(
        execute_python_sandbox(
            str(event.get("code") or ""),
            timeout_seconds,
            stream=stream,
        )
    )


class _EventHttpStreamSink:
    def __init__(self, base_url: str, token: str, *, timeout: float = 2.0) -> None:
        self._url = f"{base_url.rstrip('/')}/internal/stream"
        self._token = token
        self._timeout = timeout

    def emit(self, event: StreamEvent) -> None:
        if event.stream_id is None:
            return
        body = json.dumps(asdict(event), default=str).encode("utf-8")
        request = Request(
            self._url,
            data=body,
            headers={
                "Content-Type": "application/json",
                "X-Stream-Token": self._token,
            },
            method="POST",
        )
        try:
            with urlopen(request, timeout=self._timeout):
                pass
        except Exception:
            # Visibility-only; sandbox execution result remains authoritative.
            pass


def _stream_sink_from_event(event: dict[str, Any]) -> _EventHttpStreamSink | None:
    sink = event.get("stream_sink")
    if not isinstance(sink, dict):
        return None
    url = sink.get("url")
    token = sink.get("token")
    if not isinstance(url, str) or not url.strip():
        return None
    if not isinstance(token, str) or not token.strip():
        return None
    return _EventHttpStreamSink(url, token)


def _requested_timeout(event: dict[str, Any], context: Any) -> int:
    timeout_seconds = int(event.get("timeout_seconds") or MAX_TIMEOUT_SECONDS)
    remaining_millis = getattr(context, "get_remaining_time_in_millis", None)
    if callable(remaining_millis):
        remaining_seconds = max(
            1,
            int(remaining_millis() / 1000) - SANDBOX_DEADLINE_BUFFER_SECONDS,
        )
        timeout_seconds = min(timeout_seconds, remaining_seconds)
    return max(1, min(timeout_seconds, MAX_TIMEOUT_SECONDS))
