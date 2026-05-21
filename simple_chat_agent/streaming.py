from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

from claude_harness.streaming import StreamEvent

STREAM_DIR = Path(".simple_chat_streams")


def stream_path(stream_id: str) -> Path:
    return STREAM_DIR / f"{stream_id}.jsonl"


class JsonlStreamSink:
    def emit(self, event: StreamEvent) -> None:
        if event.stream_id is None:
            return

        STREAM_DIR.mkdir(parents=True, exist_ok=True)
        with stream_path(event.stream_id).open(
            "a",
            encoding="utf-8",
            buffering=1,
        ) as stream:
            stream.write(json.dumps(asdict(event), default=str))
            stream.write("\n")
            stream.flush()
