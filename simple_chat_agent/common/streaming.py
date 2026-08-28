from __future__ import annotations

import asyncio
import json
import logging
import os
from contextlib import suppress
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
from redis.exceptions import RedisError

from agent_harness.streaming import StreamEvent, StreamSink
from simple_chat_agent.common.redis_streams import RedisStreamAppend, RedisStreamStore

STREAM_DIR = Path(".simple_chat_streams")
LOGGER = logging.getLogger(__name__)
REDIS_STREAM_BATCH_INTERVAL_SECONDS = 0.010
REDIS_STREAM_BATCH_SIZE = 256
REDIS_STREAM_QUEUE_SIZE = 4096

_configured_redis_store: RedisStreamStore | None = None


def stream_path(stream_id: str) -> Path:
    return STREAM_DIR / f"{stream_id}.jsonl"


class JsonlStreamSink:
    """Local-dev sink: append events to a per-stream JSONL file on disk."""

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


class HttpStreamSink:
    """Compatibility sink: POST events to the API-owned internal endpoint.

    Redis-backed deployments write directly to Redis. This remains available so
    old workers and externally hosted sandbox executors can continue to publish
    during a rolling migration.
    """

    def __init__(self, base_url: str, token: str, *, timeout: float = 2.0) -> None:
        self._url = f"{base_url.rstrip('/')}/internal/stream"
        self._token = token
        self._timeout = timeout

    async def emit(self, event: StreamEvent) -> None:
        if event.stream_id is None:
            return
        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                response = await client.post(
                    self._url,
                    json=asdict(event),
                    headers={"X-Stream-Token": self._token},
                )
                response.raise_for_status()
        except Exception as error:
            # Visibility-only; never fail the activity on a streaming hiccup.
            LOGGER.warning("HTTP stream event delivery failed: %s", error)


class RedisStreamSink:
    """Deployment sink: append worker events directly to Redis Streams."""

    def __init__(self, store: RedisStreamStore) -> None:
        self.store = store
        self._queue: asyncio.Queue[_QueuedStreamEvent] = asyncio.Queue(
            maxsize=REDIS_STREAM_QUEUE_SIZE
        )
        self._writer: asyncio.Task[None] | None = None
        self._flush_requested = asyncio.Event()
        self._immediate_delta_keys: set[tuple[str, str]] = set()

    async def start(self) -> None:
        await self.store.start()
        self._ensure_writer()

    async def emit(self, event: StreamEvent) -> None:
        if event.stream_id is None:
            return

        self._ensure_writer()
        immediate = self._should_flush_immediately(event)
        written = (
            asyncio.get_running_loop().create_future() if immediate else None
        )
        await self._queue.put(
            _QueuedStreamEvent(
                event=event,
                idempotency_key=uuid4().hex,
                written=written,
            )
        )
        if immediate:
            self._flush_requested.set()
            assert written is not None
            await written

    async def flush(self) -> None:
        if self._writer is None:
            return
        self._flush_requested.set()
        await self._queue.join()

    async def close(self) -> None:
        await self.flush()
        if self._writer is not None:
            self._writer.cancel()
            with suppress(asyncio.CancelledError):
                await self._writer
            self._writer = None
        await self.store.close()

    def _ensure_writer(self) -> None:
        if self._writer is not None and not self._writer.done():
            return
        self._writer = asyncio.create_task(
            self._write_batches(),
            name="redis-stream-writer",
        )

    def _should_flush_immediately(self, event: StreamEvent) -> bool:
        payload = event.payload if isinstance(event.payload, dict) else {}
        operation_id = str(payload.get("operation_id") or event.stream_id or "")
        if event.kind in {
            "agent_start",
            "agent_complete",
            "agent_cancelled",
            "agent_failed",
        } or event.kind.endswith(("_complete", "_cancelled", "_failed")):
            self._immediate_delta_keys.discard(
                (_delta_flush_key(operation_id, payload), "agent_text_delta")
            )
            self._immediate_delta_keys.discard(
                (_delta_flush_key(operation_id, payload), "agent_thinking_delta")
            )
            return True
        if event.kind not in {"agent_text_delta", "agent_thinking_delta"}:
            return False
        key = (_delta_flush_key(operation_id, payload), event.kind)
        if key in self._immediate_delta_keys:
            return False
        self._immediate_delta_keys.add(key)
        return True

    async def _write_batches(self) -> None:
        while True:
            first = await self._queue.get()
            queued = [first]
            if first.written is None:
                try:
                    await asyncio.wait_for(
                        self._flush_requested.wait(),
                        timeout=REDIS_STREAM_BATCH_INTERVAL_SECONDS,
                    )
                except TimeoutError:
                    pass
            self._flush_requested.clear()
            while len(queued) < REDIS_STREAM_BATCH_SIZE:
                try:
                    queued.append(self._queue.get_nowait())
                except asyncio.QueueEmpty:
                    break

            batched = _coalesce_queued_stream_events(queued)
            try:
                await self.store.append_events(
                    [
                        RedisStreamAppend(
                            stream_id=item.event.stream_id or "",
                            event="stream",
                            data=asdict(item.event),
                            idempotency_key=item.idempotency_key,
                        )
                        for item in batched
                    ]
                )
            except (RedisError, OSError, TimeoutError) as error:
                LOGGER.warning("Redis stream event delivery failed: %s", error)
                for item in queued:
                    if item.written is not None and not item.written.done():
                        item.written.set_exception(error)
            else:
                for item in queued:
                    if item.written is not None and not item.written.done():
                        item.written.set_result(None)
            finally:
                for _ in queued:
                    self._queue.task_done()


@dataclass
class _QueuedStreamEvent:
    event: StreamEvent
    idempotency_key: str
    written: asyncio.Future[None] | None = None


def _coalesce_queued_stream_events(
    queued: list[_QueuedStreamEvent],
) -> list[_QueuedStreamEvent]:
    coalesced: list[_QueuedStreamEvent] = []
    for item in queued:
        if coalesced:
            merged = _merge_delta_events(coalesced[-1].event, item.event)
            if merged is not None:
                coalesced[-1] = replace(coalesced[-1], event=merged)
                continue
        coalesced.append(item)
    return coalesced


def _merge_delta_events(left: StreamEvent, right: StreamEvent) -> StreamEvent | None:
    content_key = {
        "agent_text_delta": "text",
        "agent_thinking_delta": "thinking",
    }.get(left.kind)
    if content_key is None or right.kind != left.kind:
        return None
    if (
        left.stream_id != right.stream_id
        or left.tool_name != right.tool_name
        or left.step != right.step
        or left.agent != right.agent
        or left.tool_call_id != right.tool_call_id
        or not isinstance(left.payload, dict)
        or not isinstance(right.payload, dict)
    ):
        return None
    left_identity = {key: value for key, value in left.payload.items() if key != content_key}
    right_identity = {
        key: value for key, value in right.payload.items() if key != content_key
    }
    if left_identity != right_identity:
        return None
    left_content = left.payload.get(content_key)
    right_content = right.payload.get(content_key)
    if not isinstance(left_content, str) or not isinstance(right_content, str):
        return None
    return replace(
        left,
        payload={**left.payload, content_key: left_content + right_content},
    )


def _delta_flush_key(operation_id: str, payload: dict[str, Any]) -> str:
    attempt = payload.get("attempt") or payload.get("activity_attempt") or ""
    return f"{operation_id}:{attempt}"


def configured_stream_sink() -> StreamSink:
    """Select the stream sink from the environment.

    Redis is the deployment backplane. The HTTP sink is retained for rolling
    compatibility, and local development falls back to per-stream JSONL files.
    """
    global _configured_redis_store
    _configured_redis_store = None
    redis_store = RedisStreamStore.from_env(socket_timeout=2.0)
    if redis_store is not None:
        _configured_redis_store = redis_store
        return RedisStreamSink(redis_store)

    base_url = os.environ.get("SIMPLE_CHAT_STREAM_SINK_URL", "").strip()
    if base_url:
        return HttpStreamSink(base_url, os.environ.get("SIMPLE_CHAT_STREAM_TOKEN", ""))
    return JsonlStreamSink()


def stream_event_envelope(
    event: str,
    data: dict[str, Any],
    *,
    idempotency_key: str | None = None,
) -> dict[str, Any]:
    return {
        "event": event,
        "data": data,
        "idempotency_key": idempotency_key or "",
    }


def append_local_stream_event(
    stream_id: str,
    event: str,
    data: dict[str, Any],
    *,
    idempotency_key: str | None = None,
) -> None:
    STREAM_DIR.mkdir(parents=True, exist_ok=True)
    path = stream_path(stream_id)
    if idempotency_key and path.exists():
        try:
            with path.open("r", encoding="utf-8") as stream:
                for line in stream:
                    try:
                        payload = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if payload.get("idempotency_key") == idempotency_key:
                        return
        except OSError:
            pass

    with path.open("a", encoding="utf-8", buffering=1) as stream:
        stream.write(
            json.dumps(
                stream_event_envelope(
                    event,
                    data,
                    idempotency_key=idempotency_key,
                ),
                default=str,
            )
        )
        stream.write("\n")
        stream.flush()


async def emit_durable_stream_event(
    stream_id: str,
    event: str,
    data: dict[str, Any],
    *,
    idempotency_key: str,
    timeout: float = 10.0,
) -> None:
    redis_store = _configured_redis_store
    close_store = False
    if redis_store is None:
        redis_store = RedisStreamStore.from_env(socket_timeout=timeout)
        close_store = redis_store is not None
    if redis_store is not None:
        try:
            await redis_store.append_event(
                stream_id,
                event,
                data,
                idempotency_key=idempotency_key,
            )
        finally:
            if close_store:
                await redis_store.close()
        return

    base_url = os.environ.get("SIMPLE_CHAT_STREAM_SINK_URL", "").strip()
    if not base_url:
        append_local_stream_event(
            stream_id,
            event,
            data,
            idempotency_key=idempotency_key,
        )
        return

    token = os.environ.get("SIMPLE_CHAT_STREAM_TOKEN", "")
    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.post(
            f"{base_url.rstrip('/')}/internal/stream/event",
            json={
                "stream_id": stream_id,
                "event": event,
                "data": data,
                "idempotency_key": idempotency_key,
            },
            headers={"X-Stream-Token": token},
        )
        response.raise_for_status()
