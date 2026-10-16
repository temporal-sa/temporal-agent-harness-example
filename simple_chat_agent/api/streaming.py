from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from contextlib import suppress
from typing import Any, AsyncIterator

from fastapi import Request
from redis.exceptions import RedisError

from simple_chat_agent.common.redis_streams import RedisStreamStore
from simple_chat_agent.common.streaming import (
    append_local_stream_event,
    stream_path,
)


STREAM_ACTIVE_POLL_INTERVAL_SECONDS = 0.02
STREAM_IDLE_POLL_INTERVAL_SECONDS = 0.5
STREAM_REDIS_BLOCK_MILLISECONDS = 1000
TURN_STREAM_REPLAY_LIMIT = 100
LOGGER = logging.getLogger(__name__)


class StreamBroker:
    def __init__(self, redis_store: RedisStreamStore | None = None) -> None:
        self._redis = redis_store or RedisStreamStore.from_env()

    @property
    def redis_enabled(self) -> bool:
        return self._redis is not None

    async def start(self) -> None:
        if self._redis is not None:
            await self._redis.start()

    async def close(self) -> None:
        if self._redis is not None:
            await self._redis.close()

    async def append(self, stream_id: str, event: dict[str, Any]) -> str:
        # Compatibility publishers send the complete StreamEvent envelope,
        # including its timestamp and sequence. Hashing that stable envelope
        # makes a retry safe if Redis accepted XADD but its response was lost.
        serialized = json.dumps(
            event,
            default=str,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return await self.append_event(
            stream_id,
            "stream",
            event,
            idempotency_key=f"stream:{hashlib.sha256(serialized).hexdigest()}",
        )

    async def append_event(
        self,
        stream_id: str,
        event: str,
        data: dict[str, Any],
        *,
        idempotency_key: str | None = None,
    ) -> str:
        if self._redis is not None:
            return await self._redis.append_event(
                stream_id,
                event,
                data,
                idempotency_key=idempotency_key,
            )

        append_local_stream_event(
            stream_id,
            event,
            data,
            idempotency_key=idempotency_key,
        )
        return await self.cursor(stream_id)

    async def clear(self, stream_id: str) -> None:
        if self._redis is not None:
            try:
                await self._redis.clear(stream_id)
            except RedisError as error:
                # Clearing a visibility-only stream must not fail chat creation
                # or deletion. A new chat has a unique workflow id, and durable
                # workflow state remains the reconciliation source of truth.
                LOGGER.warning(
                    "Redis stream cleanup failed for %s: %s",
                    stream_id,
                    error,
                )
            return
        stream_path(stream_id).unlink(missing_ok=True)

    async def cursor(self, stream_id: str) -> str:
        if self._redis is not None:
            return await self._redis.cursor(stream_id)
        path = stream_path(stream_id)
        return str(path.stat().st_size if path.exists() else 0)

    async def replay(
        self,
        stream_id: str,
        *,
        cursor: str | None = None,
        limit: int = 1000,
    ) -> dict[str, Any]:
        replay = await self._replay_entries(stream_id, cursor=cursor, limit=limit)
        entries = replay.pop("entries", [])
        return {
            **replay,
            "entry_count": len(entries),
            "has_more": len(entries) >= limit,
            "events": [
                entry["data"] for entry in entries if entry["event"] == "stream"
            ],
        }

    async def turn_event_stream(
        self,
        workflow_id: str,
        request: Request,
        *,
        cursor: str,
        after_revision: int = 0,
    ) -> AsyncIterator[dict[str, str]]:
        current_cursor = cursor
        sleep_seconds = STREAM_ACTIVE_POLL_INTERVAL_SECONDS
        validate_cursor = True
        while not await request.is_disconnected():
            if self._redis is not None:
                replay = await self._redis.read_entries(
                    workflow_id,
                    cursor=current_cursor,
                    limit=TURN_STREAM_REPLAY_LIMIT,
                    block_milliseconds=STREAM_REDIS_BLOCK_MILLISECONDS,
                    validate_cursor=validate_cursor,
                )
                validate_cursor = False
            else:
                replay = self._file_replay_entries(
                    workflow_id,
                    cursor=current_cursor,
                    limit=TURN_STREAM_REPLAY_LIMIT,
                )

            if not replay["replay_available"]:
                yield self._event(
                    "reconcile",
                    {
                        "workflow_id": workflow_id,
                        "reason": replay.get("reason")
                        or "stream replay unavailable",
                    },
                    event_id=replay.get("cursor") or current_cursor,
                )
                return

            entries = replay["entries"]
            for entry in entries:
                current_cursor = entry["id"]
                event = entry["event"]
                data = dict(entry["data"])
                if event == "turn_settled":
                    result = data.get("result")
                    to_revision = (
                        result.get("to_revision")
                        if isinstance(result, dict)
                        else None
                    )
                    if (
                        to_revision is not None
                        and int(to_revision) <= after_revision
                    ):
                        continue
                    data["cursor"] = current_cursor
                yield self._event(event, data, event_id=current_cursor)
                if event == "turn_settled":
                    return

            if self._redis is not None:
                continue
            if entries:
                sleep_seconds = STREAM_ACTIVE_POLL_INTERVAL_SECONDS
            else:
                current_cursor = replay.get("cursor") or current_cursor
                sleep_seconds = STREAM_IDLE_POLL_INTERVAL_SECONDS
            await asyncio.sleep(sleep_seconds)

    async def event_stream(
        self,
        workflow_id: str,
        request: Request,
    ) -> AsyncIterator[dict[str, str]]:
        if self._redis is not None:
            async for chunk in self._redis_event_stream(workflow_id, request):
                yield chunk
            return
        async for chunk in self._file_event_stream(workflow_id, request):
            yield chunk

    @staticmethod
    def event(
        event: str,
        data: dict[str, Any],
        *,
        event_id: str | None = None,
    ) -> dict[str, str]:
        value = {
            "event": event,
            "data": json.dumps(data, default=str),
        }
        if event_id is not None:
            value["id"] = event_id
        return value

    _event = event

    async def _replay_entries(
        self,
        workflow_id: str,
        *,
        cursor: str | None,
        limit: int,
    ) -> dict[str, Any]:
        if self._redis is not None:
            return await self._redis.replay_entries(
                workflow_id,
                cursor=cursor,
                limit=limit,
            )
        return self._file_replay_entries(workflow_id, cursor=cursor, limit=limit)

    def _file_replay_entries(
        self,
        workflow_id: str,
        *,
        cursor: str | None,
        limit: int,
    ) -> dict[str, Any]:
        path = stream_path(workflow_id)
        offset = 0
        if cursor:
            with suppress(ValueError):
                offset = max(0, int(cursor))
        if not path.exists():
            if offset == 0:
                return {
                    "entries": [],
                    "cursor": "0",
                    "replay_available": True,
                    "reason": "",
                }
            return {
                "entries": [],
                "cursor": "0",
                "replay_available": False,
                "reason": "stream file unavailable",
            }
        if offset > path.stat().st_size:
            offset = 0

        entries: list[dict[str, Any]] = []
        position = offset
        with path.open("r", encoding="utf-8") as stream:
            stream.seek(offset)
            while len(entries) < limit:
                line = stream.readline()
                if not line:
                    break
                position = stream.tell()
                with suppress(json.JSONDecodeError):
                    entries.append(
                        {
                            "id": str(position),
                            **self._entry_from_json_line(json.loads(line)),
                        }
                    )

        return {
            "entries": entries,
            "cursor": str(position),
            "replay_available": True,
            "reason": "",
        }

    async def _redis_event_stream(
        self,
        workflow_id: str,
        request: Request,
    ) -> AsyncIterator[dict[str, str]]:
        assert self._redis is not None
        last_event_id = request.headers.get("last-event-id") or request.query_params.get(
            "cursor"
        )
        if not last_event_id:
            cursor = await self._redis.cursor(workflow_id)
            yield self._event(
                "reconcile",
                {
                    "workflow_id": workflow_id,
                    "reason": "stream cursor unavailable",
                },
                event_id=cursor,
            )
            return

        current_cursor = last_event_id
        validate_cursor = True
        while not await request.is_disconnected():
            replay = await self._redis.read_entries(
                workflow_id,
                cursor=current_cursor,
                limit=TURN_STREAM_REPLAY_LIMIT,
                block_milliseconds=STREAM_REDIS_BLOCK_MILLISECONDS,
                validate_cursor=validate_cursor,
            )
            validate_cursor = False
            if not replay["replay_available"]:
                yield self._event(
                    "reconcile",
                    {
                        "workflow_id": workflow_id,
                        "reason": replay.get("reason") or "stream cursor unavailable",
                    },
                    event_id=replay.get("cursor") or current_cursor,
                )
                return

            for entry in replay["entries"]:
                current_cursor = entry["id"]
                data = dict(entry["data"])
                if entry["event"] == "turn_settled":
                    data["cursor"] = current_cursor
                yield self._event(
                    entry["event"],
                    data,
                    event_id=current_cursor,
                )

    async def _file_event_stream(
        self,
        workflow_id: str,
        request: Request,
    ) -> AsyncIterator[dict[str, str]]:
        path = stream_path(workflow_id)
        offset = 0
        needs_reconcile = False
        last_event_id = request.headers.get("last-event-id") or request.query_params.get(
            "cursor"
        )
        if last_event_id:
            try:
                offset = max(0, int(last_event_id))
            except ValueError:
                needs_reconcile = True
                offset = path.stat().st_size if path.exists() else 0
            if not path.exists() or offset > path.stat().st_size:
                needs_reconcile = True
                offset = path.stat().st_size if path.exists() else 0
        else:
            offset = path.stat().st_size if path.exists() else 0
            needs_reconcile = True

        if needs_reconcile:
            yield self._event(
                "reconcile",
                {
                    "workflow_id": workflow_id,
                    "reason": "stream cursor unavailable",
                },
                event_id=str(offset),
            )
            return

        sleep_seconds = STREAM_ACTIVE_POLL_INTERVAL_SECONDS
        while not await request.is_disconnected():
            emitted = False
            if path.exists():
                if offset > path.stat().st_size:
                    offset = path.stat().st_size
                    yield self._event(
                        "reconcile",
                        {
                            "workflow_id": workflow_id,
                            "reason": "stream cursor reset",
                        },
                        event_id=str(offset),
                    )
                    return

                new_lines: list[tuple[str, int]] = []
                with path.open("r", encoding="utf-8") as stream:
                    stream.seek(offset)
                    while True:
                        line = stream.readline()
                        if not line:
                            break
                        new_lines.append((line, stream.tell()))
                    offset = stream.tell()

                for line, position in new_lines:
                    with suppress(json.JSONDecodeError):
                        emitted = True
                        entry = self._entry_from_json_line(json.loads(line))
                        yield self._event(
                            entry["event"],
                            entry["data"],
                            event_id=str(position),
                        )

            sleep_seconds = (
                STREAM_ACTIVE_POLL_INTERVAL_SECONDS
                if emitted
                else STREAM_IDLE_POLL_INTERVAL_SECONDS
            )
            await asyncio.sleep(sleep_seconds)

    @staticmethod
    def _entry_from_json_line(value: dict[str, Any]) -> dict[str, Any]:
        if isinstance(value.get("event"), str) and isinstance(value.get("data"), dict):
            return {
                "event": value["event"],
                "data": value["data"],
            }
        return {
            "event": "stream",
            "data": value,
        }
