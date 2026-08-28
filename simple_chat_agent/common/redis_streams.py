from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import (
    ConnectionError as RedisConnectionError,
    ResponseError,
    TimeoutError as RedisTimeoutError,
)


REDIS_URL_ENV = "SIMPLE_CHAT_REDIS_URL"
REDIS_STREAM_PREFIX_ENV = "SIMPLE_CHAT_REDIS_STREAM_PREFIX"
REDIS_STREAM_TTL_SECONDS_ENV = "SIMPLE_CHAT_REDIS_STREAM_TTL_SECONDS"
DEFAULT_REDIS_STREAM_PREFIX = "simple-chat:streams"
DEFAULT_REDIS_STREAM_TTL_SECONDS = 1800
ZERO_STREAM_ID = "0-0"
REDIS_OPERATION_ATTEMPTS = 3
REDIS_RETRY_BASE_DELAY_SECONDS = 0.05

_STREAM_ID_PATTERN = re.compile(r"^(0|[1-9][0-9]*)-(0|[1-9][0-9]*)$")

_APPEND_EVENT_SCRIPT = """
local idempotency_key = ARGV[3]
if idempotency_key ~= '' then
  local existing = redis.call('HGET', KEYS[2], idempotency_key)
  if existing then
    redis.call('EXPIRE', KEYS[1], ARGV[4])
    redis.call('EXPIRE', KEYS[2], ARGV[4])
    return existing
  end
end

local cursor = redis.call(
  'XADD', KEYS[1], '*',
  'event', ARGV[1],
  'data', ARGV[2]
)
if idempotency_key ~= '' then
  redis.call('HSET', KEYS[2], idempotency_key, cursor)
end
redis.call('EXPIRE', KEYS[1], ARGV[4])
redis.call('EXPIRE', KEYS[2], ARGV[4])
return cursor
"""


@dataclass(frozen=True)
class RedisStreamEntry:
    id: str
    event: str
    data: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {"id": self.id, "event": self.event, "data": self.data}


@dataclass(frozen=True)
class RedisStreamAppend:
    stream_id: str
    event: str
    data: dict[str, Any]
    idempotency_key: str


class RedisStreamStore:
    def __init__(
        self,
        client: Redis,
        *,
        prefix: str = DEFAULT_REDIS_STREAM_PREFIX,
        ttl_seconds: int = DEFAULT_REDIS_STREAM_TTL_SECONDS,
    ) -> None:
        self._client = client
        self._prefix = prefix.strip().rstrip(":") or DEFAULT_REDIS_STREAM_PREFIX
        self._ttl_seconds = max(60, ttl_seconds)

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        prefix: str | None = None,
        ttl_seconds: int | None = None,
        socket_timeout: float = 5.0,
    ) -> "RedisStreamStore":
        return cls(
            Redis.from_url(
                url,
                decode_responses=True,
                health_check_interval=30,
                socket_connect_timeout=socket_timeout,
                socket_timeout=socket_timeout,
            ),
            prefix=prefix or redis_stream_prefix(),
            ttl_seconds=ttl_seconds or redis_stream_ttl_seconds(),
        )

    @classmethod
    def from_env(cls, *, socket_timeout: float = 5.0) -> "RedisStreamStore | None":
        url = redis_url()
        if not url:
            return None
        return cls.from_url(url, socket_timeout=socket_timeout)

    async def start(self) -> None:
        await self._execute(lambda: self._client.ping())

    async def close(self) -> None:
        await self._client.aclose()

    async def append_event(
        self,
        stream_id: str,
        event: str,
        data: dict[str, Any],
        *,
        idempotency_key: str | None = None,
    ) -> str:
        stream_key, idempotency_hash = self._keys(stream_id)
        cursor = await self._execute(
            lambda: self._client.eval(
                _APPEND_EVENT_SCRIPT,
                2,
                stream_key,
                idempotency_hash,
                event,
                json.dumps(data, default=str, separators=(",", ":")),
                idempotency_key or "",
                str(self._ttl_seconds),
            ),
            # A retry after a lost response is safe only when the Lua script can
            # recognize the event it may already have appended.
            attempts=REDIS_OPERATION_ATTEMPTS if idempotency_key else 1,
        )
        return str(cursor)

    async def append_events(
        self,
        events: list[RedisStreamAppend],
    ) -> list[str]:
        if not events:
            return []

        async def append_batch() -> list[str]:
            pipeline = self._client.pipeline(transaction=False)
            for item in events:
                stream_key, idempotency_hash = self._keys(item.stream_id)
                pipeline.eval(
                    _APPEND_EVENT_SCRIPT,
                    2,
                    stream_key,
                    idempotency_hash,
                    item.event,
                    json.dumps(item.data, default=str, separators=(",", ":")),
                    item.idempotency_key,
                    str(self._ttl_seconds),
                )
            return [str(cursor) for cursor in await pipeline.execute()]

        # Every batch item has a stable key, so retrying the pipeline after a
        # lost response cannot append a duplicate.
        return await self._execute(append_batch)

    async def clear(self, stream_id: str) -> None:
        await self._execute(lambda: self._client.delete(*self._keys(stream_id)))

    async def cursor(self, stream_id: str) -> str:
        stream_key, _ = self._keys(stream_id)
        try:
            info = await self._execute(lambda: self._client.xinfo_stream(stream_key))
        except ResponseError as error:
            if "no such key" in str(error).lower():
                return ZERO_STREAM_ID
            raise
        return str(info.get("last-generated-id") or ZERO_STREAM_ID)

    async def replay_entries(
        self,
        stream_id: str,
        *,
        cursor: str | None,
        limit: int,
    ) -> dict[str, Any]:
        normalized_cursor = cursor or ZERO_STREAM_ID
        availability = await self._replay_availability(stream_id, normalized_cursor)
        if not availability["replay_available"]:
            return {
                "entries": [],
                "cursor": availability["cursor"],
                "replay_available": False,
                "reason": availability["reason"],
            }

        stream_key, _ = self._keys(stream_id)
        if not availability["exists"]:
            return {
                "entries": [],
                "cursor": ZERO_STREAM_ID,
                "replay_available": True,
                "reason": "",
            }

        raw_entries = await self._execute(
            lambda: self._client.xrange(
                stream_key,
                min=f"({normalized_cursor}",
                max="+",
                count=limit,
            )
        )
        entries = [self._decode_entry(entry_id, fields) for entry_id, fields in raw_entries]
        return {
            "entries": [entry.to_dict() for entry in entries],
            "cursor": entries[-1].id if entries else normalized_cursor,
            "replay_available": True,
            "reason": "",
        }

    async def read_entries(
        self,
        stream_id: str,
        *,
        cursor: str,
        limit: int,
        block_milliseconds: int,
        validate_cursor: bool = True,
    ) -> dict[str, Any]:
        if validate_cursor:
            availability = await self._replay_availability(stream_id, cursor)
            if not availability["replay_available"]:
                return {
                    "entries": [],
                    "cursor": availability["cursor"],
                    "replay_available": False,
                    "reason": availability["reason"],
                }

        stream_key, _ = self._keys(stream_id)
        raw_streams = await self._execute(
            lambda: self._client.xread(
                {stream_key: cursor},
                count=limit,
                block=block_milliseconds,
            )
        )
        raw_entries = raw_streams[0][1] if raw_streams else []
        entries = [self._decode_entry(entry_id, fields) for entry_id, fields in raw_entries]
        return {
            "entries": [entry.to_dict() for entry in entries],
            "cursor": entries[-1].id if entries else cursor,
            "replay_available": True,
            "reason": "",
        }

    async def _replay_availability(
        self,
        stream_id: str,
        cursor: str,
    ) -> dict[str, Any]:
        if not valid_stream_id(cursor):
            return {
                "exists": False,
                "cursor": await self.cursor(stream_id),
                "replay_available": False,
                "reason": "stream cursor unavailable",
            }

        stream_key, _ = self._keys(stream_id)
        try:
            info = await self._execute(lambda: self._client.xinfo_stream(stream_key))
        except ResponseError as error:
            if "no such key" not in str(error).lower():
                raise
            return {
                "exists": False,
                "cursor": ZERO_STREAM_ID,
                "replay_available": cursor == ZERO_STREAM_ID,
                "reason": "" if cursor == ZERO_STREAM_ID else "stream unavailable",
            }

        last_id = str(info.get("last-generated-id") or ZERO_STREAM_ID)
        first_entry = info.get("first-entry")
        first_id = str(first_entry[0]) if first_entry else ZERO_STREAM_ID
        unavailable = compare_stream_ids(cursor, last_id) > 0 or (
            cursor != ZERO_STREAM_ID and compare_stream_ids(cursor, first_id) < 0
        )
        return {
            "exists": True,
            "cursor": last_id,
            "replay_available": not unavailable,
            "reason": "stream cursor unavailable" if unavailable else "",
        }

    async def _execute(
        self,
        operation: Callable[[], Awaitable[Any]],
        *,
        attempts: int = REDIS_OPERATION_ATTEMPTS,
    ) -> Any:
        for attempt in range(max(1, attempts)):
            try:
                return await operation()
            except (RedisConnectionError, RedisTimeoutError, OSError):
                # A Redis restart invalidates every idle connection in the pool,
                # not only the connection that happened to fail first.
                with suppress(Exception):
                    await self._client.connection_pool.disconnect()
                if attempt + 1 >= attempts:
                    raise
                await asyncio.sleep(REDIS_RETRY_BASE_DELAY_SECONDS * (2**attempt))

        raise AssertionError("Redis operation retry loop exited unexpectedly")

    def _keys(self, stream_id: str) -> tuple[str, str]:
        digest = hashlib.sha256(stream_id.encode("utf-8")).hexdigest()
        hash_tag = f"{{{digest}}}"
        return (
            f"{self._prefix}:{hash_tag}:events",
            f"{self._prefix}:{hash_tag}:idempotency",
        )

    @staticmethod
    def _decode_entry(entry_id: str, fields: dict[str, str]) -> RedisStreamEntry:
        try:
            data = json.loads(fields.get("data") or "{}")
        except json.JSONDecodeError:
            data = {}
        if not isinstance(data, dict):
            data = {"value": data}
        return RedisStreamEntry(
            id=str(entry_id),
            event=str(fields.get("event") or "stream"),
            data=data,
        )


def redis_url() -> str:
    return os.environ.get(REDIS_URL_ENV, "").strip()


def redis_stream_prefix() -> str:
    return os.environ.get(
        REDIS_STREAM_PREFIX_ENV,
        DEFAULT_REDIS_STREAM_PREFIX,
    ).strip()


def redis_stream_ttl_seconds() -> int:
    raw = os.environ.get(REDIS_STREAM_TTL_SECONDS_ENV, "").strip()
    if not raw:
        return DEFAULT_REDIS_STREAM_TTL_SECONDS
    try:
        return max(60, int(raw))
    except ValueError:
        return DEFAULT_REDIS_STREAM_TTL_SECONDS


def valid_stream_id(value: str) -> bool:
    return bool(_STREAM_ID_PATTERN.fullmatch(value))


def compare_stream_ids(left: str, right: str) -> int:
    left_parts = tuple(int(part) for part in left.split("-", 1))
    right_parts = tuple(int(part) for part in right.split("-", 1))
    return (left_parts > right_parts) - (left_parts < right_parts)
