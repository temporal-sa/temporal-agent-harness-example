"""Redis storage for agent sessions: frame log, status, ownership, forwarder leases.

Frame format v1 (CTO memo 001, CTO-2). One Redis stream per agent session:

    key   {prefix}:s:{session}:f
    id    "{resume_offset + 1}-{seq}"   # explicit, so a duplicate write is rejected
    data  v=1, e=<event type>, d=<harness SSE data JSON without resume_offset>

``resume_offset`` is the harness's root-stream cursor; several subagent frames can share one,
hence the per-offset ``seq``. Redis stream ids must be greater than 0-0, so the stored
millisecond part is ``resume_offset + 1``.

Status meta lives in a key next to it and is served to clients instead of a Temporal Query,
so ``status`` costs no Actions and still answers while every worker is down. Pending message
ids live in their own set (``{prefix}:s:{session}:p``), changed only by SADD/SREM, because two
writers touch them: the API adds an id when it sends, the bridge adds and removes ids as frames
arrive. Keeping them in the meta JSON let the bridge's copy overwrite a send made mid-forward.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any

import redis.asyncio as redis
from redis.exceptions import ResponseError

FRAME_VERSION = "1"


def prefix() -> str:
    env = os.environ.get("SIMPLE_CHAT_TASK_QUEUE", "local")
    return os.environ.get("AGENT_REDIS_PREFIX", f"aah:{env}")


def session_ttl_seconds() -> int:
    return int(os.environ.get("AGENT_SESSION_TTL_SECONDS", str(15 * 24 * 3600)))


@dataclass
class StoredFrame:
    event: str
    data: dict[str, Any]
    resume_offset: int
    entry_id: str


@dataclass
class SessionMeta:
    head: int = 0  # the next resume_offset the forwarder expects
    turn_active: bool = False
    current_turn: int = 0
    pending: set[str] = field(default_factory=set)  # accepted, not yet started message ids
    closed: bool = False

    def to_status(self) -> dict[str, Any]:
        return {
            "current_turn": self.current_turn,
            "turn_active": self.turn_active,
            "pending_turns": sorted(self.pending),
        }


class AgentStore:
    def __init__(self, client: redis.Redis) -> None:
        self.r = client

    @classmethod
    def from_env(cls) -> AgentStore:
        url = os.environ.get("SIMPLE_CHAT_REDIS_URL", "redis://127.0.0.1:6379/0")
        return cls(redis.from_url(url, decode_responses=True))

    # -- keys --------------------------------------------------------------------

    def _k(self, session: str, suffix: str) -> str:
        return f"{prefix()}:s:{{{session}}}:{suffix}"

    # -- ownership ---------------------------------------------------------------

    async def set_owner(self, session: str, owner: str, *, root: str | None = None) -> None:
        await self.r.set(self._k(session, "owner"), json.dumps({"owner": owner, "root": root or session}), ex=session_ttl_seconds())

    async def owner_of(self, session: str) -> str | None:
        """The session's owner. Reading it extends its life: an active session never
        outlives its ownership record."""
        key = self._k(session, "owner")
        pipe = self.r.pipeline(transaction=True)
        pipe.get(key)
        pipe.expire(key, session_ttl_seconds())
        raw, _ = await pipe.execute()
        return json.loads(raw)["owner"] if raw else None

    # -- frames ------------------------------------------------------------------

    async def append(self, session: str, event: str, data: dict[str, Any], resume_offset: int, seq: int) -> bool:
        """Append one frame. Returns False if this exact position was already stored."""
        entry_id = f"{resume_offset + 1}-{seq}"
        try:
            await self.r.xadd(
                self._k(session, "f"),
                {"v": FRAME_VERSION, "e": event, "d": json.dumps(data, separators=(",", ":"))},
                id=entry_id,
            )
        except ResponseError as exc:
            if "equal or smaller" in str(exc):
                return False
            raise
        await self.r.expire(self._k(session, "f"), session_ttl_seconds())
        return True

    async def last_position(self, session: str) -> tuple[int, int] | None:
        """(resume_offset, seq) of the newest stored frame."""
        entries = await self.r.xrevrange(self._k(session, "f"), count=1)
        if not entries:
            return None
        ms, seq = entries[0][0].split("-")
        return int(ms) - 1, int(seq)

    async def read(self, session: str, *, after: str, count: int = 500) -> list[StoredFrame]:
        """Frames strictly after the stream id ``after`` ("0-0" for everything)."""
        entries = await self.r.xrange(self._k(session, "f"), min=f"({after}", count=count)
        return [_frame(entry_id, fields) for entry_id, fields in entries]

    async def read_at(self, session: str, resume_offset: int) -> list[StoredFrame]:
        """Every frame stored at one resume position."""
        ms = resume_offset + 1
        entries = await self.r.xrange(self._k(session, "f"), min=f"{ms}-0", max=f"{ms}-{2**64 - 1}")
        return [_frame(entry_id, fields) for entry_id, fields in entries]

    async def wait(self, session: str, *, after: str, block_ms: int) -> list[StoredFrame]:
        result = await self.r.xread({self._k(session, "f"): after}, block=block_ms, count=500)
        if not result:
            return []
        _key, entries = result[0]
        return [_frame(entry_id, fields) for entry_id, fields in entries]

    # -- meta --------------------------------------------------------------------

    async def meta(self, session: str) -> SessionMeta:
        pipe = self.r.pipeline(transaction=True)
        pipe.get(self._k(session, "m"))
        pipe.smembers(self._k(session, "p"))
        raw, pending = await pipe.execute()
        d = json.loads(raw) if raw else {}
        return SessionMeta(
            head=d.get("head", 0),
            turn_active=d.get("turn_active", False),
            current_turn=d.get("current_turn", 0),
            pending=set(pending or ()),
            closed=d.get("closed", False),
        )

    async def save_meta(
        self,
        session: str,
        meta: SessionMeta,
        *,
        pending_added: str | None = None,
        pending_removed: str | None = None,
    ) -> None:
        """Write the meta fields, and apply one pending-set change. ``meta.pending`` itself is
        never written back: the set is changed only by SADD/SREM, so a stale copy can't drop an
        id another writer added."""
        ttl = session_ttl_seconds()
        pipe = self.r.pipeline(transaction=True)
        pipe.set(
            self._k(session, "m"),
            json.dumps(
                {
                    "head": meta.head,
                    "turn_active": meta.turn_active,
                    "current_turn": meta.current_turn,
                    "closed": meta.closed,
                }
            ),
            ex=ttl,
        )
        if pending_added:
            pipe.sadd(self._k(session, "p"), pending_added)
            pipe.expire(self._k(session, "p"), ttl)
        if pending_removed:
            pipe.srem(self._k(session, "p"), pending_removed)
            # Remember it finished waiting, so a client retry of the same request id (which the
            # workflow deduplicates) doesn't put it back in the pending set.
            pipe.sadd(self._k(session, "done"), pending_removed)
            pipe.expire(self._k(session, "done"), ttl)
        await pipe.execute()

    async def note_sent(self, session: str, message_id: str) -> None:
        """A message went out through the front door; it is pending until its turn starts,
        or until the agent rejects it. A retry of a message that already left the pending
        set is not re-added."""
        await self.r.eval(
            _ADD_PENDING_UNLESS_DONE,
            2,
            self._k(session, "p"),
            self._k(session, "done"),
            message_id,
            session_ttl_seconds(),
        )

    async def clear_pending(self, session: str) -> None:
        """The session ended: nothing it held will ever start."""
        await self.r.delete(self._k(session, "p"))

    # -- forwarder lease ---------------------------------------------------------

    async def acquire_lease(self, session: str, holder: str, ttl_ms: int = 15_000) -> bool:
        """Take the lease, or extend it if ``holder`` already has it (one atomic step)."""
        return bool(await self.r.eval(_ACQUIRE_OR_RENEW, 1, self._k(session, "lease"), holder, ttl_ms))

    async def renew_lease(self, session: str, holder: str, ttl_ms: int = 15_000) -> bool:
        """Extend the lease only if ``holder`` still owns it (atomic compare-and-set)."""
        return bool(await self.r.eval(_RENEW_IF_HOLDER, 1, self._k(session, "lease"), holder, ttl_ms))

    async def release_lease(self, session: str, holder: str) -> None:
        """Delete the lease only if ``holder`` still owns it (atomic compare-and-delete)."""
        await self.r.eval(_DELETE_IF_HOLDER, 1, self._k(session, "lease"), holder)


_ADD_PENDING_UNLESS_DONE = """
if redis.call('SISMEMBER', KEYS[2], ARGV[1]) == 0 then
  redis.call('SADD', KEYS[1], ARGV[1])
  redis.call('EXPIRE', KEYS[1], ARGV[2])
end
return 0
"""

_ACQUIRE_OR_RENEW = """
local current = redis.call('GET', KEYS[1])
if current == false or current == ARGV[1] then
  redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
  return 1
end
return 0
"""

_RENEW_IF_HOLDER = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
"""

_DELETE_IF_HOLDER = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""


@dataclass
class PendingChange:
    added: str | None = None
    removed: str | None = None


def fold(meta: SessionMeta, event: str, data: dict[str, Any]) -> PendingChange:
    """Update status meta from one frame (root agent only). Returns the pending-set change,
    for ``save_meta`` to apply to the shared set."""
    change = PendingChange()
    if event == "turn_started":
        meta.turn_active = True
        meta.current_turn = max(meta.current_turn, int(data.get("turn_number") or 0))
    elif event == "turn_end":
        meta.turn_active = False
    elif event == "message_accepted" and data.get("disposition") != "joined":
        if data.get("message_id"):
            meta.pending.add(data["message_id"])
            change.added = data["message_id"]
    elif event in ("message_handler_start", "message_handler_error"):
        # A handler error with no prior start is the agent refusing a front-door message.
        if data.get("message_id"):
            meta.pending.discard(data["message_id"])
            change.removed = data["message_id"]
    return change


def _frame(entry_id: str, fields: dict[str, str]) -> StoredFrame:
    ms, _seq = entry_id.split("-")
    return StoredFrame(
        event=fields["e"],
        data=json.loads(fields["d"]),
        resume_offset=int(ms) - 1,
        entry_id=entry_id,
    )
