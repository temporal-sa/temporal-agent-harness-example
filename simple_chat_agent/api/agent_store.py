"""Redis storage for agent sessions: frame log, status, ownership, forwarder leases, wake
channel, and the durable frame archive.

Frame format v1 (CTO memo 001, CTO-2). One Redis stream per agent session:

    key   {prefix}:s:{session}:f
    id    "{resume_offset + 1}-{seq}"   # explicit, so a duplicate write is rejected
    data  v=1, e=<event type>, d=<harness SSE data JSON without resume_offset>

``resume_offset`` is the harness's root-stream cursor; several subagent frames can share one,
hence the per-offset ``seq``. Redis stream ids must be greater than 0-0, so the stored
millisecond part is ``resume_offset + 1``. Root frames also carry ``event_offset`` (=
``resume_offset - 1``) so the client dedups them by ``agent_id|event_offset``. Telemetry frames
(``x_usage``, ``x_recovery``, ``x_worker``) sit at the position of the frame before them.

Status meta lives in a key next to it and is served to clients instead of a Temporal Query,
so ``status`` costs no Actions and still answers while every worker is down. Pending message
ids live in their own set (``{prefix}:s:{session}:p``), changed only by SADD/SREM, because two
writers touch them: the API adds an id when it sends, the bridge adds and removes ids as frames
arrive. Keeping them in the meta JSON let the bridge's copy overwrite a send made mid-forward.
Fields written by the bridge's background jobs (ack, archive cursor, worker health) live in a
hash (``…:t``) for the same reason: the forward loop rewrites the meta JSON wholesale.

The archive (``FrameArchive``) is the system of record once Temporal has truncated the stream
at continue-as-new and Redis has expired: one JSONL object per session per turn, on S3 when
``SIMPLE_CHAT_S3_BUCKET`` is set, else on local disk.

Archive retention is not enforced by this code. On S3, set a bucket lifecycle rule that expires
objects under the archive prefix (``AGENT_ARCHIVE_PREFIX``, default ``agent-frames/``) after the
product's transcript retention, which must be at least the session TTL
(``AGENT_SESSION_TTL_SECONDS``, 15 days): S3 applies it with no LIST or DELETE calls from the API.
The disk backend is for local development (default: the system temp directory) and is not pruned.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import redis.asyncio as redis
from redis.exceptions import ResponseError

FRAME_VERSION = "1"
# Most recent finished message ids remembered per session (N10). Oldest are trimmed.
DONE_CAP = int(os.environ.get("AGENT_DONE_CAP", "2000"))
# Telemetry frame ids remembered per session, so a takeover never re-emits one.
EMITTED_CAP = int(os.environ.get("AGENT_EMITTED_CAP", "5000"))
MAX_SEQ = 2**64 - 1


class LeaseLost(Exception):
    """A fenced write found the forwarder lease held by someone else."""


def prefix() -> str:
    env = os.environ.get("SIMPLE_CHAT_TASK_QUEUE", "local")
    return os.environ.get("AGENT_REDIS_PREFIX", f"aah:{env}")


def session_ttl_seconds() -> int:
    return int(os.environ.get("AGENT_SESSION_TTL_SECONDS", str(15 * 24 * 3600)))


def wake_pattern() -> str:
    """Pub/sub pattern every API replica subscribes to (one connection per process)."""
    return f"{prefix()}:wake:*"


def wake_channel(session: str) -> str:
    return f"{prefix()}:wake:{session}"


def parse_entry_id(entry_id: str) -> tuple[int, int]:
    ms, seq = entry_id.split("-")
    return int(ms), int(seq)


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
    # Root stream offset of the open turn's turn_started (a takeover replays from here so
    # subagents mounted in this turn are re-mounted). None between turns.
    turn_start: int | None = None
    # Bridge bookkeeping (the ``…:t`` hash; read-only here, written by its own setters).
    acked: int = 0
    archived: str = "0-0"
    worker: dict[str, Any] | None = None
    # True from a root turn_end until that turn's telemetry is confirmed (or given up on, or
    # the deadline passed), so an attach doesn't end before the confirmed frames are stored.
    confirming: bool = False

    def to_status(self) -> dict[str, Any]:
        status: dict[str, Any] = {
            "current_turn": self.current_turn,
            "turn_active": self.turn_active,
            "pending_turns": sorted(self.pending),
        }
        if self.worker is not None:
            status["worker"] = self.worker
        return status


# -- archive -------------------------------------------------------------------------------


_ARCHIVE_NAME = re.compile(r"^(\d{12})-(\d{6,})_(\d{12})-(\d{6,})\.jsonl$")


def _safe_session(session: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]", "_", session)


class _DiskArchive:
    def __init__(self, root: Path) -> None:
        self.root = root

    def describe(self) -> str:
        return f"file://{self.root}"

    async def put(self, key: str, body: bytes) -> None:
        def _write() -> None:
            path = self.root / key
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(".tmp")
            tmp.write_bytes(body)
            os.replace(tmp, path)

        await asyncio.to_thread(_write)

    async def list(self, key_prefix: str) -> list[str]:
        def _ls() -> list[str]:
            d = self.root / key_prefix
            if not d.is_dir():
                return []
            return sorted(f"{key_prefix}{p.name}" for p in d.iterdir() if p.suffix == ".jsonl")

        return await asyncio.to_thread(_ls)

    async def get(self, key: str) -> bytes:
        return await asyncio.to_thread((self.root / key).read_bytes)


class _S3Archive:
    """The app's cached boto3 client (common/store.py), calls run off the event loop."""

    def __init__(self, bucket: str) -> None:
        self.bucket = bucket

    def _client(self) -> Any:
        from simple_chat_agent.common.store import _s3_client

        return _s3_client()

    def describe(self) -> str:
        return f"s3://{self.bucket}"

    async def put(self, key: str, body: bytes) -> None:
        await asyncio.to_thread(
            lambda: self._client().put_object(
                Bucket=self.bucket, Key=key, Body=body, ContentType="application/x-ndjson"
            )
        )

    async def list(self, key_prefix: str) -> list[str]:
        def _ls() -> list[str]:
            keys: list[str] = []
            paginator = self._client().get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.bucket, Prefix=key_prefix):
                keys.extend(obj["Key"] for obj in page.get("Contents", ()))
            return sorted(keys)

        return await asyncio.to_thread(_ls)

    async def get(self, key: str) -> bytes:
        def _get() -> bytes:
            return self._client().get_object(Bucket=self.bucket, Key=key)["Body"].read()

        return await asyncio.to_thread(_get)


class FrameArchive:
    """Append-only archive of stored frames: one JSONL object per session per turn.

    Object key ``{archive_prefix}/{session}/{first_ms:012d}-{first_seq:06d}_{last_ms:012d}-{last_seq:06d}.jsonl``
    (stream entry ids of the first and last frame it holds), so a listing sorts by position and
    a reader skips objects that end before where it starts. Each line is
    ``{"id": entry_id, "e": event, "d": data}``. Objects can overlap after a forwarder takeover
    (the same range archived twice); readers dedup by entry id.
    """

    def __init__(self, backend: Any, key_prefix: str) -> None:
        self._backend = backend
        self.key_prefix = key_prefix.strip("/")

    @classmethod
    def from_env(cls) -> FrameArchive:
        key_prefix = os.environ.get("AGENT_ARCHIVE_PREFIX") or "agent-frames/" + prefix().replace(":", "/")
        bucket = os.environ.get("SIMPLE_CHAT_S3_BUCKET", "").strip()
        if bucket:
            return cls(_S3Archive(bucket), key_prefix)
        base = os.environ.get("SIMPLE_CHAT_ARTIFACT_DIR", "").strip()
        root = Path(base) if base else Path(tempfile.gettempdir()) / "simple-chat-agent"
        return cls(_DiskArchive(root), key_prefix)

    def describe(self) -> str:
        return f"{self._backend.describe()}/{self.key_prefix}"

    def _session_prefix(self, session: str) -> str:
        return f"{self.key_prefix}/{_safe_session(session)}/"

    async def put(self, session: str, frames: list[StoredFrame], *, turn: int | None = None) -> str | None:
        """Write one object holding ``frames`` (in stream order). Returns its key."""
        if not frames:
            return None
        first, last = parse_entry_id(frames[0].entry_id), parse_entry_id(frames[-1].entry_id)
        key = (
            f"{self._session_prefix(session)}"
            f"{first[0]:012d}-{first[1]:06d}_{last[0]:012d}-{last[1]:06d}.jsonl"
        )
        body = "".join(
            json.dumps({"id": f.entry_id, "e": f.event, "d": f.data, **({"turn": turn} if turn else {})},
                       separators=(",", ":")) + "\n"
            for f in frames
        )
        await self._backend.put(key, body.encode())
        return key

    async def _objects(self, session: str) -> list[tuple[tuple[int, int], tuple[int, int], str]]:
        out = []
        for key in await self._backend.list(self._session_prefix(session)):
            m = _ARCHIVE_NAME.match(key.rsplit("/", 1)[-1])
            if m:
                out.append(((int(m[1]), int(m[2])), (int(m[3]), int(m[4])), key))
        out.sort()
        return out

    async def last_position(self, session: str) -> tuple[int, int] | None:
        """(resume_offset, seq) of the newest archived frame."""
        objects = await self._objects(session)
        if not objects:
            return None
        ms, seq = max(o[1] for o in objects)
        return ms - 1, seq

    async def read(
        self, session: str, *, after: str = "0-0", before: str | None = None, count: int | None = None
    ) -> list[StoredFrame]:
        """Archived frames with ``after < entry id < before``, in order, without duplicates."""
        lo = parse_entry_id(after)
        hi = parse_entry_id(before) if before else None
        frames: dict[tuple[int, int], StoredFrame] = {}
        # Objects are read lazily in order of their first position. Once ``count`` frames are
        # held, an object starting past the count-th smallest of them can't contribute.
        for first, last, key in await self._objects(session):
            if last <= lo or (hi is not None and first >= hi):
                continue
            if count is not None and len(frames) >= count and first > sorted(frames)[count - 1]:
                break
            for line in (await self._backend.get(key)).decode().splitlines():
                if not line.strip():
                    continue
                rec = json.loads(line)
                pos = parse_entry_id(rec["id"])
                if pos <= lo or (hi is not None and pos >= hi):
                    continue
                frames.setdefault(pos, StoredFrame(rec["e"], rec["d"], pos[0] - 1, rec["id"]))
        ordered = [frames[k] for k in sorted(frames)]
        return ordered[:count] if count is not None else ordered


# -- the store ----------------------------------------------------------------------------


class AgentStore:
    def __init__(self, client: redis.Redis, archive: FrameArchive | None = None) -> None:
        self.r = client
        self.archive = archive if archive is not None else FrameArchive.from_env()

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

    async def claim_owner(self, session: str, owner: str, *, root: str | None = None) -> bool:
        """Write the owner record only if there is none (SET NX; same key and value as
        ``set_owner``). True if this call wrote it."""
        return bool(
            await self.r.set(
                self._k(session, "owner"),
                json.dumps({"owner": owner, "root": root or session}),
                ex=session_ttl_seconds(),
                nx=True,
            )
        )

    async def owner_of(self, session: str) -> str | None:
        """The session's owner. Reading it extends its life: an active session never
        outlives its ownership record."""
        key = self._k(session, "owner")
        pipe = self.r.pipeline(transaction=True)
        pipe.get(key)
        pipe.expire(key, session_ttl_seconds())
        raw, _ = await pipe.execute()
        return json.loads(raw)["owner"] if raw else None

    async def register_subagent(
        self, child_workflow_id: str, root_session_id: str, *, agent_id: str | None = None
    ) -> None:
        """Record that ``child_workflow_id`` (a subagent workflow) belongs to the root session
        ``root_session_id``. A root given as another subagent is resolved to its own root.
        ``agent_id`` is the subagent's id on the merged stream (``subagent_started.subagent_id``),
        which ``read_agent`` filters on."""
        root = await self.root_of(root_session_id) or root_session_id
        if child_workflow_id == root:
            return
        ttl = session_ttl_seconds()
        pipe = self.r.pipeline(transaction=True)
        pipe.set(self._k(child_workflow_id, "root"), root, ex=ttl)
        if agent_id:
            pipe.set(self._subaid_key(child_workflow_id), agent_id, ex=ttl)
        await pipe.execute()

    def _subaid_key(self, child_workflow_id: str) -> str:
        return f"{prefix()}:subaid:{{{child_workflow_id}}}"

    async def agent_id_of(self, child_workflow_id: str) -> str | None:
        """The merged-stream ``agent_id`` of a registered subagent workflow."""
        return await self.r.get(self._subaid_key(child_workflow_id))

    async def root_of(self, workflow_id: str) -> str | None:
        """The root session a workflow belongs to: itself for a root session with an owner
        record, its root for a registered subagent, None when neither is known. Reading
        extends the record's life, like ``owner_of``."""
        root_key, owner_key = self._k(workflow_id, "root"), self._k(workflow_id, "owner")
        ttl = session_ttl_seconds()
        pipe = self.r.pipeline(transaction=True)
        pipe.get(root_key)
        pipe.expire(root_key, ttl)
        pipe.exists(owner_key)
        root, _, has_owner = await pipe.execute()
        if root:
            return root
        return workflow_id if has_owner else None

    # -- frames ------------------------------------------------------------------

    async def append(
        self,
        session: str,
        event: str,
        data: dict[str, Any],
        resume_offset: int,
        seq: int,
        *,
        holder: str | None = None,
    ) -> bool:
        """Append one frame. Returns False if this exact position was already stored. With
        ``holder``, the write is fenced: it happens only while ``holder`` owns the forwarder
        lease (one atomic step), else ``LeaseLost``."""
        entry_id = f"{resume_offset + 1}-{seq}"
        fields = {"v": FRAME_VERSION, "e": event, "d": json.dumps(data, separators=(",", ":"))}
        if holder is None:
            try:
                await self.r.xadd(self._k(session, "f"), fields, id=entry_id)
            except ResponseError as exc:
                if "equal or smaller" in str(exc):
                    return False
                raise
            await self.r.expire(self._k(session, "f"), session_ttl_seconds())
            return True
        result = await self.r.eval(
            _FENCED_XADD,
            2,
            self._k(session, "f"),
            self._k(session, "lease"),
            holder,
            entry_id,
            session_ttl_seconds(),
            *[x for kv in fields.items() for x in kv],
        )
        if result == "fenced":
            raise LeaseLost(session)
        return result == "ok"

    async def last_position(self, session: str) -> tuple[int, int] | None:
        """(resume_offset, seq) of the newest stored frame."""
        entries = await self.r.xrevrange(self._k(session, "f"), count=1)
        if not entries:
            return None
        ms, seq = entries[0][0].split("-")
        return int(ms) - 1, int(seq)

    async def first_entry_id(self, session: str) -> str | None:
        entries = await self.r.xrange(self._k(session, "f"), count=1)
        return entries[0][0] if entries else None

    async def read(self, session: str, *, after: str, count: int = 500) -> list[StoredFrame]:
        """Frames strictly after the stream id ``after`` ("0-0" for everything)."""
        entries = await self.r.xrange(self._k(session, "f"), min=f"({after}", count=count)
        return [_frame(entry_id, fields) for entry_id, fields in entries]

    async def read_backfilled(self, session: str, *, after: str, count: int = 500) -> list[StoredFrame]:
        """Up to ``count`` frames strictly after ``after``, in order, from the archive while
        ``after`` is older than what Redis holds (Redis expired, lost, or rebuilt from a later
        position), then from Redis.

        Contract for readers: use it in place of ``read`` for every non-blocking read, passing
        the entry id of the last frame you got as ``after``, and keep calling while it returns
        frames. An empty result means you are caught up with Redis; only then block on
        ``wait``. One call costs one ``XRANGE COUNT 1`` more than ``read`` when Redis is
        complete; the archive is touched only while ``after`` is behind Redis."""
        first = await self.first_entry_id(session)
        # Redis drops a stream whole (TTL) and never trims it, so a stream that starts at the
        # session's first position is complete. One that starts later was rebuilt after a
        # loss; an empty one may be a new session or a lost one.
        if first is not None and (
            parse_entry_id(first) <= (2, 0) or parse_entry_id(first) <= _next_id(after)
        ):
            return await self.read(session, after=after, count=count)
        try:
            archived = await self.archive.read(session, after=after, before=first, count=count)
        except Exception:  # noqa: BLE001 - archive down: serve what Redis has
            archived = []
        if len(archived) >= count:
            return archived
        tail_after = archived[-1].entry_id if archived else after
        return archived + await self.read(session, after=tail_after, count=count - len(archived))

    async def read_at(self, session: str, resume_offset: int) -> list[StoredFrame]:
        """Every frame stored at one resume position."""
        ms = resume_offset + 1
        entries = await self.r.xrange(self._k(session, "f"), min=f"{ms}-0", max=f"{ms}-{MAX_SEQ}")
        return [_frame(entry_id, fields) for entry_id, fields in entries]

    async def read_from(self, session: str, resume_offset: int) -> list[StoredFrame]:
        """Every frame stored at or after one resume position."""
        out: list[StoredFrame] = []
        after = f"{resume_offset + 1}-0"
        entries = await self.r.xrange(self._k(session, "f"), min=after, count=1000)
        while entries:
            out.extend(_frame(entry_id, fields) for entry_id, fields in entries)
            entries = await self.r.xrange(self._k(session, "f"), min=f"({entries[-1][0]}", count=1000)
        return out

    async def read_range(self, session: str, *, after: str, through: str) -> list[StoredFrame]:
        """Frames with ``after < entry id <= through``."""
        out: list[StoredFrame] = []
        cursor = after
        while True:
            entries = await self.r.xrange(self._k(session, "f"), min=f"({cursor}", max=through, count=1000)
            if not entries:
                return out
            out.extend(_frame(entry_id, fields) for entry_id, fields in entries)
            cursor = entries[-1][0]

    async def wait(self, session: str, *, after: str, block_ms: int) -> list[StoredFrame]:
        result = await self.r.xread({self._k(session, "f"): after}, block=block_ms, count=500)
        if not result:
            return []
        _key, entries = result[0]
        return [_frame(entry_id, fields) for entry_id, fields in entries]

    # -- meta --------------------------------------------------------------------

    def _queue_meta(self, pipe: Any, session: str) -> None:
        pipe.get(self._k(session, "m"))
        pipe.smembers(self._k(session, "p"))
        pipe.hgetall(self._k(session, "t"))
        pipe.exists(self._k(session, "closed"))

    async def meta(self, session: str) -> SessionMeta:
        pipe = self.r.pipeline(transaction=True)
        self._queue_meta(pipe, session)
        return _meta(*await pipe.execute())

    async def metas(self, sessions: list[str]) -> list[SessionMeta]:
        """``meta`` for many sessions in one pipeline (one round trip), in the order given.
        (The routes' ``closed_flags`` consumes it as a sequence.)"""
        if not sessions:
            return []
        pipe = self.r.pipeline(transaction=False)
        for session in sessions:
            self._queue_meta(pipe, session)
        results = await pipe.execute()
        return [_meta(*results[i * 4:(i + 1) * 4]) for i in range(len(sessions))]

    async def mark_closed(self, session: str) -> None:
        """The session ended. A key of its own (``…:closed``), so the bridge's whole-JSON meta
        writes can never clear it; ``meta().closed`` is true once either says so."""
        await self.r.set(self._k(session, "closed"), "1", ex=session_ttl_seconds())

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
        id another writer added. The ``…:t`` fields aren't written here either."""
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
                    "turn_start": meta.turn_start,
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
            # workflow deduplicates) doesn't put it back in the pending set. Bounded (N10): a
            # sorted set by insertion time, oldest trimmed past DONE_CAP, in the same MULTI.
            done = self._k(session, "dz")
            pipe.zadd(done, {pending_removed: _now_ms()}, nx=True)
            pipe.zremrangebyrank(done, 0, -(DONE_CAP + 1))
            pipe.expire(done, ttl)
        await pipe.execute()

    async def set_tracking(self, session: str, **fields: Any) -> None:
        """Set bridge bookkeeping fields (``acked``, ``archived``, ``worker``)."""
        mapping = {k: (json.dumps(v) if k == "worker" else str(v)) for k, v in fields.items()}
        pipe = self.r.pipeline(transaction=True)
        pipe.hset(self._k(session, "t"), mapping=mapping)
        pipe.expire(self._k(session, "t"), session_ttl_seconds())
        await pipe.execute()

    async def begin_confirming(self, session: str, deadline: float) -> str:
        """Mark a root turn's telemetry as being confirmed until ``deadline`` (epoch s).
        Returns the token to pass to ``end_confirming``."""
        token = repr(deadline)
        await self.set_tracking(session, confirming_until=token)
        return token

    async def end_confirming(self, session: str, token: str) -> bool:
        """Clear the confirming mark, only if it is still ``token`` (a later turn's mark is
        left alone). Atomic compare-and-clear. True if it cleared."""
        return bool(await self.r.eval(_CLEAR_IF_EQUAL, 1, self._k(session, "t"), "confirming_until", token))

    async def note_sent(self, session: str, message_id: str) -> None:
        """A message went out through the front door; it is pending until its turn starts,
        or until the agent rejects it. A retry of a message that already left the pending
        set is not re-added."""
        await self.r.eval(
            _ADD_PENDING_UNLESS_DONE,
            2,
            self._k(session, "p"),
            self._k(session, "dz"),
            message_id,
            session_ttl_seconds(),
        )

    async def clear_pending(self, session: str) -> None:
        """The session ended: nothing it held will ever start."""
        await self.r.delete(self._k(session, "p"))

    async def mark_emitted(self, session: str, frame_id: str) -> bool:
        """Record that a telemetry frame was stored. False if it already was. Bounded."""
        return bool(
            await self.r.eval(
                _BOUNDED_ADD, 1, self._k(session, "x"), frame_id, _now_ms(), EMITTED_CAP, session_ttl_seconds()
            )
        )

    async def was_emitted(self, session: str, frame_id: str) -> bool:
        return await self.r.zscore(self._k(session, "x"), frame_id) is not None

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

    async def lease_holder(self, session: str) -> str | None:
        return await self.r.get(self._k(session, "lease"))

    # -- wake --------------------------------------------------------------------

    async def publish_wake(self, session: str, kind: str = "send") -> None:
        """Tell every API replica that ``session`` has work (``send``) or that its forwarder
        let go of the lease (``released``)."""
        await self.r.publish(wake_channel(session), kind)


def _now_ms() -> int:
    import time

    return int(time.time() * 1000)


def _next_id(after: str) -> tuple[int, int]:
    ms, seq = parse_entry_id(after)
    return (ms, seq + 1)


_FENCED_XADD = """
if redis.call('GET', KEYS[2]) ~= ARGV[1] then
  return 'fenced'
end
local ok, err = pcall(redis.call, 'XADD', KEYS[1], ARGV[2], unpack(ARGV, 4))
if not ok then
  if string.find(tostring(err), 'equal or smaller') then
    return 'dup'
  end
  return redis.error_reply(tostring(err))
end
redis.call('EXPIRE', KEYS[1], ARGV[3])
return 'ok'
"""

_CLEAR_IF_EQUAL = """
if redis.call('HGET', KEYS[1], ARGV[1]) == ARGV[2] then
  redis.call('HSET', KEYS[1], ARGV[1], '0')
  return 1
end
return 0
"""

_BOUNDED_ADD = """
local added = redis.call('ZADD', KEYS[1], 'NX', ARGV[2], ARGV[1])
redis.call('ZREMRANGEBYRANK', KEYS[1], 0, -(tonumber(ARGV[3]) + 1))
redis.call('EXPIRE', KEYS[1], ARGV[4])
return added
"""

_ADD_PENDING_UNLESS_DONE = """
if redis.call('ZSCORE', KEYS[2], ARGV[1]) == false then
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
        meta.turn_start = None
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


def _meta(raw: Any, pending: Any, track: Any, closed_key: Any) -> SessionMeta:
    d = json.loads(raw) if raw else {}
    track = track or {}
    return SessionMeta(
        head=d.get("head", 0),
        turn_active=d.get("turn_active", False),
        current_turn=d.get("current_turn", 0),
        pending=set(pending or ()),
        closed=bool(d.get("closed", False) or closed_key),
        turn_start=d.get("turn_start"),
        acked=int(track.get("acked") or 0),
        archived=track.get("archived") or "0-0",
        worker=json.loads(track["worker"]) if track.get("worker") else None,
        confirming=float(track.get("confirming_until") or 0) > _now_ms() / 1000,
    )
