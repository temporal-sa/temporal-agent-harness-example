"""History budget guard (PRD A20) and spend caps (PRD A21).

``HistoryBudget`` is pure and workflow-safe: the caller passes in ``workflow.info()`` values
and its own Signal and Update counters, and gets back the reason to checkpoint, if any.

``SpendGuard`` runs in activities (the model activity) and keeps token counters in Redis. It
fails closed: if Redis can't be reached, ``check`` raises a retryable
``SpendGuardUnavailable`` (the activity retry policy retries it a few times, then the turn
fails). ``redis`` is imported lazily so this module is safe to import from workflow code.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from temporalio.exceptions import ApplicationError

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# History budget (A20)
# ---------------------------------------------------------------------------

# Temporal Cloud/server hard limits per workflow run.
TEMPORAL_MAX_HISTORY_EVENTS = 51_200
TEMPORAL_MAX_HISTORY_BYTES = 50 * 1024 * 1024
TEMPORAL_MAX_SIGNALS = 10_000
TEMPORAL_MAX_UPDATES = 2_000
CHECKPOINT_FRACTION = 0.6


@dataclass(frozen=True)
class HistoryBudget:
    """Decides when a run should checkpoint (end the turn and continue as new).

    Each limit is checkpointed at ``fraction`` of the Temporal limit (default 60%).
    """

    max_events: int = TEMPORAL_MAX_HISTORY_EVENTS
    max_bytes: int = TEMPORAL_MAX_HISTORY_BYTES
    max_signals: int = TEMPORAL_MAX_SIGNALS
    max_updates: int = TEMPORAL_MAX_UPDATES
    fraction: float = CHECKPOINT_FRACTION

    def __post_init__(self) -> None:
        if not 0 < self.fraction <= 1:
            raise ValueError("fraction must be in (0, 1]")

    def thresholds(self) -> dict[str, int]:
        return {
            "history_events": int(self.max_events * self.fraction),
            "history_bytes": int(self.max_bytes * self.fraction),
            "signals": int(self.max_signals * self.fraction),
            "updates": int(self.max_updates * self.fraction),
        }

    def should_checkpoint(
        self,
        *,
        history_length: int,
        history_size_bytes: int,
        signals: int = 0,
        updates: int = 0,
        pending_events: int = 0,
        pending_bytes: int = 0,
    ) -> str | None:
        """The reason to checkpoint, or None.

        ``history_length`` and ``history_size_bytes`` come from
        ``workflow.info().get_current_history_length()`` / ``get_current_history_size()``.
        ``pending_*`` is what the next step is expected to add (for example the model call's
        payload), so the check fires *before* the call that would cross the line. Reasons, in
        priority order: ``history_bytes``, ``history_events``, ``updates``, ``signals``.
        """
        limits = self.thresholds()
        if history_size_bytes + pending_bytes >= limits["history_bytes"]:
            return "history_bytes"
        if history_length + pending_events >= limits["history_events"]:
            return "history_events"
        if updates >= limits["updates"]:
            return "updates"
        if signals >= limits["signals"]:
            return "signals"
        return None


# ---------------------------------------------------------------------------
# Spend caps (A21)
# ---------------------------------------------------------------------------

DEFAULT_SESSION_CAP = 2_000_000
DEFAULT_USER_DAY_CAP = 5_000_000
DEFAULT_GLOBAL_DAY_CAP = 50_000_000
DAY_KEY_TTL_SECONDS = 2 * 24 * 3600  # a day counter outlives its UTC day, then expires
SPEND_CAP_REACHED = "SpendCapReached"
SPEND_GUARD_UNAVAILABLE = "SpendGuardUnavailable"

# INCRBY, then set the TTL. Day keys keep their first TTL (expire at a fixed point after the
# day starts); the session key's TTL slides with activity. One key per script: cluster-safe.
_RECORD_SCRIPT = """
local v = redis.call('INCRBY', KEYS[1], ARGV[1])
if ARGV[3] == '1' or redis.call('TTL', KEYS[1]) < 0 then
  redis.call('EXPIRE', KEYS[1], ARGV[2])
end
return v
"""


@dataclass(frozen=True)
class SpendCaps:
    """Token caps. A cap of 0 or less disables that cap."""

    session: int = DEFAULT_SESSION_CAP
    user_day: int = DEFAULT_USER_DAY_CAP
    global_day: int = DEFAULT_GLOBAL_DAY_CAP

    @classmethod
    def from_env(cls) -> SpendCaps:
        def read(name: str, default: int) -> int:
            raw = os.environ.get(name, "").strip()
            return int(raw) if raw else default

        return cls(
            session=read("AGENT_SPEND_CAP_SESSION_TOKENS", DEFAULT_SESSION_CAP),
            user_day=read("AGENT_SPEND_CAP_USER_DAY_TOKENS", DEFAULT_USER_DAY_CAP),
            global_day=read("AGENT_SPEND_CAP_GLOBAL_DAY_TOKENS", DEFAULT_GLOBAL_DAY_CAP),
        )


def _default_prefix() -> str:
    # Same default as simple_chat_agent.api.agent_store.prefix(); not imported, to keep the
    # worker free of API-module imports.
    env = os.environ.get("SIMPLE_CHAT_TASK_QUEUE", "local")
    return os.environ.get("AGENT_REDIS_PREFIX", f"aah:{env}")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class SpendGuard:
    """Per-session, per-user-per-UTC-day and global-per-UTC-day token caps in Redis.

    Call ``check`` before each model call and ``record`` after it. Check-then-call is not
    atomic: concurrent calls can overshoot a cap by at most the calls already in flight.
    """

    def __init__(
        self,
        client: Any,
        *,
        caps: SpendCaps | None = None,
        prefix: str | None = None,
        session_ttl_seconds: int | None = None,
        now: Callable[[], datetime] = _utc_now,
    ) -> None:
        self._client = client
        self.caps = caps or SpendCaps()
        self.prefix = prefix if prefix is not None else _default_prefix()
        self.session_ttl_seconds = session_ttl_seconds or int(
            os.environ.get("AGENT_SESSION_TTL_SECONDS", str(15 * 24 * 3600))
        )
        self._now = now

    # Keys. Each has its own hash tag, and every Redis call touches one key, so this works on
    # Redis Cluster. The session key shares agent_store's tag for that session.
    def session_key(self, session_id: str) -> str:
        return f"{self.prefix}:s:{{{session_id}}}:spend"

    def user_day_key(self, user_id: str, day: str | None = None) -> str:
        return f"{self.prefix}:spend:u:{{{user_id}}}:{day or self._day()}"

    def global_day_key(self, day: str | None = None) -> str:
        return f"{self.prefix}:spend:{{global}}:{day or self._day()}"

    def _day(self) -> str:
        return self._now().astimezone(timezone.utc).strftime("%Y%m%d")

    def _keys(self, session_id: str, user_id: str) -> list[tuple[str, str, int]]:
        day = self._day()
        return [
            ("global_day", self.global_day_key(day), self.caps.global_day),
            ("user_day", self.user_day_key(user_id, day), self.caps.user_day),
            ("session", self.session_key(session_id), self.caps.session),
        ]

    async def usage(self, session_id: str, user_id: str) -> dict[str, int]:
        """Current counters, by cap name. Raises ``SpendGuardUnavailable`` if Redis is down."""
        keys = self._keys(session_id, user_id)
        try:
            pipe = self._client.pipeline(transaction=False)
            for _, key, _ in keys:
                pipe.get(key)
            values = await pipe.execute()
        except Exception as exc:
            raise _unavailable(exc) from exc
        return {name: int(value or 0) for (name, _, _), value in zip(keys, values)}

    async def check(self, session_id: str, user_id: str) -> None:
        """Raise ``SpendCapReached`` (non-retryable) if any cap is reached; fail closed."""
        used = await self.usage(session_id, user_id)
        exceeded = [
            (name, cap)
            for name, _, cap in self._keys(session_id, user_id)
            if cap > 0 and used[name] >= cap
        ]
        if not exceeded:
            return
        name, cap = exceeded[0]
        raise ApplicationError(
            _CAP_MESSAGES[name],
            {
                "cap": name,
                "limit": cap,
                "used": used[name],
                "exceeded": [n for n, _ in exceeded],
                "session_id": session_id,
                "user_id": user_id,
                "day": self._day(),
            },
            type=SPEND_CAP_REACHED,
            non_retryable=True,
        )

    async def record(
        self, session_id: str, user_id: str, input_tokens: int, output_tokens: int
    ) -> bool:
        """Add a call's tokens to all three counters (atomic INCRBY with expiry).

        Never raises: the model call already happened, and failing here would make Temporal
        retry (and pay for) it again. Returns False if the counters could not be updated; the
        next ``check`` then fails closed if Redis is still down.
        """
        tokens = max(0, int(input_tokens or 0)) + max(0, int(output_tokens or 0))
        if tokens == 0:
            return True
        try:
            pipe = self._client.pipeline(transaction=False)
            for name, key, _ in self._keys(session_id, user_id):
                if name == "session":
                    pipe.eval(_RECORD_SCRIPT, 1, key, tokens, self.session_ttl_seconds, 1)
                else:
                    pipe.eval(_RECORD_SCRIPT, 1, key, tokens, DAY_KEY_TTL_SECONDS, 0)
            await pipe.execute()
        except Exception:
            log.warning(
                "spend guard: could not record %d tokens for session %s", tokens, session_id,
                exc_info=True,
            )
            return False
        return True

    async def aclose(self) -> None:
        await self._client.aclose()


_CAP_MESSAGES = {
    "session": "This session has used its token budget.",
    "user_day": "You have used today's token budget. It resets at 00:00 UTC.",
    "global_day": "The service has reached today's token budget. It resets at 00:00 UTC.",
}


def _unavailable(exc: BaseException) -> ApplicationError:
    return ApplicationError(
        "The spend counter store is unavailable; not starting a model call.",
        {"error": f"{type(exc).__name__}: {exc}"},
        type=SPEND_GUARD_UNAVAILABLE,
        non_retryable=False,
    )


def spend_guard_from_env() -> SpendGuard:
    """A guard on ``SIMPLE_CHAT_REDIS_URL`` with caps from ``AGENT_SPEND_CAP_*`` and the
    ``AGENT_REDIS_PREFIX`` key prefix.

    Short socket timeouts so a dead Redis fails fast. A *blocking* pool: redis-py's default
    async pool raises ``MaxConnectionsError`` past 100 concurrent commands instead of waiting,
    which would fail checks and lose recorded tokens under load."""
    import redis.asyncio as redis

    url = os.environ.get("SIMPLE_CHAT_REDIS_URL", "redis://127.0.0.1:6379/0")
    pool = redis.BlockingConnectionPool.from_url(
        url,
        decode_responses=True,
        socket_connect_timeout=2.0,
        socket_timeout=2.0,
        max_connections=int(os.environ.get("AGENT_SPEND_REDIS_MAX_CONNECTIONS", "50")),
        timeout=5.0,
    )
    return SpendGuard(redis.Redis(connection_pool=pool), caps=SpendCaps.from_env())
