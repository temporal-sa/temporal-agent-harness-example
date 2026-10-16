"""Tests for simple_chat_agent.agent.budget (PRD A20, A21)."""

from __future__ import annotations

import asyncio
import os
import unittest
import uuid
from datetime import datetime, timedelta, timezone
from unittest import mock

import redis.asyncio as redis
from temporalio.exceptions import ApplicationError

from simple_chat_agent.agent.budget import (
    DAY_KEY_TTL_SECONDS,
    SPEND_CAP_REACHED,
    SPEND_GUARD_UNAVAILABLE,
    HistoryBudget,
    SpendCaps,
    SpendGuard,
    spend_guard_from_env,
)

REDIS_URL = os.environ.get("TEST_SPEND_REDIS_URL", "redis://127.0.0.1:6390/0")
DEAD_URL = "redis://127.0.0.1:1/0"


class HistoryBudgetTests(unittest.TestCase):
    def test_defaults_are_60_percent_of_temporal_limits(self) -> None:
        self.assertEqual(
            HistoryBudget().thresholds(),
            {"history_events": 30_720, "history_bytes": 31_457_280, "signals": 6_000, "updates": 1_200},
        )

    def test_below_every_threshold(self) -> None:
        b = HistoryBudget()
        self.assertIsNone(b.should_checkpoint(history_length=30_719, history_size_bytes=31_457_279, signals=5_999, updates=1_199))

    def test_each_reason(self) -> None:
        b = HistoryBudget()
        self.assertEqual(b.should_checkpoint(history_length=30_720, history_size_bytes=0), "history_events")
        self.assertEqual(b.should_checkpoint(history_length=0, history_size_bytes=31_457_280), "history_bytes")
        self.assertEqual(b.should_checkpoint(history_length=0, history_size_bytes=0, signals=6_000), "signals")
        self.assertEqual(b.should_checkpoint(history_length=0, history_size_bytes=0, updates=1_200), "updates")

    def test_priority_when_several_cross(self) -> None:
        b = HistoryBudget()
        self.assertEqual(
            b.should_checkpoint(history_length=40_000, history_size_bytes=40_000_000, signals=9_000, updates=1_900),
            "history_bytes",
        )
        self.assertEqual(b.should_checkpoint(history_length=40_000, history_size_bytes=0, updates=1_900), "history_events")

    def test_pending_growth_fires_before_the_call_that_would_cross(self) -> None:
        b = HistoryBudget(max_bytes=1_000_000)
        self.assertIsNone(b.should_checkpoint(history_length=10, history_size_bytes=500_000))
        self.assertEqual(
            b.should_checkpoint(history_length=10, history_size_bytes=500_000, pending_bytes=100_000),
            "history_bytes",
        )
        self.assertEqual(
            HistoryBudget(max_events=1_000).should_checkpoint(history_length=590, history_size_bytes=0, pending_events=10),
            "history_events",
        )

    def test_forced_low_cap(self) -> None:
        b = HistoryBudget(max_events=100, fraction=0.5)
        self.assertIsNone(b.should_checkpoint(history_length=49, history_size_bytes=0))
        self.assertEqual(b.should_checkpoint(history_length=50, history_size_bytes=0), "history_events")

    def test_invalid_fraction(self) -> None:
        with self.assertRaises(ValueError):
            HistoryBudget(fraction=0)


class SpendCapsEnvTests(unittest.TestCase):
    def test_defaults(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=False):
            for name in ("AGENT_SPEND_CAP_SESSION_TOKENS", "AGENT_SPEND_CAP_USER_DAY_TOKENS", "AGENT_SPEND_CAP_GLOBAL_DAY_TOKENS"):
                os.environ.pop(name, None)
            self.assertEqual(SpendCaps.from_env(), SpendCaps(2_000_000, 5_000_000, 50_000_000))

    def test_overrides(self) -> None:
        env = {
            "AGENT_SPEND_CAP_SESSION_TOKENS": "10",
            "AGENT_SPEND_CAP_USER_DAY_TOKENS": "20",
            "AGENT_SPEND_CAP_GLOBAL_DAY_TOKENS": "0",
        }
        with mock.patch.dict(os.environ, env):
            self.assertEqual(SpendCaps.from_env(), SpendCaps(10, 20, 0))

    def test_from_env_uses_prefix_and_url(self) -> None:
        with mock.patch.dict(os.environ, {"AGENT_REDIS_PREFIX": "pfx-test", "SIMPLE_CHAT_REDIS_URL": REDIS_URL}):
            guard = spend_guard_from_env()
            self.assertEqual(guard.prefix, "pfx-test")
            self.assertEqual(guard.session_key("abc"), "pfx-test:s:{abc}:spend")
            self.assertTrue(guard.global_day_key().startswith("pfx-test:spend:{global}:"))


class _Clock:
    def __init__(self, at: datetime) -> None:
        self.at = at

    def __call__(self) -> datetime:
        return self.at


class SpendGuardRedisTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        # A blocking pool, as spend_guard_from_env uses: the default pool raises past 100
        # concurrent commands instead of waiting.
        self.client = redis.Redis(
            connection_pool=redis.BlockingConnectionPool.from_url(REDIS_URL, decode_responses=True, max_connections=20, timeout=5)
        )
        try:
            await self.client.ping()
        except Exception as exc:  # pragma: no cover - environment
            await self.client.aclose()
            self.skipTest(f"Redis not reachable at {REDIS_URL}: {exc}")
        self.prefix = f"test-spend-{uuid.uuid4().hex[:10]}"
        self.clock = _Clock(datetime(2026, 10, 16, 12, 0, tzinfo=timezone.utc))

    async def asyncTearDown(self) -> None:
        keys = [k async for k in self.client.scan_iter(match=f"{self.prefix}:*")]
        if keys:
            await self.client.delete(*keys)
        await self.client.aclose()

    def guard(self, caps: SpendCaps) -> SpendGuard:
        return SpendGuard(self.client, caps=caps, prefix=self.prefix, session_ttl_seconds=3_600, now=self.clock)

    async def test_record_counts_and_sets_expiry(self) -> None:
        g = self.guard(SpendCaps())
        self.assertTrue(await g.record("s1", "u1", 100, 50))
        self.assertTrue(await g.record("s1", "u1", 10, 5))
        self.assertEqual(await g.usage("s1", "u1"), {"global_day": 165, "user_day": 165, "session": 165})
        self.assertEqual(await self.client.get(f"{self.prefix}:s:{{s1}}:spend"), "165")
        self.assertEqual(await self.client.get(f"{self.prefix}:spend:u:{{u1}}:20261016"), "165")
        self.assertEqual(await self.client.get(f"{self.prefix}:spend:{{global}}:20261016"), "165")
        ttl_session = await self.client.ttl(g.session_key("s1"))
        ttl_day = await self.client.ttl(g.user_day_key("u1"))
        self.assertTrue(0 < ttl_session <= 3_600)
        self.assertTrue(DAY_KEY_TTL_SECONDS - 10 < ttl_day <= DAY_KEY_TTL_SECONDS)
        self.assertTrue(await g.record("s1", "u1", 0, 0))  # no-op

    async def test_session_cap(self) -> None:
        g = self.guard(SpendCaps(session=1_000, user_day=10_000, global_day=100_000))
        await g.check("s1", "u1")
        await g.record("s1", "u1", 900, 100)
        with self.assertRaises(ApplicationError) as ctx:
            await g.check("s1", "u1")
        err = ctx.exception
        self.assertEqual(err.type, SPEND_CAP_REACHED)
        self.assertTrue(err.non_retryable)
        details = err.details[0]
        self.assertEqual(details["cap"], "session")
        self.assertEqual((details["limit"], details["used"]), (1_000, 1_000))
        await g.check("s2", "u1")  # another session of the same user is fine

    async def test_user_day_cap_and_day_rollover(self) -> None:
        g = self.guard(SpendCaps(session=0, user_day=1_000, global_day=0))
        self.clock.at = datetime(2026, 10, 16, 23, 59, 59, tzinfo=timezone.utc)
        await g.record("s1", "u1", 1_000, 0)
        with self.assertRaises(ApplicationError) as ctx:
            await g.check("s2", "u1")
        self.assertEqual(ctx.exception.details[0]["cap"], "user_day")
        self.assertEqual(ctx.exception.details[0]["day"], "20261016")
        await g.check("s1", "u2")  # other users unaffected
        self.clock.at += timedelta(seconds=2)  # 2026-10-17 00:00:01 UTC
        await g.check("s1", "u1")
        await g.record("s1", "u1", 7, 0)
        self.assertEqual(await self.client.get(f"{self.prefix}:spend:u:{{u1}}:20261017"), "7")
        self.assertEqual(await self.client.get(f"{self.prefix}:spend:u:{{u1}}:20261016"), "1000")

    async def test_global_breaker_blocks_everyone(self) -> None:
        g = self.guard(SpendCaps(session=0, user_day=0, global_day=500))
        await g.record("s1", "u1", 300, 0)
        await g.record("s2", "u2", 300, 0)
        for session, user in (("s3", "u3"), ("s1", "u1")):
            with self.assertRaises(ApplicationError) as ctx:
                await g.check(session, user)
            self.assertEqual(ctx.exception.details[0]["cap"], "global_day")
            self.assertTrue(ctx.exception.non_retryable)

    async def test_disabled_caps(self) -> None:
        g = self.guard(SpendCaps(session=0, user_day=0, global_day=0))
        await g.record("s1", "u1", 10**9, 0)
        await g.check("s1", "u1")

    async def test_concurrent_increments_are_exact(self) -> None:
        g = self.guard(SpendCaps())
        results = await asyncio.gather(*(g.record(f"s{i % 3}", "u1", 7, 3) for i in range(300)))
        self.assertTrue(all(results))
        usage = await g.usage("s0", "u1")
        self.assertEqual(usage["global_day"], 3_000)
        self.assertEqual(usage["user_day"], 3_000)
        self.assertEqual(usage["session"], 1_000)


class SpendGuardFailClosedTests(unittest.IsolatedAsyncioTestCase):
    async def test_check_fails_closed_when_redis_is_down(self) -> None:
        client = redis.from_url(DEAD_URL, decode_responses=True, socket_connect_timeout=0.5, socket_timeout=0.5)
        g = SpendGuard(client, prefix="dead")
        try:
            with self.assertRaises(ApplicationError) as ctx:
                await g.check("s1", "u1")
            self.assertEqual(ctx.exception.type, SPEND_GUARD_UNAVAILABLE)
            self.assertFalse(ctx.exception.non_retryable)
            # record never raises (the model call already happened), but reports failure.
            self.assertFalse(await g.record("s1", "u1", 10, 10))
        finally:
            await client.aclose()

    async def test_from_env_fails_closed_on_dead_url(self) -> None:
        with mock.patch.dict(os.environ, {"SIMPLE_CHAT_REDIS_URL": DEAD_URL}):
            g = spend_guard_from_env()
        try:
            with self.assertRaises(ApplicationError) as ctx:
                await g.check("s1", "u1")
            self.assertEqual(ctx.exception.type, SPEND_GUARD_UNAVAILABLE)
        finally:
            await g.aclose()


if __name__ == "__main__":
    unittest.main()
