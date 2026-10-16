from __future__ import annotations

import asyncio
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock

from redis.exceptions import ConnectionError as RedisConnectionError

from agent_harness.streaming import StreamEvent
from simple_chat_agent.api.streaming import StreamBroker
from simple_chat_agent.common.redis_streams import RedisStreamStore, ZERO_STREAM_ID
from simple_chat_agent.common.streaming import RedisStreamSink


class _ConnectedRequest:
    async def is_disconnected(self) -> bool:
        return False


class RedisReconnectTests(unittest.IsolatedAsyncioTestCase):
    async def test_safe_operation_disconnects_stale_pool_and_retries(self) -> None:
        client = AsyncMock()
        client.delete.side_effect = [
            RedisConnectionError("connection lost"),
            1,
        ]
        store = RedisStreamStore(client)

        await store.clear("chat-reconnect")

        self.assertEqual(client.delete.await_count, 2)
        client.connection_pool.disconnect.assert_awaited_once_with()

    async def test_append_retries_only_with_an_idempotency_key(self) -> None:
        client = AsyncMock()
        client.eval.side_effect = [
            RedisConnectionError("response lost"),
            "1-0",
        ]
        store = RedisStreamStore(client)

        cursor = await store.append_event(
            "chat-idempotent",
            "turn_settled",
            {"result": {"revision": 1}},
            idempotency_key="turn-1",
        )

        self.assertEqual(cursor, "1-0")
        self.assertEqual(client.eval.await_count, 2)

        client.eval.reset_mock()
        client.eval.side_effect = RedisConnectionError("response lost")
        with self.assertRaises(RedisConnectionError):
            await store.append_event(
                "chat-ambiguous",
                "stream",
                {"kind": "agent_start"},
            )
        self.assertEqual(client.eval.await_count, 1)

    async def test_stream_cleanup_is_best_effort_after_retries(self) -> None:
        store = AsyncMock(spec=RedisStreamStore)
        store.clear.side_effect = RedisConnectionError("redis unavailable")
        broker = StreamBroker(store)

        await broker.clear("chat-cleanup")

        store.clear.assert_awaited_once_with("chat-cleanup")

    async def test_replay_reports_when_another_page_may_exist(self) -> None:
        store = AsyncMock(spec=RedisStreamStore)
        store.replay_entries.return_value = {
            "entries": [
                {"id": "1-0", "event": "stream", "data": {"kind": "agent_start"}},
                {"id": "2-0", "event": "turn_settled", "data": {"result": {}}},
            ],
            "cursor": "2-0",
            "replay_available": True,
            "reason": "",
        }
        broker = StreamBroker(store)

        replay = await broker.replay("chat-paged", limit=2)

        self.assertTrue(replay["has_more"])
        self.assertEqual(replay["entry_count"], 2)
        self.assertEqual(replay["events"], [{"kind": "agent_start"}])

    async def test_compatibility_append_uses_stable_idempotency_key(self) -> None:
        store = AsyncMock(spec=RedisStreamStore)
        store.append_event.return_value = "1-0"
        broker = StreamBroker(store)
        event = {
            "stream_id": "chat-compatibility",
            "kind": "agent_start",
            "sequence": 1,
            "emitted_at": "2026-08-13T14:00:00Z",
        }

        first = await broker.append("chat-compatibility", event)
        first_key = store.append_event.await_args.kwargs["idempotency_key"]
        store.append_event.reset_mock()
        second = await broker.append("chat-compatibility", dict(event))
        second_key = store.append_event.await_args.kwargs["idempotency_key"]

        self.assertEqual(first, "1-0")
        self.assertEqual(second, "1-0")
        self.assertEqual(first_key, second_key)
        self.assertTrue(first_key.startswith("stream:"))

    async def test_sink_flushes_first_token_and_coalesces_following_deltas(self) -> None:
        store = AsyncMock(spec=RedisStreamStore)
        sink = RedisStreamSink(store)

        def event(text: str, sequence: int) -> StreamEvent:
            return StreamEvent(
                stream_id="chat-batched",
                tool_name="agent",
                step=None,
                kind="agent_text_delta",
                payload={
                    "operation_id": "main:llm:1",
                    "sequence": 1,
                    "text": text,
                },
                sequence=sequence,
            )

        await sink.emit(event("A", 1))
        self.assertEqual(store.append_events.await_count, 1)

        await sink.emit(event("B", 2))
        await sink.emit(event("C", 3))
        await sink.flush()

        self.assertEqual(store.append_events.await_count, 2)
        batch = store.append_events.await_args_list[-1].args[0]
        self.assertEqual(len(batch), 1)
        self.assertEqual(batch[0].data["payload"]["text"], "BC")
        await sink.close()

    async def test_turn_stream_validates_cursor_once_and_skips_old_settlement(self) -> None:
        store = AsyncMock(spec=RedisStreamStore)
        store.read_entries.side_effect = [
            {
                "entries": [
                    {
                        "id": "1-0",
                        "event": "turn_settled",
                        "data": {"result": {"to_revision": 4}},
                    },
                    {
                        "id": "2-0",
                        "event": "stream",
                        "data": {"kind": "agent_text_delta"},
                    },
                ],
                "cursor": "2-0",
                "replay_available": True,
                "reason": "",
            },
            {
                "entries": [
                    {
                        "id": "3-0",
                        "event": "turn_settled",
                        "data": {"result": {"to_revision": 6}},
                    }
                ],
                "cursor": "3-0",
                "replay_available": True,
                "reason": "",
            },
        ]
        broker = StreamBroker(store)

        chunks = []
        async for chunk in broker.turn_event_stream(
            "chat-preopened",
            _ConnectedRequest(),  # type: ignore[arg-type]
            cursor="0-0",
            after_revision=4,
        ):
            chunks.append(chunk)

        self.assertEqual(
            [chunk["event"] for chunk in chunks],
            ["stream", "turn_settled"],
        )
        self.assertTrue(store.read_entries.await_args_list[0].kwargs["validate_cursor"])
        self.assertFalse(store.read_entries.await_args_list[1].kwargs["validate_cursor"])


@unittest.skipUnless(shutil.which("redis-server"), "redis-server is not installed")
class RedisStreamingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory()
        socket_path = Path(self._temp_dir.name) / "redis.sock"
        self._process = subprocess.Popen(
            [
                shutil.which("redis-server") or "redis-server",
                "--save",
                "",
                "--appendonly",
                "no",
                "--port",
                "0",
                "--unixsocket",
                str(socket_path),
                "--unixsocketperm",
                "700",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        self.store = RedisStreamStore.from_url(
            f"unix://{socket_path}",
            prefix="test:streams",
            socket_timeout=1.0,
        )
        for _ in range(100):
            if self._process.poll() is not None:
                output = (self._process.communicate()[0] or b"").decode(
                    "utf-8",
                    errors="replace",
                )
                await self.store.close()
                self._temp_dir.cleanup()
                if "operation not permitted" in output.lower():
                    self.skipTest("sandbox does not permit a local Redis socket")
                self.fail(
                    "redis-server exited before accepting connections: "
                    f"{output.strip()}"
                )
            try:
                await self.store.start()
                break
            except Exception:
                await asyncio.sleep(0.01)
        else:
            self.fail("redis-server did not start")

    async def asyncTearDown(self) -> None:
        await self.store.close()
        self._process.terminate()
        try:
            self._process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self._process.kill()
            self._process.wait(timeout=5)
        self._temp_dir.cleanup()

    async def test_append_replay_and_terminal_idempotency(self) -> None:
        first_cursor = await self.store.append_event(
            "chat-1",
            "stream",
            {"kind": "agent_text_delta", "payload": {"text": "hello"}},
        )
        settled_cursor = await self.store.append_event(
            "chat-1",
            "turn_settled",
            {"result": {"revision": 2}},
            idempotency_key="turn-2",
        )
        duplicate_cursor = await self.store.append_event(
            "chat-1",
            "turn_settled",
            {"result": {"revision": 999}},
            idempotency_key="turn-2",
        )

        self.assertEqual(duplicate_cursor, settled_cursor)
        replay = await self.store.replay_entries(
            "chat-1",
            cursor=ZERO_STREAM_ID,
            limit=100,
        )
        self.assertTrue(replay["replay_available"])
        self.assertEqual(
            [entry["event"] for entry in replay["entries"]],
            ["stream", "turn_settled"],
        )
        self.assertEqual(replay["entries"][0]["id"], first_cursor)
        self.assertEqual(replay["cursor"], settled_cursor)
        self.assertEqual(replay["entries"][1]["data"]["result"]["revision"], 2)

    async def test_blocking_read_wakes_for_a_new_event(self) -> None:
        read = asyncio.create_task(
            self.store.read_entries(
                "chat-2",
                cursor=ZERO_STREAM_ID,
                limit=100,
                block_milliseconds=1000,
            )
        )
        await asyncio.sleep(0.05)
        cursor = await self.store.append_event(
            "chat-2",
            "stream",
            {"kind": "agent_start"},
        )

        replay = await asyncio.wait_for(read, timeout=2)
        self.assertEqual(replay["cursor"], cursor)
        self.assertEqual(replay["entries"][0]["data"]["kind"], "agent_start")

    async def test_stream_sink_and_broker_preserve_the_ui_sse_contract(self) -> None:
        broker = StreamBroker(self.store)
        sink = RedisStreamSink(self.store)
        cursor = await broker.cursor("chat-3")
        await sink.emit(
            StreamEvent(
                stream_id="chat-3",
                tool_name="agent",
                step="llm",
                kind="agent_text_delta",
                payload={"text": "streamed"},
                sequence=1,
            )
        )
        await broker.append_event(
            "chat-3",
            "turn_settled",
            {"result": {"revision": 1}},
            idempotency_key="turn-1",
        )

        chunks = []
        async for chunk in broker.turn_event_stream(
            "chat-3",
            _ConnectedRequest(),  # type: ignore[arg-type]
            cursor=cursor,
        ):
            chunks.append(chunk)

        self.assertEqual([chunk["event"] for chunk in chunks], ["stream", "turn_settled"])
        self.assertIn('"kind": "agent_text_delta"', chunks[0]["data"])
        self.assertIn('"cursor":', chunks[1]["data"])

    async def test_clear_invalidates_a_previous_cursor(self) -> None:
        cursor = await self.store.append_event("chat-4", "stream", {"kind": "start"})
        await self.store.clear("chat-4")

        replay = await self.store.replay_entries(
            "chat-4",
            cursor=cursor,
            limit=100,
        )
        self.assertFalse(replay["replay_available"])
        self.assertEqual(replay["cursor"], ZERO_STREAM_ID)
