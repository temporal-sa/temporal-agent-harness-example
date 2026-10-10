from __future__ import annotations

import asyncio
import os
import unittest
from unittest.mock import patch

from simple_chat_agent.worker.main import (
    _run_worker_and_codec_server,
    _run_worker_until_stopped,
)
from simple_chat_agent.worker.shutdown import (
    DEFAULT_WORKER_GRACEFUL_SHUTDOWN_SECONDS,
    WORKER_GRACEFUL_SHUTDOWN_SECONDS_ENV,
    kubernetes_termination_grace_period_seconds,
    worker_graceful_shutdown_seconds,
    worker_graceful_shutdown_timeout,
)


class _FakeWorker:
    def __init__(self, *, return_immediately: bool = False) -> None:
        self.return_immediately = return_immediately
        self.shutdown_called = False
        self.run_cancelled = False
        self._stopped = asyncio.Event()

    async def run(self) -> None:
        if self.return_immediately:
            return
        try:
            await self._stopped.wait()
        except asyncio.CancelledError:
            self.run_cancelled = True
            raise

    async def shutdown(self) -> None:
        self.shutdown_called = True
        self._stopped.set()


class _FakeCodecServer:
    def __init__(self, *, return_immediately: bool = False) -> None:
        self.return_immediately = return_immediately
        self.should_exit = False

    async def serve(self) -> None:
        if self.return_immediately:
            return
        while not self.should_exit:
            await asyncio.sleep(0)


class WorkerShutdownConfigurationTests(unittest.TestCase):
    def test_default_matches_longest_provider_activity_timeout(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(
                worker_graceful_shutdown_seconds(),
                DEFAULT_WORKER_GRACEFUL_SHUTDOWN_SECONDS,
            )
            self.assertEqual(
                worker_graceful_shutdown_timeout().total_seconds(),
                20 * 60,
            )
            self.assertEqual(
                kubernetes_termination_grace_period_seconds(),
                21 * 60,
            )

    def test_rejects_invalid_timeout(self) -> None:
        for value in ("invalid", "-1"):
            with (
                self.subTest(value=value),
                patch.dict(
                    os.environ,
                    {WORKER_GRACEFUL_SHUTDOWN_SECONDS_ENV: value},
                    clear=True,
                ),
                self.assertRaises(RuntimeError),
            ):
                worker_graceful_shutdown_seconds()


class WorkerServiceSupervisorTests(unittest.IsolatedAsyncioTestCase):
    async def test_shutdown_signal_drains_worker_without_codec_server(self) -> None:
        worker = _FakeWorker()

        def request_shutdown(
            _loop: asyncio.AbstractEventLoop,
            shutdown_requested: asyncio.Event,
        ) -> list[object]:
            shutdown_requested.set()
            return []

        with patch(
            "simple_chat_agent.worker.main._register_shutdown_signal_handlers",
            side_effect=request_shutdown,
        ):
            await _run_worker_until_stopped(worker)  # type: ignore[arg-type]

        self.assertTrue(worker.shutdown_called)
        self.assertFalse(worker.run_cancelled)

    async def test_codec_exit_drains_worker_without_cancelling_it(self) -> None:
        worker = _FakeWorker()
        codec_server = _FakeCodecServer(return_immediately=True)
        with patch(
            "simple_chat_agent.worker.main._register_shutdown_signal_handlers",
            return_value=[],
        ):
            await _run_worker_and_codec_server(worker, codec_server)  # type: ignore[arg-type]

        self.assertTrue(worker.shutdown_called)
        self.assertFalse(worker.run_cancelled)

    async def test_worker_exit_stops_codec_server(self) -> None:
        worker = _FakeWorker(return_immediately=True)
        codec_server = _FakeCodecServer()
        with patch(
            "simple_chat_agent.worker.main._register_shutdown_signal_handlers",
            return_value=[],
        ):
            await _run_worker_and_codec_server(worker, codec_server)  # type: ignore[arg-type]

        self.assertTrue(codec_server.should_exit)
