from __future__ import annotations

import asyncio
import contextlib
import importlib
import io
import os
import signal
import unittest
from unittest.mock import patch

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


class WorkerEntrypointTests(unittest.IsolatedAsyncioTestCase):
    """python -m simple_chat_agent.worker.main runs the coding agent's worker (P5) and keeps the
    old entrypoint's drain: SIGTERM drains the worker, and the codec server stops with it."""

    def setUp(self) -> None:
        os.environ.setdefault("ANTHROPIC_API_KEY", "unit-test-not-used")

    @contextlib.contextmanager
    def patched(self, worker: _FakeWorker, codec: _FakeCodecServer | None):
        from simple_chat_agent.agent import worker as agent_worker

        async def connect() -> object:
            return type("Client", (), {"data_converter": None})()

        with contextlib.ExitStack() as stack:
            stack.enter_context(patch.object(agent_worker, "connect", connect))
            stack.enter_context(patch.object(agent_worker, "build_worker", lambda _c, _q: worker))
            stack.enter_context(patch.object(agent_worker, "deployment_config", lambda _q: None))
            stack.enter_context(patch.object(agent_worker, "codec_server_enabled", lambda: codec is not None))
            stack.enter_context(patch.object(agent_worker, "create_codec_app", lambda _dc: object()))
            stack.enter_context(patch.object(agent_worker.uvicorn, "Server", lambda _cfg: codec))
            stack.enter_context(patch.object(agent_worker.uvicorn, "Config", lambda *a, **k: None))
            stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
            yield

    async def test_entrypoint_is_the_coding_agent_worker(self) -> None:
        entry = importlib.import_module("simple_chat_agent.worker.main")

        worker = _FakeWorker(return_immediately=True)
        with self.patched(worker, None):
            await entry.main()
        self.assertTrue(worker.shutdown_called)

    async def test_sigterm_drains_worker_and_stops_codec_server(self) -> None:
        entry = importlib.import_module("simple_chat_agent.worker.main")

        worker = _FakeWorker()
        codec = _FakeCodecServer()
        asyncio.get_running_loop().call_later(0.05, os.kill, os.getpid(), signal.SIGTERM)
        with self.patched(worker, codec):
            await asyncio.wait_for(entry.main(), timeout=10)
        self.assertTrue(worker.shutdown_called)
        self.assertFalse(worker.run_cancelled)
        self.assertTrue(codec.should_exit)

    async def test_worker_exit_stops_codec_server(self) -> None:
        entry = importlib.import_module("simple_chat_agent.worker.main")

        worker = _FakeWorker(return_immediately=True)
        codec = _FakeCodecServer()
        with self.patched(worker, codec):
            await asyncio.wait_for(entry.main(), timeout=10)
        self.assertTrue(codec.should_exit)

    def test_help_needs_no_keys_or_server(self) -> None:
        entry = importlib.import_module("simple_chat_agent.worker.main")

        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            entry.run(["--help"])
        self.assertIn("TEMPORAL_WORKER_BUILD_ID", out.getvalue())
        self.assertIn("CODING_AGENT_TASK_QUEUE", out.getvalue())


if __name__ == "__main__":
    unittest.main()
