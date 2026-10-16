from __future__ import annotations

import os
from datetime import timedelta

WORKER_GRACEFUL_SHUTDOWN_SECONDS_ENV = "SIMPLE_CHAT_WORKER_GRACEFUL_SHUTDOWN_SECONDS"
DEFAULT_WORKER_GRACEFUL_SHUTDOWN_SECONDS = 20 * 60
KUBERNETES_SHUTDOWN_BUFFER_SECONDS = 60


def worker_graceful_shutdown_seconds() -> int:
    raw = os.environ.get(WORKER_GRACEFUL_SHUTDOWN_SECONDS_ENV)
    if raw is None:
        return DEFAULT_WORKER_GRACEFUL_SHUTDOWN_SECONDS

    try:
        seconds = int(raw)
    except ValueError as err:
        raise RuntimeError(
            f"{WORKER_GRACEFUL_SHUTDOWN_SECONDS_ENV} must be a whole number"
        ) from err
    if seconds < 0:
        raise RuntimeError(
            f"{WORKER_GRACEFUL_SHUTDOWN_SECONDS_ENV} must not be negative"
        )
    return seconds


def worker_graceful_shutdown_timeout() -> timedelta:
    return timedelta(seconds=worker_graceful_shutdown_seconds())


def kubernetes_termination_grace_period_seconds() -> int:
    return worker_graceful_shutdown_seconds() + KUBERNETES_SHUTDOWN_BUFFER_SECONDS
