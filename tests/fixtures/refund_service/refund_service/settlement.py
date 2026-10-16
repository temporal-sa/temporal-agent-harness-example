"""Settle refunds with the payment processor and reconcile the totals."""

import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field

from .models import Refund


class SettlementTimeout(Exception):
    pass


class SandboxGateway:
    """The processor's sandbox: a refund settles a fixed time after it is submitted."""

    def __init__(
        self,
        latency_seconds: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._latency = latency_seconds
        self._clock = clock
        self._submitted: dict[str, tuple[float, int]] = {}

    def submit(self, refund: Refund) -> str:
        ref = f"gw_{refund.refund_id}"
        self._submitted.setdefault(ref, (self._clock(), refund.amount_cents))
        return ref

    def status(self, ref: str) -> str:
        submitted_at, _ = self._submitted[ref]
        return "settled" if self._clock() - submitted_at >= self._latency else "pending"

    def settled_amount(self, ref: str) -> int:
        return self._submitted[ref][1]


@dataclass
class SettlementReport:
    batches: int = 0
    settled: list[str] = field(default_factory=list)
    settled_cents: int = 0


def settle(
    refunds: Iterable[Refund],
    gateway: SandboxGateway,
    *,
    batch_size: int = 25,
    poll_interval: float = 0.1,
    batch_timeout: float = 60.0,
    on_batch: Callable[[int, int], None] | None = None,
) -> SettlementReport:
    """Submit refunds in batches and wait for each batch to settle before the next."""
    pending = list(refunds)
    report = SettlementReport()
    for start in range(0, len(pending), batch_size):
        batch = pending[start : start + batch_size]
        refs = [gateway.submit(refund) for refund in batch]
        deadline = time.monotonic() + batch_timeout
        while any(gateway.status(ref) != "settled" for ref in refs):
            if time.monotonic() > deadline:
                raise SettlementTimeout(f"batch starting at {start} did not settle")
            time.sleep(poll_interval)
        report.batches += 1
        report.settled.extend(refs)
        report.settled_cents += sum(gateway.settled_amount(ref) for ref in refs)
        if on_batch is not None:
            on_batch(report.batches, len(report.settled))
    return report
