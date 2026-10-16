import os
import time

import pytest

from refund_service import InMemoryOrderStore, LineItem, Order, RefundService
from refund_service.settlement import SandboxGateway, SettlementTimeout, settle

# How long the end-to-end settlement run takes against the sandbox gateway.
SETTLEMENT_SECONDS = float(os.environ.get("REFUND_SERVICE_SETTLEMENT_SECONDS", "20"))
BATCHES = 10


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def test_gateway_settles_after_latency(order, service):
    clock = FakeClock()
    gateway = SandboxGateway(latency_seconds=5, clock=clock)
    ref = gateway.submit(service.refund(order.order_id, amount_cents=300))

    assert gateway.status(ref) == "pending"
    clock.now = 5
    assert gateway.status(ref) == "settled"
    assert gateway.settled_amount(ref) == 300


def test_resubmitting_a_refund_is_idempotent(order, service):
    clock = FakeClock()
    gateway = SandboxGateway(latency_seconds=5, clock=clock)
    refund = service.refund(order.order_id, amount_cents=300)
    first = gateway.submit(refund)
    clock.now = 4
    assert gateway.submit(refund) == first
    clock.now = 5
    assert gateway.status(first) == "settled"


def test_batch_that_never_settles_times_out(order, service):
    gateway = SandboxGateway(latency_seconds=3_600)
    refund = service.refund(order.order_id, amount_cents=300)

    with pytest.raises(SettlementTimeout):
        settle([refund], gateway, poll_interval=0.01, batch_timeout=0.05)


def _orders(store: InMemoryOrderStore, count: int) -> list[Order]:
    orders = []
    for n in range(count):
        orders.append(
            store.add(
                Order(
                    order_id=f"ord_{2000 + n}",
                    customer_id=f"cus_{n % 7}",
                    items=[
                        LineItem("TS-01", "T-shirt", 1_999 + n, quantity=1 + n % 3),
                        LineItem("SK-02", "Socks", 799, quantity=2),
                    ],
                    tax_rate_bp=(0, 600, 825, 1_000)[n % 4],
                    shipping_cents=(0, 499)[n % 2],
                )
            )
        )
    return orders


def test_nightly_settlement_reconciles_with_orders():
    store = InMemoryOrderStore()
    service = RefundService(store)
    refunds = []
    for n, order in enumerate(_orders(store, 40)):
        if n % 3 == 0:
            refunds.append(service.refund(order.order_id))
        elif n % 3 == 1:
            refunds.append(service.refund(order.order_id, amount_cents=250 + n))
        else:
            refunds.append(service.refund_items(order.order_id, "SK-02", quantity=1))

    gateway = SandboxGateway(latency_seconds=SETTLEMENT_SECONDS / BATCHES)
    started = time.monotonic()
    report = settle(
        refunds,
        gateway,
        batch_size=len(refunds) // BATCHES,
        poll_interval=0.05,
        batch_timeout=SETTLEMENT_SECONDS + 30,
        on_batch=lambda batch, done: print(f"settled batch {batch}/{BATCHES} ({done} refunds)", flush=True),
    )

    assert report.batches == BATCHES
    assert len(report.settled) == len(refunds)
    assert report.settled_cents == sum(o.refunded_cents for o in store.all())
    assert all(o.balance_cents >= 0 for o in store.all())
    assert time.monotonic() - started >= SETTLEMENT_SECONDS * 0.9
