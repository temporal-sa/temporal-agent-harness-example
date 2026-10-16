from datetime import UTC, datetime

import pytest

from refund_service import InMemoryOrderStore, LineItem, Order, RefundService

FIXED_NOW = datetime(2026, 3, 14, 9, 30, tzinfo=UTC)


@pytest.fixture
def store() -> InMemoryOrderStore:
    return InMemoryOrderStore()


@pytest.fixture
def service(store: InMemoryOrderStore) -> RefundService:
    return RefundService(store, clock=lambda: FIXED_NOW)


@pytest.fixture
def order(store: InMemoryOrderStore) -> Order:
    # Subtotal 98.99, tax 8.25% = 8.17, shipping 5.00, total 112.16.
    return store.add(
        Order(
            order_id="ord_1001",
            customer_id="cus_42",
            items=[
                LineItem("HP-200", "Wireless headphones", 7_999),
                LineItem("CB-USB-C", "USB-C cable", 950, quantity=2),
            ],
            tax_rate_bp=825,
            shipping_cents=500,
        )
    )
