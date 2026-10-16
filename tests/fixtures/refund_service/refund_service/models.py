"""Orders, line items and refunds."""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

from .money import apply_rate


class OrderStatus(str, Enum):
    PAID = "paid"
    PARTIALLY_REFUNDED = "partially_refunded"
    REFUNDED = "refunded"
    CANCELLED = "cancelled"


@dataclass(frozen=True)
class LineItem:
    sku: str
    description: str
    unit_price_cents: int
    quantity: int = 1

    @property
    def total_cents(self) -> int:
        return self.unit_price_cents * self.quantity


@dataclass(frozen=True)
class Refund:
    refund_id: str
    order_id: str
    amount_cents: int
    reason: str
    created_at: datetime
    sku: str | None = None
    quantity: int = 0


@dataclass
class Order:
    order_id: str
    customer_id: str
    items: list[LineItem]
    tax_rate_bp: int = 0
    shipping_cents: int = 0
    status: OrderStatus = OrderStatus.PAID
    refunds: list[Refund] = field(default_factory=list)

    @property
    def subtotal_cents(self) -> int:
        return sum(item.total_cents for item in self.items)

    @property
    def tax_cents(self) -> int:
        return apply_rate(self.subtotal_cents, self.tax_rate_bp)

    @property
    def total_cents(self) -> int:
        return self.subtotal_cents + self.tax_cents + self.shipping_cents

    @property
    def refunded_cents(self) -> int:
        return sum(refund.amount_cents for refund in self.refunds)

    @property
    def balance_cents(self) -> int:
        return self.total_cents - self.refunded_cents

    def item(self, sku: str) -> LineItem:
        for item in self.items:
            if item.sku == sku:
                return item
        raise KeyError(sku)

    def refunded_quantity(self, sku: str) -> int:
        return sum(r.quantity for r in self.refunds if r.sku == sku)
