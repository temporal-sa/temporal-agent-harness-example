"""Refund rules for paid orders."""

from collections.abc import Callable
from datetime import UTC, datetime

from .models import Order, OrderStatus, Refund
from .money import format_cents, prorate
from .store import InMemoryOrderStore


class RefundError(Exception):
    pass


class RefundService:
    def __init__(
        self,
        store: InMemoryOrderStore,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._store = store
        self._clock = clock

    def refund(
        self,
        order_id: str,
        amount_cents: int | None = None,
        reason: str = "requested_by_customer",
    ) -> Refund:
        """Refund part of an order, or everything still refundable when no amount is given."""
        order = self._store.get(order_id)
        self._check_refundable(order)
        amount = order.total_cents if amount_cents is None else amount_cents
        if amount <= 0:
            raise RefundError("Refund amount must be positive.")
        if amount > order.balance_cents:
            raise RefundError(
                f"Refund of {format_cents(amount)} exceeds the remaining balance of "
                f"{format_cents(order.balance_cents)}."
            )
        return self._record(order, amount, reason)

    def refund_items(
        self,
        order_id: str,
        sku: str,
        quantity: int = 1,
        reason: str = "returned",
    ) -> Refund:
        """Refund returned units of one line item, with their share of the tax."""
        order = self._store.get(order_id)
        self._check_refundable(order)
        try:
            item = order.item(sku)
        except KeyError:
            raise RefundError(f"Order {order_id} has no item {sku}.") from None
        if quantity <= 0:
            raise RefundError("Quantity must be positive.")
        remaining_units = item.quantity - order.refunded_quantity(sku)
        if quantity > remaining_units:
            raise RefundError(
                f"Only {remaining_units} unit(s) of {sku} are left to refund."
            )
        goods = item.unit_price_cents * quantity
        tax = prorate(order.tax_cents, goods, order.subtotal_cents)
        amount = min(goods + tax, order.balance_cents)
        return self._record(order, amount, reason, sku=sku, quantity=quantity)

    def cancel(self, order_id: str) -> Order:
        order = self._store.get(order_id)
        if order.refunds:
            raise RefundError("An order with refunds can't be cancelled; refund it instead.")
        if order.status is not OrderStatus.PAID:
            raise RefundError(f"Order {order_id} is {order.status.value}.")
        order.status = OrderStatus.CANCELLED
        self._store.save(order)
        return order

    def _check_refundable(self, order: Order) -> None:
        if order.status is OrderStatus.CANCELLED:
            raise RefundError(f"Order {order.order_id} was cancelled.")
        if order.status is OrderStatus.REFUNDED or order.balance_cents <= 0:
            raise RefundError(f"Order {order.order_id} is already fully refunded.")

    def _record(
        self,
        order: Order,
        amount: int,
        reason: str,
        *,
        sku: str | None = None,
        quantity: int = 0,
    ) -> Refund:
        refund = Refund(
            refund_id=f"rf_{order.order_id}_{len(order.refunds) + 1}",
            order_id=order.order_id,
            amount_cents=amount,
            reason=reason,
            created_at=self._clock(),
            sku=sku,
            quantity=quantity,
        )
        order.refunds.append(refund)
        order.status = (
            OrderStatus.REFUNDED if order.balance_cents == 0 else OrderStatus.PARTIALLY_REFUNDED
        )
        self._store.save(order)
        return refund
