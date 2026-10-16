"""An in-memory order store. Production uses Postgres behind the same interface."""

from threading import Lock

from .models import Order


class OrderNotFound(KeyError):
    pass


class InMemoryOrderStore:
    def __init__(self) -> None:
        self._orders: dict[str, Order] = {}
        self._lock = Lock()

    def add(self, order: Order) -> Order:
        with self._lock:
            if order.order_id in self._orders:
                raise ValueError(f"duplicate order {order.order_id}")
            self._orders[order.order_id] = order
        return order

    def get(self, order_id: str) -> Order:
        with self._lock:
            try:
                return self._orders[order_id]
            except KeyError:
                raise OrderNotFound(order_id) from None

    def save(self, order: Order) -> None:
        with self._lock:
            self._orders[order.order_id] = order

    def all(self) -> list[Order]:
        with self._lock:
            return list(self._orders.values())
