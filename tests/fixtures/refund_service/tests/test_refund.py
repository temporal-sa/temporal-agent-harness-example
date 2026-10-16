import pytest

from refund_service import OrderNotFound, OrderStatus, RefundError


def test_order_totals(order):
    assert order.subtotal_cents == 9_899
    assert order.tax_cents == 817
    assert order.total_cents == 11_216
    assert order.balance_cents == 11_216


def test_full_refund_of_paid_order(service, order):
    refund = service.refund(order.order_id)

    assert refund.amount_cents == 11_216
    assert refund.refund_id == "rf_ord_1001_1"
    assert order.status is OrderStatus.REFUNDED
    assert order.balance_cents == 0


def test_partial_refund(service, order):
    refund = service.refund(order.order_id, amount_cents=2_500, reason="late_delivery")

    assert refund.amount_cents == 2_500
    assert refund.reason == "late_delivery"
    assert order.status is OrderStatus.PARTIALLY_REFUNDED
    assert order.balance_cents == 8_716


def test_refund_rest_of_partially_refunded_order(service, order):
    service.refund(order.order_id, amount_cents=2_500, reason="late_delivery")

    refund = service.refund(order.order_id)

    assert refund.amount_cents == 8_716
    assert order.refunded_cents == order.total_cents
    assert order.status is OrderStatus.REFUNDED


def test_refund_more_than_balance_is_rejected(service, order):
    with pytest.raises(RefundError, match="exceeds the remaining balance"):
        service.refund(order.order_id, amount_cents=11_217)
    assert order.refunds == []


@pytest.mark.parametrize("amount", [0, -100])
def test_refund_amount_must_be_positive(service, order, amount):
    with pytest.raises(RefundError, match="positive"):
        service.refund(order.order_id, amount_cents=amount)


def test_fully_refunded_order_cannot_be_refunded_again(service, order):
    service.refund(order.order_id)

    with pytest.raises(RefundError, match="already fully refunded"):
        service.refund(order.order_id, amount_cents=1)


def test_cancelled_order_cannot_be_refunded(service, order):
    service.cancel(order.order_id)

    with pytest.raises(RefundError, match="cancelled"):
        service.refund(order.order_id)


def test_order_with_refunds_cannot_be_cancelled(service, order):
    service.refund(order.order_id, amount_cents=100)

    with pytest.raises(RefundError, match="can't be cancelled"):
        service.cancel(order.order_id)


def test_unknown_order(service):
    with pytest.raises(OrderNotFound):
        service.refund("ord_missing")


def test_item_refund_includes_its_share_of_tax(service, order):
    refund = service.refund_items(order.order_id, "CB-USB-C", quantity=1)

    # 9.50 of goods plus 817 * 950 / 9899 = 78.4 cents of tax.
    assert refund.amount_cents == 1_028
    assert refund.sku == "CB-USB-C"
    assert refund.quantity == 1
    assert order.status is OrderStatus.PARTIALLY_REFUNDED


def test_item_refund_cannot_exceed_units_bought(service, order):
    service.refund_items(order.order_id, "CB-USB-C", quantity=2)

    with pytest.raises(RefundError, match="Only 0 unit"):
        service.refund_items(order.order_id, "CB-USB-C", quantity=1)


def test_item_refund_unknown_sku(service, order):
    with pytest.raises(RefundError, match="no item"):
        service.refund_items(order.order_id, "NOPE")
