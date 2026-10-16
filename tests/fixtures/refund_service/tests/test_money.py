import pytest

from refund_service.money import apply_rate, format_cents, prorate, to_cents


@pytest.mark.parametrize(
    ("amount", "cents"),
    [("19.99", 1_999), ("0.005", 1), ("19.994", 1_999), ("19.995", 2_000), (7, 700)],
)
def test_to_cents_rounds_half_up(amount, cents):
    assert to_cents(amount) == cents


def test_to_cents_rejects_garbage():
    with pytest.raises(ValueError):
        to_cents("twelve dollars")


def test_format_cents():
    assert format_cents(123_456) == "1,234.56 USD"
    assert format_cents(-5) == "-0.05 USD"
    assert format_cents(0, "EUR") == "0.00 EUR"


def test_apply_rate():
    assert apply_rate(9_899, 825) == 817
    assert apply_rate(1_000, 0) == 0
    assert apply_rate(200, 25) == 1  # 0.5 cents rounds up


def test_prorate():
    assert prorate(817, 950, 9_899) == 78
    assert prorate(100, 1, 2) == 50
    assert prorate(817, 9_899, 9_899) == 817
    with pytest.raises(ValueError):
        prorate(100, 3, 2)
    with pytest.raises(ValueError):
        prorate(100, 0, 0)
