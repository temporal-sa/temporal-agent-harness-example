"""Money helpers. All amounts are integer cents."""

from decimal import ROUND_HALF_UP, Decimal, InvalidOperation

_CENT = Decimal("0.01")


def to_cents(amount: str | int | Decimal) -> int:
    """Parse a decimal amount such as "19.99" into cents, rounding half up."""
    try:
        value = Decimal(str(amount))
    except InvalidOperation as err:
        raise ValueError(f"not a money amount: {amount!r}") from err
    return int((value.quantize(_CENT, rounding=ROUND_HALF_UP) * 100).to_integral_value())


def format_cents(cents: int, currency: str = "USD") -> str:
    sign = "-" if cents < 0 else ""
    whole, frac = divmod(abs(cents), 100)
    return f"{sign}{whole:,}.{frac:02d} {currency}"


def apply_rate(cents: int, basis_points: int) -> int:
    """Return cents * basis_points / 10_000, rounded half up to a whole cent."""
    value = Decimal(cents) * Decimal(basis_points) / Decimal(10_000)
    return int(value.quantize(Decimal(1), rounding=ROUND_HALF_UP))


def prorate(amount_cents: int, part: int, whole: int) -> int:
    """The share of amount_cents that part/whole represents, rounded half up."""
    if whole <= 0:
        raise ValueError("whole must be positive")
    if not 0 <= part <= whole:
        raise ValueError("part must be between 0 and whole")
    value = Decimal(amount_cents) * Decimal(part) / Decimal(whole)
    return int(value.quantize(Decimal(1), rounding=ROUND_HALF_UP))
