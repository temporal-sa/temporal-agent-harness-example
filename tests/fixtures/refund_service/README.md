# refund_service (demo fixture)

A small storefront refund service used as the seeded repo for the coding-agent demo
(PRD §2.5, P1 step 1.11). Standard library only; tests need `pytest`.

```
pip install -r requirements.txt
pytest                      # ~20 s: the settlement test waits on a sandbox gateway
REFUND_SERVICE_SETTLEMENT_SECONDS=2 pytest   # quick run
```

## The planted bug (for the demo team, not for the agent)

`RefundService.refund()` in `refund_service/refunds.py`, when called without an amount
("refund whatever is left"), uses the order's **original total** (`order.total_cents`)
instead of the **remaining balance** (`order.balance_cents`). On a fresh order the two are
equal, so it works. On an order that was already partially refunded, the default amount is
too large and the balance check rejects it with
`RefundError: Refund of 112.16 USD exceeds the remaining balance of 87.16 USD.`

- Failing test: `tests/test_refund.py::test_refund_rest_of_partially_refunded_order`.
- All other tests pass.
- Fix: one line, `amount = order.balance_cents if amount_cents is None else amount_cents`.

This README is not seeded into the agent's workspace (the loader skips it), so the agent
has to find the cause itself. Nothing in the code comments points at it.

## Slow test

`tests/test_settlement.py::test_nightly_settlement_reconciles_with_orders` settles 40
refunds in 10 batches against `SandboxGateway`. It takes
`REFUND_SERVICE_SETTLEMENT_SECONDS` (default 20) so the demo can crash a worker mid-run.
