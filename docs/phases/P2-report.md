PM:

# P2 report: API and streaming

- **Date:** 2026-10-16
- **Branch:** `refactor/official-harness` (local)
- **Status: every validation step passes.**
- **Handoff:** `docs/phases/P2-handoff.md`

## Where it stands

| Step | Check | Result | Evidence |
|---|---|---|---|
| 2.1a | Harness client contract on the real API | **Pass** | `tests/contract/run_contract.py --app real`: unpatched `@temporalio/agent-harness-client` 0.1.0 runs login → `createSession`/`listSessions` through `HttpTransport` → attach → approve → queued sends, and checks the 401/404/422/403 codes. |
| 2.1b | Tenant isolation | **Pass** | `test_p2_real_app`: user B gets 404 on 13 agent and console routes, including logs and agent-interface, and A's sessions never appear in B's lists. Unit coverage spans A vs B, root vs subagent and unknown ids, forged `files/view`, and the Describe fallback. |
| 2.1c | Demo seed | **Pass** | `test_p2_real_app`: the refund repo (13 files plus the initial commit) is in the workspace before the first message. Retries never re-seed. |
| 2.2a | One Temporal reader | **Pass** | Three replicas with readers on one session: one forwarder, peak 1 concurrent poll, 3 polls per turn (same as one reader). |
| 2.2b | Workers down | **Pass** | `test_p2_real_app`, real app with the worker killed: transcript replays from Redis, `status` answers in under 200 ms, a message lands in 0.01 s and applies exactly once when the worker returns. |
| 2.2c | Lease takeover | **Pass** | Forwarding replica SIGKILLed mid-turn. Takeover in 12.3–12.9 s; 200 of 200 frames stored, no gaps or duplicates. |
| 2.2d | Recovery telemetry | **Pass** | Worker SIGKILLed mid-stream. Live `x_recovery` arrives at 12.6 s; confirmed 0.04–0.11 s after attempt 2 completes. Kept 1 model + 1 tool call (4,510 in / 52 out tokens) and repeated 1 model call, both matching history. Lost on worker-a, resumed on worker-b. |
| 2.2e | Usage telemetry | **Pass** | Confirmed `x_usage` per-turn sums equal history exactly, including Anthropic cache writes (turn 1 wrote 4,363 / turn 2 read 4,363). Live frames show cache writes as `null` until confirmed. |
| 2.2f | Ack-bounded truncation | **Pass** | `test_p2_lead`: after continue-as-new the log starts exactly at the acknowledged offset. The ack is the minimum of what is stored and what is archived. |
| 2.3 | Session manager | **Pass** | `CodingSessions` keeps its list and titles across continue-as-new. Changes are Signals (signal-with-start), so they land with workers down. Sessions are never its children. |
| 2.4 | Console | **Pass** | Unchanged harness console at `/console/?s=<id>`, served through owner-scoped routes. No CORS. Off in the catalog unless enabled. |

## Totals
- 672 unit tests pass (47 skipped are live-only).
- 25 integration tests across P0, P1 and P2 pass in one run.
- The npm client contract check passes, in real-app and dev-app modes.

## Review
1. **Deep review.** It found no cross-user path. It found one blocker, five majors and 16 minors:
   - **B1:** confirmed telemetry never reached a live tab.
   - **M1:** archive backfill stopped at 500 frames.
   - **M2:** the session list expired.
   - **M3:** idle forwarders issued Queries for 10 minutes.
   - **M4:** retries re-seeded the workspace.
   - **M5:** the ack was not gated on the archive.
   - **Minors:** CSRF, subagent routing, honesty of the recovery numbers, and others.
2. **Fix pass.**
3. **Verification.** B1 and M1–M5 are confirmed fixed. Follow-ups:
   - **N1 (honesty):** a provider error retried on another replica must not count as a "worker lost" recovery.
   - **Other:** archive polling cost, rebuild lockout, concurrent create seeding, approval retry idempotency, chunked upload size, `Origin: null`.
   - All are fixed in the follow-up commit with regression tests.

## Deferred

| Item | Where it goes |
|---|---|
| S3 lifecycle rule for the turn archive (documented; set it to at least the 15-day session TTL) | Release checklist (catalog infra) |
| Work started outside our API (Temporal CLI or UI Updates) isn't noticed until the next send or attach | Documented |
| `closed` is sticky after an operator reset | Documented |

## Next
P3, the product view. It starts with wireframes for your review (U0) before any UI code. **Don't start without the owner's go-ahead.**
