PM:

# P3 report: the product view (UI)

- **Date:** 2026-10-16
- **Branch:** `refactor/official-harness` (local). 46 commits, f297edf..07fc450.
- **Status: every validation step passes. The §2.5 demo script passed every beat, twice in a row.**
- **Handoff:** `docs/phases/P3-handoff.md`. **Wireframes (owner-approved):** `docs/phases/P3-wireframes.html`.

## Where it stands

| Step | Check | Result | Evidence |
|---|---|---|---|
| 3a | Layout at 1920 / 1366 / 320 | **Pass** | Mocked e2e at three sizes: no sideways scroll at 320, tap targets ≥ 44 px, the decision card, strip and composer in the viewport, nothing overlapping (`elementFromPoint` checks). |
| 3b | Accepted within 100 ms | **Pass** | Send, Approve, Approve all, model pick and posture pick: 15–30 ms, both with the API answering at once and with it answering after 1 s. |
| 3c | No Queries from the browser | **Pass** | Every e2e test checks requests against an allowlist. The session list rebuild was moved off Queries onto Visibility. |
| 3d | Workers down | **Pass** | Live: a reload keeps the transcript and the approval card; a message shows "In Temporal · waiting for a worker" and applies once. |
| 3e | Stacked approvals | **Pass** | Live: five `gated_echo` calls → one card, the identical pair folded "× 2", one answered alone, Approve all for the rest; each ran exactly once (distinct activity ids, attempt 1). Mocked: buttons never move (answer, arrival, opening groups, decline form). |
| 3f | Recovery | **Pass** | Live SIGKILL during `pytest`: banner in ~7 s → divider → confirmed numbers → reply once. Passed in 3 of 4 runs; the failure was the model dropping a suffix from the test's check word, not the product (test fixed in 07fc450). |
| 3g | Meters | **Pass** | Equal to confirmed `x_usage`; "not reported" and "nothing read from cache yet" rather than 0. |
| 3h | Isolation | **Pass** | Live, two users: no shared sessions in the list, a clean "No such session" page by URL, no attach attempt. |
| 3i | Demo script ×2 | **Pass** | All 8 beats in both runs, no soft observations (`web/e2e/.demo-runs/`, screenshots per beat). About 125k tokens in and 5k out per run. |
| 3j | Copy | **Pass** | Two independent UX reviews against guide 09. "Worker" capitalized (PM call); error copy says what happened and what Temporal does next. |
| 3k | Accessibility | **Pass** | Axe clean on four screens; keyboard reach with visible focus; Esc scoped; real modal dialogs; no chatty live regions; pill contrast ≥ 4.7:1. |

## What was built

- **The app:** `simple_chat_agent/web/` (React 19, TypeScript strict, Vite), served at `/app/` with a strict CSP. Harness SDK 0.1.0 is used only behind the `useCodingSession` facade; types are generated from the agent's schema.
- **The transcript:**
  - thinking, tool rows with badges, inline diffs (pink deletions, never red);
  - explorers side by side, Reviewer cards;
  - judge cards that say who approved;
  - "Attempt 1 (Worker lost)" folding, a recovery divider placed where the Worker was lost, and auto-follow.
- **The workbench:** Plan (ticks live), Changes (ignored files filtered out), Terminal (stdout and stderr tagged live, no duplicate lines after a reattach, sandbox-down notice), and Artifacts (sandboxed).
- **Decisions:**
  - **Approvals:** owner decisions A and B are both built: Approve / Always approve (file edits only) / Decline with a reason, and stacked approvals.
  - **Postures:** owner decision C, the posture menu with Clear.
  - **Auto mode:** calls the judge is still checking show as a quiet line, and a card appears only if the judge escalates.
- **Recovery and meters:**
  - the pinned "Worker isn't answering" banner, with a deep link to the pending Activity's run;
  - token, cache and recoveries meters, and the "Work kept after Worker loss" hero.

## Backend changes P3 needed

| Change | Who |
|---|---|
| `x_rejected` frames reach the browser; every `x_*` frame names its agent; run ids for deep links | Eng G |
| Subagent stream "unavailable" fixed: subagents drain their readers before completing, plus a fix for the harness merge (`harness_ext.install_merge_fixes`, to file upstream) | Eng G |
| `forget_always_allowed`, chat attachments as data, `SettingsAck` so the session list shows applied settings, `judge_status` | Eng G |
| Agent behaviour: plan first, 2 explorers, Reviewer for reviews, `git_commit` only, plus a per-request plan reminder (measured: plan 5/5, ticked 5/5 (was 0/21), explorers 5/5, Reviewer 5/5, `git_commit` 5/5) | Eng G |
| Live output tagged stdout/stderr with per-chunk seq; `write_file` overwrite diffs; `workspace.branch`; sandbox-down notices | Eng A |
| `/app/` mount and CSP; config (postures, auto mode, links, risk classes, TTL); posture/model/attachment/request-id validation; Query-free session rebuild; local-only `test_tools` | Eng F |

## Review

1. **UX review:** 3 blockers (Esc stopped the turn from anywhere, phone layout hid the card, banner scrolled away), 9 majors, 15 minors. Fixed, then **verified by a second review**, which found 2 more majors: the card and its slot both scrolling, and auto-follow lagging. Both fixed, with bounding-box e2e tests.
2. **Security and durability review:** no blockers and no security holes. One demo-visible minor (duplicate Terminal lines after a reattach) and seven hardening minors. All fixed except #8 (deferred).
3. **Live demo runs** surfaced three agent-behaviour gaps (no plan or explorers, no Reviewer, shell commits) and a UI gap (pink cards while the judge was still checking). All fixed; the reruns are clean.

## Totals

| Suite | Result |
|---|---|
| Python unit | 781 pass (51 live-only skips) |
| Integration P0–P2 | 29 of 29 pass. A first run failed 5 because the shared test user hit its 5M-token daily spend cap after a day of real-model testing; that is the guard working. The rerun with a higher cap passes. |
| npm client 0.1.0 contract | Pass |
| Web unit | 208 pass |
| Playwright, mocked | 78 pass (1 skipped by design) |
| Playwright, live | 4 pass (3f flaked once on the model's wording; fixed) |
| Demo script | 2 of 2 runs, all beats |

## Deferred

| Item | Where it goes |
|---|---|
| A deleted session can reappear after a session-list rebuild if Redis is lost while workers are down | P5 |
| A file tool's "not found" reaches the model as "Activity task failed" instead of the reason (seen once, when the model read a file before its approved write landed) | P5 |
| The JS bundle is about 470 kB (Vite warns above 500 kB); code-split later | P5 |
| `PYTHONDONTWRITEBYTECODE` in the executor environment (optional; Changes already filters ignored files) | P5 |
| A session-list rebuild loses renames (falls back to the start-time title) | Documented |
| Upstream issues: the harness merge drop (patched in `harness_ext`); the harness keeps a tool `evaluating` after an escalation | Upstream list |
| The old UI stays at `/`; swapping `/app/` to `/` and deleting the old frontend and `agent_harness/` | P5 |

## Next

P4: demo extras, which are MCP, Code Mode, browser callbacks (`ask_user`, `pick_files`), provider web search as a sources list, and the `basic_file_agent` rewrite. **Don't start without the owner's go-ahead.**
