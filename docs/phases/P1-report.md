PM:

# P1 report: agent core

- **Date:** 2026-10-16
- **Branch:** `refactor/official-harness` (local). Commits `491df18` … final P1 commit.
- **Status: every validation step passes.**
- **Handoff:** `docs/phases/P1-handoff.md`

## Where it stands

| Step | Check | Result | Evidence |
|---|---|---|---|
| 1.1a | File tools pass the approval gate; edits fill the changed-files panel | **Pass** | `test_p1_agent.test_plan_and_changes_state` |
| 1.1b | `run_command` streams output; result bounded | **Pass** | `test_1_1b`: the first chunk arrived about 4 s before the command ended, and the workspace state shows exit 0. Sub-second delivery is not asserted (review test gap). |
| 1.1c | Worker killed mid-command: reattach, command runs once | **Pass** | `test_1_1c`: one `started` line in the workspace, and the full output (including "finished") streamed after reattach. |
| 1.2 | SSRF guard | **Pass** | 26 `net_guard` tests: blocked ranges, redirect hops, DNS rebinding, IP pin |
| 1.2b | Network tools ask under the default posture; the judge decides in auto mode | **Pass** | `test_1_2b` ×2. Locally the judge approved `fetch_url` once (85%) and escalated once (60% < 80% bar). |
| 1.4a | The judge fails closed | **Pass** | Workflow-level tests: timeout, malformed verdict, "APPROVED BY REVIEWER" injection, and spend cap or no account each escalate. |
| 1.4b | Pre-rules escalate without a judge call | **Pass** | 71 positive and 25 negative cases, including `.git*`, `.gitconfig`, `.gitattributes`, `.gitmodules` |
| 1.4c | "Always allow" ignored for shell, kept for edits | **Pass** | `test_1_4c` (Signal path). The Update path is unit-tested only. |
| 1.5 | Subagents: 2 in parallel, nested, none left running, no approval clicks | **Pass** | `test_two_explorers_and_none_left_running` under the default posture |
| 1.7 | Context stays ≤85% of the window; processor is pure | **Pass** | 150 randomized histories; 200-turn session, worst fill 0.8497. Live: compaction mid tool round gives no 400. |
| 1.8a | Budget guard triggers the checkpoint card | **Pass** | `test_1_8a` (forced low cap → checkpoint reply → continue-as-new) |
| 1.8b | Spend caps stop the turn with a card; fail closed | **Pass** | `test_1_8b`; Redis-down fail-closed is unit-tested. |
| 1.9 | Steer reaches the running turn | **Pass** | `test_steer_reaches_the_running_turn` |
| 1.10 | Production worker | **Pass** | `simple_chat_agent/agent/worker.py`: start and drain smoke test |
| 1.11 | Scripted task ≥4/5 per provider | **Pass: 15/15** | Claude Sonnet 5.5 5/5 (6–7 tool calls), GPT-5.5 5/5 (17–20), Gemini 3.5 Flash 5/5 (13–20). Every run fixed the bug, got the suite green and committed. |
| 1.12 | Longest turn < 10,240 events and 10 MB | **Pass** | 30-tool Sonnet turn: 945 events, 2.56 MB. Longest scripted run: 732 events, 1.73 MB. |
| 1.13 | Recorded histories replay | **Pass** | `tests/test_replay.py`: 3 histories (tool turn + approval + provider switch; interrupt during tool; continue-as-new) |

## Totals

- 500 unit tests pass (38 skipped are live-only).
- 18 P0 and P1 integration tests pass.
- The npm client contract check passes.

## Review

1. **Deep review** of the integrated P1 code found 1 blocker, 4 majors and 17 minors:
   - **B1:** the "safe" git tools could execute workspace-planted filters.
   - **M1:** the model never saw attachment content.
   - **M2:** executor cache-id collisions after continue-as-new.
   - **M3:** subagent tools needed approvals, plus a concurrency race.
   - **M4:** one session could starve all others.
2. **Fix pass** with regression tests for each.
3. **Verification review:** B1 was still open (filter names containing `=`, and commit-time auto-gc hooks). Three regressions came from the fixes: approval rules dropped across continue-as-new, research tools decided by the API environment, and Claude thinking stripped mid tool loop.
4. **Follow-up:**
   - The git tools now **fail closed on any repo config outside a justified allowlist**. All planted vectors are refused live, including `=`-named filters, `gc.recentObjectsHook`, include files, XDG config and `.git/info/attributes`.
   - The thinking issue was **settled by a live experiment**: keeping a thinking block after a history edit returns 400 on enforced accounts. So compaction now waits for tool rounds to finish, and any edit strips thinking.

## Deferred, with reasons

| Item | Where it goes |
|---|---|
| Executor auth checked after body parsing (pre-existing pattern) | P5 hardening |
| `run_command` network egress: needs a Kubernetes NetworkPolicy in the catalog (none exists in this repo) | Release checklist |
| Jev judge calls are not charged to our spend caps (they run in TypeSafe's own activity) | Documented. Jev is off unless a TypeSafe key is configured. |
| `common/streaming.py` (old app) still imports the old `agent_harness` | Deleted in P5 |
| Unit tests for steer delivery, in-flight start counting, `set_policy_keeping_pending`, and the busy timeout | P5 |

## Found along the way

- **Claude 5.x omits thinking text** unless `display: "summarized"` is set. It is now set, so the product view can show thinking.
- **Claude commits through the shell** rather than the `git_commit` tool. The sandbox now provides a git identity to every command.
- **Two more harness/Pydantic AI glue gaps**, added to the upstream list:
  - single-model tool arguments are flattened;
  - inline tools receive raw dicts.

## Next

P2, the API and streaming: transport routes on the real API, the Redis bridge's acknowledged-offset truncation, telemetry frames, session manager `CodingSessions`, the console behind owner-scoped routes, and model catalogs. **Don't start without the owner's go-ahead.**
