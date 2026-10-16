Eng (with PM edits):

# P0 go/no-go memo: de-risking spike

- **Date:** 2026-10-16
- **Branch:** `refactor/official-harness` (local only; nothing pushed)
- **Commits:** `87ab240` … `bb630f7`
- **Handoff:** `docs/phases/P0-handoff.md`
- **Recommendation: GO for P1.** Every validation step passed. No kill criterion was triggered. Two items carry into P1 as design inputs (§4).

## 1. Validation results

All results come from real providers and a local Temporal dev server (CLI 1.9.1, server 1.32). The executor ran on macOS without bwrap; `/files` doesn't need it.

| Step | Pass condition | Result | Evidence |
|---|---|---|---|
| 0.1 | Deps install; secrets never land in git | **Pass** | `pyproject.toml` pins: `temporal-agent-harness[pydantic-ai,code-mode]==0.7.0`, `pydantic-ai-slim[…]==2.14.1`, `temporalio 1.34`. `.env.local` is mode 600 and ignored by git and Docker. `scripts/dev-secrets.sh` refuses to run if the file is tracked and never echoes values. |
| 0.2 | One Claude turn streams reply, thinking, a tool call and usage, in order | **Pass** | `tests/integration/test_p0_agent.py`: approval-gated `write_file`, safe `read_file`, usage on every model call. |
| 0.3 | Provider switch; the next turn answers about an earlier tool result | **Pass** | Same test: Claude → GPT-5.4-mini → Gemini 3.5 Flash, each recalled the word Claude wrote. Google served `gemini-3.5-flash` as `gemini-3.6-flash` (provider alias). |
| 0.4 | No new reply tokens 2 s after interrupt (9/10); approvals cleared; history valid | **Pass** | Mid-stream: 10/10 runs had zero tokens arrive after Stop (worst 0.23 s). During a tool activity: 10/10 stopped cleanly, with no further model call (added after review finding 1). During an approval wait: declined, `pending_approvals` empty, next turn coherent. |
| 0.5 | Forced continue-as-new keeps conversation, allow-list, model, turn number and offsets; a duplicate is applied once | **Pass** | `ContinueAsNewTests`: "always allow" survived (no second approval), turn numbers monotonic, duplicate `request_id` ran once. |
| 0.6 | `/files` suite refused or capped; read < 100 ms during a long command | **Pass, with a caveat** | 31 file tests, including symlink at the leaf and mid-path, traversal, hard links (now refused for reads too), FIFO, sparse files, and partial UTF-8. The lock scheme lets reads proceed during `/execute`, but this was proven in-process, not with a real 60 s bwrap command; re-check on Linux in P1. |
| 0.7 | Workers down: sends land as Signals and apply once; cross-user 404 | **Pass** | `test_p0_api.py`: a message reached Temporal in 0.01 s with no worker, visible as `WorkflowExecutionSignaled`. Status came from Redis in under 200 ms. An approval sent while workers were down applied exactly once later. A fresh attach with no workers replayed the transcript. User B got 404 on all 6 routes. Rejected and retried messages don't hang attach. |
| 0.8 | `@temporalio/agent-harness-client` 0.1.0, unpatched, works against our API | **Pass** | `tests/contract/`: 3/3 runs: attach, project, approve, queued sends, monotonic offsets. Two API races were fixed along the way. |
| 0.9 | SIGKILL detected within 15 s (10/10); reply appears once; budget measured | **Pass** | Detection 12.7–13.3 s across 10 runs. The final history held exactly one model response each time. Budget table in §3. |

## 2. CTO fork closure

| Fork | Decision held? | Notes |
|---|---|---|
| CTO-1 Pydantic AI for all three providers | **Yes** | All 7 allowlisted models stream through one agent with a shared history. Three ecosystem bugs were worked around in `harness_ext.py`; see §5. |
| CTO-2 API-hosted bridge + Redis frames | **Yes, refined** | The Signal front door (owner's idea) replaced the outbox. Messages are admitted under the API-chosen `message_id`, so the unpatched client reconciles them. |
| CTO-3 Executor `/files/*` | **Yes** | Path walk with `O_NOFOLLOW` from a root fd; reads don't wait for a running command. |
| CTO-4 Harness React/JS SDK | **Yes** | The core client works unpatched. One client wake-up race was found (§5); our facade will cover it. |
| CTO-5 Bare `@workflow.defn` run loop + continue-as-new | **Yes** | Works between turns, on a version change, and on an idle timer. A 500-event stream tail is carried across; P2 replaces it with truncation up to the offset the bridge has acknowledged. |
| CTO-6 Console behind our routes | **Not exercised in P0** | Our routes now match the console's transport contract; serving the static assets is P2. |

## 3. History and Action budget (measured)

One realistic turn: Claude Haiku 5.5, 10 file reads and a ~450-word reply, one subscriber.

| Batch interval | History events | Signals | Updates accepted | History bytes |
|---|---|---|---|---|
| 50 ms (harness default) | 546 | 94 | 30 | 336 KB |
| 250 ms | 292 | 34 | 26 | 310 KB |
| **500 ms (ours)** | **204** | **23** | **16** | **274 KB** |

- At 500 ms, about 50 such turns fit under the 10,240-event warning before continue-as-new is needed.
- Bytes are dominated by each model call carrying the full conversation. The 256 KB payload offload is now wired everywhere (fix pass) and is the main lever for long turns.
- The P1 budget guard (A20) will check event count and size before each model call.

## 4. Carried into P1 as design inputs

1. **Per-turn byte growth.** Long coding turns resend a growing conversation on every model call. P1 must enforce A20 (budget guard), A17 (history processor, tool-result truncation) and the payload offload, and re-measure with a 30-tool Sonnet turn.
2. **Bridge acknowledgement.** Continue-as-new currently keeps a fixed 500-event tail. In P2 the bridge records its acknowledged offset, and continue-as-new truncates only up to it.

## 5. Ecosystem bugs found (upstream issues to file)

1. **Harness:** `harness_event_stream_handler` calls `stream.usage()`, but in Pydantic AI 2.14.1 `usage` is a property. The call raises, the error is swallowed, and every `model_interaction_ended` has `usage=None`.
2. **Pydantic AI and genai-prices:** `RequestUsage.extract` builds `cls(**usage.__dict__)`. genai-prices ≥ 0.1.10 adds `output_reasoning_tokens`, so construction fails, is swallowed, and all OpenAI and Gemini usage reads as zero.
3. **Anthropic SDK 1.x** removed parameters (`temperature` and others) that Pydantic AI 2.14.1 still passes. Pinned below 1.0.
4. **Harness:** the Pydantic AI glue has no batch-interval knob (always 50 ms); there's no continue-as-new hook, no interrupt, and no way to admit a message with a caller-supplied id.
5. **Harness:** `as_pydantic_ai_tool` turns a cancelled tool activity (an `ActivityError`) into a "tool failed" string, so cancellation can't stop a turn.
6. **Harness client 0.1.0:** a wake-up is lost if `sendMessage` resolves while the first attach is still open.
7. **Pydantic AI:** profiles for Claude 5.x are missing, so the unified `thinking` setting sends the rejected `enabled` form; we set adaptive thinking explicitly.

Every private internal we depend on is pinned by `tests/test_harness_ext.py` (10 tests).

## 6. Review

- **Independent deep review** of `87ab240..f9c1d49`: 1 blocker (interrupt during tool activity), 4 majors, 10 minors.
- **Fix pass** `bb630f7` addressed all 15, with regression tests:
  - interrupt during tool activity, 10/10;
  - rejected and retried messages don't hang attach;
  - hard-link reads refused;
  - partial-UTF-8 range read;
  - length clamp.
- **Deliberately deferred to P5 hardening:** executor auth runs after body parsing (a pre-existing pattern shared with `/execute`).
- **Verification review** of the fix pass: see §7.

## 7. Verification review of the fix pass

The verification review of `bb630f7` passed with items:
- **Findings 1–15:**
  - fixed: 1, 3–7, 9, 11, 12, 15;
  - partially fixed: 2, 8, 10, 13 (residuals covered by the follow-up below);
  - 14: fixed, except auth-before-body, which is deferred.
- **New items from the fixes:** N1–N10. N1–N9 are fixed in the follow-up commit:
  - N1: the API validates models against the configured allowlist, so it needs no LLM keys.
  - N2: a stale pending model switch no longer overrides a newer one.
  - N3: the pending add is atomic with the done check (Lua).
  - N4: a close during handoff rejects buffered messages, and the bridge clears pending on a closed idle workflow.
  - N5: a rejection publishes accepted-then-error, so the client always shows it.
  - N6: hard-linked files are readable again (writes are still refused).
  - N7: offsets inside a character are adjusted, and a range inside one character reads through to its end.
  - N8: atomic acquire-or-renew lease, plus task pruning on every path.
  - N9: a contract test for the adapter helpers, and stricter cancellation matching.
- **Deferred:** N10 (the `done` set grows for the session's life, bounded by its TTL) goes to P2.
- **Verification after the follow-up:**
  - 193 unit tests pass;
  - 8 integration tests pass (crash 13.5 s; interrupt during tool 1/1; stream interrupt lag 0.0 s);
  - the npm client contract check passes.

## 8. Kill criteria

None triggered:
- interrupt is clean;
- continue-as-new preserves state;
- the client needed no patches;
- the budget holds a 10-tool turn at 204 events.
