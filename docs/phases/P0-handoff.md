PM:

# P0 phase handoff: de-risking spike

- **Parent:** `docs/PRD.md` v1.0 (approved 2026-10-16); `docs/decisions/CTO-memo-001-harness-rewrite.md`
- **Branch:** `refactor/official-harness` (local; nothing is pushed without owner go)
- **Time box:** this phase stops at the go/no-go memo, whatever its outcome
- **Don't start P1 without the owner's go-ahead on the P0 memo.**

## Scope

**In scope.** Spike code may be thrown away, but code that passes its validation step is kept as the P1 base.

- Dependencies and secrets hygiene (0.1)
- `CodingAgent` as a bare `@workflow.defn` with our run loop around `AgentWorkflowRunner` (0.2)
- `harness_ext.py` gap-fillers:
  - `HeartbeatingModel`
  - stream observer with a configurable batch interval
  - suppression of the per-event handler activities
  - interrupt helpers
  - Signal front door
  - continue-as-new helpers (0.2–0.5)
- Two tools: `read_file` (safe) and `write_file` (approval-gated), on executor `/files/*` (0.6)
- Minimal API: attach from Redis, bridge, Signal front door for messages and approvals, ownership check (0.7)
- Node contract check with `@temporalio/agent-harness-client` 0.1.0 against our attach (0.8)
- Measurements and go/no-go memo (0.9)

**Out of scope, with the phase that picks each up.**

| Item | Phase |
|---|---|
| All other tools | P1 |
| Judge and postures beyond the default | P1 |
| Subagents | P1 |
| Context processor | P1 |
| Spend caps | P1 |
| Telemetry frames | P2 |
| Console | P2 |
| UI | P3 |
| Code Mode, MCP, callbacks | P4 |

## Validation table

| Step | What is checked | Pass when | How |
|---|---|---|---|
| 0.1 | Deps install. Secrets never land in git. | `uv sync` succeeds with the harness and Pydantic AI pins. `.env.local` is ignored by git and Docker. The script writes mode 600 and prints no values. | Automated plus inspection |
| 0.2 | One Claude turn end to end | A text reply, a thinking summary (if any), a tool call and usage events reach a Redis-attached test client in order | Automated integration test |
| 0.3 | Provider switch | Same session: a Claude turn, then `set_model` to GPT, then a turn that answers a question about the earlier tool result | Automated integration test |
| 0.4 | Interrupt | During a pending approval and during a model stream: no new reply tokens 2 s after interrupt (9 of 10 runs); `pending_approvals` empty; next turn's history has a result for every cancelled call | Automated, looped |
| 0.5 | Continue-as-new | A forced continue-as-new keeps conversation, allow-list, model, turn number and monotonic stream offsets. A message signalled in the same task is applied exactly once. | Automated |
| 0.6 | Executor `/files/*` | The symlink, traversal, hardlink, FIFO and size suite is refused or capped. `read_file` under 100 ms while `/execute` runs a 60 s command. | Automated (Docker executor) |
| 0.7 | Signal front door, workers down | With the worker stopped, a message and an approval land as `WorkflowExecutionSignaled`. After restart they apply exactly once. A cross-user attach or approve gets 404. | Automated |
| 0.8 | Client contract | `@temporalio/agent-harness-client` 0.1.0 (Node) attaches to our API, projects the turn, approves a tool. No patches. | Automated (Node script) |
| 0.9 | Crash detection and budget | `SIGKILL` mid-stream is detected within 15 s (10 of 10). The reply appears once in the final history. Events, bytes and Actions per streamed second, per model call and per tool call recorded at 50, 250 and 500 ms. | Automated plus a measurement report |

## Rules

- No `workflow.patched()` in our code.
- Every private harness internal we touch lives in `simple_chat_agent/agent/harness_ext.py`, with a comment naming the 0.7.0 source line.
- Provider SDK client retries are off (`max_retries=0`), so every attempt is a Temporal attempt.
- Integration tests use the local Temporal dev server and the real provider keys from `.env.local`. They are tagged so the unit suite doesn't need keys.

## Kill criteria

These are recorded in the go/no-go memo.

| Step | If it fails | Then |
|---|---|---|
| 0.4 | Interrupt can't be made clean | Ship "Stop after this step" (cancel at the next tool boundary) instead of a hard interrupt |
| 0.5 | Continue-as-new can't preserve state | Continue-as-new only when fully idle, with a hard cap on session length |
| 0.8 | The client needs patches | Vendor the MIT client source behind our facade |
| 0.9 | The budget can't hold a 30-tool turn under 10,240 events | Move `reply_delta` off the WorkflowStream to a worker-side Redis tee |

## Definition of done

Every step is Done, or Failed with its kill criterion applied, and `docs/decisions/P0-go-no-go.md` has been written. An independent reviewer has read the diff. The PM relays the memo to the owner.

**Next phase:** P1 agent core. Don't start it without the owner's go-ahead.
