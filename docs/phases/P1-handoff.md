PM:

# P1 phase handoff: agent core

- **Parent documents:**
  - `docs/PRD.md` v1.1, rows A1–A7, A10, A13–A15, A17–A21 and rules §5
  - `docs/decisions/P0-go-no-go.md` (design inputs §4)
  - `docs/decisions/CTO-memo-001-harness-rewrite.md`
- **Branch:** `refactor/official-harness`. Kept local; nothing is pushed.
- **Builds on:** the P0 code in `simple_chat_agent/agent/`, `simple_chat_agent/api/agent_*.py`, and the executor `/files` endpoints.
- **Don't start P2 without the owner's go-ahead.**

## Scope

### In scope

| Step | Row | Work |
|---|---|---|
| 1.1 | A5 | Workspace tools: `list_files`, `search_code`, `edit_file`, `run_command` (streams output, heartbeats, reattaches on retry), `git_status`, `git_diff`, `git_commit` (local only) |
| 1.2 | A6, §5.4 | Research tools: `fetch_url` with an SSRF guard, and `search_web` (ported). Both are network risk class. |
| 1.3 | A7 | Artifacts and attachments: `create_artifact` and `read_attachment`, ported onto the harness |
| 1.4 | A14, A15, §5.1–5.3 | Postures: `set_posture`, criteria sets, the "always allow" rule limited to `workspace_edit`, deterministic pre-judge rules, the LLM judge, and the Jev plug-in |
| 1.5 | A10 | Subagents: `Explorer` and `Reviewer` with typed contracts; depth 1; at most 2 running at once; stopped at turn end |
| 1.6 | A13 | Observable state: `plan` via `update_plan`, `changed_files`, `workspace` |
| 1.7 | A17 | Context: a history processor (sliding window, budget fit, tool-result truncation) and one overflow retry |
| 1.8 | A20, A21 | History budget guard (checkpoint card) and spend caps (session, user per day, global) |
| 1.9 | A4 | Steer: queued text injected before the next model request |
| 1.10 | A1 | Production worker module: versioning, S3 offload in the catalog, codec, graceful shutdown, heartbeat throttles |
| 1.11 | — | Fixture repo (a small Python service with one real bug) and the scripted-task harness |

### Out of scope

| Item | Picked up in |
|---|---|
| Transport, console and session-manager rework | P2 |
| UI | P3 |
| MCP, Code Mode and browser callbacks | P4 |
| Swapping the catalog entrypoint | P5 |

## Validation table

| Step | Check | Pass when | How |
|---|---|---|---|
| 1.1a | File tools | Every tool goes through the harness approval gate. Edits produce a `changed_files` entry with +/- counts. | Integration test |
| 1.1b | `run_command` | stdout/stderr stream as `tool_progress_delta` within 1 s of output. Exit code is in the result. Output in the result is bounded (head + tail + log ref). | Integration test |
| 1.1c | `run_command` crash | A worker is SIGKILLed during a 40 s command. Attempt 2 reattaches to the same executor job, and the command runs once (one start marker in the workspace). | Integration test |
| 1.2 | SSRF | `fetch_url` refuses loopback, RFC1918, link-local, CGNAT and 169.254.169.254, including after a redirect. | Unit test |
| 1.2b | Network class | With the trust_safe posture, `fetch_url` and `search_web` ask for approval. In auto mode the judge decides. | Integration test |
| 1.4a | Judge fails closed | A judge timeout, a malformed verdict, and an "APPROVED BY REVIEWER" injection in the args each escalate to a human. | Unit and integration tests |
| 1.4b | Pre-rules | `curl`, `wget`, `nc`, `ssh`, `git push`, base64 blobs, writes to `.git/hooks` or CI config, and env dumps escalate without a judge call. | Unit test |
| 1.4c | "Always allow" limit | A `remember` on `run_command` is ignored (the next call still asks). On `edit_file` it works. | Integration test |
| 1.5 | Subagents | The parent runs 2 explorers in parallel. Their streams are nested. Usage counts toward the parent budget. No children are alive after turn end. | Integration test |
| 1.7 | Context | A 200-turn synthetic session never exceeds 85% of the window (estimated). The processor is pure (no I/O). | Unit test |
| 1.8a | Budget guard | A forced low cap triggers the checkpoint card before the model call that would cross it. | Integration test |
| 1.8b | Spend caps | Exceeding the session cap stops the turn with a card. The global breaker blocks new turns. A down counter store fails closed. | Unit and integration tests |
| 1.11 | Scripted task | "test_refund fails; fix it, run tests, commit" passes in at least 4 of 5 runs per provider (Claude, GPT, Gemini). | Integration, looped |
| 1.12 | History budget | The longest scripted turn stays under 10,240 events and 10 MB, re-measured with a 30-tool Sonnet turn. | Measurement |
| 1.13 | Replay | Three recorded `CodingAgent` histories replay clean, including one with continue-as-new. | Automated replay test |

## Rules

- No `workflow.patched()`.
- Private harness internals live only in `harness_ext.py` and are pinned in `tests/test_harness_ext.py`.
- Every tool states its risk class and retry policy (PRD §5.8).
- Tool results that enter model history are bounded.
- Each step is reviewed as a whole. One review and one fix pass, then a final check.

## File ownership (parallel build)

| Owner | Files |
|---|---|
| Lead Eng (integration) | `simple_chat_agent/agent/{workflow,contracts,runtime,harness_ext,prompts}.py`, `simple_chat_agent/agent/subagents.py`, `simple_chat_agent/agent/state.py`, `simple_chat_agent/agent/worker.py` |
| Eng A | `simple_chat_agent/agent/tools/workspace.py`, executor `/execute` job reattach, `tests/test_tools_workspace.py` |
| Eng B | `simple_chat_agent/agent/tools/research.py`, `simple_chat_agent/agent/net_guard.py`, `tests/test_tools_research.py` |
| Eng C | `simple_chat_agent/agent/approvals.py` (criteria, pre-rules, judge), `tests/test_approvals.py` |
| Eng D | `simple_chat_agent/agent/context.py`, `simple_chat_agent/agent/budget.py`, `tests/test_context.py`, `tests/test_budget.py` |
| Eng E | `simple_chat_agent/agent/tools/artifacts.py`, `tests/test_tools_artifacts.py`, `tests/fixtures/refund_service/` |

## Definition of done

- Every step in the table passes.
- The independent review is done, with fixes made.
- The PRD change log is updated.
- The PM reports with evidence.

**Next phase:** P2. Don't start it without the go-ahead.
