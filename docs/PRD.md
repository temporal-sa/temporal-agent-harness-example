# PRD: Advanced AI Agent rebuilt on the Temporal Agent Harness

- **Created:** 2026-10-16
- **Updated:** 2026-10-16
- **Owner:** Michael Jones (product owner)
- **Author:** PM
- **Status:** APPROVED v1.0 (2026-10-16). Changes from here on are amendments. Not approved. No Eng work starts until the owner approves.
- **Companion docs:** `docs/PLAN.md` (plan on a page), `docs/decisions/CTO-memo-001-harness-rewrite.md` (technical forks)

---

## What changed (newest first)

| Date | Version | Change | By |
|---|---|---|---|
| 2026-10-16 | 1.1 | **P0 spike complete; go/no-go memo recommends GO for P1** (`docs/decisions/P0-go-no-go.md`). All 9 validation steps passed and no kill criterion fired. Design inputs for P1: per-turn byte growth (enforce A17/A20 plus offload, re-measure with a 30-tool Sonnet turn), and bridge-acknowledged truncation at continue-as-new (P2). Seven ecosystem bugs to file upstream. Awaiting owner go for P1. | PM |
| 2026-10-16 | 1.0 | **Owner approved the plan (PRD v0.3 → v1.0)** by `/yes` in chat. Low-stakes defaults applied, unanswered: D5 rewrite `basic_file_agent` in P4; D7 MCP off in catalog, on locally; D8 console on in catalog once cross-user tests pass; D9 catalog audience is employees only. Eng work begins with the P0 spike on branch `refactor/official-harness`. | Owner |
| 2026-10-16 | 0.3 | **Owner closed D2:** the catalog brokers the LLM keys at deploy time; this project never provisions or supplies its own keys. Our hard spend caps (A21) still apply. Local dev pulls the shared keys via `scripts/dev-secrets.sh`. | Owner |
| 2026-10-16 | 0.3 | **Owner accepted D1:** build both judges behind one switch; our LLM judge (Claude Haiku tier) is active; Jev takes over when `TYPESAFE_API_KEY` is set. Approved by `/yes` in chat. | Owner |
| 2026-10-16 | 0.3 | **Owner accepted the CTO package (CTO-1 to CTO-6)** as written, including the API-hosted Redis forwarder after asking whether direct WorkflowStream reads would do. Reason recorded: direct reads need a worker (no reload during a crash), and every open tab adds polls to history. Approved by `/yes` in chat. | Owner |
| 2026-10-16 | 0.3 | **Owner approved the scope of the first build.** Core coding-agent story in P1–P3; Code Mode, browser callbacks, MCP and provider web search in P4; GitHub tools and Good Place cut (D3 and D6 closed as cut). Approved by `/yes` in chat; no hook record exists in this repo. | Owner |
| 2026-10-16 | 0.3 | **Signal front door replaces the Redis outbox.** The owner asked whether messages and approvals can land in Temporal while workers are down. In harness 0.7.0 they are Updates (`agent_workflow.py:2068-2111`), which Temporal does not store until a worker accepts them. Because `CodingAgent` owns its run loop (CTO-5), we add our own Signal handlers (`enqueue_message`, `enqueue_approval`, `enqueue_callback_result`). They land durably in history with no worker running, and the run loop feeds them into the runner's normal admission path when a worker returns, with dedup by `request_id`. G5, S8 and U4 are updated. This adds one private internal: calling the runner's validate and handle methods from `harness_ext`. | PM (owner's question) |
| 2026-10-16 | 0.2 | Revised after the CTO memo and four independent reviews (harness fidelity, Temporal durability, UX and demo, security and scope). Changes below. | PM |
| | | **Corrected a false claim.** Messages, approvals and callbacks are Workflow Updates, so Temporal can't accept them while no worker runs. Added an API outbox and an honest "Queued, waiting for a worker" state (G5, S8, U4). *Superseded in v0.3 by the Signal front door.* | PM (from all reviewers) |
| | | **History budget is now a first-class design constraint.** Delta batch interval 500 ms; per-event handler activities suppressed; 256 KB payload offload; bounded tool events; a per-turn cap measured in P0 (A20, CTO-5). | PM (from CTO, durability) |
| | | **Continue-as-new redesigned.** `CodingAgent` uses a bare `@workflow.defn` with our own run loop around the harness runner, so it can continue-as-new between turns, on deploy and on an idle timer, from version 1 (A18, CTO-5). | PM (from CTO) |
| | | **Interrupt redesigned.** Resolve the turn's pending approvals and callbacks, cancel the run task, return a normal reply, and rebuild history from a snapshot (A4). | PM (from fidelity) |
| | | **Model switch** now takes effect at the next turn, not the next model call (A3). | PM (from CTO, fidelity) |
| | | **MCP** now uses a per-run `FunctionToolset` of per-tool harness tools, not `ExternalToolset`, so approvals and "always allow" stay per tool (A8). | PM (from CTO, fidelity) |
| | | **Console** is now served as static assets behind our own owner-scoped routes, not as the packaged app (S5, CTO-6). | PM (from CTO, security) |
| | | **Security added.** Network is its own risk class with an SSRF guard. Executor file I/O is symlink-safe. MCP tokens are bound to their owner. "Always allow" is limited to file edits. Deterministic rules run before the judge, and the judge uses a fixed model. Hard spend caps (§5). | PM (from security) |
| | | **Honest durability numbers.** The hero metric is "Work kept after worker loss". Cache is shown as "read from provider cache (billed at a discount)". The repeated call is counted as repeated. Retried streams don't duplicate text (U5, U6, A19). | PM (from UX, durability) |
| | | **Added:** demo scenario with a seeded repo, reset, crash trigger (SIGKILL), 7-minute script as the P3 acceptance test (§2.5). | PM (from UX) |
| | | **Phases re-cut.** P0 is a go/no-go spike with kill criteria. Demo extras (Code Mode, browser callbacks, MCP, provider web search) move to P4 so the core ships first. | PM (from security, UX) |
| | | **Cutover is now an ordered runbook:** new task queue, deployment name, workflow types and ID prefixes, then terminate the old workflows (§6, D4). | PM (from durability) |
| | | **Jev:** needs a paid TypeSafe key; none exists in the catalog. Our own LLM judge ships; Jev plugs in if a key is provided (D1). | PM |
| 2026-10-16 | 0.1 | First draft. | PM |
| 2026-10-16 | — | Owner decisions: official harness replaces the in-tree one; Anthropic, OpenAI and Gemini supported; keep Redis streaming and our session manager; full rewrite OK; product becomes a coding agent; the harness console shows the raw loop; our UI is a Cursor / claude.ai-style view in the buildkit language; demo features 1, 2, 3, 5, 7 and 8 wanted, 4 questioned, 6 deferred to PM; LLM keys from catalog secrets. | Owner |

---

## 1. Overview

### Problem
This repo hand-rolls a ~6k-line agent harness. Temporal now ships an official harness (`temporal-agent-harness`, PyPI 0.7.0) and is investing there. The demo should show the official harness doing advanced work. The current UI is a chat window. It doesn't show what a coding agent does, or what Temporal's durability saves.

### Users
1. Temporal SAs and field engineers giving live demos. They are the primary users.
2. Customers' engineers reading the code as a reference.
3. Catalog visitors to the always-on demo.

### Goals
- **G1.** Every agent concern runs on `temporal-agent-harness`: turns, tools, approvals, events, subagents, Code Mode, callbacks and observable state. `agent_harness/` is deleted. Where the harness has gaps (interrupt, continue-as-new, heartbeat, batch interval), they are filled in one module (`harness_ext.py`) with tests pinned to 0.7.0, and filed upstream as issues.
- **G2.** A coding agent working in a persistent sandboxed workspace. It reads, searches and edits code, runs commands and tests, commits locally, and produces artifacts.
- **G3.** Anthropic, OpenAI or Gemini is chosen per chat. The provider can be switched between turns with the history kept.
- **G4.** A Cursor / claude.ai-style product view in the buildkit design language. The harness console is one click away and shows the raw loop.
- **G5.** Durability made visible and honest:
  - Everything already done survives a worker loss: conversation, plan, edits, approvals given, and completed model and tool calls.
  - New messages, approvals and callback answers sent while no worker runs **land in Temporal** as Signals. They are visible in the Workflow history, and they are applied when a worker returns.
  - The UI shows exactly what was kept and what was repeated.
- **G6.** Safe by default. Approval policy, with an auto mode in which a judge approves routine calls and escalates the rest. The policy fails closed.

### Non-goals
- **NG1.** A general SDK or a fork of the harness.
- **NG2.** Modifying the harness console UI. We serve its assets unchanged.
- **NG3.** Light theme (the buildkit ships dark only).
- **NG4.** Migrating existing chats (clean break, D4).
- **NG5.** Voice and audio.
- **NG6.** Users supplying their own LLM keys.
- **NG7.** Deploying inside the build phases. Release needs `/go release`.
- **NG8.** Shared multi-user sessions.
- **NG9.** `git push` or PR creation from the catalog deployment.
- **NG10.** Arbitrary third-party MCP servers in the catalog. Catalog MCP is an allowlist or off (D7).
- **NG11.** Serving artifact HTML from our own origin. Artifacts stay in a sandboxed iframe with CSP.
- **NG12.** Workspaces that live forever. Executor workspaces have a TTL and a disk quota.

### Hard boundaries (never touch)
- No secrets in the repo, workflow history, events, or the executor environment.
- No `workflow.patched()` / `deprecate_patch()` in our code. Harness-internal Code Mode markers are not code-evolution patches.
- The executor keeps its isolation profile, and it never receives LLM, MCP or OAuth credentials (enforced by its self-test).
- No route lets one user see or act on another user's sessions, subagents or workspace.

---

## 2. Scope and features

Every row has acceptance criteria (AC) and the behavior when a dependency fails. Phase tags: **P1** core, **P4** demo extras.

### 2.1 Agent (worker)

| ID | Feature | What it does | AC | When a dependency fails |
|---|---|---|---|---|
| A1 (P1) | `CodingAgent` workflow | Bare `@workflow.defn` hosting the harness `AgentWorkflowRunner` inside our own run loop (CTO-5). Init data covers provider, model, effort, workspace, posture, depth and the carried state. Typed operations are declared with `@agent.accepts`. | Started by our session manager. `agent_interface` lists the operations. The console attaches. | Workers down: sends land in Temporal as Signals (S8). UI shows "In Temporal, waiting for a worker". |
| A2 (P1) | Three providers via Pydantic AI | One `TemporalAgent` with a model registry over Anthropic, OpenAI and Gemini. A `HeartbeatingModel` wrapper in `harness_ext` turns SDK retries off and maps 4xx to non-retryable. History is a provider-neutral `list[ModelMessage]`. | A turn on each provider streams text, thinking (where provided), tool calls and usage. | 4xx other than 408/409/429: non-retryable, turn error card. 429/5xx: Temporal retries, attempt shown. |
| A3 (P1) | Switch provider or model | `set_model` (`MidTurn.ACCEPT`, `model_callable=False`), applied at the **next turn**. The model ID is validated against the allowlist. Provider-specific parts (thinking signatures, reasoning IDs) are stripped when switching. | Same chat: Claude, then GPT, then Gemini, then Claude. Each turn answers a question about an earlier turn's tool result. | Rejected model: error shown, previous model kept. |
| A4 (P1) | Chat, steer, interrupt | `chat` (ENQUEUE). `steer` (ACCEPT): queued text injected before the next model request through a capability (`ctx.enqueue`). `interrupt` (ACCEPT): (1) deny the turn's pending approvals and error its pending callbacks, reason "interrupted"; (2) cancel the run task; (3) the handler returns a normal reply `{interrupted: true}` so `turn_end` publishes; (4) history is rebuilt from the pre-request snapshot, with a synthetic "cancelled by user" result for each dangling tool call. | Token flow stops within 2 s in at least 9 of 10 runs. `pending_approvals` is empty afterwards. The next turn's history has a result for every cancelled call. | Late frames from the cancelled call are labeled "Discarded: the reply after you pressed Stop". |
| A5 (P1) | Workspace tools | `list_files`, `read_file`, `search_code` (rg in bwrap), `write_file`, `edit_file` (exact unique replace), `run_command`, `git_status`, `git_diff`, `git_commit` (local only). File ops use executor `/files/*` (CTO-3). `run_command` is an inline tool calling our own activity. That activity publishes `tool_progress_delta`, heartbeats every 5 s, is idempotent per tool-call ID, and **reattaches** to a still-running job on retry. Results are bounded: head and tail, plus a ref to the full log. | Scripted task (fixture repo: find bug, fix, run tests, commit) passes in at least 4 of 5 runs per provider. | Executor unreachable before start: retry with backoff ("The sandbox isn't answering. Temporal will retry every 5s."). Mid-run worker loss: reattach, or honest "Test run restarted (attempt 2)". |
| A6 (P1) | Research tools | `fetch_url` and `search_web` (Google grounding or SearXNG). **These are network-class tools, not inherently safe** (§5). `fetch_url` has an SSRF guard. | A red-team page asking the agent to fetch a metadata IP is refused. Exfiltration through a query string escalates to a human. | No search backend: tool hidden. |
| A7 (P1) | Artifacts and attachments | `create_artifact` (S3/Dynamo) with sandboxed preview. User uploads land in the workspace and in `read_attachment`. | Artifact preview in workbench. | Store down: tool error to the model, turn continues. |
| A8 (P4) | MCP servers | Tools listed by an activity before the run and cached in state. A per-run `FunctionToolset` holds one harness tool per MCP tool (`mcp__<server>__<tool>`), each calling one generic `mcp_call` activity. Approval, "always allow" and criteria are per MCP tool. Tokens are resolved only for the session owner. The judge never sees server-supplied descriptions. | An MCP call shows approval, tool events and console entries. A cross-user `auth_ref` is refused. | Reauth needed: non-retryable error plus a "Reconnect" chip. |
| A10 (P1) | Subagents | Distinct agent classes: `Explorer` (read-only, returns `Findings`) and `Reviewer` (returns `Review`), through `agent.subagent_toolset`. Depth comes from init data. **Release 1:** depth 1, at most 2 concurrent, fast-tier model, usage charged to the parent. Stopped at turn end, before any continue-as-new. Posture passed through init data. | The parent runs 2 explorers in parallel, nested in the UI. | Child failure returns as a tool error. |
| A11 (P4) | Code Mode | `agent.code_mode_tool([...])` built in init, with a `/workspace` VFS mount (`ExecutorWorkspaceFS`). At most 16 host calls per script and concurrency 4. "Approve script" approves its host calls once. | 20-file `gather` script completes in under 3 s. Every host call is visible. | Type-check error goes back to the model. |
| A12 (P4) | Browser callback tools | `ask_user` (choice cards; the answer is validated against the offered option IDs) and `pick_files` (click-to-choose; content is uploaded through our API with size, count and name limits; the callback carries paths only). | The agent waits durably across reload and worker restart. | Tab closed: pending until timeout, then tool error. |
| A13 (P1) | Observable state | `plan` (todo list via inline `update_plan`, inherently safe), `changed_files` (folded from tool results), `workspace` (branch, last command, exit code). | Panels update live from `state_patch`, with no Queries. | n/a: in-workflow. Gap in patch versions makes the client refetch the snapshot. |
| A14 (P1) | Approval postures | Read/search tools are `inherently_safe`. Network, shell, write, commit, MCP and artifact tools need approval. Postures:<br>• Ask for everything<br>• Trust safe tools (default)<br>• Auto mode<br>• Never ask (local only; off in catalog)<br>Changed by `set_posture` (`model_callable=False`), applying to **new** calls. Pending calls stay with the human. "Always allow" applies only to `workspace_edit` tools. | Posture line in the transcript. Catalog rejects "Never ask". | n/a. |
| A15 (P1) | Auto-mode judge | Evaluator wired in the runner constructor. **Deterministic rules run first** and escalate network/exfiltration-shaped commands (`curl`, `wget`, `nc`, `ssh`, `git push`, base64 blobs, writes to `.git/hooks` or CI config, env dumps). Then the judge, which is our LLM judge on a **fixed** model (Claude Haiku tier, configured in the worker), or Jev if `TYPESAFE_API_KEY` is set (D1). Judge inputs are tool name and args only, wrapped as data, with a structured verdict. Timeout 15 s, at most 2 attempts. Confidence threshold 0.8; irreversibility threshold 0.5. In the catalog the judge may approve only `read_only` and `workspace_edit`. | Evaluation cards appear. Tests: judge timeout, malformed verdict, and an "APPROVED BY REVIEWER" injection each escalate to a human. | Any failure escalates to a human. |
| A17 (P1) | Context management | Pydantic AI `history_processor` (pure, character-based, no I/O): last N messages plus the first user message, tool pairs kept together, budget fitting, oversized results truncated. One overflow retry, from the snapshot with a tighter budget. | A 200-turn synthetic session never exceeds 85% of the window. | Overflow after the retry: turn error card, chat still usable. |
| A18 (P1) | Continue-as-new | Our run loop continues-as-new when **all** of these hold: no turn is active, no pending turns, approvals, callbacks or subagents; **and** one of: suggested, target deployment version changed, run budget spent, or the 1-hour idle timer. Sequence: truncate the stream to the current turn start, snapshot `CarriedState` (typed, versioned, every field defaulted), continue-as-new with AUTO_UPGRADE. Recent `request_id`s are carried for dedup. | A forced continue-as-new keeps the conversation, allow-list, posture, plan, model, monotonic offsets and turn number. An idle second tab picks up the next turn. | n/a. |
| A19 (P1) | Crash-detectable model calls | `HeartbeatingModel` heartbeats from a background ticker (every 2 s) for the whole request. Model activity: heartbeat timeout 10 s, start-to-close 15 min, at most 4 attempts with backoff. Worker heartbeat throttle about 1 s. The stream observer stamps each model call's attempt (exposed through telemetry). | `SIGKILL` the worker mid-stream: detected within 15 s in 10 of 10 runs. Attempt 2 completes elsewhere. The final transcript contains the reply exactly once. A silent 25 s reasoning wait does not fail. | n/a. |
| A20 (P1) | History budget guard | Before each model call, check `get_current_history_length()` and `get_current_history_size()` plus our Signal and Update counters. Above 60% of any cap, end the turn with a "Checkpoint reached. Continue?" card, then continue-as-new. | The longest scripted turn stays under 10,240 events and 10 MB per run. | n/a. |
| A21 (P1) | Spend caps | Hard caps checked in the model activity before each call: per session, per user per day, and a global daily circuit breaker. Counters live in Redis/Dynamo. Judge and subagent tokens count toward the parent. | Exceeding a cap stops the turn with an explanatory card. Caps are configurable per env. | Counter store down: fail closed for new turns. |

Cut from release 1 (D3, D6): A9 GitHub tools, A16 Good Place censor, provider-native web search (P4 candidate as a "sources list").

### 2.2 API and streaming

| ID | Feature | What it does | AC | When a dependency fails |
|---|---|---|---|---|
| S1 (P2) | Harness transport routes | Our FastAPI implements the `HttpTransport` contract: `attach` (SSE, `from_offset`, keep-alives while a turn is open), `messages`, `approve`, `callback-result`, `status/{id}`, `workflow-status/{id}`, `sessions`, `close`. Error bodies follow the harness format. **Ownership resolves a session ID or any descendant subagent ID** to its owner. | `useAgentSession` 0.1.0 works unmodified. A contract test diffs our frames against the packaged app's. User B gets 404 on every route for user A's session and subagents. | Auth failure: 404. |
| S2 (P2) | Redis bridge | One forwarder per active session, held by a Redis lease (`SET NX PX 15000`). It runs harness `AgentClient.attach` (merged subagent frames) and `XADD`s each frame with explicit ID `{resume_offset}-{seq}`, so duplicates are rejected. A gap triggers an alarm. Status meta is served from Redis. TTL is the session TTL; AOF is on. Frame format per CTO-2. | Three tabs produce exactly one Temporal subscriber. With workers down, a new tab renders the full transcript and `status` answers in under 200 ms. Lease takeover in under 20 s with no gaps or duplicates. | Redis down: attach falls back to direct Temporal (needs workers). |
| S3 (P2) | Recovery and usage telemetry | Bridge emits `x_recovery` and `x_usage` frames on the same SSE stream, anchored to turn and model call, initially `confirmed: false`. The API confirms them from history (`ActivityTaskStarted.attempt`, identity, last failure) and from Describe (pending-activity heartbeat). Cache-write and thinking tokens are read from the model activity results. | A real `SIGKILL` produces a confirmed `x_recovery` within 10 s of attempt 2 completing. Numbers match history. | Confirmation unavailable: marker shows "Checking…". |
| S4 (P2) | Session manager | New `CodingSessions` workflow, a successor to `UserChatsWorkflow`: chat list, model and MCP settings, TTL. It continues-as-new on target-version change so new chats never start on a stale version. | Existing ownership tests are adapted and pass. | Registry at its history limit: it continues-as-new. |
| S5 (P2) | Harness console | The console's static assets are served unchanged at `/console/`, backed by our owner-scoped routes (CTO-6). `files/view` checks workspace ownership and reads through the executor. Deep link `?s=`. Off in the catalog until the scoping tests pass (D8). | Cross-user tests return 404 on all console routes. No CORS for foreign origins. | Console broken: product view unaffected. |
| S6 (P2) | Model catalog | Live model lists filtered through a **static allowlist** per environment (mid and fast tiers in the catalog). `/api/config` exposes provider, models and effort options. | The picker shows real allowed models for all three providers. | Listing fails: configured default list. |
| S7 (P2) | Tool output lookup | Full tool output comes from the bounded-result ref (workspace log or S3), with ownership checked. | Clicking a tool row shows and copies its full output. | Ref expired: "Output no longer retained" plus the truncated head and tail. |
| S8 (P2) | Signal front door | `messages`, `approve` and `callback-result` send our Signals (`enqueue_message`, `enqueue_approval`, `enqueue_callback_result`), each carrying a client `request_id`. They return `202 {state: "landed"}` as soon as Temporal records the Signal. With no worker, the run loop drains them on its next activation through the runner's validate and handle path (in `harness_ext`). The agent dedups `request_id`s, including across continue-as-new. Validation failures (unknown approval, already resolved) are published to the session as an `x_rejected` telemetry frame. The harness Update path stays available for parent agents and the console. | With workers killed, a message and an approval appear in Temporal UI history as `WorkflowExecutionSignaled`, then apply exactly once after workers return. | Temporal unreachable: request fails visibly with no silent loss. |

### 2.3 UI

| ID | Feature | What it does | AC | When a dependency fails |
|---|---|---|---|---|
| U0 (P3, first) | Wireframes | Desktop 1920×1080 screen share, 1366×768, 320 px. Reviewed by the owner before UI build. | Owner sign-off. | n/a. |
| U1 (P3) | Shell and theme | Vendored buildkit `temporal-theme.css`, Manrope, Noto Sans Mono; square corners; React + TypeScript + Vite; `@temporalio/agent-harness-react` and `-client` 0.1.0 pinned behind a `useCodingSession` facade; generated types. Presenter mode with a larger root font size. | Works at 320 px (transcript plus a "Changes (n)" sheet). Reduced motion honored. No browser path issues a Query. | API down: full-page "Can't reach the app" state with retry. |
| U2 (P3) | Transcript | Centered column with streaming markdown and smoothed typing.<br>• Thinking collapsible ("Thought for 12s", tokens only when reported; "Reasoning summary not provided by <provider>" otherwise).<br>• Slim tool rows with badges and grouped runs ("Read file × 6").<br>• Inline mini-diff on edit and write rows; path chips; expandable full output with copy.<br>• Subagents as nested rows.<br>• Retried model call: partial text collapsed under "Attempt 1 (worker lost)". | Auto-follow unless the user scrolls up. No duplicated text after a crash. | Stream gap: "Reconnecting…" and resume from offset. |
| U3 (P3) | Workbench | Tabs:<br>• **Plan** (live todo)<br>• **Changes** (unified diffs: additions green "+", deletions pink "−" with strikethrough, never red)<br>• **Terminal** (stdout off-white, stderr indigo, exit badge "exit 1 · tests failing" in yellow)<br>• **Artifacts** (sandboxed preview) | Each panel live from events and state. | Executor down: Terminal shows "The sandbox isn't answering. Temporal will retry the command." |
| U4 (P3) | Decisions | Cards above the composer: Approve / Always allow (edits only) / Decline. "Approve all N" for groups. The line "The agent is paused in Temporal and nothing is running". Judge evaluation cards. Send states: **Sending → In Temporal (waiting for a worker) → Accepted → Running**. Stop replaces Send while a turn runs (Esc). Mid-turn input offers "Steer now" or "Queue". | Cards survive reload and workers-down. Accepted renders within 100 ms of the click (Playwright, mocked API). | Outbox unavailable: inline error on the card. |
| U5 (P3) | Meters | Strip with three numbers:<br>• "Session tokens: in / out"<br>• "Input read from provider cache: 82% (billed at a discount)"<br>• "Recoveries: 0"<br>After the first recovery, the hero becomes "**Work kept after worker loss (this session)**". Per-call detail lives in the expanded model-call row. | The strip reconciles with the sum of model usage per provider. | Usage missing: "not reported", never 0. |
| U6 (P3) | Crash and recovery | Banner from bridge silence plus Describe: "Worker isn't answering. Saved: … Waiting: …" (yellow). Recovery divider: "Worker lost at 14:02:11. Another worker continued this turn at the test run. Kept: 6 model calls (18.2k tokens), 9 tool calls including the clone. Repeated: 1 model call (attempt 2)." A tooltip states the comparison: "compared with re-running this turn from your message". | Demonstrated with a real `SIGKILL`. Copy checked against buildkit guide 09. | Telemetry unconfirmed: "Checking…". |
| U7 (P3) | Controls | Model picker (applies next turn). Posture toggle. Attachments. Header links with captions:<br>• **Product view**: what your users see<br>• **Harness console**: the agent loop as the harness records it<br>• **Temporal UI**: the Workflow history | Every control acknowledges within 100 ms. | Link target down: link disabled with reason. |
| U8 (P3) | Auth and sessions | Catalog headers and local login. Session sidebar. "New demo session" from the seed. | Existing auth tests pass. Two-browser isolation check. | n/a. |

### 2.4 Kept, adapted, removed
- **Kept:**
  - sandbox executor (plus `/files/*`, workspace TTL and quota, job reattach);
  - S3/Dynamo storage;
  - MCP OAuth (P4);
  - codec server;
  - graceful shutdown;
  - entrypoint module paths (`simple_chat_agent.api.main`, `simple_chat_agent.worker.main`).
- **Removed:**
  - `agent_harness/`;
  - old providers;
  - old frontend;
  - `tool_results.py`;
  - HTTP/JSONL stream sinks;
  - dead Lambda sandbox;
  - GitHub tools (D6);
  - Good Place (D3).
- **`basic_file_agent`:** rewritten as a ~100-line minimal harness example (D5).

### 2.5 Demo scenario (P3 acceptance test)
- **Seed:** a small Python service with one real bug, a failing test and an approximately 20 s test suite, baked into the executor image. "New demo session" starts from it.
- **Crash trigger:** locally `make crash-worker` (SIGKILL); in the catalog, the crashable environment's worker control with grace 0. There is no crash button in the product UI.
- **Script (7 min)**, all on one session:

| Time | Beat |
|---|---|
| 0:00 | Open the Product view, posture Auto mode, Claude. |
| 0:40 | "test_refund fails. Find the cause, fix it, run the tests, commit." The plan fills; 2 explorers run in parallel. |
| 1:50 | The inline diff appears. The judge approves `pytest`. Terminal streams. |
| 2:40 | Crash during the test run. Banner. Temporal UI shows the pending activity. Recovery divider. Tests go green. |
| 4:10 | `git_commit` escalates to a human. Reload the page; the card is still there. Approve. |
| 4:50 | Switch to GPT: "Review the diff." The Reviewer subagent returns a structured review. |
| 5:40 | Harness console on the same session. |
| 6:30 | Close on the meters. |

---

## 3. Architecture

| Component | Technology | Job | When it's down |
|---|---|---|---|
| Worker | Python 3.12. `temporalio>=1.34`. Pinned: `temporal-agent-harness[pydantic-ai,code-mode]==0.7.0`, `pydantic-ai-slim[temporal,anthropic,openai,google]==2.14.x` | `CodingSessions`, `CodingAgent`, `Explorer`, `Reviewer`, all activities | The UI keeps working from Redis. Sends land in Temporal as Signals. Banner shown. |
| API | FastAPI | Auth, transport routes, Signal front door, bridge, telemetry, catalogs, uploads, console routes | UI unavailable. Workflows continue. |
| Redis (AOF) | Streams plus hashes | Session frame log (the only full transcript after continue-as-new truncation), status meta, leases, spend counters | Attach falls back to Temporal. New turns blocked by the spend guard (fail closed). |
| Executor | Existing bwrap service | Workspace exec, `/files/*`, TTL and quota | Tools retry. UI shows the sandbox state. |
| Temporal | Cloud / dev server | Durable state, WorkflowStream, versioning | Everything waits. |
| S3/Dynamo | As today. Payload offload threshold 256 KB, mandatory | Payloads, artifacts, attachments, metadata | Startup fails (catalog). |
| Providers | Anthropic / OpenAI / Gemini | Model and judge calls | Retries per policy, then a turn error. |

Turn flow:
1. UI `POST messages`.
2. Signal `enqueue_message` (lands in history), which the run loop admits into the runner.
3. Runner opens a turn and runs our `chat` handler.
4. The handler runs `TemporalAgent.run(model=…)`.
5. The model activity (`HeartbeatingModel` plus observer at 500 ms) publishes deltas to the WorkflowStream.
6. Tool calls go through `runner.run_tool` and the approval gate (rules, judge, human), then to an activity or inline tool.
7. The bridge attaches and writes frames to Redis.
8. The UI attach SSE reads from Redis.

---

## 4. Data sources and dependencies

| Dependency | Cost | Main risk | Automatic backup |
|---|---|---|---|
| `temporal-agent-harness` 0.7.0 | Free | Pre-alpha churn. We use 5 private internals (CTO memo table). | Pinned. `harness_ext.py` with contract tests. Upstream issues filed. |
| Pydantic AI 2.14.x | Free | `TemporalAgent` limits | `HeartbeatingModel`, handler suppression. The `TemporalDurability` path is evaluated in P0 as an exit. |
| `@temporalio/agent-harness-react` / `-client` 0.1.0 | Free | 0.x | Facade; vendored MIT source as fallback. |
| Harness codegen | Free | Not on npm | Generated types committed. Regenerated from the pinned tag. |
| LLM providers | Usage-based | Spend on catalog-brokered keys | Hard caps (A21), allowlist, caching. Keys are brokered by the catalog (D2). |
| TypeSafe Jev | Paid; no key | No key | LLM judge (A15). |
| Redis | Existing | Data loss | AOF. Transcript snapshot to S3 at turn end. |
| Temporal Cloud Actions | Per action | Signals, Updates and activities per turn | 500 ms batch, single subscriber, suppressed handler activities. Cost per demo run is measured in P0 and P4. |

---

## 5. Workflow and content rules (enforced in code)

1. **The approval gate fails closed.** A non-safe tool runs only with human approval, a judge APPROVE in an allowed criteria set, or a remembered edit allow. Judge error, timeout or low confidence escalates.
2. **Risk classes:**
   - `read_only`: safe.
   - `workspace_edit`: approval; "always allow" allowed.
   - `shell`: approval; no "always allow"; deterministic pre-rules.
   - `network`: `fetch_url`, `search_web`; approval or judge; SSRF guard.
   - `external_side_effect`: MCP, artifacts; human in catalog.
3. **The model can't change its own guardrails.** `set_posture`, `set_model` and criteria edits are `model_callable=False` and excluded from subagent tool policies.
4. **SSRF guard.** Resolve, then refuse private, loopback, link-local, CGNAT and metadata addresses. Re-check on every redirect and pin the IP. Applies to `fetch_url` and MCP server URLs.
5. **Executor scoping.**
   - `workspace_id` is derived in init from the session ID, never from the model.
   - Every workspace activity resolves its owning `CodingAgent` by walking `parent_id` (at most 3 levels, cached).
   - `/files/*` confines paths with `O_NOFOLLOW` component walks; caps are 1 MB read and 5 MB write.
   - API paths check ownership before calling the executor.
6. **Secrets.**
   - Keys only in worker and API env, never in the executor (self-test).
   - MCP tokens resolved only when `record.user_id == owner`.
   - Local auth refuses to start without a JWT secret unless bound to 127.0.0.1.
   - `**/.env*` is in `.gitignore` and `.dockerignore`.
7. **Budgets.** Max output tokens clamped per model. Context (A17). History (A20). Spend (A21). Subagent depth 1 in release 1.
8. **Retry policies by class:**

| Class | Policy |
|---|---|
| Model | 4 attempts, 2 s initial, ×2, 60 s cap; 4xx non-retryable; Retry-After honored |
| Read tools | 5 attempts |
| `run_command` | 1 attempt after start; retries only "unreachable before start" |
| Write and commit | Idempotency key per tool-call ID, checked in the executor |
| Judge | 2 attempts, 20 s schedule-to-close |
| Subagent turn | Bounded |

---

## 6. Hosting and operations

- **Entrypoints:** unchanged.
- **New env:** `OPENAI_API_KEY` (brokered by the catalog; the project yaml requests it), optional `TYPESAFE_API_KEY`, `CONSOLE_ENABLED`, spend cap settings, model allowlists.
- **Local secrets:** `scripts/dev-secrets.sh` pulls the three keys from Secrets Manager (us-west-1) into `.env.local` with mode 600. It never echoes and refuses to run if the file is tracked.
- **Worker Versioning:** PINNED. No patching.
- **Build ID:** derived from the source plus `uv.lock`. A Build ID is never reused for different code.
- **CI:** replays same-build histories. CI includes golden fixtures of `CarriedState` from version N decoded by version N+1.
- **Cutover runbook (D4, needs owner go at release):**
  1. Ship on a new task queue, Worker Deployment name, workflow types (`CodingSessions`, `CodingAgent`, `Explorer`, `Reviewer`) and ID prefixes.
  2. Keep the old deployment running.
  3. Batch-terminate old `SimpleChatWorkflow` and `UserChatsWorkflow` by visibility query, and purge their S3 payloads.
  4. Decommission the old deployment.

---

## 7. Build phases

| Phase | Work | Exit criterion (measurable) | Status |
|---|---|---|---|
| **P0 Spike (time-boxed)** | Claude end to end. Then `set_model` to GPT in the same chat. Bare-defn run loop plus forced continue-as-new. Interrupt. Worker `SIGKILL` detection. `useAgentSession` against our attach. Cross-user 404. Executor `/files` symlink suite. History and Action budget at 50, 250 and 500 ms. | A written **go/no-go memo** closing CTO-1 to CTO-6. Each item has a kill criterion, for example "continue-as-new can't preserve pending state, so we only continue-as-new when idle and cap session length". | Not started |
| **P1 Agent core** | A1–A7, A10, A13–A15, A17–A21 | Scripted task passes in at least 4 of 5 runs per provider. Replay tests pass over 3 recorded histories. Red-team tests pass (SSRF, injection, judge failures). The longest turn stays under 10,240 events and 10 MB. | Not started |
| **P2 API and streaming** | S1–S8 | Contract test against client 0.1.0. Three tabs use one subscriber. Workers-down: transcript renders; sends land as Signals, then apply exactly once. Cross-user 404 on every route. | Not started |
| **P3 UI** | U0 (owner review first), U1–U8, demo seed | The §2.5 demo script passes end to end twice in a row. Playwright: 100 ms accepted state; no Query from the browser. Copy review against guide 09. | Not started |
| **P4 Demo extras** | A8 MCP, A11 Code Mode, A12 callbacks, provider web search as a sources list, `basic_file_agent` | Each feature's AC. A cost per demo run is measured and written to the README. | Not started |
| **P5 Hardening** | Security checklist (each item linked to a test), docs, Docker, cutover runbook dry run on a dev namespace, deletion of old code | Independent deep review passes. Image builds. Recorded local demo. | Not started |
| Release | Catalog deploy | Owner `/go release` | Not started |

---

## 8. Risks

| Risk | Impact | Mitigation |
|---|---|---|
| Harness 0.7.0 churn, plus 5 private internals | Rework on upgrade | Pin. `harness_ext` contract tests. Upstream issues. |
| History or Action limits in long turns | Chat terminated | A20 guard, 500 ms batch, suppressed handler activities, offload, bounded events, continue-as-new. Measured in P0. |
| Interrupt and continue-as-new depend on private runner state | Breakage | Isolated in `harness_ext`. P0 kill criteria. |
| Prompt injection from repos or pages | Data exfiltration, tool misuse | Network risk class, SSRF guard, pre-judge rules, executor egress allowlist (K8s NetworkPolicy, catalog), red-team tests. |
| Executor abused as compute or a relay | Cost, abuse | TTL, quota, egress allowlist, spend caps. |
| Shared-key exhaustion affects other catalog demos | Outage elsewhere | Hard caps and circuit breaker. The catalog owns key brokering (D2). |
| Console scoping | Cross-user exposure | Owner-scoped routes; off in catalog until tests pass. |
| Who catalog SSO admits | Abuse model | **Open:** D9. |
| React SDK 0.1.0 gaps | UI blocked | Facade, vendored fallback. |
| Scope | Schedule | P4 extras can slip without touching the core story. |

---

## 9. Open items

### Answered
- [x] Official harness replaces the in-tree one. (Owner)
- [x] Three providers. (Owner)
- [x] Keep Redis streaming and our session manager. (Owner)
- [x] Full rewrite; coding agent; harness console for the raw loop; buildkit UI. (Owner)
- [x] Demo features wanted: auto-mode judge, provider switching, Code Mode, live state panels, token meters, typed operations. (Owner)
- [x] Keys from catalog secrets, us-west-1. (Owner; PM verified)

### Still open
Each item gives: the decision; options; recommendation (role); default; decision owner.

- [x] **Scope of the first build.** Core first (P1–P3), extras in P4, cut Good Place and GitHub. Approved by the owner, 2026-10-16.
- [x] **CTO package (CTO-1 to CTO-6).** Accepted by the owner, 2026-10-16. Pydantic AI for all three; API-hosted Redis bridge with Signal front door (v0.3, replacing the outbox); executor `/files/*`; harness React SDK; bare-defn run loop with continue-as-new; console behind owner-scoped routes.
  - Recommendation: CTO.
  - Default: accept all six.
  - Decision owner: Owner.
- [x] **D1 Jev.** Accepted: (c), both behind one switch, 2026-10-16. Options: (a) provide a TypeSafe key; (b) LLM judge only; (c) both, Jev when a key exists.
  - Recommendation: (c) (PM).
  - Default: (c), with the LLM judge active.
  - Decision owner: Owner.
- [x] **D2 Keys.** Closed: the catalog brokers keys; we never supply our own (Owner, 2026-10-16). Original options kept for the record: Options: shared catalog keys with hard caps, or dedicated per-demo keys with provider-side limits.
  - Recommendation: dedicated keys for the catalog; shared keys acceptable locally (security reviewer).
  - Default: shared keys plus hard caps.
  - Decision owner: Owner.
- [x] **D3 Good Place censor.** Cut. Approved by the owner with the scope decision.
- [ ] **D4 Cutover.** The §6 runbook with termination of old chats.
  - Recommendation: PM and durability reviewer.
  - Default: runbook.
  - Decision owner: Owner. This can't be undone, so it needs your go at release.
- [x] **D5 (default applied 2026-10-16: rewrite as minimal example in P4)** `basic_file_agent`.** Recommendation: rewrite as a minimal example (PM). Default: rewrite, in P4. Decision owner: Owner.
- [x] **D6 GitHub tools.** Cut. Approved by the owner with the scope decision.
- [x] **D7 (default applied 2026-10-16: off in catalog, on locally)** MCP in the catalog.** Options: off, or an allowlist of servers.
  - Recommendation: off in release 1 (security reviewer).
  - Default: off in catalog; on locally.
  - Decision owner: Owner.
- [x] **D8 (default applied 2026-10-16: on in catalog once cross-user tests pass)** Console in the catalog.** Recommendation: enable once the cross-user tests pass, since the demo script uses it (UX reviewer). Default: same. Decision owner: Owner.
- [x] **D9 (default applied 2026-10-16: employees only)** Catalog audience.** Does catalog SSO admit only employees, or customers too? This drives the cap values. Default: employees only. Decision owner: Owner.

---

## 10. Tactical steps

| Step | Component | Work | Where | AI fit | Status |
|---|---|---|---|---|---|
| 0.1 | Repo | Pinned deps, temporalio bump, `.gitignore`/`.dockerignore`, `dev-secrets.sh` | root, `scripts/` | Yes | Not started |
| 0.2 | Agent | Bare-defn `CodingAgent` + runner + `chat` on Claude; `harness_ext` (`HeartbeatingModel`, observer batch, handler suppression) | `simple_chat_agent/agent/` | Yes | Not started |
| 0.3 | Agent | `set_model`, interrupt, forced continue-as-new | same | Yes | Not started |
| 0.4 | API | Minimal attach from Redis + bridge + messages/approve via Signal front door | `simple_chat_agent/api/` | Yes | Not started |
| 0.5 | Executor | `/files/*` prototype + symlink suite | `worker/sandbox/` | Yes | Not started |
| 0.6 | Test | SIGKILL, interrupt, cross-user, budget measurement; go/no-go memo | `tests/integration/`, `docs/decisions/` | Yes | Not started |
| 1.x–5.x | All | One step per feature row | per row | Yes | Not started |
