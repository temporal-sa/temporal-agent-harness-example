PM:

# P2 phase handoff: API and streaming

- **Parent docs:** `docs/PRD.md` v1.2, rows S1–S8; `docs/decisions/CTO-memo-001-harness-rewrite.md` (CTO-2, CTO-6); `docs/phases/P1-report.md`.
- **Branch:** `refactor/official-harness`. It stays local.
- **Builds on:** `simple_chat_agent/api/agent_{store,bridge,routes,dev_app}.py` (P0) and the P1 agent.
- **Next phase:** P3. Don't start it without the owner's go-ahead.

## Decisions made for P2 (PM, within the approved plan)

1. **Sessions.** The API starts `CodingAgent` workflows directly; they are not children of a registry. Each workflow carries an owner search attribute (`CodingAgentOwner`, a keyword holding a digest of the user id). There are two reasons:
   - A child inherits its parent's pinned version, so a long-lived pinned registry would keep starting new sessions on old code (durability review).
   - A Describe call answers "who owns this workflow" with no worker running.

   A small per-user `CodingSessions` workflow is kept as the session manager. It holds the session list, titles and settings (MCP registrations come in P4). It is updated through Updates, it continues-as-new on a version change and on history size, and it is never the parent of a session.
2. **Model config** comes from the static allowlist plus a limits table. The API makes no live provider listing calls, so it needs no LLM keys.
3. **The console** is the harness's unchanged static UI, served at `/console/`. It talks to our own owner-scoped routes at `/console/api/` and never to the packaged app (CTO-6).
4. **Stream truncation at continue-as-new.** The bridge Signals the agent `stream_acked(offset)` once per turn end. When it continues as new, the agent keeps everything after `min(acked, head − 50)`.

## Scope

**In scope**

| Step | Rows | Work |
|---|---|---|
| 2.1 | S1, S6, S7, U8 | Agent routes on the real API (`simple_chat_agent/api/main.py`), behind catalog and local auth. Covers sessions (create with an optional demo seed, list, title), owner checks for the session and every subagent, harness error bodies, agent-interface (cached per build), model config, uploads into a session, and tool-output lookup. |
| 2.2 | S2, S3 | The bridge and telemetry:<br>• Redis pub/sub wake across API replicas.<br>• The subagent ownership registry, filled from `subagent_started`.<br>• The `stream_acked` Signal.<br>• Frame archive to S3 or local disk at turn end.<br>• A bounded `done` set.<br>• `x_recovery` and `x_usage` frames, confirmed through Describe and history.<br>• Lease takeover. |
| 2.3 | S4 | `CodingSessions` workflow, owner search attribute, worker registration |
| 2.4 | S5 | Console static files plus owner-scoped console API |
| 2.5 | S7 | Executor endpoint that reads a job's stored log by execution id, scoped to the workspace |
| 2.6 | — | Agent-side `stream_acked` handling and truncation |

**Out of scope**

| Item | Phase |
|---|---|
| New UI | P3 |
| MCP, Code Mode, callbacks | P4 |
| Entrypoint swap and old-code deletion | P5 |

## Validation table

| Step | Check | Pass when | How |
|---|---|---|---|
| 2.1a | Contract | `@temporalio/agent-harness-client` 0.1.0 runs against the real API with local auth: attach, project, approve, queued sends. Creating and listing sessions goes through `HttpTransport` itself. | `tests/contract` |
| 2.1b | Ownership | User B gets 404 on every agent and console route for user A's session and its subagents. This includes `files/view` with a forged source and the tool-output lookup. | Integration |
| 2.1c | Demo seed | "New demo session" seeds `refund_service` (files plus the initial commit) before the first message. | Integration |
| 2.2a | One Temporal reader | With three tabs open on one session, Temporal shows one subscription chain. | Integration (count poll Updates) |
| 2.2b | Workers down | A new tab renders the full transcript from Redis. `status` answers in under 200 ms. Sends land and apply exactly once. | Integration |
| 2.2c | Lease takeover | The forwarder's API process is killed mid-turn. Another process takes over in under 20 s, with no gaps or duplicates in the client projection. | Integration |
| 2.2d | Recovery telemetry | A real SIGKILL produces an `x_recovery` frame that turns `confirmed: true` within 10 s of attempt 2 completing. Kept and repeated counts match history. | Integration |
| 2.2e | Usage telemetry | `x_usage` per model call includes cache read **and** write tokens. The per-turn sum matches history. | Integration |
| 2.2f | Truncation | After continue-as-new, the carried log is ≤ 50 events past the acked offset, and the client transcript has no gaps. | Integration |
| 2.3 | Session manager | List, title and settings survive continue-as-new. A version change moves the registry. Agents are never its children. | Integration |
| 2.4 | Console | `/console/?s=<id>` shows the live loop for an owned session. No CORS for foreign origins. | Manual plus integration (HTTP) |

## Rules

- Same as P1: no patching, private internals only in `harness_ext.py`, and a regression test for every finding.
- Browser paths issue no Queries (buildkit rule). `status` comes from Redis; Describe is allowed.
- Every route checks the caller is the owner. Unknown IDs and IDs owned by someone else both get 404, never 403.

## File ownership

| Owner | Files |
|---|---|
| Lead | `simple_chat_agent/agent/**` (workflow, sessions workflow, worker), `tests/integration/test_p2_*.py` (shared helpers) |
| Eng F (API) | `simple_chat_agent/api/agent_routes.py`, `simple_chat_agent/api/main.py` (mounting only), `simple_chat_agent/api/agent_models.py` (new), `tests/test_agent_routes.py`, `tests/contract/**` |
| Eng G (bridge) | `simple_chat_agent/api/agent_bridge.py`, `simple_chat_agent/api/agent_store.py`, `simple_chat_agent/api/agent_telemetry.py` (new), `tests/test_agent_bridge.py`, `tests/test_agent_telemetry.py` |
| Eng H (console, after Eng F) | `simple_chat_agent/api/console.py` (new), `tests/test_console.py` |
| Eng A (executor) | `simple_chat_agent/worker/sandbox/executor.py`, `simple_chat_agent/agent/tools/workspace.py` (log reader) |

## Definition of done

- Every validation step passes.
- Independent review, a fix pass, then verification.
- Report written; PRD log updated.
