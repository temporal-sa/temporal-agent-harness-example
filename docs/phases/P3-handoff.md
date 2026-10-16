PM:

# P3 phase handoff: the product view (UI)

- **Parent docs:** `docs/PRD.md` v1.2, rows U0–U8 and §2.5 (demo script); the approved wireframes `docs/phases/P3-wireframes.html` (owner `/yes`, 2026-10-16); `docs/phases/P2-report.md`.
- **Branch:** `refactor/official-harness`. It stays local.
- **Builds on:** the P2 agent API (`/api/agent/*`), the Redis bridge and its `x_usage` / `x_recovery` / `x_rejected` / `x_worker` telemetry frames, and the agent's observable state (`plan`, `changes`, `workspace`).
- **Next phase:** P4. Don't start it without the owner's go-ahead.

## Owner decisions for P3

| # | Decision | Source |
|---|---|---|
| 1a | Workbench is a right-hand panel beside the transcript. | Wireframes `/yes` |
| 2a | Full session sidebar at 1920; an icon rail at 1366 and in presenter mode. | Wireframes `/yes` |
| 3a | Hero gradient only. No starfield or grid floor. Motion only marks a state change. | Wireframes `/yes` |
| 4a | Meters strip under the header. After the first recovery, its first number becomes "Work kept after worker loss (this session)". | Wireframes `/yes` |
| A | Every approval card offers **Approve / Always approve / Decline**. "Always approve" stays limited to `workspace_edit` (shown greyed with a reason elsewhere). Decline takes an optional reason. | Owner, in chat |
| B | **Stacked approvals:** calls waiting at once share one card with "Approve all N". They group by tool; identical arguments fold to "× n". Each group has Approve n / Decline n, and each item can be opened and answered alone. New arrivals join their group without moving the buttons. | Owner, in chat |
| C | Auto mode is a posture in the composer's posture menu (Ask for everything / Trust safe tools (default) / Auto mode / Never ask (local only, off in catalog)). The menu lists what is always approved this session, with Clear. | Owner, in chat |

## Decisions made for P3 (PM, within the approved plan)

1. **Where the app lives.** A new Vite + React + TypeScript app at `simple_chat_agent/web/`, built to `simple_chat_agent/web/dist/` and served by the API at **`/app/`**. The old frontend stays at `/` until P5 swaps them and deletes the old one.
2. **Harness SDK.** `@temporalio/agent-harness-react` and `@temporalio/agent-harness-client` pinned at exactly `0.1.0`, used only behind our own `useCodingSession` facade (`web/src/session/`). Components never import the harness packages directly, so an SDK upgrade touches one folder.
3. **Generated types.** `temporal-agent-harness` emits the agent's schema as JSON Schema; we generate TypeScript from it into `web/src/generated/` with a pinned generator and a `npm run gen` script. Generated files are committed, never hand-edited.
4. **No Queries from the browser.** Status comes from the attach stream, `/api/agent/status` (Redis) and Describe-backed routes only. A Playwright test fails if any browser request reaches a Query-backed path.
5. **Theme.** The buildkit `temporal-theme.css` tokens, Manrope and Noto Sans Mono are vendored into `web/src/theme/` (fonts bundled, not hotlinked). Square corners. Red only for real errors; diffs use pink deletions with strikethrough.
6. **Crash trigger.** `make crash-worker` SIGKILLs the local dev worker (no crash button in the product UI).

## Scope

**In scope**

| Step | Rows | Work | Owner |
|---|---|---|---|
| 3.0 | U1, U7, U8, §2.5 | Serving and tooling: mount `/app/` (catalog and local auth), confirm `/api/agent/config` gives what the model picker and posture menu need (providers, models, efforts, postures allowed in this environment), TS type generation, `Makefile` targets (`web`, `web-dev`, `gen`, `crash-worker`, `dev-stack`), Playwright install. | Eng F |
| 3.1 | U1, U5, U7, U8 | App shell and session facade: theme, layout grid at 1920 / 1366 / 320, header (status pill, view links with captions, presenter mode), session sidebar with "New demo session", meters strip (including the "work kept" hero and "not reported"), API-down page, reduced motion. `useCodingSession` facade: wraps `useAgentSession`, folds our `x_*` telemetry frames, exposes send states, model, posture. | Eng U |
| 3.2 | U2, U4 (composer), U7 | Transcript and composer: streaming markdown with smoothed typing, thinking rows, tool rows with badges and grouped runs, inline mini-diff, path chips, expandable output with copy (S7 lookup), nested subagent rows, structured Review/Findings cards, "Attempt 1 (worker lost)" folding, "Discarded" after Stop, auto-follow. Composer: Send / Stop (Esc), Steer now / Queue, model picker (applies next turn), posture menu, attachments. | Eng U |
| 3.3 | U3 | Workbench: Plan, Changes (unified diffs), Terminal (stdout / stderr colors, exit badge, paused marker, reattach line), Artifacts (sandboxed preview). Mobile bottom sheet. | Eng W |
| 3.4 | U4, U6 | Decisions and recovery: stacked approval card (decisions A and B), judge evaluation cards, send states (Sending → In Temporal → Accepted → Running), checkpoint and spend-cap cards, the "Worker isn't answering" banner, the recovery divider with "Checking…" until confirmed. | Eng W |
| 3.5 | all | Playwright suite, the demo script run, copy review. | Lead |

**Out of scope**

| Item | Phase |
|---|---|
| MCP, Code Mode, `ask_user` / `pick_files`, web search sources | P4 |
| Swapping `/app/` to `/`, deleting the old frontend and `agent_harness/` | P5 |
| Widening "Always approve" beyond file edits | Owner chose not to |

## Validation table

| Step | Check | Pass when | How |
|---|---|---|---|
| 3a | Layout | 1920 × 1080, 1366 × 768 and 320 px match the wireframes' structure; no horizontal scroll at 320; tap targets ≥ 44 px on phone. | Playwright screenshots at three sizes, reviewed against the wireframes |
| 3b | Accepted in 100 ms | Clicking Send, Approve, Approve all, a model or a posture shows its accepted state within 100 ms with a mocked API. | Playwright, mocked API |
| 3c | No Queries | No browser request reaches a Query-backed path during the full demo script. | Playwright request log against an allowlist |
| 3d | Workers down | With the worker killed, a reloaded tab renders the full transcript, pending approval cards survive, and a sent message shows "In Temporal, waiting for a worker". | Playwright against the real stack |
| 3e | Stacked approvals | Five concurrent calls (3 edits, 2 identical commands) show one card with two groups, "× 2" on the commands; answering one item updates counts; Approve all answers only what is still waiting; each answer applies exactly once. | Playwright against the real stack (test tools) |
| 3f | Recovery | A real `make crash-worker` during the test run shows the banner, then the divider; the divider's numbers turn from "Checking…" to confirmed and match history; the reply appears once. | Playwright + real SIGKILL |
| 3g | Meters | The strip equals the sum of confirmed `x_usage` per provider; missing usage reads "not reported". | Playwright against the real stack |
| 3h | Isolation | Two browsers as two users: neither sees the other's sessions in the sidebar or by URL. | Playwright, two contexts |
| 3i | Demo script | §2.5 passes end to end **twice in a row** on the local stack. | Lead, recorded |
| 3j | Copy | Every on-screen string reviewed against buildkit guide 09. | Independent reviewer |
| 3k | Accessibility | Keyboard reaches every control with visible focus; status never by color alone; reduced motion honored. | Playwright + axe |

## Rules

- No patching, as in every phase (worker versioning handles it).
- Browser paths issue no Queries.
- Never print or bundle a key. The web app never sees a provider key.
- Components import harness packages only through `web/src/session/`.
- Every review finding gets a regression test.

## File ownership

| Owner | Files |
|---|---|
| Eng F (API, tooling) | `simple_chat_agent/api/main.py` (mount only), `simple_chat_agent/api/agent_routes.py`, `tests/test_agent_routes.py`, `Makefile`, `scripts/crash-worker.sh`, `simple_chat_agent/web/src/generated/**`, `simple_chat_agent/web/scripts/gen*` |
| Eng U (shell, transcript) | `simple_chat_agent/web/` scaffolding (`package.json`, `vite.config.ts`, `tsconfig*.json`, `index.html`), `web/src/{main.tsx,app,session,shell,theme,transcript,composer,meters}/**` |
| Eng W (workbench, decisions) | `web/src/{workbench,decisions,recovery}/**` |
| Lead | `simple_chat_agent/web/e2e/**`, `playwright.config.ts`, docs |

Eng U publishes the facade's types (`web/src/session/types.ts`) first; Eng W builds against them and asks Eng U, through the PM, for anything missing.

## Definition of done

- Every validation step passes.
- Independent review (UX, copy, security of the browser surface), a fix pass, then verification.
- Report written; PRD log updated.
