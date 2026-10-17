PM:

# P5 report: hardening and release prep

- **Date:** 2026-10-16
- **Branch:** `refactor/official-harness` (local). Commits 91abb85..b693b56.
- **Status:** the exit criteria are met. The deep review found no blockers; its two majors are fixed. The image builds at HEAD. The recorded demo is for the owner to capture (see below).
- **Handoff:** `docs/phases/P5-handoff.md`

## What changed

- **The switchover (Eng R):**
  - The UI is served at `/`; `/app/...` redirects there, and the redirect can't be used to send users to another site.
  - `python -m simple_chat_agent.worker.main` runs the new agent worker.
  - **The old app is deleted:** 121 files, about 38.6k lines. That includes `agent_harness/`, the old frontend, the old workflows, tools and routes, and the old-only tests.
  - The artifact routes the UI still uses moved to `api/artifacts.py`.
  - Docker builds the new UI.
- **Hardening (Eng H):**
  - Every deferred P3/P4 item is fixed. Highlights:
    - `pick_files` uploads are accepted only while that pick is pending (the bridge mirrors open picks in Redis, with no Query);
    - the MCP loopback rule covers only the named ports;
    - OAuth flows time out and are capped per user;
    - a new bearer token is checked before it is stored;
    - the web search toggle survives a reload;
    - the MCP tool list is carried across continue-as-new;
    - the model sees the real reason when a file isn't found;
    - the executor checks auth before reading the request body.
- **Local sign-in (Lead):** it refuses to start without a real JWT secret (32+ characters, no built-in fallback), as PRD §5.6 requires.
- **Docs:**
  - `docs/SECURITY-CHECKLIST.md`: 74 rules and fixes, each linked to its test. Two have no automated test, for reasons stated there.
  - A rewritten `README.md`, with the demo script and the cost per run.
  - `docs/catalog.md`: env names, ports, health checks, codec auth, NetworkPolicy and the S3 lifecycle rule.
  - `docs/CUTOVER.md`.

## Cutover dry run

The dry run passed on the local dev server, in a new namespace (`cutover-dryrun`):
- I ran stubs of the old `SimpleChatWorkflow` ×3 and `UserChatsWorkflow` ×1.
- The new worker ran on its own task queue, with its own Worker Deployment and Build ID.
- A batch terminate by visibility query took the old runs from 4 running to 0.
- The new session list (`CodingSessions`) kept running.

The full record is in `docs/CUTOVER.md`.

## Deep review (one pass, the release gate)

- **No blockers.**
- **M1, fixed:** idle sessions never continued-as-new, so an old open run could point at expired claim-check payloads. An idle session now continues-as-new at least daily (`MAX_RUN_AGE`), and the doc's S3 guidance matches.
- **M2, fixed in the docs:** the catalog doc no longer said the codec server needs auth. `catalog.md` now requires `SIMPLE_CHAT_CODEC_AUTH_ENABLED=1`, the JWKS URL and the allowed origins.
- **Minors fixed:**
  - local files no longer leak into image builds (`.dockerignore`);
  - `dev.db` is ignored;
  - the README creates the Redis folder first;
  - a checklist link is corrected;
  - the executor returns 401, not 500, for a non-ASCII token;
  - the runbook has the yaml prerequisites and the S3 purge command.

## Totals

| Check | Result |
|---|---|
| Python unit | 747 pass (51 live-only skips), including the replay fixtures |
| Web | Typecheck passes, 220 unit tests pass, the build passes |
| Playwright mocked | 83 pass (Eng R's run) |
| Live smoke (Haiku, the app at `/`) | 4 of 4 pass in 53 s: crash and recovery, workers down, isolation, stacked approvals |
| Image | `make image` passes at HEAD (557 MB). No local files inside, both entrypoints start, no secrets |

## Open, accepted or deferred

| Item | Status |
|---|---|
| Idle sessions are never closed (they now continue-as-new daily, so this is safe). Closing them after the session TTL would save worker wake-ups. | Follow-up after release |
| A failed MCP OAuth re-authorization overwrites the working token, as the bearer path did before its fix (local only) | Follow-up |
| No CI config, and no golden `CarriedState` fixtures across versions (PRD §6). Replay fixtures and round-trip tests stand in. | Follow-up; needs a CI decision |
| The recorded local demo | Owner. The script is in `README.md`, and the smoke test and P3 demo runs pass |
| The PRD's "activities walk `parent_id` (≤3 levels)" wording doesn't match the design (activities take `workspace_id` from init data) | PRD wording fix |

## Release checklist (needs the owner's `/go release`)

1. Push the branch and open the PR to `main`.
2. Update the catalog project yaml (see `docs/catalog.md` and `docs/CUTOVER.md`, "Before you start"):
   - request the brokered `OPENAI_API_KEY`;
   - set `CODING_AGENT_TASK_QUEUE` and a new `TEMPORAL_DEPLOYMENT_NAME`;
   - turn on codec auth;
   - add the executor NetworkPolicy and the S3 lifecycle rule;
   - make sure the `CodingAgentOwner` search attribute exists;
   - drop `SIMPLE_CHAT_STREAM_TOKEN`.
3. Deploy. Then run `docs/CUTOVER.md` steps 1–4. **Step 3 terminates the old chats (D4), which can't be undone.**
