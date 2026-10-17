PM:

# P5 phase handoff: hardening and release prep

- **Parent docs:**
  - `docs/PRD.md` v1.2: §5 (rules), §6 (hosting and cutover) and row P5;
  - `docs/phases/P3-report.md` and `docs/phases/P4-report.md` (deferred items).
- **Branch:** `refactor/official-harness`. It stays local.
- **Owner go:** `/yes`, 2026-10-16, "remember streamlined process". This go covers deleting the old code on this branch.
- **Release needs a separate go.** Pushing, deploying and terminating old chats all wait for the owner's `/go release`.

## Lean test plan (owner instruction)

- **Unit tests:** cover new or changed logic only. Run the full unit suite once, before your final commit.
- **Live checks:** none per item. The Lead does one live smoke test at the end, on Claude Haiku: one demo session that fixes the bug, survives a crash and gets its commit approved.
- **Review:** one deep independent review at the end (PRD exit criterion). There's a re-review only if it finds a blocker.
- **Recorded demo:** a screen capture of the local demo is the owner's to record. We provide the script and confirm that the smoke test passes.

## Scope and owners

### Step 5.1: cutover of the code (Eng R)

Swap the app over:
- `/app/` becomes `/`, served by the API.
- `simple_chat_agent.worker.main` runs the new agent worker. The module path stays the same, per PRD §2.4.
- The `simple_chat_agent.api.main` entrypoint stays.

Delete the old code:
- `agent_harness/`;
- the old frontend (`simple_chat_agent/frontend/`);
- the old workflows and tools: `worker/workflow.py`, `user_chats_workflow.py`, `good_place*`, `worker/tools/`, `streaming_activities.py`, `workflow_runner.py`;
- the old API routes and helpers that only the old app used: `api/routes/`, `schemas.py`, `thinking.py`, `tool_results.py`, `anthropic_models.py`, `streaming.py`, `github_oauth.py`, and others;
- `common/streaming.py` and `common/redis_streams.py` if nothing else uses them;
- the dead Lambda sandbox;
- the old tests.

Keep everything §2.4 keeps: the executor, S3 and Dynamo storage, MCP OAuth, the codec server and graceful shutdown.

Docker:
- the image builds the new web app instead of the old frontend;
- `docker build` succeeds;
- the containers start.

### Step 5.2: hardening and docs (Eng H)

**Hardening:**
- The deferred items from the P3 and P4 reports.
- Executor auth checked before the request body is parsed.
- Accept `pick_files` uploads only while that pick is pending. The API mirrors open `callback_requested` tool ids in Redis from the bridge.

**Docs:**
- A security checklist, `docs/SECURITY-CHECKLIST.md`: every §5 rule linked to the test that proves it.
- `README.md` rewritten for the new demo, including the demo script and the measured cost per demo run.
- Catalog notes in `docs/catalog.md`: env, task queue, Worker Deployment name, the `CodingAgentOwner` search attribute, NetworkPolicy for executor egress, and the S3 lifecycle rule.
- The cutover runbook, `docs/CUTOVER.md`, built from PRD §6.

**Runbook dry run:** run it once against a dev namespace on the local dev server (port 7244, never 7233):
1. Start a few old-style workflows. Use the old code from git history if needed, or fake executions with the old type names.
2. Ship the new worker.
3. Terminate the old workflows with the batch visibility query.
4. Record what happened.

### Step 5.3: smoke test, deep review and report (Lead)

## File ownership

| Owner | Files |
|---|---|
| Eng R | Deletions, `simple_chat_agent/api/main.py`, `simple_chat_agent/worker/main.py`, Dockerfiles, `Makefile`, `pyproject.toml` / `uv.lock`, `simple_chat_agent/web/vite.config.ts` (base path) |
| Eng H | `simple_chat_agent/agent/**`, `simple_chat_agent/api/agent_*.py`, `simple_chat_agent/worker/sandbox/**`, `docs/**` except phase docs, `README.md` |

`agent_routes.py` belongs to Eng H. Eng R may change only its mount or path prefix, and should coordinate through their report.

## Rules

- No patching (Worker Versioning, PINNED).
- Never print key values.
- Stage only your own files.
- Commit messages start with your role.
- Nothing is pushed.
