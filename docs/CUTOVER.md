# Cutover runbook

This runbook moves the catalog from the old chat app (`SimpleChatWorkflow`, `UserChatsWorkflow`) to the coding agent (`CodingSessions`, `CodingAgent`, `Explorer`, `Reviewer`). It follows PRD §6 and decision D4: no history is migrated, old chats are terminated, and users start new sessions.

**This needs the owner's `/go release`.** Nothing here runs against the catalog before that go. The dry run at the end was done on the local dev server only.

## What changes

| | Old app | Coding agent |
|---|---|---|
| Workflow types | `SimpleChatWorkflow`, `UserChatsWorkflow` | `CodingSessions`, `CodingAgent`, `Explorer`, `Reviewer` |
| Task Queue | `SIMPLE_CHAT_TASK_QUEUE` (default `simple-chat-agent`) | `CODING_AGENT_TASK_QUEUE` (default `coding-agent`) |
| Worker Deployment | `TEMPORAL_DEPLOYMENT_NAME` (old name) | a **new** `TEMPORAL_DEPLOYMENT_NAME` |
| Build ID | `TEMPORAL_WORKER_BUILD_ID` | `TEMPORAL_WORKER_BUILD_ID`, now **required** (no fallback) |
| Workflow ID prefixes | the old app's chat and user ids | `coding-agent-…`, `coding-sessions-…` |
| Versioning flag | the old app's flag | `CODING_AGENT_WORKER_VERSIONING_ENABLED` (default on, PINNED) |
| UI | port 8080 (old frontend) | served by the API on 8000 at `/`; `/app/…` redirects to `/` |
| Worker health | | codec `/health` on 8001 |

`SIMPLE_CHAT_TASK_QUEUE` is still injected into both the API and the worker: user identity and the Redis key prefix are derived from it. See [catalog.md](catalog.md) for the full environment.

## Before you start

1. The owner has given `/go release`.
2. The new image is built (`make image`) and the executor image (`make executor-image`, or the catalog's build).
3. The `CodingAgentOwner` Keyword search attribute exists in the namespace. Session list rebuilds and the owner check depend on it.
4. Pick the new Worker Deployment name. It must not be the old one. The Build ID comes from the source plus `uv.lock` and is never reused for different code (PRD §6).
5. Count what will be terminated, and tell users that old chats end:

   ```sh
   temporal workflow count --namespace "$NS" \
     --query 'WorkflowType IN ("SimpleChatWorkflow", "UserChatsWorkflow") AND ExecutionStatus = "Running"'
   ```

## Steps

### 1. Ship the new worker on its own Task Queue and Deployment

Deploy the API and worker with `CODING_AGENT_TASK_QUEUE`, the new `TEMPORAL_DEPLOYMENT_NAME` and the new `TEMPORAL_WORKER_BUILD_ID`. Make the new version current for its deployment:

```sh
temporal worker deployment set-current-version --namespace "$NS" \
  --deployment-name "$NEW_DEPLOYMENT" --build-id "$BUILD_ID" --yes
```

Check: open the product view, start a session, and confirm in Temporal UI that `CodingSessions` and `CodingAgent` run PINNED on `$NEW_DEPLOYMENT:$BUILD_ID`.

### 2. Keep the old deployment running

Leave the old workers polling the old Task Queue. Old executions are PINNED to the old deployment, and the new workers can't run them. Nothing routes between the two queues.

### 3. Batch-terminate old executions and purge their payloads

```sh
temporal workflow terminate --namespace "$NS" \
  --query 'WorkflowType IN ("SimpleChatWorkflow", "UserChatsWorkflow") AND ExecutionStatus = "Running"' \
  --reason "Cutover to the coding agent (PRD §6, D4)" --yes
temporal batch list --namespace "$NS"     # wait for the job to show Completed
```

Then run the count from "Before you start" again; it must be 0.

Purge the old app's S3 claim-check payloads only after their histories have passed namespace retention, so no retained history points at a deleted payload. The new app's archive and claim-check prefixes are separate (see [catalog.md](catalog.md#s3-lifecycle)); don't touch them.

### 4. Decommission the old deployment

Once the count is 0, scale the old workers to zero and remove them. The old Worker Deployment can be deleted when it has no pollers and no running executions:

```sh
temporal worker deployment describe --namespace "$NS" --name "$OLD_DEPLOYMENT"
temporal worker deployment delete --namespace "$NS" --name "$OLD_DEPLOYMENT"
```

## Rollback

Before step 3, roll back by pointing the catalog at the old image and queue again; nothing has been terminated. After step 3, old chats are gone by design (D4). Rolling back the coding agent itself means making the previous Build ID current again: PINNED sessions keep their version, and a continue-as-new run on a newer version waits (fails its workflow task) until a version that understands its `CarriedState` is current again.

## Dry run, 2026-10-16

Run once on the local dev server (`127.0.0.1:7244`, never 7233), in a fresh namespace, by Eng H. The old app's code had already been deleted, so stub workflows with the old type names stood in for it (a scratch script, not committed). The CLI was run with `--disable-config-file --disable-config-env` so that it couldn't pick up a cloud profile.

| Step | Command | Result |
|---|---|---|
| Namespace | `temporal operator namespace create --namespace cutover-dryrun --retention 1d` | Registered |
| Search attribute | `temporal operator search-attribute create --namespace cutover-dryrun --name CodingAgentOwner --type Keyword` | Added |
| Old executions | Stub worker on queue `simple-chat-agent`; started `chat-dryrun-0..2` (`SimpleChatWorkflow`) and `user-chats-dryrun` (`UserChatsWorkflow`) | 4 Running |
| 1. Ship new worker | `python -m simple_chat_agent.worker.main` with `CODING_AGENT_TASK_QUEUE=coding-agent`, `TEMPORAL_DEPLOYMENT_NAME=advanced-ai-agent-coding`, `TEMPORAL_WORKER_BUILD_ID=ca-17b563f8e6f8` (sha256 of the `simple_chat_agent` tree and `uv.lock` at HEAD) | `coding-agent worker: task_queue=coding-agent versioning=on` |
| | `temporal worker deployment set-current-version --deployment-name advanced-ai-agent-coding --build-id ca-17b563f8e6f8 --yes` | Current version set |
| | `temporal workflow start --type CodingSessions --task-queue coding-agent --workflow-id coding-sessions-dryrun --input '{"owner_digest":"dryrun"}'` | Running, `Behavior Pinned`, `advanced-ai-agent-coding:ca-17b563f8e6f8` |
| 2. Old deployment kept | Stub worker left polling `simple-chat-agent` | Old executions untouched by step 1 |
| 3. Count | `temporal workflow count --query 'WorkflowType IN ("SimpleChatWorkflow", "UserChatsWorkflow") AND ExecutionStatus = "Running"'` | `Total: 4` |
| 3. Terminate | `temporal workflow terminate --query '<same>' --reason "Cutover to the coding agent (PRD §6, D4)" --yes` | Batch job `259c2e3e-…` Completed within 4 s |
| 3. Recount | same count | `Total: 0`; the 4 old executions show Terminated; `coding-sessions-dryrun` still Running |
| 3. S3 purge | n/a locally (disk storage) | Not exercised |
| 4. Decommission | Stopped the stub worker, then the new worker (SIGTERM) | Both exited cleanly, no errors in either log |

Findings:
- The batch query and termination behaved as written. The visibility query matches by type and status only, so it can't touch `CodingSessions` or `CodingAgent`.
- The worker refuses to start without provider keys (`No LLM provider keys are configured`). That is expected; the catalog brokers the keys.
- The local `temporal` CLI used a cloud profile by default. Pass the address, namespace and TLS settings explicitly, or the disable flags, when running this against a specific server.
