CTO:

# CTO memo 001: technical forks for the harness rewrite

- **Date:** 2026-10-16
- **Responds to:** `docs/PRD.md` v0.1, open items CTO-1 to CTO-4. This memo adds CTO-5 (continue-as-new and history budget) and CTO-6 (console and API surface).
- **Status:** Advice only. The owner decides. Nothing here changes repo code.
- **Audience:** engineers who will build P0 and P1.

## Sources and citation prefixes

Every claim below was checked against source. Line numbers refer to these trees:

| Prefix | What | Version |
|---|---|---|
| `TAH/` | `temporal-agent-harness` checkout (`temporal_agent_harness/`, `packages/`) | tag 0.7.0 |
| `PAI/` | `pydantic_ai/` from the `pydantic-ai-slim` wheel | 2.14.1 |
| `WS/` | `temporalio/contrib/workflow_streams/` | temporalio 1.34.0 |
| `NPM/` | `@temporalio/agent-harness-client` source in `TAH/packages/client/src` (matches the npm 0.1.0 `dist`) | 0.1.0 |
| repo paths | this repository at `50f6540` | — |
| `MONTY/` | `~/Source/temporal-demo-agent-harness` (the SA demo already on the harness) | harness 0.6.0 |

Reference code was read, not run. The numbers in CTO-5 are estimates from the source. P0 must measure them.

## Summary

| Fork | Recommendation | Main reason |
|---|---|---|
| CTO-1 Provider integration | Pydantic AI for all three providers. One `TemporalAgent` with a model registry, plus a small `harness_ext` module. | It's the only integration where one provider-neutral history serves Anthropic, OpenAI and Gemini, and the harness ships glue for it. |
| CTO-2 Stream bridge | One forwarder per session, hosted in the API and held by a Redis lease. It runs the harness's own merged `attach` and writes those frames to Redis. Telemetry goes on the same SSE stream as `x_*` frames. An API outbox holds sends while workers are down. | Each extra Temporal subscriber writes the stream into history again. Redis must hold the transcript anyway once we truncate at continue-as-new. |
| CTO-3 Workspace files | Executor `/files/*` endpoints with confined file I/O, a read/write lock, and one `ActivityFileSystem` that backs both the tools and the Code Mode VFS. | The shell path is serialized behind the exec lock and spawns `bwrap` on every call, which is too slow for Code Mode fan-out. |
| CTO-4 UI client | `@temporalio/agent-harness-react` and `-client` 0.1.0, pinned exactly, behind our own hook facade. Vendoring the source is the fallback. | The projection logic (approvals, callbacks, subagents, state patches) is about 1.1k lines we would otherwise rewrite. |
| CTO-5 Continue-as-new and history (new) | Declare `CodingAgent` with a bare `@workflow.defn` and our own run loop. Continue-as-new between turns and on a version change, from the first shipped version. Add history-budget controls. | With PINNED and no patching, a version that ships without continue-as-new strands its sessions on that version for good. |
| CTO-6 Console and API surface (new) | Serve the console's static assets, and back them with our own owner-scoped routes, as Monty does. Don't mount `create_agent_harness_app`. | The packaged app lists every session in the namespace, has open CORS, starts its own session manager, and its file viewer takes the config from the request. |

## Blockers found in the PRD

These stop parts of the PRD from shipping as written. Each one is handled in the fork named.

1. **"Accepted while workers are down" can't work through the harness API** (G5, A1, U4, §3 Worker row). `send_agent_message`, `tool_approval` and `provide_callback_result` are all Workflow Updates (`TAH/harness/agent_workflow.py:2068-2110`). A worker has to run the validator before an Update is accepted. Temporal doesn't buffer Updates durably while no worker is polling. `MONTY/README.md:118` hit exactly this. The status Query also needs a worker. Fix: an API outbox and status served from Redis (CTO-2).
2. **The harness owns `run`, and `run` exits only on `close`** (`agent_workflow.py:1109-1121`, `:1131-1141`, `:3005`). A18 has no place to call continue-as-new. `WorkflowStream.continue_as_new` can't pass `initial_versioning_behavior` (`WS/_stream.py:288-335`). Fix: CTO-5.
3. **S5 as written leaks sessions between users.** `create_agent_harness_app` lists every running agent in the namespace through a Visibility query (`TAH/web/app.py:253`, `:518-533`). It sets `allow_origins=["*"]` (`:188`) and starts the harness `SessionManagerWorkflow`. Its file viewer runs `vfs.<name>.view` with a `config` and `task_queue` taken from the request body (`TAH/web/file_view.py:83-90`, `:129`). Putting our auth in front doesn't scope any of this per user. Fix: CTO-6.
4. **History budget.** The harness flushes deltas every 50 ms by default (`agent_workflow.py:306`). The Pydantic AI glue uses that default (`pydantic_ai_harness.py:157`). Every flush is a Signal, and every subscriber poll is an Update (`WS/_client.py:438`, `:545`). On top of that, each model activity carries the whole conversation as its input (`PAI/durable_exec/temporal/_model.py:180-186`). One long coding turn can hit Temporal's limits before any between-turn continue-as-new runs. The PRD's 100–250 ms batch interval is necessary but probably not enough. Fix: CTO-5.
5. **A3 "takes effect from the next model call"** can't be done with the legacy `TemporalAgent`. The model is fixed for the whole run by a contextvar (`_model.py:301-314`, `_agent.py:444`). Fix: switch at the next turn, or use the capability path (CTO-1).
6. **A4 interrupt leaves dangling approvals.** If a task that is waiting at the approval gate gets cancelled, its entry stays PENDING. Only `close` finalizes it (`agent_workflow.py:609-622`). The legacy `TemporalAgent` also forbids `iter()` inside a workflow (`_agent.py:1074-1078`), so the partial history isn't directly available. Fix: CTO-1 gap-fillers.
7. **§5 rule 3 (executor scoping) can't be enforced as written for subagents.** Subagent IDs are `{key}-subagent-{uuid}` (`agent_workflow.py:3180`), and `activity.Info` has no root or parent ID. Fix: CTO-3.

## Corrections to the known facts

- *"Executing toolsets passed per run are rejected"*: this is partly wrong. A per-run `FunctionToolset` is accepted when every tool in it has `metadata={'temporal': False}` (`PAI/durable_exec/_runtime_toolsets.py:82-87`, `_agent.py:1083-1088`). Harness tools already run in the workflow, so A8 can expose per-user MCP tools as a per-run `FunctionToolset` of harness tools. Each tool delegates to one generic MCP activity tool. `ExternalToolset` isn't needed.
- *"The model activity does not heartbeat"*: true only for the legacy `TemporalAgent` (`_model.py:110-127`). Pydantic AI 2.14.1 also ships the `TemporalDurability` capability. It heartbeats in the background with a 30 s default timeout (`PAI/durable_exec/temporal/_durability.py:86-133`, `:267-273`).
- *"No event sink"*: confirmed. The event union is also closed. `AgentStreamItem` is a fixed discriminated union (`TAH/harness/agent_protocol/events.py:1068`), so we can't put custom events on the harness topic.

---

## CTO-1. Provider integration

### Findings

- The harness has three integrations: OpenAI Agents (vendored), Google GenAI (vendored), and Pydantic AI (thin glue, upstream plugin unmodified) (`TAH/ai_sdks/`). None is Anthropic-native.
- In the Pydantic AI glue, harness tools run in the workflow through `runner.run_tool`, so approvals and tool events stay with the harness (`pydantic_ai_harness.py:369-414`). The model stream is translated inside the model activity (`:328-354`).
- Gaps in the glue:
  - native-tool spans are dropped (`:125`);
  - `TokenUsage` maps only cache *reads* (`:266`), and has no cache-write field (`events.py:456-490`);
  - tool exceptions become strings the model sees (`:401-407`);
  - the batch interval can't be set (`:157`).
- When a handler is set, Pydantic AI runs **one activity per tool-call event and one per tool-result event**. This is true under both `TemporalAgent` (`_agent.py:243-268`) and the capability (`PAI/durable_exec/_base.py:158-163`). The harness admits it (`pydantic_ai_harness.py:341`). Each of these activities carries the full tool result again.
- `TemporalAgent` (the legacy wrapper the harness example uses, `TAH/examples/pydantic_ai_hello/workflow.py`):
  - no heartbeat;
  - `iter()` is forbidden in the workflow;
  - the model is fixed for each run;
  - `models=` takes a registry of `Model` instances (`_model.py:68-81`).
- `TemporalDurability` (the newer capability):
  - background heartbeat;
  - checkpoints Anthropic `pause_turn` continuations (`PAI/durable_exec/_utils.py:80-118`);
  - lets an outer capability swap the model per request (`_durability.py:211-217`);
  - doesn't block `iter()`.
  - Its activity result also returns every captured stream event (`_durability.py:346-364`, `_utils.py:145-176`).
  - The harness glue hasn't been tested on it: `build_harness_toolset` emits `tool_activity_config` for `TemporalAgent`, while the capability reads per-tool `metadata`.
- The OpenAI Agents integration is the most mature in the harness (`auto_heartbeater`, a `streaming_batch_interval` knob, `TAH/ai_sdks/openai_agents/_invoke_model_activity.py:358-415`). It can reach Anthropic and Gemini only through LiteLLM or any-llm (`oaa0220/agents/extensions/models/`). Its history is in OpenAI Responses format.

### Options

| Option | For | Against |
|---|---|---|
| **A. Pydantic AI, one `TemporalAgent` with a model registry** | Documented harness path with an example. One provider-neutral `list[ModelMessage]`, and `prepare_messages` translates history across providers (`_model.py:386-397`). One set of activities. | No heartbeat (we add one). No mid-run model switch. No `iter()`. Per-event handler activities (we suppress them). |
| A′. Pydantic AI on `TemporalDurability` | Heartbeat, `pause_turn` handling, per-request model swap and `iter()` come built in. | Harness glue untested on it. Activity results roughly double in size. Newer API. |
| B. A different SDK per provider (OpenAI Agents + GenAI + something for Anthropic) | Each SDK is native. | No shared history, so G3 (switching mid-chat) breaks. No harness integration for Anthropic. Three code paths. |
| C. OpenAI Agents + LiteLLM for Anthropic and Gemini | Best harness integration. | Lossy conversion of thinking blocks, caching and native web search for non-OpenAI models. A heavy dependency. |
| D. Our own loop and provider adapters | Full control. | This is the ~6k lines G1 tells us to delete. |

### Recommendation

**A**, with a P0 spike on **A′** as an exit if two or more A-gaps resist workarounds. `agent/harness_ext.py` holds the gap-fillers, each with a test pinned to 0.7.0:

1. `HeartbeatingModel(WrapperModel)`, registered in `models=`. It does three things:
   - heartbeats on a background task for the whole request, so the connect phase and silent reasoning are covered (A19's "heartbeat on each stream event" would time out on a quiet reasoning model);
   - maps provider 4xx other than 429 to a non-retryable `ApplicationError`;
   - builds provider clients with SDK retries off, so every attempt is a visible Temporal attempt.
2. Our own stream observer: the harness observer with `batch_interval` exposed (CTO-5 value).
3. A `TemporalAgent` subclass whose workflow-side `event_stream_handler` drains `HandleResponseEvent`s without running activities. The live model handler is unchanged, because `TemporalModel` captured it at init (`_agent.py:183-190`).
4. A3: change the model at the next turn (`run(model=<registry key>)`). Keep "next model call" only if we move to A′.
5. A4 interrupt:
   - cancel the handler's inner task, after resolving that turn's pending approvals and callbacks as denied through `runner._resolve_and_publish` (private);
   - recover the partial history from a `history_processor` snapshot taken before each model request, and synthesize cancelled tool results for the dangling calls.

### Question for the owner

> Do you approve Pydantic AI 2.14.x as the single integration for all three providers, with our own `harness_ext` gap-fillers (heartbeat, error mapping, batch interval, interrupt), instead of mixing the harness's per-provider SDK integrations? (yes/no)

### Test gate (P0 exit)

- One scripted turn on each provider streams text, thinking, a tool call and usage to a test client.
- One chat runs Claude, then GPT, then Gemini, then Claude. Each turn answers a question about an earlier turn's tool result. Native web-search parts in the history don't break the next provider.
- Killing the worker mid-stream on a reasoning model with no visible output fails the attempt within 1.5× the heartbeat timeout, and attempt 2 completes on another worker. A 25 s wait for the provider's first byte does *not* fail the attempt.
- A forced 400 fails the turn with no retry. A forced 429 retries, and the attempt count is visible.
- History check: a turn with 10 tool calls schedules no `…__event_stream_handler` activities.
- Interrupt during a pending approval, then during a running tool:
  - token flow stops within 2 s;
  - `agent_status.pending_approvals` is empty;
  - the next turn's history has a result block for every cancelled call.

---

## CTO-2. Stream bridge topology, Redis frame format, recovery telemetry

### Findings

- Every subscriber poll is an Update (`WS/_client.py:545`). An accepted Update writes its result, the polled items, into history. The module itself warns: "each poll grows workflow history" (`:518`).
- Topic filtering doesn't make polls rarer. The poll wakes on any new item in any topic (`WS/_stream.py:403-409`, `:426-437`). So a second, telemetry-only subscription costs as much history as the first one.
- Subagent streams aren't mirrored onto the parent's stream (`agent_workflow.py:3219`). The harness's merged `attach` mounts each child stream with a 10 ms poll cooldown (`TAH/harness/stream_merge/cursor.py:28`, `:94-99`).
- Harness `attach` replays and then **ends when the agent is idle** (`TAH/harness/agent_client.py:557-618`). `resume_offset` is a root-stream offset that only root events advance (`:579-594`).
- The SSE frame is `event: <type>` plus `data` = event fields + `agent_id`, `turn_id`, `turn_number`, `message_id`, `timestamp`, `resume_offset` (`TAH/web/app.py:739-761`).
- On the client:
  - every frame's `resume_offset` goes through `Math.max` (`NPM/session.ts:358`), so a frame without a number breaks resume;
  - unknown event types reach `default` and do nothing at runtime (`NPM/projection.ts:367-368`);
  - frames are deduplicated by `agent_id|event_offset` when the server sends `event_offset`, or by the whole JSON when it doesn't (`NPM/session.ts:458-465`).
- A truncated offset makes the stream client restart from the base offset without saying so (`WS/_client.py:557-563`).
- Our Redis store keeps streams for 30 minutes (`simple_chat_agent/common/redis_streams.py:25`).
- Buildkit guide 15:52 asks for one SSE stream per view. Guide 15:86 notes that every Signal, Update and Query is a billable Action.

### Options: topology

| Option | For | Against |
|---|---|---|
| **A. Forwarder in the API, one per active session, held by a Redis lease** | No new deployable; catalog entrypoints unchanged. One Temporal subscription per session, whatever the number of tabs. | Lease handover on API restarts. |
| B. Worker-side tee (the model activity writes to Redis) | Lowest latency. | Covers only activity-published events. Events published in the workflow (tool lifecycle, approvals, state patches, turn brackets) can't do I/O. Two sources lose their ordering. Retries duplicate. Workers need Redis credentials. |
| C. A separate forwarder process | Clean isolation. | A new component and a catalog change, against §6 "unchanged". |

### Options: what Redis stores

| Option | For | Against |
|---|---|---|
| **A. The harness-merged frames, verbatim, one Redis stream per session** | Reuses the harness merge (public `AgentClient.attach`). Attach is a plain read of the stream. | `event_offset` only for root frames. |
| B. Raw frames per workflow, merged per request | Exact offsets for every agent. | Reimplements `stream_merge` (~800 lines) against Redis. |

### Options: telemetry delivery (S3)

| Option | For | Against |
|---|---|---|
| **A. `x_*` frames on the same SSE stream, with the `resume_offset` of the frame before them** | One stream per view. Ordered with the transcript. `useAgentSession().frames` exposes them without a fork. | Outside the TypeScript frame union, so we cast. Retained in `frames`. |
| B. A separate SSE side stream | Clean types. | A second stream per view, against guide 15. Must be anchored to the transcript by ID. |
| C. A second Temporal topic | Produced by the worker. | Doubles poll history (see the findings). |

### Recommendation

**Topology A + storage A + telemetry A**, plus an outbox:

- **Forwarder loop.** Take the lease, then call `AgentClient.attach(from_offset=<last stored resume_offset>)`. When it ends (the agent is idle), wait for new work: an outbox send, a status change, or a 2 s Describe poll, which isn't a billable Action. Then attach again. Each session has exactly one Temporal reader.
- **Redis frame format v1:**
  - Stream `aah:{env}:s:{agent_wf_id}:f`.
  - Explicit entry ID `{resume_offset}-{seq}`, where `seq` counts frames that share a `resume_offset`.
  - Fields:
    - `v=1`;
    - `e` = the SSE event name;
    - `d` = the harness data JSON, plus `event_offset = resume_offset-1` on root frames;
    - `r` = the workflow run ID.
  - Meta hash `…:m` holds the head offset, `turn_active`, `current_turn`, pending counts and the last worker-health view. It serves `GET /status` **without a Temporal Query**, so status costs no Actions and works while workers are down.
  - Lease key `…:lease`: `SET NX PX 15000`, renewed every 5 s. After a handover, start from the current turn's first offset and drop frames already present by content hash.
  - TTL equal to the session TTL, not 30 minutes, with AOF on.
  - Optional later: coalesce `reply_delta` and `thought_summary` runs after `turn_end`.
- **Attach.** `XRANGE` from `{from_offset}-0`. This re-sends subagent frames that share that offset; client dedup absorbs them, and it's better than the harness's per-root-event loss. Then `XREAD BLOCK`. End at idle and caught-up, as the harness does. If Redis is down, fall back to harness `attach` directly; that needs workers.
- **Telemetry.** The bridge derives `x_recovery` and `x_usage`. Each carries anchors (`turn_id`, `message_id`, the model-interaction ordinal) and `confirmed: false`. The API confirms them from history with no workflow involvement:
  - the model activity's `ActivityTaskStarted.attempt > 1`;
  - its `identity`;
  - the last failure.
  - Cache-write tokens come from the `ModelResponse.usage` in the model activity's completion, read at `turn_end`. "Tokens not re-spent" is the sum of usage from model calls in the turn that completed before the retried attempt.
- **Outbox (fixes blocker 1).**
  - `POST messages`, `approve` and `callback-result` write an outbox entry keyed by `request_id`, then try the Update with `wait_for_stage=ACCEPTED` and the harness update ID `msg-{request_id}` (`TAH/web/app.py:730-736`).
  - If no worker accepts within 2 s, return `202 {state: "queued"}` and retry in the background.
  - Update IDs don't deduplicate across continue-as-new, so `CodingAgent` also keeps a bounded set of recent `request_id`s and carries it through continue-as-new.

### Question for the owner

> Do you approve one forwarder per session in the API, with Redis holding the merged frames for the session's life (it becomes the only full transcript once a run has continued-as-new), plus an API outbox that shows "queued" while workers are down? (yes/no)

### Test gate (P2 exit)

- Three tabs on one session. Temporal shows exactly one poller per workflow: one subscription chain per agent workflow, counted from `UpdateAccepted` events with the poll handler name.
- Kill every worker mid-turn:
  - a new tab still renders the full transcript from Redis;
  - `GET /status` answers in under 200 ms;
  - a message and an approval come back `queued`, then apply exactly once after the workers return;
  - a test that forces a duplicate retry across a continue-as-new doesn't duplicate the message.
- Kill the forwarder's API replica mid-turn. Another replica takes the lease within 20 s, and the transcript has no gaps and no duplicates in the client projection.
- A real worker kill produces an `x_recovery` frame that becomes `confirmed: true` within 10 s of the attempt completing. The "tokens not re-spent" number matches history.
- A contract test feeds `x_*` frames to client 0.1.0: the projection doesn't change, and `resume_offset` stays a number.

---

## CTO-3. Workspace file access and the Code Mode VFS

### Findings

- Today the executor has one endpoint, `POST /execute`. Each call:
  - takes a **per-workspace lock**;
  - spawns `bwrap`;
  - walks the workspace before and after to diff it;
  - caches the result by `execution_id` (`simple_chat_agent/worker/sandbox/executor.py:152-168`, `:185-201`, `:282-293`).
- Auth is one shared bearer token (`:474-483`). `workspace_id` is hashed into a directory (`:500-505`).
- `ActivityFileSystem` turns `stat`, `read`, `list` (plus `write`, `mkdir`, `delete`, `rename`) into `vfs.<name>.*` activities (`TAH/harness/code_mode/activity_fs.py:143-232`, `:274-348`). It treats the config as untrusted (`:24-29`) and reads whole files into activity results, so into history (`:351-388`).
- Each VFS operation is also an `fs_*` harness tool call, gated by approval (`TAH/harness/code_mode/vfs.py:8-12`).
- Monty steps run in the workflow thread under a deadlock watchdog. Aborts are recorded with harness-internal `workflow.patched` markers (`TAH/harness/code_mode/driver.py:193-214`). That's consistent with the PRD's carve-out.
- `activity.Info` has `workflow_id` but no root or parent ID (temporalio 1.34 `activity.py:107-141`). Describe does expose `root_id` and `parent_id` (`temporalio/client/_workflow.py:1263-1347`). Our `CodingAgent` is a child of `UserChatsWorkflow`, so the root is per user, not per session.

### Options

| Option | For | Against |
|---|---|---|
| **A. Executor `/files/*` (`stat`, `read` with range, `list`, `write`, `edit`, `mkdir`, `delete`, `rename`)** | Milliseconds per call. Not stuck behind a long `run_command`. Binary-safe. One backend for the tools, the Code Mode VFS and the UI Files tab. | New attack surface outside `bwrap`. Symlinks the agent creates in its workspace can point anywhere the executor can read. |
| B. Shell-based (`cat`, `ls`, `stat` through `/execute`) | Inherits `bwrap` isolation. No new endpoints. | Serialized behind the exec lock: a 5-minute `pytest` blocks every read. Spawns `bwrap` and walks the workspace on each call. A ten-read Code Mode `gather` runs one at a time. |
| C. `/files/*` run inside a per-request `bwrap` | Isolation of B, no lock contention. | Still pays the spawn cost on each call. |

### Recommendation

**A**, with these controls:

- **Path confinement.** Resolve each path one component at a time with `os.open(..., O_NOFOLLOW, dir_fd=…)` from the workspace root. Reject `..`, absolute paths and any symlink, including one at the leaf.
- **Size caps.** Read 1 MB per call; anything bigger returns an error telling the model to use `run_command`. Write 5 MB.
- **Locking.** A per-workspace read/write lock: reads are shared, and writes take the exclusive side against `/execute`.
- `search_code` stays on `/execute` (`rg` inside `bwrap`; one call per search, so the cost is fine).
- One `ExecutorWorkspaceFS(ActivityFileSystem[WorkspaceFsConfig], name="workspace")` backs both the A5 file tools and the A11 `/workspace` mount.
- **Scoping, replacing §5 rule 3:**
  1. `workspace_id` is built in `@agent.init` and passed as `Injected[...]` or in the mount config. The model never controls it.
  2. Every workspace activity resolves the owning `CodingAgent`:
     - describe `activity.info().workflow_id`;
     - walk `parent_id` up at most three levels until the type is `CodingAgent`;
     - cache the answer per workflow ID in the process.
     
     It rejects a `workspace_id` that isn't derived from that agent's ID. The config can carry a token instead, but a bearer token in history goes against §1's no-secrets boundary, so it isn't recommended.
  3. API paths (the Files tab and the console viewer) check session ownership before calling the executor directly. They don't use standalone activities, which need server support (`TAH/web/file_view.py:130-137`).

### Question for the owner

> Do you approve adding `/files/*` endpoints to the executor that run outside Bubblewrap but are confined by `O_NOFOLLOW` path walking, with workspace ownership checked by walking each workflow's parent chain? (yes/no)

### Test gate (P1 exit, A5/A11)

- A security suite against `/files/*`:
  - `../`, absolute paths and encoded traversal;
  - a symlink to `/etc/passwd` and one to another workspace, at the leaf and mid-path;
  - a hardlink created in the sandbox;
  - a FIFO;
  - a 2 GB sparse file.
  
  Every case is refused or capped.
- An activity scheduled by workflow X with workspace Y's config is rejected, including from a depth-2 subagent and from a forged console viewer request.
- During a 60 s `run_command`, `read_file` returns in under 100 ms (p95) on the catalog cluster.
- A Code Mode script that reads 20 files with `asyncio.gather` finishes in under 3 s. Each read appears as its own tool event and passes the approval gate.
- The executor isolation self-tests still pass (`executor.py:527-619`).

---

## CTO-4. UI client library

### Findings

- `SessionTransport` is a small interface. `HttpTransport` takes `baseUrl`, `fetch` and `headers` (`NPM/transport.ts:36-62`), so our auth fits without a fork.
- The session core handles:
  - reconnect with backoff;
  - idle polling of `agentStatus`;
  - callback-tool dispatch;
  - frame dedup (`NPM/session.ts:243-332`, `:350-404`).
- The projection covers every harness event, including subagents and state patches (`NPM/projection.ts:163-370`).
- MONTY ships on the Svelte binding with its own scoped routes (`MONTY/README.md:63`, `:79-85`).
- Costs we'll inherit:
  - `frames` grows by `concat` on every flush (`TAH/packages/react/src/state.ts:93-95`);
  - without `event_offset`, the dedup key is the whole frame JSON (`NPM/session.ts:458-465`). Long coding sessions will have tens of thousands of frames.
- The codegen package isn't on npm. Types are generated from a harness checkout (`MONTY/README.md:68-75`).

### Options

| Option | For | Against |
|---|---|---|
| **A. `-react` + `-client` 0.1.0, pinned exactly, behind our `useCodingSession` facade** | No protocol code to write. Same projection the console uses. A proven pattern (MONTY). | 0.x with one release. Frame retention cost. |
| B. `-client` core plus our own React hook | Control over frame retention. | We maintain a binding (the react package is three files). |
| C. Our own client | Full control. | Rewrites roughly 1.1k lines of projection and session logic that the harness keeps current with its event schema. |

### Recommendation

**A.** Components import only our facade, so moving to B, or to a vendored copy of the MIT client source, is a local change. The server sends `event_offset` on root frames (CTO-2) so dedup keys stay short. Types are generated from `CodingAgent` with the harness codegen at the pinned tag, and committed.

### Question for the owner

> Do you approve building the UI on `@temporalio/agent-harness-react` and `-client` 0.1.0, pinned exactly, with vendoring the MIT source as the fallback, instead of writing our own client? (yes/no)

### Test gate (P2/P3)

- `useAgentSession` smoke test against our API with no patches (S1 acceptance).
- A recorded session of 50,000 frames replays from offset 0 in under 3 s on a mid-range laptop, and the tab stays under 300 MB heap.
- At 320 px, the replay renders without a long task over 200 ms after the first paint.
- Reconnect after a forced SSE drop mid-turn produces no duplicate parts.
- Two tabs see the same approval card. Approving in one resolves it in the other within 1 s.

---

## CTO-5 (new). Continue-as-new and history budget

This is the costliest fork to reverse. The worker is PINNED (`simple_chat_agent/worker/main.py:280`) and patching is forbidden. The first `CodingAgent` version that ships without continue-as-new on a version change keeps its sessions on that version until they close. Its carried-state schema is also the only contract between versions, because a v1 run that continues-as-new hands v1-shaped input to v2.

### Findings

- The harness generates `run` and rejects one written by the author (`agent_workflow.py:1099-1141`). Its loop exits only on `close` (`:3005-3027`).
- A bare `@workflow.defn` agent is supported:
  - handler discovery falls back (`:967-975`);
  - init data falls back (`:792-802`);
  - the signature backstop runs (`:698-724`).
- Admission has no `await` (`:2119-2200`), so status read in the same activation is authoritative.
- What can be restored through public API:
  - approval policy, including the remembered allow-list from `with_tool_allowed` (`:2293-2295`), through `AgentConfig.approval_policy`;
  - criteria;
  - `agent_id` (`:2021`).
- The turn counter can't be restored publicly (`_WorkflowStatus._current_turn`, `:1539`). After continue-as-new, `turn_number` restarts at 1. The client's idle poll compares `current_turn` with the last turn it saw (`NPM/session.ts:320-324`), so it would miss work that another tab starts.
- Subagents are started with `ParentClosePolicy.TERMINATE` (`:3214`). Continuing as new while a subagent runs kills it.
- `WorkflowStream` keeps its whole log in memory and copies it into the continue-as-new state (`WS/_stream.py:237-268`), unless we call `truncate` first (`:337-364`).
- `UserChatsWorkflow` already continues as new with `AUTO_UPGRADE` (`user_chats_workflow.py:159-185`).
- History growth per turn. These are estimates for P0 to measure:

| Source | Mechanism | Per unit |
|---|---|---|
| Delta flushes | 1 Signal, about 1 workflow task (3 events), and about 1 poll Update completed and accepted (2 events) | ~6 events per flush. At 50 ms: ~120 events/s while streaming, so ~1.8k events for a 15 s call. |
| Model call input | The whole `messages` list in `ActivityTaskScheduled` (`_model.py:180-186`). The offload threshold is 1.5 MB (`TAH/utils/large_payload.py:72`). | ~0.4 MB per call at 100k tokens of context, stored inline. |
| Tool events | 2 handler activities per tool call (CTO-1) | ~6 events and the result payload twice. |
| Subagent stream | The parent's merge polls the child every 10 ms | The child pays the delta cost again. |

Against Temporal Cloud's limits of 51,200 events and 50 MB per run (warnings at 10,240 events and 10 MB):

- At 50 ms, about 28 streamed calls hit the event limit. One coding turn can do that.
- A 40-call turn writes about 16 MB of model inputs alone.

Every Signal and Update is also a billable Action, so the 50 ms default costs about 20 Actions per second per streaming session.

### Options

| Option | For | Against |
|---|---|---|
| A. No continue-as-new; short sessions only | Simplest. | Sessions are stuck on their version. A long turn still fails. |
| **B. Bare `@workflow.defn` with our own run loop** | Uses public API apart from one private write (the turn counter). Continue-as-new happens synchronously in the same activation, so there's no window for a message to be admitted mid-drain. | We own about 80 lines of the loop. |
| C. Keep `@agent.defn`; set the private `runner._closed`, then continue-as-new in `@agent.teardown` | Keeps the decorator. | More private API. Messages admitted but not yet run are dropped, because the loop exits without dispatching them. |
| D. Wait for upstream support | No glue. | No date. |

### Recommendation

**B**, built in from P1. It isn't optional polish.

- `run` runs `runner.run(self)` as a task. It waits on: closed, or (no turn active, no pending turns, approvals, callbacks or subagents) and (`is_continue_as_new_suggested()`, or `is_target_worker_deployment_version_changed()`, or the run's own budget is spent).
- When the condition fires, it re-checks without awaiting. It then:
  1. truncates the stream to the current turn's start offset (Redis already holds the earlier frames, CTO-2);
  2. snapshots the carried state;
  3. calls `workflow.continue_as_new(args=[config, data], initial_versioning_behavior=AUTO_UPGRADE)`.
  
  Stream pollers that are in flight follow the new run (`WS/_client.py:573-578`).
- A durable timer (1 hour) wakes idle sessions so they can move after a deploy.
- Carried state lives in `CodingAgentInit.resume`, typed as `CarriedState | None`, never `Any` (`WS/_stream.py:120-125`):
  - `state_version`;
  - conversation;
  - `WorkflowStreamState`;
  - turn counter;
  - recent `request_id`s;
  - plan, changed files and workspace state;
  - model and settings.
  
  Every field gets a default, so a newer version reads older input.
- Budget controls:
  - batch interval of **300–500 ms** for model deltas; the UI smooths text so it still reads as typing;
  - suppress per-event handler activities (CTO-1);
  - payload offload threshold of **256 KB** on S3, for workers, API and console alike. The cost: Temporal UI shows claim references instead of message bodies;
  - `tool_end` output truncated in events (the full output stays in the workspace);
  - a per-turn soft cap on model calls, set from P0 data so that a capped turn stays under 10,240 events and 10 MB. At the cap the agent asks "Continue?".

### Question for the owner

> Do you approve declaring `CodingAgent` with a bare `@workflow.defn` and our own run loop, so that continue-as-new between turns and on deploys ships in the first version, at the cost of owning about 80 lines the harness would otherwise generate and one private write (the turn counter)? (yes/no)

### Test gate (P0 measure, P1 exit)

- P0 measures events and bytes per streamed second, per model call and per tool call at 50, 250 and 500 ms on the dev server. The P1 soft cap is set from that data.
- In a forced continue-as-new test, these all survive:
  - the conversation;
  - the remembered allow-list;
  - posture and criteria;
  - plan, changed files and model;
  - monotonic stream offsets;
  - `turn_number`.
  
  A second tab that was idle during the continue-as-new picks up the next turn.
- A message sent in the same workflow task that decides to continue-as-new is neither lost nor duplicated.
- Deploying version N+1 moves a busy session at its next turn boundary and an idle session within the timer period. No workflow task fails on replay.
- A replay test runs in CI over recorded `CodingAgent` histories, including one with a continue-as-new (§6).
- The longest scripted P1 coding task ends with history under 10,240 events and 10 MB per run.

---

## CTO-6 (new). Console embedding and API surface

### Findings

The packaged app can't be made per-user (blocker 3). MONTY serves the console's static files unchanged and mounts its own owner-scoped routes at `/console/api` (`MONTY/README.md:79-85`, `:100-104`). It imports a few private helpers from `temporal_agent_harness.web.app` to send byte-identical responses (`MONTY/README.md:119`).

### Options

| Option | For | Against |
|---|---|---|
| **A. Static console, served by our routes (owner-scoped, CTO-2-backed `attach` and `status`)** | One auth model. Workers-down behaviour matches the product UI. | We must cover the routes the console calls: `/api/agents`, `/api/sessions`, `attach`, `messages`, `approve`, `callback-result`, `status`, `workflow-status`, `close`, `agent-interface`, `files/view`. |
| B. Mount `create_agent_harness_app` behind auth | No code. | Leaks across users. Starts a second session manager. Takes file-viewer config from the request. |

### Recommendation

**A.** `/api/files/view` checks that the session owns the mount's `workspace_id`, then reads through the executor. `CONSOLE_ENABLED` stays off in the catalog until the scoping tests pass.

### Question for the owner

> Do you approve replacing S5's `create_agent_harness_app` mount with the unchanged console assets served by our own owner-scoped routes? (yes/no)

### Test gate (P2)

- With two users, every console route returns 404 for the other user's session and subagents, including `files/view` with a forged `source.config`.
- No CORS header is sent for any origin except our own.
- Opening the console from a chat deep-links to the live loop (`?s=`).

---

## Private harness internals we'd depend on

All of these live in `agent/harness_ext.py` with tests pinned to 0.7.0. Review the list on every harness upgrade.

| Internal | Why |
|---|---|
| `runner._resolve_and_publish` | Deny a turn's pending approvals and callbacks on interrupt (CTO-1). |
| `runner._status._current_turn` | Restore the turn counter after continue-as-new (CTO-5). |
| Overriding the `TemporalAgent.event_stream_handler` property | Suppress the per-event handler activities (CTO-1). |
| `PydanticAIStreamObserver` internals | Expose `batch_interval` (CTO-1, CTO-5). |
| `temporal_agent_harness.web.app` helpers (`_yield_item`, `_sse`) | Send byte-identical frames (CTO-2, CTO-6). |
