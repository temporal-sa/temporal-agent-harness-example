# Worker Runtime

`simple_chat_agent/worker` contains the Temporal worker process and all workflow
code for the demo application. It is the durable execution boundary: chat
state, user chat registry state, approvals, tool
orchestration, and continue-as-new all live here.

The worker is not an HTTP server for the browser. The API sends signals,
updates, and queries to workflows. The worker executes those workflows and the
activities they schedule.

## Design Philosophy

Workflow code should own durable orchestration and deterministic state
transitions. Activities should own side effects.

That split is especially important for an agent harness:

- model API calls are activities;
- tool side effects are activities routed through the generic tool router;
- guard side effects are activities routed through the generic guard router;
- artifact writes, OAuth-backed API calls, URL fetches, sandbox execution, and
  Kubernetes operations happen outside workflow code;
- workflow code records only stable references and deterministic results.

The worker composes `agent_harness` with application-specific workflows and
tools. The harness stays provider-neutral; this application currently constructs
a `ClaudeAgent` for chat workflows.

## Entrypoints

| File | Responsibility |
| --- | --- |
| `main.py` | Connects to Temporal, configures stream/MCP/data-converter helpers, registers workflows and activities, and optionally runs the codec server. |
| `__main__.py` | Module entrypoint for `python -m simple_chat_agent.worker`. |
| `codec_server.py` | Temporal Web codec server for decoding claim-checked payloads. |
| `replay.py` | Local workflow-history replay entrypoint. |
| `streaming_activities.py` | Durable settlement activity used to send `turn_settled` to the API stream endpoint. |
| `workflow.py` | Per-chat workflow. |
| `user_chats_workflow.py` | Per-user registry/entity workflow. |
| `tools/` | Application tool providers and tool activities. |
| `sandbox/` | Credential-free Bubblewrap executor plus legacy Python sandbox compatibility. |

## Registered Workflows

`main.py` registers these workflows:

- `SimpleChatWorkflow`: one chat conversation.
- `UserChatsWorkflow`: one user registry, including chat list, MCP server
  configuration, .
- `SubagentWorkflow`: recursively delegating child agent workflow used by the
  `create_subagent` tool. The main workflow is depth 0; subagents through depth
  2 may delegate, while depth-3 subagents do not receive `create_subagent`.
  Every descendant keeps the main chat workflow as its approval and artifact
  authority while recording its immediate delegator for stream lineage.

The worker also registers the provider activity, generic tool and guard
activity routers, turn-settlement activity, .

## SimpleChatWorkflow

`SimpleChatWorkflow` owns one conversation. It receives user messages through
signals, queues them, runs the agent, records a rendered transcript for the UI,
and exposes bounded queries.

Important state:

- pending user messages and active message index;
- current tool availability, GitHub connection id, and MCP servers;
- pending approval requests;
- rendered transcript revisions and a bounded delta buffer;
- agent continuation state across continue-as-new;
- last touch time for run TTL behavior.

The workflow deliberately distinguishes two agent continuation states:

- `agent_state`: hot resume state when continue-as-new happens mid-agent-run;
- `agent_context_state`: compacted idle context when the workflow is between
  turns.

That distinction lets an agent run continue across workflow runs without
pretending it received a new user message.

The `run` method is organized around four steps:

1. restore carried workflow and agent state;
2. wait for a queued message or a safe checkpoint point;
3. activate one turn and run the agent;
4. clear active state, emit settlement, and continue as new when requested.

Signal handlers should continue to stay small: update workflow state, enqueue
work, or record approval decisions. The main loop reacts to that state and is
the place that schedules provider/tool activity work.

## UserChatsWorkflow

`UserChatsWorkflow` is an entity workflow keyed by user id. It creates chat
child workflows, tracks active chat records, stores configured MCP servers, and
tracks configured MCP servers.

The API asks this workflow to create chats instead of directly starting
`SimpleChatWorkflow`. That keeps per-user chat state in one durable place and
lets MCP/tool configuration changes be propagated consistently.

The registry has a 15-day run TTL. If it was touched during the run, it
continues as new and carries its compact state forward. If it was not touched,
it signals/cancels associated chats and exits.

## Catalog Workspaces

The catalog provisions, crashes, scales, and removes workspaces. The worker
hosts only the application workflows; it requires no Kubernetes permissions.
See [Catalog deployment](../../docs/catalog.md).

## Activities

The worker registers three broad activity categories:

- provider activity: `call_agent_api`, currently implemented by the Claude
  provider module;
- generic routers: `agent_harness.run_tool_activity` and
  `agent_harness.run_guard_activity`;
- application activities: turn-settlement,
  persistent workspace commands, artifact writes, GitHub calls, research calls, and
  other tool side effects.

Workflow code should schedule activities with explicit timeouts. Long-running
activities should set `heartbeat_timeout` and either rely on the generic router
auto-heartbeat or call `RoutedActivityContext.heartbeat(...)` at useful progress
points.

## Worker Versioning

The worker reads the catalog's `TEMPORAL_DEPLOYMENT_NAME` and
`TEMPORAL_WORKER_BUILD_ID`, with the existing `SIMPLE_CHAT_*` settings as local
fallbacks. Workflows default to PINNED. Local development can disable versioning
with `SIMPLE_CHAT_WORKER_VERSIONING_ENABLED=0`.

The catalog keeps worker identities separate for crashable environments. Do not
retire a version while workflows remain pinned to it. This catalog branch starts
in a fresh namespace; it does not migrate the previous deployments.

## Codec Server And External Payload Storage

The worker can run a Temporal Web codec server in-process. The codec server uses
the same data converter as the worker so Temporal Web can decode claim-checked
payloads.

External payload storage is configured by `simple_chat_agent.common`:

- local dev uses a JSON-file storage driver;
- deployed environments use S3 through Temporal's S3 external storage driver.

The worker, API, and codec must all use the same data converter and storage
configuration. If they do not, workflows may fail to decode activity inputs or
results after pod restarts.

## Streaming And Settlement

Provider and tool activities emit sideband stream events through a configured
stream sink. In Kubernetes, workers append directly to Redis Streams while the
API exposes that ordered log through authenticated SSE. Workspace executor output
returns over its authenticated internal response stream; the worker publishes it
to Redis without giving the sandbox Redis credentials. Local dev can use JSONL
stream files.

Most stream events are best-effort UX. The exception is turn settlement:
`SimpleChatWorkflow` commits transcript state, then schedules
`emit_turn_settled`. That activity atomically appends an idempotent settlement
event so the UI can reconcile final transcript state in order.

## Interrupts And Cancellation

The API signals interrupts to `SimpleChatWorkflow`. The workflow forwards the
interrupt into the agent. If a tool activity is running, Temporal cancellation
is requested through the activity handle. Heartbeating activities observe
cancellation promptly; non-heartbeating activities may not see cancellation
until their underlying operation returns.

Tool partial output can still be passed back to the model when the tool author
returns it as a tool result. Cancellation itself should not be swallowed.

## Replay

Use `replay.py` to replay exported workflow histories locally. This is the main
way to verify deterministic changes to workflow code before deploying.

Keep the matching external payload store available when replaying a history that
contains claim-checked payloads. Otherwise the replayer cannot decode historical
activity inputs or results.

## Adding Worker Behavior

When adding code here, first decide whether it is deterministic orchestration or
a side effect.

- Deterministic orchestration belongs in workflow methods.
- Side effects belong in activities.
- Product route/auth behavior belongs in the API.
- Generic provider-neutral agent behavior belongs in `agent_harness`.
- App-specific tools belong in `worker/tools`.

If new workflow state can grow over time, define a bound and a continue-as-new
strategy when adding it.
