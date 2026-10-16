# Agent Harness

`agent_harness` is the provider-neutral core of the demo. It is not the product
application and it is not a model SDK wrapper. It is the reusable agent runtime
shape that the application composes inside Temporal workflow code.

The harness owns the generic rules that should remain true if the application
switches from Claude to another provider:

- how an agent run is sequenced;
- how context is recorded, windowed, snapshotted, and restored;
- how tools and guards are registered and executed;
- how tool side effects are routed through Temporal activities;
- how interrupts, steering, and continue-as-new are represented;
- how provider streaming is normalized into generic agent stream events.

The application still owns product behavior: login, user state, chat workflows,
tool availability, OAuth, artifacts, attachments storage, demo workspaces, and
UI rendering.

## Design Philosophy

The harness is intentionally split along deterministic boundaries.

Workflow-safe code may make policy decisions, update in-memory workflow state,
call deterministic conversion helpers, and schedule Temporal commands. It must
not perform network I/O, read environment variables, read clocks, or inspect
local files.

Non-deterministic work belongs in activities. Provider API calls, HTTP fetches,
GitHub calls, storage writes, sandbox execution, and Kubernetes operations all
run outside workflow code. The harness provides generic activity routers so
tool and guard authors can keep readable workflow history without registering a
new Temporal activity type for every small operation.

Provider-specific code is an adapter, not a second agent runtime. A provider
module converts between generic harness messages and a vendor request/response,
then calls the vendor SDK in an activity. The `Agent` class should not know
whether the provider is Claude, Gemini, ChatGPT, or something else.

## Design Review Checklist

Use the repo's `design-guides/` material as the review bar for harness changes:

- workflow code must stay deterministic and side-effect free;
- provider, tool, and guard side effects must run in activities with explicit
  timeout/retry choices;
- mutating activities need an idempotency strategy owned by the tool author;
- long-running activity work should heartbeat at a useful liveness interval;
- context snapshots and query payloads should stay bounded;
- continue-as-new state must carry enough information to resume without
  relying on old workflow history;
- cost optimizations should not erase the history needed to debug tool,
  guard, or provider failures.

If a change makes one of those checks harder to answer, keep the change in the
application layer or add a smaller harness primitive first.

## Module Map

| File | Responsibility |
| --- | --- |
| `agent.py` | Provider-neutral agent loop. Records context, calls the provider activity, executes requested tools, applies LLM guards, handles steering/interrupts, and returns continuation state. |
| `providers/interface.py` | The provider contract. Workflow-safe conversion methods plus the provider activity hook. |
| `providers/` | Vendor-specific implementations and convenience agent subclasses. |
| `context_manager.py` | Provider-neutral context manager protocol and token-budget shape. |
| `sliding_window_context_manager.py` | Default context manager implementation used by this demo. It compacts model-visible history and inserts compaction markers. |
| `messages.py` | Generic message and content block helpers used between the agent, providers, tools, and context manager. |
| `tools.py` | Tool decorators, `ToolSet`, `ToolContext`, tool activity routing entrypoint, idempotency key helper, and schema generation. |
| `guards.py` | Guard decorators, guard policy, guard execution, and guard activity routing entrypoint. |
| `llm_guards.py` | Pre/post LLM guard pipeline for provider requests and responses. |
| `activity_router.py` | Runtime router for tool/guard activity functions, including `RoutedActivityContext` and heartbeats. |
| `activity_options.py` | Shared activity option defaults and override helpers. |
| `invocation.py` | Shared callback invocation helper used by tools, guards, and routed activity functions. |
| `workflow_activities.py` | Shared workflow-side helper for scheduling routed tool and guard activities with readable summaries. |
| `streaming.py` | Generic sideband stream protocol and provider-facing `AgentStreamWriter`. |
| `attachments.py` | Generic attachment reference types. Storage and retrieval are application-owned. |
| `mcp.py`, `mcp_types.py` | HTTP MCP discovery and dynamic MCP tool adapter. |
| `tool_types.py` | Tool categories used by guard policy. |
| `errors.py` | User-facing error type for clean workflow-to-UI failures. |

## Agent Run Lifecycle

The generic loop in `Agent.run(...)` does the same sequence for every provider:

1. Restore state when resuming after continue-as-new.
2. Record the initiating user message and attachments when this is a new turn.
3. Build a provider request from the context manager's model-visible messages.
4. Run pre-LLM guards.
5. Schedule the provider API activity.
6. Run post-LLM guards.
7. If the provider returned tool calls, execute those tools in workflow context.
8. Record tool results in the context manager and continue the loop.
9. If the provider returned a final assistant message, record it and return an
   `AgentResult`.
10. If Temporal suggests continue-as-new mid-loop, return continuation state so
    the application workflow can continue-as-new at a clean point.

The loop treats partial streaming as sideband UX. Durable conversation state is
only committed when the provider activity returns and the workflow records the
assistant message or tool results. `configure_stream_sink(...)` also accepts one
optional post-LLM guard. When assigned, text deltas, thinking deltas, and
completed agent text pass through that guard before the sink emits them. With no
guard assigned, streaming is unchanged. Stream guards run in the provider
activity, so they must be pure in-process transforms and cannot schedule their
own Temporal activities.

## Context

The context manager is the transcript the model can actually see. It is not the
same as the product UI transcript. The UI may render only user messages and
final assistant responses; the context manager also records provider tool-use
blocks, tool results, steering context, guard rewrites, and compaction markers.

`ContextManager` is the provider-neutral interface. `SlidingWindowContextManager`
is the default implementation. It keeps a full context snapshot for the current
run, can produce a compacted continuation snapshot, and can return a bounded
message set for the next model call.

When a workflow continues as new, the application should carry the compacted
state returned by the agent. That keeps workflow histories bounded and prevents
old claim-checked payloads from being required indefinitely.

## Tools And Guards

Tools are workflow functions decorated with `@tool(...)`. They can perform
deterministic logic directly, and route side effects through `ctx.activity(...)`.
The routed activity uses one generic Temporal activity type:

```text
agent_harness.run_tool_activity
```

The activity summary is the tool name, or `tool_name:step` when a tool has
multiple side-effecting steps. This keeps history readable while avoiding a
large activity registration surface.

Guards follow the same pattern with `@guard(...)` and:

```text
agent_harness.run_guard_activity
```

Tool categories drive guard requirements. The harness ships with a built-in
`ToolType` enum for `READ`, `MUTATING`, `MCP`, and `ADMIN`, but applications can
also define their own string-compatible categories. By default, `MUTATING`,
`MCP`, and `ADMIN` tools require a pre-guard. This demo uses that to require
approval for mutating tools and third-party MCP tools.

Tool authors own idempotency. The harness can generate a stable
`ctx.idempotency_key(...)` from the stream, tool name, provider tool call id,
and any extra parts. The tool still has to enforce that key against the system
it mutates, or choose conservative retry options when enforcement is impossible.

The smallest useful setup is a workflow-owned `ToolSet` with explicit policy
and registrations:

```python
from agent_harness.guards import GuardPolicy
from agent_harness.tools import ToolSet

tools = ToolSet(
    guard_policy=GuardPolicy.require_pre("write_file"),
    tools=[read_file, write_file],
)
```

Use `tools.add_tool(...)`, `tools.add_guard(...)`, and `tools.add_provider(...)`
when the application assembles availability in stages.

## Provider Boundary

The provider interface is deliberately broad enough to keep `Agent` generic but
narrow enough to keep provider code honest.

Provider methods that run inside workflow code must be pure conversion methods:
no network, no clocks, no environment reads. Provider activities do the SDK
calls, retries, cancellation handling, heartbeats, and raw stream translation.

See [providers/README.md](providers/README.md) for the provider-specific
implementation contract.

## Streaming

The harness emits generic `agent_*` events through `AgentStreamWriter`.
Providers translate vendor stream events into these fixed event kinds:

- `agent_start`
- `agent_text_delta`
- `agent_thinking_start`
- `agent_thinking_delta`
- `agent_tool_input_start`
- `agent_tool_input_delta`
- `agent_tool_input_complete`
- `agent_complete`
- `agent_cancelled`
- `agent_failed`

The harness also emits correlated `harness_*` lifecycle events for LLM guards,
tools, tool guards, and routed tool/guard Activities. Each lifecycle payload has
an `operation_id`, and nested operations have a `parent_operation_id`. Activity
events include the Temporal attempt number so consumers can retain failed
attempts and render the transition into a retry. `StreamEvent.emitted_at`
contains the Activity-side UTC timestamp.

The sink is configured by the application runtime. The harness must not require
stream emission to succeed for workflow correctness. In this demo, the worker
emits sideband events to the API, and the API reconciles those events with
durable workflow snapshot/delta queries.

Agents can attach `stream_agent` metadata with a stable `id`, `parent_id`,
`kind`, and user-facing `label`. A child can also include
`parent_tool_call_id`, which links its runtime to the tool call that created it.
The same metadata follows provider deltas, tool and guard activities, and
sandbox output so a shared conversation stream can be grouped accurately when
multiple child agents run concurrently.

## Attachments

Attachments are represented as `AttachmentRef` values in generic messages. The
harness does not own blob storage or extraction. The application stores uploads,
creates attachment references, and exposes tools such as `read_attachment`.

If a provider supports native file or image inputs, that mapping belongs in the
provider conversion layer. The generic agent loop should continue to see stable
attachment references.

## What Should Not Go Here

Do not put these concerns in `agent_harness`:

- product auth or OAuth flows;
- user/session/chat registry persistence;
- concrete API routes or UI-specific state;
- provider account credentials;
- app-specific tools such as GitHub, artifacts, or demo workspaces;
- storage backends such as S3, DynamoDB, SQLite, or local artifact files.

Those concerns live in `simple_chat_agent`.

## Extending The Harness

When adding a generic capability, ask whether it would still make sense for a
different application and a different provider. If yes, it probably belongs
here. If it depends on this demo's UI, auth, storage, or business behavior, keep
it in the application layer and pass only provider-neutral references into the
harness.
