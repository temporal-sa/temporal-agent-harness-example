# Application Tools

`simple_chat_agent/worker/tools` contains the tool providers offered by this
demo application. These are not generic harness features. They are examples of
how an application can expose business capabilities to the provider-neutral
agent loop.

The generic tool framework lives in `agent_harness.tools`. This folder decides
which concrete tools exist, which users/chats can see them, how app credentials
are resolved, and which side effects each tool performs.

## Design Philosophy

Tool code runs in two places:

1. The decorated tool function runs in workflow context and must be
   deterministic.
2. Any side effect is scheduled through `ctx.activity(...)` and runs as a
   Temporal activity.

That split lets the workflow history show exactly which tool was requested,
which guard approved it, and which activity step performed the effect, while
keeping network/storage/process work outside deterministic workflow replay.

Tools should return `ToolResult(payload=..., error=True)` for expected,
LLM-visible failures. Unexpected exceptions from tool execution are converted
into LLM-visible tool errors by the harness so the chat can continue. Temporal
cancellation should still propagate.

## Registry And Availability

`__init__.py` builds the application `ToolSet`.

`build_tools(...)` registers every built-in provider:

- mutating-tool approval guard;
- attachment reader;
- artifact creator;
- `fetch_url`;
- persistent Bubblewrap workspace shell;
- research tools;
- GitHub tools;
- subagent tool;
- dynamic HTTP MCP tools.

`AppToolSet` filters that registry at runtime using the chat's
`available_tool_names`. A tool can be implemented and registered but still not
offered to a specific chat. Tool availability is chosen when a chat starts and
can be updated by workflow signals when connection state changes.

`tool_names_for_connections(...)` is the shared helper used by the API and
workflow to derive the initial visible tool list from GitHub connection state,
configured MCP servers, and enabled research providers.

## Tool Types And Approval

This demo uses the built-in categories from `agent_harness.tool_types.ToolType`.
Other applications can define their own string-compatible tool categories.

| Type | Meaning in this app |
| --- | --- |
| `READ` | Fetch or inspect data without intentionally mutating external state. |
| `MUTATING` | Creates, writes, executes user code, or changes an external system. |
| `MCP` | Third-party MCP tool; treated as approval-required by default because the app does not know if it mutates. |
| `ADMIN` | Reserved for high-risk administrative tools. |

The guard in `approval.py` fulfills both `MUTATING` and `MCP`. It pauses the
workflow and waits for the UI to approve or reject the requested tool call.

## Current Tool Providers

| File | Tools |
| --- | --- |
| `attachments.py` | `read_attachment` reads text-like uploaded attachments. |
| `artifacts.py` | `create_artifact` stores durable files that appear in the UI. |
| `fetch_url.py` | `fetch_url` retrieves a URL and returns extracted readable content, metadata, and useful links. |
| `github.py` | `github_authenticated_user`, `github_list_repositories`, `github_list_issues`, `github_open_issue`. |
| `workspace_shell.py` | `workspace_shell` runs Bash, Python, and other commands through the credential-free workspace executor. It also retains a hidden `python_sandbox` compatibility alias. |
| `research.py` | Optional SearXNG and Google-backed research tools. |
| `subagent.py` | `create_subagent` starts a child agent workflow for delegated work. Every child receives the tool and can delegate recursively; approvals remain anchored to the main chat workflow. |
| `approval.py` | Approval guard, not a user-visible tool. |

Dynamic HTTP MCP tools are registered by `AppToolSet._sync_mcp_tools()` from the
current chat's MCP server configs.

## Activities

Tool activities are normal Python functions that can be called by the generic
activity router. They must be module-level importable functions so the router
can serialize a stable function reference into workflow history.

Use `ctx.activity(...)` from a tool function:

```python
result = await ctx.activity(
    _my_tool_activity,
    step="write",
    args={"value": value},
)
```

Use `step=` when a tool schedules more than one activity. The harness enforces
this so Temporal history remains readable:

```text
my_tool:load
my_tool:write
my_tool:verify
```

## Idempotency

Temporal may retry activities. The harness cannot make arbitrary external
systems idempotent for a tool author.

For mutating tools, use `ctx.idempotency_key(...)` and enforce it in the target
system when possible. The GitHub issue tool is the reference example: GitHub
does not expose an idempotency key for issue creation, so the tool writes a
hidden marker into the issue body and searches for that marker before creating a
new issue on retry.

For tools where idempotency is impossible or unknowable, use conservative retry
policies and document the behavior. The workspace shell can execute arbitrary
commands, so it uses one Activity attempt and the executor records a completed
result by tool-call id. That prevents completed calls from being repeated but
cannot roll back partial filesystem or network side effects after a hard crash.
The executor also stays unready if it detects AWS credential variables, a
ServiceAccount token, any reachable metadata endpoint, or a reachable Kubernetes
API. This validates the egress rules installed by the Pod's short-lived firewall
init container and provides a runtime backstop for CNI-specific behavior.

## Heartbeats And Cancellation

Long-running tool activities should set `heartbeat_timeout`. The generic router
will send conservative liveness heartbeats, and activity implementations can
accept a `RoutedActivityContext` parameter to send meaningful progress details:

```python
from agent_harness.activity_router import RoutedActivityContext


async def _activity(
    *,
    activity_ctx: RoutedActivityContext,
) -> dict:
    activity_ctx.heartbeat({"phase": "loading"})
    ...
```

Heartbeat details are for Temporal liveness and cancellation, not high-frequency
UI progress. Use the sideband stream context for user-visible progress updates.

## Streaming

Activity functions can accept `StreamContext` when they need to emit tool
visibility events. The stream is sideband state and must not be required for
workflow correctness.

Use streaming for:

- workspace-shell stdout/stderr;
- large tool input/output visibility;
- user-facing progress during long-running activities.

Do not use streaming as durable state. Return the final tool result payload to
the workflow.

## Attachments And Artifacts

Attachments are user-provided inputs. The API stores bytes and passes
`AttachmentRef` values to the workflow. The `read_attachment` tool lets the
agent inspect text-like attachments by reference.

Artifacts are agent-created outputs. The `create_artifact` tool writes content
through storage and returns metadata that the UI can view or download.

Both use app storage, not workflow history, for file bytes.

## Research Tools

Research tools are feature-gated by environment:

- `SIMPLE_CHAT_SEARXNG_BASE_URL` enables `search_web`.
- `GOOGLE_API_KEY` or per-service Google keys enable Fact Check, Knowledge
  Graph, Books, YouTube, and Safe Browsing helpers.

If a provider is not configured, the corresponding tool disappears from the
Tools UI and is not offered to new chats.

## Adding A New Tool

1. Decide the `ToolType`.
2. Add a deterministic decorated tool function or provider class.
3. Route all side effects through `ctx.activity(...)`.
4. Define an idempotency strategy for mutating side effects.
5. Add approval guards when the tool mutates or invokes untrusted third-party
   behavior.
6. Add heartbeat timeouts for long-running activities.
7. Add stream events only for UI visibility, not correctness.
8. Include the tool in `build_tools(...)`.
9. Include it in `tool_names_for_connections(...)` or another feature gate when
   it should be offered to chats.
10. Update this README when the tool introduces a new design pattern.
