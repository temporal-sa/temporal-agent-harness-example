# API Runtime

`simple_chat_agent/api` is the product control plane for the demo. It exposes
HTTP routes for the browser, owns login and OAuth flows, bridges browser actions
to Temporal workflows, and serves the sideband stream transport used by the UI.

The API should not be treated as durable agent memory. Durable conversation
state lives in Temporal workflows. Large payload bytes, OAuth records,
attachments, and artifacts live in the configured storage backends. The API
coordinates those systems and returns UI-shaped views of them.

## Design Philosophy

The API exists at the non-deterministic edge. It can read environment variables,
call Temporal clients, use the app store, perform OAuth handshakes, and receive
HTTP stream events from workers. It should keep those side effects out of
workflow code.

The API also owns product decisions that should not leak into `agent_harness`:

- who the current user is;
- which chat workflows belong to that user;
- which tools are enabled for a new chat;
- where uploaded attachment bytes are stored;
- how Google, GitHub, and MCP OAuth are initiated and completed;
- how browser snapshots, deltas, and stream events are reconciled.

## Runtime Initialization

`main.py` creates the FastAPI app and initializes shared process state in the
lifespan hook:

- a Temporal client configured with the shared data converter;
- `AppStore` for OAuth, artifact, attachment, and local metadata storage;
- a Redis-backed `StreamBroker` (or local JSONL fallback);
- MCP auth resolvers used by dynamic HTTP MCP tools.

Local development can serve the built frontend from the API process. In
Kubernetes, the dedicated web deployment serves static assets and the API
deployment serves only API and OAuth routes.

## Route Modules

| Module | Owns |
| --- | --- |
| `main.py` | App construction, Google OAuth, logout, internal stream ingest, shared dependency functions, router wiring, static fallback. |
| `routes/sessions.py` | Chat lifecycle, snapshots, transcript pages/deltas, chat/steer/interrupt signals, approvals, artifacts, attachments, and browser SSE routes. |
| `routes/tools.py` | Tools window data, GitHub OAuth, HTTP MCP configuration, MCP OAuth, and MCP enable/delete operations. |
| `routes/demo_workspace.py` | Main-to-temp workspace controls, temp workspace login handoff, crash/delete/create actions, and parent workspace state queries. |
| `schemas.py` | Request shapes accepted from the browser. |
| `serialization.py` | UI-shaped response dictionaries for workflow state, transcripts, artifacts, and attachments. |
| `streaming.py` | Redis/JSONL stream broker and SSE helpers. |
| `auth.py`, `local_auth.py`, `google_oauth.py`, `github_oauth.py` | Login/session/OAuth helpers. |
| `features.py`, `thinking.py`, `anthropic_models.py` | Runtime feature gates and model/thinking configuration. |
| `artifacts.py` | Artifact/attachment response helpers for view and download routes. |

## Temporal Boundary

The API talks to Temporal through client handles and generated dependency
functions. The browser never talks to Temporal directly.

The primary workflow interactions are:

- start or query the per-user `UserChatsWorkflow`;
- ask the registry workflow to create chat child workflows;
- signal `SimpleChatWorkflow` for chat input, steering, interrupt, approval,
  delete, and tool configuration changes;
- query bounded state snapshots, message pages, transcript deltas, and small
  state patches;
- update `DemoWorkspaceWorkflow` when creating, crashing, or deleting isolated
  demo workspaces.

The API validates ownership before exposing a workflow to the current user. It
describes the chat through Temporal and verifies that its parent is the current
user's deterministic `UserChatsWorkflow`; this service-side check remains
available when no worker is polling. Signal routes then send by workflow ID, and
registry touch bookkeeping is also a Signal, so worker outages do not prevent
Temporal from accepting interactive input. If a workflow is gone, the API
removes stale registry and artifact metadata where appropriate and returns a
clean 404 to the UI.

## Snapshot, Delta, And Stream Reconciliation

The UI has two data paths:

1. Durable state from workflow queries.
2. Best-effort sideband stream events from the worker.

Durable reads are deliberately bounded:

- `/api/sessions/{workflow_id}/snapshot` returns current workflow state plus a
  bounded latest transcript page and artifact/attachment metadata.
- `/api/sessions/{workflow_id}/messages` returns older transcript pages.
- `/api/sessions/{workflow_id}/messages/deltas` returns compact settled-message
  deltas after a known transcript revision.
- `/api/sessions/{workflow_id}/state/patch` returns a small non-transcript state
  patch when only status/tool/approval data changed.

Workers append stream events directly to Redis Streams, and `StreamBroker` reads
that ordered log for browser SSE and replay APIs. The important durable
transition is `turn_settled`: the workflow emits that as an activity after
committing the final turn state, so the browser can reconcile in-order with the
transcript revision carried by the event. Its idempotency key is enforced
atomically with the Redis append.

`/internal/stream` and `/internal/stream/event` remain as authenticated ingress
for rolling compatibility with older workers and the retired sandbox Lambda.
Those routes append to Redis rather than process memory. The workspace executor
returns output to the worker over its authenticated response stream instead.
Local development keeps the JSONL fallback when `SIMPLE_CHAT_REDIS_URL` is unset.

## Sessions And Chats

New chats are created through the user's registry workflow, not directly in the
API. This gives one durable place to track chat records, configured MCP servers,
and demo workspace state for the user.

For a new chat, the API chooses:

- model, output token cap, and context token cap;
- thinking config;
- initial tool availability;
- GitHub connection id, if connected;
- MCP server list;
- research tool availability based on environment configuration;
- whether the Good Place demo guard is enabled.

After creation, a chat workflow owns its own state. The API only signals it and
queries bounded rendered views.

## Auth And OAuth

The API stores a signed session cookie and derives the current
`AuthenticatedUser` on each request. The session format is provider-neutral;
Google OAuth and local testing auth are only ways to mint that session.

`GET /api/auth/config` tells the UI which login mode to render:

- `google`: Google OAuth is configured.
- `local`: local testing auth is explicitly enabled and Google OAuth is not
  configured.
- `none`: no login method is configured.

`POST /api/auth/local/login` is registered only in `local` mode. If
`GOOGLE_OAUTH_CLIENT_ID` and `GOOGLE_OAUTH_CLIENT_SECRET` are present, the local
route is not registered and the UI uses Google login. This keeps the local
demo/demo login unavailable in shared or deployed Google-authenticated
environments.

GitHub and MCP OAuth are optional tool-enablement flows. OAuth tokens are stored
in `AppStore`, keyed by user/provider. Workflows receive stable connection ids
or MCP server configs, never raw OAuth tokens.

Temporary demo workspaces route Google login through the parent environment so
the Google OAuth client can use a stable callback domain. The parent returns a
short-lived session token to the temp workspace login route.

## Attachments And Artifacts

The API accepts uploads and paste-block attachments, validates them, stores
bytes through the artifact store, and gives chat workflows only
`AttachmentRef` metadata. The agent can read text-like attachments through the
`read_attachment` tool.

Generated artifacts use the same storage path but are marked as agent-created.
The UI receives separate lists for generated artifacts and user attachments.

HTML artifacts have a dedicated preview response and render inside a sandboxed
iframe. The preview allows self-contained HTML, CSS, data images, and inline
SVG, but blocks scripts, network access, forms, nested frames, browser
capabilities, and access to the application origin. The normal inline artifact
route continues to serve HTML as plain text.

Artifact and attachment records expose expiration metadata. If retained bytes
expire, read/download routes return clear unavailable responses instead of
pretending the agent can still inspect the original content.

## Demo Workspace Mode

The parent environment can create isolated demo workspaces. The API starts or
updates `DemoWorkspaceWorkflow`, records the workspace on the user's registry,
and blocks temp-workspace registry recreation when the parent workspace is
deleted, deleting, failed, or otherwise unavailable.

Inside a temp workspace, the API is mostly the same application with different
environment values:

- a unique workflow prefix;
- a unique task queue;
- parent public URL for login handoff;
- feature flags hiding workspace creation controls and GitHub tooling where
  appropriate.

## Adding API Behavior

When adding a route, decide which layer owns the state:

- Workflow state should be changed through a signal, update, or child workflow.
- Browser-only rendering state should remain in the frontend.
- OAuth/storage state belongs in `AppStore` or a dedicated backend.
- Agent runtime behavior belongs in `agent_harness` or the worker, not the API.

Keep workflow query responses bounded. If a response can grow with conversation
length, add count and byte limits and make the UI fall back to snapshot/page
loads when deltas are too large.
