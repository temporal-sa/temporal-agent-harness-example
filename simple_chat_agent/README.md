# Simple Chat Agent

This is the demo application for the Claude harness. It is intentionally not a
generic agent SDK. It shows how an application can compose the harness with its
own workflow shape, auth model, tool registry, approval UX, artifact storage,
and streaming UI.

The app includes:

- A signal-driven `SimpleChatWorkflow` that owns chat state and the `ClaudeAgent`
  instance for one conversation.
- A `UserChatsWorkflow` entity workflow that tracks active chats and configured
  HTTP MCP servers for one user.
- A React/Vite frontend and FastAPI API with login, chat history, streaming
  visibility, tool approvals, tool configuration, and artifact viewing/downloading.
- A Temporal worker that hosts the chat workflows, subagent workflow, Claude
  activity, generic tool activity, and generic guard activity.
- Example tools for URL fetches, Python sandbox execution, artifact creation,
  GitHub operations, HTTP MCP servers, and subagents.

The Python code is split by runtime boundary:

- `simple_chat_agent/api/`: FastAPI app, OAuth flows, SSE, and HTTP API routes.
- `simple_chat_agent/worker/`: Temporal worker, workflows, tools, codec server,
  replay tooling, and sandbox Lambda code.
- `simple_chat_agent/common/`: shared storage, payload conversion, streaming,
  MCP auth, and environment helpers.
- `simple_chat_agent/frontend/`: React/Vite SPA and the production static
  frontend server.

## Quick Start

Install dependencies from the repository root:

```bash
uv sync
```

Create a repo-root `.env` file:

```bash
ANTHROPIC_API_KEY=sk-ant-...
ANTHROPIC_MODEL=claude-sonnet-4-5

SIMPLE_CHAT_JWT_SECRET=replace-me-for-any-shared-demo

# Sign in with Google (required to log in)
GOOGLE_OAUTH_CLIENT_ID=...
GOOGLE_OAUTH_CLIENT_SECRET=...
GOOGLE_OAUTH_REDIRECT_URI=http://127.0.0.1:8000/oauth/google/callback
GOOGLE_OAUTH_ALLOWED_DOMAIN=temporal.io
```

Start a local Temporal dev server:

```bash
temporal server start-dev
```

Start the worker in another terminal:

```bash
uv run python -m simple_chat_agent.worker.main
```

The worker also starts a Temporal Web codec server at
`http://127.0.0.1:8001` by default. Configure Temporal Web's codec endpoint to
that URL to decode claim-checked payloads from the local JSON-file external
storage.

Locally, the `python_sandbox` tool runs inside a subprocess owned by the worker.
For deployment, set `PYTHON_SANDBOX_LAMBDA_FUNCTION` on the worker to invoke a
dedicated executor Lambda instead. The Temporal Activity remains on the normal
worker so workflow history still shows sandbox schedule/start/close timing and
failures, but arbitrary Python executes outside the agent worker.

Package `simple_chat_agent.worker.sandbox.lambda_handler.lambda_handler` as the
Lambda handler. The executor Lambda does not need Temporal credentials or app
environment variables. The worker passes a narrow stream endpoint/token in the
Lambda invoke payload so long-running code can post stdout/stderr/progress
events back to the API; the sandbox child process still receives only its
minimal runner environment. Do not pass agent model keys, OAuth credentials,
artifact storage config, database config, app session secrets, or Temporal config
into the Lambda environment. The Lambda execution role should not have app IAM
permissions and the deploy script should force its configured environment to an
empty map. Before spawning sandbox code on Linux, the host process marks itself
non-dumpable where permitted; Lambda runtimes that deny that call fall back to
overwriting sensitive AWS/Lambda entries in the C process environment before
unsetting them so same-UID child code cannot recover those values through
`/proc/<pid>/environ`.

The worker that hosts the Temporal Activity needs permission to invoke only that
sandbox Lambda. Set `PYTHON_SANDBOX_STREAM_SINK_URL` on that worker to an app URL
reachable from Lambda; `/internal/stream` must route to the API. Set
`PYTHON_SANDBOX_LAMBDA_QUALIFIER` when invoking a
published version or alias. The activity retries Lambda invoke/control-plane
failures, but completed sandbox execution failures are returned to the LLM
instead of retried.

For local development, build the frontend once and let the API serve the static
files:

```bash
cd simple_chat_agent/frontend
npm ci
npm run build
cd ../..
uv run python -m simple_chat_agent.api.main
```

Open `http://127.0.0.1:8000` and log in. The default credentials are
`demo` / `demo` unless you override them in `.env`.

## What To Try

- Start a new chat and ask a normal question.
- Ask the agent to fetch a URL.
- Ask it to write and save a small Python file. The `create_artifact` tool will
  create a persistent artifact that can be viewed or downloaded in the UI.
- Ask it to run Python. The sandbox tool is a mutating tool, so the workflow will
  pause for an approval decision before the tool runs.
- Connect an HTTP MCP server from the Tools window and start a new chat so the
  chat workflow receives the updated tool list.

## Optional GitHub OAuth

GitHub tools are only offered to the agent after the user connects GitHub in the
UI. To enable the Connect flow, create a GitHub OAuth app with this callback:

```text
http://127.0.0.1:8000/oauth/github/callback
```

Then add these values to `.env`:

```bash
GITHUB_OAUTH_CLIENT_ID=...
GITHUB_OAUTH_CLIENT_SECRET=...
GITHUB_OAUTH_REDIRECT_URI=http://127.0.0.1:8000/oauth/github/callback
GITHUB_OAUTH_SCOPES=read:user,user:email,public_repo
```

`public_repo` is needed for the demo issue-creation tool against public
repositories. The issue-creation tool is also guarded by the same approval UI as
other mutating tools.

## HTTP MCP Servers

Open the Tools window in the web UI and choose `Add HTTP MCP`.

Supported auth modes:

- `none`: unauthenticated HTTP MCP server.
- `bearer`: user-provided bearer token stored in the app store.
- `oauth`: explicit MCP OAuth authorization, when supported by the MCP server.

Configured MCP servers are stored in the user entity workflow and broadcast to
active chat workflows as tool availability updates. The workflow receives the
MCP tools as regular Claude tools; the UI owns authentication and connection
management.

## Runtime Data

Local demo state is intentionally stored outside workflow code:

- `.simple_chat_agent/simple_chat.sqlite3`: chat metadata, OAuth connections,
  MCP connection records, and artifact metadata.
- `.simple_chat_agent/artifacts/`: created artifact files.
- `.simple_chat_agent/external_payloads.json`: file-backed payload codec storage
  for large Temporal payloads in the demo.
- `.simple_chat_streams/`: non-durable JSONL sideband stream files.

These paths are ignored by git and can be deleted to reset the demo.

Useful overrides:

```bash
SIMPLE_CHAT_DB_PATH=.simple_chat_agent/simple_chat.sqlite3
SIMPLE_CHAT_ARTIFACT_DIR=.simple_chat_agent/artifacts
SIMPLE_CHAT_EXTERNAL_STORAGE_PATH=.simple_chat_agent/external_payloads.json
SIMPLE_CHAT_EXTERNAL_STORAGE_THRESHOLD_BYTES=1024
SIMPLE_CHAT_CODEC_SERVER_ENABLED=true
SIMPLE_CHAT_CODEC_SERVER_HOST=127.0.0.1
SIMPLE_CHAT_CODEC_SERVER_PORT=8001
TEMPORAL_UI_URL=http://localhost:8233
```

## How The UI Gets Updates

The browser opens an SSE connection to:

```text
GET /api/sessions/{workflow_id}/events
```

The FastAPI app sends two kinds of events:

- `state`: durable workflow state, read by polling the `SimpleChatWorkflow.state`
  query. This includes transcript, status, pending approvals, tool availability,
  and artifacts.
- `stream`: non-durable sideband stream events. Local dev reads the JSONL stream
  file written by `JsonlStreamSink`; deployment receives the same events through
  the API-owned `/internal/stream` endpoint. This is used for Claude token
  deltas, streamed tool input construction, and tool activity visibility.

The workflow does not push directly to the browser.

## Zed Replay Debugging

The repo includes a Zed debugger profile at `.zed/debug.json` that launches the
Temporal Python replayer through:

```bash
python -m simple_chat_agent.worker.replay
```

Export a workflow history into the local scratch directory:

```bash
mkdir -p .replay
temporal workflow show \
  --workflow-id simple-chat-... \
  --output json > .replay/history.json
```

If you are replaying against Temporal Cloud or a non-default namespace, include
the same Temporal CLI address, namespace, TLS, and auth settings you normally use
when fetching the history.

Then edit `.zed/debug.json` and replace `simple-chat-REPLACE_ME` with the
workflow ID you exported. In Zed, open the debugger panel, choose
`Replay: Simple Chat Workflow History`, set breakpoints in workflow code, and
start the session.

The replay entrypoint uses the same workflow list and JSON-file external storage
data converter as the worker. If the history contains claim-checked payloads,
keep `.simple_chat_agent/external_payloads.json` from the original run available
locally before replaying.

## Troubleshooting

- `workflow not found`: your local Temporal dev server was probably reset while
  the browser still had an old chat selected. Start a new chat, or delete local
  runtime data.
- Claude calls fail immediately: check that `ANTHROPIC_API_KEY` is present in
  the repo-root `.env`, then restart both the worker and web process.
- GitHub stays disconnected: verify the OAuth app callback and the GitHub env
  vars, then restart the web process.
- The agent does not see a newly added MCP server: start a new chat or make sure
  the current chat received the tool availability update.
