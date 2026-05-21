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
- A FastAPI web app with login, chat history, streaming visibility, tool
  approvals, tool configuration, and artifact viewing/downloading.
- A Temporal worker that hosts the chat workflows, subagent workflow, Claude
  activity, generic tool activity, and generic guard activity.
- Example tools for URL fetches, Python sandbox execution, artifact creation,
  GitHub operations, HTTP MCP servers, and subagents.

## Quick Start

Install dependencies from the repository root:

```bash
uv sync
```

Create a repo-root `.env` file:

```bash
ANTHROPIC_API_KEY=sk-ant-...
ANTHROPIC_MODEL=claude-sonnet-4-5

SIMPLE_CHAT_USERNAME=demo
SIMPLE_CHAT_PASSWORD=demo
SIMPLE_CHAT_JWT_SECRET=replace-me-for-any-shared-demo
```

Start a local Temporal dev server:

```bash
temporal server start-dev
```

Start the worker in another terminal:

```bash
uv run python -m simple_chat_agent.worker
```

Start the web UI in a third terminal:

```bash
uv run python -m simple_chat_agent.web
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
- `oauth`: MCP OAuth discovery flow, when supported by the MCP server.

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
- `stream`: non-durable sideband stream events, read by tailing the JSONL stream
  file written by `JsonlStreamSink`. This is used for Claude token deltas,
  streamed tool input construction, and tool activity visibility.

The workflow does not push directly to the browser.

## Terminal Client

The older terminal client is still useful for quick signal and interrupt tests:

```bash
uv run python -m simple_chat_agent.chat
```

CLI commands:

```text
/steer <message>       Add steering before the next Claude call.
/after-tool <message>  Add steering after the next tool result.
/interrupt <message>   Cancel the in-flight Claude call and continue with context.
/queue <message>       Queue a normal chat message even while busy.
/status                Show workflow status.
/quit                  Exit.
```

Plain text is sent as a normal chat message while the workflow is idle. If
Claude is responding, plain text is sent as immediate steering.

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
