# Local development

The README's [Run it locally](../README.md#run-it-locally) starts the stack. This page lists the settings you may want to change and the problems people run into.

## `.env.local`

`scripts/dev-secrets.sh` writes the three provider keys here (mode 600) and keeps any other lines you add. The Make targets source it without printing it. It is git- and docker-ignored; the script refuses to run if that ever stops being true.

| Variable | Used by | Purpose |
|---|---|---|
| `ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, `GEMINI_API_KEY` | worker | Provider keys. The worker refuses to start with none. `make dev-api` unsets them: the API never holds them. |
| `WORKSPACE_EXECUTOR_TOKEN` | worker, API, executor | Any random value, shared by all three |
| `TYPESAFE_API_KEY` | worker | Optional: auto mode uses Jev as its judge |
| `AGENT_MCP_LOOPBACK_PORTS` | worker, API | Ports on 127.0.0.1 that MCP servers may use locally (e.g. `8765`). Loopback is refused on every other port. |
| `AGENT_RESEARCH_TOOLS` | API | `fetch_url` (default) or `fetch_url,search_web` |
| `GOOGLE_SEARCH_GROUNDING_API_KEY` or `SIMPLE_CHAT_SEARXNG_BASE_URL` | worker | A backend for `search_web` |
| `AGENT_WEB_SEARCH` | API | `off` hides the provider web search toggle |
| `CONSOLE_ENABLED` | API | The harness console is on locally by default |
| `SIMPLE_CHAT_LOCAL_AUTH_USERNAME`, `SIMPLE_CHAT_LOCAL_AUTH_PASSWORD` | API | Local login (default `demo` / `demo`) |
| `SIMPLE_CHAT_JWT_SECRET` | API | Signs the local session cookie. `make dev-api` makes a fresh one per run if unset, so a restart signs you out. |
| `AGENT_SPEND_CAP_SESSION_TOKENS`, `AGENT_SPEND_CAP_USER_DAY_TOKENS`, `AGENT_SPEND_CAP_GLOBAL_DAY_TOKENS` | worker | Spend caps ([catalog.md](catalog.md#spend-caps) has the defaults) |
| `AGENT_MODELS_ANTHROPIC`, `AGENT_MODELS_OPENAI`, `AGENT_MODELS_GOOGLE` | worker, API | Override the model allowlists (comma-separated; the first is the default) |

The Make targets set the rest: `AGENT_ENVIRONMENT=local`, Temporal on `127.0.0.1:7244`, Redis on 6390, the executor on 8083 and Task Queue `coding-agent-dev`. Override any of them on the command line, e.g. `make dev-worker TASK_QUEUE=mine`.

## Ports

| Port | What |
|---|---|
| 7244 / 8244 | Temporal dev server and its UI. Never use the server on 7233. |
| 6390 | Redis |
| 8083 | Executor (container port 8082) |
| 8000 | API and the product view at `/` |
| 5173 | `make web-dev` (Vite) |

Storage is local by default: payloads, artifacts and the frame archive go to disk under the system temp directory. Set `SIMPLE_CHAT_S3_BUCKET` and the DynamoDB tables to use AWS instead.

## Troubleshooting

- **The worker exits with "No LLM provider keys are configured".** Run `scripts/dev-secrets.sh`, or put a key in `.env.local`.
- **The executor's `/healthz` fails with "No permissions to create a new namespace".** Run the container with `--security-opt seccomp=unconfined` (see [the executor README](../simple_chat_agent/worker/sandbox/README.md)).
- **The executor's `/healthz` fails its environment check.** It saw a provider or cloud key. Pass it only `WORKSPACE_EXECUTOR_TOKEN` and `WORKSPACE_EXECUTOR_ROOT`.
- **`make crash-worker` says no worker is running.** It matches `simple_chat_agent.agent.dev_worker`; start the worker with `make dev-worker`.
- **The `temporal` CLI talks to the wrong server.** A profile in your CLI config can point it at Temporal Cloud. Pass `--address 127.0.0.1:7244`, or `--disable-config-file --disable-config-env`.
- **Sessions vanished from the sidebar after Redis was flushed.** The API rebuilds the list from Temporal Visibility through the `CodingAgentOwner` search attribute. Start the dev server with `--search-attribute CodingAgentOwner=Keyword`.
