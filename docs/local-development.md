# Running Locally

This guide runs the demo on your machine with a single external dependency: the LLM provider. 

## Required Tools

- `uv`
  - Package: https://github.com/astral-sh/uv
  - Homebrew install: `brew install uv`
- Node.js 22 and `npm`
  - Package: https://nodejs.org/
  - Homebrew install: `brew install node@22`
  - `npm` is bundled with Node.js.
- Temporal CLI
  - Package: https://github.com/temporalio/cli
  - Homebrew install: `brew install temporal`

Optional tools:

- Docker, if you prefer running dependencies in containers.
- Docker buildx, if you want to validate the catalog image locally.

## Environment

Create a repo-root `.env` file:

```bash
ANTHROPIC_API_KEY=sk-ant-...
SIMPLE_CHAT_JWT_SECRET=replace-me-with-any-local-secret

SIMPLE_CHAT_AUTH_MODE=local
SIMPLE_CHAT_LOCAL_AUTH_ENABLED=1
SIMPLE_CHAT_LOCAL_AUTH_USERNAME=demo
SIMPLE_CHAT_LOCAL_AUTH_PASSWORD=demo

TEMPORAL_NAMESPACE=default
TEMPORAL_ENDPOINT=127.0.0.1:7233
TEMPORAL_TLS=0
SIMPLE_CHAT_TASK_QUEUE=simple-chat-agent
SIMPLE_CHAT_WORKER_VERSION=1.0.0
SIMPLE_CHAT_WORKER_VERSIONING_ENABLED=0
```

Deployed authentication is handled by the catalog. Local login is explicitly
selected by the settings above.

Optional local values:

```bash
# Enables GitHub tools after you connect GitHub from the Tools window.
GITHUB_OAUTH_CLIENT_ID=...
GITHUB_OAUTH_CLIENT_SECRET=...
GITHUB_OAUTH_REDIRECT_URI=http://127.0.0.1:8000/oauth/github/callback
GITHUB_OAUTH_SCOPES=read:user,user:email,public_repo

# Enables optional research tools.
SIMPLE_CHAT_SEARXNG_BASE_URL=http://127.0.0.1:8080
GOOGLE_API_KEY=...
```

## Optional Local SearXNG

The `search_web` tool is enabled when both the API and worker processes see
`SIMPLE_CHAT_SEARXNG_BASE_URL`. For local development, you can run SearXNG in
Docker and point the app at it:

Create a `settings.yml` file, for example at
`.local/searxng/config/settings.yml`:

```bash
mkdir -p .local/searxng/config .local/searxng/cache
```

```yaml
use_default_settings: true

general:
  instance_name: "Agent Harness Local Search"
  enable_metrics: false

search:
  safe_search: 1
  autocomplete: ""
  formats:
    - json

server:
  port: 8080
  bind_address: "0.0.0.0"
  secret_key: "local-agent-harness-searxng"
  limiter: false
  image_proxy: false
  method: "GET"

outgoing:
  request_timeout: 4.0
  max_request_timeout: 8.0
  useragent_suffix: "temporal-agent-harness-local"
```

Mount that config directory when you start the SearXNG container:

```bash
docker run --rm --name agent-harness-searxng \
  -p 8080:8080 \
  -e SEARXNG_BASE_URL=http://127.0.0.1:8080/ \
  -e SEARXNG_PORT=8080 \
  -e SEARXNG_BIND_ADDRESS=0.0.0.0 \
  -v "$PWD/.local/searxng/config:/etc/searxng" \
  -v "$PWD/.local/searxng/cache:/var/cache/searxng" \
  ghcr.io/searxng/searxng:latest
```

In your repo-root `.env`, set:

```bash
SIMPLE_CHAT_SEARXNG_BASE_URL=http://127.0.0.1:8080
```

Restart both the API and worker after changing the value. The API uses it to
advertise `search_web` in the Tools window, and the worker uses it when the
agent actually calls the tool.

You can sanity-check the local SearXNG instance before restarting the app:

```bash
curl 'http://127.0.0.1:8080/search?q=temporal&format=json'
```

## Install Dependencies

From the repo root:

```bash
uv sync
```

Build the frontend once so the API can serve it:

```bash
cd simple_chat_agent/frontend
npm ci
npm run build
cd ../..
```

## Start Temporal

In one terminal:

```bash
temporal server start-dev
```

Temporal Web will usually be available at `http://localhost:8233`.

## Start The Worker

In a second terminal:

```bash
uv run python -m simple_chat_agent.worker.main
```

The worker also starts a Temporal Web codec server at `http://127.0.0.1:8001`
by default. If you inspect claim-checked payloads in Temporal Web, configure
Temporal Web to use that codec endpoint.

## Start The API

In a third terminal:

```bash
uv run python -m simple_chat_agent.api.main
```

Open `http://127.0.0.1:8000`. With the local auth settings above, the login
screen shows a local username/password form. The default credentials are
`demo` / `demo`.

## Storage Defaults

When S3 and DynamoDB environment variables are unset, the app uses local
file/SQLite storage for claim-checks, artifacts, attachments, and OAuth records.
That is enough for local development and avoids requiring AWS credentials.

Local storage is not a production durability boundary. Deleting local state or
resetting the Temporal dev server can orphan old browser-selected chat ids; use
New Chat if that happens.
