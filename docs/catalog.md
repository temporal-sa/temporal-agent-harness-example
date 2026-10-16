# Advanced AI Agent in the catalog

The catalog declaration is
`~/Source/cluster-gitops-config/projects/demo/advanced-ai-agent.yaml`. It tracks
the `catalog/advanced-ai-agent` source branch, initially based on
`refactor/agent-harness`. The application name is **Advanced AI Agent** and the
public URL is `https://advanced-ai-agent.tmprl-demo.cloud`.

The catalog owns login, ingress/TLS, Temporal namespace and credentials, Worker
Deployment identities, shared Anthropic and Gemini keys, container builds,
promotion, and crashable environments. This repository supplies the application
and container entrypoints. Its former Kubernetes manifests, infrastructure
pipelines, Google login, and workspace controller are retired on this branch.

## Runtime

| Component | Command | Purpose |
| --- | --- | --- |
| advanced-ai-agent-api | `python -m simple_chat_agent.api.main` | API and built React UI on port 8000 |
| advanced-ai-agent-worker | `python -m simple_chat_agent.worker.main` | Chat/user/subagent workflows and Activities; codec on 8001 |
| redis | `redis-server` | Best-effort streaming shared by API and worker |
| executor | `tini -g -- python -m simple_chat_agent.worker.sandbox.executor` | Credential-free Bubblewrap shell on port 8082 |

The catalog sets `SIMPLE_CHAT_API_HOST` and `SIMPLE_CHAT_CODEC_SERVER_HOST` to
`0.0.0.0` so Services and pod probes can reach their listeners. The API runs
without the development reloader. The internal worker codec requires a signed
Temporal Cloud access token; its root readiness endpoint remains accessible.

The API accepts the catalog's `X-Temporal-Auth-Subject` and
`X-Temporal-Auth-Email` headers. Ingress removes browser-supplied identity
headers before forward authentication. Keep the API behind that ingress; do
not expose its Service directly. Catalog mode never accepts the application's
local session cookie. Local development explicitly selects
`SIMPLE_CHAT_AUTH_MODE=local` and `SIMPLE_CHAT_LOCAL_AUTH_ENABLED=1`.

The catalog injects `SIMPLE_CHAT_TASK_QUEUE` into both API and worker for each
crashable environment. User identity includes that queue, so the user's chat
registry and attachment/OAuth records cannot collide with the always-on demo
or another workspace. Worker identity comes from `TEMPORAL_DEPLOYMENT_NAME`
and `TEMPORAL_WORKER_BUILD_ID`; the catalog also isolates those in clones.

Workers remain versioned and workflows are PINNED. The catalog renders a
1260-second pod shutdown window for the SDK's 1200-second drain timeout.
Crash/scale-down controls affect workers, leaving the API, Redis, and executor
available. Submit approvals or messages while workers are stopped, then scale
up to show Temporal delivering the buffered Signals. Messages preserve the
chat's current tool configuration if the MCP settings query times out.

## Storage and internal credentials

API and workers share private S3 claim-check/attachment/artifact bytes and
DynamoDB artifact/OAuth metadata. The catalog infrastructure definition is
`infra/advanced_ai_agent.tf`; it creates dedicated stores and the scoped
`tmprl-dem-cld-advanced-ai-agent-data` workload role. No static AWS keys are
used. `SIMPLE_CHAT_REQUIRE_SHARED_STORAGE=true` makes startup fail when the
shared stores are absent rather than falling back to pod-local payload files.
Do not expire S3 claims while a retained Temporal history may reference them.

The project runtime secret is
`tmprl-dem-cld/advanced-ai-agent/runtime`, with independently generated
`SIMPLE_CHAT_STREAM_TOKEN` and `WORKSPACE_EXECUTOR_TOKEN` properties. The
catalog provisions and distributes these credentials. Provider keys come from
`platform/shared/anthropic` and `platform/shared/gemini`; no user supplies an
LLM key through this application's UI.

Redis is visibility-only. Losing it can lose live chunks; Temporal state and
transcript reconciliation recover completed turns. Executor files persist
between shell calls for the lifetime of its pod. Worker crashes preserve those
files. Replacing the executor starts a fresh shell filesystem; generated
reports and attachments remain in S3/DynamoDB.

The executor uses the catalog's `isolatedExecutor` profile: no AWS/Temporal
credentials or service-account token, non-root, read-only root filesystem,
public HTTP/HTTPS egress only, and writable `/workspaces` and `/tmp`. Its
startup and health checks verify Bubblewrap, process reaping, and blocked
metadata/Kubernetes access. The sandbox masks `/proc`, other agent workspaces,
and the outer executor environment before executing user commands.

GitHub tools remain available for local development with explicit OAuth
configuration. The catalog deployment leaves them disabled; research,
fetching, attachments, artifacts, MCP configuration, shell execution, and
recursive subagent delegation remain available.

## Validation and rollout

Run backend tests with `.venv/bin/python -m unittest discover -s tests`, and
frontend checks with `npm test` and `npm run build` in
`simple_chat_agent/frontend`. Build the AMD64 application image with
`docker build --platform linux/amd64 -f simple_chat_agent/Dockerfile .`.

Validate the catalog with `python3 scripts/validate_projects.py`. Publish
isolated-executor support before applying the new project. Verify candidate
promotion, worker routing, login, a real agent conversation with a report,
cross-user ownership, and a crashable environment's stopped-worker approval
and recovery. `/healthz` gates basic startup; it does not prove those flows.

This branch starts in a fresh catalog namespace. It does not replace workers
polling old demo queues or migrate their histories. Keep the original branch
and old workers for any workflows still pinned to those deployments.
Later catalog runtime replacements also need old workers preserved until
compatible pinned executions have been replayed and moved to the replacement.
