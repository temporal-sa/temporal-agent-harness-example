# Advanced AI Agent in the catalog

These are the facts the catalog declaration (`~/Source/cluster-gitops-config/projects/demo/advanced-ai-agent.yaml`) needs for the coding agent. The public URL stays `https://advanced-ai-agent.tmprl-demo.cloud`. The catalog owns login, ingress and TLS, the Temporal namespace and credentials, Worker Deployment identities, the shared LLM keys, image builds, promotion and crashable environments. This repo supplies the app and its container entrypoints. To move from the old chat app, follow [CUTOVER.md](CUTOVER.md).

## Containers

One image (`simple_chat_agent/Dockerfile`, `make image`) serves three commands:

| Component | Command | Port | Health |
|---|---|---|---|
| API and UI | `python -m simple_chat_agent.api.main` | 8000 (UI at `/`; `/app/…` redirects to `/`) | `/healthz` |
| Worker | `python -m simple_chat_agent.worker.main` | 8001 (codec server) | codec `/health` |
| Executor | `tini -g -- python -m simple_chat_agent.worker.sandbox.executor` | 8082 | `/healthz` (also fails if it can see any LLM key) |
| Redis | `redis-server --appendonly yes` | 6379 | |

Port 8080 (the old frontend) is gone. The image includes pytest, which the seeded demo repo's tests need inside the sandbox.

## Environment

### Keys

Keys are brokered by the catalog; the app never asks a user for one.

| Variable | Container | Notes |
|---|---|---|
| `ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, `GEMINI_API_KEY` | worker only | From `platform/shared/{anthropic,openai,gemini}`. The project yaml requests the OpenAI key too. The API runs without provider keys (P2 decision 2). |
| `TYPESAFE_API_KEY` | worker | Optional. When set, auto mode uses Jev as its judge; otherwise the LLM judge on a fixed Haiku-tier model. |
| `WORKSPACE_EXECUTOR_TOKEN` | API, worker, executor | Bearer token for the executor. From the project runtime secret. |
| `SIMPLE_CHAT_JWT_SECRET` | API | Local login only. Not used in the catalog. |

`SIMPLE_CHAT_STREAM_TOKEN` and `SIMPLE_CHAT_GOOD_PLACE` are no longer read; drop them.

### Temporal, Task Queue and Worker Deployment

| Variable | Container | Value |
|---|---|---|
| `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`, `TEMPORAL_API_KEY`, `TEMPORAL_TLS` | API, worker | From the catalog |
| `CODING_AGENT_TASK_QUEUE` | API **and** worker | Default `coding-agent`. Each crashable environment injects its own into both. |
| `SIMPLE_CHAT_TASK_QUEUE` | API **and** worker | Keep injecting it: user identity and the Redis key prefix include it, so a clone's users and data can't collide with the always-on demo. |
| `TEMPORAL_DEPLOYMENT_NAME` | worker | A **new** name for the coding agent (not the old app's). |
| `TEMPORAL_WORKER_BUILD_ID` | worker | **Required**; the worker refuses to start without it. Derived from the source plus `uv.lock`; never reused for different code. |
| `CODING_AGENT_WORKER_VERSIONING_ENABLED` | worker | Default `true`. Workflows are PINNED; there is no patching. |

The worker's graceful drain is 1200 s; keep the pod's shutdown window at 1260 s.

### Search attribute

Register `CodingAgentOwner` (Keyword) in the namespace before the first deploy. Every `CodingAgent` and `CodingSessions` workflow carries its owner's digest there. The API rebuilds a lost session list from Visibility with it (no Query), and owner checks fall back to it.

### Feature defaults in the catalog

`AGENT_ENVIRONMENT` defaults to `catalog`, which is the stricter mode. Local development sets `local`.

| Feature | Catalog default | Switch |
|---|---|---|
| Harness console (`/console/`) | Off | `CONSOLE_ENABLED=1`. Its routes are owner-scoped and cross-user tested (D8). |
| MCP servers | Off, and the routes refuse to register anything (D7) | Local only in release 1 |
| Provider web search | Off | `AGENT_WEB_SEARCH_CATALOG=1` makes it available; each session still starts with it off. `AGENT_WEB_SEARCH=off` turns it off everywhere. |
| "Never ask" posture | Rejected | Local only |
| Judge approvals | Only `read_only` and `workspace_edit` | Fixed in code |
| Models | Static allowlist per provider (mid and fast tiers) | `AGENT_MODELS_ANTHROPIC`, `AGENT_MODELS_OPENAI`, `AGENT_MODELS_GOOGLE` (comma-separated) |

### Spend caps

Hard caps, in tokens (input plus output, including judge and subagent calls), checked in the model activity before each call. When the counter store (Redis) is down, new turns fail closed.

| Variable | Default |
|---|---|
| `AGENT_SPEND_CAP_SESSION_TOKENS` | 2,000,000 |
| `AGENT_SPEND_CAP_USER_DAY_TOKENS` | 5,000,000 |
| `AGENT_SPEND_CAP_GLOBAL_DAY_TOKENS` | 50,000,000 (circuit breaker) |

One demo run uses about 130k tokens (see the README), so a user can do roughly 35 runs a day under the defaults.

### Storage and other settings

| Variable | Container | Notes |
|---|---|---|
| `SIMPLE_CHAT_REDIS_URL` | API, worker | Redis with AOF on |
| `SIMPLE_CHAT_S3_BUCKET`, `SIMPLE_CHAT_ARTIFACTS_TABLE`, `SIMPLE_CHAT_DYNAMODB_TABLE` | API, worker | Shared claim-check payloads, the frame archive, artifacts and OAuth metadata. Uses the workload role; no static AWS keys. |
| `SIMPLE_CHAT_REQUIRE_SHARED_STORAGE=true` | API, worker | Startup fails if the shared stores are missing |
| `AGENT_SESSION_TTL_SECONDS` | API, worker | Default 1,296,000 (15 days) |
| `AGENT_ARCHIVE_PREFIX` | API | Default `agent-frames/<redis prefix>` |
| `WORKSPACE_EXECUTOR_URL` | API, worker | The executor Service |
| `SIMPLE_CHAT_CODEC_AUTH_ENABLED=1`, `SIMPLE_CHAT_CODEC_JWKS_URL`, `SIMPLE_CHAT_CODEC_ALLOWED_ORIGINS` | worker | **Required in the catalog.** The codec server's `/decode` can fetch S3 payloads, so it must check the Temporal Cloud token (JWKS) and only allow the Temporal UI origin (e.g. `https://cloud.temporal.io`). The code defaults to auth off and CORS `*` for local use. |
| `SIMPLE_CHAT_API_HOST`, `SIMPLE_CHAT_CODEC_SERVER_HOST` | API, worker | `0.0.0.0` so Services and probes can reach them |
| `SIMPLE_CHAT_PUBLIC_URL` | API | Public base URL (OAuth callbacks) |

Identity comes from the catalog's `X-Temporal-Auth-Subject` and `X-Temporal-Auth-Email` headers. Ingress strips browser-supplied copies; never expose the API Service directly.

## Executor egress (NetworkPolicy)

The executor runs model-chosen commands, so it holds no credentials and reaches only the public internet on HTTP and HTTPS (package installs, `git clone` over HTTPS). It must not reach cluster services, the node, private ranges or the metadata endpoint. Its own startup check probes these and fails if they're reachable. Only the API and worker may call it.

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: advanced-ai-agent-executor
spec:
  podSelector:
    matchLabels: {app.kubernetes.io/component: executor}
  policyTypes: [Ingress, Egress]
  ingress:
    - from:
        - podSelector:
            matchExpressions:
              - {key: app.kubernetes.io/component, operator: In, values: [api, worker]}
      ports: [{protocol: TCP, port: 8082}]
  egress:
    - to:   # cluster DNS only
        - namespaceSelector: {matchLabels: {kubernetes.io/metadata.name: kube-system}}
          podSelector: {matchLabels: {k8s-app: kube-dns}}
      ports: [{protocol: UDP, port: 53}, {protocol: TCP, port: 53}]
    - to:   # public internet on 80/443 only
        - ipBlock:
            cidr: 0.0.0.0/0
            except: [10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16, 100.64.0.0/10, 169.254.0.0/16, 127.0.0.0/8, 0.0.0.0/8]
      ports: [{protocol: TCP, port: 80}, {protocol: TCP, port: 443}]
```

Match the labels to the catalog's chart. The catalog's `isolatedExecutor` profile also applies: non-root, read-only root filesystem, no service-account token, and writable `/workspaces` and `/tmp` only.

## S3 lifecycle

Add one lifecycle rule to the app bucket for the frame archive:

- **Prefix:** the archive prefix (`agent-frames/` by default).
- **Expire after:** at least 15 days, the session TTL (`AGENT_SESSION_TTL_SECONDS`). The archive holds the only full transcript once Temporal has truncated the stream at continue-as-new and Redis has expired, so a shorter rule loses transcripts of live sessions.

Don't expire claim-check payloads sooner than the namespace's retention plus one day. A closed run's retained history may still point at them, and an open session continues-as-new at least daily (`MAX_RUN_AGE` in `agent/workflow.py`), so no open run points at payloads older than that.

## Validation

- Backend: `.venv/bin/python -m unittest discover -s tests`.
- Web: `npm run typecheck`, `npm test` and `npm run build` in `simple_chat_agent/web`.
- Image: `make image` (linux/amd64) and `make executor-image`.
- After deploying: log in, run the demo script from the README once (one session, crash the worker during the test run, approve the commit), and check cross-user isolation with two browsers. `/healthz` only proves the process started.
