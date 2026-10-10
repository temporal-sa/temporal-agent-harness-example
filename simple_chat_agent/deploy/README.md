# Deploying simple_chat_agent

Deploys the frontend, API, Redis stream backplane, Temporal worker (with its
in-process codec server), and credential-free workspace executor as independent workloads into the
`temporal-michaelj-agent-harness-demo` namespace on the `sa-demo` EKS cluster
(`us-west-1`, account `429214323166`), fronted by Traefik on
`*.tmprl-demo.cloud`.

| Component | URL |
|-----------|-----|
| Frontend  | https://agent-harness-demo.tmprl-demo.cloud |
| API       | https://agent-harness-demo.tmprl-demo.cloud/api |
| Codec     | https://codec.agent-harness-demo.tmprl-demo.cloud |

All shared state is external (S3 claim-checks + artifacts, DynamoDB OAuth and
artifact metadata, and Redis Streams), so the runtimes no longer share a pod or
any local volume:

- `agent-harness-web` — static frontend only. It receives no app secrets and no
  service-account token.
- `agent-harness-api` — FastAPI, OAuth, SSE, artifacts, and the authenticated
  browser gateway over Redis Streams.
- `agent-harness-redis` — ordered/replayable stream events, protected with the
  existing stream token and using AOF on a pod-local `emptyDir`.
- `agent-harness-worker` — Temporal worker + codec. Horizontally scalable (bump
  `replicas`); the codec reads claim-checks from S3, so any worker pod decodes.
- `agent-harness-workspace-executor` — authenticated Bubblewrap command runner
  with public-web-only egress. Production and testing use a 5 Gi `emptyDir`
  because the `sa-demo` cluster has no dynamic block-storage CSI provisioner.
  Files persist between tool calls for the executor pod's lifetime, but an
  executor rollout starts with a fresh workspace. A `NET_ADMIN` init container
  installs the Pod's egress firewall and exits; the long-running executor is
  non-root and capless. It receives no app secret, ServiceAccount token, or IAM
  role, and refuses readiness if an ambient AWS or Kubernetes credential path
  is detected. Tini runs as PID 1 to reap orphaned Bubblewrap children, and the
  health probe periodically launches a real sandbox command and rejects an
  executor that accumulates orphaned zombie processes.

## Deployment Philosophy

The deployment keeps runtime responsibilities separated even though all three
processes are built from one Docker image.

- The web pod is static and should not receive application secrets.
- The API pod owns login, OAuth, browser routes, artifact metadata, and the SSE
  projection of Redis Streams.
- The worker pod owns Temporal workflow/activity execution and the codec server.
- The workspace executor owns untrusted shell processes and persistent agent files.
- Redis owns short-lived, ordered stream events and resumable cursors.
- S3 and DynamoDB are the durable cross-pod stores.
- Temporal Cloud is the durable workflow store.

This keeps pod restarts and worker crashes demo-friendly. Crashing a worker
should interrupt execution, not erase state. Crashing the web pod should only
affect static asset serving. API restarts no longer erase sideband stream events;
Redis AOF preserves them across Redis container restarts in the same pod. A pod
replacement loses the short-lived stream log and the UI reconciles from durable
Temporal workflow state.

## Manifest And Script Map

| Path | Purpose |
| --- | --- |
| `deployment.yaml` | Production web/API/worker Deployments and environment wiring. |
| `redis.yaml` | Password-protected Redis Deployment and Service. |
| `service.yaml` | ClusterIP Services for web, API, and codec. |
| `ingressroute.yaml` | Traefik routes, TLS, HTTPS redirect middleware, and codec host. |
| `certificate.yaml` | Production wildcard and app certificates. |
| `serviceaccount.yaml` | Shared ServiceAccount and IRSA role annotation. |
| `demo-workspace-rbac.yaml` | RBAC that lets the controller create/delete temp namespaces and workloads. |
| `searxng.yaml` | Internal SearXNG deployment/service used by research tools. |
| `workspace-executor.yaml` | Bubblewrap executor, one-shot egress-firewall init container, PVC, internal Service, and defense-in-depth NetworkPolicy. |
| `configure-s3-lifecycle.sh` | Applies the broad S3 expiration policy used by claim-checks, artifacts, and attachments. |
| `deploy.sh` | Build, push, apply manifests, set images, wait for rollout, refresh S3 lifecycle. |
| `testing/` | Parallel testing stack manifests and testing deploy scripts. |

Manifests are intentionally plain Kubernetes YAML plus Traefik CRDs. The demo
does not use Helm or Kustomize so the rendered runtime shape is easy to inspect
while presenting.

## Build & push the image

Built for `linux/amd64` (EKS node arch). Build from the **repo root**:

```bash
export AWS_PROFILE="SolutionsArchitecture/AWSAdministratorAccess"
REGION=us-west-1; ACCT=429214323166
IMG=$ACCT.dkr.ecr.$REGION.amazonaws.com/temporal-michaelj-agent-harness-demo

aws ecr get-login-password --region $REGION \
  | docker login --username AWS --password-stdin $ACCT.dkr.ecr.$REGION.amazonaws.com

docker buildx build --platform linux/amd64 \
  -f simple_chat_agent/Dockerfile \
  -t "$IMG:1.0" --push .
```

## Create the secret from `.env`

The entire root `.env` is loaded as a Secret consumed by the API and worker via
`envFrom`. The static frontend Deployment does not consume this Secret. A few
values are overridden for the deployed environment (the public GitHub callback
URL, proxy trust, a real session secret, local auth disabled, and codec
settings):

```bash
NS=temporal-michaelj-agent-harness-demo
ENVFILE=$(mktemp)
grep -vE '^(SIMPLE_CHAT_PUBLIC_URL|GITHUB_OAUTH_REDIRECT_URI|FORWARDED_ALLOW_IPS|SIMPLE_CHAT_JWT_SECRET|SIMPLE_CHAT_LOCAL_AUTH_ENABLED|SIMPLE_CHAT_LOCAL_AUTH_USERNAME|SIMPLE_CHAT_LOCAL_AUTH_PASSWORD|SIMPLE_CHAT_CODEC_SERVER_HOST|SIMPLE_CHAT_CODEC_AUTH_ENABLED|SIMPLE_CHAT_STREAM_TOKEN)=' .env > "$ENVFILE"
cat >> "$ENVFILE" <<EOF
SIMPLE_CHAT_PUBLIC_URL=https://agent-harness-demo.tmprl-demo.cloud
GITHUB_OAUTH_REDIRECT_URI=https://agent-harness-demo.tmprl-demo.cloud/oauth/github/callback
FORWARDED_ALLOW_IPS=*
SIMPLE_CHAT_JWT_SECRET=$(openssl rand -hex 32)
SIMPLE_CHAT_LOCAL_AUTH_ENABLED=0
SIMPLE_CHAT_CODEC_SERVER_HOST=0.0.0.0
SIMPLE_CHAT_CODEC_AUTH_ENABLED=1
SIMPLE_CHAT_STREAM_TOKEN=$(openssl rand -hex 32)
EOF
kubectl create secret generic agent-harness-secrets -n $NS \
  --from-env-file="$ENVFILE" --dry-run=client -o yaml | kubectl apply -f -
rm -f "$ENVFILE"
```

## Apply

`deploy.sh` creates the private `agent-harness-workspace-executor-auth` Secret on
first use. When applying manifests manually, create it first:

```bash
kubectl -n temporal-michaelj-agent-harness-demo create secret generic \
  agent-harness-workspace-executor-auth \
  --from-literal="token=$(openssl rand -hex 32)"
```

```bash
kubectl apply -f simple_chat_agent/deploy/
```

Certificates take ~1 minute to be issued by Let's Encrypt; watch with
`kubectl get certificate -n $NS`.

For normal production rollouts, prefer `deploy.sh`; it builds an immutable
timestamp tag, applies manifests, updates the three Deployments, waits for
rollout, and refreshes the S3 lifecycle policy.

## Testing Stack

The testing deploy script supports two targets. With no overrides, the stack
lives alongside production in the existing namespace but uses separate
Deployment names, hostnames, workflow prefixes, task queues, and environment
overrides. Setting `K8S_NAMESPACE` selects the isolated-namespace path used by
the manual `Deploy Testing` GitHub Actions workflow and the Terraform resources
under `infra/stacks/testing-runtime`.

Use:

```bash
./simple_chat_agent/deploy/testing/deploy-testing.sh
```

The testing script:

1. builds the current branch into a `testing-<timestamp>` image tag;
2. creates the app secret for an isolated namespace and the workspace-executor
   authentication secret when absent;
3. renders namespace-specific manifests and selects service accounts that match
   the target namespace's IRSA roles;
4. applies SearXNG, Redis, and testing-specific web/API/worker/executor
   manifests;
5. rolls out the testing Deployments;
6. refreshes the S3 lifecycle policy for the default shared-namespace path.

Local rollout scripts default to the explicit `sa-demo` kubectl context; they
do not depend on whichever context happens to be current. The GitHub Actions
workflow clears `KUBE_CONTEXT` after configuring its own kubeconfig. Each
rollout also sets a unique timestamp-based Temporal Worker Deployment build ID.

The testing URL is:

```text
https://agent-harness-demo.testing.tmprl-demo.cloud
```

Use testing before merging risky workflow, stream, provider, or deployment
changes into production.

## Claim-check storage (S3)

Offloaded ("claim-check") payloads are stored in S3 so they survive pod
restarts and are shared across the API/worker/codec. This is driven by
`SIMPLE_CHAT_S3_BUCKET` (set on the Deployment); when unset, the app falls back
to a local on-disk store (used for local dev).

- **Bucket**: `michaelj-agent-harness-claimcheck-429214323166` (`us-west-1`,
  public access blocked).
- **Access**: IRSA — the `agent-harness` ServiceAccount
  (`serviceaccount.yaml`) is annotated with IAM role
  `temporal-michaelj-agent-harness-demo-s3`, whose trust policy is scoped to
  this namespace + ServiceAccount and whose inline policy grants
  Get/Put/Delete/List on the bucket only. No static AWS keys in the cluster.
- **Key layout** (official `temporalio.contrib.aws.s3driver`):
  `v0/ns/<namespace>/wt/<workflow-type>/wi/<workflow-id>/ri/<run-id>/d/sha256/<hash>`.
- **Lifecycle**: deploy scripts configure a broad 30-day S3 expiration rule for
  claim-check payloads and artifact bytes. User and chat workflows checkpoint
  or close on a 15-day run TTL, so no open run should depend on an object older
  than the lifecycle horizon. Artifacts and attachments expose `expires_at` and
  return clear expired/unavailable errors when read after retention.
- **Purge on close**: deleting a chat purges its payloads via a prefix delete
  on `v0/ns/<namespace>/wt/SimpleChatWorkflow/wi/<workflow-id>/`
  (`web.py` `_forget_conversation` → `purge_workflow_payloads`).

## Durable state

| State | Backend | Notes |
|-------|---------|-------|
| Claim-check payloads and artifact bytes | S3 (`SIMPLE_CHAT_S3_BUCKET`) | survives redeploys; chat delete purges known prefixes |
| GitHub/MCP OAuth tokens | DynamoDB (`SIMPLE_CHAT_DYNAMODB_TABLE`, table `…-oauth`) | survives redeploys; SSE-encrypted; accessed via IRSA |
| Ordered deployed stream events and cursors | Redis Streams + AOF on `emptyDir` | 30-minute idle TTL; survives API and Redis-container restarts, not Redis-pod replacement |
| Production and testing agent command workspaces | 5 Gi `emptyDir` | stable between tool calls; lost when the executor Pod is replaced |
| Transient OAuth handshake state and local-dev stream/artifact files | local `emptyDir` | ephemeral; lost on restart |

When the S3 / DynamoDB env vars are unset (local dev), the app falls back to
on-disk file + SQLite storage with no AWS dependency.

Local testing auth is for private local runs only. Deployed environments should
leave `SIMPLE_CHAT_LOCAL_AUTH_ENABLED=0` and use Google OAuth. Even if local
auth is accidentally enabled, the local login route is not registered while
Google OAuth credentials are configured.

Deployed workers append sideband events directly to Redis Streams. The API reads
the same ordered log and serves authenticated browser SSE without changing the
frontend contract. `/internal/stream` remains for rolling compatibility with
older workers and the retired sandbox Lambda; the API forwards those events to
Redis. Stream keys expire 30 minutes after their last event by default.

## Temporary Demo Workspaces

The main environment can create temporary isolated workspaces for crash demos.
The controller workflow creates a namespace, copies relevant configuration,
deploys web/API/worker workloads with a unique task queue and workflow prefix,
and routes a unique host through the wildcard certificate.

Temp workspaces intentionally reuse shared infrastructure where it is safe:

- same image repository;
- same ServiceAccount/IAM role shape;
- same S3 bucket with per-workflow key prefixes;
- same internal SearXNG service URL;
- same Temporal Cloud namespace, but with unique task queues and workflow id
  prefixes.

The temp namespace can be crashed with normal Kubernetes commands without
affecting the main pods. Workspace cleanup deletes the namespace and purges
known workspace payload prefixes.

Worker versioning is enabled by default for the main worker. Temp workers may
disable versioning because each workspace has a short-lived unique task queue.

## Manual steps (cannot be done with kubectl)

1. **Temporal Cloud** → namespace `michaelj-agent-harness-demo.a2dd6` → set the
   Codec Server endpoint to `https://codec.agent-harness-demo.tmprl-demo.cloud`
   and enable **Pass access token**. (Requires Namespace Admin.) The codec
   verifies the forwarded JWT against Temporal Cloud's JWKS endpoint.
2. **GitHub OAuth app** → add callback URL
   `https://agent-harness-demo.tmprl-demo.cloud/oauth/github/callback`.
3. **Google OAuth client** → add authorized redirect URI
   `https://agent-harness-demo.tmprl-demo.cloud/oauth/google/callback`.

## Rollout Verification

After any rollout, check:

```bash
kubectl get deploy agent-harness-web agent-harness-api agent-harness-worker agent-harness-redis \
  -n temporal-michaelj-agent-harness-demo -o wide

kubectl get pods -n temporal-michaelj-agent-harness-demo -o wide

curl -sS -L --max-time 20 -o /dev/null -w '%{http_code}\n' \
  https://agent-harness-demo.tmprl-demo.cloud
```

For testing, use the `*-testing` deployment names and the testing hostname.
