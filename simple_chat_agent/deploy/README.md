# Deploying simple_chat_agent

Deploys the frontend, API, and Temporal worker (with its in-process codec
server) as **three independent Deployments** into the
`temporal-michaelj-agent-harness-demo` namespace on the `sa-demo` EKS cluster
(`us-west-1`, account `429214323166`), fronted by Traefik on
`*.tmprl-demo.cloud`.

| Component | URL |
|-----------|-----|
| Frontend  | https://agent-harness-demo.tmprl-demo.cloud |
| API       | https://agent-harness-demo.tmprl-demo.cloud/api |
| Codec     | https://codec.agent-harness-demo.tmprl-demo.cloud |

All shared state is external (S3 claim-checks + artifacts, DynamoDB OAuth and
artifact metadata, and an API-owned HTTP streaming endpoint), so the runtimes no
longer share a pod or any local volume:

- `agent-harness-web` — static frontend only. It receives no app secrets and no
  service-account token.
- `agent-harness-api` — FastAPI, OAuth, SSE, artifacts, and internal stream API.
  Single replica while it owns the in-memory stream buffer; scaling needs a
  shared backplane such as Redis.
- `agent-harness-worker` — Temporal worker + codec. Horizontally scalable (bump
  `replicas`); the codec reads claim-checks from S3, so any worker pod decodes.

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
URL, proxy trust, a real session secret, and codec settings):

```bash
NS=temporal-michaelj-agent-harness-demo
ENVFILE=$(mktemp)
grep -vE '^(SIMPLE_CHAT_PUBLIC_URL|GITHUB_OAUTH_REDIRECT_URI|FORWARDED_ALLOW_IPS|SIMPLE_CHAT_JWT_SECRET|SIMPLE_CHAT_CODEC_SERVER_HOST|SIMPLE_CHAT_CODEC_AUTH_ENABLED|SIMPLE_CHAT_STREAM_TOKEN)=' .env > "$ENVFILE"
cat >> "$ENVFILE" <<EOF
SIMPLE_CHAT_PUBLIC_URL=https://agent-harness-demo.tmprl-demo.cloud
GITHUB_OAUTH_REDIRECT_URI=https://agent-harness-demo.tmprl-demo.cloud/oauth/github/callback
FORWARDED_ALLOW_IPS=*
SIMPLE_CHAT_JWT_SECRET=$(openssl rand -hex 32)
SIMPLE_CHAT_CODEC_SERVER_HOST=0.0.0.0
SIMPLE_CHAT_CODEC_AUTH_ENABLED=1
SIMPLE_CHAT_STREAM_TOKEN=$(openssl rand -hex 32)
EOF
kubectl create secret generic agent-harness-secrets -n $NS \
  --from-env-file="$ENVFILE" --dry-run=client -o yaml | kubectl apply -f -
rm -f "$ENVFILE"
```

## Apply

```bash
kubectl apply -f simple_chat_agent/deploy/
```

Certificates take ~1 minute to be issued by Let's Encrypt; watch with
`kubectl get certificate -n $NS`.

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
- **Purge on close**: deleting a chat purges its payloads via a prefix delete
  on `v0/ns/<namespace>/wt/SimpleChatWorkflow/wi/<workflow-id>/`
  (`web.py` `_forget_conversation` → `purge_workflow_payloads`).

## Durable state

| State | Backend | Notes |
|-------|---------|-------|
| Claim-check payloads | S3 (`SIMPLE_CHAT_S3_BUCKET`) | survives redeploys; purged on chat delete |
| GitHub/MCP OAuth tokens | DynamoDB (`SIMPLE_CHAT_DYNAMODB_TABLE`, table `…-oauth`) | survives redeploys; SSE-encrypted; accessed via IRSA |
| Transient OAuth handshake state and local-dev stream/artifact files | local `emptyDir` | ephemeral; lost on restart |

When the S3 / DynamoDB env vars are unset (local dev), the app falls back to
on-disk file + SQLite storage with no AWS dependency.

The deployed API receives sideband stream events over `/internal/stream` and
serves browser SSE from its in-memory stream buffer. That keeps the frontend pod
static, but the API should stay at one replica until the stream buffer moves to a
shared backplane.

## Manual steps (cannot be done with kubectl)

1. **Temporal Cloud** → namespace `michaelj-agent-harness-demo.a2dd6` → set the
   Codec Server endpoint to `https://codec.agent-harness-demo.tmprl-demo.cloud`
   and enable **Pass access token**. (Requires Namespace Admin.) The codec
   verifies the forwarded JWT against Temporal Cloud's JWKS endpoint.
2. **GitHub OAuth app** → add callback URL
   `https://agent-harness-demo.tmprl-demo.cloud/oauth/github/callback`.
3. **Google OAuth client** → add authorized redirect URI
   `https://agent-harness-demo.tmprl-demo.cloud/oauth/google/callback`.
