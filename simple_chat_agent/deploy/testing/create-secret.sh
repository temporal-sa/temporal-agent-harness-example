#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${K8S_NAMESPACE:-temporal-agent-harness}"
SECRET_NAME="${K8S_SECRET_NAME:-agent-harness-secrets}"

required_keys=(
  ANTHROPIC_API_KEY
  TEMPORAL_API_KEY
  TEMPORAL_ENDPOINT
  TEMPORAL_NAMESPACE
  SIMPLE_CHAT_JWT_SECRET
  SIMPLE_CHAT_STREAM_TOKEN
)

optional_keys=(
  ANTHROPIC_BASE_URL
  ANTHROPIC_MODEL
  ANTHROPIC_MODEL_OPTIONS
  ANTHROPIC_MODEL_CACHE_SECONDS
  GOOGLE_API_KEY
  GOOGLE_OAUTH_ALLOWED_DOMAIN
  GOOGLE_OAUTH_CLIENT_ID
  GOOGLE_OAUTH_CLIENT_SECRET
  GOOGLE_OAUTH_REDIRECT_URI
  GITHUB_OAUTH_CLIENT_ID
  GITHUB_OAUTH_CLIENT_SECRET
  GITHUB_OAUTH_REDIRECT_URI
  GITHUB_OAUTH_SCOPES
  SIMPLE_CHAT_CODEC_AUTH_ENABLED
  SIMPLE_CHAT_CODEC_ALLOWED_ORIGINS
  SIMPLE_CHAT_GOOD_PLACE
  SIMPLE_CHAT_LOCAL_AUTH_ENABLED
  SIMPLE_CHAT_LOCAL_AUTH_USERNAME
  SIMPLE_CHAT_LOCAL_AUTH_PASSWORD
  TEMPORAL_TLS
  TEMPORAL_UI_URL
)

args=()

for key in "${required_keys[@]}"; do
  if [[ -z "${!key:-}" ]]; then
    echo "required environment variable ${key} is not set" >&2
    exit 1
  fi
  args+=(--from-literal="${key}=${!key}")
done

for key in "${optional_keys[@]}"; do
  if [[ -n "${!key:-}" ]]; then
    args+=(--from-literal="${key}=${!key}")
  fi
done

kubectl create secret generic "${SECRET_NAME}" \
  --namespace "${NAMESPACE}" \
  "${args[@]}" \
  --dry-run=client \
  --output yaml \
  | kubectl apply --namespace "${NAMESPACE}" -f -
