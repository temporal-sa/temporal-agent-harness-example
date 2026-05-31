#!/usr/bin/env bash
#
# Build, push, and roll out a new simple_chat_agent image.
#
# Use this to ship code/dependency updates. (For infra changes — manifests,
# secret — use `kubectl apply -f simple_chat_agent/deploy/` instead.)
#
# Prerequisites:
#   - Your AWS CLI is authenticated (e.g. `aws sso login --profile <profile>`),
#     with access to ECR + the EKS cluster in account 429214323166 / us-west-1.
#   - docker (with buildx) and kubectl are installed.
#
# Usage (from anywhere):
#   ./simple_chat_agent/deploy/deploy.sh

set -euo pipefail

REGION="us-west-1"
ACCOUNT="429214323166"
REPO="temporal-michaelj-agent-harness-demo"
NAMESPACE="temporal-michaelj-agent-harness-demo"
FRONTEND_DEPLOYMENT="agent-harness-web"
API_DEPLOYMENT="agent-harness-api"
WORKER_DEPLOYMENT="agent-harness-worker"

REGISTRY="${ACCOUNT}.dkr.ecr.${REGION}.amazonaws.com"
IMAGE="${REGISTRY}/${REPO}"
TAG="$(date +%Y%m%d-%H%M%S)"

# Repo root is two levels up from this script; the image must be built with the
# repo root as the build context.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT}"

echo ">> Logging in to ECR (${REGISTRY})"
aws ecr get-login-password --region "${REGION}" \
  | docker login --username AWS --password-stdin "${REGISTRY}"

echo ">> Building and pushing ${IMAGE}:${TAG} (and :latest) for linux/amd64"
docker buildx build --platform linux/amd64 \
  -f simple_chat_agent/Dockerfile \
  -t "${IMAGE}:${TAG}" \
  -t "${IMAGE}:latest" \
  --push .

echo ">> Rolling out frontend + API + worker in ${NAMESPACE} to ${TAG}"
kubectl set image "deployment/${FRONTEND_DEPLOYMENT}" "web=${IMAGE}:${TAG}" -n "${NAMESPACE}"
kubectl set image "deployment/${API_DEPLOYMENT}" "api=${IMAGE}:${TAG}" -n "${NAMESPACE}"
kubectl set image "deployment/${WORKER_DEPLOYMENT}" "worker=${IMAGE}:${TAG}" -n "${NAMESPACE}"
kubectl rollout status "deployment/${FRONTEND_DEPLOYMENT}" -n "${NAMESPACE}" --timeout=300s
kubectl rollout status "deployment/${API_DEPLOYMENT}" -n "${NAMESPACE}" --timeout=300s
kubectl rollout status "deployment/${WORKER_DEPLOYMENT}" -n "${NAMESPACE}" --timeout=300s

echo ">> Done. Deployed ${IMAGE}:${TAG}"
