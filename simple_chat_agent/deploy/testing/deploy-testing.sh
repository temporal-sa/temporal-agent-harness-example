#!/usr/bin/env bash
#
# Build the current branch and deploy the isolated TEST stack (frontend + API +
# worker) at https://agent-harness-demo.testing.tmprl-demo.cloud.
#
# By default this updates the existing shared-namespace test stack. Set
# K8S_NAMESPACE to deploy through the isolated-namespace infrastructure path.
#
# Prereqs: AWS CLI authenticated (ECR + EKS), docker buildx, kubectl, and the
# namespace/runtime IAM stack already applied.
# Usage (from anywhere): ./simple_chat_agent/deploy/testing/deploy-testing.sh

set -euo pipefail

SOURCE_NAMESPACE="${SOURCE_K8S_NAMESPACE:-temporal-michaelj-agent-harness-demo}"
REGION="${AWS_REGION:-us-west-1}"
ACCOUNT="${AWS_ACCOUNT_ID:-429214323166}"
REPO="${ECR_REPOSITORY:-temporal-michaelj-agent-harness-demo}"
NAMESPACE="${K8S_NAMESPACE:-${SOURCE_NAMESPACE}}"
KUBE_CONTEXT="${KUBE_CONTEXT-sa-demo}"
REGISTRY="${ACCOUNT}.dkr.ecr.${REGION}.amazonaws.com"
IMAGE="${REGISTRY}/${REPO}"
TAG="${IMAGE_TAG:-testing-$(date +%Y%m%d-%H%M%S)}"
BUILD_IMAGE="${BUILD_IMAGE:-1}"
CREATE_NAMESPACE="${CREATE_NAMESPACE:-0}"

if [[ "${NAMESPACE}" == "${SOURCE_NAMESPACE}" ]]; then
  SERVICE_ACCOUNT_MANIFEST="simple_chat_agent/deploy/testing/serviceaccount.yaml"
  DEFAULT_CREATE_APP_SECRET="0"
  DEFAULT_DEMO_WORKSPACES_ENABLED="1"
  DEFAULT_DEMO_WORKSPACE_ROLE_ARN="arn:aws:iam::429214323166:role/temporal-michaelj-agent-harness-demo-worker-testing"
  DEFAULT_REFRESH_S3_LIFECYCLE="1"
else
  SERVICE_ACCOUNT_MANIFEST="simple_chat_agent/deploy/testing/serviceaccount-isolated.yaml"
  DEFAULT_CREATE_APP_SECRET="1"
  DEFAULT_DEMO_WORKSPACES_ENABLED="0"
  DEFAULT_DEMO_WORKSPACE_ROLE_ARN="arn:aws:iam::429214323166:role/temporal-agent-harness-testing-worker"
  DEFAULT_REFRESH_S3_LIFECYCLE="0"
fi

CREATE_APP_SECRET="${CREATE_APP_SECRET:-${DEFAULT_CREATE_APP_SECRET}}"
TESTING_DEMO_WORKSPACES_ENABLED="${TESTING_DEMO_WORKSPACES_ENABLED:-${DEFAULT_DEMO_WORKSPACES_ENABLED}}"
TESTING_DEMO_WORKSPACE_ROLE_ARN="${TESTING_DEMO_WORKSPACE_ROLE_ARN:-${DEFAULT_DEMO_WORKSPACE_ROLE_ARN}}"
REFRESH_S3_LIFECYCLE="${REFRESH_S3_LIFECYCLE:-${DEFAULT_REFRESH_S3_LIFECYCLE}}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "${ROOT}"

KUBECTL=(kubectl)
if [[ -n "${KUBE_CONTEXT}" ]]; then
  KUBECTL+=(--context "${KUBE_CONTEXT}")
fi

if [[ "${CREATE_NAMESPACE}" == "1" ]]; then
  echo ">> Ensuring namespace ${NAMESPACE}"
  "${KUBECTL[@]}" create namespace "${NAMESPACE}" \
    --dry-run=client \
    --output yaml \
    | "${KUBECTL[@]}" apply -f -
fi

if [[ "${BUILD_IMAGE}" == "1" ]]; then
  echo ">> ECR login (${REGISTRY})"
  aws ecr get-login-password --region "${REGION}" \
    | docker login --username AWS --password-stdin "${REGISTRY}"

  echo ">> Building/pushing ${IMAGE}:${TAG} (linux/amd64)"
  docker buildx build --platform linux/amd64 \
    -f simple_chat_agent/Dockerfile \
    -t "${IMAGE}:${TAG}" \
    -t "${IMAGE}:testing" \
    --push .
fi

if [[ "${CREATE_APP_SECRET}" == "1" ]]; then
  echo ">> Applying testing secret in ${NAMESPACE}"
  K8S_NAMESPACE="${NAMESPACE}" KUBE_CONTEXT="${KUBE_CONTEXT}" \
    "${ROOT}/simple_chat_agent/deploy/testing/create-secret.sh"
fi

if ! "${KUBECTL[@]}" get secret agent-harness-workspace-executor-auth-testing \
  --namespace "${NAMESPACE}" >/dev/null 2>&1; then
  echo ">> Creating testing workspace executor authentication secret"
  WORKSPACE_EXECUTOR_SECRET="$(openssl rand -hex 32)"
  "${KUBECTL[@]}" create secret generic agent-harness-workspace-executor-auth-testing \
    --namespace "${NAMESPACE}" \
    --from-literal="token=${WORKSPACE_EXECUTOR_SECRET}"
  unset WORKSPACE_EXECUTOR_SECRET
fi

apply_rendered_manifest() {
  K8S_NAMESPACE="${NAMESPACE}" \
    SOURCE_K8S_NAMESPACE="${SOURCE_NAMESPACE}" \
    TESTING_DEMO_WORKSPACES_ENABLED="${TESTING_DEMO_WORKSPACES_ENABLED}" \
    TESTING_DEMO_WORKSPACE_ROLE_ARN="${TESTING_DEMO_WORKSPACE_ROLE_ARN}" \
    "${ROOT}/simple_chat_agent/deploy/render-manifest.sh" "$@" \
    | "${KUBECTL[@]}" apply --namespace "${NAMESPACE}" -f -
}

echo ">> Applying testing manifests in ${NAMESPACE}"
apply_rendered_manifest simple_chat_agent/deploy/searxng.yaml
apply_rendered_manifest "${SERVICE_ACCOUNT_MANIFEST}"
apply_rendered_manifest simple_chat_agent/deploy/testing/service.yaml
apply_rendered_manifest simple_chat_agent/deploy/testing/certificate.yaml
apply_rendered_manifest simple_chat_agent/deploy/testing/redis.yaml
apply_rendered_manifest simple_chat_agent/deploy/testing/workspace-executor.yaml

if [[ "${TESTING_DEMO_WORKSPACES_ENABLED}" == "1" ]]; then
  apply_rendered_manifest simple_chat_agent/deploy/testing/demo-workspace-rbac.yaml
fi

echo ">> Waiting for Redis before changing the application stack"
"${KUBECTL[@]}" rollout status deployment/agent-harness-redis-testing \
  --namespace "${NAMESPACE}" --timeout=300s

apply_rendered_manifest simple_chat_agent/deploy/testing/deployment.yaml
apply_rendered_manifest simple_chat_agent/deploy/testing/ingressroute.yaml

echo ">> Setting images + rolling out the test stack"
"${KUBECTL[@]}" set image deployment/agent-harness-web-testing \
  web="${IMAGE}:${TAG}" --namespace "${NAMESPACE}"
"${KUBECTL[@]}" set image deployment/agent-harness-api-testing \
  api="${IMAGE}:${TAG}" --namespace "${NAMESPACE}"
"${KUBECTL[@]}" set image deployment/agent-harness-worker-testing \
  worker="${IMAGE}:${TAG}" --namespace "${NAMESPACE}"
"${KUBECTL[@]}" set image deployment/agent-harness-workspace-executor-testing \
  workspace-executor="${IMAGE}:${TAG}" workspace-firewall="${IMAGE}:${TAG}" \
  --namespace "${NAMESPACE}"

"${KUBECTL[@]}" rollout status deployment/agent-harness-web-testing \
  --namespace "${NAMESPACE}" --timeout=300s
"${KUBECTL[@]}" rollout status deployment/agent-harness-api-testing \
  --namespace "${NAMESPACE}" --timeout=300s
"${KUBECTL[@]}" rollout status deployment/agent-harness-workspace-executor-testing \
  --namespace "${NAMESPACE}" --timeout=300s
"${KUBECTL[@]}" rollout status deployment/agent-harness-worker-testing \
  --namespace "${NAMESPACE}" --timeout=300s

if [[ "${REFRESH_S3_LIFECYCLE}" == "1" ]]; then
  "${ROOT}/simple_chat_agent/deploy/configure-s3-lifecycle.sh"
fi

echo ">> Done. Test stack on ${IMAGE}:${TAG}"
echo ">> https://agent-harness-demo.testing.tmprl-demo.cloud"
