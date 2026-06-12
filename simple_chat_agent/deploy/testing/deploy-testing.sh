#!/usr/bin/env bash
#
# Build the current branch and deploy the isolated TEST stack (frontend + API +
# worker) into the testing namespace, at
# https://agent-harness-demo.testing.tmprl-demo.cloud
#
# Prereqs: AWS CLI authenticated (ECR + EKS), docker buildx, kubectl, and the
# namespace/runtime IAM stack already applied.
# Usage (from anywhere): ./simple_chat_agent/deploy/testing/deploy-testing.sh

set -euo pipefail

REGION="${AWS_REGION:-us-west-1}"
ACCOUNT="${AWS_ACCOUNT_ID:-429214323166}"
REPO="${ECR_REPOSITORY:-temporal-michaelj-agent-harness-demo}"
NAMESPACE="${K8S_NAMESPACE:-temporal-agent-harness}"
REGISTRY="${ACCOUNT}.dkr.ecr.${REGION}.amazonaws.com"
IMAGE="${REGISTRY}/${REPO}"
TAG="${IMAGE_TAG:-testing-$(date +%Y%m%d-%H%M%S)}"
BUILD_IMAGE="${BUILD_IMAGE:-1}"
CREATE_NAMESPACE="${CREATE_NAMESPACE:-0}"
REFRESH_S3_LIFECYCLE="${REFRESH_S3_LIFECYCLE:-0}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "${ROOT}"

if [[ "${CREATE_NAMESPACE}" == "1" ]]; then
  echo ">> Ensuring namespace ${NAMESPACE}"
  kubectl create namespace "${NAMESPACE}" \
    --dry-run=client \
    --output yaml \
    | kubectl apply -f -
fi

if [[ "${BUILD_IMAGE}" == "1" ]]; then
  echo ">> ECR login (${REGISTRY})"
  aws ecr get-login-password --region "${REGION}" \
    | docker login --username AWS --password-stdin "${REGISTRY}"

  echo ">> Building/pushing ${IMAGE}:${TAG} (linux/amd64)"
  docker buildx build --platform linux/amd64 \
    -f simple_chat_agent/Dockerfile \
    -t "${IMAGE}:${TAG}" \
    --push .
fi

echo ">> Applying testing secret in ${NAMESPACE}"
"${ROOT}/simple_chat_agent/deploy/testing/create-secret.sh"

apply_rendered_manifest() {
  K8S_NAMESPACE="${NAMESPACE}" "${ROOT}/simple_chat_agent/deploy/render-manifest.sh" "$@" \
    | kubectl apply --namespace "${NAMESPACE}" -f -
}

echo ">> Applying testing manifests in ${NAMESPACE}"
apply_rendered_manifest simple_chat_agent/deploy/searxng.yaml
apply_rendered_manifest simple_chat_agent/deploy/testing/serviceaccount.yaml
apply_rendered_manifest simple_chat_agent/deploy/testing/service.yaml
apply_rendered_manifest simple_chat_agent/deploy/testing/certificate.yaml
apply_rendered_manifest simple_chat_agent/deploy/testing/deployment.yaml
apply_rendered_manifest simple_chat_agent/deploy/testing/ingressroute.yaml

echo ">> Setting images + rolling out the test stack"
kubectl set image deployment/agent-harness-web-testing web="${IMAGE}:${TAG}" -n "${NAMESPACE}"
kubectl set image deployment/agent-harness-api-testing api="${IMAGE}:${TAG}" -n "${NAMESPACE}"
kubectl set image deployment/agent-harness-worker-testing worker="${IMAGE}:${TAG}" -n "${NAMESPACE}"
kubectl rollout status deployment/agent-harness-web-testing -n "${NAMESPACE}" --timeout=300s
kubectl rollout status deployment/agent-harness-api-testing -n "${NAMESPACE}" --timeout=300s
kubectl rollout status deployment/agent-harness-worker-testing -n "${NAMESPACE}" --timeout=300s

if [[ "${REFRESH_S3_LIFECYCLE}" == "1" ]]; then
  "${ROOT}/simple_chat_agent/deploy/configure-s3-lifecycle.sh"
fi

echo ">> Done. Test stack on ${IMAGE}:${TAG}"
echo ">> https://agent-harness-demo.testing.tmprl-demo.cloud"
