#!/usr/bin/env bash
set -euo pipefail

TARGET_NAMESPACE="${K8S_NAMESPACE:?K8S_NAMESPACE is required}"
SOURCE_NAMESPACE="${SOURCE_K8S_NAMESPACE:-temporal-michaelj-agent-harness-demo}"
DEMO_WORKSPACES_ENABLED="${TESTING_DEMO_WORKSPACES_ENABLED:-1}"
DEMO_WORKSPACE_ROLE_ARN="${TESTING_DEMO_WORKSPACE_ROLE_ARN:-arn:aws:iam::429214323166:role/temporal-michaelj-agent-harness-demo-worker-testing}"

if [[ "${DEMO_WORKSPACES_ENABLED}" != "0" && "${DEMO_WORKSPACES_ENABLED}" != "1" ]]; then
  echo "TESTING_DEMO_WORKSPACES_ENABLED must be 0 or 1" >&2
  exit 2
fi

if [[ "$#" -eq 0 ]]; then
  echo "usage: K8S_NAMESPACE=<namespace> $0 <manifest.yaml>..." >&2
  exit 2
fi

sed -E \
  -e "s/^([[:space:]]*namespace:) ${SOURCE_NAMESPACE}$/\\1 ${TARGET_NAMESPACE}/" \
  -e "s/^([[:space:]]*value:) ${SOURCE_NAMESPACE}$/\\1 ${TARGET_NAMESPACE}/" \
  -e "s|^([[:space:]]*value:) .* # render:testing-demo-workspaces-enabled$|\\1 \"${DEMO_WORKSPACES_ENABLED}\" # render:testing-demo-workspaces-enabled|" \
  -e "s|^([[:space:]]*value:) .* # render:testing-demo-workspace-role-arn$|\\1 ${DEMO_WORKSPACE_ROLE_ARN} # render:testing-demo-workspace-role-arn|" \
  -e "s|agent-harness-searxng\\.temporal-michaelj-agent-harness-demo\\.svc\\.cluster\\.local|agent-harness-searxng.${TARGET_NAMESPACE}.svc.cluster.local|g" \
  "$@"
