#!/usr/bin/env bash
set -euo pipefail

TARGET_NAMESPACE="${K8S_NAMESPACE:?K8S_NAMESPACE is required}"
SOURCE_NAMESPACE="${SOURCE_K8S_NAMESPACE:-temporal-michaelj-agent-harness-demo}"

if [[ "$#" -eq 0 ]]; then
  echo "usage: K8S_NAMESPACE=<namespace> $0 <manifest.yaml>..." >&2
  exit 2
fi

sed -E \
  -e "s/^([[:space:]]*namespace:) ${SOURCE_NAMESPACE}$/\\1 ${TARGET_NAMESPACE}/" \
  -e "s/^([[:space:]]*value:) ${SOURCE_NAMESPACE}$/\\1 ${TARGET_NAMESPACE}/" \
  -e "s|agent-harness-searxng\\.temporal-michaelj-agent-harness-demo\\.svc\\.cluster\\.local|agent-harness-searxng.${TARGET_NAMESPACE}.svc.cluster.local|g" \
  "$@"
