#!/usr/bin/env bash
#
# Deploy the isolated Python sandbox executor Lambda used by the testing worker.
# The Lambda execution role intentionally has no app permissions and no env
# variables; the worker passes the narrow stream URL/token in the invoke payload.

set -euo pipefail

REGION="us-west-1"
ACCOUNT="429214323166"
NAMESPACE="temporal-michaelj-agent-harness-demo"
FUNCTION_NAME="temporal-michaelj-agent-harness-demo-python-sandbox-testing"
LAMBDA_ROLE="temporal-michaelj-agent-harness-demo-python-sandbox-testing"
WORKER_ROLE="temporal-michaelj-agent-harness-demo-worker-testing"
WORKER_SERVICE_ACCOUNT="agent-harness-worker-testing"
BASE_APP_ROLE="temporal-michaelj-agent-harness-demo-s3"
OIDC_PROVIDER_ARN="arn:aws:iam::429214323166:oidc-provider/oidc.eks.us-west-1.amazonaws.com/id/A19A2DDF9700D5C8E559A966B862CDAD"
OIDC_PROVIDER_HOST="oidc.eks.us-west-1.amazonaws.com/id/A19A2DDF9700D5C8E559A966B862CDAD"
FUNCTION_ARN="arn:aws:lambda:${REGION}:${ACCOUNT}:function:${FUNCTION_NAME}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
BUILD_DIR="${ROOT}/.lambda-build/python-sandbox-testing"
ZIP_PATH="${BUILD_DIR}/python-sandbox.zip"

mkdir -p "${BUILD_DIR}"

echo ">> Packaging Python sandbox Lambda (${ZIP_PATH})"
uv run python - "${ROOT}" "${ZIP_PATH}" <<'PY'
from __future__ import annotations

import sys
import zipfile
from pathlib import Path

root = Path(sys.argv[1])
zip_path = Path(sys.argv[2])
files = [
    "claude_harness/__init__.py",
    "claude_harness/streaming.py",
    "simple_chat_agent/__init__.py",
    "simple_chat_agent/worker/__init__.py",
    "simple_chat_agent/worker/sandbox/__init__.py",
    "simple_chat_agent/worker/sandbox/lambda_handler.py",
    "simple_chat_agent/worker/sandbox/runtime.py",
]

with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
    for rel_path in files:
        archive.write(root / rel_path, rel_path)
PY

LAMBDA_TRUST="${BUILD_DIR}/lambda-trust.json"
WORKER_TRUST="${BUILD_DIR}/worker-trust.json"
LAMBDA_INVOKE_POLICY="${BUILD_DIR}/lambda-invoke-policy.json"

uv run python - "${LAMBDA_TRUST}" <<'PY'
from __future__ import annotations

import json
import sys
from pathlib import Path

Path(sys.argv[1]).write_text(
    json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "lambda.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        },
        indent=2,
    ),
    encoding="utf-8",
)
PY

uv run python - "${WORKER_TRUST}" "${OIDC_PROVIDER_ARN}" "${OIDC_PROVIDER_HOST}" "${NAMESPACE}" "${WORKER_SERVICE_ACCOUNT}" <<'PY'
from __future__ import annotations

import json
import sys
from pathlib import Path

path, provider_arn, provider_host, namespace, service_account = sys.argv[1:6]
Path(path).write_text(
    json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Federated": provider_arn},
                    "Action": "sts:AssumeRoleWithWebIdentity",
                    "Condition": {
                        "StringEquals": {
                            f"{provider_host}:aud": "sts.amazonaws.com",
                            f"{provider_host}:sub": (
                                f"system:serviceaccount:{namespace}:{service_account}"
                            ),
                        }
                    },
                }
            ],
        },
        indent=2,
    ),
    encoding="utf-8",
)
PY

uv run python - "${LAMBDA_INVOKE_POLICY}" "${FUNCTION_ARN}" <<'PY'
from __future__ import annotations

import json
import sys
from pathlib import Path

function_arn = sys.argv[2]
Path(sys.argv[1]).write_text(
    json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "lambda:InvokeFunction",
                    "Resource": [function_arn, f"{function_arn}:*"],
                }
            ],
        },
        indent=2,
    ),
    encoding="utf-8",
)
PY

echo ">> Ensuring Lambda execution role (${LAMBDA_ROLE})"
if ! aws iam get-role --role-name "${LAMBDA_ROLE}" >/dev/null 2>&1; then
  aws iam create-role \
    --role-name "${LAMBDA_ROLE}" \
    --description "No-permission execution role for testing Python sandbox Lambda" \
    --assume-role-policy-document "file://${LAMBDA_TRUST}" >/dev/null
else
  aws iam update-assume-role-policy \
    --role-name "${LAMBDA_ROLE}" \
    --policy-document "file://${LAMBDA_TRUST}" >/dev/null
fi

# Enforce the no-app-permissions contract on the Lambda role. CloudWatch Logs
# are intentionally omitted; invocation payloads remain the debugging surface.
for policy_name in $(aws iam list-role-policies --role-name "${LAMBDA_ROLE}" --query 'PolicyNames[]' --output text); do
  aws iam delete-role-policy --role-name "${LAMBDA_ROLE}" --policy-name "${policy_name}"
done
for policy_arn in $(aws iam list-attached-role-policies --role-name "${LAMBDA_ROLE}" --query 'AttachedPolicies[].PolicyArn' --output text); do
  aws iam detach-role-policy --role-name "${LAMBDA_ROLE}" --policy-arn "${policy_arn}"
done

aws iam wait role-exists --role-name "${LAMBDA_ROLE}"
LAMBDA_ROLE_ARN="$(aws iam get-role --role-name "${LAMBDA_ROLE}" --query 'Role.Arn' --output text)"
echo ">> Waiting for Lambda role propagation"
sleep 10

echo ">> Creating/updating Lambda function (${FUNCTION_NAME})"
if aws lambda get-function --function-name "${FUNCTION_NAME}" --region "${REGION}" >/dev/null 2>&1; then
  aws lambda update-function-code \
    --function-name "${FUNCTION_NAME}" \
    --zip-file "fileb://${ZIP_PATH}" \
    --region "${REGION}" >/dev/null
  aws lambda wait function-updated-v2 --function-name "${FUNCTION_NAME}" --region "${REGION}"
  aws lambda update-function-configuration \
    --function-name "${FUNCTION_NAME}" \
    --role "${LAMBDA_ROLE_ARN}" \
    --handler simple_chat_agent.worker.sandbox.lambda_handler.lambda_handler \
    --runtime python3.12 \
    --timeout 900 \
    --memory-size 1024 \
    --ephemeral-storage '{"Size":1024}' \
    --environment '{"Variables":{}}' \
    --region "${REGION}" >/dev/null
  aws lambda wait function-updated-v2 --function-name "${FUNCTION_NAME}" --region "${REGION}"
else
  aws lambda create-function \
    --function-name "${FUNCTION_NAME}" \
    --description "Testing Python sandbox executor for temporal-agent-harness-example" \
    --runtime python3.12 \
    --handler simple_chat_agent.worker.sandbox.lambda_handler.lambda_handler \
    --role "${LAMBDA_ROLE_ARN}" \
    --timeout 900 \
    --memory-size 1024 \
    --ephemeral-storage '{"Size":1024}' \
    --environment '{"Variables":{}}' \
    --architectures x86_64 \
    --zip-file "fileb://${ZIP_PATH}" \
    --region "${REGION}" >/dev/null
  aws lambda wait function-active-v2 --function-name "${FUNCTION_NAME}" --region "${REGION}"
fi

echo ">> Ensuring testing worker IRSA role (${WORKER_ROLE})"
if ! aws iam get-role --role-name "${WORKER_ROLE}" >/dev/null 2>&1; then
  aws iam create-role \
    --role-name "${WORKER_ROLE}" \
    --description "Testing worker role with app storage and Python sandbox invoke access" \
    --assume-role-policy-document "file://${WORKER_TRUST}" >/dev/null
else
  aws iam update-assume-role-policy \
    --role-name "${WORKER_ROLE}" \
    --policy-document "file://${WORKER_TRUST}" >/dev/null
fi

for policy_name in s3-claimcheck dynamodb-oauth dynamodb-artifacts; do
  policy_file="${BUILD_DIR}/${policy_name}.json"
  aws iam get-role-policy \
    --role-name "${BASE_APP_ROLE}" \
    --policy-name "${policy_name}" \
    --query PolicyDocument \
    --output json > "${policy_file}"
  aws iam put-role-policy \
    --role-name "${WORKER_ROLE}" \
    --policy-name "${policy_name}" \
    --policy-document "file://${policy_file}" >/dev/null
done

aws iam put-role-policy \
  --role-name "${WORKER_ROLE}" \
  --policy-name python-sandbox-lambda-invoke \
  --policy-document "file://${LAMBDA_INVOKE_POLICY}" >/dev/null

echo ">> Python sandbox Lambda ready: ${FUNCTION_ARN}"
echo ">> Testing worker role ready: arn:aws:iam::${ACCOUNT}:role/${WORKER_ROLE}"
