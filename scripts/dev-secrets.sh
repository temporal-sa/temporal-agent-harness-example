#!/usr/bin/env bash
# Pulls the catalog's shared LLM keys into .env.local for local development.
# Values are never printed. The file is written with mode 600.
set -euo pipefail
set +x

REGION="${AWS_REGION_SECRETS:-us-west-1}"
PREFIX="tmprl-dem-cld/platform/shared"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="$ROOT/.env.local"

if git -C "$ROOT" ls-files --error-unmatch .env.local >/dev/null 2>&1; then
  echo "refusing: .env.local is tracked by git" >&2
  exit 1
fi
if ! git -C "$ROOT" check-ignore -q .env.local; then
  echo "refusing: .env.local is not ignored by git" >&2
  exit 1
fi

umask 077
tmp="$(mktemp "$ROOT/.env.local.XXXXXX")"
trap 'rm -f "$tmp"' EXIT

# Keep any non-LLM lines a developer already has.
if [[ -f "$OUT" ]]; then
  grep -vE '^(ANTHROPIC_API_KEY|OPENAI_API_KEY|GEMINI_API_KEY|GOOGLE_API_KEY)=' "$OUT" >"$tmp" || true
fi

for name in anthropic openai gemini; do
  aws secretsmanager get-secret-value \
    --region "$REGION" \
    --secret-id "$PREFIX/$name" \
    --query SecretString \
    --output text |
    python3 -c 'import json,sys; [print(f"{k}={v}") for k, v in json.load(sys.stdin).items()]' >>"$tmp"
done

chmod 600 "$tmp"
mv "$tmp" "$OUT"
trap - EXIT
echo "wrote $(grep -c '=' "$OUT") entries to .env.local (values not shown)"
