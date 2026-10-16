#!/usr/bin/env bash
# SIGKILL the local coding-agent dev worker(s): the crash trigger for the demo (PRD §2.5) and the
# P3 recovery checks. Finds processes by their command line, `simple_chat_agent.agent.dev_worker`.
#
#   scripts/crash-worker.sh                 # every local dev worker
#   scripts/crash-worker.sh <task-queue>    # only workers polling that task queue
#   make crash-worker [TASK_QUEUE=...]
#
# Prints what it killed. Exits 1 when no matching worker is running.
set -euo pipefail

PATTERN="simple_chat_agent.agent.dev_worker"
QUEUE="${1:-${TASK_QUEUE:-}}"

victims=()
while read -r pid cmd; do
  [[ -z "${pid:-}" || "$pid" == "$$" ]] && continue
  [[ "$cmd" == *"$PATTERN"* ]] || continue
  [[ "$cmd" == *crash-worker* ]] && continue
  if [[ -n "$QUEUE" && "$cmd" != *"--task-queue $QUEUE"* && "$cmd" != *"--task-queue=$QUEUE"* ]]; then
    continue
  fi
  victims+=("$pid")
  echo "SIGKILL pid $pid: $cmd"
done < <(ps -axo pid=,command=)

if [[ ${#victims[@]} -eq 0 ]]; then
  echo "crash-worker: no running $PATTERN${QUEUE:+ on task queue $QUEUE}" >&2
  exit 1
fi

status=0
for pid in "${victims[@]}"; do
  kill -KILL "$pid" 2>/dev/null || { echo "crash-worker: pid $pid was already gone" >&2; status=1; }
done
exit "$status"
