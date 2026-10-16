# Workspace executor

`executor.py` is the credential-free service that owns agent workspaces. It serves:

- `/files/*`: file operations confined to the workspace with `O_NOFOLLOW` component walks.
- `/execute`: a bash command inside Bubblewrap, streamed back as NDJSON.
- `/execute/cancel`: stops a running or queued job.
- `/healthz`: health check.

## `/execute` jobs and reattach

A job is keyed by `(workspace, execution_id)`, and it outlives the HTTP stream that started it.

- **First request** for an `execution_id` starts the job. The job waits for the workspace lock, then runs.
- **Same `execution_id` while the job is queued or running**: the request attaches to that job; it does not start a second process. The executor first replays the buffered events, then streams the rest live. Every stream event carries a `seq`, so a client can skip events it has already seen. The `start` event says `"attached": true`.
- **Same `execution_id` after the job finished**: the executor returns the cached result with `"cached": true`.
- **Same `execution_id` with a different command** while the job is live: HTTP 409.
- **Executor restarted mid-job**: the job is gone, but a `.started` marker remains. The request gets a `WorkspaceJobLost` result. The command is never run a second time.
- **Replay buffer limits**: the buffer is capped at `JOB_REPLAY_BUFFER_CHARS` (128k) and `JOB_REPLAY_BUFFER_EVENTS` (2048), and the oldest events are dropped first. A client that fell behind gets one `{"kind": "dropped"}` marker. The final result is stored outside the buffer and is never dropped.
- **Disconnects**: a client disconnect does not kill the job. A job with no attached client for `WORKSPACE_EXECUTOR_ORPHAN_GRACE_SECONDS` (default 120) is stopped, with result type `WorkspaceShellAbandoned`. `/execute/cancel` stops a job immediately, with result type `WorkspaceShellCancelled`.

The per-workspace locks are unchanged. A job holds `exec` while it runs, so file mutations wait for it and file reads do not.

The worker's `run_command` tool derives `execution_id` from the tool call id. A Temporal retry after a worker crash therefore reattaches to the running job instead of starting a second one.

## Local container (Docker Desktop)

The macOS-native executor has no Bubblewrap, so `/execute` needs this container.

Build from the repo root:

```sh
docker build -f simple_chat_agent/worker/sandbox/Dockerfile.executor -t agent-executor:dev .
```

Run it on port 8083. Pass only the executor token and root, never any LLM or integration keys. The executor's self-test reports unhealthy if it sees any of them.

```sh
export WORKSPACE_EXECUTOR_TOKEN=...   # the same value the worker uses
docker run -d --name agent-executor \
  -p 127.0.0.1:8083:8082 \
  -e WORKSPACE_EXECUTOR_TOKEN \
  -e WORKSPACE_EXECUTOR_ROOT=/workspaces \
  --security-opt seccomp=unconfined \
  --security-opt apparmor=unconfined \
  -v agent-executor-workspaces:/workspaces \
  agent-executor:dev
curl -s 127.0.0.1:8083/healthz     # {"status":"ok"}
```

Then point the worker at it with `WORKSPACE_EXECUTOR_URL=http://127.0.0.1:8083`.

### Privileges

Bubblewrap creates user, mount and PID namespaces. Docker's default seccomp profile blocks those calls, and without the seccomp flag the self-test fails with `No permissions to create a new namespace`.

| Flags | Result on Docker Desktop (tested) |
|---|---|
| none | fails |
| `--security-opt seccomp=unconfined` | works (minimum) |
| `seccomp=unconfined` + `apparmor=unconfined` | works (recommended) |

- `apparmor=unconfined` makes no difference on Docker Desktop, whose VM has no AppArmor. On Ubuntu hosts it is needed, because the `docker-default` AppArmor profile blocks mounts. It is in the command above so the same command works on both.
- Neither `--cap-add SYS_ADMIN` nor `--privileged` is needed. The container runs as uid 65532 with no added capabilities.
- `tini` is PID 1 so that sandbox children are reaped.

### Live check

`tests/test_tools_workspace.py` contains a live suite that is skipped by default. It checks that `run_command` streams and that a concurrent request with the same `execution_id` reattaches. Run it against the container:

```sh
WORKSPACE_EXECUTOR_LIVE_URL=http://127.0.0.1:8083 WORKSPACE_EXECUTOR_TOKEN=... \
  .venv/bin/python -m unittest tests.test_tools_workspace.LiveExecutorTests -v
```
