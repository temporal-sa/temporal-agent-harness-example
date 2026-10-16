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
- **Same `execution_id` after the job finished**: the executor replays the job's saved replay buffer (stored next to the result, so it has the same bounds), then returns the cached result with `"cached": true`. A client that reattaches after the command ended still gets the output it missed, and it can skip events by `seq`.
- **Same `execution_id` with a different command** while the job is live: HTTP 409.
- **Executor restarted mid-job**: the job is gone, but a `.started` marker remains. The request gets a `WorkspaceJobLost` result. The command is never run a second time.
- **Replay buffer limits**: the buffer is capped at `JOB_REPLAY_BUFFER_CHARS` (128k) and `JOB_REPLAY_BUFFER_EVENTS` (2048), and the oldest events are dropped first. A client that fell behind gets one `{"kind": "dropped"}` marker. The final result is stored outside the buffer and is never dropped.
- **Disconnects**: a client disconnect does not kill the job. A job with no attached client for `WORKSPACE_EXECUTOR_ORPHAN_GRACE_SECONDS` (default 120) is stopped, with result type `WorkspaceShellAbandoned`. `/execute/cancel` stops a job immediately, with result type `WorkspaceShellCancelled`.

- **Stream events**: one JSON object per line, `{"kind": ..., "seq": n, "payload": {...}}`. `kind` is `start`, `stdout`, `stderr` (`payload.text`), `progress`, `dropped` or `result`. Output events keep their `stdout`/`stderr` kind in the replay buffer, so a reattached or late client still knows which stream each chunk came from. The worker's `run_command` forwards them to the browser as tagged frames (see the docstring of `simple_chat_agent/agent/tools/workspace.py`).
- **Result `git_branch`**: the result payload names the branch checked out in the workspace root's repo after the command, read from `.git/HEAD` without running git and without following symlinks. It is `"HEAD (no branch)"` when detached, and `null` when the root isn't a repo or HEAD can't be read (a `.git` file, reftable storage). An older executor omits the field; the worker treats that as unknown.

- **Fingerprint**: every live job and every cached result records a fingerprint of its command, cwd, timeout, network and stdin. A request whose fingerprint doesn't match gets HTTP 409, so a reused `execution_id` never receives another command's result. The worker derives ids from workflow id + run id + activity/tool id, so continue-as-new can't collide.

### Locks and concurrency

- A job takes its workspace's `exec` lock first, then a global slot (`WORKSPACE_EXECUTOR_MAX_CONCURRENT`, default 4). Calls queued behind a long command in one session hold no slots, so they can't starve other sessions.
- File mutations take `exec`, then `files` (exclusive), so they wait for a running command; reads don't.
- A mutation may pass `busy_timeout_seconds`. If the workspace is still busy after that time, it fails with `409 workspace_busy` and nothing is written. The message says whether a command is running or only queued. If the workspace is free, the lock is taken immediately, so `0` means "only if free right now". The worker's `write_file`/`edit_file` pass 10 s, so the result is either "written" or "busy, not written", never a client timeout for a write that lands later.

### Git safety

- `/files/*` refuses to change git control data (`403 protected_path`, compared case-insensitively): anything under `.git/`, a root-level `.gitconfig`, and anything under `.config/git/`. The sandbox `HOME` is `/workspace`, so the last two are git's global config locations.
- Sandboxed commands run with `XDG_CONFIG_HOME=/tmp/.config`, a per-command tmpfs, so `/workspace/.config/git/{config,attributes}` never act as global git config.
- The worker's git tools (`git_status`, `git_diff`, `git_commit`) **fail closed**. Before any git call, a preflight checks the repo-local config and `config.worktree`, both read without following includes. Any key outside a small allowlist refuses the call with a non-retryable `GitConfigRefused` that names the keys: `include.*`/`includeIf.*`, filters, diff drivers, gc hooks, and so on. A non-empty `.git/info/attributes` refuses too.
  - The allowlist covers what `git init` and `git clone` write (core layout flags, `remote.*.url`/`fetch`, `branch.*.remote`/`merge`/`rebase`), plus identity and line-ending settings. Each key is justified in `simple_chat_agent/agent/tools/workspace.py`.
  - Git also runs with no global or system config or attributes, attributes from an empty tree (`--attr-source`), and overrides passed through `GIT_CONFIG_COUNT`/`GIT_CONFIG_KEY_n`/`GIT_CONFIG_VALUE_n` (never `-c key=value`). The overrides turn off hooks, fsmonitor, signing, auto-gc and maintenance.
- The executor requires git 2.40 or later: a startup self-test makes `/healthz` fail on an older git. The Dockerfile pins the base image by tag and digest.
- Every sandboxed command gets a git identity: `GIT_AUTHOR_*` and `GIT_COMMITTER_*`, "Coding Agent" / `agent@coding-agent.invalid` by default. Override them with `WORKSPACE_EXECUTOR_GIT_NAME` and `WORKSPACE_EXECUTOR_GIT_EMAIL`.

### State cleanup

Per-execution state files (result, replay events, started marker) older than `WORKSPACE_EXECUTOR_STATE_TTL_SECONDS` (default 86400) are deleted at startup and then at most hourly. Files of live jobs are kept. The TTL is far longer than any activity's retry window, so idempotency is unaffected.

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
