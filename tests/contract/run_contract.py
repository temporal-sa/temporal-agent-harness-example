"""P0 step 0.8 / P2 step 2.1a: run the Node harness-client contract check against our agent API.

    .venv/bin/python tests/contract/run_contract.py              # the real API app (default)
    .venv/bin/python tests/contract/run_contract.py --app dev    # the P0 agent dev app

Starts a dev worker and the API on a fresh task queue and Redis prefix, runs
``node harness-client/contract.mjs <base>``, tears both down and exits with node's exit code.

``--app real`` serves ``uvicorn simple_chat_agent.api.main:app`` with local auth (throwaway
credentials and JWT secret, a scratch SQLite store) and the agent routes on the worker's task
queue (``CODING_AGENT_TASK_QUEUE``); the node script logs in through the local login route
and uses the session cookie. ``--app dev`` serves the dev app with its ``X-Test-User`` header.
Needs the local Temporal dev server (TEMPORAL_TEST_ADDRESS, default 127.0.0.1:7244), Redis
(SIMPLE_CHAT_REDIS_URL, default redis://127.0.0.1:6390/0), the sandbox executor and the keys in
.env.local. Run ``npm ci`` in tests/contract/harness-client once first.
"""

from __future__ import annotations

import argparse
import os
import secrets
import socket
import tempfile
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
CLIENT_DIR = HERE / "harness-client"
READY_TIMEOUT = 60.0


def load_env_local(env: dict[str, str]) -> None:
    """Same parsing as tests/integration/support.load_env_local, into ``env``. Never printed."""
    path = ROOT / ".env.local"
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            key, _, value = line.partition("=")
            env.setdefault(key.strip(), value.strip())


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for_line(proc: subprocess.Popen[str], needle: str, timeout: float) -> None:
    """Wait for ``needle`` on the process's stdout; keep echoing its output to stderr after."""
    ready = threading.Event()

    def pump() -> None:
        assert proc.stdout is not None
        for line in proc.stdout:
            sys.stderr.write(line)
            if needle in line:
                ready.set()

    threading.Thread(target=pump, daemon=True).start()
    deadline = time.monotonic() + timeout
    while not ready.wait(0.1):
        if proc.poll() is not None:
            raise RuntimeError(f"worker exited with {proc.returncode} before it was ready")
        if time.monotonic() > deadline:
            raise RuntimeError("worker did not report ready in time")


def wait_for_http(proc: subprocess.Popen[str], url: str, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"API exited with {proc.returncode} before it was ready")
        try:
            # Any HTTP answer means uvicorn is serving (401 without X-Test-User is fine).
            urllib.request.urlopen(url, timeout=1)
            return
        except urllib.error.HTTPError:
            return
        except OSError:
            time.sleep(0.2)
    raise RuntimeError("API did not start in time")


def stop(proc: subprocess.Popen[str] | None) -> None:
    if proc is None or proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--app", choices=("real", "dev"), default="real")
    args = parser.parse_args()
    if not (CLIENT_DIR / "node_modules" / "@temporalio" / "agent-harness-client").exists():
        print("run `npm ci` in tests/contract/harness-client first", file=sys.stderr)
        return 2

    env = dict(os.environ)
    load_env_local(env)
    env.setdefault("SIMPLE_CHAT_REDIS_URL", "redis://127.0.0.1:6390/0")
    env["AGENT_REDIS_PREFIX"] = f"aah-contract-{uuid.uuid4().hex[:6]}"
    env["PYTHONUNBUFFERED"] = "1"
    task_queue = f"p0c-{uuid.uuid4().hex[:6]}"
    port = free_port()
    python = sys.executable

    # The node script gets no provider keys: it only talks HTTP to our API.
    node_env = {k: v for k, v in os.environ.items() if not k.endswith("_API_KEY")}
    scratch = tempfile.TemporaryDirectory(prefix="contract-")
    if args.app == "real":
        username, password = "contract", secrets.token_urlsafe(12)
        # The API process holds no LLM keys (P2 decision 2); the worker gets them.
        api_env = {k: v for k, v in env.items() if not k.endswith("_API_KEY")}
        api_env.update(
            SIMPLE_CHAT_AUTH_MODE="local",
            SIMPLE_CHAT_LOCAL_AUTH_ENABLED="1",
            SIMPLE_CHAT_LOCAL_AUTH_USERNAME=username,
            SIMPLE_CHAT_LOCAL_AUTH_PASSWORD=password,
            SIMPLE_CHAT_JWT_SECRET=secrets.token_urlsafe(32),
            SIMPLE_CHAT_DB_PATH=os.path.join(scratch.name, "app.sqlite3"),
            TEMPORAL_ADDRESS=env.get("TEMPORAL_TEST_ADDRESS", "127.0.0.1:7244"),
            CODING_AGENT_TASK_QUEUE=task_queue,
        )
        api_cmd = [python, "-m", "uvicorn", "simple_chat_agent.api.main:app", "--host", "127.0.0.1",
                   "--port", str(port), "--log-level", "warning"]
        ready_path = "/api/auth/config"
        node_env["CONTRACT_LOGIN"] = f"{username}:{password}"
    else:
        api_env = env
        api_cmd = [python, "-m", "simple_chat_agent.api.agent_dev_app", "--port", str(port), "--task-queue", task_queue]
        ready_path = "/api/agent/status/none"
        node_env.pop("CONTRACT_LOGIN", None)

    worker = api = None
    try:
        worker = subprocess.Popen(
            [python, "-m", "simple_chat_agent.agent.dev_worker", "--task-queue", task_queue],
            cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=sys.stderr, text=True,
        )
        api = subprocess.Popen(api_cmd, cwd=ROOT, env=api_env, stdout=sys.stderr, stderr=sys.stderr, text=True)
        wait_for_line(worker, "worker ready", READY_TIMEOUT)
        base = f"http://127.0.0.1:{port}"
        wait_for_http(api, f"{base}{ready_path}", READY_TIMEOUT)
        print(f"[contract] worker and {args.app} API ready (task queue {task_queue}, port {port})", file=sys.stderr)
        return subprocess.run(["node", "contract.mjs", base], cwd=CLIENT_DIR, env=node_env).returncode
    finally:
        stop(api)
        stop(worker)
        scratch.cleanup()


if __name__ == "__main__":
    sys.exit(main())
