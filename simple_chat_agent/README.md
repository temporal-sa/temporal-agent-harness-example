# simple_chat_agent

The coding agent demo app. The repo root `README.md` explains the demo; this file maps the
package.

| Path | What it is | Entrypoint |
| --- | --- | --- |
| `agent/` | The coding agent on the official harness: `CodingAgent`, `CodingSessions`, subagents, tools, approvals, MCP. | `agent/worker.py` (production), `agent/dev_worker.py` (local) |
| `worker/main.py` | The catalog's worker entrypoint; runs `agent/worker.py`. | `python -m simple_chat_agent.worker.main` |
| `worker/codec_server.py`, `worker/shutdown.py` | Temporal Web codec server (`/health`) and the graceful-drain settings. | started by the worker |
| `worker/sandbox/` | The credential-free Bubblewrap executor. | `python -m simple_chat_agent.worker.sandbox.executor` |
| `api/` | FastAPI: `/api/agent/*` (`agent_*.py`), sign-in and `/api/me`, artifacts, the console at `/console/`, and the web app at `/`. | `python -m simple_chat_agent.api.main` |
| `common/` | Shared S3/Dynamo storage, payload offload, attachments, MCP OAuth, env loading. | |
| `web/` | The product view (Vite + React), built to `web/dist` and served by the API at `/`. | `make web`, `make web-dev` |

Docker: `Dockerfile` builds one image for the API and the worker (default command: the
worker); `worker/sandbox/Dockerfile.executor` builds the executor; `redis.Dockerfile` builds
Redis.
