"""The catalog's worker entrypoint (PRD §2.4: the module path stays).

    python -m simple_chat_agent.worker.main

Runs the coding agent's production worker (simple_chat_agent.agent.worker): CodingAgent,
CodingSessions, the subagents and their activities, with Worker Versioning (PINNED), the
codec server and the graceful drain on SIGTERM / SIGINT.
"""

from __future__ import annotations

import asyncio
import sys


async def main() -> None:
    # Imported here so --help works without provider keys (the agent's model registry
    # refuses to load without one).
    from simple_chat_agent.agent.worker import main as agent_worker_main

    await agent_worker_main()


USAGE = """\
usage: python -m simple_chat_agent.worker.main

Runs the coding agent's Temporal worker (simple_chat_agent.agent.worker) and, unless
SIMPLE_CHAT_CODEC_SERVER_ENABLED=0, the codec server (/health) on SIMPLE_CHAT_CODEC_SERVER_PORT.

Environment:
  TEMPORAL_ADDRESS (or TEMPORAL_ENDPOINT), TEMPORAL_NAMESPACE, TEMPORAL_API_KEY, TEMPORAL_TLS
  CODING_AGENT_TASK_QUEUE           task queue (default coding-agent)
  TEMPORAL_WORKER_BUILD_ID          required while Worker Versioning is on
  TEMPORAL_DEPLOYMENT_NAME          Worker Deployment name (default: the task queue)
  CODING_AGENT_WORKER_VERSIONING_ENABLED  default true (PINNED)
  SIMPLE_CHAT_WORKER_GRACEFUL_SHUTDOWN_SECONDS  drain timeout on SIGTERM
"""


def run(argv: list[str] | None = None) -> None:
    args = sys.argv[1:] if argv is None else argv
    if any(a in ("-h", "--help") for a in args):
        print(USAGE, end="")
        return
    asyncio.run(main())


if __name__ == "__main__":
    run()
