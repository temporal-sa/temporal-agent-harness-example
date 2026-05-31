from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from typing import Any

from temporalio.client import WorkflowHistory
from temporalio.worker import Replayer

from simple_chat_agent.common.env import load_dotenv
from simple_chat_agent.common.external_storage import simple_chat_data_converter
from simple_chat_agent.worker.tools.subagent import SubagentWorkflow
from simple_chat_agent.worker.user_chats_workflow import UserChatsWorkflow
from simple_chat_agent.worker.workflow import SimpleChatWorkflow


WORKFLOWS = [SimpleChatWorkflow, UserChatsWorkflow, SubagentWorkflow]


async def main() -> None:
    args = _parse_args()
    load_dotenv()

    history_json = _read_history(args.history)
    workflow_id = args.workflow_id or _workflow_id_from_history(history_json)
    if workflow_id is None:
        raise SystemExit(
            "Could not infer workflow ID from history JSON. "
            "Pass --workflow-id <workflow-id>."
        )

    history = WorkflowHistory.from_json(workflow_id, history_json)
    replayer = Replayer(
        workflows=WORKFLOWS,
        data_converter=simple_chat_data_converter(),
        debug_mode=args.debug_mode,
    )

    await replayer.replay_workflow(history)
    print(f"Replay succeeded for {workflow_id} from {args.history}")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replay a Simple Chat Agent Temporal workflow history."
    )
    parser.add_argument(
        "--history",
        type=Path,
        required=True,
        help="Path to workflow history JSON exported from Temporal CLI or UI.",
    )
    parser.add_argument(
        "--workflow-id",
        help=(
            "Workflow ID for the history. Required when the exported JSON does "
            "not include the workflow ID."
        ),
    )
    parser.add_argument(
        "--debug-mode",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Run the Temporal replayer in SDK debug mode. Defaults to true.",
    )
    return parser.parse_args()


def _read_history(path: Path) -> str | dict[str, Any]:
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError as err:
        raise SystemExit(f"History file not found: {path}") from err

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return raw
    if not isinstance(parsed, dict):
        raise SystemExit("History JSON must be an object.")
    return parsed


def _workflow_id_from_history(history: str | dict[str, Any]) -> str | None:
    if not isinstance(history, dict):
        return None

    candidates = [
        history.get("workflowId"),
        history.get("workflow_id"),
        _dig(history, "workflowExecution", "workflowId"),
        _dig(history, "execution", "workflowId"),
        _dig(history, "workflowExecutionInfo", "execution", "workflowId"),
    ]
    for candidate in candidates:
        if isinstance(candidate, str) and candidate:
            return candidate
    return None


def _dig(value: dict[str, Any], *keys: str) -> Any:
    current: Any = value
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


if __name__ == "__main__":
    asyncio.run(main())
