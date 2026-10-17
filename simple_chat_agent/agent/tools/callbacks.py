"""Browser callback tools (PRD A12): the agent asks the person in the browser.

Both are harness ``callback_tool_defn`` tools: no worker-side body. The call publishes
``callback_requested`` (``tool_input`` plus the result's JSON schema) and waits in the workflow,
durably (a ``wait_condition``, no activity), until the browser answers through
``POST /api/agent/callback-result`` (the ``enqueue_callback_result`` Signal), the wait times out,
or the turn is stopped. A timeout or a stop reaches the model as a tool error.

* ``ask_user(question, options)``: choice cards. The answer is ``{"option_id": ...}`` and must
  be one of the offered ids (:func:`check_callback_result`).
* ``pick_files(prompt, max_files)``: the user picks files; the browser uploads each through
  ``POST /api/agent/sessions/{id}/callbacks/{tool_id}/files``, which writes it into the
  workspace under ``uploads/`` and returns its path. The answer is ``{"paths": [...]}``,
  workspace paths only, never content.

Both are inherently safe (asking is harmless; the person decides), risk class ``read_only``.
"""

# No `from __future__ import annotations`: the harness resolves tool type hints at runtime.

import re
from datetime import timedelta
from typing import Any

from temporalio.exceptions import ApplicationError

from temporal_agent_harness.harness import agent

from simple_chat_agent.agent.contracts import (
    ASK_MAX_OPTIONS,
    PICK_MAX_FILES,
    PICK_MAX_NAME_CHARS,
    PICK_UPLOAD_DIR,
    AskUserAnswer,
    ChoiceOption,
    PickedFiles,
)

# How long a question or file request waits for the browser (tab closed: then a tool error).
CALLBACK_TIMEOUT = timedelta(minutes=30)

# A path pick_files may return: one file directly under uploads/, with an upload-safe name.
UPLOAD_PATH = re.compile(
    rf"^{re.escape(PICK_UPLOAD_DIR)}/[A-Za-z0-9][A-Za-z0-9._-]{{0,{PICK_MAX_NAME_CHARS - 1}}}$"
)


@agent.callback_tool_defn(inherently_safe=True, timeout=CALLBACK_TIMEOUT)
async def ask_user(question: str, options: list[ChoiceOption]) -> AskUserAnswer:
    """Ask the user one multiple-choice question and wait for their pick. Use it when you need
    a decision only they can make (which approach, which file, whether to go ahead). Offer 2 to
    6 options, each with a short unique `id` and a `label`. Returns the chosen option's id."""
    ...


@agent.callback_tool_defn(inherently_safe=True, timeout=CALLBACK_TIMEOUT)
async def pick_files(prompt: str, max_files: int = 5) -> PickedFiles:
    """Ask the user to choose files from their computer (up to `max_files`, at most 10, 5 MB
    each). They are uploaded into the workspace under uploads/ and you get their workspace
    paths; read them with read_file. Use it when you need a file the workspace doesn't have."""
    ...


CALLBACK_TOOLS = (ask_user, pick_files)
CALLBACK_TOOL_NAMES = frozenset(t.__name__ for t in CALLBACK_TOOLS)


def _invalid(message: str) -> ApplicationError:
    return ApplicationError(message, type="InvalidCallbackResult", non_retryable=True)


def offered_option_ids(tool_input: dict[str, Any]) -> list[str]:
    """The option ids an ``ask_user`` call offered (first ASK_MAX_OPTIONS, as the UI shows)."""
    ids: list[str] = []
    for option in (tool_input.get("options") or [])[:ASK_MAX_OPTIONS]:
        option_id = option.get("id") if isinstance(option, dict) else getattr(option, "id", None)
        if isinstance(option_id, str) and option_id:
            ids.append(option_id)
    return ids


def pick_limit(tool_input: dict[str, Any]) -> int:
    """How many files a ``pick_files`` call accepts: its ``max_files``, within 1..PICK_MAX_FILES."""
    try:
        requested = int(tool_input.get("max_files", 5))
    except (TypeError, ValueError):
        requested = 5
    return max(1, min(requested, PICK_MAX_FILES))


def check_callback_result(tool_name: str, tool_input: dict[str, Any], result: Any) -> None:
    """Refuse an answer that doesn't fit the question it answers (raises ApplicationError
    ``InvalidCallbackResult``). Runs in the workflow, on both the Signal front door and the
    harness ``provide_callback_result`` Update, after the harness's own type check."""
    if tool_name == "ask_user":
        option_id = result.get("option_id") if isinstance(result, dict) else getattr(result, "option_id", None)
        offered = offered_option_ids(tool_input)
        if option_id not in offered:
            raise _invalid(f"{option_id!r} is not one of the offered options ({', '.join(offered)}).")
    elif tool_name == "pick_files":
        paths = result.get("paths") if isinstance(result, dict) else getattr(result, "paths", None)
        if not isinstance(paths, list):
            raise _invalid("paths must be a list of workspace paths.")
        limit = pick_limit(tool_input)
        if len(paths) > limit:
            raise _invalid(f"At most {limit} file(s) were asked for; got {len(paths)}.")
        if len(set(paths)) != len(paths):
            raise _invalid("The same file is listed twice.")
        for path in paths:
            if not isinstance(path, str) or not UPLOAD_PATH.fullmatch(path):
                raise _invalid(f"{str(path)[:120]!r} is not an uploaded file path ({PICK_UPLOAD_DIR}/<name>).")
