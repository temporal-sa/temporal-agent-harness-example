"""Recovery, usage and worker-health telemetry for the bridge (PRD S3, U5, U6; CTO-2).

The bridge appends four frame types to the session's Redis stream, each at the position of the
frame before it (``x_rejected``: at the position of the stream event it replaces):

``x_usage``     one per model call. Live from ``model_interaction_ended`` (``confirmed: false``),
                then confirmed from the model activity's result in workflow history at the
                agent's ``turn_end`` (``confirmed: true``), which adds cache-write tokens.
``x_recovery``  a model call or tool activity that ran again because a worker was lost. Live from
                the stream (a second ``model_interaction_started`` for the same call), enriched
                by Describe, confirmed from history (``ActivityTaskStarted.attempt > 1``, its
                identity and the previous attempt's failure). Also for ``run_command`` reattach.
``x_worker``    "is a worker answering" hints while a turn is open and the stream is silent,
                from Describe and DescribeTaskQueue (no worker needed, not Queries).
``x_rejected``  a front-door Signal (``enqueue_message`` / ``enqueue_approval``) the agent
                refused: unknown or already-resolved approval, unknown or malformed message,
                session closed. The workflow publishes it on its own event stream (a
                ``message_handler_error`` whose ``message_id`` starts with
                ``REJECTION_MESSAGE_PREFIX`` and whose text is the rejection as JSON); the
                bridge stores this frame in its place. No Query, and it survives a reload.

Every frame has an ``id``; the latest frame with a given ``id`` wins (a confirmed frame
replaces the live one). Fields a provider doesn't report are ``null``, never 0. Every frame
has an ``agent_id``: the agent whose turn it describes, else the root agent's, so a client
never attributes one to an unknown agent.

Honesty rules (the UI shows recoveries as durability savings):

* A retry counts as a lost worker only when the failed attempt hit its heartbeat timeout, or hit
  its start-to-close timeout and the next attempt ran on another worker. A start-to-close
  timeout on the same worker is a slow provider or tool (``cause: "timeout"``); any other
  failure keeps its type as ``cause`` (e.g. ``application_error:RateLimited``). Both are
  confirmed with ``retracted: true`` when the stream had shown a marker, else not shown.
* ``kept`` counts only work that finished: model calls that completed and tools that ended
  without error, before the failed attempt. It never includes the repeated call.
* ``repeated.resent_input_tokens_estimate`` is the retried call's prompt size times the extra
  attempts: the input tokens sent again. It is an estimate (the lost attempt's own usage was
  never reported) and is never added to anything kept.

Nothing here needs a worker: history, Describe and DescribeTaskQueue are served by Temporal.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Literal

from temporalio.api.common.v1 import WorkflowExecution
from temporalio.api.enums.v1 import PendingActivityState, PendingWorkflowTaskState, TimeoutType
from temporalio.api.enums.v1 import TaskQueueType
from temporalio.api.history.v1 import HistoryEvent
from temporalio.api.taskqueue.v1 import TaskQueue
from temporalio.api.workflowservice.v1 import (
    DescribeTaskQueueRequest,
    GetWorkflowExecutionHistoryReverseRequest,
)
from temporalio.client import Client

log = logging.getLogger(__name__)

X_USAGE = "x_usage"
X_RECOVERY = "x_recovery"
X_WORKER = "x_worker"
X_REJECTED = "x_rejected"
# Same value as harness_ext.REJECTION_MESSAGE_PREFIX (the workflow side); a test pins them.
REJECTION_MESSAGE_PREFIX = "x_rejected:"

MODEL_ACTIVITY_SUFFIXES = ("__model_request_stream", "__model_request")
INTERNAL_ACTIVITY_SUFFIXES = ("__event_stream_handler", "__cancel_suspended_response")
INTERNAL_ACTIVITY_NAMES = frozenset({"judge_tool_call", "run_subagent_turn"})
COMMAND_ACTIVITY_NAMES = frozenset({"run_command", "workspace.run_command"})
# Most recent (workflow, activity, attempt) -> worker identities remembered per session.
IDENTITY_CAP = 1000

# A pending activity that heartbeats but hasn't for this long looks orphaned.
HEARTBEAT_STALE_SECONDS = 6.0
# A workflow or activity task nobody has picked up for this long has no worker.
UNCLAIMED_TASK_SECONDS = 5.0
# A poller seen this recently counts as a live worker (Temporal's own window is ~1 min).
POLLER_FRESH_SECONDS = 70.0

ActivityKind = Literal["model", "tool", "internal"]


def activity_kind(name: str) -> ActivityKind:
    if name.endswith(MODEL_ACTIVITY_SUFFIXES):
        return "model"
    if name.endswith(INTERNAL_ACTIVITY_SUFFIXES) or name in INTERNAL_ACTIVITY_NAMES:
        return "internal"
    return "tool"


# -- usage ---------------------------------------------------------------------------------


def _family(provider: str | None) -> str | None:
    p = (provider or "").lower()
    for fam in ("anthropic", "openai", "google"):
        if p.startswith(fam):
            return fam
    return None


def usage_from_response(response: dict[str, Any]) -> dict[str, Any]:
    """Usage of one model call from the model activity's result (a Pydantic AI
    ``ModelResponse`` as JSON). ``input_tokens`` is the whole prompt, cache reads and writes
    included. A count the provider doesn't report is None."""
    u = response.get("usage") or {}
    details = u.get("details") or {}
    fam = _family(response.get("provider_name"))
    cache_read = u.get("cache_read_tokens")
    if fam is None and not cache_read:
        cache_read = None
    # Only Anthropic bills (and reports) prompt-cache writes; others cache implicitly.
    if fam == "anthropic":
        cache_write = u.get("cache_write_tokens", 0)
    else:
        cache_write = u.get("cache_write_tokens") or None
    if "reasoning_tokens" in details:
        thinking = details["reasoning_tokens"]
    elif "thoughts_tokens" in details:
        thinking = details["thoughts_tokens"]
    elif fam == "google":
        thinking = 0  # Gemini reports thoughts_token_count; Pydantic AI records it only when > 0
    else:
        thinking = None  # Anthropic counts thinking inside output_tokens and doesn't split it
    return {
        "model": response.get("model_name"),
        "provider": response.get("provider_name"),
        "input_tokens": u.get("input_tokens"),
        "output_tokens": u.get("output_tokens"),
        "cache_read_tokens": cache_read,
        "cache_write_tokens": cache_write,
        "thinking_tokens": thinking,
    }


def usage_from_live(usage: dict[str, Any] | None) -> dict[str, Any] | None:
    """Usage from a ``model_interaction_ended`` frame (harness ``TokenUsage``). It has no
    cache-write field, and the harness reports a zero cache read as None."""
    if usage is None:
        return None
    return {
        "input_tokens": usage.get("input_tokens"),
        "output_tokens": usage.get("output_tokens"),
        "cache_read_tokens": usage.get("cached_tokens"),
        "cache_write_tokens": None,
        "thinking_tokens": usage.get("thought_tokens"),
    }


# -- failures ------------------------------------------------------------------------------


def failure_cause(failure: Any) -> str | None:
    """A short cause for an activity attempt's failure (a ``temporal.api.failure.v1.Failure``)."""
    if failure is None or not failure.ByteSize():
        return None
    f = failure
    for _ in range(4):
        if f.HasField("timeout_failure_info"):
            tt = f.timeout_failure_info.timeout_type
            return {
                TimeoutType.TIMEOUT_TYPE_HEARTBEAT: "heartbeat_timeout",
                TimeoutType.TIMEOUT_TYPE_START_TO_CLOSE: "start_to_close_timeout",
                TimeoutType.TIMEOUT_TYPE_SCHEDULE_TO_START: "schedule_to_start_timeout",
                TimeoutType.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE: "schedule_to_close_timeout",
            }.get(tt, "timeout")
        if f.HasField("application_failure_info"):
            return f"application_error:{f.application_failure_info.type or 'error'}"
        if f.HasField("canceled_failure_info"):
            return "canceled"
        if not f.HasField("cause"):
            break
        f = f.cause
    return "other"


# -- history -------------------------------------------------------------------------------


def _ts(pb_ts: Any) -> float:
    return pb_ts.seconds + pb_ts.nanos / 1e9


@dataclass
class ActivityRecord:
    activity_id: str
    name: str
    kind: ActivityKind
    scheduled_event_id: int
    scheduled_at: float
    run_id: str = ""
    attempt: int = 1
    identity: str | None = None
    last_failure: Any = None  # the previous attempt's failure (ActivityTaskStarted.last_failure)
    closed_event_id: int | None = None
    closed_at: float | None = None
    status: str = "scheduled"
    result: Any = None  # temporal.api.common.v1.Payload of a completed activity

    @property
    def cause(self) -> str | None:
        return failure_cause(self.last_failure)


def worker_lost(cause: str | None, previous_worker: str | None, worker: str | None) -> bool:
    """Was a retry caused by losing a worker? Only on a heartbeat timeout (the attempt stopped
    heartbeating), or on a start-to-close timeout followed by an attempt on another worker.
    A start-to-close timeout on the same worker is a slow call, and any other failure (a
    provider or tool error) is not a crash even if the retry landed on another replica."""
    if cause == "heartbeat_timeout":
        return True
    if cause == "start_to_close_timeout":
        return bool(previous_worker and worker and previous_worker != worker)
    return False


def shown_cause(cause: str | None, lost: bool) -> str | None:
    if not lost and cause == "start_to_close_timeout":
        return "timeout"
    return cause


def parse_activities(events: list[HistoryEvent], run_id: str = "") -> list[ActivityRecord]:
    """Activities in one run's events (forward order), by scheduled event id."""
    by_id: dict[int, ActivityRecord] = {}
    for ev in events:
        if ev.HasField("activity_task_scheduled_event_attributes"):
            a = ev.activity_task_scheduled_event_attributes
            by_id[ev.event_id] = ActivityRecord(
                activity_id=a.activity_id,
                name=a.activity_type.name,
                kind=activity_kind(a.activity_type.name),
                scheduled_event_id=ev.event_id,
                scheduled_at=_ts(ev.event_time),
                run_id=run_id,
            )
        elif ev.HasField("activity_task_started_event_attributes"):
            a = ev.activity_task_started_event_attributes
            rec = by_id.get(a.scheduled_event_id)
            if rec:
                rec.attempt = a.attempt or 1
                rec.identity = a.identity or None
                rec.last_failure = a.last_failure if a.HasField("last_failure") else None
                rec.status = "started"
        else:
            for attr, status in (
                ("activity_task_completed_event_attributes", "completed"),
                ("activity_task_failed_event_attributes", "failed"),
                ("activity_task_timed_out_event_attributes", "timed_out"),
                ("activity_task_canceled_event_attributes", "canceled"),
            ):
                if ev.HasField(attr):
                    a = getattr(ev, attr)
                    rec = by_id.get(a.scheduled_event_id)
                    if rec:
                        rec.status = status
                        rec.closed_event_id = ev.event_id
                        rec.closed_at = _ts(ev.event_time)
                        if status == "completed" and a.HasField("result") and a.result.payloads:
                            rec.result = a.result.payloads[0]
                    break
    return sorted(by_id.values(), key=lambda r: r.scheduled_event_id)


def in_window(records: list[ActivityRecord], start_ts: float | None, end_ts: float | None) -> list[ActivityRecord]:
    """Activities scheduled inside a turn. ``start_ts``/``end_ts`` are the harness timestamps
    of the agent's ``turn_started``/``turn_end`` (workflow time of the tasks that published
    them); an activity of the turn is scheduled by a task at or after the first and before
    the second."""
    lo = (start_ts or 0.0) - 0.0005
    hi = (end_ts if end_ts is not None else float("inf")) + 0.0005
    return [r for r in records if lo <= r.scheduled_at <= hi]


class HistoryReader:
    """Reads a turn's activities from workflow history, newest first (reverse history), so
    the cost scales with the turn, not the session."""

    def __init__(self, client: Client, *, page_size: int = 250) -> None:
        self._client = client
        self._page_size = page_size

    async def _run_events_since(self, workflow_id: str, run_id: str, since: float) -> tuple[list[HistoryEvent], HistoryEvent | None]:
        """This run's events from the newest back to ``since`` (forward order), and the run's
        WorkflowExecutionStarted event if the walk reached it."""
        svc = self._client.workflow_service
        token = b""
        collected: list[HistoryEvent] = []
        started: HistoryEvent | None = None
        while True:
            resp = await svc.get_workflow_execution_history_reverse(
                GetWorkflowExecutionHistoryReverseRequest(
                    namespace=self._client.namespace,
                    execution=WorkflowExecution(workflow_id=workflow_id, run_id=run_id),
                    maximum_page_size=self._page_size,
                    next_page_token=token,
                )
            )
            stop = False
            for ev in resp.history.events:
                if ev.HasField("workflow_execution_started_event_attributes"):
                    started = ev
                collected.append(ev)
                if _ts(ev.event_time) < since - 1.0:
                    stop = True
                    break
            token = resp.next_page_token
            if stop or not token:
                break
        collected.reverse()
        return collected, started

    async def activities(
        self, workflow_id: str, start_ts: float | None, end_ts: float | None = None
    ) -> list[ActivityRecord]:
        """Activities scheduled in ``[start_ts, end_ts]``, across a continue-as-new boundary."""
        desc = await self._client.get_workflow_handle(workflow_id).describe()
        run_id = desc.run_id
        since = start_ts or 0.0
        records: list[ActivityRecord] = []
        for _ in range(3):  # this run and at most two earlier ones
            events, started = await self._run_events_since(workflow_id, run_id, since)
            records = parse_activities(events, run_id) + records
            if started is None or _ts(started.event_time) <= since:
                break
            prev = started.workflow_execution_started_event_attributes.continued_execution_run_id
            if not prev:
                break
            run_id = prev
        return in_window(records, start_ts, end_ts)

    async def decode(self, payload: Any) -> Any:
        """A payload as plain JSON data (offloaded payloads are fetched)."""
        if payload is None:
            return None
        return (await self._client.data_converter.decode([payload]))[0]


# -- live state ----------------------------------------------------------------------------


@dataclass
class Call:
    index: int
    message_id: str | None
    model: str | None
    started_pos: int
    ended: bool = False
    usage: dict[str, Any] | None = None
    retries: int = 0  # extra attempts seen on the stream
    chars: int = 0  # streamed this attempt
    failed_chars: int = 0  # streamed by attempts that didn't finish
    detected_at: float | None = None
    lost_at: float | None = None
    activity_id: str | None = None
    lost_worker: str | None = None
    resumed_on: str | None = None


@dataclass
class Turn:
    agent_id: str
    workflow_id: str
    turn_id: str
    turn_number: int
    started_ts: float | None = None
    ended_ts: float | None = None
    message_id: str | None = None
    calls: list[Call] = field(default_factory=list)
    tool_ends: int = 0
    last_frame_ts: float | None = None
    is_root: bool = False

    @property
    def key(self) -> tuple[str, str]:
        return (self.agent_id, self.turn_id)


@dataclass
class Job:
    kind: Literal["recovery_detected", "recovery_confirm", "turn_confirm"]
    turn: Turn
    call: Call | None = None


@dataclass
class Observation:
    frames: list[tuple[str, dict[str, Any]]] = field(default_factory=list)
    jobs: list[Job] = field(default_factory=list)


def _frame(event: str, turn: Turn, payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    return event, {
        "agent_id": turn.agent_id,
        "turn_id": turn.turn_id,
        "turn_number": turn.turn_number,
        **payload,
        "timestamp": time.time(),
    }


def usage_frame(
    turn: Turn,
    *,
    call_index: int,
    message_id: str | None,
    model: str | None,
    usage: dict[str, Any],
    confirmed: bool,
    attempt: int | None = None,
) -> tuple[str, dict[str, Any]]:
    return _frame(
        X_USAGE,
        turn,
        {
            "id": f"usage:{turn.agent_id}:{turn.turn_id}:{call_index}",
            "message_id": message_id,
            "call_index": call_index,
            "model": model,
            "input_tokens": usage.get("input_tokens"),
            "output_tokens": usage.get("output_tokens"),
            "cache_read_tokens": usage.get("cache_read_tokens"),
            "cache_write_tokens": usage.get("cache_write_tokens"),
            "thinking_tokens": usage.get("thinking_tokens"),
            "confirmed": confirmed,
            **({"provider": usage["provider"]} if usage.get("provider") else {}),
            **({"attempt": attempt} if attempt is not None else {}),
        },
    )


def recovery_frame(
    turn: Turn,
    *,
    key: str,
    kind: str,
    confirmed: bool,
    detected_at: float | None,
    lost_at: float | None,
    lost_worker: str | None,
    resumed_on: str | None,
    kept: dict[str, int],
    repeated: dict[str, Any],
    attempt: int | None = None,
    cause: str | None = None,
    call_index: int | None = None,
    activity_id: str | None = None,
    tool_name: str | None = None,
    retracted: bool = False,
) -> tuple[str, dict[str, Any]]:
    return _frame(
        X_RECOVERY,
        turn,
        {
            "id": f"recovery:{turn.agent_id}:{turn.turn_id}:{key}",
            "kind": kind,
            "call_index": call_index,
            "activity_id": activity_id,
            "tool_name": tool_name,
            "attempt": attempt,
            "cause": cause,
            "detected_at": detected_at,
            "lost_at": lost_at,
            "lost_worker": lost_worker,
            "resumed_on": resumed_on,
            "kept": kept,
            "repeated": repeated,
            "confirmed": confirmed,
            "retracted": retracted,
        },
    )


class Telemetry:
    """Live telemetry state for one session's forwarder. ``observe`` every stored frame (and
    every frame a takeover replays) in stream order; run the returned jobs with the
    ``run_job`` coroutine (they need Temporal) and store the frames they return."""

    def __init__(self, session_id: str) -> None:
        self.session = session_id
        self.workflows: dict[str, str] = {}  # subagent agent_id -> child workflow id
        self.turns: dict[tuple[str, str], Turn] = {}
        self.open: dict[str, Turn] = {}  # agent_id -> its open turn
        # (workflow_id, activity_id, attempt) -> worker identity, from Describe. Bounded.
        self.identities: OrderedDict[tuple[str, str, int], str] = OrderedDict()
        # Tool ids waiting on a human (approval) or a client (callback), per agent.
        self.awaiting: dict[str, set[str]] = {}
        # The root agent's id (the harness mints it per session), for frames outside any turn.
        self.root_agent_id: str | None = None

    @property
    def awaiting_human(self) -> bool:
        return any(self.awaiting.values())

    def workflow_for(self, agent_id: str) -> str:
        return self.workflows.get(agent_id, self.session)

    def open_workflows(self) -> list[str]:
        wfs = [self.session] + [t.workflow_id for t in self.open.values() if t.workflow_id != self.session]
        return list(dict.fromkeys(wfs))

    def _turn(self, data: dict[str, Any], is_root: bool) -> Turn | None:
        agent, turn_id = data.get("agent_id"), data.get("turn_id")
        if not agent or not turn_id:
            return None
        turn = self.turns.get((agent, turn_id))
        if turn is None:
            turn = Turn(
                agent_id=agent,
                workflow_id=self.workflow_for(agent),
                turn_id=turn_id,
                turn_number=int(data.get("turn_number") or 0),
                is_root=is_root,
            )
            self.turns[turn.key] = turn
            # Keep the map small: finished turns are only needed until their confirmation.
            if len(self.turns) > 200:
                for k in [k for k, t in self.turns.items() if t.ended_ts is not None][:100]:
                    self.turns.pop(k, None)
        return turn

    def observe(self, event: str, data: dict[str, Any], position: int, *, is_root: bool) -> Observation:
        obs = Observation()
        if event.startswith("x_"):
            return obs
        if is_root and data.get("agent_id"):
            self.root_agent_id = data["agent_id"]
        if event == "subagent_started" and data.get("subagent_id") and data.get("workflow_id"):
            self.workflows[data["subagent_id"]] = data["workflow_id"]
        turn = self._turn(data, is_root)
        if turn is None:
            return obs
        ts = data.get("timestamp")
        if event == "turn_started":
            turn.started_ts = ts
            self.open[turn.agent_id] = turn
        elif event == "message_handler_start":
            turn.message_id = turn.message_id or data.get("message_id")
        elif event == "model_interaction_started":
            prev = turn.calls[-1] if turn.calls else None
            if prev is not None and (not prev.ended or prev.usage is None):
                # The same call again: its previous attempt never finished (the worker died
                # mid-stream) or ended without usage (the stream failed).
                prev.retries += 1
                prev.failed_chars += prev.chars
                prev.chars = 0
                prev.ended = False
                prev.usage = None
                prev.detected_at = ts
                prev.lost_at = turn.last_frame_ts
                obs.jobs.append(Job("recovery_detected", turn, prev))
            else:
                turn.calls.append(
                    Call(index=len(turn.calls), message_id=data.get("message_id"), model=data.get("model"), started_pos=position)
                )
        elif event in ("reply_delta", "thought_summary"):
            if turn.calls and not turn.calls[-1].ended:
                turn.calls[-1].chars += len(str(data.get("text") or data.get("delta") or ""))
        elif event == "model_interaction_ended":
            if turn.calls:
                call = turn.calls[-1]
                call.ended = True
                call.usage = usage_from_live(data.get("usage"))
                if call.usage is not None:
                    obs.frames.append(
                        usage_frame(
                            turn,
                            call_index=call.index,
                            message_id=call.message_id,
                            model=data.get("model") or call.model,
                            usage=call.usage,
                            confirmed=False,
                        )
                    )
                    if call.retries:
                        obs.jobs.append(Job("recovery_confirm", turn, call))
        elif event == "tool_end":
            turn.tool_ends += 1  # only tools that finished without error count as kept
        elif event in ("tool_approval_requested", "callback_requested") and data.get("tool_id"):
            self.awaiting.setdefault(turn.agent_id, set()).add(data["tool_id"])
        elif event == "turn_end":
            turn.ended_ts = ts
            self.awaiting.pop(turn.agent_id, None)
            if self.open.get(turn.agent_id) is turn:
                self.open.pop(turn.agent_id, None)
            obs.jobs.append(Job("turn_confirm", turn))
        if event in ("tool_approval_resolved", "callback_resolved", "tool_start", "tool_end", "tool_error"):
            self.awaiting.get(turn.agent_id, set()).discard(data.get("tool_id"))
        if ts is not None:
            turn.last_frame_ts = ts
        return obs

    def note_pending(self, workflow_id: str, activity_id: str, attempt: int, identity: str) -> None:
        if identity:
            key = (workflow_id, activity_id, attempt)
            self.identities[key] = identity
            self.identities.move_to_end(key)
            while len(self.identities) > IDENTITY_CAP:
                self.identities.popitem(last=False)

    def live_kept(self, turn: Turn, call: Call) -> dict[str, int]:
        done = [c for c in turn.calls if c.index < call.index and c.usage is not None]
        return {
            "model_calls": len(done),
            "tool_calls": turn.tool_ends,
            "tokens_in": sum(c.usage.get("input_tokens") or 0 for c in done),  # type: ignore[union-attr]
            "tokens_out": sum(c.usage.get("output_tokens") or 0 for c in done),  # type: ignore[union-attr]
        }


# -- jobs (need Temporal) --------------------------------------------------------------------


async def _pending_model_activity(client: Client, workflow_id: str) -> Any | None:
    desc = await client.get_workflow_handle(workflow_id).describe()
    for pa in desc.raw_description.pending_activities:
        if activity_kind(pa.activity_type.name) == "model":
            return pa
    return None


def _kept_from_history(
    records: list[ActivityRecord], target: ActivityRecord, usages: dict[int, dict[str, Any]]
) -> dict[str, int]:
    """Work in the turn that completed before ``target`` was scheduled."""
    before = [
        r
        for r in records
        if r.status == "completed" and r.closed_event_id is not None and r.run_id == target.run_id
        and r.closed_event_id < target.scheduled_event_id
    ] + [r for r in records if r.status == "completed" and r.run_id != target.run_id and r.scheduled_at < target.scheduled_at]
    models = [r for r in before if r.kind == "model"]
    return {
        "model_calls": len(models),
        "tool_calls": len([r for r in before if r.kind == "tool"]),
        "tokens_in": sum((usages.get(r.scheduled_event_id) or {}).get("input_tokens") or 0 for r in models),
        "tokens_out": sum((usages.get(r.scheduled_event_id) or {}).get("output_tokens") or 0 for r in models),
    }


async def _usages(reader: HistoryReader, records: list[ActivityRecord]) -> dict[int, dict[str, Any]]:
    out: dict[int, dict[str, Any]] = {}
    for r in records:
        if r.kind == "model" and r.status == "completed" and r.result is not None:
            try:
                response = await reader.decode(r.result)
            except Exception as exc:  # noqa: BLE001 - an unreadable payload leaves that call unconfirmed
                log.warning("telemetry: can't decode model result %s: %s", r.activity_id, exc)
                continue
            if isinstance(response, dict):
                out[r.scheduled_event_id] = usage_from_response(response)
    return out


async def detect_recovery(client: Client, tel: Telemetry, turn: Turn, call: Call) -> list[tuple[str, dict[str, Any]]]:
    """A second ``model_interaction_started`` for one call: Describe the agent's workflow for
    the retried activity and emit an unconfirmed ``x_recovery``."""
    cause = None
    attempt = call.retries + 1
    try:
        pa = await _pending_model_activity(client, turn.workflow_id)
    except Exception as exc:  # noqa: BLE001 - Describe failing doesn't stop the marker
        log.info("telemetry: describe failed for %s: %s", turn.workflow_id, exc)
        pa = None
    if pa is not None:
        if pa.attempt <= 1:
            # Not a Temporal retry (a new activity after a failed stream); history settles it.
            return []
        call.activity_id = pa.activity_id
        attempt = pa.attempt
        cause = failure_cause(pa.last_failure) if pa.HasField("last_failure") else None
        if pa.state == PendingActivityState.PENDING_ACTIVITY_STATE_STARTED:
            call.resumed_on = pa.last_worker_identity or None
        call.lost_worker = tel.identities.get((turn.workflow_id, pa.activity_id, pa.attempt - 1))
        if cause is not None and not worker_lost(cause, call.lost_worker, call.resumed_on):
            # A slow or failing call retried on the same worker: no crash to show.
            # History confirms (and retracts) at completion.
            return []
    return [
        recovery_frame(
            turn,
            key=f"model:{call.index}",
            kind="model_call",
            confirmed=False,
            detected_at=call.detected_at,
            lost_at=call.lost_at,
            lost_worker=call.lost_worker,
            resumed_on=call.resumed_on,
            kept=tel.live_kept(turn, call),
            repeated={"model_calls": attempt - 1, "tool_calls": 0, "resent_input_tokens_estimate": None},
            attempt=attempt,
            cause=cause,
            call_index=call.index,
            activity_id=call.activity_id,
        )
    ]


def _model_recovery_from_history(
    tel: Telemetry,
    turn: Turn,
    records: list[ActivityRecord],
    rec: ActivityRecord,
    index: int,
    usages: dict[int, dict[str, Any]],
    live: Call | None,
) -> tuple[str, dict[str, Any]] | None:
    if rec.attempt <= 1:
        if not (live and live.retries):
            return None
    lost_worker = (live.lost_worker if live else None) or tel.identities.get(
        (turn.workflow_id, rec.activity_id, rec.attempt - 1)
    )
    lost = rec.attempt > 1 and worker_lost(rec.cause, lost_worker, rec.identity)
    if not lost and not (live and live.retries):
        return None  # a retry for another reason, never shown as a recovery
    usage = usages.get(rec.scheduled_event_id) or {}
    resent = None
    if usage.get("input_tokens") is not None and rec.attempt > 1:
        resent = usage["input_tokens"] * (rec.attempt - 1)
    return recovery_frame(
        turn,
        key=f"model:{index}",
        kind="model_call",
        confirmed=True,
        retracted=not lost,
        detected_at=live.detected_at if live else None,
        lost_at=live.lost_at if live else None,
        lost_worker=lost_worker,
        resumed_on=rec.identity,
        kept=_kept_from_history(records, rec, usages),
        repeated={"model_calls": max(rec.attempt - 1, 0), "tool_calls": 0, "resent_input_tokens_estimate": resent},
        attempt=rec.attempt,
        cause=shown_cause(rec.cause, lost),
        call_index=index,
        activity_id=rec.activity_id,
    )


async def confirm_recovery(
    client: Client, tel: Telemetry, turn: Turn, call: Call, *, timeout: float = 15.0
) -> list[tuple[str, dict[str, Any]]]:
    """The retried call finished: wait for its completion in history, then confirm."""
    reader = HistoryReader(client)
    deadline = time.monotonic() + timeout
    while True:
        records = await reader.activities(turn.workflow_id, turn.started_ts)
        models = [r for r in records if r.kind == "model"]
        rec = None
        if call.activity_id:
            rec = next((r for r in models if r.activity_id == call.activity_id), None)
        if rec is None and call.index < len(models):
            rec = models[call.index]
        if rec is not None and rec.status == "completed":
            usages = await _usages(reader, records)
            frame = _model_recovery_from_history(tel, turn, records, rec, models.index(rec), usages, call)
            return [frame] if frame else []
        if time.monotonic() > deadline:
            return []
        await asyncio.sleep(0.5)


async def confirm_turn(client: Client, tel: Telemetry, turn: Turn) -> list[tuple[str, dict[str, Any]]]:
    """At the agent's ``turn_end``: confirmed ``x_usage`` for every model call in the turn and
    confirmed ``x_recovery`` for every activity that ran again after a worker loss, plus
    ``run_command`` calls that reattached to their running command."""
    reader = HistoryReader(client)
    records = await reader.activities(turn.workflow_id, turn.started_ts, turn.ended_ts)
    usages = await _usages(reader, records)
    frames: list[tuple[str, dict[str, Any]]] = []
    models = [r for r in records if r.kind == "model"]
    for i, rec in enumerate(models):
        live = turn.calls[i] if i < len(turn.calls) else None
        usage = usages.get(rec.scheduled_event_id)
        if usage is not None:
            frames.append(
                usage_frame(
                    turn,
                    call_index=i,
                    message_id=(live.message_id if live else None) or turn.message_id,
                    model=usage.get("model") or (live.model if live else None),
                    usage=usage,
                    confirmed=True,
                    attempt=rec.attempt,
                )
            )
        if rec.status == "completed":
            frame = _model_recovery_from_history(tel, turn, records, rec, i, usages, live)
            if frame:
                frames.append(frame)
    for rec in records:
        if rec.kind != "tool" or rec.status != "completed":
            continue
        reattached = False
        if rec.name in COMMAND_ACTIVITY_NAMES and rec.result is not None:
            try:
                result = await reader.decode(rec.result)
                reattached = isinstance(result, dict) and bool(result.get("reattached"))
            except Exception:  # noqa: BLE001
                reattached = False
        lost_worker = tel.identities.get((turn.workflow_id, rec.activity_id, rec.attempt - 1))
        lost = rec.attempt > 1 and worker_lost(rec.cause, lost_worker, rec.identity)
        if not (reattached or lost):
            continue
        kept = _kept_from_history(records, rec, usages)
        if reattached:
            kept["tool_calls"] += 1  # the command kept running and its work was kept
        frames.append(
            recovery_frame(
                turn,
                key=f"tool:{rec.activity_id}",
                kind="command_reattached" if reattached else "tool_call",
                confirmed=True,
                detected_at=None,
                lost_at=None,
                lost_worker=lost_worker,
                resumed_on=rec.identity,
                kept=kept,
                repeated={"model_calls": 0, "tool_calls": 0 if reattached else rec.attempt - 1,
                          "resent_input_tokens_estimate": None},
                attempt=rec.attempt,
                cause=shown_cause(rec.cause, lost),
                activity_id=rec.activity_id,
                tool_name=rec.name,
            )
        )
    return frames


async def run_job(client: Client, tel: Telemetry, job: Job) -> list[tuple[str, dict[str, Any]]]:
    if job.kind == "recovery_detected" and job.call is not None:
        return await detect_recovery(client, tel, job.turn, job.call)
    if job.kind == "recovery_confirm" and job.call is not None:
        return await confirm_recovery(client, tel, job.turn, job.call)
    if job.kind == "turn_confirm":
        return await confirm_turn(client, tel, job.turn)
    return []


# -- worker health ---------------------------------------------------------------------------


@dataclass
class Health:
    state: Literal["suspect", "ok"]
    reason: str
    pollers: int | None = None
    # Live tool recoveries seen while pending: (workflow_id, activity record-ish dict).
    tool_retries: list[dict[str, Any]] = field(default_factory=list)
    # Current run id of each workflow Described (root and open subagents): follows
    # continue-as-new, for Temporal UI deep links.
    runs: dict[str, str] = field(default_factory=dict)


async def check_health(client: Client, tel: Telemetry, *, now: float | None = None) -> Health:
    """Describe the session's open workflows (and their task queue's pollers) for signs that
    no worker is answering. Records the identity of each running attempt so a later recovery
    can name the worker that was lost."""
    now = time.time() if now is None else now
    reasons: list[str] = []
    tool_retries: list[dict[str, Any]] = []
    runs: dict[str, str] = {}
    task_queue = None
    for wf in tel.open_workflows():
        try:
            desc = await client.get_workflow_handle(wf).describe()
        except Exception as exc:  # noqa: BLE001
            log.info("telemetry: describe %s failed: %s", wf, exc)
            continue
        if getattr(desc, "run_id", None):
            runs[wf] = desc.run_id
        task_queue = task_queue or desc.task_queue
        raw = desc.raw_description
        for pa in raw.pending_activities:
            name = pa.activity_type.name
            started = pa.state == PendingActivityState.PENDING_ACTIVITY_STATE_STARTED
            if started and pa.last_worker_identity:
                tel.note_pending(wf, pa.activity_id, pa.attempt, pa.last_worker_identity)
            cause = failure_cause(pa.last_failure) if pa.HasField("last_failure") else None
            previous = tel.identities.get((wf, pa.activity_id, pa.attempt - 1))
            current = pa.last_worker_identity if started else None
            if pa.attempt > 1 and worker_lost(cause, previous, current):
                why = (cause or "a worker change").replace("_", " ")
                reasons.append(f"{name} is on attempt {pa.attempt} after {why}")
                if activity_kind(name) == "tool":
                    tool_retries.append({"workflow_id": wf, "activity_id": pa.activity_id, "name": name,
                                         "attempt": pa.attempt, "cause": cause,
                                         "resumed_on": pa.last_worker_identity if started else None})
            if started and pa.HasField("last_heartbeat_time"):
                silent = now - _ts(pa.last_heartbeat_time)
                if silent > HEARTBEAT_STALE_SECONDS:
                    who = pa.last_worker_identity or "its worker"
                    reasons.append(f"no heartbeat from {who} for {silent:.0f}s ({name})")
            elif pa.state == PendingActivityState.PENDING_ACTIVITY_STATE_SCHEDULED and pa.HasField("scheduled_time"):
                waiting = now - _ts(pa.scheduled_time)
                if waiting > UNCLAIMED_TASK_SECONDS:
                    reasons.append(f"{name} has waited {waiting:.0f}s for a worker")
        if raw.HasField("pending_workflow_task"):
            wt = raw.pending_workflow_task
            if wt.state == PendingWorkflowTaskState.PENDING_WORKFLOW_TASK_STATE_SCHEDULED:
                waiting = now - _ts(wt.scheduled_time)
                if waiting > UNCLAIMED_TASK_SECONDS:
                    reasons.append(f"a workflow task has waited {waiting:.0f}s for a worker")
    pollers = None
    if task_queue:
        try:
            resp = await client.workflow_service.describe_task_queue(
                DescribeTaskQueueRequest(
                    namespace=client.namespace,
                    task_queue=TaskQueue(name=task_queue),
                    task_queue_type=TaskQueueType.TASK_QUEUE_TYPE_WORKFLOW,
                )
            )
            pollers = sum(1 for p in resp.pollers if now - _ts(p.last_access_time) < POLLER_FRESH_SECONDS)
            if pollers == 0:
                reasons.append("no worker is polling the task queue")
        except Exception as exc:  # noqa: BLE001 - optional signal
            log.debug("telemetry: describe task queue failed: %s", exc)
    if reasons:
        return Health("suspect", "; ".join(reasons), pollers, tool_retries, runs)
    return Health("ok", "workers are answering", pollers, tool_retries, runs)


def worker_frame(
    state: str,
    reason: str,
    since: float,
    *,
    turn: Turn | None,
    pollers: int | None,
    root_agent_id: str | None = None,
    workflow_id: str | None = None,
    runs: dict[str, str] | None = None,
) -> tuple[str, dict[str, Any]]:
    """Worker health is the root workflow's: outside a turn it belongs to the root agent.
    ``run_id`` is the root workflow's current run; ``runs`` maps every workflow id seen
    (root and subagents) to its current run, for Temporal UI deep links."""
    runs = dict(runs or {})
    return X_WORKER, {
        "id": "worker",
        "workflow_id": workflow_id,
        "run_id": runs.get(workflow_id) if workflow_id else None,
        "runs": runs,
        "agent_id": turn.agent_id if turn else root_agent_id,
        "turn_id": turn.turn_id if turn else None,
        "turn_number": turn.turn_number if turn else None,
        "state": state,
        "reason": reason,
        "since": since,
        "pollers": pollers,
        "timestamp": time.time(),
    }


def live_tool_recovery_frame(tel: Telemetry, turn: Turn, retry: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """A tool activity seen pending on attempt > 1 after a worker loss (from Describe)."""
    return recovery_frame(
        turn,
        key=f"tool:{retry['activity_id']}",
        kind="tool_call",
        confirmed=False,
        detected_at=time.time(),
        lost_at=turn.last_frame_ts,
        lost_worker=tel.identities.get((retry["workflow_id"], retry["activity_id"], retry["attempt"] - 1)),
        resumed_on=retry.get("resumed_on"),
        kept={
            "model_calls": len([c for c in turn.calls if c.usage is not None]),
            "tool_calls": turn.tool_ends,
            "tokens_in": sum((c.usage or {}).get("input_tokens") or 0 for c in turn.calls),
            "tokens_out": sum((c.usage or {}).get("output_tokens") or 0 for c in turn.calls),
        },
        repeated={"model_calls": 0, "tool_calls": retry["attempt"] - 1, "resent_input_tokens_estimate": None},
        attempt=retry["attempt"],
        cause=retry["cause"],
        activity_id=retry["activity_id"],
        tool_name=retry["name"],
    )


def is_rejection(event: str, data: dict[str, Any]) -> bool:
    """True for the stream event the workflow publishes for a refused front-door Signal."""
    return event == "message_handler_error" and str(data.get("message_id") or "").startswith(
        REJECTION_MESSAGE_PREFIX
    )


def _epoch(ts: Any) -> float:
    if isinstance(ts, (int, float)):
        return float(ts)
    try:
        return datetime.fromisoformat(str(ts)).timestamp()
    except (TypeError, ValueError):
        return time.time()


def rejected_frame(data: dict[str, Any], *, root_agent_id: str | None = None) -> tuple[str, dict[str, Any]]:
    """The ``x_rejected`` frame for a rejection event (``is_rejection``). Its ``id`` is fixed
    by the request id, so a takeover that replays the event never stores it twice."""
    request_id = str(data["message_id"])[len(REJECTION_MESSAGE_PREFIX):]
    try:
        detail = json.loads(data.get("message") or "{}")
        if not isinstance(detail, dict):
            raise ValueError("not an object")
    except ValueError:
        detail = {"reason": data.get("message")}
    kind = detail.get("kind") if detail.get("kind") in ("message", "approval") else None
    return X_REJECTED, {
        "id": f"rejected:{request_id}",
        "agent_id": data.get("agent_id") or root_agent_id,
        "turn_id": None,
        "turn_number": None,
        "kind": kind,
        "request_id": request_id,
        "tool_id": detail.get("tool_id"),
        "message_id": detail.get("message_id"),
        "code": detail.get("code") or "rejected",
        "reason": detail.get("reason") or "The agent did not accept this request.",
        "error_type": detail.get("error_type"),
        "timestamp": _epoch(data.get("timestamp")),
    }
