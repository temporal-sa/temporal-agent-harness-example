"""Bridge telemetry (P2 step 2.2, PRD S3): usage normalization, failure causes, history
parsing, live recovery detection, history confirmation and worker-health hints.

No services: history comes from a recorded fixture or is built in memory, and Temporal is a
fake.
"""

from __future__ import annotations

import json
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock

from temporalio.api.enums.v1 import PendingActivityState, PendingWorkflowTaskState, TimeoutType
from temporalio.api.failure.v1 import ApplicationFailureInfo, Failure, TimeoutFailureInfo
from temporalio.api.taskqueue.v1 import PollerInfo
from temporalio.api.workflow.v1 import PendingActivityInfo, PendingWorkflowTaskInfo
from temporalio.api.workflowservice.v1 import DescribeTaskQueueResponse, DescribeWorkflowExecutionResponse
from temporalio.client import WorkflowHistory
from temporalio.converter import DataConverter

from simple_chat_agent.api import agent_telemetry as t

FIXTURES = Path(__file__).parent / "fixtures" / "histories"

ANTHROPIC_RESPONSE = {
    "model_name": "claude-haiku-5-5",
    "provider_name": "anthropic",
    "usage": {
        "input_tokens": 4474, "cache_write_tokens": 4470, "cache_read_tokens": 0, "output_tokens": 52,
        "details": {"input_tokens": 4, "output_tokens": 52, "cache_creation_input_tokens": 4470,
                    "cache_read_input_tokens": 0},
    },
}


def heartbeat_failure() -> Failure:
    return Failure(message="activity Heartbeat timeout",
                   timeout_failure_info=TimeoutFailureInfo(timeout_type=TimeoutType.TIMEOUT_TYPE_HEARTBEAT))


def ts_pb(seconds: float):
    from google.protobuf.timestamp_pb2 import Timestamp

    out = Timestamp()
    out.FromNanoseconds(int(seconds * 1e9))
    return out


class UsageTests(unittest.TestCase):
    def test_anthropic_reports_cache_writes_and_no_thinking_split(self) -> None:
        u = t.usage_from_response(ANTHROPIC_RESPONSE)
        self.assertEqual(u["input_tokens"], 4474)
        self.assertEqual(u["cache_write_tokens"], 4470)
        self.assertEqual(u["cache_read_tokens"], 0)  # reported, and really zero
        self.assertIsNone(u["thinking_tokens"])

    def test_openai_has_reasoning_but_no_cache_writes(self) -> None:
        u = t.usage_from_response({
            "provider_name": "openai", "model_name": "gpt-x",
            "usage": {"input_tokens": 100, "output_tokens": 20, "cache_read_tokens": 64,
                      "cache_write_tokens": 0, "details": {"reasoning_tokens": 12}},
        })
        self.assertIsNone(u["cache_write_tokens"])
        self.assertEqual(u["cache_read_tokens"], 64)
        self.assertEqual(u["thinking_tokens"], 12)

    def test_google_thoughts(self) -> None:
        u = t.usage_from_response({"provider_name": "google-gla", "usage": {"input_tokens": 9, "output_tokens": 3,
                                                                           "details": {"thoughts_tokens": 40}}})
        self.assertEqual(u["thinking_tokens"], 40)
        self.assertIsNone(u["cache_write_tokens"])
        u = t.usage_from_response({"provider_name": "google-vertex", "usage": {"input_tokens": 9}})
        self.assertEqual(u["thinking_tokens"], 0)

    def test_unknown_provider_reports_null_not_zero(self) -> None:
        u = t.usage_from_response({"provider_name": "acme", "usage": {"input_tokens": 1, "output_tokens": 1}})
        self.assertIsNone(u["cache_read_tokens"])
        self.assertIsNone(u["cache_write_tokens"])
        self.assertIsNone(u["thinking_tokens"])

    def test_live_usage_has_no_cache_writes(self) -> None:
        u = t.usage_from_live({"input_tokens": 5, "output_tokens": 2, "cached_tokens": None, "thought_tokens": None})
        self.assertEqual(u, {"input_tokens": 5, "output_tokens": 2, "cache_read_tokens": None,
                             "cache_write_tokens": None, "thinking_tokens": None})
        self.assertIsNone(t.usage_from_live(None))


class FailureTests(unittest.TestCase):
    def test_causes(self) -> None:
        self.assertEqual(t.failure_cause(heartbeat_failure()), "heartbeat_timeout")
        self.assertEqual(
            t.failure_cause(Failure(timeout_failure_info=TimeoutFailureInfo(
                timeout_type=TimeoutType.TIMEOUT_TYPE_START_TO_CLOSE))), "start_to_close_timeout")
        self.assertEqual(
            t.failure_cause(Failure(application_failure_info=ApplicationFailureInfo(type="RateLimited"))),
            "application_error:RateLimited")
        nested = Failure(message="outer", cause=heartbeat_failure())
        self.assertEqual(t.failure_cause(nested), "heartbeat_timeout")
        self.assertIsNone(t.failure_cause(None))
        self.assertIsNone(t.failure_cause(Failure()))

    def test_activity_kinds(self) -> None:
        self.assertEqual(t.activity_kind("agent__coding-agent__model_request_stream"), "model")
        self.assertEqual(t.activity_kind("agent__coding-agent__event_stream_handler"), "internal")
        self.assertEqual(t.activity_kind("judge_tool_call"), "internal")
        self.assertEqual(t.activity_kind("workspace.run_command"), "tool")


class HistoryParsingTests(unittest.IsolatedAsyncioTestCase):
    async def test_fixture_history_model_call_and_usage(self) -> None:
        history = WorkflowHistory.from_json(
            "x", json.loads((FIXTURES / "interrupt_during_tool.json").read_text())
        )
        records = t.parse_activities(list(history.events))
        models = [r for r in records if r.kind == "model"]
        self.assertEqual(len(models), 1)
        self.assertEqual(models[0].status, "completed")
        self.assertEqual(models[0].attempt, 1)
        self.assertTrue(models[0].identity)
        self.assertFalse(models[0].retried_after_worker_loss)
        response = (await DataConverter.default.decode([models[0].result]))[0]
        self.assertEqual(t.usage_from_response(response)["cache_write_tokens"], 4470)
        tools = [r for r in records if r.kind == "tool"]
        self.assertEqual([r.name for r in tools], ["wait_seconds"])
        # The window keeps activities scheduled between the turn's brackets.
        at = models[0].scheduled_at
        self.assertEqual(t.in_window(records, at - 1, at + 0.0001), [models[0]])
        self.assertEqual(t.in_window(records, at + 0.01, None), tools)


def ev(kind: str, *, agent: str = "root", turn: str = "t1", ts: float = 0.0, **fields: Any) -> tuple[str, dict]:
    return kind, {"agent_id": agent, "turn_id": turn, "turn_number": 1, "message_id": "m1", "timestamp": ts, **fields}


class LiveTelemetryTests(unittest.TestCase):
    def feed(self, tel: t.Telemetry, frames: list[tuple[str, dict]], root: bool = True) -> t.Observation:
        out = t.Observation()
        for pos, (kind, data) in enumerate(frames, start=1):
            obs = tel.observe(kind, data, pos, is_root=root)
            out.frames += obs.frames
            out.jobs += obs.jobs
        return out

    def test_second_started_for_an_unfinished_call_is_a_recovery(self) -> None:
        tel = t.Telemetry("s")
        usage = {"input_tokens": 100, "output_tokens": 10, "cached_tokens": 80, "thought_tokens": None}
        obs = self.feed(tel, [
            ev("turn_started", ts=1.0),
            ev("model_interaction_started", ts=1.1, model="claude"),
            ev("model_interaction_ended", ts=1.5, model="claude", usage=usage),
            ev("tool_end", ts=1.6, tool_id="x", tool_name="wait_seconds", tool_output="ok"),
            ev("model_interaction_started", ts=2.0, model="claude"),
            ev("reply_delta", ts=2.1, text="a" * 400),
            # worker dies; attempt 2 starts the same call again
            ev("model_interaction_started", ts=14.0, model="claude"),
            ev("reply_delta", ts=14.1, text="b" * 40),
            ev("model_interaction_ended", ts=15.0, model="claude", usage=usage),
            ev("turn_end", ts=15.1),
        ])
        turn = tel.turns[("root", "t1")]
        self.assertEqual([c.index for c in turn.calls], [0, 1])  # the retry did not open a call
        self.assertEqual(turn.calls[1].retries, 1)
        self.assertEqual(turn.calls[1].failed_chars, 400)
        self.assertEqual(turn.calls[1].lost_at, 2.1)
        self.assertEqual([j.kind for j in obs.jobs], ["recovery_detected", "recovery_confirm", "turn_confirm"])
        self.assertEqual(tel.live_kept(turn, turn.calls[1]),
                         {"model_calls": 1, "tool_calls": 1, "tokens_in": 100, "tokens_out": 10})
        usages = [d for e, d in obs.frames if e == t.X_USAGE]
        self.assertEqual([d["call_index"] for d in usages], [0, 1])
        self.assertEqual(usages[0]["cache_read_tokens"], 80)
        self.assertIsNone(usages[0]["cache_write_tokens"])
        self.assertFalse(usages[0]["confirmed"])
        self.assertEqual(usages[0]["id"], "usage:root:t1:0")

    def test_failed_stream_then_new_start_is_the_same_call(self) -> None:
        tel = t.Telemetry("s")
        obs = self.feed(tel, [
            ev("turn_started"), ev("model_interaction_started"),
            ev("model_interaction_ended", usage=None), ev("model_interaction_started"),
        ])
        self.assertEqual(len(tel.turns[("root", "t1")].calls), 1)
        self.assertEqual([j.kind for j in obs.jobs], ["recovery_detected"])

    def test_subagent_turns_are_tracked_against_the_child_workflow(self) -> None:
        tel = t.Telemetry("root-wf")
        tel.observe(*ev("subagent_started", subagent_id="root.s1", agent_key="k", workflow_id="child-wf"), 1, is_root=True)
        tel.observe(*ev("turn_started", agent="root.s1", turn="ct"), 1, is_root=False)
        self.assertEqual(tel.turns[("root.s1", "ct")].workflow_id, "child-wf")
        self.assertEqual(tel.open_workflows(), ["root-wf", "child-wf"])
        obs = tel.observe(*ev("turn_end", agent="root.s1", turn="ct"), 1, is_root=False)
        self.assertEqual(obs.jobs[0].turn.workflow_id, "child-wf")
        self.assertEqual(tel.open_workflows(), ["root-wf"])

    def test_telemetry_frames_are_not_observed(self) -> None:
        tel = t.Telemetry("s")
        self.assertEqual(tel.observe("x_usage", {"agent_id": "a", "turn_id": "t"}, 1, is_root=False).jobs, [])
        self.assertEqual(tel.turns, {})


def record(sid: int, name: str, *, attempt: int = 1, closed: int | None = None, failure: Failure | None = None,
           result: Any = None, identity: str = "w-b", activity_id: str | None = None) -> t.ActivityRecord:
    return t.ActivityRecord(
        activity_id=activity_id or str(sid), name=name, kind=t.activity_kind(name), scheduled_event_id=sid,
        scheduled_at=float(sid), run_id="r", attempt=attempt, identity=identity, last_failure=failure,
        closed_event_id=closed, closed_at=float(closed) if closed else None,
        status="completed" if closed else "started", result=result,
    )


MODEL = "agent__coding-agent__model_request_stream"


def response(inp: int, out: int, write: int = 0, read: int = 0) -> dict:
    return {"model_name": "claude-haiku-5-5", "provider_name": "anthropic",
            "usage": {"input_tokens": inp, "output_tokens": out, "cache_write_tokens": write,
                      "cache_read_tokens": read, "details": {}}}


class FakeReader:
    def __init__(self, records: list[t.ActivityRecord]) -> None:
        self.records = records

    async def activities(self, *_a: Any, **_k: Any) -> list[t.ActivityRecord]:
        return self.records

    async def decode(self, payload: Any) -> Any:
        return payload  # records here carry plain dicts


class ConfirmationTests(unittest.IsolatedAsyncioTestCase):
    def turn(self, tel: t.Telemetry) -> t.Turn:
        turn = t.Turn(agent_id="root", workflow_id="s", turn_id="t1", turn_number=1, started_ts=0.0, ended_ts=99.0,
                      message_id="m1", is_root=True)
        tel.turns[turn.key] = turn
        return turn

    async def test_turn_confirmation_usage_recovery_and_reattach(self) -> None:
        tel = t.Telemetry("s")
        turn = self.turn(tel)
        live = t.Call(index=1, message_id="m1", model="claude", started_pos=5, retries=1, failed_chars=400,
                      detected_at=14.0, lost_at=2.1)
        turn.calls = [t.Call(index=0, message_id="m1", model="claude", started_pos=2), live]
        tel.note_pending("s", "6", 1, "w-a")
        records = [
            record(1, MODEL, closed=3, result=response(1000, 50, write=900)),
            record(4, "wait_seconds", closed=5, result="Waited 1 seconds."),
            record(6, MODEL, attempt=2, closed=9, failure=heartbeat_failure(), result=response(1100, 700, read=900)),
            record(10, "workspace.run_command", attempt=2, closed=12, failure=heartbeat_failure(),
                   result={"reattached": True, "exit_code": 0}),
            record(13, "judge_tool_call", closed=14, result={}),
        ]
        with mock.patch.object(t, "HistoryReader", lambda client: FakeReader(records)):
            frames = await t.confirm_turn(None, tel, turn)  # type: ignore[arg-type]
        usage = [d for e, d in frames if e == t.X_USAGE]
        self.assertEqual([(d["call_index"], d["cache_write_tokens"], d["confirmed"]) for d in usage],
                         [(0, 900, True), (1, 0, True)])
        self.assertEqual(sum(d["input_tokens"] for d in usage), 2100)
        rec = {d["kind"]: d for e, d in frames if e == t.X_RECOVERY}
        model = rec["model_call"]
        self.assertEqual(model["kept"], {"model_calls": 1, "tool_calls": 1, "tokens_in": 1000, "tokens_out": 50})
        self.assertEqual(model["repeated"], {"model_calls": 1, "tool_calls": 0, "tokens_estimate": 1100 + 100})
        self.assertEqual((model["lost_worker"], model["resumed_on"], model["cause"]), ("w-a", "w-b", "heartbeat_timeout"))
        self.assertTrue(model["confirmed"])
        self.assertFalse(model["retracted"])
        self.assertEqual(model["id"], "recovery:root:t1:model:1")
        cmd = rec["command_reattached"]
        self.assertEqual(cmd["kept"]["model_calls"], 2)
        self.assertEqual(cmd["kept"]["tool_calls"], 2)  # wait_seconds plus the reattached command
        self.assertEqual(cmd["repeated"]["tool_calls"], 0)

    async def test_a_retry_that_was_not_a_worker_loss_is_retracted(self) -> None:
        tel = t.Telemetry("s")
        turn = self.turn(tel)
        turn.calls = [t.Call(index=0, message_id="m1", model="c", started_pos=1, retries=1)]
        rate_limited = Failure(application_failure_info=ApplicationFailureInfo(type="RateLimited"))
        records = [record(1, MODEL, attempt=2, closed=3, failure=rate_limited, result=response(10, 1))]
        with mock.patch.object(t, "HistoryReader", lambda client: FakeReader(records)):
            frames = await t.confirm_turn(None, tel, turn)  # type: ignore[arg-type]
        recovery = [d for e, d in frames if e == t.X_RECOVERY]
        self.assertEqual(len(recovery), 1)
        self.assertTrue(recovery[0]["retracted"])

    async def test_history_only_recovery_without_live_detection(self) -> None:
        tel = t.Telemetry("s")
        turn = self.turn(tel)
        records = [record(1, MODEL, attempt=3, closed=3, failure=heartbeat_failure(), result=response(10, 1))]
        with mock.patch.object(t, "HistoryReader", lambda client: FakeReader(records)):
            frames = await t.confirm_turn(None, tel, turn)  # type: ignore[arg-type]
        recovery = [d for e, d in frames if e == t.X_RECOVERY][0]
        self.assertEqual(recovery["repeated"]["model_calls"], 2)
        self.assertIsNone(recovery["detected_at"])

    async def test_confirm_recovery_waits_for_completion(self) -> None:
        tel = t.Telemetry("s")
        turn = self.turn(tel)
        call = t.Call(index=0, message_id="m1", model="c", started_pos=1, retries=1, activity_id="1")
        turn.calls = [call]
        pending = [record(1, MODEL, attempt=2, failure=heartbeat_failure())]
        done = [record(1, MODEL, attempt=2, closed=3, failure=heartbeat_failure(), result=response(10, 1))]
        readers = iter([FakeReader(pending), FakeReader(done)])
        state = {"reader": next(readers)}

        class Switching(FakeReader):
            async def activities(self, *_a: Any, **_k: Any) -> list[t.ActivityRecord]:
                current = state["reader"].records
                state["reader"] = next(readers, state["reader"])
                return current

        with mock.patch.object(t, "HistoryReader", lambda client: Switching([])), \
                mock.patch.object(t.asyncio, "sleep", mock.AsyncMock()):
            frames = await t.confirm_recovery(None, tel, turn, call)  # type: ignore[arg-type]
        self.assertEqual(len(frames), 1)
        self.assertTrue(frames[0][1]["confirmed"])


class FakeService:
    def __init__(self, pollers: list[PollerInfo]) -> None:
        self.pollers = pollers

    async def describe_task_queue(self, _req: Any) -> DescribeTaskQueueResponse:
        return DescribeTaskQueueResponse(pollers=self.pollers)


class FakeClient:
    namespace = "default"

    def __init__(self, raw: DescribeWorkflowExecutionResponse, pollers: list[PollerInfo]) -> None:
        self.raw = raw
        self.workflow_service = FakeService(pollers)

    def get_workflow_handle(self, _wid: str) -> Any:
        raw = self.raw

        class H:
            async def describe(self) -> Any:
                return SimpleNamespace(task_queue="tq", raw_description=raw)

        return H()


class HealthTests(unittest.IsolatedAsyncioTestCase):
    async def test_stale_heartbeat_is_suspect_and_records_the_identity(self) -> None:
        now = time.time()
        raw = DescribeWorkflowExecutionResponse(pending_activities=[PendingActivityInfo(
            activity_id="7", activity_type={"name": MODEL}, attempt=1, last_worker_identity="w-a",
            state=PendingActivityState.PENDING_ACTIVITY_STATE_STARTED, last_heartbeat_time=ts_pb(now - 9))])
        tel = t.Telemetry("s")
        health = await t.check_health(FakeClient(raw, [PollerInfo(last_access_time=ts_pb(now), identity="w-b")]), tel, now=now)  # type: ignore[arg-type]
        self.assertEqual(health.state, "suspect")
        self.assertIn("no heartbeat from w-a", health.reason)
        self.assertEqual(health.pollers, 1)
        self.assertEqual(tel.identities[("s", "7", 1)], "w-a")

    async def test_unclaimed_workflow_task_and_no_pollers(self) -> None:
        now = time.time()
        raw = DescribeWorkflowExecutionResponse(pending_workflow_task=PendingWorkflowTaskInfo(
            state=PendingWorkflowTaskState.PENDING_WORKFLOW_TASK_STATE_SCHEDULED, scheduled_time=ts_pb(now - 30)))
        health = await t.check_health(FakeClient(raw, []), t.Telemetry("s"), now=now)  # type: ignore[arg-type]
        self.assertEqual(health.state, "suspect")
        self.assertIn("workflow task", health.reason)
        self.assertIn("no worker is polling", health.reason)

    async def test_tool_retry_after_worker_loss_is_reported(self) -> None:
        now = time.time()
        raw = DescribeWorkflowExecutionResponse(pending_activities=[PendingActivityInfo(
            activity_id="9", activity_type={"name": "workspace.run_command"}, attempt=2,
            last_failure=heartbeat_failure(), last_worker_identity="w-b",
            state=PendingActivityState.PENDING_ACTIVITY_STATE_STARTED, last_heartbeat_time=ts_pb(now))])
        tel = t.Telemetry("s")
        tel.note_pending("s", "9", 1, "w-a")
        health = await t.check_health(FakeClient(raw, [PollerInfo(last_access_time=ts_pb(now))]), tel, now=now)  # type: ignore[arg-type]
        self.assertEqual(len(health.tool_retries), 1)
        turn = t.Turn(agent_id="root", workflow_id="s", turn_id="t1", turn_number=1)
        _, frame = t.live_tool_recovery_frame(tel, turn, health.tool_retries[0])
        self.assertEqual((frame["kind"], frame["lost_worker"], frame["resumed_on"]), ("tool_call", "w-a", "w-b"))
        self.assertFalse(frame["confirmed"])

    async def test_waiting_for_approval_is_ok(self) -> None:
        now = time.time()
        health = await t.check_health(
            FakeClient(DescribeWorkflowExecutionResponse(), [PollerInfo(last_access_time=ts_pb(now))]),  # type: ignore[arg-type]
            t.Telemetry("s"), now=now)
        self.assertEqual(health.state, "ok")


if __name__ == "__main__":
    unittest.main()
