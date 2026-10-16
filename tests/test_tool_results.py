from __future__ import annotations

import unittest
from collections.abc import AsyncIterator

from temporalio.api.history.v1 import HistoryEvent
from temporalio.converter import DataConverter

from agent_harness.tools import RUN_TOOL_ACTIVITY_NAME, ToolActivityRequest
from simple_chat_agent.api.tool_results import tool_result_from_events


class ToolResultHistoryTests(unittest.IsolatedAsyncioTestCase):
    async def test_completed_activity_result_is_decoded_by_tool_call_id(self) -> None:
        converter = DataConverter.default
        request_payloads = await converter.encode(
            [
                ToolActivityRequest(
                    function_ref="example:search",
                    args={"query": "Temporal"},
                    tool_name="search_web",
                    step="searxng",
                    tool_call_id="tool-1",
                )
            ]
        )
        result_payloads = await converter.encode(
            [{"query": "Temporal", "results": [{"title": "Temporal Docs"}]}]
        )
        events = [
            _started_event(),
            _scheduled_activity_event(2, request_payloads),
            _completed_activity_event(3, 2, result_payloads),
        ]

        result, previous_run_id = await tool_result_from_events(
            _events(events),
            data_converter=converter,
            workflow_id="chat-1",
            tool_call_id="tool-1",
        )

        self.assertIsNone(previous_run_id)
        self.assertEqual(result["status"], "complete")
        self.assertEqual(
            result["result"],
            {"query": "Temporal", "results": [{"title": "Temporal Docs"}]},
        )
        self.assertEqual(result["activities"][0]["step"], "searxng")

    async def test_error_result_is_reported_as_failed(self) -> None:
        converter = DataConverter.default
        request_payloads = await converter.encode(
            [
                ToolActivityRequest(
                    function_ref="example:fetch",
                    args={"url": "https://example.test"},
                    tool_name="fetch_url",
                    tool_call_id="tool-2",
                )
            ]
        )
        result_payloads = await converter.encode(
            [{"status": 403, "error": "HTTP 403: Forbidden"}]
        )

        result, _ = await tool_result_from_events(
            _events(
                [
                    _started_event(),
                    _scheduled_activity_event(2, request_payloads),
                    _completed_activity_event(3, 2, result_payloads),
                ]
            ),
            data_converter=converter,
            workflow_id="chat-1",
            tool_call_id="tool-2",
        )

        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["error"], "HTTP 403: Forbidden")
        self.assertEqual(result["result"]["status"], 403)

    async def test_previous_run_id_is_returned_when_tool_is_not_in_run(self) -> None:
        started = _started_event(previous_run_id="previous-run")

        result, previous_run_id = await tool_result_from_events(
            _events([started]),
            data_converter=DataConverter.default,
            workflow_id="chat-1",
            tool_call_id="missing-tool",
        )

        self.assertIsNone(result)
        self.assertEqual(previous_run_id, "previous-run")

    async def test_child_workflow_result_resolves_create_subagent(self) -> None:
        converter = DataConverter.default
        request_payloads = await converter.encode(
            [{"parent_tool_call_id": "subagent-tool-1", "task": "Research"}]
        )
        result_payloads = await converter.encode(
            [{"text": "Findings", "turns": 2, "stop_reason": "end_turn"}]
        )
        initiated = HistoryEvent(event_id=2)
        initiated_attributes = (
            initiated.start_child_workflow_execution_initiated_event_attributes
        )
        initiated_attributes.workflow_id = "chat-1-subagent-child"
        initiated_attributes.workflow_type.name = "SubagentWorkflow"
        initiated_attributes.input.payloads.extend(request_payloads)
        completed = HistoryEvent(event_id=3)
        completed_attributes = completed.child_workflow_execution_completed_event_attributes
        completed_attributes.initiated_event_id = 2
        completed_attributes.result.payloads.extend(result_payloads)

        result, _ = await tool_result_from_events(
            _events([_started_event(), initiated, completed]),
            data_converter=converter,
            workflow_id="chat-1",
            tool_call_id="subagent-tool-1",
        )

        self.assertEqual(result["status"], "complete")
        self.assertEqual(result["result"]["text"], "Findings")
        self.assertEqual(result["activities"][0]["workflow_type"], "SubagentWorkflow")


def _started_event(previous_run_id: str = "") -> HistoryEvent:
    event = HistoryEvent(event_id=1)
    event.workflow_execution_started_event_attributes.continued_execution_run_id = (
        previous_run_id
    )
    return event


def _scheduled_activity_event(event_id: int, payloads: list) -> HistoryEvent:
    event = HistoryEvent(event_id=event_id)
    attributes = event.activity_task_scheduled_event_attributes
    attributes.activity_id = "activity-1"
    attributes.activity_type.name = RUN_TOOL_ACTIVITY_NAME
    attributes.input.payloads.extend(payloads)
    return event


def _completed_activity_event(
    event_id: int,
    scheduled_event_id: int,
    payloads: list,
) -> HistoryEvent:
    event = HistoryEvent(event_id=event_id)
    attributes = event.activity_task_completed_event_attributes
    attributes.scheduled_event_id = scheduled_event_id
    attributes.result.payloads.extend(payloads)
    return event


async def _events(events: list[HistoryEvent]) -> AsyncIterator[HistoryEvent]:
    for event in events:
        yield event
