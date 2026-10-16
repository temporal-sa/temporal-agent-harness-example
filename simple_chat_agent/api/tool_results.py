from __future__ import annotations

from dataclasses import asdict, is_dataclass
from typing import Any, AsyncIterator

from temporalio.api.history.v1 import HistoryEvent
from temporalio.client import Client
from temporalio.converter import DataConverter

from agent_harness.tools import RUN_TOOL_ACTIVITY_NAME


MAX_HISTORY_RUNS = 20


async def tool_result_from_history(
    client: Client,
    *,
    workflow_id: str,
    tool_call_id: str,
) -> dict[str, Any] | None:
    """Resolve a tool's durable output without copying it into the SSE stream."""

    run_id: str | None = None
    for _ in range(MAX_HISTORY_RUNS):
        handle = client.get_workflow_handle(workflow_id, run_id=run_id)
        result, previous_run_id = await tool_result_from_events(
            handle.fetch_history_events(),
            data_converter=client.data_converter,
            workflow_id=workflow_id,
            tool_call_id=tool_call_id,
        )
        if result is not None:
            return result
        if not previous_run_id:
            return None
        run_id = previous_run_id
    return None


async def tool_result_from_events(
    events: AsyncIterator[HistoryEvent],
    *,
    data_converter: DataConverter,
    workflow_id: str,
    tool_call_id: str,
) -> tuple[dict[str, Any] | None, str | None]:
    matching_activities: dict[int, dict[str, Any]] = {}
    matching_children: dict[int, dict[str, Any]] = {}
    outcomes: list[dict[str, Any]] = []
    previous_run_id: str | None = None

    async for event in events:
        if event.HasField("workflow_execution_started_event_attributes"):
            started = event.workflow_execution_started_event_attributes
            previous_run_id = started.continued_execution_run_id or None
            continue

        if event.HasField("activity_task_scheduled_event_attributes"):
            scheduled = event.activity_task_scheduled_event_attributes
            if scheduled.activity_type.name != RUN_TOOL_ACTIVITY_NAME:
                continue
            request = await _decode_single(data_converter, scheduled.input.payloads)
            if _field(request, "tool_call_id") != tool_call_id:
                continue
            matching_activities[event.event_id] = {
                "activity_id": scheduled.activity_id,
                "activity_type": scheduled.activity_type.name,
                "function": _field(request, "function_ref"),
                "step": _field(request, "step"),
                "tool_name": _field(request, "tool_name"),
            }
            continue

        if event.HasField("start_child_workflow_execution_initiated_event_attributes"):
            initiated = event.start_child_workflow_execution_initiated_event_attributes
            request = await _decode_single(data_converter, initiated.input.payloads)
            if _field(request, "parent_tool_call_id") != tool_call_id:
                continue
            matching_children[event.event_id] = {
                "workflow_id": initiated.workflow_id,
                "workflow_type": initiated.workflow_type.name,
                "tool_name": "create_subagent",
            }
            continue

        if event.HasField("activity_task_completed_event_attributes"):
            completed = event.activity_task_completed_event_attributes
            metadata = matching_activities.get(completed.scheduled_event_id)
            if metadata is None:
                continue
            result = await _decode_result(data_converter, completed.result.payloads)
            outcomes.append(
                {
                    **metadata,
                    "status": "failed" if _result_is_error(result) else "complete",
                    "result": result,
                }
            )
            continue

        if event.HasField("activity_task_failed_event_attributes"):
            failed = event.activity_task_failed_event_attributes
            metadata = matching_activities.get(failed.scheduled_event_id)
            if metadata is None:
                continue
            outcomes.append(
                {
                    **metadata,
                    "status": "failed",
                    "error": await _decode_failure(data_converter, failed.failure),
                }
            )
            continue

        if event.HasField("activity_task_timed_out_event_attributes"):
            timed_out = event.activity_task_timed_out_event_attributes
            metadata = matching_activities.get(timed_out.scheduled_event_id)
            if metadata is not None:
                outcomes.append(
                    {
                        **metadata,
                        "status": "failed",
                        "error": {
                            "type": "ActivityTimeout",
                            "message": "The tool activity timed out.",
                        },
                    }
                )
            continue

        if event.HasField("activity_task_canceled_event_attributes"):
            canceled = event.activity_task_canceled_event_attributes
            metadata = matching_activities.get(canceled.scheduled_event_id)
            if metadata is not None:
                outcomes.append(
                    {
                        **metadata,
                        "status": "failed",
                        "error": {
                            "type": "ActivityCancelled",
                            "message": "The tool activity was cancelled.",
                        },
                    }
                )
            continue

        if event.HasField("child_workflow_execution_completed_event_attributes"):
            completed = event.child_workflow_execution_completed_event_attributes
            metadata = matching_children.get(completed.initiated_event_id)
            if metadata is None:
                continue
            outcomes.append(
                {
                    **metadata,
                    "status": "complete",
                    "result": await _decode_result(
                        data_converter,
                        completed.result.payloads,
                    ),
                }
            )
            continue

        if event.HasField("child_workflow_execution_failed_event_attributes"):
            failed = event.child_workflow_execution_failed_event_attributes
            metadata = matching_children.get(failed.initiated_event_id)
            if metadata is None:
                continue
            outcomes.append(
                {
                    **metadata,
                    "status": "failed",
                    "error": await _decode_failure(data_converter, failed.failure),
                }
            )

    if not outcomes:
        return None, previous_run_id

    status = "failed" if any(item["status"] == "failed" for item in outcomes) else "complete"
    primary = outcomes[-1]
    response: dict[str, Any] = {
        "workflow_id": workflow_id,
        "tool_call_id": tool_call_id,
        "source": "temporal_history",
        "status": status,
        "activities": outcomes,
    }
    if "result" in primary:
        response["result"] = primary["result"]
        if _result_is_error(primary["result"]):
            response["error"] = primary["result"]["error"]
    if "error" in primary:
        response["error"] = primary["error"]
    return response, previous_run_id


async def _decode_single(data_converter: DataConverter, payloads: Any) -> Any:
    values = await data_converter.decode(list(payloads))
    return values[0] if values else None


async def _decode_result(data_converter: DataConverter, payloads: Any) -> Any:
    values = await data_converter.decode(list(payloads))
    if not values:
        return None
    if len(values) == 1:
        return _json_value(values[0])
    return [_json_value(value) for value in values]


async def _decode_failure(data_converter: DataConverter, failure: Any) -> dict[str, Any]:
    error = await data_converter.decode_failure(failure)
    error_type = getattr(error, "type", None) or type(error).__name__
    return {
        "type": str(error_type),
        "message": str(error),
    }


def _field(value: Any, name: str) -> Any:
    if isinstance(value, dict):
        return value.get(name)
    return getattr(value, name, None)


def _result_is_error(result: Any) -> bool:
    return isinstance(result, dict) and "error" in result


def _json_value(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return asdict(value)
    return value
