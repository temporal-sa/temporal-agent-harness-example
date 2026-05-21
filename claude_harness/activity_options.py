from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from temporalio.common import Priority, RetryPolicy
from temporalio.workflow import ActivityCancellationType, VersioningIntent


@dataclass(frozen=True)
class ActivityOptions:
    task_queue: str | None = None
    schedule_to_close_timeout: timedelta | None = None
    schedule_to_start_timeout: timedelta | None = None
    start_to_close_timeout: timedelta | None = None
    heartbeat_timeout: timedelta | None = None
    retry_policy: RetryPolicy | None = None
    cancellation_type: ActivityCancellationType | None = None
    versioning_intent: VersioningIntent | None = None
    priority: Priority | None = None

    def with_overrides(
        self,
        *,
        task_queue: str | None = None,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        retry_policy: RetryPolicy | None = None,
        cancellation_type: ActivityCancellationType | None = None,
        versioning_intent: VersioningIntent | None = None,
        priority: Priority | None = None,
    ) -> "ActivityOptions":
        return ActivityOptions(
            task_queue=self.task_queue if task_queue is None else task_queue,
            schedule_to_close_timeout=(
                self.schedule_to_close_timeout
                if schedule_to_close_timeout is None
                else schedule_to_close_timeout
            ),
            schedule_to_start_timeout=(
                self.schedule_to_start_timeout
                if schedule_to_start_timeout is None
                else schedule_to_start_timeout
            ),
            start_to_close_timeout=(
                self.start_to_close_timeout
                if start_to_close_timeout is None
                else start_to_close_timeout
            ),
            heartbeat_timeout=(
                self.heartbeat_timeout
                if heartbeat_timeout is None
                else heartbeat_timeout
            ),
            retry_policy=self.retry_policy if retry_policy is None else retry_policy,
            cancellation_type=(
                self.cancellation_type
                if cancellation_type is None
                else cancellation_type
            ),
            versioning_intent=(
                self.versioning_intent
                if versioning_intent is None
                else versioning_intent
            ),
            priority=self.priority if priority is None else priority,
        )

    def to_execute_activity_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {}
        for name in (
            "task_queue",
            "schedule_to_close_timeout",
            "schedule_to_start_timeout",
            "start_to_close_timeout",
            "heartbeat_timeout",
            "retry_policy",
            "cancellation_type",
            "versioning_intent",
            "priority",
        ):
            value = getattr(self, name)
            if value is not None:
                kwargs[name] = value
        return kwargs


DEFAULT_ACTIVITY_OPTIONS = ActivityOptions(
    start_to_close_timeout=timedelta(minutes=5)
)


def activity_options_with_overrides(
    defaults: ActivityOptions,
    *,
    activity_options: ActivityOptions | None = None,
    task_queue: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: RetryPolicy | None = None,
    cancellation_type: ActivityCancellationType | None = None,
    versioning_intent: VersioningIntent | None = None,
    priority: Priority | None = None,
) -> ActivityOptions:
    options = defaults
    if activity_options is not None:
        options = options.with_overrides(**activity_options.to_execute_activity_kwargs())

    return options.with_overrides(
        task_queue=task_queue,
        schedule_to_close_timeout=schedule_to_close_timeout,
        schedule_to_start_timeout=schedule_to_start_timeout,
        start_to_close_timeout=start_to_close_timeout,
        heartbeat_timeout=heartbeat_timeout,
        retry_policy=retry_policy,
        cancellation_type=cancellation_type,
        versioning_intent=versioning_intent,
        priority=priority,
    )
