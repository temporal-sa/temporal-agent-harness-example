from __future__ import annotations

import unittest
from datetime import timedelta
from unittest.mock import AsyncMock, patch

from temporalio.common import RetryPolicy

from agent_harness.activity_options import DEFAULT_TOOL_ACTIVITY_OPTIONS
from agent_harness.tools import ToolContext, ToolSet


async def example_tool_activity() -> dict[str, bool]:
    return {"ok": True}


class ToolRetryTests(unittest.IsolatedAsyncioTestCase):
    async def test_tool_activities_use_exponential_backoff_by_default(self) -> None:
        ctx = ToolContext(tool_name="example", _tools=ToolSet())

        with patch(
            "agent_harness.tools.execute_routed_activity",
            new=AsyncMock(return_value={"ok": True}),
        ) as execute:
            await ctx.activity(example_tool_activity)

        options = execute.await_args.kwargs["defaults"]
        retry_policy = options.retry_policy
        self.assertIs(options, DEFAULT_TOOL_ACTIVITY_OPTIONS)
        self.assertIsNotNone(retry_policy)
        self.assertEqual(retry_policy.initial_interval, timedelta(seconds=1))
        self.assertEqual(retry_policy.backoff_coefficient, 2.0)
        self.assertEqual(retry_policy.maximum_interval, timedelta(seconds=30))
        self.assertEqual(retry_policy.maximum_attempts, 6)

    async def test_tool_activity_can_override_default_retry_policy(self) -> None:
        ctx = ToolContext(tool_name="example", _tools=ToolSet())
        override = RetryPolicy(maximum_attempts=1)

        with patch(
            "agent_harness.tools.execute_routed_activity",
            new=AsyncMock(return_value={"ok": True}),
        ) as execute:
            await ctx.activity(
                example_tool_activity,
                retry_policy=override,
            )

        self.assertIs(execute.await_args.kwargs["retry_policy"], override)


if __name__ == "__main__":
    unittest.main()
