# Temporal Agent Harness Example

This repo is not trying to be a generic agent SDK.

It is an example of how a team might build a small, opinionated agent harness that matches its own operational needs: durable execution, readable Temporal history, explicit tool categories, runtime guard enforcement, and narrowly controlled model access.

The current harness is Claude-specific on purpose. The interesting part is not "how to call an LLM"; it is how the agent loop, tool registry, guard policy, and Temporal execution model fit together.

## What This Shows

- Agent loops can be ordinary Temporal workflow code.
- LLM calls can be isolated in one activity.
- Tool implementations can orchestrate durable work instead of being simple request/response functions.
- Guards can be enforced by the harness at runtime, not left to every tool author to remember.
- Event history can stay readable even when many tools share one generic activity implementation.
- Optional sideband streaming can provide non-durable progress updates without coupling product UX to Temporal history.

## Core Shape

The Claude call is an activity. Tool execution happens from workflow code. If a tool needs side effects, it calls through `ctx.activity(...)`, which routes through a generic activity while setting a useful Temporal summary.

```python
@TOOLS.tool(
    name="lookup_customer",
    description="Look up a customer by id.",
    tool_type=ToolType.READ,
)
async def lookup_customer(ctx: ToolContext, customer_id: str) -> ToolResult:
    payload = await ctx.activity(
        _lookup_customer_activity,
        args={"customer_id": customer_id},
    )
    return ToolResult(payload=payload, error=False)
```

If no `step` is provided, the activity summary is the tool name:

```python
await ctx.activity(_lookup_customer_activity)
# summary: "lookup_customer"
```

If a tool has multiple activity steps, the tool author names them:

```python
await ctx.activity(_load_customer, step="load")
await ctx.activity(_update_customer, step="update")
# summaries: "lookup_customer:load", "lookup_customer:update"
```

This keeps the activity type generic while making Temporal history useful to humans.

## Activity Defaults

Agent construction is where the application sets normal activity behavior for tools and guards: task queues, timeouts, retry policy, cancellation behavior, and related Temporal options.

```python
from datetime import timedelta

from temporalio.common import RetryPolicy

from claude_harness.activity_options import ActivityOptions
from claude_harness.claude_agent import ClaudeAgent

agent = ClaudeAgent(
    "You are an internal operations agent.",
    TOOLS,
    model="claude-sonnet-4-5",
    activity_options=ActivityOptions(
        schedule_to_start_timeout=timedelta(seconds=30),
        start_to_close_timeout=timedelta(minutes=5),
        retry_policy=RetryPolicy(maximum_attempts=3),
    ),
)
```

Tool and guard authors can override those defaults for a specific activity step:

```python
async def export_report(ctx: ToolContext, report_id: str) -> ToolResult:
    result = await ctx.activity(
        _export_report_activity,
        step="export",
        args={"report_id": report_id},
        start_to_close_timeout=timedelta(hours=1),
        retry_policy=RetryPolicy(maximum_attempts=1),
    )
    return ToolResult(payload=result, error=False)
```

The harness keeps the summary convention while allowing each step to use the Temporal execution policy it actually needs.

## Reusable Guard Workflow Example

This example shows a reusable customer confirmation guard. `CUSTOMER_CHANGE` is an example company-specific tool category: the point is that a class of tools can require the same guard, while the guard chooses the right confirmation path from the tool context.

The customer confirmation workflow is reusable infrastructure:

```python
# workflows/customer_confirmation_workflow.py
import asyncio
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Literal

from temporalio import activity, workflow


ConfirmationStatus = Literal["accepted", "rejected", "timed_out"]


@dataclass
class CustomerConfirmationRequest:
    customer_email: str
    template: str
    payload: dict[str, Any]


@dataclass
class CustomerConfirmationEmail:
    message_id: str
    confirmation_workflow_id: str


@dataclass
class CustomerConfirmationResult:
    status: ConfirmationStatus
    accepted: bool
    email: CustomerConfirmationEmail


@activity.defn
async def send_customer_confirmation_email(
    request: CustomerConfirmationRequest,
    confirmation_workflow_id: str,
) -> CustomerConfirmationEmail:
    # The email should link to an app endpoint that signals
    # CustomerConfirmationWorkflow.confirm on this workflow id.
    # request.template selects the email template; request.payload fills it.
    return CustomerConfirmationEmail(
        message_id="email-message-id",
        confirmation_workflow_id=confirmation_workflow_id,
    )


@workflow.defn
class CustomerConfirmationWorkflow:
    def __init__(self) -> None:
        self._accepted: bool | None = None

    @workflow.signal
    async def confirm(self, accepted: bool) -> None:
        self._accepted = accepted

    @workflow.run
    async def run(
        self, request: CustomerConfirmationRequest
    ) -> CustomerConfirmationResult:
        confirmation_workflow_id = workflow.info().workflow_id
        email = await workflow.execute_activity(
            send_customer_confirmation_email,
            args=[request, confirmation_workflow_id],
            start_to_close_timeout=timedelta(minutes=1),
            summary=f"customer_confirmation:{request.template}",
        )

        try:
            await workflow.wait_condition(
                lambda: self._accepted is not None,
                timeout=timedelta(days=5),
                timeout_summary="customer_confirmation_timeout",
            )
        except asyncio.TimeoutError:
            return CustomerConfirmationResult(
                status="timed_out",
                accepted=False,
                email=email,
            )

        if not self._accepted:
            return CustomerConfirmationResult(
                status="rejected",
                accepted=False,
                email=email,
            )

        return CustomerConfirmationResult(
            status="accepted",
            accepted=True,
            email=email,
        )
```

The guard is reusable policy. It maps the current tool call to a confirmation template and starts the child workflow:

```python
# guards/customer_confirmation_guard.py
from dataclasses import asdict

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from claude_harness.guards import GuardContext, GuardResult
    from claude_harness.tool_types import ToolType
    from my_agent.registry import TOOLS
    from my_agent.workflows.customer_confirmation_workflow import (
        CustomerConfirmationRequest,
        CustomerConfirmationWorkflow,
    )


def _customer_confirmation_request(ctx: GuardContext) -> CustomerConfirmationRequest:
    if ctx.tool_name == "substitute_item":
        return CustomerConfirmationRequest(
            customer_email=ctx.tool_args["customer_email"],
            template="substitute_item",
            payload={
                "order_id": ctx.tool_args["order_id"],
                "unavailable_sku": ctx.tool_args["unavailable_sku"],
                "substitute_sku": ctx.tool_args["substitute_sku"],
            },
        )

    if ctx.tool_name == "change_shipping_address":
        return CustomerConfirmationRequest(
            customer_email=ctx.tool_args["customer_email"],
            template="change_shipping_address",
            payload={
                "order_id": ctx.tool_args["order_id"],
                "new_address": ctx.tool_args["new_address"],
            },
        )

    raise ValueError(f"No customer confirmation configured for {ctx.tool_name}")


@TOOLS.guard(name="confirm_customer_change", fulfills=ToolType.CUSTOMER_CHANGE)
async def confirm_customer_change(ctx: GuardContext) -> GuardResult:
    request = _customer_confirmation_request(ctx)
    result = await workflow.execute_child_workflow(
        CustomerConfirmationWorkflow.run,
        request,
        id=f"{workflow.info().workflow_id}-{ctx.tool_name}-confirmation-{workflow.uuid4()}",
        static_summary=f"{ctx.guard_name}:{ctx.tool_name}",
    )

    if not result.accepted:
        return GuardResult(
            passed=False,
            reason=result.status,
            llm_payload={
                "error": "Customer did not approve the requested change.",
                "confirmation": asdict(result),
            },
        )

    return GuardResult(
        passed=True,
        internal_payload={"confirmation": asdict(result)},
    )
```

The tool stays focused on the actual mutation:

```python
# tools/substitute_item_tool.py
from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from claude_harness.tool_types import ToolType
    from claude_harness.tools import ToolContext, ToolResult
    from my_agent.guards.customer_confirmation_guard import confirm_customer_change
    from my_agent.registry import TOOLS


async def _apply_substitution(
    order_id: str,
    unavailable_sku: str,
    substitute_sku: str,
) -> dict[str, str]:
    return {
        "order_id": order_id,
        "removed": unavailable_sku,
        "added": substitute_sku,
    }


@TOOLS.tool(
    name="substitute_item",
    description=(
        "Substitute an unavailable order item after customer confirmation."
    ),
    tool_type=ToolType.CUSTOMER_CHANGE,
    pre_guards=[confirm_customer_change],
)
async def substitute_item(
    ctx: ToolContext,
    order_id: str,
    unavailable_sku: str,
    substitute_sku: str,
    customer_email: str,
) -> ToolResult:
    applied = await ctx.activity(
        _apply_substitution,
        step="apply_substitution",
        args={
            "order_id": order_id,
            "unavailable_sku": unavailable_sku,
            "substitute_sku": substitute_sku,
        },
        start_to_close_timeout=timedelta(minutes=2),
    )
    return ToolResult(
        payload={
            "substitution_applied": True,
            "applied": applied,
        },
        error=False,
    )
```

The app endpoint behind the email link is product code. It uses `confirmation_workflow_id` from the email payload to signal `CustomerConfirmationWorkflow.confirm`.

This is the kind of thing the harness is meant to unlock. From Claude's point of view, `substitute_item` is one tool call. From the application's point of view, the reusable guard performs durable policy orchestration: a child workflow sends an email and waits for a customer signal or a five-day timer. The tool only runs after that guard passes, and its code is limited to the order mutation.

Adding `change_shipping_address` would be another tool with `tool_type=ToolType.CUSTOMER_CHANGE` and `pre_guards=[confirm_customer_change]`; the guard would choose the address-change template from `ctx.tool_name`.

## Guards

Tools are categorized with `ToolType`. The harness can require guards for specific categories. Today, `ToolType.ADMIN` requires a pre-guard by default.

```python
@TOOLS.guard(name="require_ops_approval", fulfills=ToolType.ADMIN)
async def require_ops_approval(ctx: GuardContext) -> GuardResult:
    approval = await ctx.activity(
        _request_ops_approval,
        step="approval",
        args={"tool_name": ctx.tool_name, "tool_args": ctx.tool_args},
    )

    if not approval["approved"]:
        return GuardResult(
            passed=False,
            reason="ops_approval_denied",
            llm_payload={
                "error": "Ops approval denied",
                "reason": "ops_approval_denied",
            },
        )

    return GuardResult(passed=True, internal_payload=approval)
```

The protected tool declares the guard explicitly:

```python
@TOOLS.tool(
    name="restart_service",
    description="Restart a production service.",
    tool_type=ToolType.ADMIN,
    pre_guards=[require_ops_approval],
)
async def restart_service(ctx: ToolContext, service_name: str) -> ToolResult:
    result = await ctx.activity(
        _restart_service,
        args={"service_name": service_name},
    )
    return ToolResult(payload=result, error=False)
```

Pre-guard failure prevents the tool from running. Post-guard failure prevents the model from receiving the raw tool result and returns the guard's `llm_payload` instead.

This does not make it impossible for a developer to write a bad guard. It does make missing guard coverage explicit and runtime-enforced.

## What This Unlocks

Because tools are workflow code, they can do more than call one function:

- Start child workflows for delegated work.
- Wait for durable approval flows, signals, updates, timers, or external state.
- Fan out to multiple activities and summarize each step clearly.
- Apply organization-specific guard policy before and after execution.
- Stream best-effort progress to a sideband sink when one is configured.

That means examples in this repo should be read less as "agent demos" and more as "harness capability demos." The agent is the vehicle; the point is showing what the harness makes easy, observable, and harder to misuse.

## Worker Registration

Applications using this harness should register the Claude activity plus the generic tool and guard routers:

```python
from claude_harness.claude_agent import call_claude
from claude_harness.guards import run_guard_activity
from claude_harness.tools import run_tool_activity
from my_agent.workflows.customer_confirmation_workflow import (
    CustomerConfirmationWorkflow,
    send_customer_confirmation_email,
)

activities = [
    call_claude,
    run_tool_activity,
    run_guard_activity,
    send_customer_confirmation_email,
]

workflows = [
    AgentWorkflow,
    CustomerConfirmationWorkflow,
]
```

The Anthropic SDK reads credentials from `ANTHROPIC_API_KEY`.

## Non-Goals

- A provider-neutral agent framework.
- A complete authorization system.
- A replacement for application-specific workflows.
- A polished library API.

The goal is to make the architectural tradeoffs concrete enough that another
team could adapt the pattern to its own internal agent platform.
