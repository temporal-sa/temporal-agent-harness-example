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

from claude_harness.claude_agent import ClaudeAgent
from claude_harness.tools import ActivityOptions

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

## Complete Long-Running Tool Example

This can live in one tool file. The worker still needs to import that file and register the child workflow class plus the direct activities used by that child workflow, but the tool, request/response types, child workflow, signal handler, and activity functions can be owned together.

```python
# substitute_item_tool.py
import asyncio
from dataclasses import asdict, dataclass
from datetime import timedelta
from typing import Literal

from temporalio import activity, workflow

with workflow.unsafe.imports_passed_through():
    from claude_harness.tools import ToolContext, ToolResult, ToolType
    from my_agent.registry import TOOLS


ConfirmationStatus = Literal["accepted", "rejected", "timed_out"]


@dataclass
class SubstitutionConfirmationRequest:
    order_id: str
    unavailable_sku: str
    substitute_sku: str
    customer_email: str


@dataclass
class SubstitutionEmail:
    message_id: str
    confirmation_workflow_id: str


@dataclass
class SubstitutionConfirmationResult:
    status: ConfirmationStatus
    accepted: bool
    email: SubstitutionEmail
    applied: dict[str, str] | None = None


@activity.defn
async def send_substitution_email(
    request: SubstitutionConfirmationRequest,
    confirmation_workflow_id: str,
) -> SubstitutionEmail:
    # The email should link to an app endpoint that signals
    # SubstitutionConfirmationWorkflow.confirm_substitution on this workflow id.
    return SubstitutionEmail(
        message_id="email-message-id",
        confirmation_workflow_id=confirmation_workflow_id,
    )


@activity.defn
async def apply_substitution(
    request: SubstitutionConfirmationRequest,
) -> dict[str, str]:
    return {
        "order_id": request.order_id,
        "removed": request.unavailable_sku,
        "added": request.substitute_sku,
    }


@workflow.defn
class SubstitutionConfirmationWorkflow:
    def __init__(self) -> None:
        self._accepted: bool | None = None

    @workflow.signal
    async def confirm_substitution(self, accepted: bool) -> None:
        self._accepted = accepted

    @workflow.run
    async def run(
        self, request: SubstitutionConfirmationRequest
    ) -> SubstitutionConfirmationResult:
        confirmation_workflow_id = workflow.info().workflow_id
        email = await workflow.execute_activity(
            send_substitution_email,
            args=[request, confirmation_workflow_id],
            start_to_close_timeout=timedelta(minutes=1),
            summary="substitute_item:email_customer",
        )

        try:
            await workflow.wait_condition(
                lambda: self._accepted is not None,
                timeout=timedelta(days=5),
                timeout_summary="substitution_confirmation_timeout",
            )
        except asyncio.TimeoutError:
            return SubstitutionConfirmationResult(
                status="timed_out",
                accepted=False,
                email=email,
            )

        if not self._accepted:
            return SubstitutionConfirmationResult(
                status="rejected",
                accepted=False,
                email=email,
            )

        applied = await workflow.execute_activity(
            apply_substitution,
            request,
            start_to_close_timeout=timedelta(minutes=2),
            summary="substitute_item:apply_substitution",
        )
        return SubstitutionConfirmationResult(
            status="accepted",
            accepted=True,
            email=email,
            applied=applied,
        )


@TOOLS.tool(
    name="substitute_item",
    description=(
        "Ask a customer to confirm an item substitution, wait up to 5 days for "
        "their response, and apply the substitution if accepted."
    ),
    tool_type=ToolType.MUTATING,
)
async def substitute_item(
    ctx: ToolContext,
    order_id: str,
    unavailable_sku: str,
    substitute_sku: str,
    customer_email: str,
) -> ToolResult:
    result = await workflow.execute_child_workflow(
        SubstitutionConfirmationWorkflow.run,
        SubstitutionConfirmationRequest(
            order_id=order_id,
            unavailable_sku=unavailable_sku,
            substitute_sku=substitute_sku,
            customer_email=customer_email,
        ),
        id=f"{workflow.info().workflow_id}-substitution-{workflow.uuid4()}",
        static_summary=f"{ctx.tool_name}:customer_confirmation",
    )
    return ToolResult(payload=asdict(result), error=False)
```

The app endpoint behind the email link is product code, not part of the tool file. It uses `confirmation_workflow_id` from the email payload to signal `SubstitutionConfirmationWorkflow.confirm_substitution`.

This is the kind of thing the harness is meant to unlock. From Claude's point of view, `substitute_item` is one tool call. From the application's point of view, it is durable orchestration: a child workflow sends an email, waits for a customer signal or a five-day timer, then conditionally mutates the order.

Splitting this file is not technically required. It becomes useful when activity implementations need heavy imports, client setup, or separate ownership. In that case, keep the tool and workflow shape the same and move the direct activity functions behind importable module-level functions.

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
from claude_harness.tools import run_guard_activity, run_tool_activity
from my_agent.tools.substitute_item_tool import (
    SubstitutionConfirmationWorkflow,
    apply_substitution,
    send_substitution_email,
)

activities = [
    call_claude,
    run_tool_activity,
    run_guard_activity,
    send_substitution_email,
    apply_substitution,
]

workflows = [
    AgentWorkflow,
    SubstitutionConfirmationWorkflow,
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
