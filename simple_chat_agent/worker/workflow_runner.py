from __future__ import annotations

from temporalio.worker.workflow_sandbox import (
    SandboxedWorkflowRunner,
    SandboxRestrictions,
)


def agent_harness_workflow_runner() -> SandboxedWorkflowRunner:
    """Return the shared workflow runner used by workers and history replay."""

    restrictions = SandboxRestrictions.default.with_passthrough_modules(
        "agent_harness",
        "annotated_types",
    )
    return SandboxedWorkflowRunner(restrictions=restrictions)
