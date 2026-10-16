"""CodingAgent: the coding agent's workflow, on temporal-agent-harness 0.7.0.

A bare ``@workflow.defn`` (not ``@agent.defn``) so that we own ``run`` and can continue as
new between turns, which the harness's generated run cannot do (CTO memo 001, CTO-5). The
harness ``AgentWorkflowRunner`` still owns turns, approvals, tool events and the event
stream; Pydantic AI owns the model/tool loop inside each ``chat`` turn.
"""

from __future__ import annotations

import asyncio
from collections import deque
from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from pydantic_ai.capabilities import Hooks
    from pydantic_ai.messages import ModelMessage, ModelMessagesTypeAdapter
    from temporal_agent_harness.ai_sdks.pydantic_ai_harness import HarnessDeps
    from temporal_agent_harness.harness import agent
    from temporal_agent_harness.harness.agent_protocol import (
        AgentConfig,
        AgentMessage,
        MidTurn,
        ToolApprovalDecision,
        ToolApprovalPolicy,
    )
    from temporal_agent_harness.harness.agent_workflow import AgentWorkflowRunner, MessageContext
    from temporalio.contrib.workflow_streams import WorkflowStream

    from simple_chat_agent.agent import harness_ext
    from simple_chat_agent.agent.contracts import (
        AGENT_WORKFLOW_TYPE,
        CARRIED_STATE_VERSION,
        ENQUEUE_APPROVAL_SIGNAL,
        ENQUEUE_MESSAGE_SIGNAL,
        Ack,
        CarriedState,
        ChatMessage,
        ChatReply,
        CodingAgentInit,
        FrontDoorApproval,
        FrontDoorMessage,
        InterruptRequest,
        ModelChoice,
        Posture,
        SetModel,
    )
    from simple_chat_agent.agent.runtime import CODING_AGENT, DEFAULT_MODEL, is_allowed_model, model_settings
    from simple_chat_agent.agent.tools.workspace import TEST_TOOLS, WORKSPACE_TOOLS

# Wake an idle session this often so a deploy can move it to the new version (PRD A18).
IDLE_WAKE = timedelta(hours=1)
# How many front-door request ids to remember for dedup, across continue-as-new.
RECENT_REQUEST_IDS = 500
# Stream events carried across continue-as-new (see _continue_as_new).
CAN_RETAINED_EVENTS = 500
INTERRUPTED_TOOL_NOTE = (
    "Stopped by the user before this result was used. The call may or may not have completed;\n"
    "check before relying on its effect."
)


def posture_policy(posture: Posture) -> ToolApprovalPolicy:
    if posture == "ask_everything":
        return ToolApprovalPolicy.always_require_human_approval()
    if posture == "never_ask":
        return ToolApprovalPolicy.dangerously_skip_all()
    if posture == "auto":
        return ToolApprovalPolicy.auto_mode(pre_approve_inherently_safe=True)
    return ToolApprovalPolicy.allow_inherently_safe()


# Versioning behavior comes from the worker's deployment config (PINNED by default), so the
# same class also runs on an unversioned local worker.
@workflow.defn(name=AGENT_WORKFLOW_TYPE)
class CodingAgent:
    """A coding agent that works in one sandboxed workspace."""

    @workflow.init
    def __init__(self, config: AgentConfig, data: CodingAgentInit) -> None:
        carried = data.carried or CarriedState(model=data.model)
        if carried.state_version > CARRIED_STATE_VERSION:
            # A newer version continued into this one, e.g. Current was rolled back after a
            # deploy. Fail the workflow TASK (not the workflow): Temporal retries it until a
            # version that understands this state is Current again. Nothing is lost.
            raise RuntimeError(
                f"carried state v{carried.state_version} is newer than this worker "
                f"understands (v{CARRIED_STATE_VERSION}); waiting for a compatible version"
            )
        self._data = data
        self._stream = WorkflowStream(prior_state=carried.stream_state)
        self._runner = AgentWorkflowRunner(
            config,
            stream=self._stream,
            approval_policy_default=posture_policy(data.posture),
        )
        harness_ext.restore_turn_counter(self._runner, carried.turn_counter)
        self._history: list[ModelMessage] = ModelMessagesTypeAdapter.validate_python(
            carried.conversation
        )
        self._model: ModelChoice = carried.model
        self._pending_model: ModelChoice | None = carried.pending_model
        self._seen_requests: deque[str] = deque(carried.recent_request_ids, maxlen=RECENT_REQUEST_IDS)
        self._deferred: list[FrontDoorMessage] = list(carried.pending_messages)
        self._workspace_id = data.workspace_id or workflow.info().workflow_id
        self._turns_this_run = 0
        self._continuing = False
        # Interrupt bookkeeping for the turn in flight.
        self._run_task: asyncio.Task[str] | None = None
        self._run_turn_id: str | None = None
        self._interrupted = False
        self._snapshot: list[ModelMessage] | None = None
        self._rejected: list[dict[str, Any]] = []

        # Messages sent through the harness Update are refused while we hand off to the next
        # run; callers retry and reach the new run. The Signal front door buffers instead.
        harness_ext.guard_send_agent_message(self._runner, lambda: self._continuing)

        # Messages that arrived during the previous run's handoff go first, before any Signal
        # delivered in this run's first activation (signal handlers run before `run` starts).
        for message in self._deferred:
            self._admit(message)
        self._deferred = []

    # -- Lifecycle --------------------------------------------------------------

    @workflow.run
    async def run(self, config: AgentConfig, data: CodingAgentInit) -> None:
        loop = asyncio.ensure_future(self._runner.run(self))
        while True:
            try:
                await workflow.wait_condition(
                    lambda: loop.done() or self._ready_to_continue_as_new(),
                    timeout=IDLE_WAKE,
                )
            except asyncio.TimeoutError:
                pass  # Re-check the deploy target below.
            if loop.done():
                loop.result()
                return
            if self._ready_to_continue_as_new():
                await self._continue_as_new(config, data)

    def _wants_continue_as_new(self) -> bool:
        info = workflow.info()
        forced = self._data.force_continue_as_new_after_turns
        return (
            info.is_continue_as_new_suggested()
            or info.is_target_worker_deployment_version_changed()
            or (forced is not None and self._turns_this_run >= forced)
        )

    def _ready_to_continue_as_new(self) -> bool:
        return self._wants_continue_as_new() and harness_ext.is_quiescent(self._runner)

    async def _continue_as_new(self, config: AgentConfig, data: CodingAgentInit) -> None:
        # From here on, Signals buffer into _deferred and the Update path refuses, so nothing
        # is admitted while handlers drain.
        self._continuing = True
        self._stream.detach_pollers()
        await workflow.wait_condition(workflow.all_handlers_finished)
        if harness_ext.is_closed(self._runner):
            # A close arrived during the handoff: finish this run instead of continuing.
            self._continuing = False
            return
        # Carry only the tail of the event log. Offsets stay monotonic; a reader that was
        # mid-poll at the handoff resumes inside the retained tail instead of falling behind
        # the truncation and silently skipping the last turn's closing events. (P2: truncate
        # to the Redis bridge's acknowledged offset instead of a fixed tail.)
        head = harness_ext.stream_head(self._stream)
        self._stream.truncate(max(harness_ext.stream_base(self._stream), head - CAN_RETAINED_EVENTS))
        carried = CarriedState(
            conversation=ModelMessagesTypeAdapter.dump_python(self._history, mode="json"),
            stream_state=self._stream.get_state(),
            turn_counter=harness_ext.turn_counter(self._runner),
            recent_request_ids=list(self._seen_requests),
            model=self._model,
            pending_model=self._pending_model,
            pending_messages=self._deferred,
        )
        next_config = AgentConfig(
            approval_policy=self._runner.approval_policy,
            auto_approval_criteria=self._runner.auto_approval_criteria,
            agent_id=config.agent_id or harness_ext.agent_id(self._runner),
        )
        next_data = data.model_copy(
            update={"carried": carried, "force_continue_as_new_after_turns": None}
        )
        workflow.continue_as_new(
            args=[next_config, next_data],
            initial_versioning_behavior=workflow.ContinueAsNewVersioningBehavior.AUTO_UPGRADE,
        )

    @workflow.query(name="history_stats")
    def history_stats(self) -> dict[str, Any]:
        """Diagnostics for tests and operators (the product UI never queries)."""
        responses = [m for m in self._history if m.kind == "response"]
        return {
            "messages": len(self._history),
            "responses": len(responses),
            "turn_counter": harness_ext.turn_counter(self._runner),
            "model": self._model.registry_key,
            "allowed_tools": sorted(self._runner.approval_policy.auto_approve_tools),
            "rejected": self._rejected,
            "last_response_text": "".join(
                getattr(p, "content", "") for p in responses[-1].parts if p.part_kind == "text"
            )
            if responses
            else "",
        }

    # -- Signal front door (PRD S8) -----------------------------------------------

    @workflow.signal(name=ENQUEUE_MESSAGE_SIGNAL)
    async def enqueue_message(self, message: FrontDoorMessage) -> None:
        if self._is_duplicate(message.request_id):
            return
        if self._continuing:
            self._deferred.append(message)
            return
        self._admit(message)

    @workflow.signal(name=ENQUEUE_APPROVAL_SIGNAL)
    async def enqueue_approval(self, approval: FrontDoorApproval) -> None:
        if self._is_duplicate(approval.request_id):
            return
        decision = ToolApprovalDecision(
            tool_id=approval.tool_id,
            approved=approval.approved,
            reason=approval.reason,
            remember=approval.remember,
        )
        try:
            await harness_ext.admit_approval(self._runner, decision)
        except ApplicationError as exc:
            self._reject(approval.request_id, exc)

    def _admit(self, message: FrontDoorMessage) -> None:
        try:
            harness_ext.admit_message(
                self._runner,
                AgentMessage(type=message.type, payload=message.payload),
                message_id=message.request_id,
            )
        except ApplicationError as exc:
            self._reject(message.request_id, exc)
            harness_ext.publish_rejection(
                self._runner, message_id=message.request_id, reason=f"Not accepted: {exc.message}"
            )

    def _is_duplicate(self, request_id: str) -> bool:
        if request_id in self._seen_requests:
            return True
        self._seen_requests.append(request_id)
        return False

    def _reject(self, request_id: str, exc: ApplicationError) -> None:
        workflow.logger.warning("front door rejected %s: %s", request_id, exc)
        self._rejected = [*self._rejected[-49:], {"request_id": request_id, "type": exc.type, "message": str(exc)}]

    # -- Operations ---------------------------------------------------------------

    @agent.accepts(mid_turn=MidTurn.ENQUEUE)
    async def chat(self, msg: ChatMessage, ctx: agent.Injected[MessageContext]) -> ChatReply:
        """Ask the coding agent to do something in its workspace, or ask it a question."""
        if self._pending_model is not None:
            self._model, self._pending_model = self._pending_model, None
        if not is_allowed_model(self._model):
            # The model left this worker's allowlist (config change or a different deploy).
            self._model = DEFAULT_MODEL
        self._turns_this_run += 1
        self._interrupted = False
        self._snapshot = None
        self._run_turn_id = ctx.turn_id
        task = asyncio.ensure_future(self._run_model(msg.text))
        self._run_task = task
        try:
            return ChatReply(text=await task)
        except BaseException as exc:
            # Cancelling the run surfaces as asyncio.CancelledError, as ActivityError("Activity
            # cancelled") from an in-flight activity, or as TurnInterrupted from the
            # before-request hook. Anything else is a real failure, interrupt or not.
            if not (self._interrupted and harness_ext.is_cancellation(exc)):
                raise
            # The turn was stopped. If it reached the model, keep everything up to the last
            # model response, close the tool calls it left open, and record the stop so the
            # next turn has context. If it never reached the model, history is unchanged.
            if self._snapshot is not None:
                self._history = harness_ext.close_with_note(
                    self._snapshot,
                    tool_note=INTERRUPTED_TOOL_NOTE,
                    user_note="(I stopped this turn. Don't continue it unless I ask.)",
                )
            return ChatReply(text="", interrupted=True)
        finally:
            self._run_task = None
            self._run_turn_id = None

    async def _run_model(self, text: str) -> str:
        choice = self._model
        hooks = Hooks(
            before_model_request=self._before_model_request,
            after_model_request=self._after_model_request,
        )
        toolset = harness_ext.per_run_toolset(
            [*WORKSPACE_TOOLS, *(TEST_TOOLS if self._data.test_tools else ())],
            id="workspace",
            injections={"workspace_id": self._workspace_id},
            is_interrupted=lambda: self._interrupted,
        )
        result = await CODING_AGENT.run(
            text,
            deps=HarnessDeps(runner=self._runner),
            message_history=self._history,
            model=choice.registry_key,
            model_settings=model_settings(choice, session_id=self._data.session_id),
            toolsets=[toolset],
            capabilities=[hooks],
        )
        self._history = result.all_messages()
        return str(result.output)

    async def _before_model_request(self, ctx: Any, request_context: Any) -> Any:
        if self._interrupted:
            # A tool result arrived after Stop; don't start another model call.
            raise harness_ext.TurnInterrupted()
        self._snapshot = list(request_context.messages)
        return request_context

    async def _after_model_request(self, ctx: Any, *, request_context: Any, response: Any) -> Any:
        self._snapshot = [*request_context.messages, response]
        return response

    @agent.accepts(mid_turn=MidTurn.ACCEPT, model_callable=False)
    async def set_model(self, msg: SetModel) -> Ack:
        """Switch the provider and/or model. Takes effect from the next turn."""
        if not is_allowed_model(msg.model):
            return Ack(ok=False, detail=f"{msg.model.registry_key} is not an allowed model")
        if self._run_task is None:
            self._model = msg.model
            return Ack(detail=f"Now using {msg.model.registry_key}.")
        self._pending_model = msg.model
        return Ack(detail=f"Switching to {msg.model.registry_key} from the next turn.")

    @agent.accepts(mid_turn=MidTurn.ACCEPT, model_callable=False)
    async def interrupt(self, msg: InterruptRequest) -> Ack:
        """Stop the turn in progress. Pending approvals in it are declined."""
        task, turn_id = self._run_task, self._run_turn_id
        if task is None or turn_id is None or task.done():
            return Ack(ok=False, detail="Nothing is running.")
        declined = harness_ext.decline_pending_approvals(
            self._runner, turn_id=turn_id, reason=msg.reason
        )
        self._interrupted = True
        task.cancel()
        return Ack(detail=f"Stopped. Declined {declined} pending approval(s).")
