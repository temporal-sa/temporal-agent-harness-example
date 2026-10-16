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
    from temporal_agent_harness.harness import agent
    from temporal_agent_harness.harness.agent_protocol import (
        AgentConfig,
        AgentMessage,
        MidTurn,
        ToolApprovalDecision,
    )
    from temporal_agent_harness.harness.agent_workflow import AgentWorkflowRunner, MessageContext
    from temporalio.contrib.workflow_streams import WorkflowStream

    from simple_chat_agent.agent import approvals, context, harness_ext
    from simple_chat_agent.agent.budget import HistoryBudget
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
        SetModel,
        SetPosture,
        SteerMessage,
    )
    from simple_chat_agent.agent.prompts import CODING_AGENT_INSTRUCTIONS
    from simple_chat_agent.agent.runtime import (
        CODING_AGENT,
        DEFAULT_MODEL,
        ENABLED_RESEARCH_TOOLS,
        CodingDeps,
        is_allowed_model,
        model_settings,
    )
    from simple_chat_agent.agent.state import (
        STATE_TOOLS,
        ChangesState,
        PlanState,
        WorkspaceState,
        fold_tool_result,
    )
    from simple_chat_agent.agent.subagents import (
        MAX_CONCURRENT_SUBAGENTS,
        SubagentInit,
        subagent_tools,
    )
    from simple_chat_agent.agent.tools.artifacts import ARTIFACT_TOOLS
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


# Versioning behavior comes from the worker's deployment config (PINNED by default), so the
# same class also runs on an unversioned local worker.
@workflow.defn(name=AGENT_WORKFLOW_TYPE)
class CodingAgent:
    """A coding agent that works in one sandboxed workspace."""

    # Observable state: the UI's Plan and Changes panels (PRD A13).
    plan = agent.state(PlanState)
    changes = agent.state(ChangesState)
    workspace = agent.state(WorkspaceState)

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
        # Restore observable state before the runner attaches it, so the first snapshot the
        # new run publishes already holds it.
        for name, ref in (("plan", self.plan), ("changes", self.changes), ("workspace", self.workspace)):
            if name in carried.observable:
                ref.set(type(ref.current).model_validate(carried.observable[name]))
        self._steer: list[str] = list(carried.steer_queue)
        self._stream = WorkflowStream(prior_state=carried.stream_state)
        criteria = approvals.default_criteria(data.environment)
        if data.environment == "catalog":
            # In the catalog the operator's criteria are the only criteria: a caller can't
            # retarget, say, run_command to a class the judge may approve.
            config = config.model_copy(update={"auto_approval_criteria": None})
        self._runner = AgentWorkflowRunner(
            config,
            stream=self._stream,
            approval_policy_default=approvals.policy_for(data.posture, data.environment),
            auto_approval_criteria_default=criteria,
            # Always wired, so switching to auto mode mid-session works (the harness rejects
            # an auto-mode policy without an evaluator).
            auto_mode_evaluator=approvals.build_evaluator(data.environment, data.judge),
        )
        harness_ext.guard_tool_approval(self._runner, self._limit_remember)
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
        self._signals_this_run = 0
        self._checkpoint_requested = False
        self._history_budget = (
            HistoryBudget(max_events=data.test_history_event_cap, fraction=1.0)
            if data.test_history_event_cap
            else HistoryBudget()
        )

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
            self._checkpoint_requested
            or info.is_continue_as_new_suggested()
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
            # A close arrived during the handoff: finish this run instead of continuing, and
            # tell senders their buffered messages won't run.
            for message in self._deferred:
                harness_ext.publish_rejection(
                    self._runner, message_id=message.request_id, reason="The session closed."
                )
            self._deferred = []
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
            observable={
                "plan": self.plan.current.model_dump(mode="json"),
                "changes": self.changes.current.model_dump(mode="json"),
                "workspace": self.workspace.current.model_dump(mode="json"),
            },
            steer_queue=self._steer,
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
        self._signals_this_run += 1
        if self._is_duplicate(message.request_id):
            return
        if self._continuing:
            self._deferred.append(message)
            return
        self._admit(message)

    @workflow.signal(name=ENQUEUE_APPROVAL_SIGNAL)
    async def enqueue_approval(self, approval: FrontDoorApproval) -> None:
        self._signals_this_run += 1
        if self._is_duplicate(approval.request_id):
            return
        decision = self._limit_remember(
            ToolApprovalDecision(
                tool_id=approval.tool_id,
                approved=approval.approved,
                reason=approval.reason,
                remember=approval.remember,
            )
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

    def _limit_remember(self, decision: ToolApprovalDecision) -> ToolApprovalDecision:
        """"Always allow" only for workspace edits (PRD A14): for shell, network and
        external tools an approval applies to this one call."""
        if not decision.remember:
            return decision
        tool = harness_ext.pending_tool_name(self._runner, decision.tool_id) or ""
        risk = approvals.risk_class_for(tool, self._runner.auto_approval_criteria)
        if approvals.remember_allowed(risk):
            return decision
        return decision.model_copy(update={"remember": False})

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
        task = asyncio.ensure_future(self._run_turn(msg.text))
        self._run_task = task
        try:
            return ChatReply(text=await task)
        except harness_ext.CheckpointNeeded as cp:
            # Keep the work so far and hand off to a fresh run; the user continues from there.
            if self._snapshot is not None:
                self._history = harness_ext.close_with_note(
                    self._snapshot,
                    tool_note=INTERRUPTED_TOOL_NOTE,
                    user_note="(The turn paused at a checkpoint. Continue when I say so.)",
                )
            self._checkpoint_requested = True
            return ChatReply(
                text="I paused here to start a fresh workflow run. Say \"continue\" to carry on.",
                checkpoint=cp.reason,
            )
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
            self._steer.clear()
            # Subagents live for one turn: none outlive the turn that started them (and none
            # may be running when the agent continues as new).
            for subagent_id in harness_ext.active_subagent_ids(self._runner):
                await self._runner.stop_subagent(subagent_id)

    def _tools(self) -> list[Any]:
        tools: list[Any] = [*WORKSPACE_TOOLS, *ENABLED_RESEARCH_TOOLS, *ARTIFACT_TOOLS, *STATE_TOOLS]
        if self._data.depth == 0:
            tools += subagent_tools(
                task_queue=workflow.info().task_queue,
                init=SubagentInit(
                    workspace_id=self._workspace_id,
                    owner_id=self._data.owner_id,
                    session_id=self._data.session_id,
                    model=self._model,
                ),
            )
        if self._data.test_tools:
            tools += TEST_TOOLS
        return tools

    def _before_tool(self, name: str) -> str | None:
        if name.startswith("start_") and len(harness_ext.active_subagent_ids(self._runner)) >= MAX_CONCURRENT_SUBAGENTS:
            return (
                f"At most {MAX_CONCURRENT_SUBAGENTS} subagents can run at once. Use or stop "
                "one that is running first."
            )
        return None

    def _after_tool(self, name: str, tool_id: str, args: dict[str, Any], result: Any) -> None:
        fold_tool_result(
            self.changes, self.workspace, tool_name=name, tool_id=tool_id, args=args, result=result
        )

    async def _run_turn(self, text: str) -> str:
        """One model run, with one retry on a context overflow using a tighter budget,
        resumed from the last request so tools that already ran don't run again."""
        budget = context.budget_for(self._model.registry_key)
        try:
            return await self._run_model(text, budget)
        except BaseException as exc:
            if self._interrupted or not context.is_context_overflow(exc) or self._snapshot is None:
                raise
            self._history = self._snapshot
            return await self._run_model(None, context.tighter(budget))

    async def _run_model(self, text: str | None, budget: Any) -> str:
        choice = self._model
        hooks = Hooks(
            before_model_request=self._before_model_request,
            after_model_request=self._after_model_request,
        )
        toolset = harness_ext.per_run_toolset(
            self._tools(),
            id="coding",
            injections={
                "workspace_id": self._workspace_id,
                "plan": self.plan,
                "owner_id": self._data.owner_id,
                "session_id": self._data.session_id or workflow.info().workflow_id,
            },
            is_interrupted=lambda: self._interrupted,
            before_call=self._before_tool,
            on_result=self._after_tool,
        )
        result = await CODING_AGENT.run(
            text,
            instructions=CODING_AGENT_INSTRUCTIONS,
            deps=CodingDeps(
                runner=self._runner, session_id=self._data.session_id, owner_id=self._data.owner_id
            ),
            message_history=self._history,
            model=choice.registry_key,
            model_settings=model_settings(choice, session_id=self._data.session_id),
            toolsets=[toolset],
            # ContextFit before Hooks: the snapshot then holds the history as sent.
            capabilities=[context.ContextFit(budget), hooks],
        )
        self._history = result.all_messages()
        return str(result.output)

    async def _before_model_request(self, ctx: Any, request_context: Any) -> Any:
        if self._interrupted:
            # A tool result arrived after Stop; don't start another model call.
            raise harness_ext.TurnInterrupted()
        info = workflow.info()
        reason = self._history_budget.should_checkpoint(
            history_length=info.get_current_history_length(),
            history_size_bytes=info.get_current_history_size(),
            signals=self._signals_this_run,
        )
        if reason is not None:
            self._snapshot = list(request_context.messages)
            raise harness_ext.CheckpointNeeded(reason)
        self._snapshot = list(request_context.messages)
        self._deliver_steer(ctx)
        return request_context

    async def _after_model_request(self, ctx: Any, *, request_context: Any, response: Any) -> Any:
        self._snapshot = [*request_context.messages, response]
        # Steer that arrived during this model call goes in before the next one (or redirects
        # the run if this response would have ended it).
        self._deliver_steer(ctx)
        return response

    def _deliver_steer(self, ctx: Any) -> None:
        if self._steer:
            text = "\n\n".join(self._steer)
            self._steer.clear()
            ctx.enqueue(f"(The user adds, while you work:)\n{text}")

    @agent.accepts(mid_turn=MidTurn.ACCEPT, model_callable=False)
    async def steer(self, msg: SteerMessage) -> Ack:
        """Guide the turn in progress. The text reaches the agent before its next model call."""
        if self._run_task is None:
            return Ack(ok=False, detail="Nothing is running; send a message instead.")
        self._steer.append(msg.text)
        return Ack(detail="Queued for the agent's next step.")

    @agent.accepts(mid_turn=MidTurn.ACCEPT, model_callable=False)
    async def set_model(self, msg: SetModel) -> Ack:
        """Switch the provider and/or model. Takes effect from the next turn."""
        if not is_allowed_model(msg.model):
            return Ack(ok=False, detail=f"{msg.model.registry_key} is not an allowed model")
        if self._run_task is None:
            self._model = msg.model
            self._pending_model = None
            return Ack(detail=f"Now using {msg.model.registry_key}.")
        self._pending_model = msg.model
        return Ack(detail=f"Switching to {msg.model.registry_key} from the next turn.")

    @agent.accepts(mid_turn=MidTurn.ACCEPT, model_callable=False)
    async def set_posture(self, msg: SetPosture) -> Ack:
        """Change how tool calls are approved. Applies to new calls; pending ones stay with you."""
        try:
            policy = approvals.policy_for(msg.posture, self._data.environment)
        except ApplicationError as exc:
            return Ack(ok=False, detail=exc.message)
        # Keep "always allow" rules the user already gave (they are workspace-edit only).
        for tool in self._runner.approval_policy.auto_approve_tools:
            policy = policy.with_tool_allowed(tool)
        self._runner.set_approval_policy(policy)
        self._data = self._data.model_copy(update={"posture": msg.posture})
        return Ack(detail=f"Approvals: {msg.posture.replace('_', ' ')}.")

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
