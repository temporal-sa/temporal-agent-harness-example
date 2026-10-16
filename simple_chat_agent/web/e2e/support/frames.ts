// Hand-built SSE frames in the shapes the app really receives:
// * harness events: the flattened `AgentEvent` (client protocol.d.ts / frames.d.ts): the
//   payload's own fields plus agent_id, turn_id, turn_number, message_id, timestamp; the mock
//   server adds event_offset and resume_offset the way the bridge stores them;
// * our telemetry frames, exactly as simple_chat_agent/api/agent_telemetry.py builds them
//   (usage_frame, recovery_frame, worker_frame, rejected_frame).

export interface Frame {
  event: string;
  data: Record<string, unknown>;
}

export const ROOT = "agent-root";

let clock = 1_792_000_000; // seconds, like the bridge's time.time()
const now = () => (clock += 0.25);

export interface TurnRef {
  agentId: string;
  turnId: string;
  turnNumber: number;
  messageId: string;
}

export function turn(turnNumber: number, messageId: string, agentId = ROOT): TurnRef {
  return { agentId, turnId: `${agentId}-turn-${turnNumber}`, turnNumber, messageId };
}

/** One harness event for `t`. */
export function ev(t: TurnRef, type: string, payload: Record<string, unknown> = {}, messageId: string | null = t.messageId): Frame {
  return {
    event: type,
    data: { type, ...payload, agent_id: t.agentId, turn_id: t.turnId, turn_number: t.turnNumber, message_id: messageId, timestamp: now() },
  };
}

// -- harness events --------------------------------------------------------------------------

export const accepted = (t: TurnRef, handler: string, payload: Record<string, unknown>, disposition = "opened") =>
  ev(t, "message_accepted", { handler, payload, disposition });

/** message_accepted, message_handler_start, turn_started. */
export function openTurn(t: TurnRef, text: string, handler = "chat"): Frame[] {
  return [accepted(t, handler, handler === "chat" ? { text } : { question: text }), ev(t, "message_handler_start"), ev(t, "turn_started")];
}

export function closeTurn(t: TurnRef, output: Record<string, unknown>): Frame[] {
  return [ev(t, "message_handler_end", { output }), ev(t, "turn_end")];
}

export interface Usage {
  input: number | null;
  output: number | null;
  cacheRead?: number | null;
  cacheWrite?: number | null;
  thinking?: number | null;
}

/** model_interaction_started/ended plus the bridge's live x_usage for it. */
export function modelCall(
  t: TurnRef,
  callIndex: number,
  usage: Usage,
  opts: { model?: string; provider?: string; text?: string; thought?: string; confirmed?: boolean } = {},
): Frame[] {
  const model = opts.model ?? "claude-sonnet-5-5";
  const frames: Frame[] = [ev(t, "model_interaction_started", { model })];
  if (opts.thought) frames.push(ev(t, "thought_summary", { delta: { content: opts.thought } }));
  if (opts.text) frames.push(ev(t, "reply_delta", { text: opts.text }));
  frames.push(
    ev(t, "model_interaction_ended", {
      model,
      usage: {
        input_tokens: usage.input,
        output_tokens: usage.output,
        thought_tokens: usage.thinking ?? null,
        cached_tokens: usage.cacheRead ?? null,
        tool_use_tokens: null,
        total_tokens: usage.input !== null && usage.output !== null ? usage.input + usage.output : null,
      },
    }),
  );
  frames.push(xUsage(t, callIndex, usage, { model, provider: opts.provider ?? "anthropic", confirmed: opts.confirmed ?? false }));
  return frames;
}

export const toolRequested = (t: TurnRef, toolId: string, toolName: string, input: Record<string, unknown>) =>
  ev(t, "tool_requested", { tool_id: toolId, tool_name: toolName, tool_input: input });

export const approvalRequested = (t: TurnRef, toolId: string, toolName: string, input: Record<string, unknown>) =>
  ev(t, "tool_approval_requested", { tool_id: toolId, tool_name: toolName, tool_input: input });

export const approvalResolved = (t: TurnRef, toolId: string, toolName: string, approved: boolean, remember = false, reason: string | null = null) =>
  ev(t, "tool_approval_resolved", { tool_id: toolId, tool_name: toolName, approved, reason, remember });

/** A judge evaluation that approves the call, then the resolution the harness publishes for
 *  it (agent_workflow.py: `_resolve_and_publish(tool_id, approved=True, reason=decision.reason)`). */
export function judgeApproves(t: TurnRef, toolId: string, toolName: string, reason: string, confidence = 0.93, criteria = "shell"): Frame[] {
  const evaluation_id = `eval-${toolId}`;
  return [
    ev(t, "auto_approval_evaluation_started", { tool_id: toolId, tool_name: toolName, evaluation_id, evaluator: "llm_judge" }),
    ev(t, "auto_approval_evaluation_ended", {
      tool_id: toolId,
      tool_name: toolName,
      evaluation_id,
      evaluator: "llm_judge",
      verdict: "approve",
      reason: `judge llm: ${reason}`,
      details: { confidence, judge_reason: reason, criteria_set: criteria },
    }),
    approvalResolved(t, toolId, toolName, true, false, `judge llm: ${reason}`),
  ];
}

export const toolStart = (t: TurnRef, toolId: string, toolName: string, input: Record<string, unknown>) =>
  ev(t, "tool_start", { tool_id: toolId, tool_name: toolName, tool_input: input });

export const toolProgress = (t: TurnRef, toolId: string, toolName: string, delta: string) =>
  ev(t, "tool_progress_delta", { tool_id: toolId, tool_name: toolName, progress_delta: delta });

export const toolEnd = (t: TurnRef, toolId: string, toolName: string, output: string) =>
  ev(t, "tool_end", { tool_id: toolId, tool_name: toolName, tool_output: output });

/** A read-only tool that runs without asking. */
export function quickTool(t: TurnRef, toolId: string, toolName: string, input: Record<string, unknown>, output: string): Frame[] {
  return [toolRequested(t, toolId, toolName, input), toolStart(t, toolId, toolName, input), toolEnd(t, toolId, toolName, output)];
}

export const snapshot = (t: TurnRef, stateId: string, version: number, value: unknown) =>
  ev(t, "state_snapshot", { state_id: stateId, version, value }, null);

export const patch = (t: TurnRef, stateId: string, version: number, ops: unknown[]) =>
  ev(t, "state_patch", { state_id: stateId, version, ops }, null);

// -- subagents ------------------------------------------------------------------------------

export interface Sub {
  agentId: string;
  agentKey: string;
  workflowId: string;
}

/** The parent's frames that start a subagent turn (the hidden driving tool plus the subagent row). */
export function subagentAsked(parent: TurnRef, sub: Sub, toolId: string, question: string): Frame[] {
  const input = { question };
  return [
    toolRequested(parent, toolId, "explorer_investigate", input),
    toolStart(parent, toolId, "explorer_investigate", input),
    ev(parent, "subagent_started", { subagent_id: sub.agentId, agent_key: sub.agentKey, workflow_id: sub.workflowId }),
    ev(parent, "subagent_message_sent", {
      subagent_id: sub.agentId,
      agent_key: sub.agentKey,
      workflow_id: sub.workflowId,
      handler: "investigate",
      subagent_turn: 1,
      from_offset: 0,
    }),
  ];
}

export function subagentReplied(parent: TurnRef, sub: Sub, toolId: string, answer: string): Frame[] {
  return [
    ev(parent, "subagent_reply_received", {
      subagent_id: sub.agentId,
      agent_key: sub.agentKey,
      workflow_id: sub.workflowId,
      handler: "investigate",
      subagent_turn: 1,
      outcome: "ok",
    }),
    toolEnd(parent, toolId, "explorer_investigate", answer),
  ];
}

// -- our telemetry (agent_telemetry.py) -------------------------------------------------------

export function xUsage(
  t: TurnRef,
  callIndex: number,
  u: Usage,
  opts: { model?: string; provider?: string | null; confirmed?: boolean; attempt?: number } = {},
): Frame {
  return {
    event: "x_usage",
    data: {
      agent_id: t.agentId,
      turn_id: t.turnId,
      turn_number: t.turnNumber,
      id: `usage:${t.agentId}:${t.turnId}:${callIndex}`,
      message_id: t.messageId,
      call_index: callIndex,
      model: opts.model ?? "claude-sonnet-5-5",
      input_tokens: u.input,
      output_tokens: u.output,
      cache_read_tokens: u.cacheRead ?? null,
      cache_write_tokens: u.cacheWrite ?? null,
      thinking_tokens: u.thinking ?? null,
      confirmed: opts.confirmed ?? false,
      ...(opts.provider ? { provider: opts.provider } : {}),
      ...(opts.attempt !== undefined ? { attempt: opts.attempt } : {}),
      timestamp: now(),
    },
  };
}

export interface RecoveryOpts {
  key: string;
  kind: "model_call" | "tool_call" | "command_reattached";
  confirmed: boolean;
  lostAt: number;
  kept: { model_calls: number; tool_calls: number; tokens_in: number; tokens_out: number };
  repeated: { model_calls: number; tool_calls: number; resent_input_tokens_estimate: number | null };
  attempt?: number;
  toolName?: string | null;
  resumedOn?: string | null;
  lostWorker?: string | null;
  activityId?: string | null;
  callIndex?: number | null;
  cause?: string | null;
}

export function xRecovery(t: TurnRef, o: RecoveryOpts): Frame {
  return {
    event: "x_recovery",
    data: {
      agent_id: t.agentId,
      turn_id: t.turnId,
      turn_number: t.turnNumber,
      id: `recovery:${t.agentId}:${t.turnId}:${o.key}`,
      kind: o.kind,
      call_index: o.callIndex ?? null,
      activity_id: o.activityId ?? null,
      tool_name: o.toolName ?? null,
      attempt: o.attempt ?? 2,
      cause: o.cause ?? "heartbeat_timeout",
      detected_at: o.lostAt + 7,
      lost_at: o.lostAt,
      lost_worker: o.lostWorker ?? "dev-worker-a",
      resumed_on: o.resumedOn ?? "dev-worker-b",
      kept: o.kept,
      repeated: o.repeated,
      confirmed: o.confirmed,
      retracted: false,
      timestamp: now(),
    },
  };
}

export function xWorker(
  state: "ok" | "suspect",
  reason: string,
  opts: { turn?: TurnRef | null; since?: number; pollers?: number | null; workflowId?: string; runId?: string } = {},
): Frame {
  const runs = opts.workflowId && opts.runId ? { [opts.workflowId]: opts.runId } : {};
  return {
    event: "x_worker",
    data: {
      id: "worker",
      workflow_id: opts.workflowId ?? null,
      run_id: opts.workflowId && opts.runId ? opts.runId : null,
      runs,
      agent_id: opts.turn?.agentId ?? ROOT,
      turn_id: opts.turn?.turnId ?? null,
      turn_number: opts.turn?.turnNumber ?? null,
      state,
      reason,
      since: opts.since ?? now(),
      pollers: opts.pollers ?? null,
      timestamp: now(),
    },
  };
}

export function xRejected(requestId: string, o: { kind: "approval" | "message"; toolId?: string; messageId?: string; code: string; reason: string }): Frame {
  return {
    event: "x_rejected",
    data: {
      id: `rejected:${requestId}`,
      agent_id: ROOT,
      turn_id: null,
      turn_number: null,
      kind: o.kind,
      request_id: requestId,
      tool_id: o.toolId ?? null,
      message_id: o.messageId ?? null,
      code: o.code,
      reason: o.reason,
      error_type: null,
      timestamp: now(),
    },
  };
}
