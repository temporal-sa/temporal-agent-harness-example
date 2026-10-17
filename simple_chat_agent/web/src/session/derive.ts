// Facts the facade derives from the harness's view of the stream. Pure, so it is testable
// without React or a connection.

import type {
  AgentNode,
  Evaluation,
  Message,
  ModelChoice,
  PendingApproval,
  Posture,
  SessionStatus,
  ToolPart,
  WorkerHealth,
} from "./types";

const POSTURES: ReadonlySet<string> = new Set(["ask_everything", "trust_safe", "auto", "never_ask"]);

function isObj(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}

/** The agent's SettingsAck for a finished set_model / set_posture, if it has one. */
export function settingsAck(m: Pick<Message, "output" | "status">): { ok: boolean; detail: string; model: unknown; posture: unknown } | null {
  if (m.status !== "done" || !isObj(m.output)) return null;
  const o = m.output;
  return typeof o.ok === "boolean" ? { ok: o.ok, detail: typeof o.detail === "string" ? o.detail : "", model: o.model, posture: o.posture } : null;
}

/** A control message counts once the agent admitted it, unless it failed or the agent
 *  refused it (SettingsAck ok=false: the setting stays as it was). */
function admitted(m: Message): boolean {
  return m.acceptedAt !== null && m.status !== "error" && settingsAck(m)?.ok !== false;
}

/** The model the next turn uses: the session's start model, then each admitted set_model. */
export function currentModel(messages: readonly Message[], seed: ModelChoice | null): ModelChoice | null {
  let model = seed;
  for (const m of messages) {
    if (m.handler !== "set_model" || !admitted(m)) continue;
    // The value the agent applied (its reply), else what was asked for while it's pending.
    const applied = settingsAck(m)?.model;
    const input = m.input as unknown;
    const choice = isObj(applied) ? applied : isObj(input) && isObj(input.model) ? input.model : null;
    if (choice && typeof choice.provider === "string" && typeof choice.model === "string") {
      model = {
        provider: choice.provider as ModelChoice["provider"],
        model: choice.model,
        effort: (choice.effort as ModelChoice["effort"]) ?? null,
      };
    }
  }
  return model;
}

/** Whether the latest set_model was admitted into the turn that is still open, so it applies
 *  from the next one. */
export function modelAppliesNextTurn(messages: readonly Message[], agentStatus: "idle" | "busy"): boolean {
  if (agentStatus !== "busy") return false;
  const openTurn = Math.max(0, ...messages.filter((m) => m.handler === "chat" && m.turnNumber !== null).map((m) => m.turnNumber ?? 0));
  for (let i = messages.length - 1; i >= 0; i--) {
    const m = messages[i];
    if (!m || m.handler !== "set_model") continue;
    if (!admitted(m)) return false;
    return m.turnNumber !== null && m.turnNumber >= openTurn;
  }
  return false;
}

/** The model the open turn runs on: while a model change waits for the next turn, the one
 *  before it; otherwise the current one. */
export function modelInUse(messages: readonly Message[], seed: ModelChoice | null, agentStatus: "idle" | "busy"): ModelChoice | null {
  if (!modelAppliesNextTurn(messages, agentStatus)) return currentModel(messages, seed);
  let last = -1;
  for (let i = messages.length - 1; i >= 0; i--) {
    if (messages[i]?.handler === "set_model") {
      last = i;
      break;
    }
  }
  return currentModel(messages.slice(0, Math.max(0, last)), seed);
}

export function currentPosture(messages: readonly Message[], seed: Posture | null): Posture | null {
  let posture = seed;
  for (const m of messages) {
    if (m.handler !== "set_posture" || !admitted(m)) continue;
    const applied = settingsAck(m)?.posture;
    const input = m.input as unknown;
    const p = typeof applied === "string" ? applied : isObj(input) ? input.posture : null;
    if (typeof p === "string" && POSTURES.has(p)) posture = p as Posture;
  }
  return posture;
}

function* toolParts(agents: Readonly<Record<string, AgentNode>>): Generator<ToolPart> {
  for (const agent of Object.values(agents)) {
    for (const m of agent.messages) {
      for (const p of m.parts) if (p.type === "tool") yield p;
    }
  }
}

/** Every tool part in the tree by tool id. */
export function toolIndex(agents: Readonly<Record<string, AgentNode>>): Map<string, ToolPart> {
  const index = new Map<string, ToolPart>();
  for (const p of toolParts(agents)) index.set(p.toolId, p);
  return index;
}

/** The harness projection files frames without an agent id (an `x_worker` frame outside a
 *  turn) under a "null" agent. Drop it and the root. */
export function subagentsOf(
  agents: Readonly<Record<string, AgentNode>>,
  rootAgentId: string | null,
): Record<string, AgentNode> {
  const out: Record<string, AgentNode> = {};
  for (const [id, view] of Object.entries(agents)) {
    if (!id || id === "null" || id === "undefined" || id === rootAgentId) continue;
    if (view.parentId === null && view.messages.length === 0) continue;
    out[id] = view;
  }
  return out;
}

export function sessionStatus(input: {
  pendingApprovals: readonly PendingApproval[];
  /** ask_user / pick_files calls waiting on the person. */
  pendingCallbacks?: number;
  anyBusy: boolean;
  worker: WorkerHealth;
  waitingForWorker: boolean;
}): SessionStatus {
  // Calls the judge is still deciding don't wait on you yet: the turn is running.
  if (input.pendingApprovals.some((w) => !w.judging) || (input.pendingCallbacks ?? 0) > 0) return "waiting_for_you";
  if (input.pendingApprovals.length > 0) return "running";
  if ((input.anyBusy && !input.worker.answering) || input.waitingForWorker) return "worker_unresponsive";
  if (input.anyBusy) return "running";
  return "idle";
}

/** Tag each waiting call with its approval class from config, and whether "Always approve"
 *  can apply to it. Neither is hard-coded: both come from /api/agent/config. */
export function withRiskClass<T extends { part: { toolName: string } }>(
  approvals: readonly T[],
  classes: Readonly<Record<string, string>> | null | undefined,
  fallback: string | null | undefined,
  rememberAllowed: readonly string[] | null | undefined,
): (T & { riskClass: string | null; rememberAllowed: boolean })[] {
  return approvals.map((w) => {
    const riskClass = classes?.[w.part.toolName] ?? fallback ?? null;
    return { ...w, riskClass, rememberAllowed: riskClass !== null && (rememberAllowed ?? []).includes(riskClass) };
  });
}

/** Root messages without the workflow's rejection carriers ("x_rejected:<request_id>"). */
export function visibleMessages<T extends Pick<Message, "id">>(messages: readonly T[]): readonly T[] {
  return messages.some((m) => m.id.startsWith("x_rejected:")) ? messages.filter((m) => !m.id.startsWith("x_rejected:")) : messages;
}

/**
 * Tools the user approved with "Always approve" in the root agent, as of the latest frame.
 * Folded in stream order from tool_approval_resolved (approved and remembered; the agent
 * narrows `remember` before it resolves, so the frame says what actually stuck). An admitted
 * forget_always_allowed message clears the list.
 */
export function alwaysApprovedFrom(frames: readonly { event: string; data: unknown }[], rootAgentId: string | null): string[] {
  const names = new Set<string>();
  for (const f of frames) {
    const d = isObj(f.data) ? f.data : null;
    if (!d || (rootAgentId !== null && d.agent_id !== rootAgentId)) continue;
    if (f.event === "tool_approval_resolved" && d.approved === true && d.remember === true && typeof d.tool_name === "string") {
      names.add(d.tool_name);
    } else if (f.event === "message_accepted" && d.handler === FORGET_HANDLER) {
      names.clear();
    }
  }
  return [...names].sort();
}

export const FORGET_HANDLER = "forget_always_allowed";

/** The chat payload's attachments (contracts.ChatAttachment) from upload results; at most 20. */
export const MAX_ATTACHMENTS = 20;
export function toChatAttachments(
  uploads: readonly { attachment_id: string; name?: string | null; size_bytes?: number | null; mime_type?: string | null }[],
): { attachment_id: string; name?: string; size?: number | null; content_type?: string | null }[] {
  return uploads.slice(0, MAX_ATTACHMENTS).map((u) => ({
    attachment_id: u.attachment_id,
    ...(u.name ? { name: u.name } : {}),
    ...(u.size_bytes !== undefined ? { size: u.size_bytes } : {}),
    ...(u.mime_type !== undefined ? { content_type: u.mime_type } : {}),
  }));
}

/** Whether the judge is deciding a waiting call, and its latest finished evaluation. */
export function judgeState(part: Pick<ToolPart, "evaluations">): { judging: boolean; evaluation: Evaluation | null } {
  const evals = part.evaluations ?? [];
  const latest = evals[evals.length - 1];
  let evaluation: Evaluation | null = null;
  for (let i = evals.length - 1; i >= 0; i--) {
    if (evals[i]!.status !== "running") {
      evaluation = evals[i]!;
      break;
    }
  }
  return { judging: latest?.status === "running", evaluation };
}

/** Tag each waiting call with the judge's state. */
export function withJudging<T extends { part: Pick<ToolPart, "evaluations"> }>(approvals: readonly T[]): (T & { judging: boolean; evaluation: Evaluation | null })[] {
  return approvals.map((w) => ({ ...w, ...judgeState(w.part) }));
}
