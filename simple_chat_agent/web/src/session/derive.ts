// Facts the facade derives from the harness's view of the stream. Pure, so it is testable
// without React or a connection.

import type {
  AgentNode,
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

/** A control message counts once the agent admitted it (it may still be running). */
function admitted(m: Message): boolean {
  return m.acceptedAt !== null && m.status !== "error";
}

/** The model the next turn uses: the session's start model, then each admitted set_model. */
export function currentModel(messages: readonly Message[], seed: ModelChoice | null): ModelChoice | null {
  let model = seed;
  for (const m of messages) {
    if (m.handler !== "set_model" || !admitted(m)) continue;
    const input = m.input as unknown;
    const choice = isObj(input) && isObj(input.model) ? input.model : null;
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

export function currentPosture(messages: readonly Message[], seed: Posture | null): Posture | null {
  let posture = seed;
  for (const m of messages) {
    if (m.handler !== "set_posture" || !admitted(m)) continue;
    const input = m.input as unknown;
    const p = isObj(input) ? input.posture : null;
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

/** Tool names the user approved with "Always approve" this session. The agent narrows
 *  `remember` to workspace edits, so only resolved approvals that kept it count. */
export function alwaysApproved(agents: Readonly<Record<string, AgentNode>>): string[] {
  const names = new Set<string>();
  for (const p of toolParts(agents)) {
    if (p.approval?.approved && p.approval.remember) names.add(p.toolName);
  }
  return [...names].sort();
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
  anyBusy: boolean;
  worker: WorkerHealth;
  waitingForWorker: boolean;
}): SessionStatus {
  if (input.pendingApprovals.length > 0) return "waiting_for_you";
  if ((input.anyBusy && !input.worker.answering) || input.waitingForWorker) return "worker_unresponsive";
  if (input.anyBusy) return "running";
  return "idle";
}

/** Tag each waiting call with its approval class from config, and whether "Always approve"
 *  can apply to it. Neither is hard-coded: both come from /api/agent/config. */
export function withRiskClass<T extends PendingApproval>(
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
