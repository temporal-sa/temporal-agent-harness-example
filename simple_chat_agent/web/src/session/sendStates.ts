// The send-state machine for local actions (PRD U4): Sending → In Temporal (waiting for a
// worker) → Accepted → Running → done, or failed. Pure: the facade feeds it clicks, API
// replies and what the stream shows; tests feed it the same.
//
// Where each phase comes from:
// * sending      the click.
// * in_temporal  the API's 2xx: the Signal is in workflow history (PRD S8). For a subagent's
//                approval the API waits for the Update, which means a worker already took it.
// * accepted     message: its `message_accepted` arrived (the harness sets `acceptedAt` only
//                from that frame; the 2xx alone leaves it null). approval: the tool call left
//                `awaiting_approval` / `evaluating`, or carries the decision.
// * running      message: `message_handler_start` (`startedAt`). approval: the tool started.
// * done         message: the handler returned. approval: the tool ended or was denied.
// * failed       the request failed, the handler raised, or the agent rejected it.

import type { Message, Rejection, SendKind, SendPhase, SendRecord, ToolPart } from "./types";

/** The approval was already answered (another tab, Approve all, the judge): not an error. */
export const ALREADY_RESOLVED = "already_resolved";

const ORDER: Record<SendPhase, number> = {
  sending: 0,
  in_temporal: 1,
  accepted: 2,
  running: 3,
  done: 4,
  failed: 5,
};
const FORWARD: SendPhase[] = ["sending", "in_temporal", "accepted", "running", "done"];

export function isSettled(phase: SendPhase): boolean {
  return phase === "done" || phase === "failed";
}

export function startRecord(
  id: string,
  kind: SendKind,
  now: number,
  extra: Partial<Pick<SendRecord, "toolId" | "approved" | "remember">> = {},
): SendRecord {
  return {
    id,
    kind,
    messageId: null,
    toolId: extra.toolId ?? null,
    approved: extra.approved ?? null,
    remember: extra.remember ?? false,
    phase: "sending",
    at: { sending: now },
    error: null,
    errorCode: null,
    errorStatus: null,
  };
}

/**
 * Move a record forward to `phase`, stamping every phase it skips with the same time. Never
 * moves backward, and a settled record (done or failed) doesn't change.
 */
export function advance(
  record: SendRecord,
  phase: SendPhase,
  now: number,
  error: string | null = null,
  why: { code?: string | null; status?: number | null } = {},
): SendRecord {
  if (isSettled(record.phase)) return record;
  if (phase === "failed") {
    return {
      ...record,
      phase,
      at: { ...record.at, failed: now },
      error: error ?? "The request failed.",
      errorCode: why.code ?? null,
      errorStatus: why.status ?? null,
    };
  }
  if (ORDER[phase] <= ORDER[record.phase]) return record;
  const at = { ...record.at };
  for (const p of FORWARD) {
    if (ORDER[p] > ORDER[record.phase] && ORDER[p] <= ORDER[phase] && at[p] === undefined) at[p] = now;
  }
  return { ...record, phase, at };
}

/** The API returned: the request is in Temporal. */
export function landed(record: SendRecord, now: number, messageId: string | null = null): SendRecord {
  const next = advance(record, "in_temporal", now);
  return messageId && next.messageId === null ? { ...next, messageId } : next;
}

/** The phase a message's stream state implies, or null if the stream hasn't shown it yet. */
export function messagePhase(message: Pick<Message, "status" | "acceptedAt" | "startedAt" | "error">): SendPhase | null {
  if (message.status === "error") return "failed";
  if (message.status === "done") return "done";
  if (message.status === "running" || message.startedAt !== null) return "running";
  // `status: "accepted"` alone is what the client sets when the POST returns; only the
  // stream's message_accepted fills in acceptedAt.
  if (message.acceptedAt !== null) return "accepted";
  return null;
}

/** The phase an approval's tool part implies, or null if it is still waiting. */
export function approvalPhase(part: Pick<ToolPart, "state" | "approval">): SendPhase | null {
  switch (part.state) {
    case "awaiting_approval":
    case "evaluating":
    case "requested":
      return part.approval ? "accepted" : null;
    case "approved":
      return "accepted";
    case "running":
    case "awaiting_callback":
      return "running";
    case "denied":
    case "done":
    case "failed":
      return "done";
    default:
      return null;
  }
}

export interface StreamView {
  /** Root messages by id (the harness's admitted messages). */
  message(id: string): Message | undefined;
  /** A tool part anywhere in the tree by tool id. */
  tool(toolId: string): ToolPart | undefined;
  /** A rejection (x_rejected) for a message's request id. */
  rejection(requestId: string): Rejection | undefined;
  /** A rejection of an approval, by the tool call it answered. */
  approvalRejection?(toolId: string): Rejection | undefined;
}

/** Advance every unsettled record from what the stream now shows. Returns the same array when
 *  nothing changed. */
export function observe(records: readonly SendRecord[], view: StreamView, now: number): readonly SendRecord[] {
  let changed = false;
  const next = records.map((r) => {
    if (isSettled(r.phase) || r.phase === "sending") return r;
    let updated = r;
    if (r.kind === "approval") {
      const part = r.toolId ? view.tool(r.toolId) : undefined;
      const phase = part ? approvalPhase(part) : null;
      const rejected = r.toolId ? view.approvalRejection?.(r.toolId) : undefined;
      if (rejected && rejected.code === ALREADY_RESOLVED) updated = advance(r, "done", now);
      else if (rejected && !phase) updated = advance(r, "failed", now, rejected.message, { code: rejected.code });
      else if (phase) updated = advance(r, phase, now);
    } else if (r.messageId) {
      const rejected = view.rejection(r.messageId);
      if (rejected) {
        updated = advance(r, "failed", now, rejected.message, { code: rejected.code });
      } else {
        const m = view.message(r.messageId);
        const phase = m ? messagePhase(m) : null;
        if (phase === "failed") updated = advance(r, "failed", now, m?.error ?? null);
        else if (phase) updated = advance(r, phase, now);
      }
    }
    if (updated !== r) changed = true;
    return updated;
  });
  return changed ? next : records;
}

/** How long a request may sit in Temporal before the header says no worker is answering. */
export const WAITING_FOR_WORKER_MS = 5_000;

export function stuckInTemporal(records: readonly SendRecord[], now: number): boolean {
  return records.some((r) => r.phase === "in_temporal" && now - (r.at.in_temporal ?? now) >= WAITING_FOR_WORKER_MS);
}
