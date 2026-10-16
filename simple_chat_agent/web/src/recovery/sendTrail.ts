// Send-state chips (PRD U4): Sending → In Temporal → Accepted → Running. Pure.

import { errorCopy, type ErrorCopy, type SendPhase, type SendRecord } from "../session";

export type StepState = "done" | "now" | "todo";

export interface TrailStep {
  phase: SendPhase;
  label: string;
  state: StepState;
}

const ORDER: SendPhase[] = ["sending", "in_temporal", "accepted", "running"];

const LABEL: Record<string, string> = {
  sending: "Sending",
  in_temporal: "In Temporal",
  accepted: "Accepted",
  running: "Running",
};

/** After this long in Temporal with no worker taking it, say so. */
export const WAITING_FOR_WORKER_MS = 2000;

export function trailSteps(
  record: Pick<SendRecord, "kind" | "approved" | "phase" | "at">,
  opts: { now?: number; workerAnswering?: boolean } = {},
): TrailStep[] {
  // A decline never runs anything: it ends once the agent accepts it.
  const phases = record.kind === "approval" && record.approved === false ? ORDER.slice(0, 3) : ORDER;
  const at = record.phase === "done" ? phases.length : phases.indexOf(record.phase);
  return phases.map((phase, i) => {
    const state: StepState = i < at ? "done" : i === at ? "now" : "todo";
    let label = LABEL[phase] ?? phase;
    if (phase === "in_temporal" && state === "now") {
      const since = record.at.in_temporal ?? null;
      const slow = since !== null && opts.now !== undefined && opts.now - since >= WAITING_FOR_WORKER_MS;
      if (opts.workerAnswering === false || slow) label = "In Temporal · waiting for a Worker";
    }
    return { phase, label, state };
  });
}

/** Copy for a failed send: plain words from the facade's `errorCopy`, the raw server or
 *  agent text kept apart as `detail` (for a "Details" disclosure). */
export function failedCopy(record: Pick<SendRecord, "phase" | "error" | "errorCode" | "errorStatus">): ErrorCopy | null {
  if (record.phase !== "failed") return null;
  const copy = errorCopy({ code: record.errorCode ?? null, status: record.errorStatus ?? null, message: record.error }, "Check your connection and try again.");
  return { text: `Not sent. ${copy.text}`, detail: copy.detail };
}
