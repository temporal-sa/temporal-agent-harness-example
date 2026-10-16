// Folds our bridge's telemetry frames (x_usage, x_recovery, x_worker, x_rejected) out of the
// session's SSE stream (simple_chat_agent/api/agent_telemetry.py). Pure: no React, no harness.
//
// Rules carried over from the bridge:
// * Every frame has an `id`; the latest frame with a given id wins (a confirmed frame replaces
//   the live one).
// * A count the provider didn't report is null. Null is "not reported", never 0, and never
//   added as 0: a total is null when no call reports that count.
// * Retracted recoveries (not a lost worker after all) are kept but never counted or shown.

import type {
  ModelCallUsage,
  Recovery,
  RecoveryTotals,
  Rejection,
  SessionTelemetry,
  TurnRecovery,
  UsageTotals,
  WorkCounts,
  WorkerHealth,
} from "./types";

/** The harness's own event types (client protocol.ts). Anything else not ours is unknown. */
export const HARNESS_EVENTS: ReadonlySet<string> = new Set([
  "auto_approval_evaluation_ended",
  "auto_approval_evaluation_error",
  "auto_approval_evaluation_started",
  "auto_approval_evaluation_superseded",
  "callback_requested",
  "callback_resolved",
  "message_accepted",
  "message_handler_end",
  "message_handler_error",
  "message_handler_start",
  "model_interaction_ended",
  "model_interaction_started",
  "reply_delta",
  "state_patch",
  "state_snapshot",
  "subagent_message_sent",
  "subagent_reply_received",
  "subagent_started",
  "subagent_stopped",
  "subagent_stream_unavailable",
  "text_annotation",
  "thought_summary",
  "tool_approval_requested",
  "tool_approval_resolved",
  "tool_end",
  "tool_error",
  "tool_progress_delta",
  "tool_requested",
  "tool_start",
  "turn_end",
  "turn_started",
  "stream_error",
]);

export const X_USAGE = "x_usage";
export const X_RECOVERY = "x_recovery";
export const X_WORKER = "x_worker";
export const X_REJECTED = "x_rejected";


export interface RawFrame {
  event: string;
  data: unknown;
}

/** Mutable accumulator; `foldFrames` appends to it, `summarize` reads it. */
export interface TelemetryAcc {
  usage: Map<string, ModelCallUsage>;
  recoveries: Map<string, Recovery>;
  worker: WorkerHealth;
  rejections: Map<string, Rejection>;
  runIds: Record<string, string>;
  runIdsByWorkflow: Record<string, string>;
  unknown: Record<string, number>;
}

export const HEALTHY: WorkerHealth = {
  answering: true,
  reason: null,
  since: null,
  pollers: null,
  agentId: null,
  turnId: null,
};

export function emptyAcc(): TelemetryAcc {
  return { usage: new Map(), recoveries: new Map(), worker: HEALTHY, rejections: new Map(), runIds: {}, runIdsByWorkflow: {}, unknown: {} };
}

// -- field readers (frames are untrusted JSON) ---------------------------------------------

type Obj = Record<string, unknown>;
const isObj = (v: unknown): v is Obj => typeof v === "object" && v !== null && !Array.isArray(v);
const num = (v: unknown): number | null => (typeof v === "number" && Number.isFinite(v) ? v : null);
const str = (v: unknown): string | null => (typeof v === "string" && v !== "" ? v : null);
const int0 = (v: unknown): number => num(v) ?? 0;
/** Bridge timestamps are Unix seconds; the UI uses ms. */
const ms = (v: unknown): number | null => {
  const s = num(v);
  return s === null ? null : Math.round(s * 1000);
};

function parseUsage(d: Obj): ModelCallUsage | null {
  const id = str(d.id);
  if (!id) return null;
  return {
    id,
    agentId: str(d.agent_id),
    turnId: str(d.turn_id),
    turnNumber: num(d.turn_number),
    messageId: str(d.message_id),
    callIndex: num(d.call_index),
    model: str(d.model),
    provider: str(d.provider),
    attempt: num(d.attempt),
    input: num(d.input_tokens),
    output: num(d.output_tokens),
    cacheRead: num(d.cache_read_tokens),
    cacheWrite: num(d.cache_write_tokens),
    thinking: num(d.thinking_tokens),
    confirmed: d.confirmed === true,
  };
}

function parseRecovery(d: Obj): Recovery | null {
  const id = str(d.id);
  if (!id) return null;
  const kept = isObj(d.kept) ? d.kept : {};
  const repeated = isObj(d.repeated) ? d.repeated : {};
  return {
    id,
    agentId: str(d.agent_id),
    turnId: str(d.turn_id),
    turnNumber: num(d.turn_number),
    kind: str(d.kind) ?? "unknown",
    callIndex: num(d.call_index),
    activityId: str(d.activity_id),
    toolName: str(d.tool_name),
    attempt: num(d.attempt),
    cause: str(d.cause),
    detectedAt: ms(d.detected_at),
    lostAt: ms(d.lost_at),
    lostWorker: str(d.lost_worker),
    resumedOn: str(d.resumed_on),
    kept: {
      modelCalls: int0(kept.model_calls),
      toolCalls: int0(kept.tool_calls),
      tokensIn: int0(kept.tokens_in),
      tokensOut: int0(kept.tokens_out),
    },
    repeated: {
      modelCalls: int0(repeated.model_calls),
      toolCalls: int0(repeated.tool_calls),
      resentInputTokensEstimate: num(repeated.resent_input_tokens_estimate),
    },
    confirmed: d.confirmed === true,
    retracted: d.retracted === true,
  };
}

function parseWorker(d: Obj): WorkerHealth {
  return {
    answering: d.state !== "suspect",
    reason: str(d.reason),
    since: ms(d.since),
    pollers: num(d.pollers),
    agentId: str(d.agent_id),
    turnId: str(d.turn_id),
  };
}

/**
 * Fold frames into `acc`. Returns whether anything telemetry-related changed. Harness events
 * are skipped (the harness projection owns them); unknown events are counted and ignored, and
 * a malformed telemetry frame is ignored rather than thrown on.
 */
export function foldFrames(acc: TelemetryAcc, frames: readonly RawFrame[]): boolean {
  let changed = false;
  for (const frame of frames) {
    const event = frame.event;
    const d = isObj(frame.data) ? frame.data : null;
    // Any frame may carry its agent's run id (the bridge adds it); the latest wins.
    const runId = d && str(d.run_id);
    const agentId = d && str(d.agent_id);
    if (runId && agentId && acc.runIds[agentId] !== runId) {
      acc.runIds = { ...acc.runIds, [agentId]: runId };
      changed = true;
    }
    if (HARNESS_EVENTS.has(event)) continue;
    switch (event) {
      case X_USAGE: {
        const u = d && parseUsage(d);
        if (!u) break;
        const prev = acc.usage.get(u.id);
        // A late live frame never downgrades a confirmed one.
        if (prev?.confirmed && !u.confirmed) break;
        acc.usage.set(u.id, u);
        changed = true;
        break;
      }
      case X_RECOVERY: {
        const r = d && parseRecovery(d);
        if (!r) break;
        const prev = acc.recoveries.get(r.id);
        if (prev?.confirmed && !r.confirmed) break;
        acc.recoveries.set(r.id, r);
        changed = true;
        break;
      }
      case X_WORKER: {
        if (!d) break;
        const runs: Record<string, string> = {};
        if (isObj(d.runs)) for (const [w, r] of Object.entries(d.runs)) if (typeof r === "string" && r) runs[w] = r;
        const wf = str(d.workflow_id);
        if (wf && runId) runs[wf] = runId;
        if (Object.entries(runs).some(([w, r]) => acc.runIdsByWorkflow[w] !== r)) acc.runIdsByWorkflow = { ...acc.runIdsByWorkflow, ...runs };
        acc.worker = parseWorker(d);
        changed = true;
        break;
      }
      case X_REJECTED: {
        const requestId = d && (str(d.request_id) ?? str(d.message_id) ?? str(d.id));
        if (!d || !requestId) break;
        const code = str(d.code) ?? str(d.type);
        const kind = d.kind === "approval" || d.kind === "message" ? d.kind : null;
        acc.rejections.set(requestId, {
          requestId,
          kind,
          toolId: str(d.tool_id),
          messageId: str(d.message_id),
          code,
          type: code,
          message: str(d.reason) ?? str(d.message) ?? "The agent refused this request.",
        });
        changed = true;
        break;
      }
      default:
        acc.unknown[event] = (acc.unknown[event] ?? 0) + 1;
    }
  }
  return changed;
}

// -- summaries -------------------------------------------------------------------------------

/** Sum a nullable count over calls: null when no call reports it. */
function sumReported(calls: readonly ModelCallUsage[], pick: (c: ModelCallUsage) => number | null): number | null {
  let total: number | null = null;
  for (const c of calls) {
    const v = pick(c);
    if (v !== null) total = (total ?? 0) + v;
  }
  return total;
}

export function usageTotals(calls: readonly ModelCallUsage[]): UsageTotals {
  // The cache share only counts calls that report both their input and their cache reads, so
  // a provider that doesn't report cache reads doesn't dilute the figure toward 0.
  let shareIn = 0;
  let shareRead = 0;
  for (const c of calls) {
    if (c.input !== null && c.cacheRead !== null) {
      shareIn += c.input;
      shareRead += c.cacheRead;
    }
  }
  return {
    calls: calls.length,
    confirmed: calls.every((c) => c.confirmed),
    input: sumReported(calls, (c) => c.input),
    output: sumReported(calls, (c) => c.output),
    cacheRead: sumReported(calls, (c) => c.cacheRead),
    cacheWrite: sumReported(calls, (c) => c.cacheWrite),
    thinking: sumReported(calls, (c) => c.thinking),
    cacheShare: shareIn > 0 ? shareRead / shareIn : null,
  };
}

const ZERO: WorkCounts = { modelCalls: 0, toolCalls: 0, tokensIn: 0, tokensOut: 0 };
const keptSize = (k: WorkCounts) => k.modelCalls + k.toolCalls;

/**
 * Group recoveries by turn. A turn's `kept` is the largest any of its recoveries reports:
 * kept counts are cumulative within a turn (work finished before the failed attempt), so
 * adding a model recovery and a tool recovery from the same crash would count it twice.
 */
export function groupByTurn(recoveries: readonly Recovery[]): TurnRecovery[] {
  const turns = new Map<string, TurnRecovery>();
  for (const r of recoveries) {
    if (r.retracted) continue;
    const key = `${r.agentId ?? ""}|${r.turnId ?? r.id}`;
    let t = turns.get(key);
    if (!t) {
      t = {
        agentId: r.agentId,
        turnId: r.turnId,
        turnNumber: r.turnNumber,
        recoveries: [],
        kept: ZERO,
        repeatedModelCalls: 0,
        repeatedToolCalls: 0,
        lostAt: null,
        confirmed: true,
      };
      turns.set(key, t);
    }
    t.recoveries.push(r);
    if (keptSize(r.kept) > keptSize(t.kept) || (keptSize(r.kept) === keptSize(t.kept) && r.kept.tokensIn > t.kept.tokensIn)) {
      t.kept = r.kept;
    }
    t.repeatedModelCalls += r.repeated.modelCalls;
    t.repeatedToolCalls += r.repeated.toolCalls;
    const lost = r.lostAt ?? r.detectedAt;
    if (lost !== null && (t.lostAt === null || lost < t.lostAt)) t.lostAt = lost;
    t.confirmed = t.confirmed && r.confirmed;
  }
  return [...turns.values()];
}

export function recoveryTotals(turns: readonly TurnRecovery[]): RecoveryTotals {
  const kept = { ...ZERO };
  for (const t of turns) {
    kept.modelCalls += t.kept.modelCalls;
    kept.toolCalls += t.kept.toolCalls;
    kept.tokensIn += t.kept.tokensIn;
    kept.tokensOut += t.kept.tokensOut;
  }
  return { count: turns.length, kept, confirmed: turns.every((t) => t.confirmed) };
}

export function summarize(acc: TelemetryAcc): SessionTelemetry {
  const calls = [...acc.usage.values()];
  const byProviderCalls = new Map<string, ModelCallUsage[]>();
  for (const c of calls) {
    const p = c.provider ?? "unknown";
    const list = byProviderCalls.get(p);
    if (list) list.push(c);
    else byProviderCalls.set(p, [c]);
  }
  const byProvider: Record<string, UsageTotals> = {};
  for (const [p, list] of byProviderCalls) byProvider[p] = usageTotals(list);
  const recoveries = [...acc.recoveries.values()];
  const turnRecoveries = groupByTurn(recoveries);
  return {
    calls,
    totals: usageTotals(calls),
    byProvider,
    recoveries,
    turnRecoveries,
    recoveryTotals: recoveryTotals(turnRecoveries),
    worker: acc.worker,
    runIds: acc.runIds,
    runIdsByWorkflow: acc.runIdsByWorkflow,
    rejections: [...acc.rejections.values()],
    unknownFrames: { ...acc.unknown },
  };
}

/**
 * Incremental folding over the harness's growing `frames` array: only new frames are folded,
 * and the summary object is reused while nothing changed (stable for React memoization).
 */
export class TelemetryFolder {
  #acc = emptyAcc();
  #seen = 0;
  #source: readonly RawFrame[] | null = null;
  #summary: SessionTelemetry = summarize(this.#acc);

  update(frames: readonly RawFrame[]): SessionTelemetry {
    // The harness only appends; a shorter array means a new session or a reset.
    if (frames.length < this.#seen || (this.#source !== null && frames.length > 0 && this.#source.length > 0 && frames[0] !== this.#source[0])) {
      this.reset();
    }
    this.#source = frames;
    if (frames.length === this.#seen) return this.#summary;
    const changed = foldFrames(this.#acc, frames.slice(this.#seen));
    this.#seen = frames.length;
    if (changed) this.#summary = summarize(this.#acc);
    else if (Object.keys(this.#acc.unknown).length > 0) this.#summary = { ...this.#summary, unknownFrames: { ...this.#acc.unknown } };
    return this.#summary;
  }

  reset(): void {
    this.#acc = emptyAcc();
    this.#seen = 0;
    this.#source = null;
    this.#summary = summarize(this.#acc);
  }
}
