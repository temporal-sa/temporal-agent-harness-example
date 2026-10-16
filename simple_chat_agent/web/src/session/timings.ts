// When each model call ran, from the stream's own timestamps (Unix seconds on every harness
// event). The harness projection keeps parts but not their times; "Thought for 12s" needs
// them. Pure and incremental, like TelemetryFolder.

import type { ModelCallTiming } from "./types";
import type { RawFrame } from "./telemetry";

type Obj = Record<string, unknown>;
const isObj = (v: unknown): v is Obj => typeof v === "object" && v !== null && !Array.isArray(v);

export const timingKey = (agentId: string, messageId: string) => `${agentId}|${messageId}`;

export class TimingFolder {
  #calls = new Map<string, ModelCallTiming[]>();
  #seen = 0;
  #first: RawFrame | undefined;

  update(frames: readonly RawFrame[]): ReadonlyMap<string, ModelCallTiming[]> {
    if (frames.length < this.#seen || (this.#first !== undefined && frames[0] !== this.#first)) {
      this.#calls = new Map();
      this.#seen = 0;
    }
    this.#first = frames[0];
    for (let i = this.#seen; i < frames.length; i++) {
      const f = frames[i];
      if (f) this.#fold(f);
    }
    this.#seen = frames.length;
    return this.#calls;
  }

  #fold(frame: RawFrame): void {
    const d = isObj(frame.data) ? frame.data : null;
    if (!d || typeof d.agent_id !== "string" || typeof d.message_id !== "string") return;
    const ts = typeof d.timestamp === "number" ? Math.round(d.timestamp * 1000) : null;
    if (ts === null) return;
    const key = timingKey(d.agent_id, d.message_id);
    const list = this.#calls.get(key);
    const last = list?.[list.length - 1];
    switch (frame.event) {
      case "model_interaction_started": {
        const t: ModelCallTiming = { startedAt: ts, firstThoughtAt: null, firstOutputAt: null, endedAt: null };
        if (list) list.push(t);
        else this.#calls.set(key, [t]);
        return;
      }
      case "thought_summary":
        if (last && last.firstThoughtAt === null) last.firstThoughtAt = ts;
        return;
      case "reply_delta":
      case "tool_requested":
        if (last && last.firstOutputAt === null) last.firstOutputAt = ts;
        return;
      case "model_interaction_ended":
        if (last) last.endedAt = ts;
        return;
    }
  }
}

/** Seconds a call spent before its first output: what "Thought for Ns" reports. */
export function thinkingSeconds(t: ModelCallTiming | undefined): number | null {
  if (!t) return null;
  const end = t.firstOutputAt ?? t.endedAt;
  if (end === null) return null;
  return Math.max(0, Math.round((end - t.startedAt) / 1000));
}
