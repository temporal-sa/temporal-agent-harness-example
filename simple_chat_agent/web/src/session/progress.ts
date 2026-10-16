// Live tool output and sandbox notices, folded from tool_progress_delta frames. Pure and
// incremental.
//
// The harness event has one free-text field, `progress_delta`, so run_command tags each chunk
// inside it (format: top of simple_chat_agent/agent/tools/workspace.py):
// * "\x1e" + {"v":1,"type":"output","seq":n,"chunks":[{"stream":"stdout"|"stderr"|"note","text":…}]}
//   `seq` only grows within a run, across a reattach too: a frame whose seq was already shown
//   is a duplicate and is dropped.
// * "\x1e" + {"v":1,"type":"sandbox","state":"down"|"up",…}: the executor isn't answering (or
//   is again). Never output: it feeds `sandbox`.
// * Anything not starting with "\x1e" is legacy untagged output: stdout.
// A tagged frame we can't read is dropped rather than shown as text.

import type { RawFrame } from "./telemetry";
import type { SandboxHealth, ToolProgressChunk } from "./types";

export const RECORD_SEPARATOR = "\x1e";

type Obj = Record<string, unknown>;
const isObj = (v: unknown): v is Obj => typeof v === "object" && v !== null && !Array.isArray(v);
const num = (v: unknown): number | null => (typeof v === "number" && Number.isFinite(v) ? v : null);
const str = (v: unknown): string | null => (typeof v === "string" && v !== "" ? v : null);

export const SANDBOX_ANSWERING: SandboxHealth = {
  answering: true,
  attempt: null,
  maxAttempts: null,
  willRetry: null,
  nextRetryAt: null,
  reason: null,
  message: null,
  toolId: null,
};

export type ParsedDelta =
  | { kind: "output"; seq: number | null; chunks: { stream: "stdout" | "stderr" | "note"; text: string }[] }
  | { kind: "sandbox"; notice: Obj }
  | { kind: "ignored" };

/** One `progress_delta` string, parsed. */
export function parseDelta(delta: string): ParsedDelta {
  if (!delta.startsWith(RECORD_SEPARATOR)) {
    return delta ? { kind: "output", seq: null, chunks: [{ stream: "stdout", text: delta }] } : { kind: "ignored" };
  }
  let body: unknown;
  try {
    body = JSON.parse(delta.slice(1));
  } catch {
    return { kind: "ignored" };
  }
  if (!isObj(body)) return { kind: "ignored" };
  if (body.type === "sandbox") return { kind: "sandbox", notice: body };
  if (body.type !== "output" || !Array.isArray(body.chunks)) return { kind: "ignored" };
  const chunks: { stream: "stdout" | "stderr" | "note"; text: string }[] = [];
  for (const c of body.chunks) {
    if (!isObj(c) || typeof c.text !== "string") continue;
    const stream = c.stream === "stderr" || c.stream === "note" ? c.stream : "stdout";
    chunks.push({ stream, text: c.text });
  }
  return { kind: "output", seq: num(body.seq), chunks };
}

export function sandboxFrom(notice: Obj, toolId: string | null): SandboxHealth {
  if (notice.state === "up") return { ...SANDBOX_ANSWERING, attempt: num(notice.attempt) };
  const retryAt = num(notice.next_retry_at);
  return {
    answering: false,
    attempt: num(notice.attempt),
    maxAttempts: num(notice.max_attempts),
    willRetry: typeof notice.will_retry === "boolean" ? notice.will_retry : null,
    nextRetryAt: retryAt === null ? null : Math.round(retryAt * 1000),
    reason: str(notice.reason),
    message: str(notice.message),
    toolId,
  };
}

/**
 * Our own notes (reattach, dropped output, limit reached) sit on a line of their own with no
 * blank lines around them: a note starts on a fresh line, ends with one newline, and the
 * output right after a note drops its leading newlines.
 */
export function tidy(
  chunk: { stream: string; text: string },
  previous: { stream: string } | undefined,
  textSoFar: string,
): string {
  if (chunk.stream === "note") {
    const line = chunk.text.replace(/^\n+/, "").replace(/\n+$/, "");
    if (!line) return "";
    const lead = textSoFar === "" || textSoFar.endsWith("\n") ? "" : "\n";
    return `${lead}${line}\n`;
  }
  return previous?.stream === "note" ? chunk.text.replace(/^\n+/, "") : chunk.text;
}

export interface ProgressView {
  /** Output per tool id: stdout, stderr and notes, in arrival order. */
  text: Readonly<Record<string, string>>;
  chunks: Readonly<Record<string, readonly ToolProgressChunk[]>>;
  sandbox: SandboxHealth;
}

const EMPTY: ProgressView = { text: {}, chunks: {}, sandbox: SANDBOX_ANSWERING };

export class ProgressFolder {
  #text: Record<string, string> = {};
  #chunks: Record<string, ToolProgressChunk[]> = {};
  #lastSeq: Record<string, number> = {};
  #sandbox: SandboxHealth = SANDBOX_ANSWERING;
  #seen = 0;
  #first: RawFrame | undefined;
  #view: ProgressView = EMPTY;

  update(frames: readonly RawFrame[]): ProgressView {
    if (frames.length < this.#seen || (this.#first !== undefined && frames[0] !== this.#first)) {
      this.#text = {};
      this.#chunks = {};
      this.#lastSeq = {};
      this.#sandbox = SANDBOX_ANSWERING;
      this.#seen = 0;
      this.#view = EMPTY;
    }
    this.#first = frames[0];
    let changed = false;
    for (let i = this.#seen; i < frames.length; i++) {
      const f = frames[i];
      if (!f || !isObj(f.data)) continue;
      const toolId = str(f.data.tool_id);
      // The call ended: a notice about it no longer holds.
      if ((f.event === "tool_end" || f.event === "tool_error") && toolId && this.#sandbox.toolId === toolId) {
        this.#sandbox = SANDBOX_ANSWERING;
        changed = true;
        continue;
      }
      if (f.event !== "tool_progress_delta" || !toolId || typeof f.data.progress_delta !== "string") continue;
      const parsed = parseDelta(f.data.progress_delta);
      if (parsed.kind === "sandbox") {
        this.#sandbox = sandboxFrom(parsed.notice, toolId);
        changed = true;
        continue;
      }
      if (parsed.kind !== "output") continue;
      if (parsed.seq !== null) {
        const last = this.#lastSeq[toolId];
        if (last !== undefined && parsed.seq <= last) continue;
        this.#lastSeq[toolId] = parsed.seq;
      }
      const at = typeof f.data.timestamp === "number" ? Math.round(f.data.timestamp * 1000) : null;
      for (const c of parsed.chunks) {
        const list = (this.#chunks[toolId] ??= []);
        const before = this.#text[toolId] ?? "";
        const text = tidy(c, list[list.length - 1], before);
        if (!text) continue;
        this.#text[toolId] = before + text;
        list.push({ text, stream: c.stream, at });
        changed = true;
      }
    }
    this.#seen = frames.length;
    if (changed) {
      this.#view = {
        text: { ...this.#text },
        chunks: Object.fromEntries(Object.entries(this.#chunks).map(([k, v]) => [k, [...v]])),
        sandbox: this.#sandbox,
      };
    }
    return this.#view;
  }
}
