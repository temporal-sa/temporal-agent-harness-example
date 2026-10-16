// Every live output chunk of each tool call, accumulated from tool_progress_delta frames (the
// harness projection keeps only the latest chunk). Pure and incremental.

import type { RawFrame } from "./telemetry";
import type { ToolProgressChunk } from "./types";

type Obj = Record<string, unknown>;
const isObj = (v: unknown): v is Obj => typeof v === "object" && v !== null && !Array.isArray(v);

export interface ProgressView {
  text: Readonly<Record<string, string>>;
  chunks: Readonly<Record<string, readonly ToolProgressChunk[]>>;
}

export class ProgressFolder {
  #text: Record<string, string> = {};
  #chunks: Record<string, ToolProgressChunk[]> = {};
  #seen = 0;
  #first: RawFrame | undefined;
  #view: ProgressView = { text: {}, chunks: {} };

  update(frames: readonly RawFrame[]): ProgressView {
    if (frames.length < this.#seen || (this.#first !== undefined && frames[0] !== this.#first)) {
      this.#text = {};
      this.#chunks = {};
      this.#seen = 0;
      this.#view = { text: {}, chunks: {} };
    }
    this.#first = frames[0];
    let changed = false;
    for (let i = this.#seen; i < frames.length; i++) {
      const f = frames[i];
      if (f?.event !== "tool_progress_delta" || !isObj(f.data)) continue;
      const toolId = f.data.tool_id;
      const delta = f.data.progress_delta;
      if (typeof toolId !== "string" || typeof delta !== "string") continue;
      const stream = f.data.stream === "stdout" || f.data.stream === "stderr" ? f.data.stream : null;
      const at = typeof f.data.timestamp === "number" ? Math.round(f.data.timestamp * 1000) : null;
      this.#text[toolId] = (this.#text[toolId] ?? "") + delta;
      (this.#chunks[toolId] ??= []).push({ text: delta, stream, at });
      changed = true;
    }
    this.#seen = frames.length;
    if (changed) this.#view = { text: { ...this.#text }, chunks: Object.fromEntries(Object.entries(this.#chunks).map(([k, v]) => [k, [...v]])) };
    return this.#view;
  }
}
