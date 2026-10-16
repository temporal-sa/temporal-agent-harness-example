// Turns one message's parts (stream order) into the rows the transcript draws. Pure, so the
// folding rules are tested without React:
//
// * Parts split at each model_interaction part. A call that is still "running" when a later
//   call has started was cut off by a lost worker: its partial text folds under
//   "Attempt N (worker lost)" and is never shown twice.
// * Thinking: a row per call that produced thought text or reported thought tokens.
// * Consecutive reply_delta parts join into one text row.
// * Consecutive read-only calls of one tool fold into one grouped row ("Read file × 3").
// * The parent's subagent-driving tools are hidden; the subagent rows stand for them, and
//   subagents asked at once sit side by side.
// * After Stop, the reply text that followed the last tool call is folded as "Discarded".

import type { Message, ModelInteractionPart, SubagentPart, ToolPart, TurnRecovery } from "../session";
import { GROUPABLE, SUBAGENT_TOOLS, wordCount } from "./toolInfo";

export type Item =
  | { kind: "thinking"; key: string; callIndex: number; model: string | null; text: string; thoughtTokens: number | null; running: boolean }
  | { kind: "text"; key: string; text: string; streaming: boolean }
  | { kind: "tool"; key: string; part: ToolPart }
  | { kind: "group"; key: string; label: string; parts: ToolPart[] }
  | { kind: "subagents"; key: string; parts: SubagentPart[] }
  | { kind: "attempt"; key: string; attempt: number; words: number; text: string }
  | { kind: "discarded"; key: string; words: number; text: string }
  | { kind: "recovery"; key: string; recovery: TurnRecovery };

interface Segment {
  call: ModelInteractionPart | null;
  callIndex: number;
  parts: Message["parts"];
}

function segments(parts: Message["parts"]): Segment[] {
  const out: Segment[] = [{ call: null, callIndex: -1, parts: [] }];
  let index = -1;
  for (const p of parts) {
    if (p.type === "model_interaction") {
      index += 1;
      out.push({ call: p, callIndex: index, parts: [] });
    } else {
      out[out.length - 1]?.parts.push(p);
    }
  }
  return out.filter((s) => s.call !== null || s.parts.length > 0);
}

const isWaiting = (p: ToolPart) => p.state === "awaiting_approval" || p.state === "evaluating";

export interface BuildOptions {
  /** The message's handler is still running (its last text row is streaming). */
  live: boolean;
  /** The turn ended because the user pressed Stop. */
  interrupted: boolean;
  /** Recoveries in this message's turn (root agent). */
  recoveries?: readonly TurnRecovery[];
}

export function buildItems(message: Pick<Message, "id" | "parts">, options: BuildOptions): Item[] {
  const segs = segments(message.parts);
  const lastCall = Math.max(-1, ...segs.map((s) => s.callIndex));
  const items: Item[] = [];
  let attempts = 0;
  let firstLostAt = -1;

  for (const seg of segs) {
    const key = `${message.id}:${seg.callIndex}`;
    const lost = seg.call !== null && seg.call.status === "running" && seg.callIndex < lastCall;
    if (lost) {
      attempts += 1;
      const text = seg.parts.map((p) => (p.type === "reply_delta" ? p.text : "")).join("");
      if (firstLostAt < 0) firstLostAt = items.length;
      items.push({ kind: "attempt", key: `${key}:attempt`, attempt: attempts, words: wordCount(text), text });
      // Tool calls the lost attempt made still happened (or were retried by Temporal); keep
      // them visible.
      pushContent(items, seg.parts.filter((p) => p.type !== "reply_delta" && p.type !== "thought_summary"), key, false);
      continue;
    }
    if (seg.call) {
      const thought = seg.parts.map((p) => (p.type === "thought_summary" ? p.text : "")).join("");
      const tokens = seg.call.usage?.thought_tokens ?? null;
      if (thought || (tokens !== null && tokens > 0)) {
        items.push({
          kind: "thinking",
          key: `${key}:thinking`,
          callIndex: seg.callIndex,
          model: seg.call.model,
          text: thought,
          thoughtTokens: tokens,
          running: seg.call.status === "running",
        });
      }
    }
    pushContent(items, seg.parts, key, options.live);
  }

  // Stop: the trailing reply text (after the last tool call) was thrown away.
  if (options.interrupted) {
    const last = items[items.length - 1];
    if (last?.kind === "text") {
      items.pop();
      items.push({ kind: "discarded", key: `${message.id}:discarded`, words: wordCount(last.text), text: last.text });
    }
  }

  // Mark the streaming row: only the very last text row of a live message streams.
  for (const it of items) if (it.kind === "text") it.streaming = false;
  const tail = items[items.length - 1];
  if (options.live && tail?.kind === "text") tail.streaming = true;

  // Recovery dividers: where the first lost attempt was, else before the final reply text.
  for (const recovery of options.recoveries ?? []) {
    const divider: Item = { kind: "recovery", key: `${message.id}:recovery:${recovery.turnId ?? ""}`, recovery };
    if (firstLostAt >= 0) {
      items.splice(firstLostAt, 0, divider);
    } else {
      let at = items.length;
      while (at > 0 && (items[at - 1]?.kind === "text" || items[at - 1]?.kind === "discarded")) at -= 1;
      items.splice(at, 0, divider);
    }
  }
  return items;
}

function pushContent(items: Item[], parts: Message["parts"], key: string, live: boolean): void {
  let n = 0;
  for (const p of parts) {
    n += 1;
    const k = `${key}:${n}`;
    const prev = items[items.length - 1];
    if (p.type === "reply_delta") {
      if (prev?.kind === "text") prev.text += p.text;
      else items.push({ kind: "text", key: k, text: p.text, streaming: live });
    } else if (p.type === "tool") {
      if (SUBAGENT_TOOLS.has(p.toolName)) continue;
      const label = GROUPABLE[p.toolName];
      if (label && !isWaiting(p)) {
        if (prev?.kind === "group" && prev.parts[0]?.toolName === p.toolName) {
          prev.parts.push(p);
          continue;
        }
        if (prev?.kind === "tool" && prev.part.toolName === p.toolName && !isWaiting(prev.part)) {
          items[items.length - 1] = { kind: "group", key: prev.key, label, parts: [prev.part, p] };
          continue;
        }
      }
      items.push({ kind: "tool", key: k, part: p });
    } else if (p.type === "subagent") {
      if (prev?.kind === "subagents") prev.parts.push(p);
      else items.push({ kind: "subagents", key: k, parts: [p] });
    }
    // thought_summary is drawn by the thinking row; text_annotation has no row of its own.
  }
}

