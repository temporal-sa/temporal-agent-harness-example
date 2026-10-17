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
import { nestHostCalls } from "../session/extras";
import { GROUPABLE, SUBAGENT_TOOLS, wordCount } from "./toolInfo";

export type Item =
  | { kind: "thinking"; key: string; callIndex: number; model: string | null; text: string; thoughtTokens: number | null; running: boolean }
  | { kind: "text"; key: string; text: string; streaming: boolean }
  | { kind: "tool"; key: string; part: ToolPart; hostCalls?: ToolPart[] }
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
  /** When each tool call ran, to find the row that was running when a Worker was lost. */
  toolTiming?(toolId: string): { startedAt: number | null; endedAt: number | null } | undefined;
}

export function buildItems(message: Pick<Message, "id" | "parts">, options: BuildOptions): Item[] {
  // A Code Mode script's host calls (`<script id>/<n>`) sit under the script's row.
  const nested = nestHostCalls(message.parts);
  const segs = segments(nested.parts);
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
  for (const it of items) {
    if (it.kind === "tool") {
      const calls = nested.hostCalls.get(it.part.toolId);
      if (calls) it.hostCalls = calls;
    }
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

  // Recovery dividers go where the Worker was lost (PRD U6, wireframe Frame 3): right after
  // the row of the tool that was running then; else where the first lost model attempt was;
  // else before the final reply text.
  for (const recovery of options.recoveries ?? []) {
    const divider: Item = { kind: "recovery", key: `${message.id}:recovery:${recovery.turnId ?? ""}`, recovery };
    const toolId = lostToolId(recovery, message.parts, options.toolTiming);
    const row = toolId ? items.findIndex((it) => (it.kind === "tool" && it.part.toolId === toolId) || (it.kind === "group" && it.parts.some((p) => p.toolId === toolId))) : -1;
    if (row >= 0) {
      items.splice(row + 1, 0, divider);
    } else if (firstLostAt >= 0) {
      items.splice(firstLostAt, 0, divider);
    } else {
      let at = items.length;
      while (at > 0 && (items[at - 1]?.kind === "text" || items[at - 1]?.kind === "discarded")) at -= 1;
      items.splice(at, 0, divider);
    }
  }
  return items;
}

/** "workspace.run_command" (an activity type) → "run_command" (the tool's name). */
const bareToolName = (name: string) => name.slice(name.lastIndexOf(".") + 1);

/**
 * The tool call that was running when the Worker was lost: among the turn's calls of the
 * recovery's tool, the one whose run spans the loss time (or, without times, the only one).
 * Null when a recovery names no tool (a model call) or nothing matches.
 */
export function lostToolId(
  recovery: TurnRecovery,
  parts: Message["parts"],
  timing?: (toolId: string) => { startedAt: number | null; endedAt: number | null } | undefined,
): string | null {
  const tools = (recovery.recoveries ?? []).filter((r) => r.toolName && r.kind !== "model_call");
  if (tools.length === 0) return null;
  const names = new Set(tools.map((r) => bareToolName(r.toolName!)));
  const candidates = parts.filter((p): p is ToolPart => p.type === "tool" && names.has(p.toolName));
  if (candidates.length === 0) return null;
  const lostAt = recovery.lostAt ?? tools.find((r) => r.lostAt !== null)?.lostAt ?? null;
  if (lostAt !== null && timing) {
    const spanning = candidates.filter((p) => {
      const t = timing(p.toolId);
      return t?.startedAt != null && t.startedAt <= lostAt && (t.endedAt == null || t.endedAt >= lostAt);
    });
    if (spanning.length > 0) return spanning[spanning.length - 1]!.toolId;
    // Started before the loss and the latest such: the run that was cut off.
    const before = candidates.filter((p) => {
      const t = timing(p.toolId);
      return t?.startedAt != null && t.startedAt <= lostAt;
    });
    if (before.length > 0) return before[before.length - 1]!.toolId;
  }
  // A command that kept running and was reattached says so in its result.
  if (tools.some((r) => r.kind === "command_reattached")) {
    const reattached = candidates.filter((p) => / · reattached\b/.test(p.output ?? ""));
    if (reattached.length > 0) return reattached[reattached.length - 1]!.toolId;
  }
  return candidates.length === 1 ? candidates[0]!.toolId : null;
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

