// Stacked approvals (owner decision B). Pure, so the rules are unit-tested:
//
// * Every call waiting at once shares one card with "Approve all N".
// * Calls group by tool; calls with identical arguments fold into one row with "× n".
// * Each group has Approve n / Decline n. "Always approve" is live only on file-edit groups.
// * An answered call leaves the counts at once (it stays on the card, marked, until the agent
//   settles it), and Approve all and the group buttons answer only calls still waiting.
// * A call that arrives while the pointer is on the card is held and joins when the pointer
//   leaves, so the buttons never move under the cursor. Held calls are never answered by
//   Approve all: you approve only what you saw.

import type { PendingApproval, SendPhase, SendRecord } from "../session";
import { canonical, type Risk } from "./describe";

export interface ApprovalItem {
  toolId: string;
  toolName: string;
  input: Record<string, unknown>;
  agentId: string;
  messageId: string;
  /** From the facade (config): the call's class, and whether "Always approve" applies. */
  risk: Risk;
  rememberAllowed: boolean;
}

export function itemOf(p: PendingApproval): ApprovalItem {
  return {
    toolId: p.part.toolId,
    toolName: p.part.toolName,
    input: p.part.input,
    agentId: p.agentId,
    messageId: p.messageId,
    risk: p.riskClass ?? null,
    rememberAllowed: p.rememberAllowed === true,
  };
}

/** This browser's answer to a call, from its send record. */
export interface Answer {
  approved: boolean;
  phase: SendPhase;
  error: string | null;
  record: SendRecord;
}

/** The latest approval send per tool id. */
export function answersFrom(sends: readonly SendRecord[]): Map<string, Answer> {
  const out = new Map<string, Answer>();
  for (const r of sends) {
    if (r.kind !== "approval" || !r.toolId) continue;
    out.set(r.toolId, { approved: r.approved === true, phase: r.phase, error: r.error, record: r });
  }
  return out;
}

/** Answered here and not failed: it no longer counts as waiting. */
export function isAnswered(a: Answer | undefined): boolean {
  return !!a && a.phase !== "failed";
}

export interface Fold {
  /** toolName + canonical arguments. */
  key: string;
  toolName: string;
  input: Record<string, unknown>;
  /** Every call in the fold, in arrival order. */
  items: ApprovalItem[];
  /** The calls still waiting on you. */
  waiting: ApprovalItem[];
}

export interface Group {
  toolName: string;
  risk: Risk;
  /** Every call in the group allows "Always approve" (one tool, so one class). */
  rememberAllowed: boolean;
  folds: Fold[];
  /** Tool ids still waiting, in arrival order. */
  waitingIds: string[];
  /** Calls on the card in this group, answered or not. */
  total: number;
}

export interface Stack {
  groups: Group[];
  /** Every tool id still waiting, in arrival order: what "Approve all N" answers. */
  waitingIds: string[];
  /** Calls on the card, answered or not. */
  total: number;
}

export function foldKey(item: Pick<ApprovalItem, "toolName" | "input">): string {
  return `${item.toolName}\u0000${canonical(item.input)}`;
}

/** Group and fold the card's calls. `items` is the card's display order. */
export function buildStack(items: readonly ApprovalItem[], answers: ReadonlyMap<string, Answer>): Stack {
  const groups = new Map<string, Group>();
  const folds = new Map<string, Fold>();
  const waitingIds: string[] = [];
  for (const item of items) {
    let g = groups.get(item.toolName);
    if (!g) {
      g = { toolName: item.toolName, risk: item.risk, rememberAllowed: item.rememberAllowed, folds: [], waitingIds: [], total: 0 };
      groups.set(item.toolName, g);
    }
    const key = foldKey(item);
    let f = folds.get(key);
    if (!f) {
      f = { key, toolName: item.toolName, input: item.input, items: [], waiting: [] };
      folds.set(key, f);
      g.folds.push(f);
    }
    f.items.push(item);
    g.total++;
    g.rememberAllowed &&= item.rememberAllowed;
    if (!isAnswered(answers.get(item.toolId))) {
      f.waiting.push(item);
      g.waitingIds.push(item.toolId);
      waitingIds.push(item.toolId);
    }
  }
  return { groups: [...groups.values()], waitingIds, total: items.length };
}

// -- arrivals and departures ----------------------------------------------------------------

export interface CardState {
  /** Tool ids on the card, in display order. */
  order: string[];
  /** The call behind each id, kept after the agent settles it so its row doesn't jump. */
  items: Record<string, ApprovalItem>;
  /** Arrived while the pointer was on the card; they join when it leaves. */
  held: string[];
}

export const EMPTY_CARD: CardState = { order: [], items: {}, held: [] };

/**
 * The card's next state.
 * - A call still pending stays where it is. A call this browser answered stays (marked) while
 *   any call on the card is still pending; one settled elsewhere leaves.
 * - New calls append to the end (their group keeps its place), unless `hold` is set while the
 *   card is showing: then they wait in `held`.
 * - When nothing on the card is pending any more, the card empties.
 */
export function reconcile(
  prev: CardState,
  pending: readonly ApprovalItem[],
  answeredHere: (toolId: string) => boolean,
  hold: boolean,
): CardState {
  const pendingIds = new Set(pending.map((p) => p.toolId));
  const items: Record<string, ApprovalItem> = {};
  const kept = prev.order.filter((id) => pendingIds.has(id) || answeredHere(id));
  const showing = kept.some((id) => pendingIds.has(id));
  const known = new Set(kept);
  const fresh = pending.filter((p) => !known.has(p.toolId)).map((p) => p.toolId);

  let order = showing ? kept : [];
  let held: string[] = [];
  if (fresh.length) {
    if (hold && showing) held = fresh;
    else order = [...order, ...fresh];
  }
  for (const id of order) {
    const p = pending.find((x) => x.toolId === id) ?? prev.items[id];
    if (p) items[id] = p;
  }
  return { order, items, held };
}

/** The card's calls in display order. */
export function cardItems(state: CardState): ApprovalItem[] {
  return state.order.map((id) => state.items[id]).filter((x): x is ApprovalItem => !!x);
}

/** Tool ids a group button answers: only calls still waiting. */
export function groupTargets(group: Group): string[] {
  return [...group.waitingIds];
}

/** Tool ids "Approve all N" answers: only calls still waiting and shown. */
export function approveAllTargets(stack: Stack): string[] {
  return [...stack.waitingIds];
}
