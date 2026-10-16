// The recovery divider's copy (PRD U6). Pure, so "Checking…" until confirmed is tested.

import type { Recovery, TurnRecovery } from "../session";
import { count, hms, tokens } from "./format";

export const CHECKING = "Checking…";
export const COMPARISON = "compared with re-running this turn from your message";

export interface DividerFigure {
  label: "Kept" | "Repeated";
  value: string;
  tone: "kept" | "rep";
}

export interface DividerView {
  title: string;
  lostAt: string | null;
  body: string;
  figures: DividerFigure[];
  checking: boolean;
}

function first(r: TurnRecovery): Recovery | null {
  let best: Recovery | null = null;
  for (const x of r.recoveries) {
    const t = x.lostAt ?? x.detectedAt ?? Number.MAX_SAFE_INTEGER;
    const b = best ? (best.lostAt ?? best.detectedAt ?? Number.MAX_SAFE_INTEGER) : Number.MAX_SAFE_INTEGER;
    if (!best || t < b) best = x;
  }
  return best;
}

function toolName(name: string | null): string | null {
  return name ? name.replace(/^workspace\./, "") : null;
}

function where(r: TurnRecovery): string {
  const reattached = r.recoveries.some((x) => x.kind === "command_reattached");
  const lead = first(r);
  const worker = lead?.resumedOn ? ` (${lead.resumedOn})` : "";
  if (reattached) {
    return `Another worker${worker} continued this turn at the command it was running. It reattached to the same run, so the command did not start over.`;
  }
  if (lead?.kind === "tool_call") {
    const name = toolName(lead.toolName);
    return `Another worker${worker} continued this turn at ${name ?? "the tool call"}, which ran again.`;
  }
  return `Another worker${worker} continued this turn at the model call that was in flight.`;
}

function maxAttempt(r: TurnRecovery, kind: string): number | null {
  let a: number | null = null;
  for (const x of r.recoveries) if (x.kind === kind && x.attempt !== null) a = Math.max(a ?? 0, x.attempt);
  return a;
}

export function dividerView(r: TurnRecovery): DividerView {
  const lostAt = hms(r.lostAt);
  const title = lostAt ? `Recovered · worker lost at ${lostAt}` : "Recovered · worker lost";
  const body = where(r);
  if (!r.confirmed) {
    return {
      title,
      lostAt,
      body,
      checking: true,
      figures: [
        { label: "Kept", value: CHECKING, tone: "kept" },
        { label: "Repeated", value: CHECKING, tone: "rep" },
      ],
    };
  }
  const k = r.kept;
  const tok = k.tokensIn + k.tokensOut;
  const reattached = r.recoveries.some((x) => x.kind === "command_reattached");
  const figures: DividerFigure[] = [
    { label: "Kept", value: `${count(k.modelCalls, "model call")}${tok > 0 ? ` (${tokens(tok)} tokens)` : ""}`, tone: "kept" },
    { label: "Kept", value: `${count(k.toolCalls, "tool call")}${reattached ? " plus the running command" : ""}`, tone: "kept" },
  ];
  const rep: string[] = [];
  if (r.repeatedModelCalls > 0) {
    const a = maxAttempt(r, "model_call");
    rep.push(`${count(r.repeatedModelCalls, "model call")}${a ? ` (attempt ${a})` : ""}`);
  }
  if (r.repeatedToolCalls > 0) rep.push(count(r.repeatedToolCalls, "tool call"));
  figures.push({ label: "Repeated", value: rep.length ? rep.join(", ") : "nothing", tone: "rep" });
  return { title, lostAt, body, checking: false, figures };
}
