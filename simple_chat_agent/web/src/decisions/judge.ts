// Judge evaluation cards (PRD U4): approved (green) or escalated (pink), with confidence and a
// one-line reason. Read from the harness's `auto_approval_evaluation_*` events. Pure.

import type { Evaluation, ToolPart } from "../session";
import { argSummary, criteriaOf, riskNoun, type Risk } from "./describe";

export type JudgeTone = "approved" | "escalated" | "declined" | "deciding";

export interface JudgeView {
  id: string;
  tone: JudgeTone;
  badge: string;
  tool: string;
  args: string;
  /** "confidence 0.93" or "rule: commits need a person". */
  score: string | null;
  why: string;
}

function sentence(s: string): string {
  const t = s.trim().replace(/\s+/g, " ");
  if (!t) return t;
  const c = t[0]!.toUpperCase() + t.slice(1);
  return /[.!?]$/.test(c) ? c : `${c}.`;
}

/** The judge's reason without the machine prefix and the "escalated to a human" tail. */
export function reasonLine(e: Pick<Evaluation, "reason" | "details" | "error">, risk: Risk, toolName: string): string {
  const d = e.details ?? {};
  if (typeof d.judge_reason === "string" && d.judge_reason.trim()) return sentence(d.judge_reason);
  const r = e.reason ?? e.error ?? "";
  if (/always go to a human|need a human in the/.test(r)) {
    const tail = toolName === "git_commit" ? " A commit changes the repository's history." : "";
    return `The judge doesn't approve ${riskNoun(risk, toolName)} in this environment.${tail}`;
  }
  const stripped = r
    .replace(/^pre-rule [\w-]+:\s*/i, "")
    .replace(/^judge [^:]*:\s*/i, "")
    .replace(/;?\s*escalated to a human[^.]*\.?/i, "")
    .replace(/\s*Escalated to a human\.?$/i, "")
    .replace(/\s*\(.*?\)\s*$/, "");
  return sentence(stripped) || "No reason given.";
}

export function scoreLabel(e: Pick<Evaluation, "details" | "error">, risk: Risk, toolName: string): string | null {
  const d = e.details ?? {};
  if (typeof d.confidence === "number") return `confidence ${d.confidence.toFixed(2)}`;
  if (typeof d.pre_rule === "string") return `rule: ${d.pre_rule.replace(/_/g, " ")}`;
  if (e.error || typeof d.error === "string") return "judge didn't answer";
  if (typeof d.criteria_set === "string") return `rule: ${riskNoun(risk, toolName)} need a person`;
  return null;
}

export function judgeView(tool: Pick<ToolPart, "toolName" | "input" | "evaluations">, e: Evaluation): JudgeView | null {
  if (e.status === "superseded") return null;
  const risk = criteriaOf(tool.evaluations);
  const base = { id: e.id, tool: tool.toolName, args: argSummary(tool.toolName, tool.input) };
  if (e.status === "running") {
    return { ...base, tone: "deciding", badge: "Judge deciding", score: null, why: "The judge is reading this call. You can still answer it yourself." };
  }
  const verdict = e.status === "error" ? "escalate" : e.verdict;
  const tone: JudgeTone = verdict === "approve" ? "approved" : verdict === "deny" ? "declined" : "escalated";
  const badge = tone === "approved" ? "Judge approved" : tone === "declined" ? "Judge declined" : "Escalated to you";
  return { ...base, tone, badge, score: scoreLabel(e, risk, tool.toolName), why: reasonLine(e, risk, tool.toolName) };
}

/** Every card to show for a tool call, oldest first. */
export function judgeViews(tool: Pick<ToolPart, "toolName" | "input" | "evaluations">): JudgeView[] {
  return tool.evaluations.map((e) => judgeView(tool, e)).filter((v): v is JudgeView => v !== null);
}
