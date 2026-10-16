// How a gated tool call reads on an approval or judge card: its risk class, badge, label,
// and whether "Always approve" applies. Pure.

import type { Evaluation } from "../session";
import { count } from "../recovery/format";
import { addedFilePreview, countLines, editPreview, type DiffLine } from "../workbench/diff";

export type RiskClass = "read_only" | "workspace_edit" | "shell" | "network" | "external_side_effect";

/**
 * The agent's risk class per tool (simple_chat_agent/agent/approvals.py TOOL_RISK_CLASSES).
 * The facade doesn't carry it yet; an evaluation's `details.criteria_set` wins when present.
 */
const TOOL_RISK: Record<string, RiskClass> = {
  read_file: "read_only",
  list_files: "read_only",
  search_code: "read_only",
  git_status: "read_only",
  git_diff: "read_only",
  read_attachment: "read_only",
  write_file: "workspace_edit",
  edit_file: "workspace_edit",
  import_attachment_to_workspace: "workspace_edit",
  git_commit: "shell",
  run_command: "shell",
  fetch_url: "network",
  search_web: "network",
  create_artifact: "external_side_effect",
};

const RISK: ReadonlySet<string> = new Set(["read_only", "workspace_edit", "shell", "network", "external_side_effect"]);

export function riskClass(toolName: string, evaluations: readonly Evaluation[] = []): RiskClass {
  for (const e of evaluations) {
    const c = e.details?.criteria_set;
    if (typeof c === "string" && RISK.has(c)) return c as RiskClass;
  }
  if (TOOL_RISK[toolName]) return TOOL_RISK[toolName];
  // The subagent drivers (start_explorer, explorer_ask, stop_explorer) are read-only.
  if (/^(start|stop)_\w+$|^(explorer|reviewer)_\w+$/.test(toolName)) return "read_only";
  return "external_side_effect";
}

/** Whether "Always approve" is offered. The agent honors `remember` for file edits only. */
export function canAlwaysApprove(risk: RiskClass): boolean {
  return risk === "workspace_edit";
}

export const ALWAYS_APPROVE_LABEL = "Always approve file edits";
export const ALWAYS_APPROVE_WHY = "Always approve lasts for this session and covers file edits only. You can undo it from the posture menu.";

/** Why "Always approve" is greyed out for this class. */
export function alwaysApproveReason(risk: RiskClass): string {
  switch (risk) {
    case "workspace_edit":
      return "";
    case "shell":
      return "Commits and shell commands need a yes each time";
    case "network":
      return "Network calls need a yes each time";
    case "read_only":
      return "Always approve covers file edits only";
    default:
      return "Calls that act outside the workspace need a yes each time";
  }
}

export type BadgeKind = "edit" | "shell" | "git" | "net" | "read" | "agent" | "art";

export function toolBadge(toolName: string, risk: RiskClass): { kind: BadgeKind; text: string } {
  if (toolName === "git_commit" || toolName.startsWith("git_")) return { kind: risk === "read_only" ? "read" : "git", text: "Git" };
  if (toolName === "run_command") return { kind: "shell", text: "Shell" };
  if (risk === "workspace_edit") return { kind: "edit", text: "Edit" };
  if (risk === "network") return { kind: "net", text: "Network" };
  if (toolName === "create_artifact") return { kind: "art", text: "Artifact" };
  if (risk === "read_only") return { kind: "read", text: "Read" };
  return { kind: "shell", text: "Tool" };
}

function str(input: Record<string, unknown>, key: string): string {
  const v = input[key];
  return typeof v === "string" ? v : "";
}

/** Stable JSON (sorted keys) for folding identical arguments. */
export function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (value && typeof value === "object") {
    const o = value as Record<string, unknown>;
    return `{${Object.keys(o)
      .sort()
      .filter((k) => o[k] !== undefined)
      .map((k) => `${JSON.stringify(k)}:${canonical(o[k])}`)
      .join(",")}}`;
  }
  return JSON.stringify(value ?? null);
}

function short(s: string, n = 80): string {
  const one = s.replace(/\s+/g, " ").trim();
  return one.length > n ? `${one.slice(0, n - 1)}…` : one;
}

/** The one-line argument summary: a path, a command, a commit message, or compact JSON. */
export function argSummary(toolName: string, input: Record<string, unknown>): string {
  switch (toolName) {
    case "edit_file":
    case "write_file":
    case "read_file":
      return str(input, "path");
    case "run_command":
      return short(str(input, "command"), 120);
    case "git_commit":
      return `"${short(str(input, "message"), 90)}"`;
    case "fetch_url":
      return str(input, "url");
    case "search_web":
      return short(str(input, "query"));
    default: {
      const json = JSON.stringify(input);
      return short(json === "{}" ? "" : json);
    }
  }
}

/** What reviewing a call shows: a diff for file edits, the arguments otherwise. */
export type Review = { kind: "diff"; lines: DiffLine[]; note: string | null } | { kind: "args"; text: string };

export function reviewOf(toolName: string, input: Record<string, unknown>): Review {
  if (toolName === "edit_file") return { kind: "diff", lines: editPreview(str(input, "old_string"), str(input, "new_string")), note: null };
  if (toolName === "write_file")
    return { kind: "diff", lines: addedFilePreview(str(input, "content")), note: "The file's full new content. If the file exists, it is replaced." };
  if (toolName === "run_command") {
    const rows = [`$ ${str(input, "command")}`];
    const cwd = str(input, "cwd");
    if (cwd && cwd !== ".") rows.push(`cwd: ${cwd}`);
    return { kind: "args", text: rows.join("\n") };
  }
  if (toolName === "git_commit") {
    const rows = ["git_commit", `message: "${str(input, "message")}"`];
    if (input.add_all === false) rows.push("stages: only what is already staged");
    return { kind: "args", text: rows.join("\n") };
  }
  return { kind: "args", text: `${toolName}\n${JSON.stringify(input, null, 2)}` };
}

/** "+1 −1" for a file edit's preview. */
export function editStat(toolName: string, input: Record<string, unknown>): string | null {
  const r = reviewOf(toolName, input);
  if (r.kind !== "diff") return null;
  const c = countLines(r.lines);
  return `+${c.added} −${c.removed}`;
}

/** The single-call card's title. */
export function callTitle(toolName: string, input: Record<string, unknown>, ctx: { branch?: string | null; files?: number } = {}): string {
  switch (toolName) {
    case "edit_file":
      return `Edit ${str(input, "path")}`;
    case "write_file":
      return `Write ${str(input, "path")}`;
    case "run_command":
      return "Run a command";
    case "git_commit": {
      const files = ctx.files ? ` ${count(ctx.files, "file")}` : "";
      return `Commit${files}${ctx.branch ? ` to ${ctx.branch}` : ""}`;
    }
    case "fetch_url":
      return "Fetch a web page";
    case "search_web":
      return "Search the web";
    case "create_artifact":
      return "Publish an artifact";
    default:
      return `Call ${toolName}`;
  }
}
