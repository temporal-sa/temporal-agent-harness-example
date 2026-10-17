// How a gated tool call reads on an approval or judge card: its risk class, badge, label,
// and whether "Always approve" applies. Pure.

import type { Evaluation } from "../session";
import { SCRIPT_TOOL, mcpTool, toolDisplayName } from "../session/extras";
import { count } from "../recovery/format";
import { addedFilePreview, countLines, editPreview, type DiffLine } from "../workbench/diff";

/** A tool's approval class, from the facade (`PendingApproval.riskClass`, which comes from
 *  config). Null while config hasn't loaded. Not narrowed: config may name new classes. */
export type Risk = string | null;

/** The class an evaluation recorded (`details.criteria_set`), for judge cards. */
export function criteriaOf(evaluations: readonly Evaluation[]): Risk {
  for (const e of evaluations) {
    const c = e.details?.criteria_set;
    if (typeof c === "string" && c) return c;
  }
  return null;
}

/** The live button's label. The agent honors `remember` for the classes config allows. */
export function alwaysApproveLabel(risk: Risk): string {
  return risk === "workspace_edit" || risk === null ? ALWAYS_APPROVE_LABEL : `Always approve ${riskNoun(risk, "")}`;
}

export const ALWAYS_APPROVE_LABEL = "Always approve file edits";
export const ALWAYS_APPROVE_WHY = "Always approve lasts for this session and covers file edits only. You can undo it from the posture menu.";

/** Plural noun for a class: "file edits", "shell commands", "commits" (git_commit). */
export function riskNoun(risk: Risk, toolName: string): string {
  if (toolName === "git_commit") return "commits";
  switch (risk) {
    case "shell":
      return "shell commands";
    case "network":
      return "network calls";
    case "workspace_edit":
      return "file edits";
    case "read_only":
      return "reads";
    case "external_side_effect":
      return "actions outside the workspace";
    default:
      return "these calls";
  }
}

/** Why "Always approve" is greyed out, specific to the call's class (and to commits). */
export function alwaysApproveReason(risk: Risk, toolName = ""): string {
  if (toolName === "git_commit") return "Commits need a yes each time";
  switch (risk) {
    case "shell":
      return "Shell commands need a yes each time";
    case "network":
      return "Network calls need a yes each time";
    case "read_only":
    case null:
      return "Always approve covers file edits only";
    case "external_side_effect":
      return "Calls that act outside the workspace need a yes each time";
    default: {
      const noun = riskNoun(risk, "");
      return `${noun[0]!.toUpperCase()}${noun.slice(1)} need a yes each time`;
    }
  }
}

/** What a group's calls are, for accessible names: "shell commands", "file edits". */
export function callNoun(toolName: string, n: number): string {
  const [one, many] =
    toolName === "edit_file"
      ? ["file edit", "file edits"]
      : toolName === "write_file"
        ? ["file write", "file writes"]
        : toolName === "run_command"
          ? ["shell command", "shell commands"]
          : toolName === "git_commit"
            ? ["commit", "commits"]
            : [`${toolName} call`, `${toolName} calls`];
  return n === 1 ? one : many;
}

export type BadgeKind = "edit" | "shell" | "git" | "net" | "read" | "agent" | "art";

export function toolBadge(toolName: string, risk: Risk): { kind: BadgeKind; text: string } {
  if (toolName === "git_commit" || toolName.startsWith("git_")) return { kind: risk === "read_only" ? "read" : "git", text: "Git" };
  if (toolName === "run_command") return { kind: "shell", text: "Shell" };
  if (risk === "workspace_edit") return { kind: "edit", text: "Edit" };
  if (risk === "network") return { kind: "net", text: "Network" };
  if (toolName === "create_artifact") return { kind: "art", text: "Artifact" };
  if (toolName === SCRIPT_TOOL) return { kind: "read", text: "Script" };
  if (mcpTool(toolName)) return { kind: "net", text: "MCP" };
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
    case SCRIPT_TOOL: {
      const first = str(input, "script").split("\n").map((l) => l.trim()).find((l) => l && !l.startsWith("#")) ?? "";
      return short(first, 90);
    }
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
  if (toolName === SCRIPT_TOOL) return { kind: "args", text: str(input, "script") };
  return { kind: "args", text: `${toolDisplayName(toolName)}\n${JSON.stringify(input, null, 2)}` };
}

/** "Always approve" isn't offered at all for a script: its one approval covers its host calls. */
export function offersAlways(toolName: string): boolean {
  return toolName !== SCRIPT_TOOL;
}

export const SCRIPT_WHY = "Approving the script also lets it make its host calls (read-only workspace tools) without asking again.";

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
    case SCRIPT_TOOL:
      return "Run a script";
    default:
      return `Call ${toolDisplayName(toolName)}`;
  }
}
