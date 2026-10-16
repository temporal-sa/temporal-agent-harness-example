// What the transcript shows for a tool call: its badge, a one-line argument preview, command
// results, mini-diffs and path chips. Pure. Tool names and result text come from
// simple_chat_agent/agent/tools/workspace.py (`model_text()` is what reaches the stream).

import type { ToolPart } from "../session";

export type BadgeKind = "read" | "edit" | "shell" | "git" | "agent" | "good" | "prog";

export interface ToolBadge {
  label: string;
  kind: BadgeKind;
}

const BADGES: Record<string, ToolBadge> = {
  read_file: { label: "Read", kind: "read" },
  list_files: { label: "List", kind: "read" },
  search_code: { label: "Search", kind: "read" },
  read_attachment: { label: "Read", kind: "read" },
  update_plan: { label: "Plan", kind: "read" },
  fetch_url: { label: "Web", kind: "read" },
  write_file: { label: "Write", kind: "edit" },
  edit_file: { label: "Edit", kind: "edit" },
  import_attachment_to_workspace: { label: "Import", kind: "edit" },
  create_artifact: { label: "Artifact", kind: "edit" },
  run_command: { label: "Shell", kind: "shell" },
  git_status: { label: "Git", kind: "git" },
  git_diff: { label: "Git", kind: "git" },
  git_commit: { label: "Git", kind: "git" },
  wait_seconds: { label: "Wait", kind: "read" },
};

export function toolBadge(name: string): ToolBadge {
  return BADGES[name] ?? { label: "Tool", kind: "read" };
}

/** The parent's tools that drive subagents. The subagent row shows them instead. */
export const SUBAGENT_TOOLS: ReadonlySet<string> = new Set([
  "start_explorer",
  "explorer_investigate",
  "stop_explorer",
  "start_reviewer",
  "reviewer_review",
  "stop_reviewer",
]);

/** Read-only tools whose consecutive calls fold into one row ("Read file × 3"). */
export const GROUPABLE: Readonly<Record<string, string>> = {
  read_file: "Read file",
  list_files: "List files",
  search_code: "Search code",
  git_status: "Git status",
  read_attachment: "Read attachment",
};

const str = (input: Record<string, unknown>, key: string): string => {
  const v = input[key];
  return typeof v === "string" ? v : typeof v === "number" ? String(v) : "";
};

export function oneLine(s: string, max = 160): string {
  const flat = s.replace(/\s+/g, " ").trim();
  return flat.length > max ? `${flat.slice(0, max - 1)}…` : flat;
}

/** The argument preview beside the tool name. */
export function argPreview(part: Pick<ToolPart, "toolName" | "input">): string {
  const i = part.input ?? {};
  switch (part.toolName) {
    case "run_command":
      return oneLine(str(i, "command"));
    case "search_code":
      return `"${oneLine(str(i, "pattern"), 80)}"${str(i, "path") && str(i, "path") !== "." ? ` in ${str(i, "path")}` : ""}`;
    case "git_commit":
      return `"${oneLine(str(i, "message"), 120)}"`;
    case "git_diff":
      return str(i, "path") || (i.staged ? "staged" : "");
    case "update_plan": {
      const items = Array.isArray(i.items) ? i.items.length : 0;
      return items ? `${items} steps` : "";
    }
    case "fetch_url":
      return str(i, "url");
    case "read_attachment":
      return str(i, "attachment_id");
    case "wait_seconds":
      return `${str(i, "seconds")}s`;
    default:
      return str(i, "path") || str(i, "name");
  }
}

// -- run_command -------------------------------------------------------------------------------

export interface CommandSummary {
  exitCode: number | null;
  seconds: number | null;
  timedOut: boolean;
  reattached: boolean;
  /** The executor's id for the full log, when the result was cut short. */
  logRef: string | null;
  changed: { status: "A" | "M" | "D"; path: string }[];
}

/** Parse CommandResult.model_text(): "$ cmd\nexit 0 · 1.2s[ · timed out][ · reattached …]". */
export function commandSummary(output: string | null): CommandSummary | null {
  if (!output) return null;
  const lines = output.split("\n");
  const status = lines.find((l) => /^exit (\d+|\?) · /.test(l));
  if (!status) return null;
  const m = /^exit (\d+|\?) · ([\d.]+)s/.exec(status);
  const logRef = /\[output truncated; full log: ([^\]\s]+)\]/.exec(output)?.[1] ?? null;
  const changedLine = lines.find((l) => l.startsWith("changed: "));
  const changed: CommandSummary["changed"] = [];
  if (changedLine) {
    for (const item of changedLine.slice("changed: ".length).replace(/ \(\+\d+ more\)$/, "").split(", ")) {
      const cm = /^([AMD]) (.+)$/.exec(item.trim());
      if (cm?.[1] && cm[2]) changed.push({ status: cm[1] as "A" | "M" | "D", path: cm[2] });
    }
  }
  return {
    exitCode: m?.[1] && m[1] !== "?" ? Number(m[1]) : null,
    seconds: m?.[2] ? Number(m[2]) : null,
    timedOut: status.includes("timed out"),
    reattached: status.includes("reattached"),
    logRef,
    changed,
  };
}

// -- diffs -------------------------------------------------------------------------------------

export type DiffKind = "hunk" | "add" | "del" | "ctx";
export interface DiffRow {
  kind: DiffKind;
  text: string;
}

export function parseDiff(text: string): DiffRow[] {
  const rows: DiffRow[] = [];
  for (const line of text.replace(/\r\n/g, "\n").split("\n")) {
    if (line.startsWith("--- ") || line.startsWith("+++ ") || line.startsWith("\\")) continue;
    if (line.startsWith("@@")) rows.push({ kind: "hunk", text: line });
    else if (line.startsWith("+")) rows.push({ kind: "add", text: line.slice(1) });
    else if (line.startsWith("-")) rows.push({ kind: "del", text: line.slice(1) });
    else if (line.startsWith(" ")) rows.push({ kind: "ctx", text: line.slice(1) });
  }
  while (rows.length && rows[rows.length - 1]?.kind === "ctx" && rows[rows.length - 1]?.text === "") rows.pop();
  return rows;
}

export interface MiniDiff {
  rows: DiffRow[];
  added: number;
  removed: number;
  /** Rows left out of `rows` to keep the row short. */
  hidden: number;
}

const count = (rows: DiffRow[], kind: DiffKind) => rows.filter((r) => r.kind === kind).length;

function bounded(rows: DiffRow[], max: number, added?: number, removed?: number): MiniDiff {
  return {
    rows: rows.slice(0, max),
    added: added ?? count(rows, "add"),
    removed: removed ?? count(rows, "del"),
    hidden: Math.max(0, rows.length - max),
  };
}

/** The inline diff for an edit or write row: from edit_file's result text, a new file's
 *  content, or (while it waits for approval) the call's own arguments. */
export function miniDiff(part: Pick<ToolPart, "toolName" | "input" | "output" | "state">, max = 12): MiniDiff | null {
  const i = part.input ?? {};
  if (part.toolName === "edit_file") {
    const out = part.output ?? "";
    const nl = out.indexOf("\n");
    if (out.startsWith("Edited ") && nl > 0) {
      const head = /\(\+(\d+) -(\d+)\)/.exec(out.slice(0, nl));
      return bounded(parseDiff(out.slice(nl + 1)), max, head ? Number(head[1]) : undefined, head ? Number(head[2]) : undefined);
    }
    const oldS = str(i, "old_string");
    const newS = str(i, "new_string");
    if (!out && (oldS || newS)) {
      const rows: DiffRow[] = [
        ...(oldS ? oldS.split("\n").map((t) => ({ kind: "del" as const, text: t })) : []),
        ...(newS ? newS.split("\n").map((t) => ({ kind: "add" as const, text: t })) : []),
      ];
      return bounded(rows, max);
    }
    return null;
  }
  if (part.toolName === "write_file") {
    const out = part.output ?? "";
    const head = /\(\d+ bytes, \+(\d+) -(\d+)\)/.exec(out);
    const content = str(i, "content");
    const created = out.startsWith("Created ") || (!out && content !== "");
    if (created && content) {
      const rows = content.replace(/\n$/, "").split("\n").map((t) => ({ kind: "add" as const, text: t }));
      return bounded(rows, max, head ? Number(head[1]) : undefined, head ? Number(head[2]) : 0);
    }
    // A rewrite of an existing file carries edit_file's unified diff after its first line.
    const nl = out.indexOf("\n");
    if (head && nl > 0) return bounded(parseDiff(out.slice(nl + 1)), max, Number(head[1]), Number(head[2]));
    if (head) return { rows: [], added: Number(head[1]), removed: Number(head[2]), hidden: 0 };
  }
  return null;
}

/** Files a call touched or read, for path chips. */
export function pathsOf(part: Pick<ToolPart, "toolName" | "input" | "output">): string[] {
  const i = part.input ?? {};
  const paths = new Set<string>();
  if (["read_file", "write_file", "edit_file"].includes(part.toolName) && str(i, "path")) paths.add(str(i, "path"));
  if (part.toolName === "run_command") for (const c of commandSummary(part.output)?.changed ?? []) paths.add(c.path);
  return [...paths];
}

export function wordCount(text: string): number {
  const t = text.trim();
  return t ? t.split(/\s+/).length : 0;
}
