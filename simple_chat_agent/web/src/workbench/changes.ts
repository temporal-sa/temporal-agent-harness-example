// The Changes tab's model: each changed file with the diffs the agent's edits produced, and
// the workspace facts (branch, last command, exit). Pure.

import type { ChangesState, WorkspaceState } from "../session";
import { addedFilePreview, countLines, diffFromEditOutput, type DiffLine } from "./diff";
import { strArg, type LocatedTool } from "./tree";

export interface FileChanges {
  path: string;
  status: string;
  added: number;
  removed: number;
  /** One block per edit that reported a diff, oldest first. Empty when none did. */
  hunks: { toolId: string; lines: DiffLine[]; truncated: boolean }[];
  /** Edits whose diff the tool didn't report (a rewrite with write_file). */
  unshown: number;
}

const EDIT_TOOLS = new Set(["edit_file", "write_file"]);

/** Normalise "./a/b.py" and "a//b.py" so tool paths match the Changes state's paths. */
export function normPath(p: string): string {
  return p.replace(/^\.\/+/, "").replace(/\/{2,}/g, "/");
}

export function fileChanges(changes: ChangesState | null, tools: readonly LocatedTool[]): FileChanges[] {
  const byPath = new Map<string, FileChanges>();
  for (const f of changes?.files ?? []) {
    byPath.set(normPath(f.path), { path: f.path, status: f.status, added: f.added, removed: f.removed, hunks: [], unshown: 0 });
  }
  for (const t of tools) {
    const p = t.part;
    if (!EDIT_TOOLS.has(p.toolName) || p.state !== "done") continue;
    const path = normPath(strArg(p.input, "path"));
    const entry = byPath.get(path);
    if (!entry) continue;
    if (p.toolName === "edit_file") {
      const d = diffFromEditOutput(p.output);
      if (d && d.lines.length) entry.hunks.push({ toolId: p.toolId, lines: d.lines, truncated: d.truncated });
      else entry.unshown++;
    } else if (/^Created /.test(p.output ?? "")) {
      entry.hunks.push({ toolId: p.toolId, lines: addedFilePreview(strArg(p.input, "content"), 400), truncated: false });
    } else {
      entry.unshown++;
    }
  }
  return [...byPath.values()];
}

/** "+3 −1" from a file's counts (or its hunks when the state reports none). */
export function fileStat(f: FileChanges): { added: number; removed: number } {
  if (f.added || f.removed) return { added: f.added, removed: f.removed };
  return f.hunks.reduce(
    (acc, h) => {
      const c = countLines(h.lines);
      return { added: acc.added + c.added, removed: acc.removed + c.removed };
    },
    { added: 0, removed: 0 },
  );
}

/** The branch from the latest git_status result ("On branch main"), if any. */
export function branchFrom(tools: readonly LocatedTool[]): string | null {
  let branch: string | null = null;
  for (const t of tools) {
    if (t.part.toolName !== "git_status" || t.part.state !== "done") continue;
    const m = /^On branch (\S+)/.exec(t.part.output ?? "");
    if (m) branch = m[1] ?? null;
  }
  return branch;
}

export interface WorkspaceFacts {
  branch: string | null;
  filesChanged: number;
  lastCommand: string | null;
  exit: string;
}

export function workspaceFacts(
  workspace: WorkspaceState | null,
  changes: ChangesState | null,
  tools: readonly LocatedTool[],
): WorkspaceFacts {
  const exitCode = workspace?.last_exit_code;
  return {
    branch: branchFrom(tools),
    filesChanged: changes?.files.length ?? 0,
    lastCommand: workspace?.last_command || null,
    exit: !workspace?.last_command ? "none yet" : exitCode === null || exitCode === undefined ? "not reported" : String(exitCode),
  };
}

/** The Changes entry a transcript path chip points at: an exact match after normalising,
 *  else the one file whose path ends with the chip's path (a chip may show a shorter path). */
export function focusedFile(files: readonly Pick<FileChanges, "path">[], path: string): string | null {
  const want = normPath(path);
  if (!want) return null;
  const exact = files.find((f) => normPath(f.path) === want);
  if (exact) return exact.path;
  const tail = files.filter((f) => normPath(f.path).endsWith(`/${want}`));
  return tail.length === 1 ? tail[0]!.path : null;
}
