// Copy for the stacked approval card. Pure.

import { count } from "../recovery/format";
import { argSummary } from "./describe";
import type { Fold, Group } from "./grouping";

/** A group's title. `code` is shown in mono; `fold` is the "× n, same …" hint. */
export interface GroupTitle {
  text: string;
  code: string | null;
  fold: string | null;
}

function distinctPaths(fold: readonly Fold[]): number {
  const paths = new Set<string>();
  for (const f of fold) for (const i of f.waiting) paths.add(String(i.input.path ?? ""));
  return paths.size;
}

export function groupTitle(group: Group): GroupTitle {
  const live = group.folds.filter((f) => f.waiting.length > 0);
  const n = group.waitingIds.length;
  if (n === 0) return { text: "All answered", code: null, fold: null };
  const same = live.length === 1 ? live[0]! : null;
  const foldHint = (what: string) => (same && n > 1 ? `× ${n}, same ${what}` : null);
  switch (group.toolName) {
    case "edit_file":
    case "write_file": {
      const verb = group.toolName === "edit_file" ? "Edit" : "Write";
      const paths = distinctPaths(live);
      if (same && n > 1) return { text: verb, code: String(same.input.path ?? ""), fold: foldHint("change") };
      return paths === n
        ? { text: `${verb} ${count(n, "file")}`, code: null, fold: null }
        : { text: `${count(n, verb === "Edit" ? "edit" : "write")} to ${count(paths, "file")}`, code: null, fold: null };
    }
    case "run_command":
      return same
        ? { text: "Run", code: argSummary("run_command", same.input), fold: foldHint("command") }
        : { text: `Run ${count(n, "command")}`, code: null, fold: null };
    case "git_commit":
      return same
        ? { text: "Commit", code: argSummary("git_commit", same.input), fold: foldHint("commit") }
        : { text: count(n, "commit"), code: null, fold: null };
    default:
      return same
        ? { text: group.toolName, code: argSummary(group.toolName, same.input) || null, fold: foldHint("arguments") }
        : { text: `${group.toolName} × ${n}`, code: null, fold: null };
  }
}

/** The card's heading: "5 tool calls are waiting". */
export function stackHeading(waiting: number): string {
  if (waiting === 0) return "All answered";
  return waiting === 1 ? "1 tool call is waiting" : `${waiting} tool calls are waiting`;
}

/** The answered badge: "Approved by you" / "Declined by you" / "Always approved by you". */
export function answeredText(approved: boolean, remember: boolean): string {
  if (!approved) return "Declined by you";
  return remember ? "Always approved by you" : "Approved by you";
}
