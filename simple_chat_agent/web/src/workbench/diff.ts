// Unified diffs for the Changes tab and the approval card. Pure.
//
// Sources, in the order the agent produces them:
// * edit_file's result text is "Edited <path> (+a -r).\n<unified diff>";
// * write_file's result has no diff text, so a new file is shown from its content and a
//   rewrite shows its counts only;
// * a pending edit_file or write_file is previewed from its arguments.

export type DiffLineKind = "hunk" | "add" | "del" | "ctx" | "meta";

export interface DiffLine {
  kind: DiffLineKind;
  /** The line without its leading marker. */
  text: string;
}

export interface FileDiff {
  path: string;
  lines: DiffLine[];
  added: number;
  removed: number;
  /** The tool cut the diff short. */
  truncated: boolean;
}

/** Parse unified diff text. File headers ("--- a/x", "+++ b/x") are dropped. */
export function parseUnifiedDiff(text: string): DiffLine[] {
  const out: DiffLine[] = [];
  const rows = text.replace(/\r\n/g, "\n").split("\n");
  if (rows.length && rows[rows.length - 1] === "") rows.pop();
  for (const row of rows) {
    if (row.startsWith("--- ") || row.startsWith("+++ ") || row.startsWith("diff --git") || row.startsWith("index ")) continue;
    if (row.startsWith("@@")) out.push({ kind: "hunk", text: row });
    else if (row.startsWith("+")) out.push({ kind: "add", text: row.slice(1) });
    else if (row.startsWith("-")) out.push({ kind: "del", text: row.slice(1) });
    else if (row.startsWith(" ")) out.push({ kind: "ctx", text: row.slice(1) });
    else if (row.startsWith("\\")) out.push({ kind: "meta", text: row });
    else if (row === "") out.push({ kind: "ctx", text: "" });
    else out.push({ kind: "meta", text: row });
  }
  return out;
}

export function countLines(lines: readonly DiffLine[]): { added: number; removed: number } {
  let added = 0;
  let removed = 0;
  for (const l of lines) {
    if (l.kind === "add") added++;
    else if (l.kind === "del") removed++;
  }
  return { added, removed };
}

const TRUNCATION = /\[?diff truncated\]?|…\s*\(diff truncated\)/i;

/** The diff inside an edit_file result ("Edited p (+1 -1).\n@@ ..."), or null. */
export function diffFromEditOutput(output: string | null): { lines: DiffLine[]; truncated: boolean } | null {
  if (!output) return null;
  const nl = output.indexOf("\n");
  if (nl < 0) return null;
  const body = output.slice(nl + 1);
  if (!/^(@@|---|\+\+\+)/m.test(body)) return null;
  return { lines: parseUnifiedDiff(body).filter((l) => !(l.kind === "meta" && TRUNCATION.test(l.text))), truncated: TRUNCATION.test(body) };
}

function splitLines(s: string): string[] {
  const rows = s.replace(/\r\n/g, "\n").split("\n");
  if (rows.length > 1 && rows[rows.length - 1] === "") rows.pop();
  return rows;
}

/** A preview of an edit_file call from its arguments: old lines out, new lines in. */
export function editPreview(oldString: string, newString: string, maxLines = 40): DiffLine[] {
  const before = splitLines(oldString);
  const after = splitLines(newString);
  // Trim shared leading and trailing lines into context so the change stands out.
  let head = 0;
  while (head < before.length && head < after.length && before[head] === after[head]) head++;
  let tail = 0;
  while (
    tail < before.length - head &&
    tail < after.length - head &&
    before[before.length - 1 - tail] === after[after.length - 1 - tail]
  )
    tail++;
  const lines: DiffLine[] = [
    ...before.slice(0, head).map((text) => ({ kind: "ctx" as const, text })),
    ...before.slice(head, before.length - tail).map((text) => ({ kind: "del" as const, text })),
    ...after.slice(head, after.length - tail).map((text) => ({ kind: "add" as const, text })),
    ...before.slice(before.length - tail).map((text) => ({ kind: "ctx" as const, text })),
  ];
  return clip(lines, maxLines);
}

/** A preview of a new file: every line added. */
export function addedFilePreview(content: string, maxLines = 40): DiffLine[] {
  return clip(
    splitLines(content).map((text) => ({ kind: "add" as const, text })),
    maxLines,
  );
}

function clip(lines: DiffLine[], maxLines: number): DiffLine[] {
  if (lines.length <= maxLines) return lines;
  return [...lines.slice(0, maxLines), { kind: "meta", text: `… ${lines.length - maxLines} more lines` }];
}

/** The marker shown before a line. Deletions use the minus sign, not a hyphen. */
export function marker(kind: DiffLineKind): string {
  switch (kind) {
    case "add":
      return "+";
    case "del":
      return "−";
    case "ctx":
      return " ";
    default:
      return "";
  }
}
