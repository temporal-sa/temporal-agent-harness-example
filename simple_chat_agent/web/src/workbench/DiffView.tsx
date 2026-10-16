// A unified diff. Additions are green with "+"; deletions are pink with "−" and a
// strikethrough. Red is never used for a diff (it is reserved for real errors). The marker
// and the <ins>/<del> elements carry the meaning, so it never rests on color alone.

import { marker, type DiffLine } from "./diff";

export interface DiffViewProps {
  lines: readonly DiffLine[];
  /** Accessible name, e.g. "Diff of refund_service/refunds.py". */
  label?: string;
  /** Draw a border (standalone use, e.g. inside an approval card). */
  framed?: boolean;
  truncated?: boolean;
}

export function DiffView({ lines, label, framed, truncated }: DiffViewProps) {
  return (
    <div className={`wb-diff${framed ? " framed" : ""}`} role="group" aria-label={label}>
      {lines.map((l, i) => {
        const m = marker(l.kind);
        switch (l.kind) {
          case "add":
            return (
              <ins key={i} className="add">
                <span className="mk" aria-hidden="true">{m}</span>
                <span className="sr-only">Added: </span>
                {l.text}
              </ins>
            );
          case "del":
            return (
              <del key={i} className="del">
                <span className="mk" aria-hidden="true">{m}</span>
                <span className="sr-only">Removed: </span>
                <span className="txt">{l.text}</span>
              </del>
            );
          case "hunk":
            return (
              <div key={i} className="h">
                {l.text}
              </div>
            );
          case "meta":
            return (
              <div key={i} className="meta">
                {l.text}
              </div>
            );
          default:
            return (
              <div key={i} className="ctx">
                <span className="mk" aria-hidden="true">{m}</span>
                {l.text}
              </div>
            );
        }
      })}
      {truncated && <div className="meta">The tool shortened this diff.</div>}
    </div>
  );
}
