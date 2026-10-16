// The recovery divider (PRD U6), rendered by the transcript where a worker was lost.

import { useId } from "react";

import type { RecoveryDividerProps } from "../session";
import { COMPARISON, dividerView } from "./divider";
import "./recovery.css";

export function RecoveryDivider({ recovery }: RecoveryDividerProps) {
  const v = dividerView(recovery);
  const tip = useId();
  return (
    <section className="rc-divider" aria-label={v.title} aria-busy={v.checking || undefined}>
      <div className="t">
        <span>{v.title}</span>
        <span className="i" id={tip} title={`Kept and repeated work is ${COMPARISON}.`}>
          <span aria-hidden="true">ⓘ </span>
          {COMPARISON}
        </span>
      </div>
      <p className="b">{v.body}</p>
      <ul className="kept" aria-describedby={tip} aria-live="polite">
        {v.figures.map((f, i) => (
          <li key={i} className={f.tone}>
            {f.label} <b className={v.checking ? "checking" : undefined}>{f.value}</b>
          </li>
        ))}
      </ul>
    </section>
  );
}
