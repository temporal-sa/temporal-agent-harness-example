// Send-state chips for one local action. Each step has its own text, and the current one is
// filled and marked aria-current, so the state never rests on color alone.

import { useEffect, useState } from "react";

import type { SendStateTrailProps as SlotProps } from "../session";
import { failedText, trailSteps, WAITING_FOR_WORKER_MS } from "./sendTrail";
import "./recovery.css";

export interface SendStateTrailProps extends SlotProps {
  /** From `session.telemetry.worker.answering`: say "waiting for a worker" at once. */
  workerAnswering?: boolean;
}

export function SendStateTrail({ record, workerAnswering }: SendStateTrailProps) {
  const [now, setNow] = useState(() => Date.now());
  const waiting = record.phase === "in_temporal";
  useEffect(() => {
    if (!waiting) return;
    const since = record.at.in_temporal ?? Date.now();
    const left = Math.max(0, since + WAITING_FOR_WORKER_MS - Date.now());
    const t = window.setTimeout(() => setNow(Date.now()), left + 50);
    return () => window.clearTimeout(t);
  }, [waiting, record.at.in_temporal]);

  const failed = failedText(record);
  if (failed) {
    return (
      <p className="sendstate failed" role="alert">
        {failed}
      </p>
    );
  }
  const steps = trailSteps(record, { now, ...(workerAnswering === undefined ? {} : { workerAnswering }) });
  return (
    <ol className="sendstate" aria-label="Delivery">
      {steps.map((s, i) => (
        <li key={s.phase} className={s.state} aria-current={s.state === "now" ? "step" : undefined}>
          {i > 0 && (
            <span className="arrow" aria-hidden="true">
              →
            </span>
          )}
          <span className="chip">
            {s.state === "done" && <span className="sr-only">Done: </span>}
            {s.state === "now" && <span className="sr-only">Now: </span>}
            {s.label}
          </span>
        </li>
      ))}
    </ol>
  );
}
