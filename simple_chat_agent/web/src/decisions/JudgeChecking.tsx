// Calls the Auto-mode judge is still checking (about 3 s with Haiku). They aren't waiting on a
// person yet, so they get a quiet line instead of the pink approval card: "The judge is
// checking run_command pytest -q…". "Answer it yourself" lets someone answer before the judge.
// When the judge escalates (or fails) the call moves to the card; when it approves, the
// transcript's judge card shows the verdict and this line goes away.

import { useMemo } from "react";

import type { CodingSession } from "../session";
import { argSummary } from "./describe";
import { answersFrom, isAnswered, isChecking } from "./grouping";
import { answeredText } from "./labels";
import "./decisions.css";

export function checkingLine(toolName: string, input: Record<string, unknown>): string {
  const args = argSummary(toolName, input);
  return `The judge is checking ${toolName}${args ? ` ${args}` : ""}…`;
}

export function JudgeChecking({ session }: { session: CodingSession }) {
  const checking = session.pendingApprovals.filter(isChecking);
  const answers = useMemo(() => answersFrom(session.sends), [session.sends]);
  if (checking.length === 0) return null;
  return (
    <ul className="dc-checking" aria-label="Calls the judge is checking">
      {checking.map((p) => {
        const { toolId, toolName, input } = p.part;
        const a = answers.get(toolId);
        const label = argSummary(toolName, input) || toolName;
        return (
          <li key={toolId} className="row">
            <span className="what" role="status">
              {checkingLine(toolName, input)}
            </span>
            {isAnswered(a) && a ? (
              <span className={`badge ${a.approved ? "good" : "leaving"}`}>{answeredText(a.approved, a.record.remember)}</span>
            ) : (
              <details className="yourself">
                <summary>Answer it yourself</summary>
                <span className="acts">
                  <button type="button" className="btn green small" aria-label={`Approve ${toolName}: ${label}`} onClick={() => void session.approve([toolId])}>
                    Approve
                  </button>
                  <button type="button" className="btn ghost small" aria-label={`Decline ${toolName}: ${label}`} onClick={() => void session.decline([toolId])}>
                    Decline
                  </button>
                </span>
              </details>
            )}
          </li>
        );
      })}
    </ul>
  );
}
