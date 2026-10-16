// Judge evaluation cards, inline in the transcript above the tool they decided. The transcript
// renders <JudgeCard part evaluation /> for one evaluation, or <JudgeCards part /> for every
// evaluation of a call (nothing when it had none).

import type { JudgeCardProps as SlotProps, ToolPart } from "../session";
import { judgeView, judgeViews, type JudgeView } from "./judge";
import "./decisions.css";

export type JudgeCardProps = SlotProps;

export function JudgeCard({ part, evaluation }: JudgeCardProps) {
  const v = judgeView(part, evaluation);
  return v ? <JudgeCardView view={v} /> : null;
}

export interface JudgeCardsProps {
  part: Pick<ToolPart, "toolName" | "input" | "evaluations">;
}

export function JudgeCards({ part }: JudgeCardsProps) {
  const views = judgeViews(part);
  if (!views.length) return null;
  return (
    <>
      {views.map((v) => (
        <JudgeCardView key={v.id} view={v} />
      ))}
    </>
  );
}

function JudgeCardView({ view }: { view: JudgeView }) {
  return (
    <div className={`dc-judge ${view.tone}`} role="note" aria-label={`${view.badge}: ${view.tool}`}>
      <span className="badge">{view.badge}</span>
      <span className="what">
        {view.tool} {view.args && <span className="mono args">{view.args}</span>}
      </span>
      {view.score ? <span className="score">{view.score}</span> : <span />}
      <span className="why">{view.why}</span>
    </div>
  );
}
