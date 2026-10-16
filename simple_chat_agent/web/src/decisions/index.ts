// Eng W's decisions slot (PRD U4). The shell renders <DecisionDock session /> above the
// composer; the transcript renders <JudgeCard part evaluation /> (or <JudgeCards part />) with each tool row.
export { DecisionDock, ApprovalCard, PAUSED_LINE } from "./DecisionDock";
export { JudgeCard, JudgeCards, type JudgeCardProps, type JudgeCardsProps } from "./JudgeCard";
export { StopCards, latestStop } from "./StopCards";
export { judgeView, judgeViews, type JudgeView } from "./judge";
export { buildStack, reconcile, answersFrom, type Stack, type Group, type Fold, type ApprovalItem } from "./grouping";
// Send-state chips live in recovery/; re-exported for convenience.
export { SendStateTrail } from "../recovery/SendStateTrail";
