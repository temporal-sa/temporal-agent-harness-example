import { describe, expect, it } from "vitest";

import type { PendingApproval, SendRecord, ToolPart } from "../session";
import { alwaysApproveReason } from "./describe";
import {
  answersFrom,
  approveAllTargets,
  buildStack,
  cardItems,
  EMPTY_CARD,
  groupTargets,
  isAnswered,
  itemOf,
  reconcile,
  type ApprovalItem,
  type CardState,
} from "./grouping";
import { groupTitle, stackHeading } from "./labels";

// What the facade sets from config (tool_risk_classes, remember_allowed_classes).
const CONFIG: Record<string, string> = { edit_file: "workspace_edit", write_file: "workspace_edit", run_command: "shell", git_commit: "shell" };

function pending(toolId: string, toolName: string, input: Record<string, unknown>, agentId = "root"): PendingApproval {
  const riskClass = CONFIG[toolName] ?? "external_side_effect";
  const part: ToolPart = {
    type: "tool",
    toolId,
    toolName,
    state: "awaiting_approval",
    input,
    output: null,
    error: null,
    progress: null,
    approval: null,
    evaluations: [],
    callback: null,
  };
  return { agentId, messageId: "m1", part, riskClass, rememberAllowed: riskClass === "workspace_edit" };
}

const edit = (id: string, path: string) => itemOf(pending(id, "edit_file", { path, old_string: "a", new_string: "b" }));
const pytest = (id: string, agent = "root") => itemOf(pending(id, "run_command", { command: "pytest -q" }, agent));

function sent(toolId: string, approved: boolean, phase: SendRecord["phase"] = "sending"): SendRecord {
  return { id: `s-${toolId}`, kind: "approval", messageId: null, toolId, approved, remember: false, phase, at: { sending: 1 }, error: null };
}

/** The decision-B scenario: 3 edits and 2 identical commands (validation step 3e). */
const FIVE: ApprovalItem[] = [edit("e1", "refund_service/refunds.py"), pytest("c1", "explorer-1"), edit("e2", "tests/test_refund.py"), pytest("c2", "explorer-2"), edit("e3", "refund_service/money.py")];

describe("grouping and folding", () => {
  it("puts five concurrent calls on one card in two groups, folding identical commands to × 2", () => {
    const s = buildStack(FIVE, new Map());
    expect(s.total).toBe(5);
    expect(s.waitingIds).toEqual(["e1", "c1", "e2", "c2", "e3"]);
    expect(s.groups.map((g) => g.toolName)).toEqual(["edit_file", "run_command"]);
    const [edits, cmds] = s.groups;
    expect(edits!.folds).toHaveLength(3);
    expect(cmds!.folds).toHaveLength(1);
    expect(cmds!.folds[0]!.items.map((i) => i.toolId)).toEqual(["c1", "c2"]);
    expect(groupTitle(edits!)).toEqual({ text: "Edit 3 files", code: null, fold: null });
    expect(groupTitle(cmds!)).toEqual({ text: "Run", code: "pytest -q", fold: "× 2, same arguments" });
    expect(stackHeading(s.waitingIds.length)).toBe("5 tool calls are waiting");
  });

  it("folds on identical arguments regardless of key order, and not on different ones", () => {
    const a = itemOf(pending("a", "run_command", { command: "pytest -q", cwd: "." }));
    const b = itemOf(pending("b", "run_command", { cwd: ".", command: "pytest -q" }));
    const c = itemOf(pending("c", "run_command", { command: "pytest -q tests/test_refund.py" }));
    const s = buildStack([a, b, c], new Map());
    expect(s.groups).toHaveLength(1);
    expect(s.groups[0]!.folds.map((f) => f.items.length)).toEqual([2, 1]);
    expect(groupTitle(s.groups[0]!)).toEqual({ text: "Run 3 commands", code: null, fold: null });
  });

  it("groups mixed tools by tool name, in arrival order", () => {
    const commit = itemOf(pending("g1", "git_commit", { message: "Fix refund" }));
    const write = itemOf(pending("w1", "write_file", { path: "NOTES.md", content: "x" }));
    const s = buildStack([commit, edit("e1", "a.py"), write, edit("e2", "b.py")], new Map());
    expect(s.groups.map((g) => [g.toolName, g.waitingIds])).toEqual([
      ["git_commit", ["g1"]],
      ["edit_file", ["e1", "e2"]],
      ["write_file", ["w1"]],
    ]);
  });

  it("offers Always approve where the facade allows it (file-edit groups only)", () => {
    const s = buildStack([edit("e1", "a.py"), pytest("c1"), itemOf(pending("g1", "git_commit", { message: "m" }))], new Map());
    const live = s.groups.map((g) => g.rememberAllowed);
    expect(live).toEqual([true, false, false]);
    // The greyed reason is specific to the class, and to commits (UX review nit).
    expect(alwaysApproveReason(s.groups[1]!.risk, s.groups[1]!.toolName)).toBe("Shell commands need a yes each time");
    expect(alwaysApproveReason(s.groups[2]!.risk, s.groups[2]!.toolName)).toBe("Commits need a yes each time");
    expect(alwaysApproveReason("network", "fetch_url")).toBe("Network calls need a yes each time");
  });
});

describe("risk class from the facade", () => {
  it("doesn't offer Always approve while config hasn't loaded", () => {
    const raw = pending("e1", "edit_file", { path: "a.py" });
    const item = itemOf({ agentId: raw.agentId, messageId: raw.messageId, part: raw.part, riskClass: null, rememberAllowed: false });
    expect(item).toMatchObject({ risk: null, rememberAllowed: false });
    expect(buildStack([item], new Map()).groups[0]!.rememberAllowed).toBe(false);
  });
});

describe("partial answers", () => {
  it("takes an answered call out of its group's counts while it stays on the card", () => {
    const answers = answersFrom([sent("e3", false)]);
    const s = buildStack(FIVE, answers);
    const edits = s.groups[0]!;
    expect(edits.total).toBe(3);
    expect(groupTargets(edits)).toEqual(["e1", "e2"]);
    expect(groupTitle(edits).text).toBe("Edit 2 files");
    expect(s.waitingIds).toEqual(["e1", "c1", "e2", "c2"]);
  });

  it("answering one of two identical calls leaves × 1 waiting in the fold", () => {
    const s = buildStack(FIVE, answersFrom([sent("c1", true)]));
    const cmds = s.groups[1]!;
    expect(cmds.folds[0]!.items).toHaveLength(2);
    expect(cmds.folds[0]!.waiting.map((i) => i.toolId)).toEqual(["c2"]);
    expect(groupTitle(cmds)).toEqual({ text: "Run", code: "pytest -q", fold: null });
  });

  it("a failed answer puts the call back in the counts", () => {
    const answers = answersFrom([{ ...sent("e1", true), phase: "failed", error: "503" }]);
    expect(isAnswered(answers.get("e1"))).toBe(false);
    expect(buildStack(FIVE, answers).waitingIds).toContain("e1");
  });

  it("uses the latest send per tool id", () => {
    const answers = answersFrom([{ ...sent("e1", true), phase: "failed" }, { ...sent("e1", true), id: "s2", phase: "in_temporal" }]);
    expect(answers.get("e1")!.phase).toBe("in_temporal");
  });
});

describe("Approve all", () => {
  it("answers only calls still waiting", () => {
    const answers = answersFrom([sent("e2", false), sent("c1", true, "accepted")]);
    expect(approveAllTargets(buildStack(FIVE, answers))).toEqual(["e1", "c2", "e3"]);
  });

  it("answers nothing when everything is answered", () => {
    const answers = answersFrom(FIVE.map((i) => sent(i.toolId, true)));
    const s = buildStack(FIVE, answers);
    expect(approveAllTargets(s)).toEqual([]);
    expect(stackHeading(0)).toBe("All answered");
  });

  it("never includes a call held back while the pointer was on the card", () => {
    let card = reconcile(EMPTY_CARD, FIVE.slice(0, 2), () => false, false);
    card = reconcile(card, FIVE.slice(0, 3), () => false, true);
    expect(card.held).toEqual(["e2"]);
    expect(approveAllTargets(buildStack(cardItems(card), new Map()))).toEqual(["e1", "c1"]);
  });
});

describe("arrivals", () => {
  const step = (prev: CardState, items: ApprovalItem[], hold = false, answered: string[] = []) =>
    reconcile(prev, items, (id) => answered.includes(id), hold);

  it("appends new calls to the end and keeps existing positions", () => {
    let card = step(EMPTY_CARD, [FIVE[0]!, FIVE[1]!]);
    expect(card.order).toEqual(["e1", "c1"]);
    // The stream lists pending calls in its own order; the card keeps its own.
    card = step(card, [FIVE[2]!, FIVE[1]!, FIVE[0]!]);
    expect(card.order).toEqual(["e1", "c1", "e2"]);
    // A new edit joins the edit group, which keeps its place above the commands.
    const s = buildStack(cardItems(card), new Map());
    expect(s.groups.map((g) => g.waitingIds)).toEqual([["e1", "e2"], ["c1"]]);
  });

  it("holds arrivals while the pointer is on the card and adds them when it leaves", () => {
    let card = step(EMPTY_CARD, FIVE.slice(0, 3));
    card = step(card, FIVE, true);
    expect(card.order).toEqual(["e1", "c1", "e2"]);
    expect(card.held).toEqual(["c2", "e3"]);
    card = step(card, FIVE, false);
    expect(card.order).toEqual(["e1", "c1", "e2", "c2", "e3"]);
    expect(card.held).toEqual([]);
  });

  it("doesn't hold the first calls of an empty card", () => {
    const card = step(EMPTY_CARD, FIVE.slice(0, 2), true);
    expect(card.order).toEqual(["e1", "c1"]);
  });

  it("keeps a call answered here in place until nothing on the card is pending", () => {
    let card = step(EMPTY_CARD, FIVE);
    // e1 was answered here and the agent settled it: it leaves the pending list.
    card = step(card, FIVE.slice(1), false, ["e1"]);
    expect(card.order[0]).toBe("e1");
    expect(cardItems(card)[0]!.toolId).toBe("e1");
    // A call settled elsewhere (another tab, the judge) leaves the card.
    card = step(card, FIVE.slice(2), false, ["e1"]);
    expect(card.order).toEqual(["e1", "e2", "c2", "e3"]);
    // Nothing pending: the card empties.
    card = step(card, [], false, ["e1", "e2", "c2", "e3"]);
    expect(card.order).toEqual([]);
  });
});
