// @vitest-environment happy-dom
// Auto mode: a call the judge is still checking stays off the approval card until the judge
// escalates or fails; a judge approval means the card never shows it.
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import type { CodingSession, Evaluation, PendingApproval, ToolPart } from "../session";
import { DecisionDock } from "./DecisionDock";
import { isChecking } from "./grouping";

(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;

const ev = (status: Evaluation["status"], verdict: Evaluation["verdict"] = null, id = "ev1"): Evaluation => ({
  id,
  evaluator: "coding_judge",
  status,
  verdict,
  reason: null,
  details: {},
  error: status === "error" ? "judge timed out" : null,
});

/** A pending approval as the facade builds it: `judging` while the latest evaluation runs,
 *  `evaluation` the latest finished one. */
function call(toolId: string, state: ToolPart["state"], evaluations: Evaluation[], input: Record<string, unknown> = { command: "pytest -q" }): PendingApproval {
  const part: ToolPart = { type: "tool", toolId, toolName: "run_command", state, input, output: null, error: null, progress: null, approval: null, evaluations, callback: null };
  const latest = evaluations[evaluations.length - 1];
  const finished = [...evaluations].reverse().find((e) => e.status === "ended" || e.status === "error") ?? null;
  return { agentId: "root", messageId: "m1", part, riskClass: "shell", rememberAllowed: false, judging: latest?.status === "running", evaluation: finished };
}

function session(pending: PendingApproval[], calls: string[][] = []): CodingSession {
  return {
    sessionId: "s1",
    rootAgentId: "root",
    subagents: {},
    agent: () => undefined,
    messages: [],
    agentStatus: "busy",
    pendingApprovals: pending,
    sends: [],
    changes: null,
    workspace: null,
    telemetry: { worker: { answering: true, reason: null, since: null, pollers: 1, agentId: null, turnId: null }, recoveries: [] },
    approve: async (ids: readonly string[]) => {
      calls.push([...ids]);
      return [];
    },
    decline: async () => [],
  } as unknown as CodingSession;
}

describe("isChecking", () => {
  it("follows the facade's judging, true while the judge's latest evaluation runs", () => {
    expect(isChecking(call("a", "evaluating", [ev("running")]))).toBe(true);
    // The harness keeps the call `evaluating` after an escalation: not checking any more.
    expect(isChecking(call("a", "evaluating", [ev("ended", "escalate")]))).toBe(false);
    expect(isChecking(call("a", "awaiting_approval", [ev("error")]))).toBe(false);
    // A re-check after an escalation: checking again.
    expect(isChecking(call("a", "evaluating", [ev("ended", "escalate", "e1"), ev("running", null, "e2")]))).toBe(true);
    // Superseded by a second evaluation that is still running: still checking.
    expect(isChecking(call("a", "evaluating", [ev("superseded", null, "e1"), ev("running", null, "e2")]))).toBe(true);
    // No judge (Ask for everything, Trust safe tools): a plain approval.
    expect(isChecking(call("a", "awaiting_approval", []))).toBe(false);
  });
});

describe("the dock through the judge's transitions", () => {
  let host: HTMLDivElement;
  let root: Root;
  beforeEach(() => {
    host = document.createElement("div");
    document.body.appendChild(host);
    root = createRoot(host);
  });
  afterEach(() => {
    act(() => root.unmount());
    host.remove();
  });
  const render = (s: CodingSession) => act(() => root.render(<DecisionDock session={s} />));
  const cardShown = () => host.querySelector("section.dc-card") !== null;
  const checkingText = () => host.querySelector(".dc-checking")?.textContent ?? "";

  it("started: a quiet line, no card, no count, no pink", () => {
    render(session([call("c1", "evaluating", [ev("running")])]));
    expect(cardShown()).toBe(false);
    expect(checkingText()).toContain("The judge is checking run_command pytest -q…");
    expect(host.textContent).not.toMatch(/need approval|Needs approval/);
  });

  it("started → approved: the call settles and the card never appears", () => {
    render(session([call("c1", "evaluating", [ev("running")])]));
    render(session([]));
    expect(cardShown()).toBe(false);
    expect(host.querySelector(".dc-checking")).toBeNull();
  });

  it("started → escalated: the call joins the card as a normal pending call", () => {
    render(session([call("c1", "evaluating", [ev("running")])]));
    // The harness leaves the call `evaluating`; the facade's judging is false.
    render(session([call("c1", "evaluating", [ev("ended", "escalate")])]));
    expect(cardShown()).toBe(true);
    expect(host.querySelector("section.dc-card")!.textContent).toContain("Needs approval");
    expect(host.querySelector(".dc-checking")).toBeNull();
  });

  it("started → error: the call joins the card", () => {
    render(session([call("c1", "evaluating", [ev("running")])]));
    render(session([call("c1", "awaiting_approval", [ev("error")])]));
    expect(cardShown()).toBe(true);
  });

  it("a checked call doesn't count on a stacked card of other calls", () => {
    render(
      session([
        call("c1", "awaiting_approval", []),
        call("c2", "awaiting_approval", [], { command: "ls" }),
        call("c3", "evaluating", [ev("running")], { command: "pytest -q tests" }),
      ]),
    );
    expect([...host.querySelectorAll("button")].some((b) => b.textContent === "Approve all 2")).toBe(true);
    expect(checkingText()).toContain("pytest -q tests");
  });

  it("Answer it yourself answers before the judge", () => {
    const calls: string[][] = [];
    render(session([call("c1", "evaluating", [ev("running")])], calls));
    const details = host.querySelector<HTMLDetailsElement>(".dc-checking details")!;
    expect(details.querySelector("summary")!.textContent).toBe("Answer it yourself");
    act(() => {
      details.open = true;
    });
    act(() => host.querySelector<HTMLButtonElement>('.dc-checking button[aria-label^="Approve run_command"]')!.click());
    expect(calls).toEqual([["c1"]]);
  });
});
