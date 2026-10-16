// @vitest-environment happy-dom
// Regression tests for the P3 UX review items on the approval card (M1–M3, M6, m4, m5, nits).
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { renderToStaticMarkup } from "react-dom/server";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import type { CodingSession, PendingApproval, SendRecord, ToolPart } from "../session";
import { DECLINE_PLACEHOLDER, DecisionDock } from "./DecisionDock";
import { groupTitle } from "./labels";
import { buildStack, itemOf } from "./grouping";

(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;

const RISK: Record<string, string> = { edit_file: "workspace_edit", run_command: "shell", git_commit: "shell" };

function tool(toolId: string, toolName: string, input: Record<string, unknown>): ToolPart {
  return { type: "tool", toolId, toolName, state: "awaiting_approval", input, output: null, error: null, progress: null, approval: null, evaluations: [], callback: null };
}

function waiting(parts: ToolPart[], agentId = "root"): PendingApproval[] {
  return parts.map((part) => ({ agentId, messageId: "m1", part, riskClass: RISK[part.toolName] ?? null, rememberAllowed: RISK[part.toolName] === "workspace_edit" }));
}

function sent(toolId: string, approved = true): SendRecord {
  return { id: `s-${toolId}`, kind: "approval", messageId: null, toolId, approved, remember: false, phase: "in_temporal", at: { sending: 1, in_temporal: 2 }, error: null };
}

function session(pending: PendingApproval[], sends: SendRecord[] = [], calls: { approve: string[][] } = { approve: [] }): CodingSession {
  return {
    sessionId: "s1",
    rootAgentId: "root",
    subagents: {},
    agent: (id: string) => (id === "explorer-1" ? { agentKey: "explorer" } : undefined),
    messages: [],
    agentStatus: "busy",
    pendingApprovals: pending,
    sends,
    changes: null,
    workspace: null,
    telemetry: { worker: { answering: true, reason: null, since: null, pollers: 1, agentId: null, turnId: null }, recoveries: [] },
    approve: async (ids: readonly string[]) => {
      calls.approve.push([...ids]);
      return [];
    },
    decline: async () => [],
  } as unknown as CodingSession;
}

const E1 = tool("e1", "edit_file", { path: "refund_service/refunds.py", old_string: "a", new_string: "b" });
const E2 = tool("e2", "edit_file", { path: "tests/test_refund.py", old_string: "", new_string: "x" });
const C1 = tool("c1", "run_command", { command: "pytest -q" });
const C2 = tool("c2", "run_command", { command: "pytest -q" });
const C3 = tool("c3", "run_command", { command: "pytest -q" });

describe("approval card in a browser", () => {
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
  const card = () => host.querySelector<HTMLElement>("section.dc-card")!;
  const button = (name: string | RegExp) =>
    [...host.querySelectorAll<HTMLButtonElement>("button")].find((b) => {
      const n = b.getAttribute("aria-label") ?? b.textContent ?? "";
      return typeof name === "string" ? n === name : name.test(n);
    });

  it("M1b: holds an arrival while keyboard focus is in the card, then adds it when focus leaves", () => {
    render(session(waiting([E1, E2, C1])));
    act(() => button("Approve all 3")!.focus());
    render(session(waiting([E1, E2, C1, C2])));
    expect(button("Approve all 3")).toBeTruthy();
    expect(host.querySelector('[role="status"].held')!.textContent).toContain("1 more call is waiting.");
    // Focus moves out of the card: the call joins.
    const outside = document.createElement("button");
    document.body.appendChild(outside);
    act(() => outside.focus());
    render(session(waiting([E1, E2, C1, C2])));
    expect(button("Approve all 4")).toBeTruthy();
    outside.remove();
  });

  it("M1a: an answered row keeps its place with a badge only; the card shows one trail, in its footer", () => {
    const pending = waiting([E1, E2, C1]);
    render(session(pending));
    act(() => host.querySelector<HTMLButtonElement>('[data-row="g:edit_file"] [data-group-toggle]')!.click());
    render(session(pending, [sent("e1")]));
    const row = host.querySelector('[data-row="i:e1"]')!;
    expect(row.textContent).toContain("Approved by you");
    expect(row.querySelector(".sendstate")).toBeNull();
    expect(card().querySelectorAll(".sendstate")).toHaveLength(1);
    expect(card().querySelector(".dc-foot .sendstate")).not.toBeNull();
    // The footer is there before any answer, so answering doesn't add a row.
    expect(card().lastElementChild!.className).toBe("dc-foot");
  });

  it("M3: after answering one call, focus moves to the next waiting call, not to <body>", () => {
    const pending = waiting([E1, E2, C1, C2]);
    const calls = { approve: [] as string[][] };
    render(session(pending, [], calls));
    act(() => host.querySelector<HTMLButtonElement>('[data-row="g:edit_file"] [data-group-toggle]')!.click());
    const first = button("Approve file edit: refund_service/refunds.py")!;
    act(() => first.focus());
    act(() => first.click());
    expect(calls.approve).toEqual([["e1"]]);
    render(session(pending, [sent("e1")], calls));
    expect(document.activeElement).toBe(button("Approve file edit: tests/test_refund.py"));
  });

  it("M3: after a group answer, focus moves to the next group's Approve", () => {
    const pending = waiting([E1, E2, C1, C2]);
    render(session(pending));
    const edits = button("Approve 2 file edits")!;
    act(() => edits.focus());
    act(() => edits.click());
    render(session(pending, [sent("e1"), sent("e2")]));
    expect(document.activeElement).toBe(button("Approve 2 shell commands"));
  });

  it("M3: after the last answer, focus goes to the card heading", () => {
    const pending = waiting([E1, C1]);
    render(session(pending));
    const all = button("Approve all 2")!;
    act(() => all.focus());
    act(() => all.click());
    render(session(pending, [sent("e1"), sent("c1")]));
    expect(document.activeElement?.hasAttribute("data-heading")).toBe(true);
  });

  it("m5: a group of identical calls opens straight to its calls, with no second level", () => {
    render(session(waiting([E1, C1, C2, C3])));
    act(() => host.querySelector<HTMLButtonElement>('[data-row="g:run_command"] [data-group-toggle]')!.click());
    const group = host.querySelector('[data-row="g:run_command"]')!.closest("li")!;
    // One toggle (the group's), then the three calls with their own answers.
    expect(group.querySelectorAll("[aria-expanded]").length).toBe(2); // the group toggle and Decline 3
    expect(button("Approve call 2 of 3: pytest -q")).toBeTruthy();
  });
});

describe("copy and names", () => {
  const html = renderToStaticMarkup(<DecisionDock session={session(waiting([E1, E2, C1, C2]))} />);

  it("m4: group buttons name the group", () => {
    expect(html).toContain('aria-label="Approve 2 file edits"');
    expect(html).toContain('aria-label="Decline 2 shell commands"');
  });

  it("m5: × n means identical arguments; other groups count calls", () => {
    expect(html).toContain("× 2, same arguments");
    const echo = (id: string, text: string) => itemOf(waiting([tool(id, "gated_echo", { text })])[0]!);
    const s = buildStack([echo("a", "1"), echo("b", "2"), echo("c", "3")], new Map());
    expect(groupTitle(s.groups[0]!)).toEqual({ text: "gated_echo · 3 calls", code: null, fold: null });
  });

  it("nit: says which subagent asked, and nothing for the main agent", () => {
    const sub = renderToStaticMarkup(<DecisionDock session={session(waiting([E1], "explorer-1"))} />);
    expect(sub).toContain("from Explorer");
    expect(renderToStaticMarkup(<DecisionDock session={session(waiting([E1]))} />)).not.toContain("main agent");
  });

  it("nit: the decline placeholder speaks to the person", () => {
    expect(DECLINE_PLACEHOLDER).toBe("Reason (optional). The agent sees this.");
  });
});

