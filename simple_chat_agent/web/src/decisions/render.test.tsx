// Render checks for the slot components against a fake facade (no harness, no network).
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import type { AgentNode, CodingSession, Evaluation, Message, PendingApproval, ToolPart, TurnRecovery } from "../session";
import { RecoveryDivider, WorkerBanner } from "../recovery";
import { Workbench } from "../workbench";
import { DecisionDock, JudgeCards } from ".";
import { judgeView } from "./judge";

function tool(toolId: string, toolName: string, input: Record<string, unknown>, over: Partial<ToolPart> = {}): ToolPart {
  return { type: "tool", toolId, toolName, state: "awaiting_approval", input, output: null, error: null, progress: null, approval: null, evaluations: [], callback: null, ...over };
}

function session(parts: ToolPart[], over: Partial<CodingSession> = {}): CodingSession {
  const msg = { id: "m1", handler: "chat", input: { text: "fix it" }, output: null, status: "running", disposition: null, turnNumber: 1, parts, error: null, acceptedAt: 1, startedAt: 1, endedAt: null } as unknown as Message;
  const root = { agentId: "root", parentId: null, agentKey: null, workflowId: null, lifecycle: "running", unavailable: null, agentStatus: "busy", messages: [msg], states: {} } as unknown as AgentNode;
  const risk: Record<string, string> = { edit_file: "workspace_edit", run_command: "shell", git_commit: "shell" };
  const pending: PendingApproval[] = parts
    .filter((p) => p.state === "awaiting_approval")
    .map((part) => ({ agentId: "root", messageId: "m1", part, riskClass: risk[part.toolName] ?? null, rememberAllowed: risk[part.toolName] === "workspace_edit" }));
  return {
    sessionId: "s1",
    rootAgentId: "root",
    subagents: {},
    agent: (id: string) => (id === "root" ? root : undefined),
    messages: [msg],
    unsent: [],
    agentStatus: "busy",
    anyBusy: true,
    status: "waiting_for_you",
    pendingApprovals: pending,
    sends: [],
    sendFor: () => undefined,
    toolProgress: {},
    toolProgressChunks: {},
    sandbox: { answering: true, attempt: null, nextRetryAt: null, reason: null, toolId: null },
    runIds: {},
    runId: null,
    runIdFor: () => null,
    plan: { goal: "", items: [{ text: "Reproduce the failing test", status: "done" }, { text: "Fix it", status: "in_progress" }] },
    changes: { files: [{ path: "refund_service/refunds.py", status: "modified", added: 1, removed: 1, tool_id: "e0" }], commits: [] },
    workspace: { last_command: "pytest -q", last_exit_code: 1, commands_run: 1 },
    telemetry: { worker: { answering: true, reason: null, since: null, pollers: 1, agentId: null, turnId: null }, recoveries: [] },
    approve: async () => [],
    decline: async () => [],
    send: async () => ({}),
    ...over,
  } as unknown as CodingSession;
}

const EDITS = [
  tool("e1", "edit_file", { path: "refund_service/refunds.py", old_string: "total_cents", new_string: "balance_cents" }),
  tool("e2", "edit_file", { path: "tests/test_refund.py", old_string: "", new_string: "def test_x(): pass" }),
  tool("e3", "edit_file", { path: "refund_service/money.py", old_string: "a", new_string: "b" }),
  tool("c1", "run_command", { command: "pytest -q" }),
  tool("c2", "run_command", { command: "pytest -q" }),
];

describe("DecisionDock", () => {
  it("renders five concurrent calls as one stacked card", () => {
    const html = renderToStaticMarkup(<DecisionDock session={session(EDITS)} />);
    expect(html).toContain("Approve all 5");
    expect(html).toContain("5 tool calls are waiting");
    expect(html).toContain("Edit 3 files");
    expect(html).toContain("× 2, same arguments");
    expect(html).toContain("Approve 3");
    expect(html).toContain("Decline 2");
    expect(html).toContain("Always approve file edits");
    expect(html).toContain("Shell commands need a yes each time");
    expect(html).toContain("The agent is paused in Temporal and nothing is running.");
    expect(html.match(/class="dc-card/g)).toHaveLength(1);
  });

  it("renders one commit as the plain card with Always approve greyed and a reason", () => {
    const html = renderToStaticMarkup(<DecisionDock session={session([tool("g1", "git_commit", { message: "Use the remaining balance" })])} />);
    expect(html).toContain("Needs approval");
    expect(html).toContain("Commit 1 file");
    expect(html).toContain('message: &quot;Use the remaining balance&quot;');
    expect(html).toMatch(/aria-disabled="true"[^>]*title="Commits need a yes each time"/);
    expect(html).toContain("This request survives reloads and Worker restarts.");
  });

  it("renders the checkpoint card after a checkpointed turn", () => {
    const msg = { id: "m9", handler: "chat", status: "done", output: { text: "", interrupted: false, checkpoint: "history", error: null }, parts: [] } as unknown as Message;
    const html = renderToStaticMarkup(<DecisionDock session={session([], { messages: [msg], agentStatus: "idle", pendingApprovals: [] })} />);
    expect(html).toContain("Checkpoint reached. Continue?");
    const capped = { ...msg, output: { text: "", interrupted: false, checkpoint: null, error: "SpendCapReached" } } as unknown as Message;
    expect(renderToStaticMarkup(<DecisionDock session={session([], { messages: [capped], agentStatus: "idle", pendingApprovals: [] })} />)).toContain(
      "This session reached its spend limit",
    );
  });
});

describe("judge cards", () => {
  const ev = (over: Partial<Evaluation>): Evaluation => ({ id: "ev1", evaluator: "coding_judge", status: "ended", verdict: "approve", reason: null, details: {}, error: null, ...over });

  it("shows an approval with confidence and a one-line reason", () => {
    const t = tool("c1", "run_command", { command: "pytest -q" }, { evaluations: [ev({ details: { confidence: 0.93, criteria_set: "shell", judge_reason: "Runs the project's own tests in the sandbox" } })] });
    expect(judgeView(t, t.evaluations[0]!)).toMatchObject({ tone: "approved", badge: "Judge approved", score: "confidence 0.93", why: "Runs the project's own tests in the sandbox." });
    expect(renderToStaticMarkup(<JudgeCards part={t} />)).toContain("dc-judge approved");
  });

  it("shows an escalation in pink with the rule", () => {
    const t = tool("g1", "git_commit", { message: "m" }, {
      evaluations: [ev({ verdict: "escalate", reason: "'shell' calls always go to a human in the catalog environment.", details: { criteria_set: "shell", environment: "catalog" } })],
    });
    expect(judgeView(t, t.evaluations[0]!)).toMatchObject({
      tone: "escalated",
      badge: "Escalated to you",
      score: "rule: commits need a person",
      why: "The judge doesn't approve commits in this environment. A commit changes the repository's history.",
    });
  });
});

describe("Workbench and recovery", () => {
  it("renders the tabs with counts", () => {
    const html = renderToStaticMarkup(<Workbench session={session([])} variant="panel" />);
    expect(html).toContain("Plan");
    expect(html).toContain("1/2");
    expect(html).toContain("Reproduce the failing test");
    expect(html).toContain('role="tablist"');
  });

  it("renders the Terminal and Changes tabs", () => {
    const run = tool("r1", "run_command", { command: "pytest -q" }, { state: "done", output: "$ pytest -q\nexit 1 · 0.3s\nstdout:\n1 failed, 13 passed in 0.31s" });
    const term = renderToStaticMarkup(<Workbench session={session([run])} variant="sheet" initialTab="terminal" />);
    expect(term).toContain("exit 1 · tests failing");
    // M7: the output isn't a live region; one status line says what happened.
    expect(term).not.toContain('role="log"');
    expect(term).not.toContain("aria-live");
    expect(term).toContain('<p class="sr-only" role="status">pytest -q finished: exit 1 · tests failing</p>');
    const running = tool("r2", "run_command", { command: "pytest -q" }, { state: "running" });
    const live = renderToStaticMarkup(<Workbench session={session([running])} variant="panel" initialTab="terminal" />);
    expect(live).toContain('role="status">Running pytest -q</p>');
    expect(live).not.toMatch(/role="status">Running pytest -q \d/);
    // No exit code to report: "finished", once.
    const noExit = tool("r3", "run_command", { command: "pytest -q" }, { state: "done", output: "something unexpected" });
    const done = renderToStaticMarkup(<Workbench session={session([noExit])} variant="panel" initialTab="terminal" />);
    expect(done).toContain('role="status">pytest -q finished</p>');
    const edit = tool("e0", "edit_file", { path: "refund_service/refunds.py" }, { state: "done", output: "Edited refund_service/refunds.py (+1 -1).\n@@ -1 +1 @@\n-a\n+b\n" });
    const changes = renderToStaticMarkup(<Workbench session={session([edit])} variant="panel" initialTab="changes" />);
    expect(changes).toContain('<del class="del">');
    expect(changes).toContain("Uncommitted · the diff the commit will contain");
  });

  it("renders the banner only while the worker isn't answering", () => {
    const run = tool("r1", "run_command", { command: "pytest -q" }, { state: "running", progress: "batch 6" });
    const ok = session([run]);
    expect(renderToStaticMarkup(<WorkerBanner session={ok} temporalUiTemplate={null} />)).toBe("");
    const down = session([run], { telemetry: { ...ok.telemetry, worker: { answering: false, reason: "no heartbeat", since: Date.UTC(2026, 9, 16, 14, 2, 11), pollers: 0, agentId: null, turnId: null } } });
    const html = renderToStaticMarkup(<WorkerBanner session={down} temporalUiTemplate="http://ui/namespaces/default/workflows/{workflow_id}" />);
    expect(html).toContain("Worker isn&#x27;t answering");
    expect(html).toContain("Saved in Temporal:");
    expect(html).toContain("the test run&#x27;s output so far");
    expect(html).toContain("for another Worker to pick up the test run");
    expect(html).toContain("Messages you send now are stored in Temporal and run when a Worker returns.");
    expect(html).not.toContain("still land");
    // No run id yet: the Workflow's page, and the link says so.
    expect(html).toContain('href="http://ui/namespaces/default/workflows/s1"');
    expect(html).toContain("Open the Workflow in Temporal UI");
    const withRun = { ...down, runIdFor: (w: string) => (w === "s1" ? "run-9" : null) } as CodingSession;
    const deep = renderToStaticMarkup(<WorkerBanner session={withRun} temporalUiTemplate="http://ui/namespaces/default/workflows/{workflow_id}" />);
    expect(deep).toContain('href="http://ui/namespaces/default/workflows/s1/run-9/pending-activities"');
    expect(deep).toContain("Open the pending Activity in Temporal UI");
  });

  it("renders the divider", () => {
    const r: TurnRecovery = { agentId: "root", turnId: "t", turnNumber: 1, recoveries: [], kept: { modelCalls: 6, toolCalls: 9, tokensIn: 1, tokensOut: 1 }, repeatedModelCalls: 1, repeatedToolCalls: 0, lostAt: null, confirmed: false };
    const html = renderToStaticMarkup(<RecoveryDivider recovery={r} />);
    expect(html).toContain("Checking…");
    expect(html).toContain("compared with re-running this turn from your message");
  });
});

describe("approval card styles (UX review)", async () => {
  const fsName = "node:fs";
  const fs = (await import(/* @vite-ignore */ fsName)) as { readFileSync(p: URL, enc: "utf8"): string };
  const css = fs.readFileSync(new URL("./decisions.css", import.meta.url), "utf8");

  it("M2: the card is capped with a scrolling body", () => {
    expect(css).toMatch(/\.dc-card \{[^}]*max-height: 50vh/);
    expect(css).toMatch(/\.dc-card > \.dc-body \{[^}]*overflow-y: auto/);
  });

  it("M6: text is sized in rem with a 0.75rem floor, never nested em", () => {
    const sizes = [...css.matchAll(/font-size:\s*([^;}\s]+)/g)].map((m) => m[1]!);
    expect(sizes.filter((v) => /em$/.test(v) && !/rem$/.test(v))).toEqual([]);
    expect(sizes.filter((v) => parseFloat(v) < 0.75)).toEqual([]);
  });

  it("phone: denser and capped at 45vh, with 44 px targets", () => {
    const phone = css.slice(css.indexOf("@media (max-width: 600px)"));
    expect(phone).toMatch(/\.dc-card \{[^}]*max-height: 45vh/);
    expect(phone).toMatch(/\.dc-card\.stacked > \.top \{[^}]*flex-wrap: nowrap/);
    expect(phone).toMatch(/\.dc-card \.dc-review \{[^}]*min-height: 44px/);
    expect(phone).toMatch(/\.dc-card \.btn, \.dc-card \.chev, \.dc-card \.linkish \{[^}]*min-height: 44px/);
  });

  it("the group title is sized in rem, not the shell's .chev em (was 9.9 px)", () => {
    expect(css).toMatch(/\.dc-card \.chev \{[^}]*font-size: \.88rem/);
    expect(css).toMatch(/\.dc-card \.title \{[^}]*font-size: \.88rem/);
  });

  it("M1: answered and waiting rows share a fixed height, and the footer is fixed", () => {
    expect(css).toMatch(/\.dc-card \.item \{[^}]*height: 2\.6rem/);
    expect(css).toMatch(/\.dc-card \.dc-foot \{[^}]*height: 1\.7rem/);
  });
});
