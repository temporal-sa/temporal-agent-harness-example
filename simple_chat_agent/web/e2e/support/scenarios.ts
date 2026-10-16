// Scripted sessions for the mocked project, built from the real frame shapes (frames.ts).

import {
  ROOT,
  approvalRequested,
  closeTurn,
  ev,
  judgeApproves,
  modelCall,
  openTurn,
  patch,
  quickTool,
  snapshot,
  subagentAsked,
  subagentReplied,
  toolEnd,
  toolProgress,
  toolRequested,
  toolStart,
  turn,
  xRecovery,
  xWorker,
  type Frame,
  type Sub,
  type TurnRef,
} from "./frames";

export const DEMO_PROMPT = "test_refund fails. Find the cause, fix it, run the tests, commit.";
export const FIX_PATH = "refund_service/refunds.py";
export const PYTEST = "pytest -q";
export const PYTEST_LOG = "log-pytest-1";

export const EXPLORERS: Sub[] = [
  { agentId: "agent-explorer-1", agentKey: "explorer", workflowId: "sess-explorer-1" },
  { agentId: "agent-explorer-2", agentKey: "explorer", workflowId: "sess-explorer-2" },
];

const PLAN = {
  goal: "Fix test_refund",
  items: [
    { text: "Find why test_refund fails", status: "done" },
    { text: "Fix the rounding in refunds.py", status: "done" },
    { text: "Run the test suite", status: "in_progress" },
    { text: "Commit the fix", status: "pending" },
  ],
};

/** The usage the working scenario reports, for the meters test. */
export const WORKING_USAGE = [
  { input: 5200, output: 180, cacheRead: 0, cacheWrite: 5100 },
  { input: 7400, output: 420, cacheRead: 5100, cacheWrite: 1900 },
  { input: 9800, output: 260, cacheRead: 7000, cacheWrite: 2400 },
];
/** Each explorer's single call. */
export const EXPLORER_USAGE = { input: 3100, output: 240, cacheRead: 2000, cacheWrite: 1000 };

function explorerTurn(sub: Sub, question: string, file: string, answer: string): Frame[] {
  const t = turn(1, `${sub.agentId}-msg-1`, sub.agentId);
  return [
    ...openTurn(t, question, "investigate"),
    ...modelCall(t, 0, EXPLORER_USAGE, { model: "claude-haiku-5-5" }),
    ...quickTool(t, `${sub.agentId}-read`, "read_file", { path: file }, `1  def refund(amount):\n2      return round(amount * 0.9)\n`),
    ...closeTurn(t, { answer, evidence: [{ path: file, line: 2, note: "round() drops cents" }] }),
  ];
}

/**
 * Frame 1 of the wireframes: the demo prompt, two explorers, the fix (inline diff), the judge
 * approving pytest, and pytest running. The turn stays open.
 */
export function working(root: TurnRef = turn(1, "msg-1")): Frame[] {
  const editInput = {
    path: FIX_PATH,
    old_string: "    return round(amount * 0.9)\n",
    new_string: "    return round(amount * 0.9, 2)\n",
  };
  const cmd = { command: PYTEST };
  return [
    ...openTurn(root, DEMO_PROMPT),
    snapshot(root, "plan", 1, { goal: "Fix test_refund", items: PLAN.items.map((i) => ({ ...i, status: "pending" })) }),
    snapshot(root, "changes", 1, { files: [], commits: [] }),
    snapshot(root, "workspace", 1, { last_command: "", last_exit_code: null, commands_run: 0 }),
    ...modelCall(root, 0, WORKING_USAGE[0]!, { thought: "The failing test is about refunds. Two questions can run in parallel.", text: "I'll look at the refund path with two explorers.\n\n" }),
    ...subagentAsked(root, EXPLORERS[0]!, "tool-explore-1", "Where is the refund amount computed?"),
    ...subagentAsked(root, EXPLORERS[1]!, "tool-explore-2", "What does test_refund expect?"),
    ...explorerTurn(EXPLORERS[0]!, "Where is the refund amount computed?", FIX_PATH, `refund() rounds to whole units in ${FIX_PATH}.`),
    ...explorerTurn(EXPLORERS[1]!, "What does test_refund expect?", "tests/test_refunds.py", "test_refund expects 9.45, not 9."),
    ...subagentReplied(root, EXPLORERS[0]!, "tool-explore-1", `The refund is rounded in ${FIX_PATH}.`),
    ...subagentReplied(root, EXPLORERS[1]!, "tool-explore-2", "test_refund expects 9.45, not 9."),
    ...modelCall(root, 1, WORKING_USAGE[1]!, { text: "The refund drops the cents. Fixing the rounding.\n\n" }),
    toolRequested(root, "tool-edit-1", "edit_file", editInput),
    ...judgeApproves(root, "tool-edit-1", "edit_file", "A one-line change inside the workspace that matches the plan.", 0.96, "workspace_edit"),
    toolStart(root, "tool-edit-1", "edit_file", editInput),
    toolEnd(
      root,
      "tool-edit-1",
      "edit_file",
      [
        `Edited ${FIX_PATH} (+1 -1).`,
        `--- a/${FIX_PATH}`,
        `+++ b/${FIX_PATH}`,
        "@@ -1,2 +1,2 @@",
        " def refund(amount):",
        "-    return round(amount * 0.9)",
        "+    return round(amount * 0.9, 2)",
      ].join("\n"),
    ),
    patch(root, "changes", 2, [{ op: "add", path: "/files/-", value: { path: FIX_PATH, status: "modified", added: 1, removed: 1, tool_id: "tool-edit-1" } }]),
    patch(root, "plan", 2, [{ op: "replace", path: "/items", value: PLAN.items }]),
    ...modelCall(root, 2, WORKING_USAGE[2]!, { text: "Running the tests.\n\n" }),
    toolRequested(root, "tool-pytest-1", "run_command", cmd),
    ...judgeApproves(root, "tool-pytest-1", "run_command", "Running the project's own test suite is read-only.", 0.93),
    toolStart(root, "tool-pytest-1", "run_command", cmd),
    toolProgress(root, "tool-pytest-1", "run_command", "....F.....\n"),
  ];
}

/** The working scenario's turn ends: pytest passes and the reply streams. */
export function workingEnds(root: TurnRef = turn(1, "msg-1"), reply = "The tests pass. test_refund was failing because the refund was rounded to whole units."): Frame[] {
  return [
    toolEnd(root, "tool-pytest-1", "run_command", `$ pytest -q\nexit 0 · 19.8s\nstdout:\n..........\n10 passed in 19.6s\n[output truncated; full log: ${PYTEST_LOG}]`),
    patch(root, "workspace", 2, [
      { op: "replace", path: "/last_command", value: PYTEST },
      { op: "replace", path: "/last_exit_code", value: 0 },
      { op: "replace", path: "/commands_run", value: 1 },
    ]),
    ...modelCall(root, 3, { input: 10400, output: 120, cacheRead: 9800, cacheWrite: 600 }, { text: reply }),
    ...closeTurn(root, { text: reply, interrupted: false, checkpoint: null, error: null }),
  ];
}

/** The crash: the bridge's worker hint, then the recovery marker (live, then confirmed). */
export function workerLost(root: TurnRef, lostAt: number, runId = "run-1"): Frame {
  return xWorker("suspect", "the pending activity run_command has not heartbeated for 7s; no pollers on the task queue", {
    turn: root,
    since: lostAt,
    pollers: 0,
    workflowId: "sess-mock",
    runId,
  });
}

export const KEPT = { model_calls: 3, tool_calls: 7, tokens_in: 22400, tokens_out: 860 };

export function recovery(root: TurnRef, lostAt: number, confirmed: boolean): Frame {
  return xRecovery(root, {
    key: "tool:pytest",
    kind: "command_reattached",
    confirmed,
    lostAt,
    kept: KEPT,
    repeated: { model_calls: 0, tool_calls: 0, resent_input_tokens_estimate: null },
    attempt: 2,
    toolName: "run_command",
    activityId: "7",
  });
}

export const workerBack = (root: TurnRef) => xWorker("ok", "workers are answering", { turn: root, pollers: 1, workflowId: "sess-mock", runId: "run-1" });

// -- stacked approvals (owner decision B, check 3e) -------------------------------------------

export const STACK_EDITS = [
  { id: "tool-edit-a", path: "refund_service/refunds.py" },
  { id: "tool-edit-b", path: "refund_service/money.py" },
  { id: "tool-edit-c", path: "tests/test_refunds.py" },
];
export const STACK_COMMANDS = ["tool-cmd-a", "tool-cmd-b"];
export const STACK_IDS = [...STACK_EDITS.map((e) => e.id), ...STACK_COMMANDS];

/** Five calls waiting at once: three edits to three files, and `pytest -q` twice. */
export function stacked(root: TurnRef = turn(1, "msg-1")): Frame[] {
  const frames: Frame[] = [
    ...openTurn(root, "Fix the rounding in all three files, then run the tests twice."),
    ...modelCall(root, 0, { input: 6100, output: 540, cacheRead: 0, cacheWrite: 6000 }, { text: "Making the three edits and running the suite.\n\n" }),
  ];
  for (const e of STACK_EDITS) {
    const input = { path: e.path, old_string: "round(x)", new_string: "round(x, 2)" };
    frames.push(toolRequested(root, e.id, "edit_file", input), approvalRequested(root, e.id, "edit_file", input));
  }
  for (const id of STACK_COMMANDS) {
    frames.push(toolRequested(root, id, "run_command", { command: PYTEST }), approvalRequested(root, id, "run_command", { command: PYTEST }));
  }
  return frames;
}

/** One call waiting (git_commit escalated by the judge, wireframe Frame 4). */
export function commitWaits(root: TurnRef = turn(1, "msg-1")): Frame[] {
  const input = { message: "Fix refund rounding" };
  return [
    ...openTurn(root, "Commit the fix."),
    ...modelCall(root, 0, { input: 4000, output: 90, cacheRead: 3000, cacheWrite: 900 }, { text: "Committing.\n\n" }),
    toolRequested(root, "tool-commit-1", "git_commit", input),
    ev(root, "auto_approval_evaluation_started", { tool_id: "tool-commit-1", tool_name: "git_commit", evaluation_id: "eval-commit", evaluator: "llm_judge" }),
    ev(root, "auto_approval_evaluation_ended", {
      tool_id: "tool-commit-1",
      tool_name: "git_commit",
      evaluation_id: "eval-commit",
      evaluator: "llm_judge",
      verdict: "escalate",
      reason: "pre-rule commits: shell commands that change history always go to a human",
      details: { pre_rule: "commits_need_a_person", criteria_set: "shell" },
    }),
  ];
}

/** An idle session: one finished turn. */
export function idle(root: TurnRef = turn(1, "msg-1")): Frame[] {
  return [
    ...openTurn(root, "What does this repo do?"),
    ...modelCall(root, 0, { input: 3000, output: 200, cacheRead: 0, cacheWrite: 2900 }, { text: "It is a small refund service with one failing test." }),
    ...closeTurn(root, { text: "It is a small refund service with one failing test.", interrupted: false, checkpoint: null, error: null }),
  ];
}

export { ROOT, turn };
