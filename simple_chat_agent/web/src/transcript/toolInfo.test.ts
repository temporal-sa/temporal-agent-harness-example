import { describe, expect, it } from "vitest";
import { argPreview, commandSummary, decisionBadge, miniDiff, pathsOf, toolBadge } from "./toolInfo";
import { tool } from "./fixtures";

const EDIT_OUT = [
  "Edited refund_service/refunds.py (+1 -1).",
  "--- a/refund_service/refunds.py",
  "+++ b/refund_service/refunds.py",
  "@@ -31,3 +31,3 @@",
  "         self._check_refundable(order)",
  "-        amount = order.total_cents if amount_cents is None else amount_cents",
  "+        amount = order.balance_cents if amount_cents is None else amount_cents",
  "         if amount <= 0:",
].join("\n");

describe("tool rows", () => {
  it("badges and previews", () => {
    expect(toolBadge("edit_file")).toEqual({ label: "Edit", kind: "edit" });
    expect(toolBadge("mystery").label).toBe("Tool");
    expect(argPreview(tool("run_command", { input: { command: "pytest -q" } }))).toBe("pytest -q");
    expect(argPreview(tool("search_code", { input: { pattern: "amount_cents is None", path: "." } }))).toBe('"amount_cents is None"');
    expect(argPreview(tool("update_plan", { input: { items: [1, 2, 3, 4, 5] } }))).toBe("5 steps");
  });

  it("reads exit code, duration, log ref and changed files from a command result", () => {
    const out = "$ pytest -q\nexit 1 · 0.3s\nstdout:\n1 failed\n[output truncated; full log: exec-abc123]\nchanged: M a.py, A tests/b.py";
    expect(commandSummary(out)).toEqual({
      exitCode: 1,
      seconds: 0.3,
      timedOut: false,
      reattached: false,
      logRef: "exec-abc123",
      changed: [
        { status: "M", path: "a.py" },
        { status: "A", path: "tests/b.py" },
      ],
    });
    expect(commandSummary("$ x\nexit 0 · 21.4s · reattached (attempt 2)")).toMatchObject({ exitCode: 0, reattached: true, logRef: null });
    expect(commandSummary(null)).toBeNull();
  });

  it("builds the mini-diff from edit_file's result, without file headers", () => {
    const d = miniDiff(tool("edit_file", { output: EDIT_OUT, input: { path: "refund_service/refunds.py" } }));
    expect(d?.added).toBe(1);
    expect(d?.removed).toBe(1);
    expect(d?.rows.map((r) => r.kind)).toEqual(["hunk", "ctx", "del", "add", "ctx"]);
  });

  it("previews a waiting edit from its arguments and a new file from its content", () => {
    const pending = miniDiff(tool("edit_file", { state: "awaiting_approval", input: { old_string: "a\nb", new_string: "c" } }));
    expect(pending?.rows).toEqual([
      { kind: "del", text: "a" },
      { kind: "del", text: "b" },
      { kind: "add", text: "c" },
    ]);
    const created = miniDiff(tool("write_file", { output: "Created t.py (20 bytes, +3 -0).", input: { content: "1\n2\n3\n" } }), 2);
    expect(created).toMatchObject({ added: 3, removed: 0, hidden: 1 });
    const rewrite = miniDiff(tool("write_file", { output: "Wrote t.py (20 bytes, +4 -2).", input: { content: "x" } }));
    expect(rewrite).toEqual({ rows: [], added: 4, removed: 2, hidden: 0 });
    const withDiff = miniDiff(
      tool("write_file", { output: "Wrote t.py (20 bytes, +1 -1).\n--- a/t.py\n+++ b/t.py\n@@ -1 +1 @@\n-old\n+new", input: { path: "t.py", content: "new" } }),
    );
    expect(withDiff).toEqual({
      rows: [
        { kind: "hunk", text: "@@ -1 +1 @@" },
        { kind: "del", text: "old" },
        { kind: "add", text: "new" },
      ],
      added: 1,
      removed: 1,
      hidden: 0,
    });
  });

  it("lists path chips for file tools and files a command changed", () => {
    expect(pathsOf(tool("read_file", { input: { path: "a.py" } }))).toEqual(["a.py"]);
    expect(pathsOf(tool("run_command", { output: "$ x\nexit 0 · 1s\nchanged: D old.py" }))).toEqual(["old.py"]);
    expect(pathsOf(tool("git_status"))).toEqual([]);
  });
});

describe("who decided", () => {
  const ev = (verdict: "approve" | "deny" | "escalate", status: "ended" | "running" = "ended") => ({
    id: "e1",
    evaluator: "judge",
    status,
    verdict,
    reason: null,
    details: {},
    error: null,
  });
  const ok = { approved: true, reason: null, remember: false };

  it("credits the judge when its evaluation decided the call", () => {
    expect(decisionBadge({ approval: ok, evaluations: [ev("approve")] })).toBe("Approved by the judge");
    expect(decisionBadge({ approval: { approved: false, reason: "judge: risky", remember: false }, evaluations: [] })).toBe("Declined by the judge");
  });

  it("credits the person after an escalation, and says when they chose Always approve", () => {
    expect(decisionBadge({ approval: ok, evaluations: [ev("escalate")] })).toBe("Approved by you");
    expect(decisionBadge({ approval: ok, evaluations: [ev("approve", "running")] })).toBe("Approved by you");
    expect(decisionBadge({ approval: { ...ok, remember: true }, evaluations: [] })).toBe("Always approved");
    expect(decisionBadge({ approval: { approved: false, reason: "no", remember: false }, evaluations: [] })).toBe("Declined by you");
    expect(decisionBadge({ approval: null, evaluations: [] })).toBeNull();
  });
});
