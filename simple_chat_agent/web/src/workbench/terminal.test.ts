import { describe, expect, it } from "vitest";

import type { Recovery, ToolPart, ToolProgressChunk } from "../session";
import { exitBadge, liveRows, parseCommandOutput, reattachFor, RunClock, runLines, SANDBOX_DOWN, sandboxDown } from "./terminal";

const out = (text: string, stream: "stdout" | "stderr" | "note" = "stdout"): ToolProgressChunk => ({ text, stream, at: null });

// CommandResult.model_text, as the harness hands it to the model and the stream.
const FAILING = [
  "$ pytest -q tests/test_refund.py",
  "exit 1 · 0.3s",
  "stdout:",
  ".....F........                                                   [100%]",
  "1 failed, 13 passed in 0.31s",
  "stderr:",
  "E   RefundError: Refund of 112.16 USD exceeds the remaining balance of 87.16 USD.",
].join("\n");

const PASSING = ["$ pytest -q", "exit 0 · 21.4s · reattached (attempt 2)", "stdout:", "tests/test_settlement.py .   [100%]", "27 passed in 21.4s"].join("\n");

const part = (state: ToolPart["state"], output: string | null = null): Pick<ToolPart, "state" | "output" | "input"> => ({
  state,
  output,
  input: { command: "pytest -q" },
});

describe("exit badge", () => {
  it("reads 'exit 1 · tests failing' for a failing test run", () => {
    expect(exitBadge(part("done", FAILING))).toEqual({ text: "exit 1 · tests failing", tone: "fail" });
  });

  it("reads 'exit 0 · 27 passed' for a passing test run", () => {
    expect(exitBadge(part("done", PASSING))).toEqual({ text: "exit 0 · 27 passed", tone: "ok" });
  });

  it("reads a plain exit code for commands that aren't tests", () => {
    expect(exitBadge(part("done", "$ ls\nexit 0 · 0.0s\nstdout:\nREADME.md"))).toEqual({ text: "exit 0", tone: "ok" });
    expect(exitBadge(part("done", "$ false\nexit 1 · 0.0s"))).toEqual({ text: "exit 1", tone: "fail" });
    expect(exitBadge(part("done", "$ sleep 999\nexit ? · 300.0s · timed out"))).toEqual({ text: "timed out", tone: "fail" });
  });

  it("pauses while no worker answers, and runs with a clock otherwise", () => {
    expect(exitBadge(part("running"), { workerAnswering: false })).toEqual({ text: "paused · waiting for a worker", tone: "wait" });
    expect(exitBadge(part("running"), { workerAnswering: true, elapsedMs: 12_400 })).toEqual({ text: "running 0:12", tone: "run" });
    expect(exitBadge(part("running"), { workerAnswering: true })).toEqual({ text: "running", tone: "run" });
    expect(exitBadge(part("awaiting_approval"))).toEqual({ text: "waiting for approval", tone: "wait" });
    expect(exitBadge(part("denied"))).toEqual({ text: "declined", tone: "muted" });
  });
});

describe("command output", () => {
  it("splits stdout and stderr and reads the reattach marker", () => {
    const out = parseCommandOutput(FAILING)!;
    expect(out.exitCode).toBe(1);
    expect(out.stdout).toContain("1 failed, 13 passed");
    expect(out.stderr).toContain("RefundError");
    expect(parseCommandOutput(PASSING)!).toMatchObject({ reattached: true, attempt: 2, exitCode: 0 });
  });

  it("colours stderr lines and marks the passing summary", () => {
    const ctx = { live: [], workerAnswering: true, pausedAt: null, sandbox: null, reattach: null };
    const failing = runLines(part("done", FAILING), ctx);
    expect(failing.find((l) => l.text.startsWith("E   RefundError"))!.kind).toBe("err");
    const passing = runLines(part("done", PASSING), ctx);
    expect(passing.map((l) => l.kind)).toEqual(["cmd", "reattach", "out", "ok"]);
  });
});

describe("live stream markers", () => {
  const recovery = { kind: "command_reattached", toolName: "workspace.run_command", agentId: "a", turnNumber: 2, retracted: false, resumedOn: "worker-b" } as Recovery;

  it("marks where the stream paused while no worker answers", () => {
    const lines = runLines(part("running"), { live: [out("tests/test_money.py ....\n")], workerAnswering: false, pausedAt: "14:02:11", sandbox: null, reattach: null });
    expect(lines.at(-1)).toEqual({ kind: "pause", text: "── stream paused at 14:02:11 · waiting for a worker ──" });
  });

  it("names the worker that reattached when telemetry says so", () => {
    const live = [out("batch 6\n"), out("\n[worker restarted; reattaching", "note"), out(" to the command (attempt 2)]\n", "note"), out("batch 7\n")];
    const lines = runLines(part("running"), { live, workerAnswering: true, pausedAt: null, sandbox: null, reattach: reattachFor({ agentId: "a", turnNumber: 2 }, [recovery]) });
    expect(lines.map((l) => l.text)).toEqual(["$ pytest -q", "batch 6", "", "── reattached on worker-b · same run, not restarted ──", "batch 7"]);
    expect(reattachFor({ agentId: "a", turnNumber: 3 }, [recovery])).toBeNull();
  });

  it("says the sandbox isn't answering, with the attempt and the next try", () => {
    const lines = runLines(part("running"), { live: [], workerAnswering: true, pausedAt: null, sandbox: { attempt: 2, nextRetryAt: 6_000 }, now: 1_000, reattach: null });
    expect(lines.slice(-2)).toEqual([
      { kind: "pause", text: SANDBOX_DOWN },
      { kind: "muted", text: "attempt 2 · next try in 5s" },
    ]);
    expect(SANDBOX_DOWN).toBe("The sandbox isn't answering. Temporal will retry the command.");
  });

  it("reads sandbox health from the facade and scopes it to the named run", () => {
    expect(sandboxDown({ answering: true, toolId: null }, "r1")).toBe(false);
    expect(sandboxDown({ answering: false, toolId: null }, "r1")).toBe(true);
    expect(sandboxDown({ answering: false, toolId: "r1" }, "r1")).toBe(true);
    expect(sandboxDown({ answering: false, toolId: "r2" }, "r1")).toBe(false);
  });

  it("colours live stderr from the facade's stream tags, across chunk boundaries", () => {
    const rows = liveRows([out("collected 27 items\nwarn", "stdout"), out("ing: deprecated\n", "stderr"), out("ok\n", "stdout")]);
    expect(rows).toEqual([
      { text: "collected 27 items", kind: "out" },
      { text: "warning: deprecated", kind: "err" },
      { text: "ok", kind: "out" },
    ]);
    const lines = runLines(part("running"), { live: [out("boom\n", "stderr")], workerAnswering: true, pausedAt: null, sandbox: null, reattach: null });
    expect(lines[1]).toEqual({ kind: "err", text: "boom" });
  });

  it("renders note chunks (our own lines) muted, not as stdout", () => {
    const live = [out("batch 3\n"), out("\n[earlier output dropped: 4,096 chars]\n", "note"), out("\n[live output limit reached; the result has the tail]\n", "note")];
    const lines = runLines(part("running"), { live, workerAnswering: true, pausedAt: null, sandbox: null, reattach: null });
    expect(lines.map((l) => l.kind)).toEqual(["cmd", "out", "out", "muted", "out", "muted"]);
    expect(liveRows([out("a note without brackets\n", "note")])).toEqual([{ text: "a note without brackets", kind: "note" }]);
  });

  it("times a run only when it was seen starting", () => {
    const clock = new RunClock();
    clock.observe("t", "running");
    clock.observe("t", "running");
    expect(clock.startedAt("t")).toBeNull(); // first seen already running: elapsed unknown
    clock.observe("u", "approved", 100);
    clock.observe("u", "running", 250);
    expect(clock.startedAt("u")).toBe(250);
  });
});
