import { describe, expect, it } from "vitest";
import { ProgressFolder, RECORD_SEPARATOR, parseDelta } from "./progress";
import { emptyAcc, foldFrames, summarize } from "./telemetry";
import { withRiskClass } from "./derive";
import type { ToolPart } from "./types";

const RS = RECORD_SEPARATOR;
const frame = (tool_id: string, progress_delta: string, timestamp = 10) => ({
  event: "tool_progress_delta",
  data: { tool_id, tool_name: "run_command", progress_delta, timestamp },
});
const tagged = (body: object) => RS + JSON.stringify(body);
const output = (seq: number | null, ...chunks: [string, string][]) =>
  tagged({ v: 1, type: "output", seq, chunks: chunks.map(([stream, text]) => ({ stream, text })) });

describe("progress parser", () => {
  it("reads tagged output with its streams", () => {
    expect(parseDelta(output(12, ["stdout", "a\n"], ["stderr", "E boom\n"]))).toEqual({
      kind: "output",
      seq: 12,
      chunks: [
        { stream: "stdout", text: "a\n" },
        { stream: "stderr", text: "E boom\n" },
      ],
    });
  });

  it("treats untagged text as stdout and drops tagged frames it can't read", () => {
    expect(parseDelta("plain")).toEqual({ kind: "output", seq: null, chunks: [{ stream: "stdout", text: "plain" }] });
    expect(parseDelta(RS + "{not json")).toEqual({ kind: "ignored" });
    expect(parseDelta(tagged({ v: 2, type: "mystery" }))).toEqual({ kind: "ignored" });
  });

  it("folds legacy and tagged chunks in order, with notes, per tool", () => {
    const folder = new ProgressFolder();
    const v = folder.update([
      frame("t1", "legacy "),
      frame("t1", output(1, ["stdout", "tests ..."], ["stderr", "warn\n"])),
      frame("t1", output(null, ["note", "── reattached ──\n"])),
      frame("t2", "x"),
    ]);
    expect(v.text.t1).toBe("legacy tests ...warn\n── reattached ──\n");
    expect(v.chunks.t1?.map((c) => c.stream)).toEqual(["stdout", "stdout", "stderr", "note"]);
    expect(v.chunks.t1?.[0]?.at).toBe(10_000);
    expect(v.text.t2).toBe("x");
  });

  it("drops a frame whose seq was already shown (a reattach replays)", () => {
    const folder = new ProgressFolder();
    const v = folder.update([
      frame("t1", output(3, ["stdout", "a"])),
      frame("t1", output(5, ["stdout", "b"])),
      frame("t1", output(5, ["stdout", "b"])),
      frame("t1", output(4, ["stdout", "a2"])),
      frame("t1", output(6, ["stdout", "c"])),
    ]);
    expect(v.text.t1).toBe("abc");
  });

  it("turns sandbox notices into sandbox health, never into output", () => {
    const folder = new ProgressFolder();
    const down = tagged({
      v: 1,
      type: "sandbox",
      state: "down",
      attempt: 1,
      max_attempts: 6,
      will_retry: true,
      retry_in_s: 5.0,
      next_retry_at: 1_760_620_005.2,
      reason: "connect timeout",
      message: "The sandbox isn't answering. Temporal will retry the command.",
    });
    let v = folder.update([frame("t1", output(1, ["stdout", "running"])), frame("t1", down)]);
    expect(v.sandbox).toEqual({
      answering: false,
      attempt: 1,
      maxAttempts: 6,
      willRetry: true,
      nextRetryAt: 1_760_620_005_200,
      reason: "connect timeout",
      message: "The sandbox isn't answering. Temporal will retry the command.",
      toolId: "t1",
    });
    expect(v.text.t1).toBe("running");
    v = folder.update([frame("t1", output(1, ["stdout", "running"])), frame("t1", down), frame("t1", tagged({ v: 1, type: "sandbox", state: "up", attempt: 2 }))]);
    expect(v.sandbox).toMatchObject({ answering: true, attempt: 2, toolId: null });
    expect(v.text.t1).toBe("running");
  });

  it("clears a notice when its tool call ends", () => {
    const folder = new ProgressFolder();
    const down = tagged({ v: 1, type: "sandbox", state: "down", attempt: 6, will_retry: false, next_retry_at: null });
    const frames = [frame("t1", down)];
    expect(folder.update(frames).sandbox).toMatchObject({ answering: false, willRetry: false, nextRetryAt: null });
    expect(folder.update([...frames, { event: "tool_error", data: { tool_id: "t1" } }]).sandbox.answering).toBe(true);
  });
});

describe("notes", () => {
  it("sit on their own line with no blank lines around them", () => {
    const folder = new ProgressFolder();
    const v = folder.update([
      frame("t1", output(1, ["stdout", "batch 6 of 10"])),
      frame("t1", output(null, ["note", "\n\n── reattached on worker-b ──\n\n"])),
      frame("t1", output(2, ["stdout", "\n\nbatch 7 of 10\n"])),
      frame("t1", output(null, ["note", "\n"])),
    ]);
    expect(v.text.t1).toBe("batch 6 of 10\n── reattached on worker-b ──\nbatch 7 of 10\n");
    expect(v.chunks.t1?.map((c) => c.stream)).toEqual(["stdout", "note", "stdout"]);
  });
});

describe("run ids", () => {
  it("reads each agent's latest run id and the x_worker runs map", () => {
    const acc = emptyAcc();
    foldFrames(acc, [
      { event: "turn_started", data: { agent_id: "root", run_id: "r1" } },
      { event: "x_worker", data: { state: "ok", agent_id: "root", workflow_id: "coding-agent-1", run_id: "r2", runs: { "coding-agent-1": "r2", "child-1": "c1" } } },
    ]);
    const t = summarize(acc);
    expect(t.runIds).toEqual({ root: "r2" });
    expect(t.runIdsByWorkflow).toEqual({ "coding-agent-1": "r2", "child-1": "c1" });
  });
});

describe("risk classes", () => {
  const pending = (toolName: string) => ({ agentId: "root", messageId: "m", part: { toolName } as ToolPart });
  it("come from config, with its default for unmapped tools", () => {
    const out = withRiskClass([pending("edit_file"), pending("run_command"), pending("mcp_thing")], { edit_file: "workspace_edit", run_command: "shell" }, "external_side_effect", ["workspace_edit"]);
    expect(out.map((o) => [o.riskClass, o.rememberAllowed])).toEqual([
      ["workspace_edit", true],
      ["shell", false],
      ["external_side_effect", false],
    ]);
    expect(withRiskClass([pending("edit_file")], null, null, null)[0]).toMatchObject({ riskClass: null, rememberAllowed: false });
  });
});
