import { describe, expect, it } from "vitest";
import { ProgressFolder } from "./progress";
import { emptyAcc, foldFrames, summarize } from "./telemetry";
import { withRiskClass } from "./derive";
import type { PendingApproval, ToolPart } from "./types";

const delta = (tool_id: string, progress_delta: string, extra: Record<string, unknown> = {}) => ({
  event: "tool_progress_delta",
  data: { tool_id, tool_name: "run_command", progress_delta, timestamp: 10, ...extra },
});

describe("tool progress", () => {
  it("accumulates every chunk per tool, with its stream tag when present", () => {
    const folder = new ProgressFolder();
    const frames = [delta("t1", "tests/a.py ..."), delta("t2", "x"), delta("t1", "..\n", { stream: "stderr" })];
    const v = folder.update(frames);
    expect(v.text.t1).toBe("tests/a.py .....\n");
    expect(v.chunks.t1?.map((c) => c.stream)).toEqual([null, "stderr"]);
    expect(v.chunks.t1?.[0]?.at).toBe(10_000);
    expect(folder.update(frames)).toBe(v);
    expect(folder.update([...frames, delta("t2", "y")]).text.t2).toBe("xy");
  });
});

describe("sandbox and run ids", () => {
  it("reads the sandbox notice and each agent's latest run id", () => {
    const acc = emptyAcc();
    foldFrames(acc, [
      { event: "turn_started", data: { agent_id: "root", run_id: "r1" } },
      { event: "x_worker", data: { state: "ok", agent_id: "root", workflow_id: "coding-agent-1", run_id: "r2", runs: { "coding-agent-1": "r2", "child-1": "c1" } } },
      { event: "x_sandbox", data: { state: "down", attempt: 2, next_retry_at: 100, tool_id: "t1", reason: "executor unavailable" } },
    ]);
    const t = summarize(acc);
    expect(t.runIds).toEqual({ root: "r2" });
    expect(t.runIdsByWorkflow).toEqual({ "coding-agent-1": "r2", "child-1": "c1" });
    expect(t.sandbox).toEqual({ answering: false, attempt: 2, nextRetryAt: 100_000, reason: "executor unavailable", toolId: "t1" });
    expect(t.unknownFrames).toEqual({});
  });
});

describe("risk classes", () => {
  const pending = (toolName: string) => ({ agentId: "root", messageId: "m", part: { toolName } as ToolPart }) as PendingApproval;
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
