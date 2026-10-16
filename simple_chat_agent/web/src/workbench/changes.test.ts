import { describe, expect, it } from "vitest";

import type { ToolPart } from "../session";
import { currentBranch, DETACHED, workspaceFacts } from "./changes";
import type { LocatedTool } from "./tree";

const status = (output: string): LocatedTool =>
  ({
    part: { type: "tool", toolId: "g", toolName: "git_status", state: "done", input: {}, output } as unknown as ToolPart,
    agentId: "root",
    agentKey: null,
    messageId: "m",
    messageStatus: "done",
    turnNumber: 1,
    seq: 0,
  }) as LocatedTool;

describe("branch", () => {
  const ws = (branch: string) => ({ branch, last_command: "", last_exit_code: null, commands_run: 0 });

  it("reads the branch from the agent's workspace state", () => {
    expect(currentBranch(ws("main"), [status("On branch fix-refund; working tree clean.")])).toBe("main");
    expect(currentBranch(ws(DETACHED), [])).toBe("HEAD (no branch)");
  });

  it("falls back to the latest git_status only while the state's branch is empty", () => {
    expect(currentBranch(ws(""), [status("On branch main\n M a.py"), status("On branch fix-refund; working tree clean.")])).toBe("fix-refund");
    expect(currentBranch(ws(""), [])).toBeNull();
    expect(currentBranch(null, [])).toBeNull();
    expect(workspaceFacts(ws("main"), null, []).branch).toBe("main");
  });
});
