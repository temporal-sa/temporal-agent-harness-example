import { describe, expect, it } from "vitest";
import { buildItems } from "./items";
import { call, text, thought, tool } from "./fixtures";
import type { TurnRecovery } from "../session";

const kinds = (items: ReturnType<typeof buildItems>) => items.map((i) => i.kind);
const opts = { live: false, interrupted: false };

describe("buildItems", () => {
  it("joins streamed text and shows a thinking row only when there was thinking", () => {
    const items = buildItems({ id: "m", parts: [call(), thought("look at "), thought("refunds"), text("Both "), text("explorers agree.")] }, opts);
    expect(kinds(items)).toEqual(["thinking", "text"]);
    expect(items[0]).toMatchObject({ text: "look at refunds", callIndex: 0 });
    expect(items[1]).toMatchObject({ text: "Both explorers agree.", streaming: false });
    expect(kinds(buildItems({ id: "m", parts: [call(), text("hi")] }, opts))).toEqual(["text"]);
  });

  it("shows thinking from reported thought tokens even without summary text", () => {
    const items = buildItems({ id: "m", parts: [call("done", { usage: { input_tokens: 1, output_tokens: 1, thought_tokens: 1204, cached_tokens: null } as never }), text("ok")] }, opts);
    expect(items[0]).toMatchObject({ kind: "thinking", text: "", thoughtTokens: 1204 });
  });

  it("groups consecutive read-only calls of one tool, but not ones waiting for approval", () => {
    const items = buildItems(
      { id: "m", parts: [call(), tool("read_file"), tool("read_file"), tool("read_file"), tool("edit_file"), tool("read_file", { state: "awaiting_approval" }), tool("read_file")] },
      opts,
    );
    expect(kinds(items)).toEqual(["group", "tool", "tool", "tool"]);
    expect(items[0]).toMatchObject({ label: "Read file" });
    expect(items[0]?.kind === "group" && items[0].parts.length).toBe(3);
  });

  it("hides subagent-driving tools and puts parallel subagents side by side", () => {
    const sub = (id: string) => ({ type: "subagent" as const, subagentId: id, agentKey: "explorer", workflowId: "w", handler: "investigate", subagentTurn: 1, status: "ok" as const, messageId: null });
    const items = buildItems({ id: "m", parts: [call(), tool("start_explorer"), tool("explorer_investigate"), sub("a"), sub("b"), tool("stop_explorer")] }, opts);
    expect(kinds(items)).toEqual(["subagents"]);
    expect(items[0]?.kind === "subagents" && items[0].parts.length).toBe(2);
  });

  it("folds a call cut off by a lost worker under Attempt 1, with no duplicated text", () => {
    const items = buildItems({ id: "m", parts: [call("running"), text("All 27 tests pass, includ"), call("done"), text("All 27 tests pass.")] }, opts);
    expect(kinds(items)).toEqual(["attempt", "text"]);
    expect(items[0]).toMatchObject({ attempt: 1, words: 5 });
    expect(items.filter((i) => i.kind === "text").map((i) => (i.kind === "text" ? i.text : ""))).toEqual(["All 27 tests pass."]);
  });

  it("places the recovery divider at the lost attempt, else before the final reply", () => {
    const rec = { turnId: "t1" } as TurnRecovery;
    const lost = buildItems({ id: "m", parts: [call("done"), tool("run_command"), call("running"), text("partial"), call("done"), text("final")] }, { ...opts, recoveries: [rec] });
    expect(kinds(lost)).toEqual(["tool", "recovery", "attempt", "text"]);
    const toolOnly = buildItems({ id: "m", parts: [call(), tool("run_command"), call(), text("27 passed")] }, { ...opts, recoveries: [rec] });
    expect(kinds(toolOnly)).toEqual(["tool", "recovery", "text"]);
  });

  it("folds the reply after Stop as Discarded", () => {
    const items = buildItems({ id: "m", parts: [call(), tool("read_file"), call("running"), text("I was about to say a lot")] }, { live: false, interrupted: true });
    expect(kinds(items)).toEqual(["tool", "discarded"]);
    expect(items[1]).toMatchObject({ words: 7 });
  });

  it("streams only the last text row of a live message", () => {
    const items = buildItems({ id: "m", parts: [call(), text("first"), tool("read_file"), call("running"), text("second")] }, { live: true, interrupted: false });
    expect(items.filter((i) => i.kind === "text").map((i) => (i.kind === "text" ? i.streaming : null))).toEqual([false, true]);
  });
});

describe("recovery divider placement (PRD U6, Frame 3)", () => {
  const rec = (over: Partial<TurnRecovery["recoveries"][number]> = {}, lostAt: number | null = 2_000): TurnRecovery =>
    ({
      turnId: "t1",
      lostAt,
      recoveries: [{ kind: "command_reattached", toolName: "workspace.run_command", lostAt, ...over }],
    }) as unknown as TurnRecovery;

  it("goes right after the row of the tool that was running when the Worker was lost", () => {
    const first = tool("run_command", { toolId: "cmd-1", output: "$ pytest -q tests/test_refund.py\nexit 1 · 0.3s" });
    const second = tool("run_command", { toolId: "cmd-2", output: "$ pytest -q\nexit 0 · 21.4s · reattached (attempt 2)" });
    const commit = tool("git_commit", { toolId: "c" });
    const times: Record<string, { startedAt: number; endedAt: number }> = {
      "cmd-1": { startedAt: 100, endedAt: 400 },
      "cmd-2": { startedAt: 1_000, endedAt: 25_000 },
      c: { startedAt: 26_000, endedAt: 27_000 },
    };
    const parts = [call(), first, call(), tool("edit_file", { toolId: "e" }), second, call(), text("All 27 pass."), commit, call(), text("Committed.")];
    const items = buildItems({ id: "m", parts }, { live: false, interrupted: false, recoveries: [rec()], toolTiming: (id) => times[id] });
    const at = items.findIndex((i) => i.kind === "recovery");
    expect(items[at - 1]).toMatchObject({ kind: "tool", part: { toolId: "cmd-2" } });
    expect(items.slice(at + 1).some((i) => i.kind === "tool" && i.part.toolId === "c")).toBe(true);
  });

  it("without times, finds the reattached command; with nothing to anchor on, stays at the end", () => {
    const a = tool("run_command", { toolId: "a", output: "$ x\nexit 1 · 1s" });
    const b = tool("run_command", { toolId: "b", output: "$ y\nexit 0 · 9s · reattached (attempt 2)" });
    const items = buildItems({ id: "m", parts: [call(), a, b, call(), text("done")] }, { live: false, interrupted: false, recoveries: [rec({}, null)] });
    expect(items[items.findIndex((i) => i.kind === "recovery") - 1]).toMatchObject({ part: { toolId: "b" } });

    const unknown = rec({ kind: "tool_call", toolName: "workspace.search_code" }, null);
    const plain = buildItems({ id: "m", parts: [call(), a, call(), text("done")] }, { live: false, interrupted: false, recoveries: [unknown] });
    expect(plain.map((i) => i.kind)).toEqual(["tool", "recovery", "text"]);
  });
});
