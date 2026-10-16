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
