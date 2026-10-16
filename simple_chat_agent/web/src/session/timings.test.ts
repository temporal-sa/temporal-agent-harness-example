import { describe, expect, it } from "vitest";
import { TimingFolder, thinkingSeconds, timingKey } from "./timings";

const f = (event: string, ts: number, message_id = "m1") => ({ event, data: { agent_id: "root", message_id, timestamp: ts } });

describe("model call timing", () => {
  it("records each call's start, first thought, first output and end", () => {
    const folder = new TimingFolder();
    const map = folder.update([
      f("model_interaction_started", 100),
      f("thought_summary", 101),
      f("thought_summary", 105),
      f("reply_delta", 112),
      f("model_interaction_ended", 114),
      f("model_interaction_started", 120),
      f("tool_requested", 123),
    ]);
    const calls = map.get(timingKey("root", "m1"));
    expect(calls).toHaveLength(2);
    expect(calls?.[0]).toEqual({ startedAt: 100_000, firstThoughtAt: 101_000, firstOutputAt: 112_000, endedAt: 114_000 });
    expect(thinkingSeconds(calls?.[0])).toBe(12);
    expect(thinkingSeconds(calls?.[1])).toBe(3);
    expect(thinkingSeconds(undefined)).toBeNull();
  });

  it("ignores frames without an agent, message or timestamp", () => {
    const folder = new TimingFolder();
    expect(folder.update([{ event: "model_interaction_started", data: { agent_id: "root" } }, { event: "x_usage", data: null }]).size).toBe(0);
  });
});

describe("tool timing", () => {
  it("records each tool call's start and end", () => {
    const folder = new TimingFolder();
    folder.update([
      { event: "tool_start", data: { agent_id: "root", message_id: "m1", tool_id: "t1", timestamp: 10 } },
      { event: "tool_end", data: { agent_id: "root", message_id: "m1", tool_id: "t1", timestamp: 31.5 } },
      { event: "tool_start", data: { agent_id: "root", message_id: "m1", tool_id: "t2", timestamp: 40 } },
    ]);
    expect(folder.tools.get("t1")).toEqual({ startedAt: 10_000, endedAt: 31_500 });
    expect(folder.tools.get("t2")).toEqual({ startedAt: 40_000, endedAt: null });
  });
});
