import { describe, expect, it } from "vitest";
import { TelemetryFolder, emptyAcc, foldFrames, summarize, type RawFrame } from "./telemetry";

const usage = (id: string, over: Record<string, unknown> = {}): RawFrame => ({
  event: "x_usage",
  data: {
    id,
    agent_id: "root",
    turn_id: "t1",
    turn_number: 1,
    call_index: 0,
    model: "claude-sonnet-5-5",
    input_tokens: 1000,
    output_tokens: 100,
    cache_read_tokens: 800,
    cache_write_tokens: null,
    thinking_tokens: null,
    confirmed: false,
    timestamp: 1_760_000_000,
    ...over,
  },
});

const recovery = (id: string, over: Record<string, unknown> = {}): RawFrame => ({
  event: "x_recovery",
  data: {
    id,
    agent_id: "root",
    turn_id: "t1",
    turn_number: 1,
    kind: "model_call",
    lost_at: 1_760_000_100.5,
    kept: { model_calls: 6, tool_calls: 9, tokens_in: 17_000, tokens_out: 1_200 },
    repeated: { model_calls: 1, tool_calls: 0, resent_input_tokens_estimate: 4000 },
    confirmed: false,
    retracted: false,
    ...over,
  },
});

function fold(frames: RawFrame[]) {
  const acc = emptyAcc();
  foldFrames(acc, frames);
  return summarize(acc);
}

describe("usage folding", () => {
  it("lets the latest frame with an id win, and a confirmed frame replace the live one", () => {
    const t = fold([usage("u1"), usage("u1", { input_tokens: 1200, cache_write_tokens: 50, provider: "anthropic", confirmed: true })]);
    expect(t.calls).toHaveLength(1);
    expect(t.totals).toMatchObject({ calls: 1, input: 1200, cacheWrite: 50, confirmed: true });
    expect(Object.keys(t.byProvider)).toEqual(["anthropic"]);
  });

  it("never lets a late live frame downgrade a confirmed one", () => {
    const t = fold([usage("u1", { confirmed: true, input_tokens: 1200 }), usage("u1", { confirmed: false, input_tokens: 999 })]);
    expect(t.totals.input).toBe(1200);
    expect(t.totals.confirmed).toBe(true);
  });

  it("sums only reported counts and keeps null (not reported) when no call reports one", () => {
    const t = fold([usage("u1"), usage("u2", { input_tokens: 500, output_tokens: null, thinking_tokens: 30 })]);
    expect(t.totals.input).toBe(1500);
    expect(t.totals.output).toBe(100);
    expect(t.totals.thinking).toBe(30);
    expect(t.totals.cacheWrite).toBeNull();
    expect(t.totals.confirmed).toBe(false);
  });

  it("is null, never 0, when every call leaves a count out", () => {
    const t = fold([usage("u1", { input_tokens: null, output_tokens: null, cache_read_tokens: null })]);
    expect(t.totals.input).toBeNull();
    expect(t.totals.output).toBeNull();
    expect(t.totals.cacheShare).toBeNull();
  });

  it("computes the cache share only over calls that report cache reads", () => {
    const t = fold([
      usage("u1", { provider: "anthropic", input_tokens: 1000, cache_read_tokens: 800 }),
      usage("u2", { provider: "openai", input_tokens: 3000, cache_read_tokens: null }),
    ]);
    expect(t.totals.cacheShare).toBeCloseTo(0.8);
    expect(t.byProvider.anthropic?.cacheShare).toBeCloseTo(0.8);
    expect(t.byProvider.openai?.cacheShare).toBeNull();
    expect(t.byProvider.openai?.input).toBe(3000);
  });

  it("files usage without a provider under 'unknown' until confirmed", () => {
    expect(Object.keys(fold([usage("u1")]).byProvider)).toEqual(["unknown"]);
  });
});

describe("recovery folding", () => {
  it("groups by turn and takes the largest kept, not the sum, within a turn", () => {
    const t = fold([
      recovery("r-model"),
      recovery("r-tool", { kind: "command_reattached", kept: { model_calls: 6, tool_calls: 10, tokens_in: 17_000, tokens_out: 1_200 }, repeated: { model_calls: 0, tool_calls: 0 } }),
    ]);
    expect(t.turnRecoveries).toHaveLength(1);
    expect(t.recoveryTotals.count).toBe(1);
    expect(t.recoveryTotals.kept).toEqual({ modelCalls: 6, toolCalls: 10, tokensIn: 17_000, tokensOut: 1_200 });
    expect(t.turnRecoveries[0]?.repeatedModelCalls).toBe(1);
    expect(t.turnRecoveries[0]?.lostAt).toBe(1_760_000_100_500);
  });

  it("sums kept across turns and is confirmed only when every recovery is", () => {
    const t = fold([recovery("a", { confirmed: true }), recovery("b", { turn_id: "t2", kept: { model_calls: 1, tool_calls: 0, tokens_in: 10, tokens_out: 5 } })]);
    expect(t.recoveryTotals.count).toBe(2);
    expect(t.recoveryTotals.kept.modelCalls).toBe(7);
    expect(t.recoveryTotals.confirmed).toBe(false);
    const confirmed = fold([recovery("a", { confirmed: true })]);
    expect(confirmed.recoveryTotals.confirmed).toBe(true);
  });

  it("drops retracted recoveries from counts and dividers", () => {
    const t = fold([recovery("a"), recovery("a", { confirmed: true, retracted: true })]);
    expect(t.recoveries).toHaveLength(1);
    expect(t.turnRecoveries).toHaveLength(0);
    expect(t.recoveryTotals.count).toBe(0);
  });
});

describe("other frames", () => {
  it("tracks worker liveness from x_worker", () => {
    const suspect = fold([{ event: "x_worker", data: { id: "worker", state: "suspect", reason: "no worker is polling the task queue", since: 1_760_000_000, pollers: 0 } }]);
    expect(suspect.worker).toMatchObject({ answering: false, pollers: 0, since: 1_760_000_000_000 });
    const back = fold([
      { event: "x_worker", data: { state: "suspect", reason: "x", since: 1 } },
      { event: "x_worker", data: { state: "ok", reason: "frames are arriving again", since: 2 } },
    ]);
    expect(back.worker.answering).toBe(true);
  });

  it("records x_rejected by request id, with its kind, tool, code and reason", () => {
    const t = fold([
      {
        event: "x_rejected",
        data: { id: "rej:1", agent_id: "root", kind: "approval", request_id: "q1", tool_id: "t9", message_id: null, code: "already_resolved", reason: "That call was already answered.", error_type: "X", timestamp: 1 },
      },
    ]);
    expect(t.rejections).toEqual([
      { requestId: "q1", kind: "approval", toolId: "t9", messageId: null, code: "already_resolved", type: "already_resolved", message: "That call was already answered." },
    ]);
  });

  it("ignores harness events, counts unknown ones, and survives malformed data", () => {
    const t = fold([
      { event: "reply_delta", data: { text: "hi" } },
      { event: "x_future", data: {} },
      { event: "x_future", data: null },
      { event: "x_usage", data: "garbage" },
      { event: "x_usage", data: { no_id: true } },
      { event: "x_recovery", data: null },
      { event: "x_worker", data: 7 },
    ]);
    expect(t.unknownFrames).toEqual({ x_future: 2 });
    expect(t.calls).toHaveLength(0);
    expect(t.worker.answering).toBe(true);
  });
});

describe("TelemetryFolder", () => {
  it("folds only new frames and returns the same summary while nothing changed", () => {
    const folder = new TelemetryFolder();
    const a = [usage("u1")];
    const s1 = folder.update(a);
    expect(folder.update(a)).toBe(s1);
    const b = [...a, { event: "reply_delta", data: {} }];
    expect(folder.update(b)).toBe(s1);
    const c = [...b, usage("u2")];
    expect(folder.update(c).totals.calls).toBe(2);
  });

  it("resets when the frames are replaced (a new connection replays from scratch)", () => {
    const folder = new TelemetryFolder();
    folder.update([usage("u1"), usage("u2")]);
    expect(folder.update([usage("u3")]).totals.calls).toBe(1);
  });
});
