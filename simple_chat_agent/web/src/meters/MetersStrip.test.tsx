import { describe, expect, it } from "vitest";
import { renderToStaticMarkup } from "react-dom/server";
import { MetersStrip, meterValues } from "./MetersStrip";
import { emptyAcc, foldFrames, summarize, type RawFrame } from "../session/telemetry";
import { formatTokens } from "./format";

function telemetry(frames: RawFrame[]) {
  const acc = emptyAcc();
  foldFrames(acc, frames);
  return summarize(acc);
}

const usage = (id: string, d: Record<string, unknown>): RawFrame => ({
  event: "x_usage",
  data: { id, input_tokens: null, output_tokens: null, cache_read_tokens: null, cache_write_tokens: null, thinking_tokens: null, confirmed: true, ...d },
});

describe("meters: not reported vs 0", () => {
  it("shows 0 when there were no model calls", () => {
    const v = meterValues(telemetry([]));
    expect(v.tokens).toBe("0 in / 0 out");
    expect(v.recoveries).toBe("0");
    expect(v.hero).toBeNull();
  });

  it("shows not reported, never 0, when calls report nothing", () => {
    const v = meterValues(telemetry([usage("u1", { provider: "openai" })]));
    expect(v.tokens).toBe("not reported");
    expect(v.tokensReported).toBe(false);
    expect(v.cache).toBe("not reported");
    expect(v.cacheReported).toBe(false);
  });

  it("shows a reported 0 as 0", () => {
    const v = meterValues(telemetry([usage("u1", { input_tokens: 1000, output_tokens: 0, cache_read_tokens: 0 })]));
    expect(v.tokens).toBe("1k in / 0 out");
    expect(v.cache).toBe("0%");
  });

  it("marks a single missing side as not reported", () => {
    const v = meterValues(telemetry([usage("u1", { input_tokens: 41_234, output_tokens: null })]));
    expect(v.tokens).toBe("41.2k in / not reported out");
  });

  it("renders not reported in the strip, without the discount note", () => {
    const html = renderToStaticMarkup(<MetersStrip telemetry={telemetry([usage("u1", {})])} modelLabel="GPT-5.5 · OpenAI" />);
    expect(html).toContain("not reported");
    expect(html).not.toContain("billed at a discount");
    expect(html).toContain("GPT-5.5 · OpenAI");
  });

  it("renders the cache share with its discount note when reported", () => {
    const html = renderToStaticMarkup(<MetersStrip telemetry={telemetry([usage("u1", { input_tokens: 1000, output_tokens: 10, cache_read_tokens: 820 })])} modelLabel={null} />);
    expect(html).toContain("82%");
    expect(html).toContain("billed at a discount");
  });
});

describe("meters: work kept hero", () => {
  const rec = (confirmed: boolean): RawFrame => ({
    event: "x_recovery",
    data: { id: "r1", turn_id: "t1", kind: "model_call", kept: { model_calls: 6, tool_calls: 9, tokens_in: 17_000, tokens_out: 1_200 }, repeated: { model_calls: 1 }, confirmed, retracted: false },
  });

  it("appears after the first recovery and says Checking… until confirmed", () => {
    const v = meterValues(telemetry([rec(false)]));
    expect(v.hero).toBe("Checking…");
    expect(v.recoveries).toBe("1");
  });

  it("shows the confirmed work kept", () => {
    const v = meterValues(telemetry([rec(true)]));
    expect(v.hero).toBe("6 model calls · 18.2k tokens · 9 tool calls");
    const html = renderToStaticMarkup(<MetersStrip telemetry={telemetry([rec(true)])} modelLabel={null} />);
    expect(html).toContain("Work kept after worker loss (this session)");
  });
});

describe("formatTokens", () => {
  it("abbreviates", () => {
    expect(formatTokens(950)).toBe("950");
    expect(formatTokens(41_234)).toBe("41.2k");
    expect(formatTokens(18_200)).toBe("18.2k");
    expect(formatTokens(250_000)).toBe("250k");
    expect(formatTokens(1_250_000)).toBe("1.25M");
  });
});
