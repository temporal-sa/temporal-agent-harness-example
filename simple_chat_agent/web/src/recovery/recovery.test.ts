import { describe, expect, it } from "vitest";

import type { Recovery, SendRecord, TurnRecovery } from "../session";
import { CHECKING, COMPARISON, dividerView } from "./divider";
import { temporalUiUrl } from "./banner";
import { hms } from "./format";
import { trailSteps } from "./sendTrail";

const LOST = Date.UTC(2026, 9, 16, 14, 2, 11);

function rec(over: Partial<Recovery> = {}): Recovery {
  return {
    id: "recovery:a:t:0",
    agentId: "a",
    turnId: "t",
    turnNumber: 1,
    kind: "model_call",
    callIndex: 6,
    activityId: "7",
    toolName: null,
    attempt: 2,
    cause: "heartbeat_timeout",
    detectedAt: LOST + 5000,
    lostAt: LOST,
    lostWorker: "worker-a",
    resumedOn: "worker-b",
    kept: { modelCalls: 6, toolCalls: 9, tokensIn: 17_000, tokensOut: 1_200 },
    repeated: { modelCalls: 1, toolCalls: 0, resentInputTokensEstimate: 4000 },
    confirmed: false,
    retracted: false,
    ...over,
  };
}

function turn(recoveries: Recovery[]): TurnRecovery {
  return {
    agentId: "a",
    turnId: "t",
    turnNumber: 1,
    recoveries,
    kept: recoveries[0]!.kept,
    repeatedModelCalls: recoveries.reduce((n, r) => n + r.repeated.modelCalls, 0),
    repeatedToolCalls: recoveries.reduce((n, r) => n + r.repeated.toolCalls, 0),
    lostAt: LOST,
    confirmed: recoveries.every((r) => r.confirmed),
  };
}

describe("recovery divider", () => {
  it("says Checking… until history confirms, then shows kept and repeated work", () => {
    const live = dividerView(turn([rec()]));
    expect(live.checking).toBe(true);
    // Until confirmed, no claim of what was saved (UX review m1).
    expect(live.title).toBe("Another Worker took over · checking");
    expect(live.body).toBe("Another Worker (worker-b) is continuing this turn at the model call that was in flight.");
    expect(live.figures).toEqual([
      { label: "Kept", value: CHECKING, tone: "kept" },
      { label: "Repeated", value: CHECKING, tone: "rep" },
    ]);

    const confirmed = dividerView(turn([rec({ confirmed: true })]));
    expect(confirmed.checking).toBe(false);
    expect(confirmed.title).toBe(`Recovered · Worker lost at ${hms(LOST)}`);
    expect(confirmed.figures.map((f) => `${f.label} ${f.value}`)).toEqual([
      "Kept 6 model calls (18.2k tokens)",
      "Kept 9 tool calls",
      "Repeated 1 model call (attempt 2)",
    ]);
    expect(confirmed.body).toBe("Another Worker (worker-b) continued this turn at the model call that was in flight.");
  });

  it("stays Checking… while any recovery in the turn is unconfirmed", () => {
    const v = dividerView(turn([rec({ confirmed: true }), rec({ id: "x", kind: "command_reattached", confirmed: false })]));
    expect(v.checking).toBe(true);
  });

  it("claims the command didn't start over only once confirmed", () => {
    const unconfirmed = dividerView(turn([rec({ kind: "command_reattached", toolName: "workspace.run_command" })]));
    expect(unconfirmed.body).not.toContain("did not start over");
    expect(unconfirmed.body).toBe("Another Worker (worker-b) is continuing this turn at the command it was running.");
  });

  it("names a reattached command and says it didn't start over", () => {
    const v = dividerView(
      turn([rec({ kind: "command_reattached", toolName: "workspace.run_command", confirmed: true, repeated: { modelCalls: 0, toolCalls: 0, resentInputTokensEstimate: null } })]),
    );
    expect(v.body).toContain("It reattached to the same run, so the command did not start over.");
    expect(v.figures[1]!.value).toBe("9 tool calls plus the running command");
    expect(v.figures[2]!.value).toBe("nothing");
  });

  it("states the comparison", () => {
    expect(COMPARISON).toBe("compared with re-running this turn from your message");
  });
});

describe("send-state trail", () => {
  const record = (phase: SendRecord["phase"], over: Partial<SendRecord> = {}): SendRecord => ({
    id: "1",
    kind: "message",
    messageId: null,
    toolId: null,
    approved: null,
    remember: false,
    phase,
    at: { sending: 1000, in_temporal: 1100 },
    error: null,
    ...over,
  });

  it("walks Sending → In Temporal → Accepted → Running", () => {
    expect(trailSteps(record("accepted")).map((s) => `${s.label}:${s.state}`)).toEqual([
      "Sending:done",
      "In Temporal:done",
      "Accepted:now",
      "Running:todo",
    ]);
    expect(trailSteps(record("done")).every((s) => s.state === "done")).toBe(true);
  });

  it("says when a send waits in Temporal for a worker", () => {
    expect(trailSteps(record("in_temporal"), { now: 1500 })[1]!.label).toBe("In Temporal");
    expect(trailSteps(record("in_temporal"), { now: 4000 })[1]!.label).toBe("In Temporal · waiting for a Worker");
    expect(trailSteps(record("in_temporal"), { now: 1200, workerAnswering: false })[1]!.label).toBe("In Temporal · waiting for a Worker");
  });

  it("ends a decline at Accepted", () => {
    expect(trailSteps(record("accepted", { kind: "approval", approved: false })).map((s) => s.label)).toEqual(["Sending", "In Temporal", "Accepted"]);
  });
});

describe("Temporal UI link", () => {
  const T = "https://ui.example/namespaces/demo/workflows/{workflow_id}";
  it("opens the run's pending Activities when the facade knows the run id", () => {
    expect(temporalUiUrl(T, "coding-agent/s 1", "abc")).toBe("https://ui.example/namespaces/demo/workflows/coding-agent%2Fs%201/abc/pending-activities");
    expect(temporalUiUrl(`${T}?x=1`, "w", "r")).toBe("https://ui.example/namespaces/demo/workflows/w/r/pending-activities?x=1");
  });
  it("falls back to the Workflow page without a run id, and to nothing without a template", () => {
    expect(temporalUiUrl(T, "w", null)).toBe("https://ui.example/namespaces/demo/workflows/w");
    expect(temporalUiUrl(null, "w", "r")).toBeNull();
  });
});
