import { describe, expect, it } from "vitest";
import { currentModel, currentPosture, sessionStatus, subagentsOf, visibleMessages } from "./derive";
import { HEALTHY } from "./telemetry";
import type { AgentNode, Message, PendingApproval } from "./types";

const control = (handler: string, input: Record<string, unknown>, over: Partial<Message> = {}): Message =>
  ({ id: handler, handler, input, output: null, status: "done", disposition: null, turnNumber: 1, parts: [], error: null, acceptedAt: 1, startedAt: 1, endedAt: 1, ...over }) as Message;

describe("session status", () => {
  const base = { pendingApprovals: [] as PendingApproval[], anyBusy: false, worker: HEALTHY, waitingForWorker: false };
  it("orders Waiting for you, Worker isn't answering, Running, Idle", () => {
    expect(sessionStatus(base)).toBe("idle");
    expect(sessionStatus({ ...base, anyBusy: true })).toBe("running");
    expect(sessionStatus({ ...base, anyBusy: true, worker: { ...HEALTHY, answering: false } })).toBe("worker_unresponsive");
    expect(sessionStatus({ ...base, waitingForWorker: true })).toBe("worker_unresponsive");
    expect(sessionStatus({ ...base, anyBusy: true, worker: { ...HEALTHY, answering: false }, pendingApprovals: [{} as PendingApproval] })).toBe("waiting_for_you");
  });
  it("ignores a suspect worker when no turn is open", () => {
    expect(sessionStatus({ ...base, worker: { ...HEALTHY, answering: false } })).toBe("idle");
  });
});

describe("model and posture", () => {
  it("starts from the session and follows admitted set_model and set_posture messages", () => {
    const seed = { provider: "anthropic" as const, model: "claude-sonnet-5-5" };
    const msgs = [
      control("set_model", { model: { provider: "openai", model: "gpt-5.5", effort: "medium" } }),
      control("set_model", { model: { provider: "google", model: "x" } }, { acceptedAt: null, status: "accepted" }),
      control("set_posture", { posture: "auto" }),
      control("set_posture", { posture: "bogus" }),
    ];
    expect(currentModel([], seed)).toEqual(seed);
    expect(currentModel(msgs, seed)).toEqual({ provider: "openai", model: "gpt-5.5", effort: "medium" });
    expect(currentPosture(msgs, "trust_safe")).toBe("auto");
  });
});

describe("subagents", () => {
  it("drops the root and the projection's null agent", () => {
    const node = (parentId: string | null, n = 0) => ({ parentId, messages: new Array(n) }) as unknown as AgentNode;
    const out = subagentsOf({ root: node(null, 1), null: node(null), "root-explorer-1": node("root", 1) }, "root");
    expect(Object.keys(out)).toEqual(["root-explorer-1"]);
  });
});

describe("rejection carriers", () => {
  it("are never shown as messages", () => {
    const list = [{ id: "m1" }, { id: "x_rejected:q1" }];
    expect(visibleMessages(list).map((m) => m.id)).toEqual(["m1"]);
    const clean = [{ id: "m1" }];
    expect(visibleMessages(clean)).toBe(clean);
  });
});
