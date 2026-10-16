import { describe, expect, it } from "vitest";
import { modelInUse, alwaysApprovedFrom, currentModel, currentPosture, sessionStatus, subagentsOf, toChatAttachments, visibleMessages } from "./derive";
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

describe("always approved", () => {
  const resolved = (tool_name: string, remember: boolean, agent_id = "root", approved = true) => ({
    event: "tool_approval_resolved",
    data: { agent_id, tool_id: `t-${tool_name}`, tool_name, approved, reason: null, remember },
  });
  const forget = { event: "message_accepted", data: { agent_id: "root", handler: "forget_always_allowed", message_id: "f1", payload: {} } };

  it("lists remembered approvals of the root agent only", () => {
    const frames = [resolved("edit_file", true), resolved("run_command", false), resolved("write_file", true, "root-explorer-1"), resolved("read_file", true, "root", false)];
    expect(alwaysApprovedFrom(frames, "root")).toEqual(["edit_file"]);
  });

  it("empties once a forget is admitted, and refills from later approvals", () => {
    expect(alwaysApprovedFrom([resolved("edit_file", true), resolved("write_file", true), forget], "root")).toEqual([]);
    expect(alwaysApprovedFrom([resolved("edit_file", true), forget, resolved("write_file", true)], "root")).toEqual(["write_file"]);
  });
});

describe("chat attachments", () => {
  it("map upload results to the chat payload, at most 20", () => {
    expect(toChatAttachments([{ attachment_id: "a1", name: "spec.md", size_bytes: 120, mime_type: "text/markdown" }])).toEqual([
      { attachment_id: "a1", name: "spec.md", size: 120, content_type: "text/markdown" },
    ]);
    expect(toChatAttachments([{ attachment_id: "a2" }])).toEqual([{ attachment_id: "a2" }]);
    const many = Array.from({ length: 25 }, (_, i) => ({ attachment_id: `a${i}` }));
    expect(toChatAttachments(many)).toHaveLength(20);
  });
});

describe("settings replies (SettingsAck)", () => {
  const seed = { provider: "anthropic" as const, model: "claude-sonnet-5-5" };
  it("uses the value the agent applied", () => {
    const msgs = [control("set_model", { model: { provider: "openai", model: "gpt-5.5" } }, { output: { ok: true, detail: "", setting: "model", model: { provider: "openai", model: "gpt-5.5", effort: "low" }, posture: null } })];
    expect(currentModel(msgs, seed)).toEqual({ provider: "openai", model: "gpt-5.5", effort: "low" });
  });

  it("reverts to the previous value when the agent refuses", () => {
    const refusedModel = control("set_model", { model: { provider: "google", model: "gemini-x" } }, { output: { ok: false, detail: "No google key.", setting: "model", model: null, posture: null } });
    const refusedPosture = control("set_posture", { posture: "auto" }, { output: { ok: false, detail: "Auto mode isn't available: the workers have no anthropic key for the judge.", setting: "posture", model: null, posture: null } });
    expect(currentModel([refusedModel], seed)).toEqual(seed);
    expect(currentPosture([refusedPosture], "trust_safe")).toBe("trust_safe");
  });

  it("applies the asked-for value while the reply is pending", () => {
    const pending = control("set_posture", { posture: "auto" }, { status: "running", output: null, endedAt: null });
    expect(currentPosture([pending], "trust_safe")).toBe("auto");
  });
});

describe("model in use (UX verification nit)", () => {
  it("is the previous model while a change waits for the next turn", () => {
    const seed = { provider: "anthropic" as const, model: "claude-sonnet-5-5" };
    const chat = control("chat", { text: "go" }, { turnNumber: 2, status: "running" });
    const change = control("set_model", { model: { provider: "anthropic", model: "claude-opus-5" } }, { turnNumber: 2 });
    expect(modelInUse([chat, change], seed, "busy")).toEqual(seed);
    expect(modelInUse([chat, change], seed, "idle")).toMatchObject({ model: "claude-opus-5" });
  });
});
