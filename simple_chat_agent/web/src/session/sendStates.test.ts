import { describe, expect, it } from "vitest";
import { recordForUnsent, advance, approvalPhase, landed, messagePhase, observe, startRecord, stuckInTemporal, WAITING_FOR_WORKER_MS, type StreamView } from "./sendStates";
import type { Message, Rejection, ToolPart } from "./types";

const msg = (over: Partial<Message>): Message =>
  ({
    id: "m1",
    handler: "chat",
    input: { text: "hi" },
    output: null,
    status: "accepted",
    disposition: null,
    turnNumber: null,
    parts: [],
    error: null,
    acceptedAt: null,
    startedAt: null,
    endedAt: null,
    ...over,
  }) as Message;

const tool = (over: Partial<ToolPart>): ToolPart => ({
  type: "tool",
  toolId: "t1",
  toolName: "edit_file",
  state: "awaiting_approval",
  input: {},
  output: null,
  error: null,
  progress: null,
  approval: null,
  evaluations: [],
  callback: null,
  ...over,
});

const rej = (message: string, over: Partial<Rejection> = {}): Rejection => ({
  requestId: "r",
  kind: "message",
  toolId: null,
  messageId: null,
  code: "rejected",
  type: "rejected",
  message,
  ...over,
});

const view = (messages: Message[] = [], tools: ToolPart[] = [], rejections: Record<string, string> = {}, byTool: Record<string, Rejection> = {}): StreamView => ({
  message: (id) => messages.find((m) => m.id === id),
  tool: (id) => tools.find((t) => t.toolId === id),
  rejection: (id) => (rejections[id] ? rej(rejections[id]) : undefined),
  approvalRejection: (id) => byTool[id],
});

describe("message send states", () => {
  it("walks Sending → In Temporal → Accepted → Running → done with timestamps", () => {
    let r = startRecord("s1", "message", 100);
    expect(r.phase).toBe("sending");
    r = landed(r, 150, "m1");
    expect(r).toMatchObject({ phase: "in_temporal", messageId: "m1" });

    // The client marks a message "accepted" as soon as the POST returns; without the stream's
    // message_accepted (acceptedAt) it is still only in Temporal.
    let [next] = observe([r], view([msg({ status: "accepted", acceptedAt: null })]), 200);
    expect(next?.phase).toBe("in_temporal");

    [next] = observe([r], view([msg({ acceptedAt: 1_760_000_000 })]), 300);
    expect(next?.phase).toBe("accepted");
    r = next!;
    [next] = observe([r], view([msg({ status: "running", acceptedAt: 1, startedAt: 2 })]), 400);
    expect(next?.phase).toBe("running");
    r = next!;
    [next] = observe([r], view([msg({ status: "done", acceptedAt: 1, startedAt: 2, endedAt: 3 })]), 500);
    expect(next?.phase).toBe("done");
    expect(next?.at).toEqual({ sending: 100, in_temporal: 150, accepted: 300, running: 400, done: 500 });
  });

  it("stamps skipped phases with the time it jumped", () => {
    const r = landed(startRecord("s1", "message", 100), 150, "m1");
    const [next] = observe([r], view([msg({ status: "running", acceptedAt: 1, startedAt: 2 })]), 900);
    expect(next?.at).toMatchObject({ accepted: 900, running: 900 });
  });

  it("never moves backward and doesn't change once settled", () => {
    const r = advance(landed(startRecord("s1", "message", 0), 1, "m1"), "running", 2);
    expect(advance(r, "accepted", 3)).toBe(r);
    const done = advance(r, "done", 4);
    expect(advance(done, "failed", 5)).toBe(done);
  });

  it("fails on a handler error or an x_rejected for its id", () => {
    const r = landed(startRecord("s1", "message", 0), 1, "m1");
    expect(observe([r], view([msg({ status: "error", error: "boom" })]), 2)[0]).toMatchObject({ phase: "failed", error: "boom" });
    expect(observe([r], view([], [], { m1: "Refused." }), 2)[0]).toMatchObject({ phase: "failed", error: "Refused." });
  });

  it("returns the same array when nothing changed", () => {
    const records = [landed(startRecord("s1", "message", 0), 1, "m1")];
    expect(observe(records, view(), 5)).toBe(records);
  });

  it("leaves a record that is still sending alone", () => {
    const records = [startRecord("s1", "message", 0)];
    expect(observe(records, view([msg({ acceptedAt: 1 })]), 5)).toBe(records);
  });

  it("maps message states to phases", () => {
    expect(messagePhase(msg({}))).toBeNull();
    expect(messagePhase(msg({ acceptedAt: 1 }))).toBe("accepted");
    expect(messagePhase(msg({ status: "done" }))).toBe("done");
  });
});

describe("approval send states", () => {
  it("stays in Temporal while the call waits, then follows the tool", () => {
    const r = landed(startRecord("a1", "approval", 0, { toolId: "t1", approved: true }), 10);
    expect(observe([r], view([], [tool({})]), 20)[0]?.phase).toBe("in_temporal");
    expect(observe([r], view([], [tool({ state: "approved", approval: { approved: true, reason: null, remember: false } })]), 30)[0]?.phase).toBe("accepted");
    expect(observe([r], view([], [tool({ state: "running" })]), 40)[0]?.phase).toBe("running");
    expect(observe([r], view([], [tool({ state: "denied" })]), 50)[0]?.phase).toBe("done");
  });

  it("fails on a rejection with its reason, except already_resolved, which counts as answered", () => {
    const r = landed(startRecord("a1", "approval", 0, { toolId: "t1", approved: true }), 10);
    const unknown = rej("That approval is unknown.", { kind: "approval", toolId: "t1", code: "unknown_approval" });
    expect(observe([r], view([], [tool({})], {}, { t1: unknown }), 20)[0]).toMatchObject({ phase: "failed", error: "That approval is unknown." });
    const resolved = rej("Already decided.", { kind: "approval", toolId: "t1", code: "already_resolved" });
    expect(observe([r], view([], [tool({ state: "denied" })], {}, { t1: resolved }), 20)[0]).toMatchObject({ phase: "done", error: null });
  });

  it("maps tool states", () => {
    expect(approvalPhase({ state: "evaluating", approval: null })).toBeNull();
    expect(approvalPhase({ state: "done", approval: null })).toBe("done");
  });
});

describe("waiting for a worker", () => {
  it("reports a request that sat in Temporal for 5 s", () => {
    const r = landed(startRecord("s1", "message", 0), 1000, "m1");
    expect(stuckInTemporal([r], 1000 + WAITING_FOR_WORKER_MS - 1)).toBe(false);
    expect(stuckInTemporal([r], 1000 + WAITING_FOR_WORKER_MS)).toBe(true);
  });
});

describe("unsent messages find their record (UX verification 3)", () => {
  it("by the error text for a failed send, else the send still going", () => {
    const failed = { ...advance(startRecord("a", "message", 0), "failed", 1, "Bad Gateway", { status: 502 }) };
    const going = startRecord("b", "steer", 2);
    const records = [failed, going];
    expect(recordForUnsent(records, { id: "local-1", handler: "chat", status: "error", error: "Bad Gateway" })).toBe(failed);
    expect(recordForUnsent(records, { id: "local-2", handler: "steer", status: "sending", error: null })).toBe(going);
    expect(recordForUnsent(records, { id: "local-3", handler: "chat", status: "error", error: "other" })).toBeUndefined();
  });
});
