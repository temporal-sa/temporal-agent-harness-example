import { describe, expect, it } from "vitest";

import { call, message, text, tool } from "../transcript/fixtures";
import { buildItems } from "../transcript/items";
import {
  callbackRequest,
  callbackStatus,
  hostCallParent,
  mcpTool,
  nestHostCalls,
  reauthServer,
  replySources,
  screenFiles,
  toolArgs,
  toolDisplayName,
  webSearchOn,
  type CallbackSend,
} from "./extras";
import type { Message, Rejection } from "./types";

const rejection = (requestId: string, over: Partial<Rejection> = {}): Rejection => ({
  requestId,
  kind: "callback",
  toolId: "t1",
  messageId: null,
  code: "invalid_answer",
  type: "invalid_answer",
  message: "That option wasn't offered.",
  ...over,
});
const send = (over: Partial<CallbackSend> = {}): CallbackSend => ({
  toolId: "t1",
  requestId: "cb-1",
  phase: "sent",
  action: "answer",
  error: null,
  errorCode: null,
  errorStatus: null,
  ...over,
});

describe("callbacks", () => {
  it("reads ask_user and pick_files input", () => {
    expect(callbackRequest("ask_user", { question: "Which?", options: [{ id: "a", label: "A" }, { id: "b" }, { label: "no id" }] })).toEqual({
      kind: "ask_user",
      question: "Which?",
      options: [
        { id: "a", label: "A", description: null },
        { id: "b", label: "b", description: null },
      ],
    });
    expect(callbackRequest("pick_files", { prompt: "CSV please", max_files: 40 })).toEqual({ kind: "pick_files", prompt: "CSV please", maxFiles: 10 });
    expect(callbackRequest("pick_files", {})).toMatchObject({ maxFiles: 5 });
    expect(callbackRequest("other_tool", {})).toEqual({ kind: "other", toolName: "other_tool" });
  });

  it("goes open → sending → sent, and failed on a refused request", () => {
    expect(callbackStatus(undefined, [])).toEqual({ state: "open" });
    expect(callbackStatus(send({ phase: "sending" }), [])).toEqual({ state: "sending", action: "answer" });
    expect(callbackStatus(send(), [])).toEqual({ state: "sent", action: "answer" });
    expect(callbackStatus(send({ phase: "failed", error: "nope", errorCode: "unknown_upload", errorStatus: 422 }), [])).toMatchObject({
      state: "failed",
      code: "unknown_upload",
    });
  });

  it("shows the agent's refusal of this answer only", () => {
    // Another request's refusal (an earlier answer) doesn't count against this one.
    expect(callbackStatus(send(), [rejection("cb-0")])).toEqual({ state: "sent", action: "answer" });
    expect(callbackStatus(send(), [rejection("cb-0"), rejection("cb-1")])).toEqual({
      state: "rejected",
      code: "invalid_answer",
      reason: "That option wasn't offered.",
    });
  });

  it("screens picked files by size and room", () => {
    const big = { name: "big.bin", size: 6 * 1024 * 1024 };
    const a = { name: "a.csv", size: 10 };
    const b = { name: "b.csv", size: 10 };
    const r = screenFiles([big, a, b], 1);
    expect(r.accepted).toEqual([a]);
    expect(r.refused.map((x) => x.file)).toEqual([big, b]);
  });
});

describe("Code Mode host calls", () => {
  it("finds a host call's script", () => {
    expect(hostCallParent("toolu_1/3")).toBe("toolu_1");
    expect(hostCallParent("toolu_1")).toBeNull();
    expect(hostCallParent("toolu_1/0")).toBeNull();
  });

  it("nests host calls under their script, in call order, and leaves orphans as rows", () => {
    const script = tool("run_script", { toolId: "s1", input: { script: "print(1)" } });
    const h2 = tool("read_file", { toolId: "s1/2" });
    const h1 = tool("list_files", { toolId: "s1/1" });
    const orphan = tool("read_file", { toolId: "gone/1" });
    const { parts, hostCalls } = nestHostCalls([script, h2, h1, orphan]);
    expect(parts).toEqual([script, orphan]);
    expect(hostCalls.get("s1")?.map((p) => p.toolId)).toEqual(["s1/1", "s1/2"]);

    const items = buildItems(message([call(), script, h1, h2, text("Done.")]), { live: false, interrupted: false });
    const row = items.find((i) => i.kind === "tool");
    expect(row?.kind === "tool" && row.hostCalls?.length).toBe(2);
    expect(items.filter((i) => i.kind === "tool" || i.kind === "group")).toHaveLength(1);
  });
});

describe("MCP", () => {
  it("names MCP tools and unwraps their arguments", () => {
    expect(mcpTool("mcp__local-echo__echo")).toEqual({ server: "local-echo", tool: "echo" });
    expect(mcpTool("mcp__gh__search__code")).toEqual({ server: "gh", tool: "search__code" });
    expect(mcpTool("read_file")).toBeNull();
    expect(toolDisplayName("mcp__local-echo__echo")).toBe("local-echo · echo");
    expect(toolArgs("mcp__local-echo__echo", { arguments: { text: "hi" } })).toEqual({ text: "hi" });
    expect(toolArgs("read_file", { path: "a" })).toEqual({ path: "a" });
  });

  it("detects a reauth error and its server", () => {
    const err = "mcp_reauth_required: MCP server 'docs' needs you to reconnect. Open MCP settings and choose Reconnect, then try again.";
    expect(reauthServer({ toolName: "mcp__docs__search", state: "failed", error: err })).toBe("docs");
    expect(reauthServer({ toolName: "other", state: "failed", error: err })).toBe("docs");
    expect(reauthServer({ toolName: "mcp__docs__search", state: "failed", error: "mcp_unreachable: down" })).toBeNull();
    expect(reauthServer({ toolName: "mcp__docs__search", state: "done", error: null })).toBeNull();
  });
});

describe("web search", () => {
  it("extracts http(s) sources with their domain, without repeats", () => {
    const out = {
      text: "…",
      sources: [
        { title: "Temporal docs", url: "https://www.docs.temporal.io/workflows" },
        { title: "", url: "http://example.com/a" },
        { title: "Again", url: "https://www.docs.temporal.io/workflows" },
        { title: "Bad", url: "javascript:alert(1)" },
        { title: "Not a url", url: "nope" },
      ],
    };
    expect(replySources(out)).toEqual([
      { title: "Temporal docs", url: "https://www.docs.temporal.io/workflows", domain: "docs.temporal.io" },
      { title: "example.com", url: "http://example.com/a", domain: "example.com" },
    ]);
    expect(replySources({ text: "no sources" })).toEqual([]);
    expect(replySources(null)).toEqual([]);
  });

  it("follows admitted set_web_search messages", () => {
    const m = (enabled: boolean, over: Partial<Message> = {}) =>
      ({ id: `w${enabled}`, handler: "set_web_search", input: { enabled }, output: null, status: "done", acceptedAt: 1, ...over }) as Message;
    expect(webSearchOn([], false)).toBe(false);
    expect(webSearchOn([m(true, { status: "accepted" })], false)).toBe(true);
    expect(webSearchOn([m(true, { output: { ok: false, web_search: false, setting: "web_search", detail: "Not here." } })], false)).toBe(false);
    expect(webSearchOn([m(true), m(false, { acceptedAt: null })], false)).toBe(true);
  });
});
