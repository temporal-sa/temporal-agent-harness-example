import { describe, expect, it } from "vitest";
import { renderToStaticMarkup } from "react-dom/server";
import { Transcript, notSentText } from "./Transcript";
import { TranscriptContext, type TranscriptContextValue } from "./context";
import { call, message, text, tool } from "./fixtures";
import type { CodingSession, Message } from "../session";
import { summarize, emptyAcc } from "../session/telemetry";

function render(messages: Message[], over: Partial<CodingSession> = {}) {
  const session = {
    messages,
    unsent: [],
    rootAgentId: "root",
    telemetry: summarize(emptyAcc()),
    sendFor: () => undefined,
    toolProgress: {},
    pendingApprovals: [],
    agent: () => undefined,
    modelCallTiming: () => ({ startedAt: 0, firstThoughtAt: 0, firstOutputAt: 4000, endedAt: 5000 }),
    ...over,
  } as unknown as CodingSession;
  const value: TranscriptContextValue = {
    session,
    openFile: () => undefined,
    modelLabel: (p, m) => `${m} (${p})`,
    providerOfModel: (m) => (m?.startsWith("gpt") ? "OpenAI" : "Anthropic"),
    postureLabel: (id) => (id === "auto" ? "Auto mode" : id),
    workerAnswering: true,
  };
  return renderToStaticMarkup(
    <TranscriptContext.Provider value={value}>
      <Transcript />
    </TranscriptContext.Provider>,
  );
}

const EDIT = "Edited a.py (+1 -1).\n--- a/a.py\n+++ b/a.py\n@@ -1 +1 @@\n-old\n+new";

describe("Transcript", () => {
  it("says when a provider gives no reasoning summary, with the time it thought", () => {
    const html = render([message([call("done", { model: "gpt-5.5", usage: { input_tokens: 1, output_tokens: 1, thought_tokens: 50, cached_tokens: null } as never }), text("Done.")])]);
    expect(html).toContain("Thought for 4s");
    expect(html).toContain("50 reasoning tokens");
    expect(html).toContain("Reasoning summary not provided by OpenAI");
  });

  it("draws edit diffs with add and del rows and never the error color", () => {
    const html = render([message([call(), tool("edit_file", { input: { path: "a.py" }, output: EDIT })])]);
    expect(html).toContain('class="del"');
    expect(html).toContain('class="add"');
    expect(html).not.toContain("badge error");
    expect(html).toContain(">a.py</button>");
  });

  it("shows an exit 1 command as a yellow badge, not an error", () => {
    const html = render([message([call(), tool("run_command", { input: { command: "pytest -q" }, output: "$ pytest -q\nexit 1 · 0.3s" })])]);
    expect(html).toContain('class="exit fail"');
    expect(html).toContain("exit 1");
  });

  it("shows model and posture changes as lines, and the reply discarded after Stop", () => {
    const html = render([
      message([], { id: "s1", handler: "set_model", input: { model: { provider: "openai", model: "gpt-5.5" } } }),
      message([], { id: "s2", handler: "set_posture", input: { posture: "auto" } }),
      message([call(), tool("read_file"), call("running"), text("half a thought")], { id: "m2", output: { text: "", interrupted: true } }),
    ]);
    expect(html).toContain("Model changed to gpt-5.5 (openai) · applies from this turn");
    expect(html).toContain("Approvals: Auto mode");
    expect(html).toContain("Discarded: the reply after you pressed Stop (3 words)");
    expect(html).not.toContain("half a thought");
  });

  it("shows attached files as chips on the user's message, not in its text", () => {
    const html = render([message([], { input: { text: "Read the spec", attachments: [{ attachment_id: "att_1", name: "spec.md" }] } })]);
    expect(html).toContain("Read the spec");
    expect(html).toContain('aria-label="Attached files"');
    expect(html).toContain(">spec.md</li>");
    expect(html).not.toContain("att_1");
  });

  it("shows the agent's answer to Clear as a line", () => {
    const html = render([
      message([], { id: "f1", handler: "forget_always_allowed", input: {}, output: { ok: true, detail: "These ask again: edit_file, write_file.", forgotten: ["edit_file", "write_file"] } }),
    ]);
    expect(html).toContain("These ask again: edit_file, write_file.");
  });

  it("words failures plainly and keeps the raw text behind Details (UX review M9)", () => {
    const html = render([
      message([call(), tool("run_command", { state: "failed", error: "ApplicationError: ExecutorUnavailable(503)" })], { status: "error", error: "Traceback: KeyError 'x'" }),
      message([], { id: "s9", handler: "set_model", status: "error", error: "model_not_allowed: openai:gpt-9" }),
    ]);
    expect(html).toContain("The agent couldn&#x27;t finish this message.");
    expect(html).toContain("<summary>Details</summary><pre>Traceback: KeyError &#x27;x&#x27;</pre>");
    expect(html).toContain("The call failed. The agent sees the error and decides what to do next.");
    expect(html).toContain("Couldn&#x27;t change the model to");
    expect(html).not.toContain("model_not_allowed: openai:gpt-9</div>");
  });

  it("capitalizes Worker in the lost-attempt fold (UX review m12)", () => {
    const html = render([message([call("running"), text("partial words"), call(), text("final")])]);
    expect(html).toContain("Attempt 1 (Worker lost)");
  });

  it("says when the agent refused a posture change, with its reason", () => {
    const html = render([
      message([], { id: "p1", handler: "set_posture", input: { posture: "auto" }, output: { ok: false, detail: "Auto mode isn't available: the workers have no anthropic key for the judge.", setting: "posture", model: null, posture: null } }),
    ]);
    expect(html).toContain("Approvals didn&#x27;t change. Auto mode isn&#x27;t available: the workers have no anthropic key for the judge.");
    expect(html).not.toContain("Approvals: auto");
  });

  it("mutes a waiting row while the decision card shows it (UX review m11)", () => {
    const waiting = tool("edit_file", { toolId: "t-wait", state: "awaiting_approval", input: { path: "a.py", old_string: "a", new_string: "b" } });
    const html = render([message([call(), waiting])], { pendingApprovals: [{ agentId: "root", messageId: "m1", part: waiting, riskClass: "workspace_edit", rememberAllowed: true }] } as never);
    expect(html).toContain('<span class="badge idle">waiting · see card</span>');
    expect(html).not.toContain(">Waiting for you<");
  });
});

describe("not-sent copy (UX verification 3)", () => {
  const rec = (status: number | null, code: string | null) => ({ errorStatus: status, errorCode: code, error: "raw" }) as never;
  it("says why by status and code", () => {
    expect(notSentText(rec(500, "internal"), { error: "raw" })).toBe("Not sent. The app hit an error. Your session is safe in Temporal; try again.");
    expect(notSentText(rec(502, null), { error: "raw" })).toMatch(/^Not sent. The app's API isn't answering/);
    expect(notSentText(rec(409, "session_closed"), { error: "raw" })).toBe("Not sent. This session is closed. Start a new one.");
    expect(notSentText(rec(0, null), { error: "Failed to fetch" })).toMatch(/^Not sent. Couldn't reach the app/);
  });
});
