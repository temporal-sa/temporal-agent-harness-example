import { describe, expect, it } from "vitest";
import { alwaysApprovedText, pickerState, postureLabel, postureOptions, postureShort } from "./choices";
import { advance, landed, startRecord } from "../session/sendStates";
import type { AgentConfig } from "../session";

const sonnet = { provider: "anthropic" as const, model: "claude-sonnet-5-5" };
const gpt = { provider: "openai" as const, model: "gpt-5.5", effort: "medium" as const };

describe("model picker", () => {
  it("shows the pick at once, then applies next turn, then the facade's model", () => {
    const rec = startRecord("r", "set_model", 0);
    expect(pickerState(sonnet, false, { choice: gpt, record: undefined })).toMatchObject({ shown: gpt, note: "Sending…" });
    expect(pickerState(sonnet, false, { choice: gpt, record: landed(rec, 1, "m") })).toMatchObject({ shown: gpt, note: "applies next turn" });
    expect(pickerState(gpt, true, null)).toMatchObject({ shown: gpt, note: "applies next turn" });
    expect(pickerState(gpt, false, null)).toMatchObject({ shown: gpt, note: null });
    expect(pickerState(sonnet, false, { choice: gpt, record: advance(rec, "failed", 2, "no") })).toMatchObject({ shown: sonnet, failed: true });
  });
});

describe("posture menu", () => {
  const config = {
    postures: [{ id: "trust_safe", label: "Trust safe tools", description: "d" }],
    default_posture: "trust_safe",
    posture_options: [
      { id: "ask_everything", label: "Ask for everything", description: "a", available: true, reason: null, default: false },
      { id: "never_ask", label: "Never ask", description: "n", available: false, reason: "Local development only.", default: false },
    ],
  } as unknown as AgentConfig;

  it("uses config's full menu with reasons, and falls back to the allowed postures", () => {
    expect(postureOptions(config).map((o) => [o.id, o.available])).toEqual([
      ["ask_everything", true],
      ["never_ask", false],
    ]);
    expect(postureOptions({ ...config, posture_options: undefined }).map((o) => [o.id, o.default])).toEqual([["trust_safe", true]]);
    expect(postureLabel(config, "never_ask")).toBe("Never ask");
  });

  it("names edit tools as file edits", () => {
    const classes = { tool_risk_classes: { edit_file: "workspace_edit", write_file: "workspace_edit" } };
    expect(alwaysApprovedText(["edit_file", "write_file"], classes)).toBe("file edits");
    expect(alwaysApprovedText(["edit_file"], null)).toBe("edit_file");
    expect(alwaysApprovedText([], classes)).toBe("");
  });
});


describe("phone labels (UX verification 4)", () => {
  it("shortens the posture for the one-row composer", () => {
    expect(postureShort("auto")).toBe("Auto");
    expect(postureShort("trust_safe")).toBe("Safe");
    expect(postureShort(null)).toBe("Approvals");
  });
});
