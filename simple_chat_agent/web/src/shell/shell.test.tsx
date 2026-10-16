/// <reference types="node" />
import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { renderToStaticMarkup } from "react-dom/server";
import { railLabel, rowCaption } from "./Sidebar";
import { Unreachable } from "./Unreachable";
import { footerText } from "../app/App";
import { NO_SESSION_REASON, viewDefs } from "./ViewLinks";
import { modelLine } from "../transcript/Transcript";
import type { SessionSummary } from "../session";

const s = (over: Partial<SessionSummary> = {}) =>
  ({ session_id: "s1", workflow_id: "s1", title: "Refund bug", label: "Refund bug", created_at: Date.now() / 1000, closed: false, model: {}, posture: null, environment: null, demo: null, ...over }) as SessionSummary;

describe("sidebar status is spelled out, not color alone (UX review m10)", () => {
  it("names the status in the row and in the rail button", () => {
    expect(rowCaption(s(), "running", "Claude Sonnet 5.5")).toBe("Running · Claude Sonnet 5.5 · now");
    expect(rowCaption(s({ closed: true }), null, "GPT-5.5")).toBe("Ended · GPT-5.5 · now");
    expect(rowCaption(s(), null, "GPT-5.5")).toBe("GPT-5.5 · now");
    expect(railLabel(s(), "waiting_for_you")).toBe("Refund bug (Waiting for you)");
    expect(railLabel(s(), null)).toBe("Refund bug");
  });
});

describe("copy", () => {
  it("API-down page says what is down and what keeps going (UX review m2)", () => {
    const html = renderToStaticMarkup(<Unreachable onRetry={() => undefined} />);
    expect(html).toContain("The app&#x27;s API isn&#x27;t answering. Workflows keep running in Temporal. Retrying every 5 seconds.");
    expect(html).not.toMatch(/offline/i);
  });

  it("sidebar footer gives the retention from config (UX review m14)", () => {
    expect(footerText("demo", "catalog", 15)).toBe("Signed in as demo through the catalog · sessions are kept 15 days");
    expect(footerText("demo", "local", null)).toBe("Signed in as demo");
  });

  it("model line says this turn once that turn has started (UX review m13)", () => {
    expect(modelLine("GPT-5.5 (OpenAI)", false, false)).toBe("Model changed to GPT-5.5 (OpenAI) · applies from the next turn");
    expect(modelLine("GPT-5.5 (OpenAI)", false, true)).toBe("Model changed to GPT-5.5 (OpenAI) · applies from this turn");
  });

  it("Waiting for you pill uses dark text on the pink gradient (UX review M5)", () => {
    const css = readFileSync(new URL("../theme/app.css", import.meta.url), "utf8");
    expect(css).toMatch(/\.pill\.you \{[^}]*color: var\(--space-black\)/);
  });
});

describe("Temporal UI link", () => {
  const enabled = { links: { temporal_ui: { enabled: true, url_template: "http://ui/namespaces/default/workflows/{workflow_id}", reason: null } } } as never;
  const off = { links: { temporal_ui: { enabled: false, url_template: null, reason: "The Temporal UI isn't linked in this environment." } } } as never;
  const temporal = (config: never, id: string | null) => viewDefs(config, null, id).find((v) => v.name === "Temporal UI")!;

  it("asks for a session when it is enabled but none is open", () => {
    expect(temporal(enabled, null)).toMatchObject({ href: null, reason: NO_SESSION_REASON });
    expect(temporal(enabled, "coding-agent-1").href).toBe("http://ui/namespaces/default/workflows/coding-agent-1");
  });

  it("keeps config's reason when it is off", () => {
    expect(temporal(off, null).reason).toBe("The Temporal UI isn't linked in this environment.");
    expect(temporal(off, "s1").href).toBeNull();
  });
});
