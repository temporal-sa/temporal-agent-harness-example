// @vitest-environment happy-dom
// The Worker banner's phone variant: one line with Details and the Temporal UI link.
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import type { CodingSession } from "../session";
import { WorkerBanner } from "./WorkerBanner";

(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;

function down(): CodingSession {
  return {
    sessionId: "s1",
    rootAgentId: "root",
    subagents: {},
    agent: () => undefined,
    status: "worker_unresponsive",
    anyBusy: true,
    toolProgress: {},
    runIdFor: () => "run-9",
    telemetry: { worker: { answering: false, reason: "no pollers", since: null, pollers: 0, agentId: null, turnId: null } },
  } as unknown as CodingSession;
}

describe("Worker banner on a phone", () => {
  let host: HTMLDivElement;
  let root: Root;
  const real = window.matchMedia;
  beforeEach(() => {
    window.matchMedia = ((q: string) => ({ matches: q.includes("max-width: 600px"), media: q, addEventListener() {}, removeEventListener() {} })) as unknown as typeof window.matchMedia;
    host = document.createElement("div");
    document.body.appendChild(host);
    root = createRoot(host);
  });
  afterEach(() => {
    act(() => root.unmount());
    host.remove();
    window.matchMedia = real;
  });

  it("is one line with Details and the Temporal UI link; the rest opens behind Details", () => {
    act(() => root.render(<WorkerBanner session={down()} temporalUiTemplate="http://ui/namespaces/d/workflows/{workflow_id}" />));
    const banner = host.querySelector("section.rc-banner")!;
    expect(banner.classList.contains("compact")).toBe(true);
    expect(banner.querySelector(".d")).toBeNull();
    const link = banner.querySelector("a")!;
    expect(link.getAttribute("href")).toBe("http://ui/namespaces/d/workflows/s1/run-9/pending-activities");
    expect(link.getAttribute("aria-label")).toBe("Open the pending Activity in Temporal UI ↗");
    const toggle = banner.querySelector<HTMLButtonElement>("button.rc-details")!;
    expect(toggle.getAttribute("aria-expanded")).toBe("false");
    act(() => toggle.click());
    expect(toggle.getAttribute("aria-expanded")).toBe("true");
    expect(banner.querySelector(".d")!.textContent).toContain("Waiting:");
  });
});
