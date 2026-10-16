// @vitest-environment happy-dom
// Workbench `focus`: a transcript path chip opens Changes and scrolls to that file, every time.
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { CodingSession, WorkbenchFocus } from "../session";
import { focusedFile } from "./changes";
import { Workbench } from "./Workbench";

(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;

const FILES = ["refund_service/refunds.py", "tests/test_refund.py", "refund_service/money.py"];

function session(): CodingSession {
  return {
    sessionId: "s1",
    rootAgentId: null,
    subagents: {},
    agent: () => undefined,
    plan: { goal: "", items: [{ text: "Fix it", status: "in_progress" }] },
    changes: { files: FILES.map((path) => ({ path, status: "modified", added: 1, removed: 1, tool_id: "" })), commits: [] },
    workspace: null,
    telemetry: { worker: { answering: true, reason: null, since: null, pollers: 1, agentId: null, turnId: null }, recoveries: [] },
  } as unknown as CodingSession;
}

describe("focusedFile", () => {
  it("matches exactly after normalising, else a unique path suffix", () => {
    const files = FILES.map((path) => ({ path }));
    expect(focusedFile(files, "./refund_service/refunds.py")).toBe("refund_service/refunds.py");
    expect(focusedFile(files, "test_refund.py")).toBe("tests/test_refund.py");
    expect(focusedFile(files, "nope.py")).toBeNull();
    expect(focusedFile([{ path: "a/x.py" }, { path: "b/x.py" }], "x.py")).toBeNull();
  });
});

describe("Workbench focus", () => {
  let host: HTMLDivElement;
  let root: Root;
  let scrolled: string[];
  const s = session();

  beforeEach(() => {
    vi.useFakeTimers();
    scrolled = [];
    Element.prototype.scrollIntoView = function (this: Element) {
      scrolled.push(this.getAttribute("aria-label") ?? "");
    };
    host = document.createElement("div");
    document.body.appendChild(host);
    root = createRoot(host);
  });
  afterEach(() => {
    act(() => root.unmount());
    host.remove();
    vi.useRealTimers();
  });

  const render = (focus: WorkbenchFocus | null) => act(() => root.render(<Workbench session={s} variant="panel" focus={focus} />));
  const selected = () => host.querySelector('[role="tab"][aria-selected="true"]')?.textContent ?? "";
  const article = (path: string) => host.querySelector(`article[aria-label^="${path},"]`)!;

  it("switches to Changes, scrolls the file into view and highlights it briefly", () => {
    render(null);
    expect(selected()).toMatch(/^Plan/);

    render({ tab: "changes", path: "tests/test_refund.py", at: 1 });
    expect(selected()).toMatch(/^Changes/);
    expect(scrolled).toEqual(["tests/test_refund.py, modified"]);
    expect(article("tests/test_refund.py").className).toContain("flash");
    expect(document.activeElement).toBe(article("tests/test_refund.py"));

    act(() => vi.advanceTimersByTime(2000));
    expect(article("tests/test_refund.py").className).not.toContain("flash");
  });

  it("scrolls again when the same file is focused again while already on Changes", () => {
    render({ tab: "changes", path: "refund_service/money.py", at: 1 });
    render({ tab: "changes", path: "refund_service/money.py", at: 2 });
    expect(scrolled).toEqual(["refund_service/money.py, modified", "refund_service/money.py, modified"]);
    expect(article("refund_service/money.py").className).toContain("flash");
  });

  it("comes back to Changes after the user moved to another tab", () => {
    render({ tab: "changes", path: "refund_service/refunds.py", at: 1 });
    const plan = [...host.querySelectorAll<HTMLButtonElement>('[role="tab"]')].find((b) => b.textContent?.startsWith("Plan"))!;
    act(() => plan.click());
    expect(selected()).toMatch(/^Plan/);
    render({ tab: "changes", path: "refund_service/refunds.py", at: 2 });
    expect(selected()).toMatch(/^Changes/);
    expect(scrolled).toHaveLength(2);
  });
});
