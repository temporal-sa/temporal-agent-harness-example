// @vitest-environment happy-dom
import { describe, expect, it } from "vitest";
import { composerMode, escapeStops } from "./choices";

function page(): Record<string, Element> {
  document.body.innerHTML = `
    <div data-slot="pinned"><button id="banner">Open in Temporal UI</button></div>
    <div data-slot="transcript"><button id="row">tool row</button></div>
    <div data-slot="decisions"><button id="approve">Approve all 2</button></div>
    <div class="composer"><textarea id="input"></textarea>
      <div class="menu-root"><div role="menu"><button id="item" role="menuitemradio">Auto mode</button></div></div>
    </div>
    <div role="dialog" aria-modal="true"><button id="close">Close</button></div>`;
  const get = (id: string) => document.getElementById(id)!;
  return { banner: get("banner"), row: get("row"), approve: get("approve"), input: get("input"), item: get("item"), close: get("close") };
}

describe("Escape stops the turn only from the composer or the transcript (UX review B1)", () => {
  it("stops from the composer and the transcript while the turn runs", () => {
    const el = page();
    expect(escapeStops("running", el.input!, false)).toBe(true);
    expect(escapeStops("running", el.row!, false)).toBe(true);
  });

  it("never from the decision card, a menu, a dialog or elsewhere, or once Escape closed something", () => {
    const el = page();
    expect(escapeStops("running", el.approve!, false)).toBe(false);
    expect(escapeStops("running", el.item!, false)).toBe(false);
    expect(escapeStops("running", el.close!, false)).toBe(false);
    expect(escapeStops("running", el.banner!, false)).toBe(false);
    expect(escapeStops("running", el.input!, true)).toBe(false);
  });

  it("not while the turn only waits on an approval, nor when idle", () => {
    const el = page();
    expect(escapeStops("waiting", el.input!, false)).toBe(false);
    expect(escapeStops("idle", el.input!, false)).toBe(false);
  });
});

describe("composer mode (UX review M4)", () => {
  it("reads as idle while the turn only waits on you", () => {
    expect(composerMode({ agentStatus: "busy", status: "waiting_for_you", pendingApprovals: [{}] })).toBe("waiting");
    expect(composerMode({ agentStatus: "busy", status: "running", pendingApprovals: [] })).toBe("running");
    expect(composerMode({ agentStatus: "busy", status: "worker_unresponsive", pendingApprovals: [] })).toBe("running");
    expect(composerMode({ agentStatus: "idle", status: "idle", pendingApprovals: [] })).toBe("idle");
  });
});
