// @vitest-environment happy-dom
import { describe, expect, it } from "vitest";
import { act, useState } from "react";
import { createRoot } from "react-dom/client";
import { Dialog, trapTarget } from "./Dialog";

(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;

function Harness() {
  const [open, setOpen] = useState(false);
  return (
    <>
      <button id="opener" onClick={() => setOpen(true)}>
        Menu
      </button>
      {open && (
        <Dialog className="drawer" label="Sessions and views" onClose={() => setOpen(false)}>
          <button id="first">Close</button>
          <button id="last">New demo session</button>
        </Dialog>
      )}
    </>
  );
}

const key = (el: Element, k: string, shiftKey = false) =>
  act(() => {
    el.dispatchEvent(new KeyboardEvent("keydown", { key: k, shiftKey, bubbles: true, cancelable: true }));
  });

describe("Dialog (UX review M8)", () => {
  it("moves focus in, keeps Tab inside, closes on Escape and gives focus back to its opener", async () => {
    document.body.innerHTML = '<div id="root"></div>';
    const root = createRoot(document.getElementById("root")!);
    await act(async () => root.render(<Harness />));
    const opener = document.getElementById("opener")!;
    opener.focus();
    await act(async () => opener.click());
    expect(document.activeElement?.id).toBe("first");

    const last = document.getElementById("last")!;
    last.focus();
    await key(last, "Tab");
    expect(document.activeElement?.id).toBe("first");
    await key(document.getElementById("first")!, "Tab", true);
    expect(document.activeElement?.id).toBe("last");

    let reachedDocument = false;
    const onDoc = (e: Event) => {
      if ((e as KeyboardEvent).key === "Escape") reachedDocument = true;
    };
    document.addEventListener("keydown", onDoc);
    await key(document.activeElement!, "Escape");
    document.removeEventListener("keydown", onDoc);
    expect(document.querySelector('[role="dialog"]')).toBeNull();
    expect(reachedDocument).toBe(false);
    expect(document.activeElement?.id).toBe("opener");
    await act(async () => root.unmount());
  });

  it("wraps Tab at either end", () => {
    const a = document.createElement("button");
    const b = document.createElement("button");
    expect(trapTarget([a, b], b, false)).toBe(a);
    expect(trapTarget([a, b], a, true)).toBe(b);
    expect(trapTarget([a, b], a, false)).toBeNull();
    expect(trapTarget([], a, false)).toBeNull();
  });
});
