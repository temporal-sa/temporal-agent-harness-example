// A modal panel (the phone menu drawer and the workbench sheet): focus moves into it, Tab
// stays inside, Escape closes it, and focus returns to whatever opened it. The shell makes
// everything behind it inert while it is open.

import { useEffect, useRef, useState, type KeyboardEvent, type ReactNode } from "react";

const FOCUSABLE =
  'a[href], button:not([disabled]), input:not([disabled]):not([type="hidden"]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])';

export function focusables(root: HTMLElement): HTMLElement[] {
  return Array.from(root.querySelectorAll<HTMLElement>(FOCUSABLE)).filter((el) => !el.closest("[inert]") && el.getAttribute("aria-hidden") !== "true");
}

/** Where Tab goes next inside `root`, wrapping at either end; null to let the browser move. */
export function trapTarget(items: readonly HTMLElement[], active: Element | null, back: boolean): HTMLElement | null {
  if (items.length === 0) return null;
  const first = items[0]!;
  const last = items[items.length - 1]!;
  const at = active ? items.indexOf(active as HTMLElement) : -1;
  if (at === -1) return back ? last : first;
  if (back && at === 0) return last;
  if (!back && at === items.length - 1) return first;
  return null;
}

export function Dialog({
  label,
  className,
  onClose,
  children,
}: {
  label: string;
  className: string;
  onClose(): void;
  children: ReactNode;
}) {
  const ref = useRef<HTMLDivElement>(null);
  const close = useRef(onClose);
  close.current = onClose;

  // Read while rendering, before the shell makes the page behind inert (which drops focus).
  const [opener] = useState(() => (document.activeElement instanceof HTMLElement ? document.activeElement : null));
  useEffect(() => {
    const root = ref.current;
    if (root && !root.contains(document.activeElement)) (focusables(root)[0] ?? root).focus();
    return () => {
      if (opener && opener.isConnected) opener.focus();
    };
  }, [opener]);

  function onKeyDown(e: KeyboardEvent<HTMLDivElement>) {
    if (e.key === "Escape") {
      // Closing is all Escape does here; it never reaches the composer's Stop.
      e.preventDefault();
      e.stopPropagation();
      close.current();
      return;
    }
    if (e.key !== "Tab" || !ref.current) return;
    const next = trapTarget(focusables(ref.current), document.activeElement, e.shiftKey);
    if (next) {
      e.preventDefault();
      next.focus();
    }
  }

  return (
    <>
      <div className="drawer-scrim" onClick={() => close.current()} aria-hidden="true" />
      <div ref={ref} className={className} role="dialog" aria-modal="true" aria-label={label} tabIndex={-1} onKeyDown={onKeyDown}>
        {children}
      </div>
    </>
  );
}
