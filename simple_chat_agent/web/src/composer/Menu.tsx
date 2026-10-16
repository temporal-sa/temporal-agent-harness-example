// A small popover menu for the composer's pickers: a button that opens a list above it.
// Escape and a click outside close it; arrow keys move between items; focus returns to the
// button. Escape inside the menu doesn't reach the composer's Stop shortcut.

import { useEffect, useId, useRef, useState, type KeyboardEvent, type ReactNode } from "react";

export function Menu({
  label,
  buttonLabel,
  buttonClass,
  buttonContent,
  children,
}: {
  label: string;
  /** The button's accessible name, e.g. "Model: GPT-5.5". */
  buttonLabel?: string;
  buttonClass: string;
  buttonContent: ReactNode;
  children(close: () => void): ReactNode;
}) {
  const [open, setOpen] = useState(false);
  const id = useId();
  const root = useRef<HTMLDivElement>(null);
  const button = useRef<HTMLButtonElement>(null);
  const list = useRef<HTMLDivElement>(null);

  const close = () => {
    setOpen(false);
    button.current?.focus();
  };

  useEffect(() => {
    if (!open) return;
    const first = list.current?.querySelector<HTMLElement>("[role^=menuitem]:not([aria-disabled=true])");
    first?.focus();
    const onDown = (e: MouseEvent) => {
      if (root.current && !root.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener("mousedown", onDown);
    return () => document.removeEventListener("mousedown", onDown);
  }, [open]);

  function onKeyDown(e: KeyboardEvent<HTMLDivElement>) {
    if (e.key === "Escape") {
      e.stopPropagation();
      e.nativeEvent.stopImmediatePropagation();
      close();
      return;
    }
    if (e.key !== "ArrowDown" && e.key !== "ArrowUp") return;
    e.preventDefault();
    const items = [...(list.current?.querySelectorAll<HTMLElement>("[role^=menuitem]") ?? [])];
    const at = items.indexOf(document.activeElement as HTMLElement);
    const next = items[(at + (e.key === "ArrowDown" ? 1 : -1) + items.length) % items.length];
    next?.focus();
  }

  return (
    <div className="menu-root" ref={root} onKeyDown={onKeyDown}>
      <button
        ref={button}
        type="button"
        className={buttonClass}
        aria-haspopup="menu"
        aria-label={buttonLabel}
        aria-expanded={open}
        aria-controls={open ? id : undefined}
        onClick={() => setOpen((v) => !v)}
      >
        {buttonContent}
      </button>
      {open && (
        <div className="menu" role="menu" id={id} aria-label={label} ref={list}>
          {children(close)}
        </div>
      )}
    </div>
  );
}
