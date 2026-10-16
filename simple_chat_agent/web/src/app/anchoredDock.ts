// Keeps the decision card's top edge still while it grows (UX review M1). The dock sits above
// the composer, which is pinned to the bottom, so a taller card would push its own top (and
// the buttons under the pointer) upward. When a card appears, the slot reserves room for it to
// grow into (at least `minPx`, at most `maxPx`, which is Eng W's 50vh card cap), and the card
// grows down inside that room; past it, the card scrolls inside the slot. The reservation drops when the card goes away.

import { useLayoutEffect, useState, type RefObject } from "react";

/** The slot height to reserve for a card of `content` px, or null when there's no card. */
export function reservedHeight(content: number, current: number | null, minPx: number, maxPx: number): number | null {
  if (content <= 0) return null;
  // Fixed while the card stays: growing or shrinking the slot would move the card's top.
  // Content past the reservation scrolls inside the slot.
  return current ?? Math.min(maxPx, Math.max(minPx, content));
}

export function useAnchoredDock(slot: RefObject<HTMLElement | null>, enabled: boolean): number | null {
  const [reserved, setReserved] = useState<number | null>(null);
  useLayoutEffect(() => {
    const el = slot.current;
    const content = el?.firstElementChild as HTMLElement | null;
    if (!enabled || !el || !content || typeof ResizeObserver === "undefined") {
      setReserved(null);
      return;
    }
    const measure = () => {
      const rem = parseFloat(getComputedStyle(document.documentElement).fontSize) || 16;
      const max = window.innerHeight * 0.5;
      setReserved((cur) => reservedHeight(content.scrollHeight, cur, Math.min(max, 18 * rem), max));
    };
    measure();
    const ro = new ResizeObserver(measure);
    ro.observe(content);
    return () => ro.disconnect();
  }, [slot, enabled]);
  return reserved;
}
