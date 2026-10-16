import { describe, expect, it } from "vitest";
import { reservedHeight } from "./anchoredDock";

describe("anchored decision card (UX review M1)", () => {
  it("reserves room when a card appears, between the floor and the cap", () => {
    expect(reservedHeight(120, null, 288, 384)).toBe(288);
    expect(reservedHeight(500, null, 288, 384)).toBe(384);
    expect(reservedHeight(0, 300, 288, 384)).toBeNull();
  });

  it("stays fixed while the card stays, so opening a group never moves the card's top", () => {
    expect(reservedHeight(150, 288, 288, 384)).toBe(288);
    expect(reservedHeight(340, 288, 288, 384)).toBe(288);
    expect(reservedHeight(900, 384, 288, 384)).toBe(384);
  });
});
