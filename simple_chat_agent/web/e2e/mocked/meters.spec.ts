// Check 3g, UI half (PRD U5): the meters strip equals the sum of the session's x_usage frames
// (every agent and provider; the latest frame per id wins), and a count no provider reported
// reads "not reported", never 0.

import type { Page } from "@playwright/test";

import { expect, test } from "../support/fixtures";
import { closeTurn, openTurn, turn, xUsage, type Frame, type Usage } from "../support/frames";
import { openScripted, sessionId } from "../support/open";
import { EXPLORER_USAGE, WORKING_USAGE, working } from "../support/scenarios";

/** The strip's own number format (meters/format.ts), restated: 950, 41.2k, 1.25M. */
function fmt(n: number): string {
  const trim = (s: string) => (s.includes(".") ? s.replace(/\.?0+$/, "") : s);
  if (Math.abs(n) < 1000) return String(Math.round(n));
  if (Math.abs(n) < 1_000_000) {
    const k = n / 1000;
    return `${k >= 100 ? Math.round(k) : trim(k.toFixed(1))}k`;
  }
  const m = n / 1_000_000;
  return `${m >= 100 ? Math.round(m) : trim(m.toFixed(2))}M`;
}

function meter(page: Page, term: string) {
  return page
    .getByLabel("Session meters")
    .filter({ visible: true })
    .getByRole("term")
    .filter({ hasText: term })
    .locator("xpath=following-sibling::dd[1]");
}

/** A finished turn whose only content is the given usage frames. */
function usageOnly(calls: { usage: Usage; provider: string | null; model: string }[]): Frame[] {
  const t = turn(1, "msg-1");
  return [
    ...openTurn(t, "Count tokens"),
    ...calls.map((c, i) => xUsage(t, i, c.usage, { provider: c.provider, model: c.model, confirmed: true })),
    ...closeTurn(t, { text: "Done.", interrupted: false, checkpoint: null, error: null }),
  ];
}

test("meters equal the sum of x_usage across the root agent and both explorers", async ({ page, mockApi }) => {
  const api = await mockApi();
  await openScripted(page, api, working(), { session_id: sessionId("meters-sum") });
  const calls = [...WORKING_USAGE, EXPLORER_USAGE, EXPLORER_USAGE];
  const input = calls.reduce((a, c) => a + c.input, 0);
  const output = calls.reduce((a, c) => a + c.output, 0);
  const read = calls.reduce((a, c) => a + c.cacheRead, 0);
  await expect(meter(page, "Session tokens")).toHaveText(`${fmt(input)} in / ${fmt(output)} out`);
  await expect(meter(page, "Input read from provider cache")).toHaveText(`${Math.round((read / input) * 100)}%`);
  await expect(meter(page, "Recoveries")).toHaveText("0");
});

test("a confirmed x_usage replaces its live frame instead of adding to it", async ({ page, mockApi }) => {
  const api = await mockApi();
  const id = sessionId("meters-confirm");
  const t = turn(1, "msg-1");
  await openScripted(page, api, [...openTurn(t, "Hi"), xUsage(t, 0, { input: 1000, output: 100, cacheRead: 0 }, { provider: "anthropic" })], { session_id: id });
  await expect(meter(page, "Session tokens")).toHaveText("1k in / 100 out");
  // Confirmed from history, with the cache-write count the live frame lacked.
  await api.push(id, [xUsage(t, 0, { input: 1200, output: 150, cacheRead: 600, cacheWrite: 600 }, { provider: "anthropic", confirmed: true })]);
  await expect(meter(page, "Session tokens")).toHaveText("1.2k in / 150 out");
  await expect(meter(page, "Input read from provider cache")).toHaveText("50%");
  // A late live frame never downgrades it.
  await api.push(id, [xUsage(t, 0, { input: 1000, output: 100, cacheRead: 0 }, { provider: "anthropic" })]);
  await page.waitForTimeout(200);
  await expect(meter(page, "Session tokens")).toHaveText("1.2k in / 150 out");
});

test("mixed providers: the cache share counts only calls that report cache reads", async ({ page, mockApi }) => {
  const api = await mockApi();
  await openScripted(
    page,
    api,
    usageOnly([
      { usage: { input: 4000, output: 200, cacheRead: 3000, cacheWrite: 1000 }, provider: "anthropic", model: "claude-sonnet-5-5" },
      { usage: { input: 6000, output: 300, cacheRead: null, cacheWrite: null }, provider: "openai", model: "gpt-5.5" },
    ]),
    { session_id: sessionId("meters-mixed") },
  );
  await expect(meter(page, "Session tokens")).toHaveText("10k in / 500 out");
  // 3000 / 4000: the OpenAI call reports no cache reads, so it doesn't dilute the share to 30%.
  await expect(meter(page, "Input read from provider cache")).toHaveText("75%");
  const cacheMeter = page.getByLabel("Session meters").filter({ visible: true }).getByRole("term").filter({ hasText: "Input read from provider cache" }).locator("..");
  await expect(cacheMeter).toHaveAttribute("title", /Anthropic: 75%.*openai: not reported|Anthropic: 75%.*OpenAI: not reported/);
});

test("no provider reports cache reads: the cache meter reads \"not reported\", never 0", async ({ page, mockApi }) => {
  const api = await mockApi();
  await openScripted(
    page,
    api,
    usageOnly([
      { usage: { input: 2500, output: 120, cacheRead: null }, provider: "openai", model: "gpt-5.5" },
      { usage: { input: 1500, output: 80, cacheRead: null }, provider: "google", model: "gemini-3.5-flash" },
    ]),
    { session_id: sessionId("meters-nocache") },
  );
  await expect(meter(page, "Session tokens")).toHaveText("4k in / 200 out");
  await expect(meter(page, "Input read from provider cache")).toHaveText("not reported");
  await expect(page.getByLabel("Session meters").filter({ visible: true })).not.toContainText("0%");
});

test("no token counts reported at all: tokens read \"not reported\"", async ({ page, mockApi }) => {
  const api = await mockApi();
  await openScripted(page, api, usageOnly([{ usage: { input: null, output: null, cacheRead: null }, provider: null, model: "local-model" }]), {
    session_id: sessionId("meters-none"),
  });
  await expect(meter(page, "Session tokens")).toHaveText("not reported");
  await expect(meter(page, "Input read from provider cache")).toHaveText("not reported");
});
