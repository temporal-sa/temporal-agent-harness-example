// Regression checks for the UX verification review's shell and transcript items (Eng U):
// the decision slot never scrolls, auto-follow lands at the bottom, send failures say why,
// the phone's composer and meters fit, the rail shows status by shape.

import type { Page } from "@playwright/test";
import { expect, test } from "../support/fixtures";
import { ev, turn } from "../support/frames";
import { openScripted, sessionId } from "../support/open";
import { recovery, stacked, working } from "../support/scenarios";

async function slotScrolls(page: Page): Promise<{ scrolls: boolean; overflow: string }> {
  return page.evaluate(() => {
    const slot = document.querySelector('[data-slot="decisions"]') as HTMLElement;
    return { scrolls: slot.scrollHeight > slot.clientHeight + 1, overflow: getComputedStyle(slot).overflowY };
  });
}

async function onTop(page: Page, selector: string): Promise<boolean> {
  return page.evaluate((sel) => {
    const el = document.querySelector(sel);
    if (!el) return false;
    const b = el.getBoundingClientRect();
    const hit = document.elementFromPoint(b.left + b.width / 2, b.top + b.height / 2);
    return !!hit && (hit === el || el.contains(hit));
  }, selector);
}

for (const vp of [
  { w: 1920, h: 1080 },
  { w: 1366, h: 768 },
]) {
  test(`decision slot never scrolls and the card header stays reachable (${vp.w})`, async ({ page, mockApi }) => {
    await page.setViewportSize({ width: vp.w, height: vp.h });
    const api = await mockApi();
    await openScripted(page, api, stacked(), { session_id: sessionId(`slot-${vp.w}`), posture: "trust_safe" });
    const card = page.locator("section.dc-card");
    await expect(card).toBeVisible();
    // Open every group, then the decline form on the last group.
    const closed = card.locator("button[aria-expanded='false'][data-group-toggle]");
    for (let i = 0; i < 6 && (await closed.count()) > 0; i++) await closed.first().click();
    const decline = card.getByRole("button", { name: /^Decline \d/ }).last();
    if (await decline.count()) await decline.click();

    const s = await slotScrolls(page);
    expect(s.scrolls, `slot overflow-y ${s.overflow}`).toBe(false);
    const all = card.getByRole("button", { name: /^Approve all/ });
    await expect(all).toBeInViewport();
    expect(await onTop(page, "section.dc-card button.all"), "Approve all is not covered").toBe(true);

    // The card sits right on the composer: no empty band under it.
    const gap = await page.evaluate(() => {
      const c = document.querySelector("section.dc-card")!.getBoundingClientRect();
      const comp = document.querySelector('[data-slot="composer"]')!.getBoundingClientRect();
      return comp.top - c.bottom;
    });
    expect(gap).toBeLessThanOrEqual(24);
  });
}

for (const vp of [
  { w: 1366, h: 768 },
  { w: 320, h: 690 },
]) {
  test(`auto-follow lands at the bottom while text streams (${vp.w})`, async ({ page, mockApi }) => {
    await page.setViewportSize({ width: vp.w, height: vp.h });
    const api = await mockApi();
    const id = sessionId(`follow-${vp.w}`);
    const root = turn(1, "msg-1");
    await openScripted(page, api, working(root), { session_id: id });
    const long = Array.from({ length: 60 }, (_, i) => `Paragraph ${i}: a few more words so the reply wraps at phone width.`).join("\n\n");
    await api.push(id, [ev(root, "reply_delta", { text: long })]);
    await expect(page.getByText("Paragraph 59:")).toBeAttached();
    await expect
      .poll(
        () =>
          page.evaluate(() => {
            const t = document.querySelector('[data-slot="transcript"]') as HTMLElement;
            return Math.round(t.scrollHeight - t.scrollTop - t.clientHeight);
          }),
        { timeout: 8000 },
      )
      .toBeLessThanOrEqual(1);
    await expect(page.getByRole("button", { name: "Jump to latest" })).toHaveCount(0);
  });
}

for (const c of [
  { status: 500, body: { error: "internal", message: "boom" }, copy: "The app hit an error. Your session is safe in Temporal; try again." },
  { status: 502, body: { error: "bad_gateway", message: "Bad Gateway" }, copy: "The app's API isn't answering." },
  { status: 409, body: { error: "session_closed", message: "The session has ended." }, copy: "This session is closed. Start a new one." },
]) {
  test(`a failed send says why (${c.status} ${c.body.error})`, async ({ page, mockApi }) => {
    await page.setViewportSize({ width: 1366, height: 768 });
    const api = await mockApi();
    await openScripted(page, api, working(), { session_id: sessionId(`send-${c.status}`) });
    await page.route("**/api/agent/messages", (route) =>
      route.fulfill({ status: c.status, contentType: "application/json", body: JSON.stringify(c.body) }),
    );
    await page.getByRole("textbox", { name: "Message" }).fill("one more thing");
    await page.keyboard.press("Enter");
    const alert = page.getByRole("alert").filter({ hasText: "Not sent." });
    await expect(alert).toContainText(c.copy);
    await expect(alert).not.toContainText("Check your connection");
  });
}

test.describe("phone", () => {
  test.use({ viewport: { width: 320, height: 690 }, hasTouch: true, isMobile: true });

  test("Stop and the posture button aren't clipped; meters stay on one line after a recovery", async ({ page, mockApi }) => {
    const api = await mockApi();
    const id = sessionId("phone-fit");
    const root = turn(1, "msg-1");
    await openScripted(page, api, working(root), { session_id: id, posture: "auto" });
    const lostAt = Math.floor(Date.now() / 1000);
    await api.push(id, [recovery(root, lostAt, true)]);
    await expect(page.locator(".meters-compact")).toContainText("kept");

    const clipped = await page.evaluate(() =>
      // The model name may ellipsize as a last resort; Stop and the posture must not.
      [...document.querySelectorAll<HTMLElement>(".composer .bar button:not(.model-menu *)")]
        .filter((b) => b.getClientRects().length && b.scrollWidth > b.clientWidth + 1)
        .map((b) => b.getAttribute("aria-label") ?? b.textContent),
    );
    expect(clipped).toEqual([]);
    const stop = page.getByRole("button", { name: /^Stop/ });
    await expect(stop).toBeVisible();
    await expect(stop.locator("kbd")).toBeHidden();
    await expect(page.getByRole("button", { name: /^Approvals: / })).toContainText("Auto");

    const lines = await page.evaluate(() => {
      const m = document.querySelector(".meters-compact") as HTMLElement;
      const tops = new Set([...m.children].map((c) => Math.round(c.getBoundingClientRect().top)));
      return tops.size;
    });
    expect(lines).toBe(1);
  });
});

test("the rail shows the open session's status by shape as well as color", async ({ page, mockApi }) => {
  await page.setViewportSize({ width: 1366, height: 768 });
  const api = await mockApi();
  await openScripted(page, api, stacked(), { session_id: sessionId("rail-shape"), posture: "trust_safe" });
  const rail = page.getByRole("complementary", { name: "Sessions" });
  const current = rail.locator('button[aria-current="page"]');
  await expect(current).toHaveAccessibleName(/\(Waiting for you\)$/);
  const transform = await current.locator(".dot").evaluate((d) => getComputedStyle(d).transform);
  expect(transform, "waiting-for-you square is turned into a diamond").not.toBe("none");
});
