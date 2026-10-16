// Regression tests for the P3 review fixes (UX review B1–B3, M1, M3, M5, M8, and the
// duplicate terminal output after a reattach).

import type { Locator, Page } from "@playwright/test";

import { expect, test } from "../support/fixtures";
import { modelCall, toolProgress, turn, approvalRequested, toolRequested, xWorker, type Frame } from "../support/frames";
import type { MockApi } from "../support/mock-api";
import { openScripted, sessionId } from "../support/open";
import { DEMO_PROMPT, PYTEST, stacked, working } from "../support/scenarios";

const interrupts = (api: MockApi) => api.bodies("POST", "/api/agent/messages").filter((b) => (b.message as { type?: string } | undefined)?.type === "interrupt");

/** Give a wrongly bound Escape time to send its interrupt. */
async function noInterrupt(page: Page, api: MockApi) {
  await page.waitForTimeout(500);
  expect(interrupts(api), "interrupts sent").toHaveLength(0);
}

async function inViewport(page: Page, l: Locator, what: string) {
  const box = await l.boundingBox();
  const vp = page.viewportSize()!;
  expect(box, `${what} has a box`).not.toBeNull();
  expect(box!.x, `${what} left edge`).toBeGreaterThanOrEqual(0);
  expect(box!.y, `${what} top edge`).toBeGreaterThanOrEqual(0);
  expect(box!.x + box!.width, `${what} right edge`).toBeLessThanOrEqual(vp.width + 0.5);
  expect(box!.y + box!.height, `${what} bottom edge`).toBeLessThanOrEqual(vp.height + 0.5);
}

const transcriptAtBottom = (page: Page) =>
  page.locator("[data-slot=transcript]").evaluate((el) => el.scrollHeight - el.scrollTop - el.clientHeight <= 4);

const stackCard = (page: Page) => page.getByRole("region", { name: /tool calls? (is|are) waiting$|^All answered$/ });
const approveAll = (page: Page) => stackCard(page).getByRole("button", { name: /^Approve all \d+$/ });

/** A long reply, so the transcript has to scroll. */
function longReply(t = turn(1, "msg-1")): Frame[] {
  const text = Array.from({ length: 40 }, (_, i) => `Paragraph ${i + 1} of the investigation notes, long enough to wrap on a phone.`).join("\n\n");
  return modelCall(t, 9, { input: 100, output: 100 }, { text: `${text}\n\n` });
}

// -- B1: Escape stops the turn only from the composer ----------------------------------------

test.describe("B1 Escape", () => {
  test("Esc in the composer during a running turn sends an interrupt", async ({ page, mockApi }) => {
    const api = await mockApi();
    await openScripted(page, api, working(), { session_id: sessionId("esc-composer") });
    await expect(page.getByRole("button", { name: /^Stop/ })).toBeVisible();
    await page.getByRole("textbox", { name: "Message" }).focus();
    await page.keyboard.press("Escape");
    await expect.poll(() => interrupts(api).length).toBe(1);
  });

  test("Esc in a composer menu closes it and sends no interrupt", async ({ page, mockApi }) => {
    const api = await mockApi();
    await openScripted(page, api, working(), { session_id: sessionId("esc-menu") });
    await expect(page.getByRole("button", { name: /^Stop/ })).toBeVisible();
    await page.getByRole("button", { name: /^Model:/ }).click();
    await expect(page.getByRole("menu", { name: "Model" })).toBeVisible();
    await page.keyboard.press("Escape");
    await expect(page.getByRole("menu", { name: "Model" })).toHaveCount(0);
    await noInterrupt(page, api);
  });

  test("Esc in the drawer closes it and sends no interrupt", async ({ page, mockApi }) => {
    await page.setViewportSize({ width: 1366, height: 768 });
    const api = await mockApi();
    await openScripted(page, api, working(), { session_id: sessionId("esc-drawer") });
    await expect(page.getByRole("button", { name: /^Stop/ })).toBeVisible();
    await page.getByRole("button", { name: "Show all sessions" }).click();
    await expect(page.getByRole("dialog", { name: "Sessions and views" })).toBeVisible();
    await page.keyboard.press("Escape");
    await expect(page.getByRole("dialog", { name: "Sessions and views" })).toHaveCount(0);
    await noInterrupt(page, api);
  });

  test("Esc on Approve all sends no interrupt", async ({ page, mockApi }) => {
    const api = await mockApi();
    await openScripted(page, api, stacked(), { session_id: sessionId("esc-all"), posture: "trust_safe" });
    await expect(page.getByRole("button", { name: /^Stop/ })).toBeVisible();
    await approveAll(page).focus();
    await page.keyboard.press("Escape");
    await noInterrupt(page, api);
    await expect(approveAll(page)).toHaveText("Approve all 5");
  });
});

// -- B2, B3: what must stay on screen ---------------------------------------------------------

test("B2 at 320 with five stacked calls: Approve all, the sheet strip and the composer are in the viewport; the transcript follows", async ({ page, mockApi }) => {
  await page.setViewportSize({ width: 320, height: 690 });
  const api = await mockApi();
  const root = turn(1, "msg-1");
  const frames = stacked(root);
  // The long reply goes before the calls, so the newest output is at the bottom.
  frames.splice(frames.findIndex((f) => f.event === "tool_requested"), 0, ...longReply(root));
  await openScripted(page, api, frames, { session_id: sessionId("b2"), posture: "trust_safe" });
  await expect(approveAll(page)).toHaveText("Approve all 5");
  await inViewport(page, approveAll(page), "Approve all");
  await inViewport(page, page.getByRole("button", { name: /^Changes \(\d+\)/ }), "Changes button");
  await inViewport(page, page.getByRole("button", { name: /^Terminal/ }), "Terminal button");
  await inViewport(page, page.getByRole("textbox", { name: "Message" }), "composer");
  await expect.poll(() => transcriptAtBottom(page), { message: "transcript scrolled to the newest output" }).toBe(true);
  // The newest output is the last call's tool row (pytest, waiting for you).
  await expect(page.locator("[data-slot=transcript]").getByRole("button", { name: /^Shell run_command/ }).last()).toBeInViewport();
});

test("B3 the Worker banner stays in the viewport while the transcript auto-follows", async ({ page, mockApi }) => {
  await page.setViewportSize({ width: 1366, height: 768 });
  const api = await mockApi();
  const id = sessionId("b3");
  const root = turn(1, "msg-1");
  await openScripted(page, api, working(root), { session_id: id });
  await expect(page.getByText(DEMO_PROMPT)).toBeVisible();
  await api.push(id, [xWorker("suspect", "no pollers", { turn: root, since: Date.now() / 1000, pollers: 0 })]);
  const banner = page.getByRole("status", { name: "Worker isn't answering" });
  await expect(banner).toBeVisible();
  // More output arrives; the transcript follows it.
  for (let i = 0; i < 6; i++) {
    await api.push(id, [toolProgress(root, "tool-pytest-1", "run_command", Array.from({ length: 20 }, (_, k) => `progress ${i}-${k}\n`).join(""))]);
  }
  await expect.poll(() => transcriptAtBottom(page)).toBe(true);
  await expect(banner).toBeInViewport({ ratio: 1 });
  await inViewport(page, banner, "Worker banner");
});

// -- M1: Approve all never moves --------------------------------------------------------------

test.describe("M1 Approve all stays put", () => {
  test.use({ viewport: { width: 1920, height: 1080 } });

  test("answering one item leaves Approve all where it was", async ({ page, mockApi }) => {
    const api = await mockApi();
    await openScripted(page, api, stacked(), { session_id: sessionId("m1-item"), posture: "trust_safe" });
    const edits = stackCard(page).getByRole("listitem").filter({ hasText: "Edit 3 files" }).first();
    await edits.getByRole("button", { name: /Edit 3 files/ }).first().click();
    const before = await approveAll(page).boundingBox();
    await stackCard(page).getByRole("button", { name: /^Approve .*refund_service\/money\.py/ }).click();
    await expect(approveAll(page)).toHaveText("Approve all 4");
    expect(await approveAll(page).boundingBox()).toEqual(before);
  });

  test("a held arrival while keyboard focus is in the card leaves Approve all where it was", async ({ page, mockApi }) => {
    const api = await mockApi();
    const id = sessionId("m1-focus");
    const root = turn(1, "msg-1");
    await openScripted(page, api, stacked(root), { session_id: id, posture: "trust_safe" });
    await page.mouse.move(0, 0);
    await approveAll(page).focus();
    const before = await approveAll(page).boundingBox();
    const input = { command: PYTEST };
    await api.push(id, [toolRequested(root, "tool-cmd-c", "run_command", input), approvalRequested(root, "tool-cmd-c", "run_command", input)]);
    await expect(stackCard(page).getByText(/1 more call is waiting/).first()).toBeVisible();
    await expect(approveAll(page)).toHaveText("Approve all 5");
    expect(await approveAll(page).boundingBox()).toEqual(before);
  });

  test("opening a group leaves Approve all where it was", async ({ page, mockApi }) => {
    const api = await mockApi();
    await openScripted(page, api, stacked(), { session_id: sessionId("m1-open"), posture: "trust_safe" });
    const before = await approveAll(page).boundingBox();
    await stackCard(page).getByRole("button", { name: /Edit 3 files/ }).first().click();
    await expect(stackCard(page).getByRole("button", { name: /^Approve .*refund_service\/money\.py/ })).toBeVisible();
    expect(await approveAll(page).boundingBox()).toEqual(before);
  });
});

// -- M3: focus after an answer ------------------------------------------------------------------

test("M3 focus after an answer is never lost to the page body", async ({ page, mockApi }) => {
  await page.setViewportSize({ width: 1920, height: 1080 });
  const api = await mockApi();
  await openScripted(page, api, stacked(), { session_id: sessionId("m3"), posture: "trust_safe" });
  const active = () => page.evaluate(() => (document.activeElement === document.body || !document.activeElement ? "body" : `${document.activeElement.tagName} ${document.activeElement.getAttribute("aria-label") ?? document.activeElement.textContent?.trim().slice(0, 40)}`));

  // A group answered from the keyboard.
  const group = stackCard(page).getByRole("button", { name: /^Approve \d+ / }).first();
  await group.focus();
  await page.keyboard.press("Enter");
  await expect.poll(active).not.toBe("body");
  // Approve all from the keyboard (it then disables).
  await approveAll(page).focus();
  await page.keyboard.press("Enter");
  await expect(stackCard(page)).toHaveAccessibleName("All answered");
  await page.waitForTimeout(200);
  expect(await active(), "focus after Approve all").not.toBe("body");
});

// -- M5: the "Waiting for you" pill ------------------------------------------------------------

test("M5 the Waiting for you pill has contrast ≥ 4.5", async ({ page, mockApi }) => {
  const api = await mockApi();
  await openScripted(page, api, stacked(), { session_id: sessionId("m5"), posture: "trust_safe" });
  const pill = page.getByRole("banner").getByRole("status").filter({ hasText: "Waiting for you" });
  await expect(pill).toBeVisible();
  // The pill transitions its colour for 0.2 s when the status changes: measure once settled.
  await page.waitForTimeout(400);
  const ratio = await pill.evaluate((el) => {
    const parse = (c: string): [number, number, number] | null => {
      const m = /rgba?\(([^)]+)\)/.exec(c);
      if (!m) return null;
      const [r, g, b, a] = m[1]!.split(/[ ,/]+/).filter(Boolean).map(Number);
      return a === 0 ? null : [r!, g!, b!];
    };
    const lum = ([r, g, b]: [number, number, number]) => {
      const f = (v: number) => {
        const s = v / 255;
        return s <= 0.03928 ? s / 12.92 : ((s + 0.055) / 1.055) ** 2.4;
      };
      return 0.2126 * f(r) + 0.7152 * f(g) + 0.0722 * f(b);
    };
    const contrast = (a: [number, number, number], b: [number, number, number]) => {
      const [hi, lo] = [lum(a), lum(b)].sort((x, y) => y - x) as [number, number];
      return (hi + 0.05) / (lo + 0.05);
    };
    const cs = getComputedStyle(el);
    const fg = parse(cs.color)!;
    // Every colour the text can sit on: gradient stops, else the nearest solid background.
    const backs: [number, number, number][] = [];
    for (const m of cs.backgroundImage.matchAll(/rgba?\([^)]+\)/g)) {
      const c = parse(m[0]);
      if (c) backs.push(c);
    }
    if (!backs.length) {
      for (let e: Element | null = el; e; e = e.parentElement) {
        const c = parse(getComputedStyle(e).backgroundColor);
        if (c) {
          backs.push(c);
          break;
        }
      }
    }
    if (!backs.length) backs.push([0, 0, 0]);
    return Math.min(...backs.map((b) => contrast(fg, b)));
  });
  test.info().annotations.push({ type: "contrast", description: ratio.toFixed(2) });
  expect(ratio).toBeGreaterThanOrEqual(4.5);
});

// -- M8: the drawer and the sheet are modal ------------------------------------------------------

test.describe("M8 dialogs", () => {
  test.use({ viewport: { width: 320, height: 690 } });

  for (const which of [
    { name: "Workbench", opener: /^Changes \(\d+\)/ },
    { name: "Sessions and views", opener: /^Sessions and views$/ },
  ]) {
    test(`the ${which.name} dialog traps Tab, closes on Esc and returns focus`, async ({ page, mockApi }) => {
      const api = await mockApi();
      await openScripted(page, api, working(), { session_id: sessionId(`m8-${which.name.length}`) });
      await expect(page.getByText(DEMO_PROMPT)).toBeVisible();
      const opener = page.getByRole("button", { name: which.opener });
      await opener.focus();
      await page.keyboard.press("Enter");
      const dialog = page.getByRole("dialog", { name: which.name });
      await expect(dialog).toBeVisible();
      for (let i = 0; i < 25; i++) {
        await page.keyboard.press(i % 7 === 6 ? "Shift+Tab" : "Tab");
        const inside = await dialog.evaluate((d) => d.contains(document.activeElement));
        expect(inside, `focus inside the dialog after Tab ${i + 1}`).toBe(true);
      }
      await page.keyboard.press("Escape");
      await expect(dialog).toHaveCount(0);
      await expect(opener).toBeFocused();
      await noInterrupt(page, api);
    });
  }
});

// -- Duplicates: a reattach replays output ------------------------------------------------------

test("a reattach replay (per-chunk seq 3–9 after 1–6) shows each terminal line once", async ({ page, mockApi }) => {
  await page.setViewportSize({ width: 1920, height: 1080 });
  const api = await mockApi();
  const id = sessionId("dups");
  const root = turn(1, "msg-1");
  const out = (seq: number, from: number, to: number) =>
    "\u001e" +
    JSON.stringify({
      v: 1,
      type: "output",
      seq,
      chunks: Array.from({ length: to - from + 1 }, (_, i) => ({ stream: "stdout", seq: from + i, text: `line ${from + i}\n` })),
    });
  const frames = working(root).filter((f) => f.event !== "tool_progress_delta");
  frames.push(toolProgress(root, "tool-pytest-1", "run_command", out(1, 1, 6)));
  await openScripted(page, api, frames, { session_id: id });
  await page.getByRole("tab", { name: /^Terminal/ }).click();
  const panel = page.getByRole("tabpanel");
  await expect(panel).toContainText("line 6");
  // The worker is lost; the new worker reattaches and replays from 3, then continues to 9.
  await api.push(id, [
    toolProgress(root, "tool-pytest-1", "run_command", "\u001e" + JSON.stringify({ v: 1, type: "output", seq: 2, chunks: [{ stream: "note", seq: null, text: "reattached to the running command\n" }] })),
    toolProgress(root, "tool-pytest-1", "run_command", out(3, 3, 9)),
  ]);
  await expect(panel).toContainText("line 9");
  const text = (await panel.innerText()).replace(/\r/g, "");
  for (let k = 1; k <= 9; k++) {
    const n = text.split("\n").filter((l) => l.trim() === `line ${k}`).length;
    expect(n, `"line ${k}" in the Terminal`).toBe(1);
  }
});
