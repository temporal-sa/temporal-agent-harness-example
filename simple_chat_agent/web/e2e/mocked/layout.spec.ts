// Check 3a: layout at 1920 × 1080, 1366 × 768 and 320 px. Screenshots are saved under
// e2e/__screenshots__ for review against docs/phases/P3-wireframes.html (no pixel diffs yet).
// At 320: no horizontal scroll, and every tap target is at least 44 × 44 CSS px.

import { mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import type { Page } from "@playwright/test";

import { expect, test } from "../support/fixtures";
import { DEMO_PROMPT, stacked, working } from "../support/scenarios";
import { openScripted, sessionId } from "../support/open";

const SHOTS = join(dirname(fileURLToPath(import.meta.url)), "..", "__screenshots__");
mkdirSync(SHOTS, { recursive: true });

const SIZES = [
  { name: "1920x1080", width: 1920, height: 1080 },
  { name: "1366x768", width: 1366, height: 768 },
  { name: "320x690", width: 320, height: 690 },
] as const;

async function shot(page: Page, name: string) {
  await page.evaluate(() => document.fonts.ready);
  await page.screenshot({ path: join(SHOTS, `${name}.png`), fullPage: false });
}

/** Visible interactive elements smaller than 44 × 44 CSS px. */
async function smallTargets(page: Page): Promise<string[]> {
  return page.evaluate(() => {
    const sel = 'button, a[href], input:not([type="hidden"]), select, textarea, summary, [role="button"], [role^="menuitem"], [role="tab"], [tabindex]:not([tabindex="-1"])';
    const out: string[] = [];
    // With a modal open, only what is inside it can be tapped.
    const scope: ParentNode = document.querySelector('[aria-modal="true"]') ?? document;
    for (const el of Array.from(scope.querySelectorAll<HTMLElement>(sel))) {
      const r = el.getBoundingClientRect();
      const cs = getComputedStyle(el);
      if (r.width === 0 || r.height === 0 || cs.visibility === "hidden" || cs.display === "none") continue;
      // Off-screen (scrolled out) targets still count; only the visually hidden helper text is skipped.
      if (r.width <= 1 && r.height <= 1) continue;
      if (r.width < 44 || r.height < 44) {
        const name = (el.getAttribute("aria-label") || el.textContent || el.tagName).trim().replace(/\s+/g, " ").slice(0, 40);
        out.push(`${el.tagName.toLowerCase()} "${name}" ${Math.round(r.width)}×${Math.round(r.height)}`);
      }
    }
    return out;
  });
}

async function horizontalOverflow(page: Page): Promise<number> {
  return page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth);
}

for (const size of SIZES) {
  test.describe(`at ${size.name}`, () => {
    test.use({ viewport: { width: size.width, height: size.height } });

    test(`3a working session layout ${size.name}`, async ({ page, mockApi }) => {
      const api = await mockApi();
      const id = sessionId("layout");
      await openScripted(page, api, working(), { session_id: id });
      await expect(page.getByText(DEMO_PROMPT)).toBeVisible();
      await expect(page.getByLabel("Session meters").filter({ visible: true })).toBeVisible();

      // Structure from the wireframes: header with status and view links, the transcript, the
      // composer; the sidebar (1920), the rail (1366) or the drawer button (320); the workbench
      // panel on desktop and the "Changes (n)" sheet button on the phone.
      await expect(page.getByRole("banner")).toBeVisible();
      await expect(page.getByRole("textbox", { name: "Message" })).toBeVisible();
      if (size.width >= 1600) {
        await expect(page.getByRole("button", { name: "+ New demo session" })).toBeVisible();
      } else if (size.width > 600) {
        await expect(page.getByRole("button", { name: "New demo session" })).toBeVisible();
        await expect(page.getByRole("button", { name: "Show all sessions" })).toBeVisible();
      } else {
        await expect(page.getByRole("button", { name: "Sessions and views" })).toBeVisible();
        await expect(page.getByRole("button", { name: /^Changes \(1\)/ })).toBeVisible();
      }
      if (size.width > 600) await expect(page.getByRole("tab", { name: /Changes/ }).first()).toBeVisible();

      await shot(page, `working-${size.name}`);
      expect(await horizontalOverflow(page), "horizontal scroll").toBeLessThanOrEqual(0);
    });

    test(`3a stacked approvals layout ${size.name}`, async ({ page, mockApi }) => {
      const api = await mockApi();
      await openScripted(page, api, stacked(), { session_id: sessionId("layout-stack"), posture: "trust_safe" });
      await expect(page.getByRole("button", { name: "Approve all 5" })).toBeVisible();
      await shot(page, `stacked-${size.name}`);
      expect(await horizontalOverflow(page), "horizontal scroll").toBeLessThanOrEqual(0);
    });
  });
}

test.describe("phone", () => {
  test.use({ viewport: { width: 320, height: 690 }, hasTouch: true, isMobile: true });

  test("3a phone: no horizontal scroll and tap targets ≥ 44 px", async ({ page, mockApi }) => {
    // GAP (Eng U shell/transcript, Eng W workbench): tap targets under 44 × 44 px at 320 px.
    // Fails on expect(problems).toEqual([]) with, on the working session: the header's
    // "Sessions and views" button (37×41), "Thought for …" (≈98×18), the explorer cards'
    // header buttons (≈293×32), tool rows (≈294×31), path chips (≈174×19), and the sheet's and
    // drawer's "Close" buttons (50×26). No horizontal scroll was found.
    test.fail();
    const api = await mockApi();
    await openScripted(page, api, working(), { session_id: sessionId("phone") });
    await expect(page.getByText(DEMO_PROMPT)).toBeVisible();

    const problems: string[] = [];
    const check = async (where: string) => {
      const over = await horizontalOverflow(page);
      if (over > 0) problems.push(`${where}: scrolls sideways by ${over}px`);
      for (const t of await smallTargets(page)) problems.push(`${where}: ${t}`);
    };
    await check("transcript");

    await page.getByRole("button", { name: /^Changes \(1\)/ }).click();
    const sheet = page.getByRole("dialog", { name: "Workbench" });
    await expect(sheet).toBeVisible();
    await shot(page, "working-320x690-sheet");
    await check("workbench sheet");
    await sheet.getByRole("button", { name: "Close" }).click();

    await page.getByRole("button", { name: "Sessions and views" }).click();
    const drawer = page.getByRole("dialog", { name: "Sessions and views" });
    await expect(drawer).toBeVisible();
    await shot(page, "working-320x690-drawer");
    await check("drawer");

    await test.info().attach("phone-problems.txt", { body: problems.join("\n") || "none", contentType: "text/plain" });
    expect(problems, "phone layout problems").toEqual([]);
  });
});
