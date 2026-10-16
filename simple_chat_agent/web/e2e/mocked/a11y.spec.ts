// Check 3k: the keyboard reaches every control with a visible focus indicator; an axe scan
// (@axe-core/playwright 4.13.0, WCAG 2.2 A/AA rules) reports no violations; reduced motion is
// honored. Violations are attached to the test as axe-<screen>.json.

import AxeBuilder from "@axe-core/playwright";
import type { Page } from "@playwright/test";

import { expect, test } from "../support/fixtures";
import { openScripted, sessionId } from "../support/open";
import { DEMO_PROMPT, stacked, working } from "../support/scenarios";

const INTERACTIVE =
  'button:not([disabled]), a[href], input:not([type="hidden"]):not([disabled]), select:not([disabled]), textarea:not([disabled]), summary, [role="button"], [role="tab"], [tabindex]:not([tabindex="-1"])';

/** Tag every visible control with an index; return a label per index. */
async function tagControls(page: Page): Promise<string[]> {
  return page.evaluate((sel) => {
    const out: string[] = [];
    for (const el of Array.from(document.querySelectorAll<HTMLElement>(sel))) {
      const r = el.getBoundingClientRect();
      const cs = getComputedStyle(el);
      if (r.width === 0 || r.height === 0 || cs.visibility === "hidden" || el.closest("[hidden],[inert],[aria-hidden=true]")) continue;
      // A tab panel's inactive tabs are reachable with the arrow keys (roving tabindex).
      if (el.getAttribute("role") === "tab" && el.getAttribute("tabindex") === "-1") continue;
      el.dataset.e2eK = String(out.length);
      const name = (el.getAttribute("aria-label") || el.textContent || el.getAttribute("placeholder") || el.tagName).trim().replace(/\s+/g, " ").slice(0, 48);
      out.push(`${el.tagName.toLowerCase()} "${name}"`);
    }
    return out;
  }, INTERACTIVE);
}

interface Stop {
  k: string | null;
  label: string;
  visibleFocus: boolean;
}

/** Tab through the page from the top; stop when focus returns to where it started. */
async function tabThrough(page: Page, max: number): Promise<Stop[]> {
  await page.evaluate(() => {
    (document.activeElement as HTMLElement | null)?.blur();
    window.scrollTo(0, 0);
  });
  const stops: Stop[] = [];
  let first: string | null = null;
  for (let i = 0; i < max; i++) {
    await page.keyboard.press("Tab");
    const s = await page.evaluate(() => {
      const el = document.activeElement as HTMLElement | null;
      if (!el || el === document.body) return null;
      const cs = getComputedStyle(el);
      const outline = cs.outlineStyle !== "none" && parseFloat(cs.outlineWidth) > 0;
      const shadow = cs.boxShadow !== "none" && cs.boxShadow !== "";
      const key = el.dataset.e2eK ?? null;
      const label = (el.getAttribute("aria-label") || el.textContent || el.tagName).trim().replace(/\s+/g, " ").slice(0, 48);
      return { k: key, label: `${el.tagName.toLowerCase()} "${label}"`, visibleFocus: el.matches(":focus-visible") && (outline || shadow) };
    });
    if (!s) continue;
    const id = s.k ?? s.label;
    if (first === null) first = id;
    else if (id === first) break;
    stops.push(s);
  }
  return stops;
}

async function keyboardCheck(page: Page, screen: string) {
  const controls = await tagControls(page);
  const stops = await tabThrough(page, controls.length * 2 + 10);
  const reached = new Set(stops.map((s) => s.k).filter((k): k is string => k !== null));
  const unreached = controls.filter((_, i) => !reached.has(String(i)));
  const noFocusRing = [...new Set(stops.filter((s) => !s.visibleFocus).map((s) => s.label))];
  await test.info().attach(`keyboard-${screen}.json`, {
    body: JSON.stringify({ controls: controls.length, stops: stops.length, unreached, noFocusRing }, null, 2),
    contentType: "application/json",
  });
  return { unreached, noFocusRing };
}

/** Violations already reported as gaps, each with its own failing test below. Any other
 *  violation fails the screen's test. */
const KNOWN: Record<string, string> = {
  "definition-list": "meters strip <dl> holds a <span> spacer and the model <span> as direct children",
};

async function axe(page: Page, screen: string) {
  const result = await new AxeBuilder({ page }).withTags(["wcag2a", "wcag2aa", "wcag21a", "wcag21aa", "wcag22aa"]).analyze();
  const violations = result.violations.map((v) => ({
    id: v.id,
    impact: v.impact,
    help: v.help,
    nodes: v.nodes.length,
    targets: v.nodes.slice(0, 5).map((n) => n.target.join(" ")),
  }));
  await test.info().attach(`axe-${screen}.json`, { body: JSON.stringify(violations, null, 2), contentType: "application/json" });
  for (const v of violations) test.info().annotations.push({ type: `axe ${screen}`, description: `${v.id} (${v.impact}, ${v.nodes} nodes): ${v.help}` });
  return violations;
}

const unknown = (vs: { id: string }[]) => vs.filter((v) => !(v.id in KNOWN));

test.describe("keyboard", () => {
  test.use({ viewport: { width: 1920, height: 1080 } });

  test("3k keyboard reaches every control on the working screen, with visible focus", async ({ page, mockApi }) => {
    const api = await mockApi();
    await openScripted(page, api, working(), { session_id: sessionId("kbd-working") });
    await expect(page.getByText(DEMO_PROMPT)).toBeVisible();
    const { unreached, noFocusRing } = await keyboardCheck(page, "working");
    expect(unreached, "controls Tab never reaches").toEqual([]);
    expect(noFocusRing, "focused controls without a visible focus indicator").toEqual([]);
  });

  test("3k keyboard reaches every control on the stacked approval card, with visible focus", async ({ page, mockApi }) => {
    const api = await mockApi();
    await openScripted(page, api, stacked(), { session_id: sessionId("kbd-stack"), posture: "trust_safe" });
    await expect(page.getByRole("button", { name: "Approve all 5" })).toBeVisible();
    const { unreached, noFocusRing } = await keyboardCheck(page, "stacked");
    expect(unreached, "controls Tab never reaches").toEqual([]);
    expect(noFocusRing, "focused controls without a visible focus indicator").toEqual([]);
  });

  test("3k the composer's menus work from the keyboard", async ({ page, mockApi }) => {
    const api = await mockApi();
    await openScripted(page, api, working(), { session_id: sessionId("kbd-menu") });
    const model = page.getByRole("button", { name: /^Claude Sonnet 5\.5 · effort/ });
    await model.focus();
    await page.keyboard.press("Enter");
    const menu = page.getByRole("menu", { name: "Model" });
    await expect(menu).toBeVisible();
    await expect(menu.getByRole("menuitemradio").first()).toBeFocused();
    await page.keyboard.press("ArrowDown");
    await expect(menu.getByRole("menuitemradio").nth(1)).toBeFocused();
    await page.keyboard.press("Escape");
    await expect(menu).toHaveCount(0);
    await expect(model).toBeFocused();
    // Escape in the menu doesn't reach Stop.
    await expect(page.getByRole("button", { name: /^Stop/ })).toBeEnabled();
  });
});

test.describe("axe", () => {
  test("3k axe: working screen at 1920", async ({ page, mockApi }) => {
    await page.setViewportSize({ width: 1920, height: 1080 });
    const api = await mockApi();
    await openScripted(page, api, working(), { session_id: sessionId("axe-working") });
    await expect(page.getByText(DEMO_PROMPT)).toBeVisible();
    expect(unknown(await axe(page, "working-1920"))).toEqual([]);
  });

  test("3k axe: stacked approvals at 1366", async ({ page, mockApi }) => {
    await page.setViewportSize({ width: 1366, height: 768 });
    const api = await mockApi();
    await openScripted(page, api, stacked(), { session_id: sessionId("axe-stack"), posture: "trust_safe" });
    await expect(page.getByRole("button", { name: "Approve all 5" })).toBeVisible();
    expect(unknown(await axe(page, "stacked-1366"))).toEqual([]);
  });

  test("3k axe: phone at 320 with the workbench sheet", async ({ page, mockApi }) => {
    await page.setViewportSize({ width: 320, height: 690 });
    const api = await mockApi();
    await openScripted(page, api, working(), { session_id: sessionId("axe-phone") });
    await expect(page.getByText(DEMO_PROMPT)).toBeVisible();
    const main = await axe(page, "phone-320");
    await page.getByRole("button", { name: /^Changes \(1\)/ }).click();
    const sheet = await axe(page, "phone-320-sheet");
    expect(unknown([...main, ...sheet])).toEqual([]);
  });

  test("3k axe: sign-in page", async ({ page, mockApi }) => {
    await mockApi({ signedOut: true });
    await page.goto("/app/");
    await expect(page.getByRole("heading", { name: "Sign in to continue" })).toBeVisible();
    expect(unknown(await axe(page, "sign-in"))).toEqual([]);
  });
});

test("3k axe: the meters strip is a valid definition list", async ({ page, mockApi }) => {
  // GAP (Eng U, meters/MetersStrip.tsx): axe `definition-list` (serious) on `.meters`. The
  // <dl class="meters meters-full"> has a <span class="spacer"> and the model <span> as direct
  // children; a <dl> may only hold <dt>/<dd> groups (or <div> wrapping them). Move the spacer
  // and model label out of the <dl>, or wrap them in a <div>.
  await page.setViewportSize({ width: 1920, height: 1080 });
  const api = await mockApi();
  await openScripted(page, api, working(), { session_id: sessionId("axe-dl") });
  await expect(page.getByText(DEMO_PROMPT)).toBeVisible();
  const ids = (await axe(page, "meters-dl")).map((v) => v.id);
  expect(ids).not.toContain("definition-list");
});

test("3k reduced motion: nothing animates or transitions", async ({ page, mockApi }) => {
  await page.emulateMedia({ reducedMotion: "reduce" });
  const api = await mockApi();
  await openScripted(page, api, working(), { session_id: sessionId("motion") });
  await expect(page.getByText(DEMO_PROMPT)).toBeVisible();
  const moving = await page.evaluate(() => {
    const secs = (v: string) => Math.max(...v.split(",").map((x) => (x.trim().endsWith("ms") ? parseFloat(x) / 1000 : parseFloat(x)) || 0));
    const out: string[] = [];
    for (const el of Array.from(document.querySelectorAll<HTMLElement>("body *"))) {
      const cs = getComputedStyle(el);
      const anim = cs.animationName !== "none" && secs(cs.animationDuration) > 0.01 && cs.animationIterationCount !== "0";
      const trans = cs.transitionProperty !== "none" && secs(cs.transitionDuration) > 0.01;
      if (anim || trans) out.push(`${el.tagName.toLowerCase()}.${el.className}: ${anim ? `animation ${cs.animationName} ${cs.animationDuration}` : ""} ${trans ? `transition ${cs.transitionDuration}` : ""}`.trim());
    }
    return out.slice(0, 30);
  });
  expect(moving).toEqual([]);
});
