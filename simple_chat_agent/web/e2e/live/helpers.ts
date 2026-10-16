// Steps of the demo script, driven through the product view against the live stack.

import { expect, type Page } from "@playwright/test";

export const HAIKU = /^Claude Haiku 5\.5$/;

export async function signIn(page: Page, base: string, user: string, password: string): Promise<void> {
  await page.goto(`${base}/app/`);
  await page.getByLabel("Username").fill(user);
  await page.getByLabel("Password").fill(password);
  await page.getByRole("button", { name: "Sign in" }).click();
  await expect(page.getByRole("button", { name: /New demo session/ }).first()).toBeVisible({ timeout: 30_000 });
}

/** The session id from ?s= once a session is open. */
export async function currentSession(page: Page): Promise<string> {
  await expect(page).toHaveURL(/[?&]s=/, { timeout: 60_000 });
  return new URL(page.url()).searchParams.get("s")!;
}

export async function newDemoSession(page: Page): Promise<string> {
  await page.getByRole("button", { name: /New demo session/ }).first().click();
  const id = await currentSession(page);
  await expect(page.getByRole("textbox", { name: "Message" })).toBeVisible({ timeout: 60_000 });
  return id;
}

export async function newEmptySession(page: Page): Promise<string> {
  await page.getByRole("button", { name: "New empty session" }).first().click();
  const id = await currentSession(page);
  await expect(page.getByRole("textbox", { name: "Message" })).toBeVisible({ timeout: 60_000 });
  return id;
}

/** The composer's model button (accessible name "Model: Claude Haiku 5.5 · effort low …"). */
const modelButton = (page: Page) => page.locator("[data-slot=composer]").getByRole("button", { name: /^Model:/ });

/** Claude Haiku 5.5, effort low: the cheapest model in the catalog. */
export async function pickHaikuLow(page: Page): Promise<void> {
  await modelButton(page).click();
  const menu = page.getByRole("menu", { name: "Model" });
  const haiku = menu.getByRole("menuitemradio", { name: /Claude Haiku 5\.5/ });
  if ((await haiku.getAttribute("aria-checked")) !== "true") await haiku.click();
  else await page.keyboard.press("Escape");
  await expect(modelButton(page)).toContainText("Claude Haiku 5.5");
  await modelButton(page).click();
  const low = page.getByRole("menu", { name: "Model" }).getByRole("menuitemradio", { name: "low" });
  if ((await low.getAttribute("aria-checked")) !== "true") await low.click();
  else await page.keyboard.press("Escape");
  await expect(modelButton(page)).toContainText("effort low");
}

export async function pickPosture(page: Page, label: string): Promise<void> {
  await page.getByRole("button", { name: /^Approvals:/ }).click();
  const item = page.getByRole("menu", { name: "Approvals" }).getByRole("menuitemradio", { name: new RegExp(`^${label}`) });
  if ((await item.getAttribute("aria-checked")) !== "true") await item.click();
  else await page.keyboard.press("Escape");
  await expect(page.getByRole("button", { name: new RegExp(`^Approvals: ${label}`) })).toBeVisible();
}

export async function send(page: Page, text: string): Promise<void> {
  const box = page.getByRole("textbox", { name: "Message" });
  await box.fill(text);
  await box.press("Enter");
  await expect(box).toHaveValue("");
}

/** Idle again: Send is back and no turn is running. */
export async function waitIdle(page: Page, timeoutMs = 240_000): Promise<void> {
  await expect(page.getByRole("button", { name: /^Stop/ })).toHaveCount(0, { timeout: timeoutMs });
  await expect(page.getByRole("button", { name: "Send" })).toBeVisible({ timeout: timeoutMs });
}

/** Every x_usage frame of a session, latest per id, read from a fresh attach (which ends once
 *  the agent is idle and caught up). Uses the page's cookies. */
export async function usageFrames(page: Page, base: string, sessionId: string): Promise<Record<string, unknown>[]> {
  const r = await page.request.get(`${base}/api/agent/attach?session_id=${encodeURIComponent(sessionId)}&from_offset=0`, { timeout: 60_000 });
  expect(r.ok()).toBe(true);
  const text = await r.text();
  const latest = new Map<string, Record<string, unknown>>();
  for (const block of text.split("\n\n")) {
    const event = /^event: (.+)$/m.exec(block)?.[1];
    const data = /^data: (.+)$/m.exec(block)?.[1];
    if (event !== "x_usage" || !data) continue;
    const d = JSON.parse(data) as Record<string, unknown>;
    const prev = latest.get(String(d.id));
    if (prev?.confirmed && !d.confirmed) continue;
    latest.set(String(d.id), d);
  }
  return [...latest.values()];
}

/** The strip's number format (meters/format.ts). */
export function fmt(n: number): string {
  const trim = (s: string) => (s.includes(".") ? s.replace(/\.?0+$/, "") : s);
  if (Math.abs(n) < 1000) return String(Math.round(n));
  if (Math.abs(n) < 1_000_000) {
    const k = n / 1000;
    return `${k >= 100 ? Math.round(k) : trim(k.toFixed(1))}k`;
  }
  const m = n / 1_000_000;
  return `${m >= 100 ? Math.round(m) : trim(m.toFixed(2))}M`;
}

export function meter(page: Page, term: string) {
  return page.getByLabel("Session meters").filter({ visible: true }).getByRole("term").filter({ hasText: term }).locator("xpath=following-sibling::dd[1]");
}

/** Expected strip text from usage frames: "<in> in / <out> out". */
export function expectedTokens(frames: Record<string, unknown>[]): string {
  const sum = (k: string) => {
    let t: number | null = null;
    for (const f of frames) if (typeof f[k] === "number") t = (t ?? 0) + (f[k] as number);
    return t;
  };
  const i = sum("input_tokens");
  const o = sum("output_tokens");
  if (frames.length === 0) return "0 in / 0 out";
  if (i === null && o === null) return "not reported";
  return `${i === null ? "not reported" : fmt(i)} in / ${o === null ? "not reported" : fmt(o)} out`;
}

/** Approve any approval card that shows up while `until` is false (Auto mode may escalate). */
export async function approveWhileWaiting(page: Page, until: () => Promise<boolean>, timeoutMs: number): Promise<number> {
  const end = Date.now() + timeoutMs;
  let approved = 0;
  while (Date.now() < end) {
    if (await until()) return approved;
    const card = page.getByRole("region", { name: "Needs approval" });
    if (await card.isVisible().catch(() => false)) {
      await card.getByRole("button", { name: "Approve", exact: true }).click().catch(() => undefined);
      approved += 1;
    }
    await page.waitForTimeout(500);
  }
  throw new Error("timed out");
}
