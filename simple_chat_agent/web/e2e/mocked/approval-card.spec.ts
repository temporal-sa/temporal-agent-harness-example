// Eng W, P3 verification fixes: the stacked approval card is a column whose header (with
// Approve all) never moves while you work in it, only its list scrolls, and wheel over it
// never scrolls the transcript. Also the group title size and the Worker banner at 320.

import type { Locator, Page } from "@playwright/test";

import { expect, test } from "../support/fixtures";
import { approvalRequested, approvalResolved, ev, openTurn, toolRequested, turn, xWorker } from "../support/frames";
import { openScripted, sessionId } from "../support/open";
import { DEMO_PROMPT, PYTEST, stacked, working } from "../support/scenarios";

const stackCard = (page: Page) => page.getByRole("region", { name: /tool calls? (is|are) waiting$|^All answered$/ });
const approveAll = (page: Page) => stackCard(page).getByRole("button", { name: /^Approve all \d+$/ });
const groupToggle = (page: Page, name: RegExp) => stackCard(page).locator("[data-group-toggle]").filter({ hasText: name });

async function box(l: Locator) {
  const b = await l.boundingBox();
  expect(b).not.toBeNull();
  return b!;
}

for (const viewport of [
  { width: 1366, height: 768 },
  { width: 1920, height: 1080 },
]) {
  test.describe(`at ${viewport.width}`, () => {
    test.use({ viewport });

    test("opening groups, answering and declining leave Approve all where it was", async ({ page, mockApi }) => {
      const api = await mockApi();
      await openScripted(page, api, stacked(), { session_id: sessionId(`col-${viewport.width}`), posture: "trust_safe" });
      await expect(approveAll(page)).toHaveText("Approve all 5");
      await stackCard(page).hover();
      const before = await box(approveAll(page));

      await groupToggle(page, /Run/).click();
      expect(await box(approveAll(page)), "after opening the commands").toEqual(before);
      await groupToggle(page, /Edit 3 files/).click();
      expect(await box(approveAll(page)), "after opening both groups").toEqual(before);

      await stackCard(page).getByRole("button", { name: /^Approve .*refund_service\/money\.py/ }).click();
      await expect(approveAll(page)).toHaveText("Approve all 4");
      expect(await box(approveAll(page)), "after answering one").toEqual(before);

      await stackCard(page).getByRole("button", { name: "Decline 2 shell commands", exact: true }).click();
      await expect(stackCard(page).getByRole("textbox", { name: "Reason for the agent" })).toBeFocused();
      expect(await box(approveAll(page)), "with the decline form open").toEqual(before);

      // The header is inside the card, at its top.
      const card = await box(stackCard(page));
      const all = await box(approveAll(page));
      expect(all.y).toBeGreaterThanOrEqual(card.y);
      expect(all.y + all.height).toBeLessThanOrEqual(card.y + card.height);
    });

    test("wheel over the card scrolls its list, not the transcript", async ({ page, mockApi }) => {
      const api = await mockApi();
      await openScripted(page, api, stacked(), { session_id: sessionId(`wheel-${viewport.width}`), posture: "trust_safe" });
      await stackCard(page).hover();
      await groupToggle(page, /Run/).click();
      await groupToggle(page, /Edit 3 files/).click();
      const list = stackCard(page).locator(".dc-body");
      const transcript = page.locator("[data-slot=transcript]");
      const t0 = await transcript.evaluate((el) => el.scrollTop);
      const l0 = await list.evaluate((el) => el.scrollTop);
      const scrollable = await list.evaluate((el) => el.scrollHeight > el.clientHeight + 1);
      await list.hover();
      await page.mouse.wheel(0, 400);
      await page.waitForTimeout(200);
      expect(await transcript.evaluate((el) => el.scrollTop), "transcript did not scroll").toBe(t0);
      if (scrollable) expect(await list.evaluate((el) => el.scrollTop), "the list scrolled").toBeGreaterThan(l0);
    });

    test("the group title is at least 0.75rem", async ({ page, mockApi }) => {
      const api = await mockApi();
      await openScripted(page, api, stacked(), { session_id: sessionId(`title-${viewport.width}`), posture: "trust_safe" });
      const [title, root] = await groupToggle(page, /Edit 3 files/)
        .locator(".title")
        .evaluate((el) => [parseFloat(getComputedStyle(el).fontSize), parseFloat(getComputedStyle(document.documentElement).fontSize)]);
      expect(title!).toBeGreaterThanOrEqual(0.75 * root! - 0.01);
    });
  });
}

test.describe("at 320", () => {
  test.use({ viewport: { width: 320, height: 690 }, hasTouch: true, isMobile: true });

  test("the card's header never clips when the list is open", async ({ page, mockApi }) => {
    const api = await mockApi();
    await openScripted(page, api, stacked(), { session_id: sessionId("col-320"), posture: "trust_safe" });
    // Keyboard: at 320 the shell's sticky sheet strip can sit over the lower rows.
    await stackCard(page).getByRole("button", { name: /Review the 5 calls/ }).press("Enter");
    await groupToggle(page, /Run/).press("Enter");
    await groupToggle(page, /Edit 3 files/).press("Enter");
    const card = await box(stackCard(page));
    const all = await box(approveAll(page));
    const heading = await box(stackCard(page).locator("[data-heading]"));
    expect(all.y, "Approve all is inside the card").toBeGreaterThanOrEqual(card.y - 0.5);
    expect(heading.y, "the heading is inside the card").toBeGreaterThanOrEqual(card.y - 0.5);
    expect(all.height).toBeGreaterThanOrEqual(44);
    expect(card.height).toBeLessThanOrEqual(690 * 0.45 + 1);
  });

  test("the Worker banner is one line with Details and the Temporal UI link", async ({ page, mockApi }) => {
    const api = await mockApi();
    const id = sessionId("banner-320");
    const root = turn(1, "msg-1");
    await openScripted(page, api, working(root), { session_id: id });
    await expect(page.getByText(DEMO_PROMPT)).toBeVisible();
    await api.push(id, [xWorker("suspect", "no pollers", { turn: root, since: Date.now() / 1000, pollers: 0 })]);
    const banner = page.getByRole("status", { name: "Worker isn't answering" });
    await expect(banner).toBeVisible();
    expect((await box(banner)).height, "one line").toBeLessThanOrEqual(56);
    const details = banner.getByRole("button", { name: /Details/ });
    await expect(details).toHaveAttribute("aria-expanded", "false");
    await details.click();
    await expect(banner).toContainText("Waiting:");
  });
});

// -- Auto mode: the judge's checking state ----------------------------------------------------

test.describe("Auto mode: a call the judge is checking", () => {
  test.use({ viewport: { width: 1366, height: 768 } });

  const input = { command: PYTEST };
  const started = (root: ReturnType<typeof turn>) => [
    toolRequested(root, "tool-judged", "run_command", input),
    approvalRequested(root, "tool-judged", "run_command", input),
    ev(root, "auto_approval_evaluation_started", { tool_id: "tool-judged", tool_name: "run_command", evaluation_id: "eval-1", evaluator: "llm_judge" }),
  ];
  const ended = (root: ReturnType<typeof turn>, verdict: "approve" | "escalate") =>
    ev(root, "auto_approval_evaluation_ended", {
      tool_id: "tool-judged",
      tool_name: "run_command",
      evaluation_id: "eval-1",
      evaluator: "llm_judge",
      verdict,
      reason: verdict === "approve" ? "judge llm: runs the project's tests" : "judge llm: unclear; escalated to a human.",
      details: { confidence: verdict === "approve" ? 0.94 : 0.6, criteria_set: "shell" },
    });

  /** Records whether an approval card ever appeared, however briefly. */
  async function watchForCard(page: Page) {
    await page.evaluate(() => {
      const w = window as unknown as { __cardSeen: boolean };
      w.__cardSeen = !!document.querySelector("section.dc-card");
      new MutationObserver(() => {
        if (document.querySelector("section.dc-card")) w.__cardSeen = true;
      }).observe(document.body, { childList: true, subtree: true });
    });
    return () => page.evaluate(() => (window as unknown as { __cardSeen: boolean }).__cardSeen);
  }

  test("started → approved: a quiet checking line, and the approval card never shows", async ({ page, mockApi }) => {
    const api = await mockApi();
    const id = sessionId("judge-approves");
    const root = turn(1, "msg-1");
    await openScripted(page, api, openTurn(root, DEMO_PROMPT), { session_id: id, posture: "auto" });
    await expect(page.getByText(DEMO_PROMPT)).toBeVisible();
    const cardSeen = await watchForCard(page);

    await api.push(id, started(root));
    await expect(page.getByText(`The judge is checking run_command ${PYTEST}…`)).toBeVisible();
    await expect(page.getByText(/need approval|Needs approval/)).toHaveCount(0);

    await api.push(id, [ended(root, "approve"), approvalResolved(root, "tool-judged", "run_command", true, false, "judge llm: runs the project's tests")]);
    await expect(page.getByText(`The judge is checking run_command ${PYTEST}…`)).toHaveCount(0);
    expect(await cardSeen(), "the approval card never appeared").toBe(false);
  });

  test("started → escalated: the call joins the approval card", async ({ page, mockApi }) => {
    const api = await mockApi();
    const id = sessionId("judge-escalates");
    const root = turn(1, "msg-1");
    await openScripted(page, api, openTurn(root, DEMO_PROMPT), { session_id: id, posture: "auto" });
    await expect(page.getByText(DEMO_PROMPT)).toBeVisible();
    const cardSeen = await watchForCard(page);

    await api.push(id, started(root));
    await expect(page.getByText(`The judge is checking run_command ${PYTEST}…`)).toBeVisible();
    expect(await cardSeen(), "no card while the judge checks").toBe(false);

    await api.push(id, [ended(root, "escalate")]);
    const card = page.getByRole("region", { name: "Needs approval" });
    await expect(card).toBeVisible();
    await expect(card.getByRole("button", { name: "Approve", exact: true })).toBeVisible();
    await expect(page.getByText(`The judge is checking run_command ${PYTEST}…`)).toHaveCount(0);
  });
});
