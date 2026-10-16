// Check 3e, UI half (owner decision B): five calls waiting at once (three edits, two identical
// `pytest -q`) share one card with two groups and "× 2" on the commands; answering one item
// updates the counts; Approve all sends only the still-waiting tool ids, each exactly once.

import { expect, test } from "../support/fixtures";
import { approvalRequested, toolRequested, turn } from "../support/frames";
import { openScripted, sessionId } from "../support/open";
import { PYTEST, STACK_COMMANDS, STACK_EDITS, STACK_IDS, stacked } from "../support/scenarios";
import type { Page } from "@playwright/test";

/** The stacked card; its accessible name is its heading ("5 tool calls are waiting"). */
const stackCard = (page: Page) => page.getByRole("region", { name: /tool calls? (is|are) waiting$|^All answered$/ });

test("stacked approvals: one card, two groups, × 2, per-item answers, Approve all sends only what waits", async ({ page, mockApi }) => {
  const api = await mockApi();
  await openScripted(page, api, stacked(), { session_id: sessionId("stack"), posture: "trust_safe" });

  // One card for all five.
  const card = stackCard(page);
  await expect(card).toHaveAccessibleName("5 tool calls are waiting");
  await expect(page.getByRole("region", { name: /tool calls? (is|are) waiting|Needs approval/ })).toHaveCount(1);
  await expect(card.getByRole("button", { name: "Approve all 5" })).toBeVisible();

  // Two groups: the edits and the command, the identical commands folded "× 2".
  const groups = card.getByRole("listitem").filter({ has: page.getByRole("button", { name: /^Approve \d+$/ }) });
  await expect(groups).toHaveCount(2);
  const edits = groups.filter({ hasText: "Edit 3 files" });
  const commands = groups.filter({ hasText: PYTEST });
  await expect(edits).toHaveCount(1);
  await expect(commands).toHaveCount(1);
  await expect(commands).toContainText("× 2");
  await expect(edits.getByRole("button", { name: "Approve 3" })).toBeVisible();
  await expect(commands.getByRole("button", { name: "Approve 2" })).toBeVisible();

  // Open the edits and approve one file on its own.
  await edits.getByRole("button", { name: /Edit 3 files/ }).click();
  const one = STACK_EDITS[1]!;
  await card.getByRole("button", { name: new RegExp(`^Approve .*${one.path.replace(/[.]/g, "\\.")}`) }).click();

  // Counts update at once.
  await expect(card).toHaveAccessibleName("4 tool calls are waiting");
  await expect(card.getByRole("button", { name: "Approve all 4" })).toBeVisible();
  await expect(card.getByRole("button", { name: "Approve 2" }).first()).toBeVisible();
  await expect(card.getByText("Approved by you")).toBeVisible();

  // Approve all: only the four still waiting.
  await card.getByRole("button", { name: "Approve all 4" }).click();
  await expect(card.getByText("All answered").first()).toBeVisible();
  await expect.poll(() => api.bodies("POST", "/api/agent/approve").length).toBe(5);

  const bodies = api.bodies("POST", "/api/agent/approve");
  const ids = bodies.map((b) => String(b.tool_id));
  expect(ids[0]).toBe(one.id);
  expect(new Set(ids).size, "each call answered exactly once").toBe(5);
  expect([...ids].sort()).toEqual([...STACK_IDS].sort());
  expect(ids.slice(1).sort(), "Approve all sent only the still-waiting ids").toEqual(STACK_IDS.filter((x) => x !== one.id).sort());
  for (const b of bodies) {
    expect(b.approved).toBe(true);
    expect(b.remember ?? false).toBe(false);
    expect(typeof b.session_id).toBe("string");
  }

  // Nothing is sent twice afterwards.
  await page.waitForTimeout(300);
  expect(api.bodies("POST", "/api/agent/approve")).toHaveLength(5);
});

test("stacked approvals: the identical commands answered together, Decline n sends declines for that fold only", async ({ page, mockApi }) => {
  const api = await mockApi();
  await openScripted(page, api, stacked(), { session_id: sessionId("stack-decline"), posture: "trust_safe" });
  const card = stackCard(page);
  await expect(card).toHaveAccessibleName("5 tool calls are waiting");
  const commands = card.getByRole("listitem").filter({ hasText: PYTEST }).filter({ has: page.getByRole("button", { name: "Decline 2" }) });
  await commands.getByRole("button", { name: "Decline 2" }).click();
  await card.getByLabel("Reason for the agent").fill("Run them once.");
  await card.getByRole("button", { name: "Decline 2" }).last().click();

  await expect(card).toHaveAccessibleName("3 tool calls are waiting");
  await expect.poll(() => api.bodies("POST", "/api/agent/approve").length).toBe(2);
  const bodies = api.bodies("POST", "/api/agent/approve");
  expect(bodies.map((b) => b.tool_id).sort()).toEqual([...STACK_COMMANDS].sort());
  for (const b of bodies) {
    expect(b.approved).toBe(false);
    expect(b.reason).toBe("Run them once.");
  }
});

test("stacked approvals: a call that arrives under the pointer waits, then joins its group", async ({ page, mockApi }) => {
  const api = await mockApi();
  const id = sessionId("stack-arrive");
  const root = turn(1, "msg-1");
  await openScripted(page, api, stacked(root), { session_id: id, posture: "trust_safe" });
  const card = stackCard(page);
  await expect(card).toHaveAccessibleName("5 tool calls are waiting");
  const all = card.getByRole("button", { name: "Approve all 5" });
  await all.hover();

  const input = { command: PYTEST };
  await api.push(id, [toolRequested(root, "tool-cmd-c", "run_command", input), approvalRequested(root, "tool-cmd-c", "run_command", input)]);
  await expect(card.getByRole("status").filter({ hasText: "1 more call is waiting." })).toBeVisible();
  // Approve all still counts only what is shown.
  await expect(all).toHaveText("Approve all 5");

  await page.mouse.move(0, 0);
  await expect(card).toHaveAccessibleName("6 tool calls are waiting");
  await expect(card.getByRole("listitem").filter({ hasText: PYTEST }).first()).toContainText("× 3");
});

test("stacked approvals: a held arrival doesn't move the buttons under the pointer", async ({ page, mockApi }) => {
  // GAP (Eng W, decisions/DecisionDock.tsx): owner decision B says new arrivals join "without
  // moving the buttons". The card sits on the composer and grows upward, so the "1 more call
  // is waiting" line pushes "Approve all" up by one line (about 24 px) while the pointer is
  // on it. Fails on: expect(await all.boundingBox()).toEqual(before) (y differs).
  test.fail();
  const api = await mockApi();
  const id = sessionId("stack-still");
  const root = turn(1, "msg-1");
  await openScripted(page, api, stacked(root), { session_id: id, posture: "trust_safe" });
  const card = stackCard(page);
  const all = card.getByRole("button", { name: "Approve all 5" });
  await all.hover();
  const before = await all.boundingBox();
  const input = { command: PYTEST };
  await api.push(id, [toolRequested(root, "tool-cmd-c", "run_command", input), approvalRequested(root, "tool-cmd-c", "run_command", input)]);
  await expect(card.getByRole("status").filter({ hasText: "1 more call is waiting." })).toBeVisible();
  expect(await all.boundingBox()).toEqual(before);
});
