// Check 3b (PRD U4, U7): Send, Approve, Approve all, a model pick and a posture pick each show
// their accepted/acknowledged state within 100 ms of the click, measured with performance.now()
// in the page (click event → DOM change → next animation frame).
//
// The API is mocked two ways: answering at once (the "accepted" state, In Temporal, is then
// reached in-budget), and answering after a second (the acknowledgement must come from the
// click itself, not from the server).

import { expect, measureAck, test, type AckResult } from "../support/fixtures";
import { openScripted, sessionId } from "../support/open";
import { commitWaits, idle, stacked } from "../support/scenarios";

const BUDGET_MS = 100;

function report(name: string, r: AckResult) {
  test.info().annotations.push({ type: "ack", description: `${name}: ${r.domMs.toFixed(1)} ms to DOM, ${r.paintMs.toFixed(1)} ms to next frame` });
}

for (const slow of [false, true]) {
  const when = slow ? "API answers after 1 s" : "API answers at once";
  test.describe(when, () => {
    test(`Send shows the message and its delivery state (${when})`, async ({ page, mockApi }) => {
      const api = await mockApi({ postDelayMs: slow ? 1000 : 0 });
      await openScripted(page, api, idle(), { session_id: sessionId("ack-send"), posture: "trust_safe" });
      const box = page.getByRole("textbox", { name: "Message" });
      await box.fill("Run the tests please");
      // Acknowledged: the message is in the transcript (it shows as soon as it is sent).
      const r = await measureAck(page, page.getByRole("button", { name: "Send" }), { selector: "[data-slot=\"transcript\"]", text: "Run the tests please" });
      report("Send → message shown", r);
      expect(r.paintMs).toBeLessThan(BUDGET_MS);
      await expect(box).toHaveValue("");
      if (!slow) {
        // Accepted by the API: "In Temporal" is the current delivery step.
        await expect(page.getByRole("list", { name: "Delivery" }).getByText(/In Temporal/)).toBeVisible();
      }
    });

    test(`Send's In Temporal step (${when})`, async ({ page, mockApi }) => {
      test.skip(slow, "Only meaningful when the API answers at once.");
      const api = await mockApi();
      await openScripted(page, api, idle(), { session_id: sessionId("ack-send-it"), posture: "trust_safe" });
      await page.getByRole("textbox", { name: "Message" }).fill("Accepted state please");
      const r = await measureAck(page, page.getByRole("button", { name: "Send" }), { selector: '[aria-label="Delivery"] [aria-current="step"]', text: "In Temporal" });
      report("Send → In Temporal", r);
      expect(r.paintMs).toBeLessThan(BUDGET_MS);
    });

    test(`Approve marks the call answered (${when})`, async ({ page, mockApi }) => {
      const api = await mockApi({ postDelayMs: slow ? 1000 : 0 });
      await openScripted(page, api, commitWaits(), { session_id: sessionId("ack-approve") });
      const card = page.getByRole("region", { name: "Needs approval" });
      const r = await measureAck(page, card.getByRole("button", { name: "Approve", exact: true }), { selector: "section", text: "Approved by you" });
      report("Approve → Approved by you", r);
      expect(r.paintMs).toBeLessThan(BUDGET_MS);
    });

    test(`Approve all marks every call answered (${when})`, async ({ page, mockApi }) => {
      const api = await mockApi({ postDelayMs: slow ? 1000 : 0 });
      await openScripted(page, api, stacked(), { session_id: sessionId("ack-all"), posture: "trust_safe" });
      const r = await measureAck(page, page.getByRole("button", { name: "Approve all 5" }), { selector: "section", text: "All answered" });
      report("Approve all → All answered", r);
      expect(r.paintMs).toBeLessThan(BUDGET_MS);
    });

    test(`a model pick shows the new model at once (${when})`, async ({ page, mockApi }) => {
      const api = await mockApi({ postDelayMs: slow ? 1000 : 0 });
      await openScripted(page, api, idle(), { session_id: sessionId("ack-model") });
      await page.getByRole("button", { name: /^Model: Claude Sonnet 5\.5 · effort/ }).click();
      const menu = page.getByRole("menu", { name: "Model" });
      const r = await measureAck(page, menu.getByRole("menuitemradio", { name: /GPT-5\.5/ }), { selector: 'button[aria-haspopup="menu"]', text: "GPT-5.5" });
      report("Model pick → button shows GPT-5.5", r);
      expect(r.paintMs).toBeLessThan(BUDGET_MS);
      await expect(page.getByRole("button", { name: /^Model: GPT-5\.5 · effort/ })).toContainText(/Sending…|applies next turn/);
    });

    test(`a posture pick shows the new posture at once (${when})`, async ({ page, mockApi }) => {
      const api = await mockApi({ postDelayMs: slow ? 1000 : 0 });
      await openScripted(page, api, idle(), { session_id: sessionId("ack-posture"), posture: "trust_safe" });
      await page.getByRole("button", { name: /^Approvals: Trust safe tools/ }).click();
      const menu = page.getByRole("menu", { name: "Approvals" });
      const r = await measureAck(page, menu.getByRole("menuitemradio", { name: /Ask for everything/ }), { selector: 'button[aria-haspopup="menu"]', text: "Ask for everything" });
      report("Posture pick → button shows Ask for everything", r);
      expect(r.paintMs).toBeLessThan(BUDGET_MS);
    });
  });
}
