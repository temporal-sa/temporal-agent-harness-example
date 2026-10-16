// Check 3h: two browsers as two local users (two API processes on the same Redis and Temporal,
// one per local login). Neither sees the other's session in the sidebar or by URL.

import { expect, test } from "../support/fixtures";
import { liveEnv } from "./stack";
import { newEmptySession, signIn } from "./helpers";

test.skip(process.env.E2E_LIVE !== "1", "live stack: set E2E_LIVE=1");

test("3h isolation: user B can't see or open user A's session, and the other way round", async ({ page, browser, requestLog }) => {
  const env = liveEnv();
  await signIn(page, env.baseA, env.userA, env.password);
  const a = await newEmptySession(page);
  await expect(page.getByRole("complementary", { name: "Sessions" }).getByRole("button", { name: /New session/ }).first()).toBeVisible();

  const ctxB = await browser.newContext();
  requestLog.watch(ctxB);
  const pageB = await ctxB.newPage();
  try {
    await signIn(pageB, env.baseB, env.userB, env.password);
    const b = await newEmptySession(pageB);
    expect(b).not.toBe(a);

    // Sidebars: each lists only its own session.
    await pageB.reload();
    await expect(pageB.locator("body")).not.toContainText(a);
    const sideB = pageB.getByRole("complementary", { name: "Sessions" });
    await expect(sideB.getByRole("button", { name: /New session/ })).toHaveCount(1);
    await page.reload();
    const sideA = page.getByRole("complementary", { name: "Sessions" });
    await expect(sideA.getByRole("button", { name: /New session/ })).toHaveCount(1);

    // By URL: B opens A's session.
    await pageB.goto(`${env.baseB}/app/?s=${encodeURIComponent(a)}`);
    // The API answers 404 ("No such session."); the app shows it and nothing of A's session.
    await expect(pageB.getByRole("alert").filter({ hasText: /No such session|doesn't exist, or isn't yours/ }).first()).toBeVisible();
    await expect(pageB.getByText("Ask the agent to do something in its workspace.")).toBeVisible();
    await expect(pageB.getByRole("banner")).not.toContainText("New session");
    // The routes themselves answer 404 for the other user's session.
    for (const path of [`sessions/${a}`, `status/${a}`, `workflow-status/${a}`, `attach?session_id=${a}&from_offset=0`]) {
      const r = await pageB.request.get(`${env.baseB}/api/agent/${path}`);
      expect(r.status(), `B: GET ${path}`).toBe(404);
    }
    const sendB = await pageB.request.post(`${env.baseB}/api/agent/messages`, {
      data: { session_id: a, message: { type: "chat", payload: { text: "hello" } } },
      headers: { "X-Requested-With": "coding-agent-web" },
    });
    expect(sendB.status(), "B: POST messages to A's session").toBe(404);

    // And A can't open B's.
    const r = await page.request.get(`${env.baseA}/api/agent/sessions/${b}`);
    expect(r.status()).toBe(404);
  } finally {
    await ctxB.close();
  }
});
