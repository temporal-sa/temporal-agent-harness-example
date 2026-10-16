// Check 3d: with the worker killed (`make crash-worker`), a reloaded tab renders the whole
// transcript, the pending approval card survives, and a message sent meanwhile shows
// "In Temporal · waiting for a worker", then applies exactly once when the worker returns.
// Claude Haiku 5.5, effort low; posture "Trust safe tools", so run_command asks first.

import { expect, test } from "../support/fixtures";
import { crashWorker, liveEnv, startWorker } from "./stack";
import { meter, expectedTokens, newDemoSession, pickHaikuLow, send, signIn, usageFrames, waitIdle } from "./helpers";

test.skip(process.env.E2E_LIVE !== "1", "live stack: set E2E_LIVE=1");

test("3d workers down: reload keeps the transcript and the card; a message waits in Temporal and applies once", async ({ page }) => {
  test.setTimeout(10 * 60_000);
  const env = liveEnv();
  await signIn(page, env.baseA, env.userA, env.password);
  const id = await newDemoSession(page);
  await pickHaikuLow(page);

  const ask = "Use run_command to run `ls` in the workspace. Do nothing else, then say what you saw in one sentence.";
  await send(page, ask);
  const card = page.getByRole("region", { name: "Needs approval" });
  await expect(card).toBeVisible({ timeout: 180_000 });
  await expect(card).toContainText("ls");

  // Kill the worker while the agent waits for the approval.
  const killed = crashWorker(env.taskQueue);
  expect(killed).toContain("SIGKILL");
  try {
    // A reloaded tab: the transcript and the card come back from the stream, no worker needed.
    await page.reload();
    await expect(page.getByText(ask)).toBeVisible({ timeout: 30_000 });
    await expect(card).toBeVisible({ timeout: 30_000 });
    await expect(card).toContainText("The agent is paused in Temporal and nothing is running.");

    // A message sent now lands in Temporal and waits for a worker.
    const question = "What is 6 times 7? Answer with only the number.";
    await send(page, question);
    const trail = page.getByRole("list", { name: "Delivery" }).last();
    await expect(trail.getByText("In Temporal · waiting for a Worker")).toBeVisible({ timeout: 20_000 });
    await expect(page.getByText(question)).toHaveCount(1);
  } finally {
    await startWorker(env.taskQueue);
  }

  // The worker is back: answer the card; the turn finishes, then the queued message runs.
  await card.getByRole("button", { name: "Approve", exact: true }).click();
  await waitIdle(page, 300_000);
  await expect(page.getByText(/^\s*42\.?\s*$/)).toHaveCount(1, { timeout: 120_000 });
  await waitIdle(page, 120_000);
  // Applied once: one user message, one answer, nothing failed.
  await expect(page.getByText("What is 6 times 7? Answer with only the number.")).toHaveCount(1);
  await expect(page.getByText(/Not sent|didn't reach Temporal/)).toHaveCount(0);

  // 3g, live: the strip equals the sum of the session's x_usage frames.
  const frames = await usageFrames(page, env.baseA, id);
  expect(frames.length).toBeGreaterThan(0);
  await expect(meter(page, "Session tokens")).toHaveText(expectedTokens(frames));
});
