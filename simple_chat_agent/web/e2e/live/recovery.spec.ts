// Check 3f (and 3g live): the demo script's crash beat. A new demo session in Auto mode asks for
// the test run; `make crash-worker` SIGKILLs the worker while pytest runs; the worker comes
// back. The banner shows, then the recovery divider, whose numbers turn from "Checking…" to
// confirmed; the reply appears once; the meters equal the sum of the session's x_usage.
// Claude Haiku 5.5, effort low.

import { expect, test } from "../support/fixtures";
import { crashWorker, liveEnv, startWorker } from "./stack";
import { approveWhileWaiting, expectedTokens, meter, newDemoSession, pickHaikuLow, pickPosture, send, signIn, usageFrames, waitIdle } from "./helpers";

test.skip(process.env.E2E_LIVE !== "1", "live stack: set E2E_LIVE=1");

test("3f recovery: crash during pytest → banner → divider → confirmed numbers, reply once", async ({ page }) => {
  test.setTimeout(12 * 60_000);
  const env = liveEnv();
  await signIn(page, env.baseA, env.userA, env.password);
  const id = await newDemoSession(page);
  await pickPosture(page, "Auto mode");
  await pickHaikuLow(page);

  // The reply's last word is built by the model, so it never appears in our own message.
  await send(
    page,
    "Run the test suite with run_command using exactly `pytest -q`, then tell me how many tests passed and failed. " +
      "Don't change any files. End your reply with the word made by joining zeb and ra7.",
  );

  // Wait until pytest is running (approve it if the judge escalates), then crash the worker.
  const running = page.getByRole("button", { name: /run_command\s*pytest -q.*running/ });
  await approveWhileWaiting(page, () => running.isVisible(), 240_000);
  await page.waitForTimeout(2000);
  const killed = crashWorker(env.taskQueue);
  expect(killed).toContain("SIGKILL");
  const crashedAt = Date.now();

  let bannerSeen = false;
  try {
    const banner = page.getByRole("status", { name: "Worker isn't answering" });
    await expect(banner).toBeVisible({ timeout: 60_000 });
    bannerSeen = true;
    await expect(banner).toContainText("Waiting:");
    test.info().annotations.push({ type: "banner", description: `after ${((Date.now() - crashedAt) / 1000).toFixed(1)} s` });
  } finally {
    await startWorker(env.taskQueue);
  }
  expect(bannerSeen).toBe(true);

  // The divider, "Checking…" until the bridge confirms it from history.
  const divider = page.getByRole("region", { name: /^Recovered · worker lost/ });
  await expect(divider).toBeVisible({ timeout: 120_000 });
  const firstText = (await divider.textContent()) ?? "";
  test.info().annotations.push({ type: "divider first seen", description: firstText.includes("Checking…") ? "Checking…" : "already confirmed" });
  await expect(divider).not.toContainText("Checking…", { timeout: 120_000 });
  await expect(divider).toContainText(/Kept\s*\d+ model calls?/);
  await expect(page.getByRole("status", { name: "Worker isn't answering" })).toHaveCount(0);

  await waitIdle(page, 300_000);
  await expect(page.getByText(/zebra7/i)).toHaveCount(1);
  await expect(page.getByText("Work kept after worker loss (this session)")).toBeVisible();
  await expect(meter(page, "Recoveries")).toHaveText("1");

  // 3g live: the strip equals the sum of the confirmed x_usage frames.
  const frames = await usageFrames(page, env.baseA, id);
  expect(frames.length).toBeGreaterThan(0);
  test.info().annotations.push({ type: "x_usage", description: `${frames.length} calls, ${frames.filter((f) => f.confirmed).length} confirmed` });
  await expect(meter(page, "Session tokens")).toHaveText(expectedTokens(frames));
});
