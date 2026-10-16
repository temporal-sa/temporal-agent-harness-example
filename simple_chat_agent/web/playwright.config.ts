// Playwright for the product view (P3 step 3.5). Two projects:
//
//   mocked  the built app (web/dist) behind e2e/support/mock-server.mjs, every /api route
//           mocked in the page; fast, no backend. Its timing checks (3b) run last as the
//           `mocked-timing` project, alone.
//   live    the real local stack (API + dev worker started by e2e/live/global-setup.ts on free
//           ports and a unique task queue). Skipped unless E2E_LIVE=1.
//
//   npx playwright test --project=mocked
//   E2E_LIVE=1 npx playwright test --project=live
//
// @playwright/test is pinned at exactly 1.63.0 (Chrome Headless Shell 153).

import { defineConfig, devices } from "@playwright/test";

const MOCK_PORT = Number(process.env.E2E_MOCK_PORT ?? 5179);
const LIVE = process.env.E2E_LIVE === "1";
// Build once before serving, unless told the dist is fresh.
// E2E_WEB_DIST points both projects at a build made elsewhere (and skips this one).
const build = process.env.E2E_SKIP_BUILD === "1" || process.env.E2E_WEB_DIST ? "" : "npm run build && ";

export default defineConfig({
  testDir: "e2e",
  outputDir: "e2e/.results",
  fullyParallel: !LIVE,
  // The live tests share one worker process on one task queue and crash it on purpose.
  workers: LIVE ? 1 : undefined,
  forbidOnly: !!process.env.CI,
  retries: 0,
  timeout: 30_000,
  expect: { timeout: 5_000 },
  reporter: [["list"], ["html", { outputFolder: "e2e/.report", open: "never" }]],
  globalSetup: LIVE ? "./e2e/live/global-setup.ts" : undefined,
  globalTeardown: LIVE ? "./e2e/live/global-teardown.ts" : undefined,
  use: {
    ...devices["Desktop Chrome"],
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
  },
  projects: [
    // The 100 ms acknowledgement checks (3b) run alone, after the main mocked tests, so CPU
    // contention can't skew the timings. They are `mocked`'s teardown project: `--project=mocked`
    // runs them too, and a failure in either project never skips the other.
    {
      name: "mocked-timing",
      testDir: "e2e/mocked",
      testMatch: /ack\.spec\.ts$/,
      fullyParallel: false,
      use: { baseURL: `http://127.0.0.1:${MOCK_PORT}` },
    },
    {
      name: "mocked",
      testDir: "e2e/mocked",
      testIgnore: /ack\.spec\.ts$/,
      teardown: "mocked-timing",
      use: { baseURL: `http://127.0.0.1:${MOCK_PORT}` },
    },
    {
      name: "live",
      testDir: "e2e/live",
      timeout: 10 * 60_000,
      expect: { timeout: 30_000 },
      use: { baseURL: process.env.E2E_LIVE_BASE_URL, trace: "retain-on-failure" },
    },
  ],
  webServer: {
    command: `${build}node e2e/support/mock-server.mjs`,
    url: `http://127.0.0.1:${MOCK_PORT}/__mock/health`,
    reuseExistingServer: !process.env.CI,
    env: { E2E_MOCK_PORT: String(MOCK_PORT), ...(process.env.E2E_WEB_DIST ? { E2E_WEB_DIST: process.env.E2E_WEB_DIST } : {}) },
    stdout: "ignore",
    stderr: "pipe",
    timeout: 120_000,
  },
});
