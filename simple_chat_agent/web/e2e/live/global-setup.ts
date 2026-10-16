// Starts the live stack once for the `live` project (E2E_LIVE=1): builds the app, then two
// APIs (`make dev-api`, users A and B, for the isolation check) and the dev worker
// (`make dev-worker`), on free ports and a task queue unique to this run. global-teardown.ts
// stops everything. Temporal, Redis and the executor must already be up (`make dev-stack`).

import { execSync } from "node:child_process";
import { randomBytes } from "node:crypto";

import { WEB, freePort, httpOk, make, startWorker, waitFor } from "./stack";

export default async function globalSetup(): Promise<void> {
  for (const [what, url] of [
    ["the sandbox executor on 8083", "http://127.0.0.1:8083/healthz"],
  ] as const) {
    if (!(await httpOk(url).catch(() => false))) throw new Error(`${what} isn't answering; start the local stack (make dev-stack)`);
  }
  if (process.env.E2E_SKIP_BUILD !== "1" && !process.env.E2E_WEB_DIST) execSync("npm run build", { cwd: WEB, stdio: "ignore" });

  const run = `${Date.now().toString(36)}${randomBytes(2).toString("hex")}`;
  const taskQueue = `e2e-${run}`;
  const password = randomBytes(12).toString("hex");
  const userA = `e2e-a-${run}`;
  const userB = `e2e-b-${run}`;
  const portA = await freePort();
  let portB = await freePort();
  while (portB === portA) portB = await freePort();

  // E2E_WEB_DIST: have the APIs serve another build (the API's SIMPLE_CHAT_WEB_DIST).
  const dist: Record<string, string> = process.env.E2E_WEB_DIST ? { SIMPLE_CHAT_WEB_DIST: process.env.E2E_WEB_DIST } : {};
  const auth = (user: string) => ({ SIMPLE_CHAT_LOCAL_AUTH_USERNAME: user, SIMPLE_CHAT_LOCAL_AUTH_PASSWORD: password, ...dist });
  const apiA = make("api-a", "dev-api", { API_PORT: String(portA), TASK_QUEUE: taskQueue }, auth(userA));
  const apiB = make("api-b", "dev-api", { API_PORT: String(portB), TASK_QUEUE: taskQueue }, auth(userB));
  const baseA = `http://127.0.0.1:${portA}`;
  const baseB = `http://127.0.0.1:${portB}`;

  Object.assign(process.env, {
    E2E_LIVE_BASE_URL: baseA,
    E2E_LIVE_BASE_URL_B: baseB,
    E2E_TASK_QUEUE: taskQueue,
    E2E_USER_A: userA,
    E2E_USER_B: userB,
    E2E_PASSWORD: password,
    E2E_API_PIDS: [apiA.pid, apiB.pid].join(","),
  });

  await waitFor("API A", () => httpOk(`${baseA}/healthz`), 60_000);
  await waitFor("API B", () => httpOk(`${baseB}/healthz`), 60_000);
  await startWorker(taskQueue);
}
