// The live stack for the `live` project: two API processes (two local users, for the isolation
// check) and the dev worker, all started through the Makefile targets so .env.local is only
// ever read by make (never printed, never read here). Temporal (7244), Redis (6390) and the
// executor (8083) must already be running (`make dev-stack`).

import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { createWriteStream, mkdirSync } from "node:fs";
import { createServer } from "node:net";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
export const REPO = resolve(here, "../../../..");
export const WEB = resolve(here, "../..");
export const LOGS = join(WEB, "e2e", ".results", "live-logs");

export interface LiveEnv {
  baseA: string;
  baseB: string;
  taskQueue: string;
  userA: string;
  userB: string;
  password: string;
}

/** What global-setup put in the environment for the test workers. */
export function liveEnv(): LiveEnv {
  const need = (k: string) => {
    const v = process.env[k];
    if (!v) throw new Error(`${k} is not set: run the live project with E2E_LIVE=1 so global-setup starts the stack`);
    return v;
  };
  return {
    baseA: need("E2E_LIVE_BASE_URL"),
    baseB: need("E2E_LIVE_BASE_URL_B"),
    taskQueue: need("E2E_TASK_QUEUE"),
    userA: need("E2E_USER_A"),
    userB: need("E2E_USER_B"),
    password: need("E2E_PASSWORD"),
  };
}

export async function freePort(avoid: number[] = [8000, 7233, 7244, 8244, 6390, 8083]): Promise<number> {
  for (;;) {
    const port = await new Promise<number>((res, rej) => {
      const srv = createServer();
      srv.once("error", rej);
      srv.listen(0, "127.0.0.1", () => {
        const p = (srv.address() as { port: number }).port;
        srv.close(() => res(p));
      });
    });
    if (!avoid.includes(port)) return port;
  }
}

function logFile(name: string) {
  mkdirSync(LOGS, { recursive: true });
  return createWriteStream(join(LOGS, `${name}.log`), { flags: "a" });
}

/** `make <target> VAR=…` in its own process group (so the whole tree can be stopped). Output
 *  goes to e2e/.results/live-logs/<name>.log, never to the console. */
export function make(name: string, target: string, vars: Record<string, string>, env: Record<string, string> = {}): ChildProcess {
  const args = [target, ...Object.entries(vars).map(([k, v]) => `${k}=${v}`)];
  const child = spawn("make", args, { cwd: REPO, env: { ...process.env, ...env }, detached: true, stdio: ["ignore", "pipe", "pipe"] });
  const out = logFile(name);
  out.write(`\n--- ${new Date().toISOString()} make ${target} (${Object.keys(vars).join(", ")})\n`);
  child.stdout?.pipe(out, { end: false });
  child.stderr?.pipe(out, { end: false });
  child.unref();
  return child;
}

/** Stop a process group started by `make`. */
export function stopGroup(pid: number | undefined, signal: NodeJS.Signals = "SIGTERM") {
  if (!pid) return;
  try {
    process.kill(-pid, signal);
  } catch {
    // already gone
  }
}

export async function waitFor(what: string, check: () => Promise<boolean> | boolean, timeoutMs = 60_000, everyMs = 250): Promise<void> {
  const end = Date.now() + timeoutMs;
  for (;;) {
    try {
      if (await check()) return;
    } catch {
      // not yet
    }
    if (Date.now() > end) throw new Error(`timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, everyMs));
  }
}

export async function httpOk(url: string): Promise<boolean> {
  const r = await fetch(url, { signal: AbortSignal.timeout(2000) });
  return r.ok;
}

/** Start the dev worker on the test's task queue and wait until it polls. */
export async function startWorker(taskQueue: string): Promise<ChildProcess> {
  const child = make("worker", "dev-worker", { TASK_QUEUE: taskQueue });
  let ready = false;
  child.stdout?.on("data", (b: Buffer) => {
    if (b.toString().includes("worker ready")) ready = true;
  });
  await waitFor("the dev worker to start", () => ready, 90_000);
  return child;
}

/** SIGKILL the dev worker(s) on this task queue (`make crash-worker TASK_QUEUE=…`). */
export function crashWorker(taskQueue: string): string {
  const r = spawnSync("make", ["crash-worker", `TASK_QUEUE=${taskQueue}`], { cwd: REPO, encoding: "utf8" });
  const out = `${r.stdout ?? ""}${r.stderr ?? ""}`;
  logFile("crash-worker").write(`\n--- ${new Date().toISOString()}\n${out}`);
  return out;
}
