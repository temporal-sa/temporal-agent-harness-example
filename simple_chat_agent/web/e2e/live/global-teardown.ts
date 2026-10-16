// Stops what global-setup and the tests started: the dev worker(s) on this run's task queue
// (SIGKILL through `make crash-worker`) and both API process groups.

import { crashWorker, stopGroup } from "./stack";

export default async function globalTeardown(): Promise<void> {
  const queue = process.env.E2E_TASK_QUEUE;
  if (queue) crashWorker(queue);
  for (const pid of (process.env.E2E_API_PIDS ?? "").split(",").filter(Boolean)) stopGroup(Number(pid));
}
