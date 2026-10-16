// Open a scripted session in the mocked app.

import type { Page } from "@playwright/test";

import type { Frame } from "./frames";
import type { MockApi } from "./mock-api";
import { summary, type Summary } from "./mock-api";

let n = 0;

/** A session id unique to this test run (the mock server keys its streams by it). */
export function sessionId(tag: string): string {
  return `sess-${tag}-${process.pid}-${++n}-${Date.now().toString(36)}`;
}

export async function openScripted(page: Page, api: MockApi, frames: Frame[], over: Partial<Summary> & { session_id: string }): Promise<Summary> {
  const s = summary(over.session_id, over);
  if (!api.sessions.some((x) => x.session_id === s.session_id)) api.sessions.unshift(s);
  await api.script(s.session_id, frames);
  await page.goto(`/app/?s=${encodeURIComponent(s.session_id)}`);
  return s;
}
