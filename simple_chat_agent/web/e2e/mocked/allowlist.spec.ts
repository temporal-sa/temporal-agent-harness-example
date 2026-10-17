// Check 3c: no browser request reaches a Query-backed path. The fixture checks every test's
// requests against the allowlist (support/allowlist.ts); this test walks the demo script's
// whole surface in the mocked app (sign in, new demo session, a turn, an approval, a queued
// message, model and posture, the full tool log, an upload, a reload) and checks that each
// allowed route was exercised. The live project runs the same check against the real stack.

import { ALLOWED, judge } from "../support/allowlist";
import { expect, test } from "../support/fixtures";
import { summary } from "../support/mock-api";
import { sessionId } from "../support/open";
import { PYTEST, commitWaits, turn, working, workingEnds } from "../support/scenarios";

test("the demo script's requests are all on the allowlist", async ({ page, mockApi, requestLog, baseURL }) => {
  const id = sessionId("allow");
  const created = summary(id, { created_at: Date.now() / 1000 });
  const api = await mockApi({ signedOut: true, sessions: [], created });
  const t1 = turn(1, "msg-1");
  await api.script(id, [...working(t1), ...workingEnds(t1), ...commitWaits(turn(2, "msg-2"))]);

  // Sign in (local login).
  await page.goto("/");
  await page.getByLabel("Username").fill("demo");
  await page.getByLabel("Password").fill("demo");
  await page.getByRole("button", { name: "Sign in" }).click();

  // New demo session.
  await page.getByRole("button", { name: "New demo session" }).first().click();
  await expect(page.getByRole("region", { name: "Needs approval" })).toBeVisible();

  // A message mid-turn (Enter queues it), the approval, model and posture.
  await page.getByRole("textbox", { name: "Message" }).fill("Then review the diff.");
  await page.getByRole("textbox", { name: "Message" }).press("Enter");
  await page.getByRole("region", { name: "Needs approval" }).getByRole("button", { name: "Approve", exact: true }).click();
  await page.getByRole("button", { name: /^Model: Claude Sonnet 5\.5 · effort/ }).click();
  await page.getByRole("menu", { name: "Model" }).getByRole("menuitemradio", { name: /GPT-5\.5/ }).click();
  await page.getByRole("button", { name: /^Approvals: Auto mode/ }).click();
  await page.getByRole("menu", { name: "Approvals" }).getByRole("menuitemradio", { name: /Trust safe tools/ }).click();

  // The full log of the test run (S7).
  await page.getByRole("button", { name: new RegExp(`run_command.*${PYTEST}`) }).first().click();
  await page.getByRole("button", { name: "Show the full log" }).click();
  await expect(page.getByText("10 passed in 19.6s").first()).toBeVisible();

  // An upload.
  const chooser = page.waitForEvent("filechooser");
  await page.getByRole("button", { name: "Attach a file" }).click();
  await (await chooser).setFiles({ name: "notes.txt", mimeType: "text/plain", buffer: Buffer.from("hello") });
  await expect(page.getByText("notes.txt · attached")).toBeVisible();

  // Reload: the session comes back from the stream.
  await page.reload();
  await expect(page.getByText("test_refund fails. Find the cause, fix it, run the tests, commit.")).toBeVisible();

  const verdicts = requestLog.verdicts();
  const bad = verdicts.filter((v) => !v.allowed).map((v) => `${v.request.method} ${v.request.url} (${v.reason})`);
  expect(bad).toEqual([]);

  const hit = new Set(verdicts.map((v) => v.allowed?.route).filter(Boolean));
  // Reached only in other states: a session opened by URL that the list lacks, a stream that
  // ended (the mock keeps it open), and the browser's own favicon request.
  const conditional = new Set(["GET /favicon.ico", "GET /api/agent/sessions/{id}", "GET /api/agent/workflow-status/{id}"]);
  const expected = ALLOWED.filter((a) => !a.feature).map((a) => a.route).filter((r) => !conditional.has(r));
  expect(expected.filter((r) => !hit.has(r)), "allowed routes this walk didn't reach").toEqual([]);
  test.info().annotations.push({ type: "routes", description: [...hit].join(", ") });
  void baseURL;
});

test("the checker flags a Query-backed path", async ({ page, mockApi, requestLog, baseURL }) => {
  requestLog.skipCheck();
  const api = await mockApi();
  void api;
  await page.goto("/");
  await page.evaluate(() => fetch("/api/chats").catch(() => undefined));
  const v = judge({ method: "GET", url: `${baseURL}/api/chats`, resourceType: "fetch" }, new URL(baseURL!).origin);
  expect(v.allowed).toBeNull();
  expect(v.reason).toBe("QUERY-BACKED path");
  expect(requestLog.verdicts().some((x) => x.reason === "QUERY-BACKED path")).toBe(true);
});
