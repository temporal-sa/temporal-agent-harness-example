// Check 3e against the real stack (owner decision B). A local session with test tools
// (POST /api/agent/sessions `test_tools: true`, Eng F) and posture "Trust safe tools", where
// `gated_echo(text)` (Eng G) always asks and runs at most once per approval. The model is asked
// for five calls in one turn: three different texts and one text twice.
//
// Everything is one tool, so Eng W's grouping (decisions/grouping.ts, labels.ts) shows one
// group, "gated_echo · 5 calls" with "Approve 5" / "Decline 5"; opened, it lists the three distinct
// calls as their own rows and the identical pair as one fold row, "× 2, same arguments".
// One call is answered alone, then Approve all answers the four still waiting. Each call must
// run exactly once: one tool_end per tool id, distinct activity ids, attempt 1.

import type { Page } from "@playwright/test";

import { expect, test } from "../support/fixtures";
import { liveEnv } from "./stack";
import { signIn, waitIdle } from "./helpers";

test.skip(process.env.E2E_LIVE !== "1", "live stack: set E2E_LIVE=1");

const TEXTS = ["alpha", "bravo", "charlie"];
const TWICE = "delta";
const PROMPT =
  "This is a test of parallel tool calls. In ONE response, make exactly five gated_echo calls in parallel, all at once: " +
  `text "${TEXTS[0]}", text "${TEXTS[1]}", text "${TEXTS[2]}", text "${TWICE}", and text "${TWICE}" again. ` +
  "Do not wait for any result before making the others. Do not call any other tool. Afterwards reply with one short sentence.";

/** The stacked card; its accessible name is its heading ("5 tool calls are waiting"). */
const stackCard = (page: Page) => page.getByRole("region", { name: /tool calls? (is|are) waiting$|^All answered$/ });

/** gated_echo arguments as the card shows them (argSummary: the arguments as JSON). */
const args = (text: string) => JSON.stringify({ text });

interface EchoRun {
  toolId: string;
  starts: number;
  ends: number;
  output: string | null;
}

/** Every gated_echo call in the session, from a fresh attach (which ends once idle). */
async function echoRuns(page: Page, base: string, sessionId: string): Promise<Map<string, EchoRun>> {
  const r = await page.request.get(`${base}/api/agent/attach?session_id=${encodeURIComponent(sessionId)}&from_offset=0`, { timeout: 60_000 });
  expect(r.ok()).toBe(true);
  const runs = new Map<string, EchoRun>();
  for (const block of (await r.text()).split("\n\n")) {
    const event = /^event: (.+)$/m.exec(block)?.[1];
    const data = /^data: (.+)$/m.exec(block)?.[1];
    if (!event || !data) continue;
    const d = JSON.parse(data) as Record<string, unknown>;
    if (d.tool_name !== "gated_echo") continue;
    const id = String(d.tool_id);
    const run = runs.get(id) ?? { toolId: id, starts: 0, ends: 0, output: null };
    if (event === "tool_start") run.starts += 1;
    if (event === "tool_end") {
      run.ends += 1;
      run.output = String(d.tool_output ?? "");
    }
    runs.set(id, run);
  }
  return runs;
}

/** activity_id and attempt from gated_echo's result ({"text", "activity_id", "attempt"}). */
function parseEcho(output: string): { text: string | null; activityId: string | null; attempt: number | null } {
  try {
    const o = JSON.parse(output) as Record<string, unknown>;
    return { text: String(o.text), activityId: String(o.activity_id), attempt: Number(o.attempt) };
  } catch {
    // A Python-style repr: {'text': 'alpha', 'activity_id': '12', 'attempt': 1}
    const field = (k: string) => new RegExp(`['"]${k}['"]\\s*:\\s*['"]?([^'",}]+)`).exec(output)?.[1] ?? null;
    const attempt = field("attempt");
    return { text: field("text"), activityId: field("activity_id"), attempt: attempt === null ? null : Number(attempt) };
  }
}

test("3e live: five gated calls, one card, answer one alone, Approve all the rest; each runs once", async ({ page }) => {
  test.setTimeout(10 * 60_000);
  const env = liveEnv();
  await signIn(page, env.baseA, env.userA, env.password);

  // A session with the test tools: only through the API (the product view has no switch).
  const model = process.env.E2E_STACK_MODEL === "sonnet" ? { provider: "anthropic", model: "claude-sonnet-5-5", effort: "low" } : { provider: "anthropic", model: "claude-haiku-5-5", effort: "low" };
  const created = await page.request.post(`${env.baseA}/api/agent/sessions`, {
    data: { title: "e2e stacked approvals", posture: "trust_safe", test_tools: true, model },
    headers: { "X-Requested-With": "coding-agent-web" },
  });
  expect(created.status(), await created.text()).toBe(200);
  const id = String((await created.json()).session_id);
  await page.goto(`${env.baseA}/app/?s=${encodeURIComponent(id)}`);
  await expect(page.getByRole("button", { name: /^Approvals: Trust safe tools/ })).toBeVisible({ timeout: 30_000 });

  const box = page.getByRole("textbox", { name: "Message" });
  await box.fill(PROMPT);
  await box.press("Enter");

  // One card for all five.
  const card = stackCard(page);
  await expect(card).toHaveAccessibleName("5 tool calls are waiting", { timeout: 180_000 });
  await expect(page.getByRole("region", { name: /tool calls? (is|are) waiting$|^Needs approval$/ })).toHaveCount(1);
  await expect(card.getByRole("button", { name: "Approve all 5" })).toBeVisible();

  // One tool, so one group: "gated_echo · 5 calls", Approve 5.
  const group = card.getByRole("listitem").filter({ has: page.getByRole("button", { name: /^Approve \d+ / }) });
  await expect(group).toHaveCount(1);
  await expect(group).toContainText("gated_echo · 5 calls");
  await expect(group.getByRole("button", { name: "Approve 5" })).toBeVisible();

  // Opened: three single rows and the identical pair folded "× 2, same arguments".
  await group.getByRole("button", { name: /gated_echo · 5 calls/ }).click();
  for (const t of TEXTS) await expect(card.getByRole("button", { name: `Approve gated_echo call: ${args(t)}`, exact: true })).toBeVisible();
  const pair = card.getByRole("button", { name: /× 2, same arguments/ });
  await expect(pair).toHaveCount(1);
  await expect(pair).toContainText(args(TWICE));
  await expect(card.getByRole("button", { name: `Approve 2 identical calls: ${args(TWICE)}` })).toBeVisible();

  // Answer one call alone: counts update at once.
  await card.getByRole("button", { name: `Approve ${args(TEXTS[1]!)}`, exact: true }).click();
  await expect(card).toHaveAccessibleName("4 tool calls are waiting");
  await expect(card.getByRole("button", { name: "Approve all 4" })).toBeVisible();

  // Approve all: the four still waiting.
  await card.getByRole("button", { name: "Approve all 4" }).click();
  await waitIdle(page, 300_000);
  await expect(page.getByText(/Not sent|didn't reach Temporal/)).toHaveCount(0);

  // Each call ran exactly once.
  const runs = [...(await echoRuns(page, env.baseA, id)).values()];
  test.info().annotations.push({ type: "gated_echo runs", description: runs.map((r) => `${r.toolId}: start ${r.starts}, end ${r.ends}, ${r.output}`).join(" | ") });
  expect(runs).toHaveLength(5);
  const echoes = runs.map((r) => {
    expect(r.starts, `${r.toolId} started`).toBe(1);
    expect(r.ends, `${r.toolId} ended`).toBe(1);
    return parseEcho(r.output ?? "");
  });
  for (const e of echoes) expect(e.attempt, "attempt").toBe(1);
  const activityIds = echoes.map((e) => e.activityId);
  expect(activityIds.every(Boolean), "every result names its activity").toBe(true);
  expect(new Set(activityIds).size, "distinct activity ids").toBe(5);
  expect(echoes.map((e) => e.text).sort()).toEqual([...TEXTS, TWICE, TWICE].sort());
});
