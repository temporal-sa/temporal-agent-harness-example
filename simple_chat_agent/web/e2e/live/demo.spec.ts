// Check 3i: the PRD §2.5 demo script, end to end through the product view, on one session, as a
// presenter would run it (1920 × 1080). Run it twice in a row:
//
//   E2E_LIVE=1 npx playwright test --project=live e2e/live/demo.spec.ts --repeat-each=2
//
// The model's moves are nondeterministic, so each beat asserts outcomes. A beat that can't be
// shown at all fails the run; a move the model made differently (one explorer instead of two,
// a commit without the escalation…) is recorded as a soft observation. Per run, beats.json
// (pass/fail, timings, observations, tokens) and screenshots per beat and per event go to
// e2e/.demo-runs/demo-run-<n>/ (gitignored). Not under e2e/.results: Playwright empties its
// output folder at the start of every run, so another run would delete the evidence.

import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import type { Page } from "@playwright/test";

import { expect, test } from "../support/fixtures";
import { crashWorker, liveEnv, startWorker, WEB } from "./stack";
import { expectedTokens, meter, newDemoSession, pickPosture, send, signIn, usageFrames, waitIdle } from "./helpers";

test.skip(process.env.E2E_LIVE !== "1", "live stack: set E2E_LIVE=1");
test.use({ viewport: { width: 1920, height: 1080 } });

const DEMO_PROMPT = "test_refund fails. Find the cause, fix it, run the tests, commit.";
const FIX_FILE = "refunds.py";

interface BeatResult {
  beat: string;
  ok: boolean;
  startedS: number;
  durationS: number;
  observations: string[];
  error?: string;
  screenshot?: string;
}

class Script {
  readonly results: BeatResult[] = [];
  readonly t0 = Date.now();
  private notes: string[] = [];
  private extra: Record<string, unknown> = {};

  constructor(
    private readonly page: Page,
    readonly dir: string,
  ) {
    mkdirSync(dir, { recursive: true });
  }

  /** Record a soft observation: the model did something differently; not a failure. */
  observe(note: string) {
    this.notes.push(note);
    test.info().annotations.push({ type: "observation", description: note });
  }

  /** A check that only records an observation when it fails. */
  async soft(what: string, check: () => Promise<unknown>) {
    try {
      await check();
      return true;
    } catch (err) {
      this.observe(`${what}: ${String((err as Error).message).split("\n")[0]}`);
      return false;
    }
  }

  async beat(name: string, run: () => Promise<void>) {
    const start = Date.now();
    this.notes = [];
    let error: string | undefined;
    try {
      await run();
    } catch (err) {
      error = String((err as Error).message).split("\n").slice(0, 3).join(" ");
    }
    const file = `${String(this.results.length + 1).padStart(2, "0")}-${name.replace(/[^\w]+/g, "-").replace(/-+$/, "")}.png`;
    await this.page.screenshot({ path: join(this.dir, file) }).catch(() => undefined);
    const r: BeatResult = {
      beat: name,
      ok: !error,
      startedS: (start - this.t0) / 1000,
      durationS: (Date.now() - start) / 1000,
      observations: this.notes,
      ...(error ? { error } : {}),
      screenshot: file,
    };
    this.results.push(r);
    test.info().annotations.push({ type: `beat ${name}`, description: `${r.ok ? "pass" : "FAIL"} in ${r.durationS.toFixed(1)} s${error ? `: ${error}` : ""}` });
    this.save();
  }

  /** Grade a beat from what the driver saw (no action of its own). */
  async grade(name: string, startedS: number | null, endedS: number | null, check: () => Promise<void>) {
    const t = Date.now();
    await this.beat(name, check);
    const r = this.results[this.results.length - 1]!;
    if (startedS !== null) r.startedS = startedS;
    r.durationS = startedS !== null && endedS !== null ? endedS - startedS : (Date.now() - t) / 1000;
    this.save();
  }

  get failed(): BeatResult[] {
    return this.results.filter((r) => !r.ok);
  }

  /** Seconds since the run started. */
  now(): number {
    return (Date.now() - this.t0) / 1000;
  }

  save(extra: Record<string, unknown> = {}) {
    Object.assign(this.extra, extra);
    writeFileSync(join(this.dir, "beats.json"), JSON.stringify({ totalS: (Date.now() - this.t0) / 1000, beats: this.results, ...this.extra }, null, 2));
  }
}

const composerModel = (page: Page) => page.locator("[data-slot=composer]").getByRole("button", { name: /^Model:/ });

async function pickModel(page: Page, name: RegExp, shown: string) {
  await composerModel(page).click();
  const item = page.getByRole("menu", { name: "Model" }).getByRole("menuitemradio", { name });
  if ((await item.getAttribute("aria-checked")) !== "true") await item.click();
  else await page.keyboard.press("Escape");
  await expect(composerModel(page)).toContainText(shown);
}

const toolRows = (page: Page, re: RegExp) => page.locator("[data-slot=transcript]").getByRole("button", { name: re });
const explorerRows = (page: Page) => page.locator("[data-slot=transcript]").getByRole("button", { name: /^Explorer/ });

/** Paragraphs that appear more than once in the transcript (a duplicated reply). */
async function duplicatedText(page: Page): Promise<string[]> {
  return page.locator("[data-slot=transcript]").evaluate((root) => {
    const seen = new Map<string, number>();
    for (const el of Array.from(root.querySelectorAll("p, li"))) {
      if (el.closest("button, pre, .attempt-wrap")) continue;
      const t = (el.textContent ?? "").trim().replace(/\s+/g, " ");
      if (t.length < 40) continue;
      seen.set(t, (seen.get(t) ?? 0) + 1);
    }
    return [...seen].filter(([, n]) => n > 1).map(([t, n]) => `${n}× ${t.slice(0, 80)}`);
  });
}

test("3i demo script (§2.5) end to end", async ({ page, browser }, testInfo) => {
  test.setTimeout(40 * 60_000);
  const env = liveEnv();
  const run = testInfo.repeatEachIndex + 1;
  const s = new Script(page, join(WEB, "e2e", ".demo-runs", `demo-run-${run}`));
  let id = "";
  const transcript = page.locator("[data-slot=transcript]");
  /** When each event was first seen (seconds since the run started). */
  const seen: Record<string, number> = {};
  const mark = (k: string) => (seen[k] ??= s.now());
  const at = (k: string) => seen[k] ?? null;
  /** Mark an event and, the first time, screenshot it (e2e/.results/demo-run-<n>/event-*.png). */
  const note = async (k: string) => {
    if (seen[k] !== undefined) return;
    mark(k);
    await page.screenshot({ path: join(s.dir, `event-${String(Math.round(seen[k]!)).padStart(4, "0")}s-${k}.png`) }).catch(() => undefined);
  };

  try {
    // -- 0:00 -------------------------------------------------------------------------------
    await s.beat("0:00 open, new demo session, Auto mode, Sonnet", async () => {
      await signIn(page, env.baseA, env.userA, env.password);
      id = await newDemoSession(page);
      await expect(page.getByRole("banner")).toContainText("Refund service");
      await pickPosture(page, "Auto mode");
      await pickModel(page, /^Claude Sonnet 5\.5/, "Claude Sonnet 5.5");
    });
    if (!id) throw new Error("no session: the demo can't start");

    // -- 0:40 to 4:10: one turn. Drive it like a presenter, noting when each beat shows. -----
    await send(page, DEMO_PROMPT);
    mark("sent");
    const stop = page.getByRole("button", { name: /^Stop/ });
    const card = page.getByRole("region", { name: "Needs approval" });
    const diff = page.getByLabel(new RegExp(`Changes to .*${FIX_FILE.replace(".", "\\.")} in this call`));
    const judgedPytest = page.getByRole("note", { name: /^Judge approved: run_command/ }).filter({ hasText: "pytest" });
    const runningPytest = toolRows(page, /run_command.*pytest.*running/);
    const banner = page.getByRole("status", { name: "Worker isn't answering" });
    const divider = page.getByRole("region", { name: /^(Another Worker took over · checking|Recovered · Worker lost)/ });
    let maxExplorers = 0;
    let crashedDuring = "";
    let terminalChecked = false;
    const cards: string[] = [];
    const deadline = Date.now() + 25 * 60_000;
    while (Date.now() < deadline) {
      if (await stop.isVisible().catch(() => false)) mark("busy");
      const n = await explorerRows(page).count();
      if (n > 0) await note("explorers");
      maxExplorers = Math.max(maxExplorers, n);
      if (await page.getByRole("tab", { name: /^Plan.*of [1-9]\d* steps/ }).isVisible().catch(() => false)) await note("plan");
      if (await diff.first().isVisible().catch(() => false)) await note("diff");
      if (await judgedPytest.first().isVisible().catch(() => false)) await note("judgedPytest");
      if (await divider.isVisible().catch(() => false)) {
        await note("divider");
        const text = (await divider.textContent().catch(() => "")) ?? "";
        if (text.includes("Checking…")) await note("dividerChecking");
        else if (/Kept\s*\d+ model calls?/.test(text)) await note("dividerConfirmed");
      }

      // 2:40: crash during a test run that comes after the fix.
      if (!seen.crash && seen.diff && (await runningPytest.first().isVisible().catch(() => false))) {
        mark("pytestRunning");
        crashedDuring = ((await runningPytest.first().textContent()) ?? "").slice(0, 120);
        if (!terminalChecked) {
          terminalChecked = true;
          await page.getByRole("tab", { name: /^Terminal/ }).click();
          if (await s.soft("Terminal shows the running pytest", () => expect(page.getByRole("tabpanel")).toContainText("pytest", { timeout: 5_000 }))) await note("terminal");
        }
        expect(crashWorker(env.taskQueue)).toContain("SIGKILL");
        mark("crash");
        try {
          await expect(banner).toBeVisible({ timeout: 60_000 });
          await note("banner");
        } catch {
          // graded below
        } finally {
          await startWorker(env.taskQueue);
          mark("workerBack");
        }
        continue;
      }

      // 4:10: an approval card. A commit: reload first (the card must survive), then approve.
      if (await card.isVisible().catch(() => false)) {
        const text = ((await card.textContent()) ?? "").replace(/\s+/g, " ");
        cards.push(text.slice(0, 160));
        if (/commit/i.test(text) && !seen.commitCard) {
          await note("commitCard");
          await page.reload();
          await expect(page.getByText(DEMO_PROMPT)).toBeVisible({ timeout: 30_000 });
          if (await card.isVisible({ timeout: 30_000 }).catch(() => false)) {
            await expect(card).toBeVisible({ timeout: 30_000 });
            if (/commit/i.test((await card.textContent()) ?? "")) mark("commitCardAfterReload");
          }
          await page.screenshot({ path: join(s.dir, "04a-commit-card-after-reload.png") });
        } else if (!/commit/i.test(text)) {
          s.observe(`the judge escalated a non-commit call: ${text.slice(0, 120)}`);
        }
        await card.getByRole("button", { name: "Approve", exact: true }).click();
        mark(/commit/i.test(text) ? "commitApproved" : "otherApproved");
        await expect(card).toHaveCount(0, { timeout: 30_000 }).catch(() => undefined);
        continue;
      }

      if (seen.busy && !(await stop.isVisible().catch(() => true)) && (await page.getByRole("button", { name: "Send" }).isVisible().catch(() => false))) {
        await note("turnEnded");
        break;
      }
      await page.waitForTimeout(250);
    }
    if (seen.divider && !seen.dividerConfirmed) {
      await expect(divider).toContainText(/Kept\s*\d+ model calls?/, { timeout: 120_000 }).then(() => mark("dividerConfirmed"), () => undefined);
    }
    s.save({ seen, cards, crashedDuring, maxExplorers });

    // -- grade 0:40 to 4:10 -----------------------------------------------------------------
    await s.grade("0:40 prompt, plan, two explorers", at("sent"), at("explorers"), async () => {
      if (maxExplorers < 2) s.observe(`${maxExplorers} explorer row(s), not 2`);
      if (!seen.plan) s.observe("no plan was written");
      expect(maxExplorers, "explorer rows (the beat needs the explorers)").toBeGreaterThan(0);
      const n = await explorerRows(page).count();
      await expect(explorerRows(page).filter({ hasText: /Done/ })).toHaveCount(n);
      await expect(transcript.getByText("Findings", { exact: true })).toHaveCount(n);
      await expect(page.getByText(/Not readable|stream unavailable|could not be read/i)).toHaveCount(0);
    });
    await s.grade("1:50 inline diff, judge approves pytest, terminal streams", at("explorers") ?? at("sent"), at("judgedPytest"), async () => {
      expect(seen.diff, `inline diff on ${FIX_FILE}`).toBeTruthy();
      expect(seen.judgedPytest, "judge card approving pytest").toBeTruthy();
      await s.soft("an edit badged 'Approved by the judge'", () => expect(page.getByText("Approved by the judge").first()).toBeVisible({ timeout: 5_000 }));
      if (!seen.terminal) s.observe("didn't see pytest in the Terminal tab while it ran");
    });
    await s.grade("2:40 crash during the test run, banner, divider, green, reply once", at("crash"), at("dividerConfirmed"), async () => {
      expect(seen.crash, "a pytest run after the fix to crash during").toBeTruthy();
      if (/::|-k /.test(crashedDuring)) s.observe(`crashed during a targeted run: ${crashedDuring}`);
      expect(seen.banner, "the worker banner").toBeTruthy();
      expect(seen.divider, "the recovery divider").toBeTruthy();
      if (!seen.dividerChecking) s.observe("divider already confirmed when first seen");
      expect(seen.dividerConfirmed, "confirmed numbers").toBeTruthy();
      await expect(toolRows(page, /run_command.*pytest.*exit 0/).first()).toBeVisible({ timeout: 10_000 });
      expect(await duplicatedText(page), "duplicated reply text").toEqual([]);
    });
    await s.grade("4:10 commit escalates, survives reload, approve, committed", at("commitCard"), at("turnEnded"), async () => {
      expect(seen.turnEnded, "the turn ended").toBeTruthy();
      if (!seen.commitCard) {
        s.observe("no approval card for the commit (no escalation)");
      } else {
        expect(seen.commitCardAfterReload, "the commit card survived the reload").toBeTruthy();
        expect(seen.commitApproved, "approved").toBeTruthy();
        if (!cards.some((c) => /git_commit/.test(c))) s.observe("the commit went through run_command (git commit), not the git_commit tool");
      }
      await page.getByRole("tab", { name: /^Changes/ }).click();
      await s.soft("the commit shows in Changes", () => expect(page.getByRole("tabpanel")).toContainText(/\d+ commits? this session/, { timeout: 15_000 }));
    });

    // -- 4:50 -------------------------------------------------------------------------------
    await s.beat("4:50 switch to GPT-5.5, review the diff", async () => {
      await pickModel(page, /^GPT-5\.5$/, "GPT-5.5");
      await expect(page.getByText(/Model changed to GPT-5\.5/)).toBeVisible({ timeout: 60_000 });
      await send(page, "Review the diff.");
      await expect(transcript.getByRole("button", { name: /^Reviewer/ }).first()).toBeVisible({ timeout: 300_000 });
      await expect(transcript.getByText(/^(Approve|Request changes)$/).first()).toBeVisible({ timeout: 420_000 });
      await waitIdle(page, 300_000);
    });

    // -- 5:40 -------------------------------------------------------------------------------
    await s.beat("5:40 harness console on the same session", async () => {
      const link = page.getByRole("navigation", { name: "Views" }).getByRole("link", { name: /Harness console/ });
      const href = await link.getAttribute("href");
      expect(href).toContain(`s=${encodeURIComponent(id)}`);
      // Another app: open it in its own context (same sign-in) so its requests stay out of
      // the product view's allowlist check.
      const ctx = await browser.newContext({ storageState: await page.context().storageState(), viewport: { width: 1920, height: 1080 } });
      try {
        const con = await ctx.newPage();
        const res = await con.goto(new URL(href!, env.baseA).toString());
        expect(res?.status()).toBe(200);
        // The console names the session by its title and shows the conversation (not the raw id).
        await s.soft("the console shows this session", () => expect(con.locator("body")).toContainText("Review the diff.", { timeout: 30_000 }));
        await con.screenshot({ path: join(s.dir, "06b-console.png") });
      } finally {
        await ctx.close();
      }
    });

    // -- 6:30 -------------------------------------------------------------------------------
    await s.beat("6:30 meters", async () => {
      await page.reload();
      await expect(page.getByText(DEMO_PROMPT)).toBeVisible({ timeout: 30_000 });
      const frames = await usageFrames(page, env.baseA, id);
      const unconfirmed = frames.filter((f) => !f.confirmed).length;
      if (unconfirmed) s.observe(`${unconfirmed} x_usage frame(s) still unconfirmed`);
      const sum = (k: string) => frames.reduce((a, f) => a + (typeof f[k] === "number" ? (f[k] as number) : 0), 0);
      const providers = [...new Set(frames.map((f) => String(f.provider ?? "unknown")))];
      s.save({ tokens: { calls: frames.length, input: sum("input_tokens"), output: sum("output_tokens"), cacheRead: sum("cache_read_tokens"), providers } });
      test.info().annotations.push({ type: "tokens", description: `${frames.length} calls, ${sum("input_tokens")} in / ${sum("output_tokens")} out, cache read ${sum("cache_read_tokens")}, ${providers.join("+")}` });
      await expect(meter(page, "Session tokens")).toHaveText(expectedTokens(frames));
      await expect(meter(page, "Session tokens")).not.toHaveText(/^0 in/);
      await expect(meter(page, "Input read from provider cache")).toHaveText(/^\d+%$/);
      await expect(meter(page, "Recoveries")).toHaveText("1");
      await expect(page.getByText("Work kept after worker loss (this session)")).toBeVisible();
    });
  } finally {
    test.info().annotations.push({ type: "total", description: `${s.now().toFixed(1)} s, session ${id}` });
    s.save({ seen });
  }
  expect(s.failed.map((r) => `${r.beat}: ${r.error}`), "beats that couldn't be shown").toEqual([]);
});
