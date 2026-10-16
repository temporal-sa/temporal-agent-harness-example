// Shared fixtures. Every test, in both projects, logs the requests its pages make and fails if
// one is off the allowlist (check 3c), so the whole suite (and the live demo script) is the
// allowlist run, not only the dedicated test.

import { test as base, expect, type BrowserContext, type Locator, type Page } from "@playwright/test";

import { judge, type SeenRequest, type Verdict } from "./allowlist";
import { MockApi, type MockOptions } from "./mock-api";

export interface RequestLog {
  seen: SeenRequest[];
  verdicts(): Verdict[];
  /** Turn the end-of-test check off (only for a test that probes a foreign URL on purpose). */
  skipCheck(): void;
  /** Log another context's requests too (a second user's browser). */
  watch(context: BrowserContext): void;
}

interface Fixtures {
  requestLog: RequestLog;
  mockApi: (opts?: MockOptions) => Promise<MockApi>;
}

export const test = base.extend<Fixtures>({
  requestLog: [
    async ({ context, baseURL }, use, testInfo) => {
      const seen: SeenRequest[] = [];
      let check = true;
      const watch = (c: BrowserContext) => c.on("request", (r) => seen.push({ method: r.method(), url: r.url(), resourceType: r.resourceType() }));
      watch(context);
      // The app's own origin(s): the mock server, or the live APIs (A and B).
      const origins = () =>
        [baseURL, process.env.E2E_LIVE_BASE_URL, process.env.E2E_LIVE_BASE_URL_B].filter((x): x is string => !!x).map((x) => new URL(x).origin);
      const log: RequestLog = {
        seen,
        verdicts: () => seen.map((r) => judge(r, origins())),
        skipCheck: () => {
          check = false;
        },
        watch,
      };
      await use(log);
      const verdicts = log.verdicts();
      await testInfo.attach("requests.json", {
        body: JSON.stringify(
          verdicts.map((v) => ({ method: v.request.method, url: v.request.url, route: v.allowed?.route ?? null, problem: v.reason })),
          null,
          2,
        ),
        contentType: "application/json",
      });
      if (check) {
        const bad = verdicts.filter((v) => !v.allowed).map((v) => `${v.request.method} ${v.request.url} (${v.reason})`);
        expect(bad, "requests off the allowlist (check 3c)").toEqual([]);
      }
    },
    { auto: true },
  ],

  mockApi: async ({ page, request, baseURL }, use) => {
    await use(async (opts?: MockOptions) => new MockApi(page, request, baseURL!, opts).install());
  },
});

export { expect };

// -- check 3b: time from a click to the acknowledged state, measured in the page ---------------

export interface AckSpec {
  /** CSS selector of the element(s) to watch; the first match whose text matches wins. */
  selector: string;
  /** Text the element must contain (substring), or a regex source with `regex: true`. */
  text: string;
  regex?: boolean;
}

export interface AckResult {
  /** From the click event to the DOM change. */
  domMs: number;
  /** From the click event to the next animation frame after the change (≈ painted). */
  paintMs: number;
}

/** Arm the page: the next click starts the clock; the first DOM state matching `spec` after it
 *  stops it. Uses performance.now() in the page. */
async function arm(page: Page, spec: AckSpec): Promise<void> {
  await page.evaluate((s) => {
    const w = window as unknown as { __ack?: { t0: number | null; t1: number | null; t2: number | null } };
    const state = { t0: null as number | null, t1: null as number | null, t2: null as number | null };
    w.__ack = state;
    const re = s.regex ? new RegExp(s.text) : null;
    const matches = () =>
      Array.from(document.querySelectorAll(s.selector)).some((el) => {
        const t = el.textContent ?? "";
        return re ? re.test(t) : t.includes(s.text);
      });
    const observer = new MutationObserver(() => {
      if (state.t0 === null || state.t1 !== null) return;
      if (!matches()) return;
      state.t1 = performance.now();
      observer.disconnect();
      requestAnimationFrame(() => {
        state.t2 = performance.now();
      });
    });
    document.addEventListener(
      "click",
      () => {
        state.t0 = performance.now();
        observer.observe(document, { subtree: true, childList: true, characterData: true, attributes: true });
        // Already true on the same task (synchronous render)?
        queueMicrotask(() => {
          if (state.t1 === null && matches()) {
            state.t1 = performance.now();
            observer.disconnect();
            requestAnimationFrame(() => {
              state.t2 = performance.now();
            });
          }
        });
      },
      { capture: true, once: true },
    );
  }, spec);
}

/** Click `target` and measure until `spec` holds. */
export async function measureAck(page: Page, target: Locator, spec: AckSpec): Promise<AckResult> {
  await target.waitFor({ state: "visible" });
  await arm(page, spec);
  await target.click();
  const handle = await page.waitForFunction(
    () => {
      const a = (window as unknown as { __ack?: { t0: number | null; t1: number | null; t2: number | null } }).__ack;
      return a && a.t0 !== null && a.t2 !== null ? { t0: a.t0, t1: a.t1!, t2: a.t2 } : null;
    },
    undefined,
    { timeout: 5000 },
  );
  const r = (await handle.jsonValue()) as { t0: number; t1: number; t2: number };
  return { domMs: r.t1 - r.t0, paintMs: r.t2 - r.t0 };
}
