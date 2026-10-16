// The meters strip under the header (PRD U5, owner decision 4a). Session totals over every
// model call of every agent and provider, from our x_usage frames; recoveries from
// x_recovery. A count no provider reported reads "not reported", never 0.

import type { SessionTelemetry, UsageTotals } from "../session";
import { CHECKING, NOT_REPORTED, formatTokens, percentOrNotReported, plural, tokensOrNotReported } from "./format";

const PROVIDER_LABELS: Record<string, string> = { anthropic: "Anthropic", openai: "OpenAI", google: "Google" };

export interface MeterValues {
  hero: string | null;
  heroConfirmed: boolean;
  tokens: string;
  tokensReported: boolean;
  cache: string;
  cacheReported: boolean;
  /** "good": some input came from cache (green, billed at a discount); "neutral": nothing
   *  did yet, or no calls yet; "nr": not reported. */
  cacheTone: "good" | "neutral" | "nr";
  /** Said after the figure when it needs it ("nothing read from cache yet"). */
  cacheNote: string | null;
  cacheByProvider: string;
  recoveries: string;
}

function tokensValue(t: UsageTotals): { text: string; reported: boolean } {
  if (t.calls === 0) return { text: "0 in / 0 out", reported: true };
  if (t.input === null && t.output === null) return { text: NOT_REPORTED, reported: false };
  return { text: `${tokensOrNotReported(t.input)} in / ${tokensOrNotReported(t.output)} out`, reported: true };
}

function cacheValue(t: UsageTotals): { text: string; reported: boolean; tone: MeterValues["cacheTone"]; note: string | null } {
  if (t.calls === 0) return { text: "no model calls yet", reported: true, tone: "neutral", note: null };
  const text = percentOrNotReported(t.cacheShare);
  if (text === NOT_REPORTED) return { text, reported: false, tone: "nr", note: null };
  if (!t.cacheRead) return { text, reported: true, tone: "neutral", note: "nothing read from cache yet" };
  return { text, reported: true, tone: "good", note: "billed at a discount" };
}

/** What the strip shows, as text, so the "not reported" rule is testable without a DOM. */
export function meterValues(telemetry: SessionTelemetry): MeterValues {
  const { totals, recoveryTotals } = telemetry;
  const tokens = tokensValue(totals);
  const cache = cacheValue(totals);
  let hero: string | null = null;
  if (recoveryTotals.count > 0) {
    const k = recoveryTotals.kept;
    hero = recoveryTotals.confirmed
      ? `${plural(k.modelCalls, "model call")} · ${formatTokens(k.tokensIn + k.tokensOut)} tokens · ${plural(k.toolCalls, "tool call")}`
      : CHECKING;
  }
  const cacheByProvider = Object.entries(telemetry.byProvider)
    .map(([p, t]) => `${PROVIDER_LABELS[p] ?? p}: ${percentOrNotReported(t.cacheShare)}`)
    .join(" · ");
  return {
    hero,
    heroConfirmed: recoveryTotals.confirmed,
    tokens: tokens.text,
    tokensReported: tokens.reported,
    cache: cache.text,
    cacheReported: cache.reported,
    cacheTone: cache.tone,
    cacheNote: cache.note,
    cacheByProvider,
    recoveries: String(recoveryTotals.count),
  };
}

export function MetersStrip({ telemetry, modelLabel }: { telemetry: SessionTelemetry; modelLabel: string | null }) {
  const v = meterValues(telemetry);
  const cacheClass = v.cacheTone === "good" ? "g" : v.cacheTone === "nr" ? "nr" : "neutral";
  return (
    <>
      <div className="meters meters-full">
        <dl className="meter-list" aria-label="Session meters">
          {v.hero !== null && (
            <div className="meter hero">
              <dt>Work kept after Worker loss (this session)</dt>
              <dd className={v.heroConfirmed ? undefined : "checking"}>{v.hero}</dd>
            </div>
          )}
          <div className="meter">
            <dt>Session tokens</dt>
            <dd className={v.tokensReported ? undefined : "nr"}>{v.tokens}</dd>
          </div>
          <div className="meter" title={v.cacheByProvider || undefined}>
            <dt>Input read from provider cache</dt>
            <dd className={cacheClass}>{v.cache}</dd>
            {v.cacheNote && <dd className="sub">{v.cacheTone === "neutral" ? `· ${v.cacheNote}` : v.cacheNote}</dd>}
          </div>
          <div className="meter">
            <dt>Recoveries</dt>
            <dd>{v.recoveries}</dd>
          </div>
        </dl>
        <span className="spacer" />
        {modelLabel && (
          <span className="model" title={modelLabel}>
            {modelLabel}
          </span>
        )}
        {/* Mounted from the start, so the first recovery is announced. */}
        <span className="sr-only" aria-live="polite">
          {v.hero !== null ? `Work kept after Worker loss: ${v.hero}` : ""}
        </span>
      </div>
      <div className="meters-compact" role="group" aria-label="Session meters">
        {v.hero !== null && (
          <span>
            Kept <b className="g">{v.hero}</b>
          </span>
        )}
        <span>
          <b className={v.tokensReported ? undefined : "nr"}>{v.tokens}</b>
        </span>
        <span>
          <b className={cacheClass}>{v.cache}</b>
          {v.cacheTone !== "neutral" || v.cacheNote ? " cached" : ""}
        </span>
        <span>
          <b>{v.recoveries}</b> {v.recoveries === "1" ? "recovery" : "recoveries"}
        </span>
      </div>
    </>
  );
}
