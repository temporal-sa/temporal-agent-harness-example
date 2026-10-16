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
  cacheByProvider: string;
  recoveries: string;
}

function tokensValue(t: UsageTotals): { text: string; reported: boolean } {
  if (t.calls === 0) return { text: "0 in / 0 out", reported: true };
  if (t.input === null && t.output === null) return { text: NOT_REPORTED, reported: false };
  return { text: `${tokensOrNotReported(t.input)} in / ${tokensOrNotReported(t.output)} out`, reported: true };
}

function cacheValue(t: UsageTotals): { text: string; reported: boolean } {
  if (t.calls === 0) return { text: "–", reported: true };
  const text = percentOrNotReported(t.cacheShare);
  return { text, reported: text !== NOT_REPORTED };
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
    cacheByProvider,
    recoveries: String(recoveryTotals.count),
  };
}

export function MetersStrip({ telemetry, modelLabel }: { telemetry: SessionTelemetry; modelLabel: string | null }) {
  const v = meterValues(telemetry);
  return (
    <>
      <div className="meters meters-full">
      <dl className="meter-list" aria-label="Session meters">
        {v.hero !== null && (
          <div className="meter hero">
            <dt>Work kept after worker loss (this session)</dt>
            <dd className={v.heroConfirmed ? undefined : "checking"} aria-live="polite">
              {v.hero}
            </dd>
          </div>
        )}
        <div className="meter">
          <dt>Session tokens</dt>
          <dd className={v.tokensReported ? undefined : "nr"}>{v.tokens}</dd>
        </div>
        <div className="meter" title={v.cacheByProvider || undefined}>
          <dt>Input read from provider cache</dt>
          <dd className={v.cacheReported ? "g" : "nr"}>{v.cache}</dd>
          {v.cacheReported && v.cache !== "–" && <dd className="sub">billed at a discount</dd>}
        </div>
        <div className="meter">
          <dt>Recoveries</dt>
          <dd>{v.recoveries}</dd>
        </div>
      </dl>
        <span className="spacer" />
        {modelLabel && <span className="model">{modelLabel}</span>}
      </div>
      <div className="meters-compact" aria-label="Session meters">
        {v.hero !== null && (
          <span>
            Kept <b className="g">{v.hero}</b>
          </span>
        )}
        <span>
          <b className={v.tokensReported ? undefined : "nr"}>{v.tokens}</b>
        </span>
        <span>
          <b className={v.cacheReported ? "g" : "nr"}>{v.cache}</b> cached
        </span>
        <span>
          <b>{v.recoveries}</b> {v.recoveries === "1" ? "recovery" : "recoveries"}
        </span>
      </div>
    </>
  );
}
