// Number formatting for the meters. A null count is "not reported", never 0 (PRD U5).

export const NOT_REPORTED = "not reported";
export const CHECKING = "Checking…";

/** 950 → "950", 41_234 → "41.2k", 1_250_000 → "1.25M". */
export function formatTokens(n: number): string {
  const abs = Math.abs(n);
  if (abs < 1000) return String(Math.round(n));
  if (abs < 1_000_000) {
    const k = n / 1000;
    return `${k >= 100 ? Math.round(k) : trim(k.toFixed(1))}k`;
  }
  const m = n / 1_000_000;
  return `${m >= 100 ? Math.round(m) : trim(m.toFixed(2))}M`;
}

function trim(s: string): string {
  return s.includes(".") ? s.replace(/\.?0+$/, "") : s;
}

/** A nullable token count: the formatted number, or "not reported". */
export function tokensOrNotReported(n: number | null): string {
  return n === null ? NOT_REPORTED : formatTokens(n);
}

/** A 0..1 share as a whole percent, or "not reported". */
export function percentOrNotReported(share: number | null): string {
  if (share === null || !Number.isFinite(share)) return NOT_REPORTED;
  return `${Math.round(share * 100)}%`;
}

export function plural(n: number, one: string, many = `${one}s`): string {
  return `${n.toLocaleString("en-US")} ${n === 1 ? one : many}`;
}
