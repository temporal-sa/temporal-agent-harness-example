// Small formatters shared by the workbench, decisions and recovery views.

/** 14:02:11 (24-hour, local time). Accepts ms or seconds since the epoch. */
export function hms(t: number | null | undefined): string | null {
  if (t === null || t === undefined || !Number.isFinite(t)) return null;
  const d = new Date(t > 1e12 ? t : t * 1000);
  return d.toLocaleTimeString("en-GB", { hour12: false });
}

/** 950 → "950", 18_240 → "18.2k", 1_250_000 → "1.25M". */
export function tokens(n: number): string {
  const abs = Math.abs(n);
  if (abs < 1000) return String(Math.round(n));
  const trim = (s: string) => (s.includes(".") ? s.replace(/\.?0+$/, "") : s);
  if (abs < 1_000_000) {
    const k = n / 1000;
    return `${k >= 100 ? Math.round(k) : trim(k.toFixed(1))}k`;
  }
  const m = n / 1_000_000;
  return `${m >= 100 ? Math.round(m) : trim(m.toFixed(2))}M`;
}

/** "1 model call", "6 model calls". */
export function count(n: number, one: string, many = `${one}s`): string {
  return `${n.toLocaleString("en-US")} ${n === 1 ? one : many}`;
}
