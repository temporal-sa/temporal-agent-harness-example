import type { SessionStatus, SessionSummary } from "../session";
import { StatusDot } from "./StatusPill";

export interface SidebarProps {
  sessions: SessionSummary[] | null;
  loadError: string | null;
  currentId: string | null;
  currentStatus: SessionStatus | null;
  modelName(summary: SessionSummary): string;
  creating: "demo" | "empty" | null;
  onSelect(id: string): void;
  onNewDemo(): void;
  onNewEmpty(): void;
  footer: string;
}

/** "now", "5m ago", "2h ago", "yesterday", "Oct 14". */
export function relativeTime(epochSeconds: number, now = Date.now()): string {
  const diff = Math.max(0, now - epochSeconds * 1000);
  const min = Math.floor(diff / 60_000);
  if (min < 1) return "now";
  if (min < 60) return `${min}m ago`;
  const h = Math.floor(min / 60);
  if (h < 24) return `${h}h ago`;
  if (h < 48) return "yesterday";
  return new Date(epochSeconds * 1000).toLocaleDateString("en-US", { month: "short", day: "numeric" });
}

function rowStatus(s: SessionSummary, p: SidebarProps): SessionStatus | null {
  return s.session_id === p.currentId ? p.currentStatus : null;
}

export function Sidebar(p: SidebarProps) {
  return (
    <aside className="side" aria-label="Sessions">
      <button type="button" className="btn primary" onClick={p.onNewDemo} disabled={p.creating !== null}>
        {p.creating === "demo" ? "Setting up the demo…" : "+ New demo session"}
      </button>
      <button type="button" className="btn ghost" onClick={p.onNewEmpty} disabled={p.creating !== null}>
        {p.creating === "empty" ? "Starting…" : "New empty session"}
      </button>
      <SessionList {...p} />
      <div className="side-foot">{p.footer}</div>
    </aside>
  );
}

function SessionList(p: SidebarProps) {
  if (p.loadError) return <p className="side-error">{p.loadError}</p>;
  if (p.sessions === null) return <p className="side-empty">Loading sessions…</p>;
  if (p.sessions.length === 0) return <p className="side-empty">No sessions yet. Start a demo session to try the agent on a small repo.</p>;
  return (
    <ul className="sess">
      {p.sessions.map((s) => (
        <li key={s.session_id}>
          <button
            type="button"
            className="sess-row"
            aria-current={s.session_id === p.currentId ? "page" : undefined}
            onClick={() => p.onSelect(s.session_id)}
          >
            <StatusDot status={rowStatus(s, p)} closed={s.closed} />
            <span className="t">{s.title}</span>
            <small>
              {p.modelName(s)} · {relativeTime(s.created_at)}
            </small>
          </button>
        </li>
      ))}
    </ul>
  );
}

/** The 1366 and presenter-mode rail: new demo session, one square per session, and a button
 *  that opens the full list. */
export function Rail(p: SidebarProps & { onExpand(): void }) {
  return (
    <aside className="rail-side" aria-label="Sessions">
      <button type="button" className="btn primary rail-new" onClick={p.onNewDemo} disabled={p.creating !== null} aria-label="New demo session" title="New demo session">
        +
      </button>
      <ul className="sess">
        {(p.sessions ?? []).slice(0, 12).map((s) => (
          <li key={s.session_id}>
            <button
              type="button"
              className="rail-btn"
              aria-current={s.session_id === p.currentId ? "page" : undefined}
              aria-label={s.title}
              title={s.title}
              onClick={() => p.onSelect(s.session_id)}
            >
              <StatusDot status={rowStatus(s, p)} closed={s.closed} />
            </button>
          </li>
        ))}
      </ul>
      <button type="button" className="rail-btn" onClick={p.onExpand} aria-label="Show all sessions" title="Show all sessions">
        <span aria-hidden="true">☰</span>
      </button>
    </aside>
  );
}
