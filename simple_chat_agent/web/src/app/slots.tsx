// Stand-ins for Eng W's slot components until web/src/{workbench,decisions,recovery} land.
// Same names and props as agreed in session/types.ts; the shell imports them from here, so
// switching to the real ones is a change to the three exports below.

import { useState } from "react";
import type { DecisionDockProps, WorkbenchProps, WorkerBannerProps } from "../session";

const TABS = ["Plan", "Changes", "Terminal", "Artifacts"] as const;

export function Workbench({ session, variant }: WorkbenchProps) {
  const [tab, setTab] = useState<(typeof TABS)[number]>("Plan");
  const plan = session.plan;
  const done = plan?.items.filter((i) => i.status === "done").length ?? 0;
  const counts: Record<string, string> = {
    Plan: plan ? `${done}/${plan.items.length}` : "",
    Changes: session.changes ? String(session.changes.files.length) : "",
  };
  return (
    <section className="bench" aria-label="Workbench" data-variant={variant}>
      <div className="tabs" role="tablist">
        {TABS.map((t) => (
          <button key={t} type="button" role="tab" className="tab" aria-selected={tab === t} onClick={() => setTab(t)}>
            {t}
            {counts[t] && <span className="n">{counts[t]}</span>}
          </button>
        ))}
      </div>
      <div className="bench-body" role="tabpanel">
        {tab === "Plan" && plan && (
          <ul style={{ margin: 0, paddingLeft: "1.2em" }}>
            {plan.items.map((i, n) => (
              <li key={n}>
                {i.text} <span className="hint">({i.status.replace("_", " ")})</span>
              </li>
            ))}
          </ul>
        )}
        {tab === "Changes" &&
          session.changes?.files.map((f) => (
            <div key={f.path} className="mono" style={{ fontSize: ".8em" }}>
              {f.path} <span className="hint">+{f.added} −{f.removed}</span>
            </div>
          ))}
        {tab === "Terminal" && session.workspace?.last_command && (
          <div className="mono" style={{ fontSize: ".8em" }}>
            $ {session.workspace.last_command}{" "}
            <span className="hint">exit {session.workspace.last_exit_code ?? "…"}</span>
          </div>
        )}
      </div>
    </section>
  );
}

export function DecisionDock({ session }: DecisionDockProps) {
  const waiting = session.pendingApprovals;
  if (waiting.length === 0) return null;
  const ids = waiting.map((w) => w.part.toolId);
  return (
    <div className="decision-card" role="region" aria-label="Waiting for your approval">
      <div className="top-line">
        <span className="badge edit">{waiting.length} need approval</span>
        <span>{waiting.map((w) => w.part.toolName).join(", ")}</span>
      </div>
      <div className="acts">
        <button type="button" className="btn green small" onClick={() => void session.approve(ids)}>
          Approve all {waiting.length}
        </button>
        <button type="button" className="btn ghost small" onClick={() => void session.decline(ids)}>
          Decline all
        </button>
      </div>
    </div>
  );
}

export function WorkerBanner({ session }: WorkerBannerProps) {
  if (session.status !== "worker_unresponsive") return null;
  return (
    <div className="banner" role="status">
      <span className="t">Worker isn't answering</span>
      <span className="d">Your messages still land in Temporal and apply when a worker returns.</span>
    </div>
  );
}
