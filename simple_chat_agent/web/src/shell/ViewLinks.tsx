import { fillLink, type AgentConfig, type Me } from "../session";

// The header's three views (PRD U7). Each carries its caption. A view that is off here is
// shown disabled with the reason.

export const NO_SESSION_REASON = "Open a session to link its Workflow.";

interface ViewDef {
  name: string;
  caption: string;
  href: string | null;
  current?: boolean;
  reason?: string | null;
}

export function viewDefs(config: AgentConfig | null, me: Me | null, sessionId: string | null): ViewDef[] {
  const links = config?.links;
  const consoleHref = links?.console
    ? fillLink(links.console, sessionId) ?? (links.console.enabled ? "/console/" : null)
    : "/console/";
  // Enabled in config, but the link names one Workflow: with no session open there is none.
  const temporalOn = links?.temporal_ui ? links.temporal_ui.enabled : me?.temporal_ui_workflows_url != null;
  const temporalHref = links?.temporal_ui ? fillLink(links.temporal_ui, sessionId) : me?.temporal_ui_workflows_url ?? null;
  const temporalReason = temporalOn
    ? sessionId
      ? null
      : NO_SESSION_REASON
    : links?.temporal_ui?.reason ?? "The Temporal UI isn't linked in this environment.";
  return [
    { name: "Product view", caption: "What your users see", href: null, current: true },
    {
      name: "Harness console",
      caption: "The agent loop as the harness records it",
      href: consoleHref,
      reason: links?.console?.reason ?? "The harness console isn't available here.",
    },
    {
      name: "Temporal UI",
      caption: "The Workflow history",
      href: temporalHref,
      reason: temporalReason,
    },
  ];
}

export function ViewLinks({ views, className = "views" }: { views: ViewDef[]; className?: string }) {
  return (
    <nav className={className} aria-label="Views">
      {views.map((v) => {
        if (v.current) {
          return (
            <span key={v.name} className="view on" aria-current="page">
              <span className="v">{v.name}</span>
              <span className="c">{v.caption}</span>
            </span>
          );
        }
        if (!v.href) {
          return (
            <span key={v.name} className="view off" aria-disabled="true" title={v.reason ?? undefined} tabIndex={0}>
              <span className="v">{v.name}</span>
              <span className="c">{v.reason ?? v.caption}</span>
            </span>
          );
        }
        return (
          <a key={v.name} className="view" href={v.href} target="_blank" rel="noopener noreferrer">
            <span className="v">
              {v.name} <span aria-hidden="true">↗</span>
              <span className="sr-only"> (opens in a new tab)</span>
            </span>
            <span className="c">{v.caption}</span>
          </a>
        );
      })}
    </nav>
  );
}
