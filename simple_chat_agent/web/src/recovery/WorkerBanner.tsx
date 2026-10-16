// "Worker isn't answering" (PRD U6). Yellow, never red: nothing has failed, and Temporal
// keeps everything until a worker returns.

import { useEffect, useState } from "react";

import { api, type AgentConfig, type WorkerBannerProps as SlotProps } from "../session";
import { bannerView, temporalUiUrl } from "./banner";
import "./recovery.css";

export interface WorkerBannerProps extends SlotProps {
  /** The Temporal UI url template (`config.links.temporal_ui.url_template`). When omitted,
   *  the banner reads it from GET /api/agent/config once. */
  temporalUiTemplate?: string | null;
}

let configOnce: Promise<AgentConfig | null> | null = null;
function loadConfig(): Promise<AgentConfig | null> {
  configOnce ??= api.getAgentConfig().catch(() => {
    configOnce = null;
    return null;
  });
  return configOnce;
}

function useTemporalUiTemplate(given: string | null | undefined, active: boolean): string | null {
  const [fetched, setFetched] = useState<string | null>(null);
  useEffect(() => {
    if (given !== undefined || !active) return;
    let live = true;
    void loadConfig().then((c) => {
      const link = c?.links?.temporal_ui;
      if (live) setFetched(link?.enabled ? (link.url_template ?? null) : null);
    });
    return () => {
      live = false;
    };
  }, [given, active]);
  return given !== undefined ? given : fetched;
}

export function WorkerBanner({ session, temporalUiTemplate }: WorkerBannerProps) {
  const v = bannerView(session);
  const template = useTemporalUiTemplate(temporalUiTemplate, v.show);
  if (!v.show) return null;
  const href = temporalUiUrl(template, v.workflowId);
  return (
    <section className="rc-banner" role="status" aria-label="Worker isn't answering">
      <span className="ic" aria-hidden="true">
        !
      </span>
      <span className="t">Worker isn't answering</span>
      {href ? (
        <a href={href} target="_blank" rel="noopener noreferrer">
          Open the pending Activity in Temporal UI ↗
        </a>
      ) : (
        <span />
      )}
      <p className="d">
        {v.lead}{" "}
        {v.saved && (
          <>
            <b>Saved in Temporal:</b> {v.saved}.{" "}
          </>
        )}
        <b>Waiting:</b> {v.waiting}
      </p>
    </section>
  );
}
