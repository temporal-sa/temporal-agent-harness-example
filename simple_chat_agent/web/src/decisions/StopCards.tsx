// The checkpoint and spend-cap cards, shown above the composer after the turn that hit them.

import { useState } from "react";

import type { CodingSession, Message, SendRecord } from "../session";
import { SendStateTrail } from "../recovery/SendStateTrail";
import "./decisions.css";

export type StopKind = "checkpoint" | "spend_cap";

/** The stop the latest finished chat turn reported, if no turn has started since. */
export function latestStop(messages: readonly Message[], agentStatus: "idle" | "busy"): { kind: StopKind; messageId: string } | null {
  if (agentStatus === "busy") return null;
  for (let i = messages.length - 1; i >= 0; i--) {
    const m = messages[i];
    if (!m || m.handler !== "chat") continue;
    if (m.status !== "done") return null;
    const out = m.output as { checkpoint?: unknown; error?: unknown } | null;
    if (out && typeof out.checkpoint === "string" && out.checkpoint) return { kind: "checkpoint", messageId: m.id };
    if (out && out.error === "SpendCapReached") return { kind: "spend_cap", messageId: m.id };
    return null;
  }
  return null;
}

export function StopCards({ session }: { session: CodingSession }) {
  const stop = latestStop(session.messages, session.agentStatus);
  const [sent, setSent] = useState<{ for: string; record: SendRecord | null } | null>(null);
  if (!stop) return null;

  if (stop.kind === "spend_cap") {
    return (
      <section className="dc-card hold" aria-label="Spend limit reached">
        <div className="top">
          <span className="badge prog">Stopped</span>
          <span className="t">This session reached its spend limit</span>
        </div>
        <p className="why">The turn stopped before the next model call. Start a new session, or try again tomorrow when the daily limit resets.</p>
      </section>
    );
  }

  const mine = sent?.for === stop.messageId ? sent : null;
  const record = mine?.record ? (session.sends.find((r) => r.id === mine.record!.id) ?? mine.record) : null;
  return (
    <section className="dc-card hold" aria-label="Checkpoint reached">
      <div className="top">
        <span className="badge prog">Checkpoint</span>
        <span className="t">Checkpoint reached. Continue?</span>
      </div>
      <p className="why">This turn is long enough that Temporal will start a fresh run for the session. Nothing is lost; the conversation carries over.</p>
      <div className="acts">
        <button
          type="button"
          className="btn primary small"
          disabled={!!mine}
          onClick={() => {
            setSent({ for: stop.messageId, record: null });
            session.send("continue").then(
              (r) => setSent({ for: stop.messageId, record: r }),
              () => setSent(null),
            );
          }}
        >
          Continue
        </button>
        {record && <SendStateTrail record={record} workerAnswering={session.telemetry.worker.answering} />}
      </div>
    </section>
  );
}
