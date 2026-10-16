import type { SessionStatus } from "../session";

// The header's session status (wireframe note 1). Text always carries the meaning; color
// repeats it.
export const STATUS_TEXT: Record<SessionStatus, string> = {
  running: "Running",
  worker_unresponsive: "Worker isn't answering",
  waiting_for_you: "Waiting for you",
  idle: "Idle",
};

const STATUS_CLASS: Record<SessionStatus, string> = {
  running: "",
  worker_unresponsive: "wait",
  waiting_for_you: "you",
  idle: "idle",
};

export function StatusPill({ status }: { status: SessionStatus }) {
  return (
    <span className={`pill ${STATUS_CLASS[status]}`} role="status" aria-live="polite">
      <span className="sr-only">Session status: </span>
      {STATUS_TEXT[status]}
    </span>
  );
}

/** The sidebar's square for a session's status, with the status as its accessible name. */
export function StatusDot({ status, closed = false }: { status: SessionStatus | null; closed?: boolean }) {
  const cls = closed ? "closed" : status === "running" ? "run" : status === "worker_unresponsive" ? "wait" : status === "waiting_for_you" ? "you" : "";
  const label = closed ? "Ended" : status ? STATUS_TEXT[status] : "Not open";
  return <span className={`dot ${cls}`} role="img" aria-label={label} title={label} />;
}
