// Step 3.1 placeholder: user messages and the agent's reply text, so the shell can be driven
// end to end. Step 3.2 replaces this with the full transcript (thinking, tool rows, diffs,
// subagents, recovery dividers).

import type { CodingSession, Message, SendPhase } from "../session";

const PHASE_TEXT: Record<SendPhase, string> = {
  sending: "Sending",
  in_temporal: "In Temporal, waiting for a worker",
  accepted: "Accepted",
  running: "Running",
  done: "",
  failed: "Not sent",
};

function text(m: Message): string {
  const input = m.input as { text?: unknown };
  return typeof input.text === "string" ? input.text : "";
}

function reply(m: Message): string {
  const streamed = m.parts.map((p) => (p.type === "reply_delta" ? p.text : "")).join("");
  if (streamed) return streamed;
  const out = m.output as { text?: unknown } | null;
  return out && typeof out.text === "string" ? out.text : "";
}

function time(ms: number | null): string {
  if (ms === null) return "";
  const d = new Date(ms > 1e12 ? ms : ms * 1000);
  return d.toLocaleTimeString("en-GB", { hour12: false });
}

export function Transcript({ session }: { session: CodingSession }) {
  const shown = session.messages.filter((m) => m.handler === "chat");
  return (
    <>
      {shown.length === 0 && session.unsent.length === 0 && <p className="hint">Ask the agent to do something in its workspace.</p>}
      {shown.map((m) => {
        const send = session.sendFor({ messageId: m.id });
        const phase = send && send.phase !== "done" ? PHASE_TEXT[send.phase] : "";
        return (
          <div key={m.id} style={{ display: "grid", gap: ".6em" }}>
            <div className="msg-user">
              {text(m)}
              <span className="when">{phase || time(m.acceptedAt)}</span>
            </div>
            {reply(m) && <div className="answer">{reply(m)}</div>}
            {m.error && <div className="notice">{m.error}</div>}
          </div>
        );
      })}
      {session.unsent.map((m) => {
        const send = session.sendFor({ messageId: m.id });
        const label = m.status === "error" ? `Not sent: ${m.error ?? ""}` : PHASE_TEXT[send?.phase ?? "sending"] || "Sending";
        return (
          <div key={m.id} className="msg-user">
            {text(m)}
            <span className="when">{label}</span>
          </div>
        );
      })}
    </>
  );
}
