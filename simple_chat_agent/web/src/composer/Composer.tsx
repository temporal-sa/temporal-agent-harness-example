// Step 3.1 placeholder: send and stop, so the shell can be driven end to end. Step 3.2
// replaces it with the full composer (Steer now / Queue, model picker, posture menu,
// attachments).

import { useEffect, useState, type KeyboardEvent } from "react";
import type { CodingSession } from "../session";

export function Composer({ session }: { session: CodingSession }) {
  const [draft, setDraft] = useState("");
  const busy = session.agentStatus === "busy";

  useEffect(() => {
    if (!busy) return;
    const onKey = (e: globalThis.KeyboardEvent) => {
      if (e.key === "Escape") void session.interrupt();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [busy, session]);

  function submit() {
    const text = draft.trim();
    if (!text) return;
    setDraft("");
    void session.send(text);
  }

  function onKeyDown(e: KeyboardEvent<HTMLTextAreaElement>) {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      submit();
    }
  }

  return (
    <div className="box">
      <label className="sr-only" htmlFor="composer-input">
        Message
      </label>
      <textarea
        id="composer-input"
        rows={2}
        value={draft}
        placeholder={busy ? "Queue a message for after this turn…" : "Ask for a change…"}
        onChange={(e) => setDraft(e.target.value)}
        onKeyDown={onKeyDown}
      />
      <div className="bar">
        <span className="spacer" />
        {busy && (
          <button type="button" className="stop" onClick={() => void session.interrupt()}>
            Stop<kbd>Esc</kbd>
          </button>
        )}
        <button type="button" className="btn primary small" onClick={submit} disabled={!draft.trim()}>
          {busy ? "Queue" : "Send"}
        </button>
      </div>
    </div>
  );
}
