import { useState } from "react";
import { useTranscript } from "./context";
import { thinkingSeconds } from "../session/timings";

/** "Thought for 12s · 1,204 reasoning tokens", collapsed; opens to the summary text. Tokens
 *  show only when the provider reports them; with no summary text the row says so. */
export function Thinking({ agentId, messageId, callIndex, model, text, thoughtTokens, running }: {
  agentId: string;
  messageId: string;
  callIndex: number;
  model: string | null;
  text: string;
  thoughtTokens: number | null;
  running: boolean;
}) {
  const { session, providerOfModel } = useTranscript();
  const [open, setOpen] = useState(false);
  const seconds = thinkingSeconds(session.modelCallTiming(agentId, messageId, callIndex));
  const head = running && !text ? "Thinking…" : seconds !== null ? `Thought for ${seconds}s` : "Thought";
  const provider = providerOfModel(model);
  const details: string[] = [];
  if (thoughtTokens !== null && thoughtTokens > 0) details.push(`${thoughtTokens.toLocaleString("en-US")} reasoning tokens`);
  if (!text && !running) details.push(`Reasoning summary not provided by ${provider ?? "the provider"}`);
  const detail = details.join(" · ");
  return (
    <div className="thinking-wrap">
      <button type="button" className="thinking" aria-expanded={open} onClick={() => setOpen((v) => !v)} disabled={!text}>
        <span className="chev" aria-hidden="true">
          {open ? "▾" : "▸"}
        </span>
        <b>{head}</b>
        {detail && <span> · {detail}</span>}
      </button>
      {open && text && <div className="thought-text">{text}</div>}
    </div>
  );
}
