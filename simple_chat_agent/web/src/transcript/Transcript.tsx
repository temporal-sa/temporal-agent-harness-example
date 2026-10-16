// The transcript (PRD U2): one centered column of the root agent's messages, in stream order.
// Chat messages show the user's text and everything the agent did for it; control messages
// (model, posture, steer, stop) show as short lines.

import { useLayoutEffect, useRef, useState, type ReactNode } from "react";
import type { CodingSession, Message, SendRecord, TurnRecovery } from "../session";
import { RecoveryDivider, SendStateTrail } from "../app/slots";
import { TranscriptContext, useTranscript, type TranscriptContextValue } from "./context";
import { buildItems, type Item } from "./items";
import { StreamingMarkdown } from "./Markdown";
import { SubagentCard } from "./SubagentCard";
import { Thinking } from "./Thinking";
import { ToolGroup, ToolRow } from "./ToolRow";

const isObj = (v: unknown): v is Record<string, unknown> => typeof v === "object" && v !== null;

function inputText(m: Message): string {
  const input = m.input as unknown;
  return isObj(input) && typeof input.text === "string" ? input.text : "";
}

/** Seconds or ms since epoch to "14:02:11". */
export function clockTime(t: number | null | undefined): string {
  if (!t) return "";
  return new Date(t > 1e12 ? t : t * 1000).toLocaleTimeString("en-GB", { hour12: false });
}

function SendNote({ record }: { record: SendRecord | undefined }) {
  if (!record || record.phase === "done" || record.phase === "running") return null;
  return <Trail record={record} />;
}

function Trail({ record }: { record: SendRecord }) {
  const { session } = useTranscript();
  return <SendStateTrail record={record} workerAnswering={session.telemetry.worker.answering} />;
}

function UserMessage({ message, record }: { message: Message; record: SendRecord | undefined }) {
  const showTrail = record && record.phase !== "done" && record.phase !== "running";
  return (
    <div className="user-turn">
      <div className="msg-user">
        {inputText(message)}
        {!showTrail && <span className="when">{clockTime(message.acceptedAt)}</span>}
      </div>
      {showTrail && <SendNote record={record} />}
    </div>
  );
}

function ItemView({ item, message, agentId }: { item: Item; message: Message; agentId: string }) {
  switch (item.kind) {
    case "thinking":
      return (
        <Thinking
          agentId={agentId}
          messageId={message.id}
          callIndex={item.callIndex}
          model={item.model}
          text={item.text}
          thoughtTokens={item.thoughtTokens}
          running={item.running}
        />
      );
    case "text":
      return <StreamingMarkdown text={item.text} streaming={item.streaming} />;
    case "tool":
      return <ToolRow part={item.part} />;
    case "group":
      return <ToolGroup label={item.label} parts={item.parts} />;
    case "subagents":
      return (
        <div className={item.parts.length > 1 ? "pair" : undefined}>
          {item.parts.map((p) => (
            <SubagentCard key={`${p.subagentId}:${p.subagentTurn}`} part={p} />
          ))}
        </div>
      );
    case "attempt":
      return <Folded label={`Attempt ${item.attempt} (worker lost) · ${item.words} words of partial reply, replaced below`} text={item.text} />;
    case "discarded":
      return <Folded label={`Discarded: the reply after you pressed Stop (${item.words} words)`} text={item.text} />;
    case "recovery":
      return <RecoveryDivider recovery={item.recovery} />;
  }
}

function Folded({ label, text }: { label: string; text: string }) {
  const [open, setOpen] = useState(false);
  return (
    <div className="attempt-wrap">
      <button type="button" className="attempt" aria-expanded={open} onClick={() => setOpen((v) => !v)} disabled={!text}>
        <span className="chev" aria-hidden="true">
          {open ? "▾" : "▸"}
        </span>
        {label}
      </button>
      {open && <div className="attempt-text">{text}</div>}
    </div>
  );
}

function ChatTurn({ message, recoveries }: { message: Message; recoveries: TurnRecovery[] }) {
  const { session } = useTranscript();
  const output = isObj(message.output) ? message.output : null;
  const live = message.status === "running" || message.status === "accepted";
  const items = buildItems(message, { live, interrupted: output?.interrupted === true, recoveries });
  const hasText = items.some((i) => i.kind === "text");
  const finalText = typeof output?.text === "string" ? output.text : "";
  return (
    <>
      <UserMessage message={message} record={session.sendFor({ messageId: message.id })} />
      {items.map((item) => (
        <ItemView key={item.key} item={item} message={message} agentId={session.rootAgentId ?? ""} />
      ))}
      {/* A reply that never streamed (e.g. a checkpoint or an explained failure). */}
      {!hasText && finalText && <StreamingMarkdown text={finalText} streaming={false} />}
      {message.status === "error" && message.error && (
        <div className="notice err" role="alert">
          {message.error}
        </div>
      )}
    </>
  );
}

function ControlLine({ message }: { message: Message }) {
  const { session, modelLabel, postureLabel } = useTranscript();
  const input = (isObj(message.input) ? message.input : {}) as Record<string, unknown>;
  const record = session.sendFor({ messageId: message.id });
  const failed = message.status === "error";
  switch (message.handler) {
    case "set_model": {
      const m = isObj(input.model) ? input.model : {};
      const name = modelLabel(String(m.provider ?? ""), String(m.model ?? ""));
      return (
        <div className="sys-line">
          {failed ? `Couldn't change the model to ${name}: ${message.error ?? ""}` : `Model changed to ${name} · applies from the next turn`}
        </div>
      );
    }
    case "set_posture":
      return (
        <div className="sys-line">
          {failed ? `Couldn't change approvals: ${message.error ?? ""}` : `Approvals: ${postureLabel(String(input.posture ?? ""))}`}
        </div>
      );
    case "steer":
      return (
        <div className="user-turn">
          <div className="msg-user steer">
            <span className="hint">You steered the turn</span>
            <br />
            {typeof input.text === "string" ? input.text : ""}
          </div>
          <SendNote record={record} />
        </div>
      );
    case "interrupt":
      return <div className="sys-line">You pressed Stop</div>;
    default:
      return null;
  }
}

export function Transcript() {
  const { session } = useTranscript();
  const recoveriesByTurn = new Map<number, TurnRecovery[]>();
  for (const r of session.telemetry.turnRecoveries) {
    if (r.agentId !== session.rootAgentId || r.turnNumber === null) continue;
    const list = recoveriesByTurn.get(r.turnNumber) ?? [];
    list.push(r);
    recoveriesByTurn.set(r.turnNumber, list);
  }
  const placed = new Set<number>();
  return (
    <>
      {session.messages.length === 0 && session.unsent.length === 0 && (
        <p className="hint">Ask the agent to do something in its workspace.</p>
      )}
      {session.messages.map((m) => {
        if (m.handler !== "chat") return <ControlLine key={m.id} message={m} />;
        let recs: TurnRecovery[] = [];
        if (m.turnNumber !== null && !placed.has(m.turnNumber)) {
          recs = recoveriesByTurn.get(m.turnNumber) ?? [];
          placed.add(m.turnNumber);
        }
        return <ChatTurn key={m.id} message={m} recoveries={recs} />;
      })}
      {session.unsent.map((m) => (
        <UnsentMessage key={m.id} message={m} session={session} />
      ))}
    </>
  );
}

function UnsentMessage({ message, session }: { message: Message; session: CodingSession }) {
  const record = session.sendFor({ messageId: message.id });
  return (
    <div className="user-turn">
      <div className={`msg-user${message.handler === "steer" ? " steer" : ""}`}>{inputText(message)}</div>
      {message.status === "error" ? (
        <div className="notice err" role="alert">
          Not sent: {message.error}
          <button type="button" className="btn ghost small" onClick={() => session.dismissUnsent(message.id)}>
            Dismiss
          </button>
        </div>
      ) : (
        record && <Trail record={record} />
      )}
    </div>
  );
}

/** The scrolling column. Follows new content unless the user has scrolled up, and then
 *  offers a way back. */
export function TranscriptPane({ value, children }: { value: TranscriptContextValue; children?: ReactNode }) {
  const scroller = useRef<HTMLDivElement>(null);
  const [following, setFollowing] = useState(true);
  const followingRef = useRef(true);

  useLayoutEffect(() => {
    const el = scroller.current;
    if (el && followingRef.current) el.scrollTop = el.scrollHeight;
  });

  function onScroll() {
    const el = scroller.current;
    if (!el) return;
    const atBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 80;
    followingRef.current = atBottom;
    if (atBottom !== following) setFollowing(atBottom);
  }

  return (
    <TranscriptContext.Provider value={value}>
      <div className="transcript-slot" data-slot="transcript" ref={scroller} onScroll={onScroll} aria-live="off">
        <div className="col">
          {children}
          <Transcript />
        </div>
        {!following && (
          <button
            type="button"
            className="btn secondary small jump"
            onClick={() => {
              followingRef.current = true;
              setFollowing(true);
              scroller.current?.scrollTo({ top: scroller.current.scrollHeight });
            }}
          >
            Jump to latest
          </button>
        )}
      </div>
    </TranscriptContext.Provider>
  );
}
