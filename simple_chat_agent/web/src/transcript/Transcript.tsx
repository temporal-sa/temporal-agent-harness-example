// The transcript (PRD U2): one centered column of the root agent's messages, in stream order.
// Chat messages show the user's text and everything the agent did for it; control messages
// (model, posture, steer, stop) show as short lines.

import { useLayoutEffect, useRef, useState } from "react";
import type { CodingSession, Message, SendRecord, TurnRecovery } from "../session";
import { RecoveryDivider, SendStateTrail } from "../app/slots";
import { TranscriptContext, useTranscript, type TranscriptContextValue } from "./context";
import { buildItems, type Item } from "./items";
import { StreamingMarkdown } from "./Markdown";
import { ErrorNote } from "./ErrorNote";
import { errorCopy } from "../session/errors";
import { settingsAck } from "../session/derive";
import { recordForUnsent } from "../session/sendStates";
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

/** The files attached to a chat message (ChatMessage.attachments). */
export function attachmentsOf(m: Pick<Message, "input">): { id: string; name: string }[] {
  const input = m.input as unknown;
  const list = isObj(input) && Array.isArray(input.attachments) ? input.attachments : [];
  return list
    .filter((a): a is Record<string, unknown> => isObj(a) && typeof a.attachment_id === "string")
    .map((a) => ({ id: String(a.attachment_id), name: typeof a.name === "string" && a.name ? a.name : String(a.attachment_id) }));
}

function AttachmentChips({ message }: { message: Message }) {
  const files = attachmentsOf(message);
  if (files.length === 0) return null;
  return (
    <ul className="msg-files" aria-label="Attached files">
      {files.map((f) => (
        <li key={f.id} className="path">
          {f.name}
        </li>
      ))}
    </ul>
  );
}

function UserMessage({ message, record }: { message: Message; record: SendRecord | undefined }) {
  const showTrail = record && record.phase !== "done" && record.phase !== "running";
  return (
    <div className="user-turn">
      <div className="msg-user">
        {inputText(message)}
        <AttachmentChips message={message} />
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
      return <Folded label={`Attempt ${item.attempt} (Worker lost) · ${item.words} words of partial reply, replaced below`} text={item.text} />;
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
        <ErrorNote alert text="The agent couldn't finish this message. Its work so far is kept in Temporal; send it again or ask for something else." detail={message.error} />
      )}
    </>
  );
}

/** The agent refused a model or posture change: its reason, and that nothing changed. */
export function refusedLine(setting: "model" | "posture", detail: string): string {
  const what = setting === "model" ? "The model didn't change." : "Approvals didn't change.";
  return detail ? `${what} ${detail}` : what;
}

/** The model-change line: "applies from the next turn" until a later turn has started. */
export function modelLine(name: string, failed: boolean, nextTurnStarted: boolean): string {
  if (failed) return `Couldn't change the model to ${name}.`;
  return `Model changed to ${name} · ${nextTurnStarted ? "applies from this turn" : "applies from the next turn"}`;
}

function ControlLine({ message, nextTurnStarted }: { message: Message; nextTurnStarted: boolean }) {
  const { session, modelLabel, postureLabel } = useTranscript();
  const input = (isObj(message.input) ? message.input : {}) as Record<string, unknown>;
  const record = session.sendFor({ messageId: message.id });
  const failed = message.status === "error";
  switch (message.handler) {
    case "set_model": {
      const m = isObj(input.model) ? input.model : {};
      const name = modelLabel(String(m.provider ?? ""), String(m.model ?? ""));
      const ack = settingsAck(message);
      if (ack && !ack.ok) return <div className="sys-line">{refusedLine("model", ack.detail)}</div>;
      return (
        <div className="sys-line" title={failed ? message.error ?? undefined : undefined}>
          {modelLine(name, failed, nextTurnStarted)}
        </div>
      );
    }
    case "set_posture": {
      const ack = settingsAck(message);
      if (ack && !ack.ok) return <div className="sys-line">{refusedLine("posture", ack.detail)}</div>;
      return (
        <div className="sys-line">
          {failed ? "Couldn't change approvals. The previous setting still applies." : `Approvals: ${postureLabel(String(input.posture ?? ""))}`}
        </div>
      );
    }
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
    case "forget_always_allowed": {
      const out = isObj(message.output) ? message.output : null;
      const detail = typeof out?.detail === "string" ? out.detail : null;
      const text = failed
        ? "Couldn't clear always-approved tools. They still apply."
        : message.status === "done"
          ? detail ?? "Always approve cleared."
          : "Clearing always-approved tools…";
      return <div className="sys-line">{text}</div>;
    }
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
      {session.messages.map((m, i) => {
        if (m.handler !== "chat") {
          const nextTurnStarted = session.messages.slice(i + 1).some((n) => n.handler === "chat" && (n.startedAt !== null || n.status === "running" || n.status === "done"));
          return <ControlLine key={m.id} message={m} nextTurnStarted={nextTurnStarted} />;
        }
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

/** "Not sent." and why, from the send's error code and status (UX verification 3). */
export function notSentText(record: SendRecord | undefined, message: Pick<Message, "error">): string {
  const why = errorCopy(
    record ? { code: record.errorCode, status: record.errorStatus, message: record.error } : { message: message.error },
    "Check your connection and try again.",
  );
  return `Not sent. ${why.text}`;
}

function UnsentMessage({ message, session }: { message: Message; session: CodingSession }) {
  const record = recordForUnsent(session.sends, message);
  return (
    <div className="user-turn">
      <div className={`msg-user${message.handler === "steer" ? " steer" : ""}`}>
        {inputText(message)}
        <AttachmentChips message={message} />
      </div>
      {message.status === "error" ? (
        <div className="notice err" role="alert">
          <span title={record?.error ?? message.error ?? undefined}>{notSentText(record, message)}</span>
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
export function TranscriptPane({ value }: { value: TranscriptContextValue }) {
  const scroller = useRef<HTMLDivElement>(null);
  const content = useRef<HTMLDivElement>(null);
  const [following, setFollowing] = useState(true);
  const followingRef = useRef(true);
  // Our own scrolls fire scroll events too; only the user's scrolling changes "following".
  const ours = useRef(false);

  const pin = () => {
    const el = scroller.current;
    if (!el || !followingRef.current) return;
    const bottom = el.scrollHeight - el.clientHeight;
    if (Math.abs(el.scrollTop - bottom) < 1) return;
    ours.current = true;
    el.scrollTop = bottom;
  };

  // New frames, and any growth of the column (smoothed typing, a card opening, a row
  // expanding) or of the pane itself (the decision card taking room): follow to the bottom.
  useLayoutEffect(pin);
  useLayoutEffect(() => {
    const el = scroller.current;
    const col = content.current;
    if (!el || !col || typeof ResizeObserver === "undefined") return;
    const ro = new ResizeObserver(() => pin());
    ro.observe(col);
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  function onScroll() {
    const el = scroller.current;
    if (!el) return;
    if (ours.current) {
      ours.current = false;
      return;
    }
    const atBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 8;
    followingRef.current = atBottom;
    if (atBottom !== following) setFollowing(atBottom);
  }

  return (
    <TranscriptContext.Provider value={value}>
      <div className="transcript-slot" data-slot="transcript" ref={scroller} onScroll={onScroll} aria-live="off">
        <div className="col" ref={content}>
          <Transcript />
        </div>
        {!following && (
          <button
            type="button"
            className="btn secondary small jump"
            onClick={() => {
              followingRef.current = true;
              setFollowing(true);
              pin();
            }}
          >
            Jump to latest
          </button>
        )}
      </div>
    </TranscriptContext.Provider>
  );
}
