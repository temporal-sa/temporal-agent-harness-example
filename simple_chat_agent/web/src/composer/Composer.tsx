// The composer (PRD U4, U7; owner decision C). Send, or Stop (Esc) while a turn runs; text
// typed mid-turn offers "Steer now" or "Queue". The model picker applies from the next turn.
// The posture menu decides which tool calls ask at all. "+ File" uploads to the session and
// mentions the file in the next message. Every control changes state on the click itself, so
// it acknowledges well within 100 ms; the send states follow from the facade.

import { useEffect, useRef, useState, type KeyboardEvent } from "react";
import type { AgentConfig, Attachment, CodingSession, ModelChoice, Posture, SendRecord } from "../session";
import { Menu } from "./Menu";
import {
  alwaysApprovedText,
  choiceLabel,
  effortsFor,
  modelName,
  pickerState,
  postureLabel,
  postureOptions,
  providerName,
} from "./choices";

interface Upload {
  key: string;
  name: string;
  state: "uploading" | "ready" | "failed";
  attachment?: Attachment;
  error?: string;
}

/** The lines added to a message so the agent knows what was attached and how to read it. */
export function withAttachments(text: string, attachments: readonly Attachment[]): string {
  if (attachments.length === 0) return text;
  const lines = attachments.map((a) => `- ${a.name} (attachment_id: ${a.attachment_id})`);
  return `${text}\n\nAttached files (read them with read_attachment):\n${lines.join("\n")}`;
}

function ModelPicker({ session, config }: { session: CodingSession; config: AgentConfig | null }) {
  const [pending, setPending] = useState<{ choice: ModelChoice; recordId: string | null } | null>(null);
  const record = pending?.recordId ? session.sends.find((r) => r.id === pending.recordId) : undefined;
  const state = pickerState(session.model, session.modelAppliesNextTurn, pending ? { choice: pending.choice, record } : null);
  // Once the change is admitted, the facade's own model and "applies next turn" take over.
  useEffect(() => {
    if (record && (record.phase === "accepted" || record.phase === "running" || record.phase === "done")) setPending(null);
  }, [record]);

  async function pick(choice: ModelChoice) {
    setPending({ choice, recordId: null });
    const r: SendRecord = await session.setModel(choice);
    setPending((p) => (p && p.choice === choice ? { choice, recordId: r.id } : p));
  }

  const shown = state.shown;
  const efforts = effortsFor(config, shown);
  return (
    <Menu
      label="Model"
      buttonClass={`ctl on${state.failed ? " bad" : ""}`}
      buttonContent={
        <>
          {choiceLabel(config, shown)} <span aria-hidden="true">▾</span>
          {state.note && <span className="note">{state.note}</span>}
        </>
      }
    >
      {(close) => (
        <>
          {(config?.providers ?? []).map((p) => (
            <div key={p.provider} role="group" aria-label={p.label}>
              <div className="menu-head">{p.label}</div>
              {p.models.map((m) => {
                const on = shown?.provider === m.provider && shown.model === m.id;
                return (
                  <button
                    key={m.registry_key}
                    type="button"
                    role="menuitemradio"
                    aria-checked={on}
                    className="menu-item"
                    onClick={() => {
                      close();
                      if (!on) void pick({ provider: m.provider, model: m.id, effort: shown?.effort ?? m.default_effort ?? null });
                    }}
                  >
                    <span>
                      {m.display_name}
                      {on && " ✓"}
                    </span>
                  </button>
                );
              })}
            </div>
          ))}
          {shown && efforts.length > 0 && (
            <div role="group" aria-label="Effort" className="menu-efforts">
              <div className="menu-head">Effort</div>
              <div className="effort-row">
                {efforts.map((e) => (
                  <button
                    key={e}
                    type="button"
                    role="menuitemradio"
                    aria-checked={shown.effort === e}
                    className="effort"
                    onClick={() => {
                      close();
                      if (shown.effort !== e) void pick({ ...shown, effort: e });
                    }}
                  >
                    {e}
                  </button>
                ))}
              </div>
            </div>
          )}
          <div className="menu-foot hint">
            Applies from the next turn. {shown ? `Now: ${modelName(config, shown)} (${providerName(config, shown.provider)}).` : ""}
          </div>
        </>
      )}
    </Menu>
  );
}

function PostureMenu({ session, config }: { session: CodingSession; config: AgentConfig | null }) {
  const [pending, setPending] = useState<{ posture: Posture; recordId: string | null } | null>(null);
  const record = pending?.recordId ? session.sends.find((r) => r.id === pending.recordId) : undefined;
  useEffect(() => {
    if (record && record.phase !== "sending" && record.phase !== "in_temporal") setPending(null);
  }, [record]);
  const current = pending?.posture ?? session.posture;
  const options = postureOptions(config);
  const always = alwaysApprovedText(session.alwaysApproved, config);
  const failed = record?.phase === "failed";

  async function choose(p: Posture) {
    setPending({ posture: p, recordId: null });
    const r = await session.setPosture(p);
    setPending((x) => (x && x.posture === p ? { posture: p, recordId: r.id } : x));
  }

  return (
    <Menu
      label="Approvals"
      buttonClass={`ctl${current === "auto" ? " auto" : ""}${failed ? " bad" : ""}`}
      buttonContent={
        <>
          {postureLabel(config, current)} <span aria-hidden="true">▾</span>
          {pending && !failed && <span className="note">{record ? "applies to new calls" : "Sending…"}</span>}
          {failed && <span className="note">Couldn't change it</span>}
        </>
      }
    >
      {(close) => (
        <>
          {options.map((o) => {
            const on = o.id === current;
            return (
              <button
                key={o.id}
                type="button"
                role="menuitemradio"
                aria-checked={on}
                aria-disabled={!o.available}
                title={o.available ? undefined : o.reason ?? undefined}
                className={`menu-item posture${on ? " on" : ""}${o.available ? "" : " off"}`}
                onClick={() => {
                  if (!o.available) return;
                  close();
                  if (!on) void choose(o.id);
                }}
              >
                <span>
                  {o.label}
                  {o.default && <span className="hint"> · default</span>}
                  {on && " ✓"}
                </span>
                <span className="hint">{o.available ? o.description : o.reason ?? o.description}</span>
              </button>
            );
          })}
          <div className="menu-foot hint">
            Always approved this session: {always || "nothing yet"}
            {always && (
              <>
                {" · "}
                <button type="button" className="link-btn" aria-disabled="true" title="Clearing isn't available yet. Start a new session to reset it.">
                  Clear
                </button>
              </>
            )}
          </div>
          <div className="menu-foot hint">Applies to new tool calls. Calls already waiting stay with you.</div>
        </>
      )}
    </Menu>
  );
}

export function Composer({ session, config }: { session: CodingSession; config: AgentConfig | null }) {
  const [draft, setDraft] = useState("");
  const [uploads, setUploads] = useState<Upload[]>([]);
  const [stopping, setStopping] = useState(false);
  const fileInput = useRef<HTMLInputElement>(null);
  const busy = session.agentStatus === "busy";
  const typed = draft.trim().length > 0;
  const ready = uploads.filter((u) => u.state === "ready" && u.attachment).map((u) => u.attachment!);
  const uploading = uploads.some((u) => u.state === "uploading");

  useEffect(() => {
    if (!busy) setStopping(false);
  }, [busy]);

  function stop() {
    if (!busy || stopping) return;
    setStopping(true);
    void session.interrupt();
  }

  useEffect(() => {
    if (!busy) return;
    const onKey = (e: globalThis.KeyboardEvent) => {
      if (e.key === "Escape" && !e.defaultPrevented) stop();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  });

  function take(): string | null {
    const text = draft.trim();
    if (!text || uploading) return null;
    setDraft("");
    setUploads([]);
    return withAttachments(text, ready);
  }

  const send = () => {
    const text = take();
    if (text) void session.send(text);
  };
  const steer = () => {
    const text = take();
    if (text) void session.steer(text);
  };

  function onKeyDown(e: KeyboardEvent<HTMLTextAreaElement>) {
    if (e.key === "Enter" && !e.shiftKey && !e.nativeEvent.isComposing) {
      e.preventDefault();
      // Enter mid-turn queues; "Steer now" is a deliberate click.
      send();
    }
  }

  async function onFiles(files: FileList | null) {
    for (const file of Array.from(files ?? [])) {
      const key = `${file.name}:${Date.now()}:${Math.random()}`;
      setUploads((u) => [...u, { key, name: file.name, state: "uploading" }]);
      try {
        const attachment = await session.attach(file);
        setUploads((u) => u.map((x) => (x.key === key ? { ...x, state: "ready", attachment } : x)));
      } catch (err) {
        const error = err instanceof Error ? err.message : "Upload failed.";
        setUploads((u) => u.map((x) => (x.key === key ? { ...x, state: "failed", error } : x)));
      }
    }
    if (fileInput.current) fileInput.current.value = "";
  }

  return (
    <div className="composer">
      <div className="box">
        <label className="sr-only" htmlFor="composer-input">
          Message
        </label>
        <textarea
          id="composer-input"
          rows={2}
          value={draft}
          placeholder={busy ? "Steer the running turn, or queue a message…" : "Ask for a change…"}
          onChange={(e) => setDraft(e.target.value)}
          onKeyDown={onKeyDown}
        />
        {uploads.length > 0 && (
          <ul className="uploads" aria-label="Attachments">
            {uploads.map((u) => (
              <li key={u.key} className={`path${u.state === "failed" ? " bad" : ""}`}>
                {u.name} · {u.state === "uploading" ? "uploading…" : u.state === "ready" ? "attached" : `not attached: ${u.error}`}
                <button type="button" className="link-btn" aria-label={`Remove ${u.name}`} onClick={() => setUploads((x) => x.filter((y) => y.key !== u.key))}>
                  ✕
                </button>
              </li>
            ))}
          </ul>
        )}
        <div className="bar">
          <ModelPicker session={session} config={config} />
          <PostureMenu session={session} config={config} />
          <button type="button" className="ctl" onClick={() => fileInput.current?.click()}>
            + File
          </button>
          <input ref={fileInput} type="file" multiple hidden onChange={(e) => void onFiles(e.target.files)} />
          <span className="spacer" />
          {busy && typed && (
            <span className="split">
              <button type="button" className="btn secondary small" onClick={send} disabled={uploading}>
                Queue
              </button>
              <button type="button" className="btn primary small" onClick={steer} disabled={uploading}>
                Steer now
              </button>
            </span>
          )}
          {busy ? (
            <button type="button" className="stop" onClick={stop} disabled={stopping}>
              {stopping ? "Stopping…" : "Stop"}
              <kbd>Esc</kbd>
            </button>
          ) : (
            <button type="button" className="btn primary small" onClick={send} disabled={!typed || uploading}>
              Send
            </button>
          )}
        </div>
      </div>
      {busy && typed && <div className="hint">You typed during a turn: steer it now, or queue this for after it finishes.</div>}
    </div>
  );
}
