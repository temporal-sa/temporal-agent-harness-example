// Callback cards in the decision dock (PRD A12): the agent asks the person something and waits
// in Temporal. `ask_user` shows its options as buttons; `pick_files` takes files from this
// computer, uploads each into the workspace, and sends their paths. Both come from the stream
// (`session.pendingCallbacks`), so they survive a reload. The status line sits in a fixed-height
// footer under the buttons, so nothing moves when it changes.

import { useId, useRef, useState, type DragEvent } from "react";

import { errorCopy, type CallbackStatus, type CodingSession, type PendingCallback } from "../session";
import { PICK_MAX_FILES, screenFiles } from "../session/extras";
import { FailedNote } from "../recovery/SendStateTrail";
import "./decisions.css";

export const CALLBACK_WAIT_LINE = "The agent is paused in Temporal until you answer. This survives reloads and Worker restarts.";

export function CallbackCards({ session }: { session: CodingSession }) {
  if (!session.pendingCallbacks?.length) return null;
  return (
    <>
      {session.pendingCallbacks.map((cb) =>
        cb.request.kind === "ask_user" ? (
          <AskCard key={cb.part.toolId} session={session} cb={cb} />
        ) : cb.request.kind === "pick_files" ? (
          <PickCard key={cb.part.toolId} session={session} cb={cb} />
        ) : (
          <OtherCard key={cb.part.toolId} session={session} cb={cb} />
        ),
      )}
    </>
  );
}

/** Answering is allowed unless an answer is on its way or landed. */
export function canAnswer(status: CallbackStatus): boolean {
  return status.state === "open" || status.state === "failed" || status.state === "rejected";
}

/** The footer line for a callback's status, or null for none. */
export function callbackStatusText(status: CallbackStatus, kind: "ask_user" | "pick_files" | "other"): string | null {
  switch (status.state) {
    case "open":
      return null;
    case "sending":
      return status.action === "decline" ? "Sending your decline…" : "Sending your answer…";
    case "sent":
      return status.action === "decline" ? "Declined. The agent continues without it." : "Sent. Waiting for the agent to take it.";
    case "rejected":
      return `The agent didn't accept that answer: ${status.reason} ${kind === "pick_files" ? "Try again." : "Pick again."}`;
    case "failed":
      return null;
  }
}

function StatusFoot({ status, kind }: { status: CallbackStatus; kind: "ask_user" | "pick_files" | "other" }) {
  if (status.state === "failed") {
    const copy = errorCopy({ code: status.code, status: status.status, message: status.error }, "Check your connection and try again.");
    return (
      <div className="dc-foot">
        <FailedNote className="err" copy={{ text: `Your answer didn't reach Temporal. ${copy.text}`, detail: copy.detail }} />
      </div>
    );
  }
  const text = callbackStatusText(status, kind);
  return (
    <div className="dc-foot">
      <p className={`held${status.state === "rejected" ? " err" : ""}`} role={status.state === "rejected" ? "alert" : "status"}>
        {text}
      </p>
    </div>
  );
}

function WaitLine() {
  return (
    <p className="paused">
      <span className="dot you" aria-hidden="true" />
      {CALLBACK_WAIT_LINE}
    </p>
  );
}

function AskCard({ session, cb }: { session: CodingSession; cb: PendingCallback }) {
  if (cb.request.kind !== "ask_user") return null;
  const { question, options } = cb.request;
  const live = canAnswer(cb.status);
  const toolId = cb.part.toolId;
  return (
    <section className="dc-card cb" aria-label="Question from the agent">
      <div className="top">
        <span className="badge agent">Question</span>
        <span className="t">{question || "The agent has a question"}</span>
      </div>
      <WaitLine />
      <div className="dc-body">
        <ul className="choices" aria-label="Options">
          {options.map((o) => (
            <li key={o.id}>
              <button type="button" className="choice" disabled={!live} onClick={() => void session.answerCallback(toolId, { option_id: o.id })}>
                <span className="label">{o.label}</span>
                {o.description && <span className="hint">{o.description}</span>}
              </button>
            </li>
          ))}
        </ul>
      </div>
      <div className="acts">
        <button type="button" className="btn ghost small" disabled={!live} onClick={() => void session.declineCallback(toolId)}>
          Decline
        </button>
        <span className="hint">The agent sees which option you pick, or that you declined.</span>
      </div>
      <StatusFoot status={cb.status} kind="ask_user" />
    </section>
  );
}

interface PickedEntry {
  key: string;
  name: string;
  size: number;
  state: "uploading" | "uploaded" | "failed" | "refused";
  progress: number;
  path?: string;
  error?: string;
}

function fileSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(0)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function PickCard({ session, cb }: { session: CodingSession; cb: PendingCallback }) {
  const [files, setFiles] = useState<PickedEntry[]>([]);
  const [dragging, setDragging] = useState(false);
  // Every upload the server accepted for this request counts toward its 10, even one removed here.
  const accepted = useRef(0);
  const input = useRef<HTMLInputElement>(null);
  const hintId = useId();
  if (cb.request.kind !== "pick_files") return null;
  const { prompt, maxFiles } = cb.request;
  const toolId = cb.part.toolId;
  const live = canAnswer(cb.status);
  const active = files.filter((f) => f.state === "uploading" || f.state === "uploaded");
  const uploaded = files.filter((f) => f.state === "uploaded" && f.path);
  const uploading = files.some((f) => f.state === "uploading");
  const room = Math.max(0, Math.min(maxFiles - active.length, PICK_MAX_FILES - accepted.current - files.filter((f) => f.state === "uploading").length));

  const update = (key: string, change: Partial<PickedEntry>) => setFiles((list) => list.map((f) => (f.key === key ? { ...f, ...change } : f)));

  function take(list: FileList | null) {
    const chosen = Array.from(list ?? []);
    if (input.current) input.current.value = "";
    if (chosen.length === 0 || !live) return;
    const { accepted: ok, refused } = screenFiles(chosen, room);
    const stamp = `${Date.now()}:${Math.random()}`;
    const entries: PickedEntry[] = [
      ...ok.map((f, i) => ({ key: `${stamp}:${i}`, name: f.name, size: f.size, state: "uploading" as const, progress: 0 })),
      ...refused.map((r, i) => ({ key: `${stamp}:r${i}`, name: r.file.name, size: r.file.size, state: "refused" as const, progress: 0, error: r.reason })),
    ];
    setFiles((l) => [...l.filter((f) => f.state !== "refused"), ...entries]);
    ok.forEach((file, i) => {
      const key = `${stamp}:${i}`;
      session.uploadPicked(toolId, file, (p) => update(key, { progress: p })).then(
        (r) => {
          accepted.current += 1;
          update(key, { state: "uploaded", progress: 1, path: r.path });
        },
        (err) => {
          const copy = errorCopy(err, "Couldn't upload this file. Try again.");
          const code = (err as { code?: unknown }).code;
          const text =
            code === "too_large" ? "Over 5 MB. Each file can be at most 5 MB." : code === "too_many_files" ? "This request already has 10 files." : copy.text;
          update(key, { state: "failed", error: text });
        },
      );
    });
  }

  function onDrop(e: DragEvent<HTMLDivElement>) {
    e.preventDefault();
    setDragging(false);
    take(e.dataTransfer.files);
  }

  const send = () => void session.answerCallback(toolId, { paths: uploaded.map((f) => f.path!) });
  return (
    <section className="dc-card cb" aria-label="Files requested by the agent">
      <div className="top">
        <span className="badge agent">Files</span>
        <span className="t">{prompt || "The agent asks for files"}</span>
      </div>
      <WaitLine />
      <div className="dc-body">
        <div
          className={`drop${dragging ? " over" : ""}`}
          onDragOver={(e) => {
            e.preventDefault();
            if (!dragging) setDragging(true);
          }}
          onDragLeave={() => setDragging(false)}
          onDrop={onDrop}
        >
          <button type="button" className="btn secondary small" disabled={!live || room === 0} aria-describedby={hintId} onClick={() => input.current?.click()}>
            Choose files
          </button>
          <span className="hint" id={hintId}>
            or drop them here. Up to {maxFiles} {maxFiles === 1 ? "file" : "files"}, 5 MB each. They go into the workspace under uploads/.
          </span>
          <input ref={input} type="file" multiple hidden aria-label="Files for the agent" onChange={(e) => take(e.target.files)} />
        </div>
        {files.length > 0 && (
          <ul className="picked" aria-label="Chosen files">
            {files.map((f) => (
              <li key={f.key} className={f.state === "failed" || f.state === "refused" ? "bad" : undefined}>
                <span className="mono name">{f.name}</span>
                <span className="hint">{fileSize(f.size)}</span>
                <span className="spacer" />
                {f.state === "uploading" && (
                  <progress max={1} value={f.progress} aria-label={`Uploading ${f.name}`}>
                    {Math.round(f.progress * 100)}%
                  </progress>
                )}
                {f.state === "uploaded" && <span className="badge good">Uploaded</span>}
                {(f.state === "failed" || f.state === "refused") && <span className="err">Not uploaded. {f.error}</span>}
                {f.state !== "uploading" && live && (
                  <button type="button" className="link-btn" aria-label={`Remove ${f.name}`} onClick={() => setFiles((l) => l.filter((x) => x.key !== f.key))}>
                    ✕
                  </button>
                )}
              </li>
            ))}
          </ul>
        )}
      </div>
      <div className="acts">
        <button type="button" className="btn green small" disabled={!live || uploading || uploaded.length === 0} onClick={send}>
          {uploaded.length > 0 ? `Send ${uploaded.length} ${uploaded.length === 1 ? "file" : "files"}` : "Send files"}
        </button>
        <button type="button" className="btn ghost small" disabled={!live} onClick={() => void session.declineCallback(toolId)}>
          Decline
        </button>
      </div>
      <StatusFoot status={cb.status} kind="pick_files" />
    </section>
  );
}

function OtherCard({ session, cb }: { session: CodingSession; cb: PendingCallback }) {
  const live = canAnswer(cb.status);
  return (
    <section className="dc-card cb" aria-label="The agent is waiting on this browser">
      <div className="top">
        <span className="badge agent">Waiting</span>
        <span className="t">The agent called {cb.part.toolName}, which this app can't answer</span>
      </div>
      <div className="acts">
        <button type="button" className="btn ghost small" disabled={!live} onClick={() => void session.declineCallback(cb.part.toolId, "This browser can't answer that tool.")}>
          Decline
        </button>
      </div>
      <StatusFoot status={cb.status} kind="other" />
    </section>
  );
}
