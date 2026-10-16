// One tool call in the transcript (PRD U2): badge, name and argument preview, its state on
// the right, an inline mini-diff for edits and writes, path chips, a live output line while
// it runs, and the full output (with copy and the S7 log lookup) when opened.

import { useEffect, useRef, useState } from "react";
import { ApiError, type ToolPart } from "../session";
import { DiffView, JudgeCards } from "../app/slots";
import { useTranscript } from "./context";
import { ErrorNote } from "./ErrorNote";
import { argPreview, commandSummary, decisionBadge, miniDiff, pathsOf, toolBadge, type MiniDiff } from "./toolInfo";

function useElapsed(running: boolean): number | null {
  const since = useRef<number | null>(null);
  const [now, setNow] = useState(() => Date.now());
  if (running && since.current === null) since.current = Date.now();
  useEffect(() => {
    if (!running) return;
    const t = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(t);
  }, [running]);
  return running && since.current !== null ? Math.max(0, Math.floor((now - since.current) / 1000)) : null;
}

const MAX_CHIPS = 5;

const clock = (s: number) => `${Math.floor(s / 60)}:${String(s % 60).padStart(2, "0")}`;

function StateBadge({ part, elapsed }: { part: ToolPart; elapsed: number | null }) {
  const { workerAnswering } = useTranscript();
  const cmd = part.toolName === "run_command" ? commandSummary(part.output) : null;
  switch (part.state) {
    case "awaiting_approval":
      return <span className="badge git">Waiting for you</span>;
    case "evaluating":
      return <span className="badge prog">Judge deciding</span>;
    case "denied":
      return <span className="badge leaving">{decisionBadge(part) ?? "Declined"}</span>;
    case "requested":
    case "approved":
    case "running":
    case "awaiting_callback":
      if (!workerAnswering) return <span className="exit wait">waiting for a Worker{elapsed !== null ? ` · ${clock(elapsed)}` : ""}</span>;
      return <span className="exit run">running{elapsed !== null ? ` ${clock(elapsed)}` : ""}</span>;
    case "failed":
      return <span className="badge error">Failed</span>;
    case "done":
      if (cmd) {
        if (cmd.timedOut) return <span className="exit fail">timed out</span>;
        if (cmd.exitCode === 0) return <span className="exit ok">exit 0</span>;
        if (cmd.exitCode !== null) return <span className="exit fail">exit {cmd.exitCode}</span>;
      }
      if (part.approval?.approved) return <span className="badge good">{decisionBadge(part)}</span>;
      return null;
  }
}

export function DiffBlock({ diff, path }: { diff: MiniDiff; path: string }) {
  if (diff.rows.length === 0) return null;
  return (
    <div className="mini-diff">
      <DiffView lines={diff.rows} label={path ? `Changes to ${path} in this call` : "Changes in this call"} />
      {diff.hidden > 0 && <div className="more-lines">… {diff.hidden} more lines in Changes</div>}
    </div>
  );
}

function OutputPanel({ part }: { part: ToolPart }) {
  const { session } = useTranscript();
  const [copied, setCopied] = useState(false);
  const [log, setLog] = useState<{ text: string; state: "idle" | "loading" | "done" | "error"; note?: string }>({ text: "", state: "idle" });
  const logRef = part.toolName === "run_command" ? commandSummary(part.output)?.logRef ?? null : null;
  const text = log.state === "done" ? log.text : part.output ?? part.error ?? "";

  async function copy() {
    try {
      await navigator.clipboard.writeText(text);
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    } catch {
      setCopied(false);
    }
  }

  async function loadLog() {
    if (!logRef) return;
    setLog({ text: "", state: "loading" });
    try {
      let all = "";
      let offset: number | null = 0;
      for (let pages = 0; offset !== null && pages < 20; pages++) {
        const page = await session.readLog(logRef, { offset });
        all += page.text;
        offset = page.next_offset;
      }
      setLog({ text: all, state: "done" });
    } catch (err) {
      const note = err instanceof ApiError && err.status === 404 ? "Output no longer retained." : "The sandbox isn't answering. Try again shortly.";
      setLog({ text: "", state: "error", note });
    }
  }

  return (
    <div className="output">
      <pre tabIndex={0}>{text || "No output."}</pre>
      <div className="output-bar">
        <button type="button" className="btn ghost small" onClick={() => void copy()}>
          {copied ? "Copied" : "Copy"}
        </button>
        {logRef && log.state !== "done" && (
          <button type="button" className="btn ghost small" onClick={() => void loadLog()} disabled={log.state === "loading"}>
            {log.state === "loading" ? "Loading the full log…" : "Show the full log"}
          </button>
        )}
        {log.note && <span className="hint">{log.note}</span>}
      </div>
    </div>
  );
}

export function ToolRow({ part, nested = false }: { part: ToolPart; nested?: boolean }) {
  const { openFile, session } = useTranscript();
  const [open, setOpen] = useState(false);
  const badge = toolBadge(part.toolName);
  const running = part.state === "running" || part.state === "approved" || part.state === "requested" || part.state === "awaiting_callback";
  const elapsed = useElapsed(running);
  const diff = miniDiff(part);
  const paths = pathsOf(part);
  // The parsed output (part.progress is the raw, tagged chunk).
  const live = session.toolProgress[part.toolId];
  const liveLine = running && live ? live.trimEnd().split("\n").pop() ?? "" : "";

  return (
    <>
      <JudgeCards part={part} />
      <div className={`tool${nested ? " nest" : ""}`}>
        <button type="button" className="tool-row" aria-expanded={open} onClick={() => setOpen((v) => !v)}>
          <span className={`badge ${badge.kind}`}>{badge.label}</span>
          <span className="name">
            {part.toolName} <span className="arg">{argPreview(part)}</span>
          </span>
          <span className="t">
            {diff && (diff.added > 0 || diff.removed > 0) && (
              <span className="counts">
                +{diff.added} −{diff.removed}
              </span>
            )}
            <StateBadge part={part} elapsed={elapsed} />
          </span>
        </button>
        {liveLine && <div className="live">{liveLine}</div>}
        {diff && <DiffBlock diff={diff} path={paths[0] ?? ""} />}
        {part.state === "denied" && part.approval?.reason && <div className="live">Your reason: {part.approval.reason}</div>}
        {part.state === "failed" && part.error && (
          <ErrorNote text="The call failed. The agent sees the error and decides what to do next." detail={part.error} />
        )}
        {paths.length > 0 && (
          <div className="chips">
            {paths.slice(0, MAX_CHIPS).map((p) => (
              <button key={p} type="button" className="path" onClick={() => openFile(p)} title="Open in Changes">
                {p}
              </button>
            ))}
            {paths.length > MAX_CHIPS && <span className="hint">+{paths.length - MAX_CHIPS} more files in Changes</span>}
          </div>
        )}
        {open && <OutputPanel part={part} />}
      </div>
    </>
  );
}

export function ToolGroup({ label, parts }: { label: string; parts: ToolPart[] }) {
  const [open, setOpen] = useState(false);
  const badge = toolBadge(parts[0]?.toolName ?? "");
  const running = parts.some((p) => p.state === "running" || p.state === "requested" || p.state === "approved");
  const failed = parts.filter((p) => p.state === "failed").length;
  const args = parts.map((p) => argPreview(p).split("/").pop() ?? "").filter(Boolean);
  return (
    <div className="tool">
      <button type="button" className="tool-row" aria-expanded={open} onClick={() => setOpen((v) => !v)}>
        <span className={`badge ${badge.kind}`}>{badge.label}</span>
        <span className="name">
          {label} × {parts.length} <span className="arg">{args.join(", ")}</span>
        </span>
        <span className="t">{running ? <span className="exit run">running</span> : failed ? <span className="badge error">{failed} failed</span> : null}</span>
      </button>
      {open && (
        <div className="group-items">
          {parts.map((p) => (
            <ToolRow key={p.toolId} part={p} nested />
          ))}
        </div>
      )}
    </div>
  );
}
