// The workbench (PRD U3): Plan, Changes, Terminal and Artifacts, live from the agent's
// observable state and tool events. A right-hand panel on desktop; on a phone the shell puts
// the "sheet" variant in its bottom sheet.

import { useEffect, useId, useMemo, useRef, useState, type KeyboardEvent } from "react";

import type { CodingSession, PlanItem, WorkbenchFocus, WorkbenchProps as SlotProps } from "../session";
import { count, hms } from "../recovery/format";
import { artifactsOf, PREVIEW_SANDBOX, type ArtifactItem } from "./artifacts";
import { fileChanges, fileStat, focusedFile, workspaceFacts, type WorkspaceFacts } from "./changes";

const FLASH_MS = 1600;
import { DiffView } from "./DiffView";
import {
  COMMAND_TOOLS,
  exitBadge,
  ProgressLog,
  reattachFor,
  runLines,
  SANDBOX_DOWN,
  sandboxDown,
  type ExitBadge,
  type TermLine,
} from "./terminal";
import { toolsOf, type LocatedTool } from "./tree";
import "./workbench.css";

export const WORKBENCH_TABS = ["plan", "changes", "terminal", "artifacts"] as const;
export type WorkbenchTab = (typeof WORKBENCH_TABS)[number];

const TAB_LABEL: Record<WorkbenchTab, string> = {
  plan: "Plan",
  changes: "Changes",
  terminal: "Terminal",
  artifacts: "Artifacts",
};

export interface WorkbenchProps extends SlotProps {
  /** The tab to open first (the phone sheet opens on the tab its button named). */
  initialTab?: WorkbenchTab;
}

export function Workbench({ session, variant, initialTab, focus }: WorkbenchProps) {
  const [tab, setTab] = useState<WorkbenchTab>(focus ? "changes" : (initialTab ?? "plan"));
  useEffect(() => {
    if (initialTab) setTab(initialTab);
  }, [initialTab]);
  // A path chip in the transcript asks to see a file: open Changes. `focus.at` changes on every
  // click, so focusing the same file again scrolls to it again.
  const focusAt = focus?.at ?? null;
  useEffect(() => {
    if (focusAt !== null) setTab("changes");
  }, [focusAt]);
  const base = useId();
  const tabRefs = useRef<Record<string, HTMLButtonElement | null>>({});

  const tools = useMemo(() => toolsOf(session), [session]);
  const runs = useMemo(() => tools.filter((t) => COMMAND_TOOLS.has(t.part.toolName)), [tools]);
  const artifacts = useMemo(() => artifactsOf(tools), [tools]);
  const facts = useMemo(() => workspaceFacts(session.workspace, session.changes, tools), [session.workspace, session.changes, tools]);

  const plan = session.plan;
  const done = plan?.items.filter((i) => i.status === "done").length ?? 0;
  const live = runs.some((r) => r.part.state === "running" || r.part.state === "approved");

  const counts: Record<WorkbenchTab, { shown: string; spoken: string } | null> = {
    plan: plan && plan.items.length ? { shown: `${done}/${plan.items.length}`, spoken: `${done} of ${plan.items.length} steps done` } : null,
    changes: session.changes ? { shown: String(session.changes.files.length), spoken: count(session.changes.files.length, "file") } : null,
    terminal: null,
    artifacts: { shown: String(artifacts.length), spoken: count(artifacts.length, "artifact") },
  };

  const onKey = (e: KeyboardEvent<HTMLDivElement>) => {
    const i = WORKBENCH_TABS.indexOf(tab);
    let next: number | null = null;
    if (e.key === "ArrowRight") next = (i + 1) % WORKBENCH_TABS.length;
    else if (e.key === "ArrowLeft") next = (i - 1 + WORKBENCH_TABS.length) % WORKBENCH_TABS.length;
    else if (e.key === "Home") next = 0;
    else if (e.key === "End") next = WORKBENCH_TABS.length - 1;
    if (next === null) return;
    e.preventDefault();
    const t = WORKBENCH_TABS[next]!;
    setTab(t);
    tabRefs.current[t]?.focus();
  };

  return (
    <section className={`wb wb-${variant}`} aria-label="Workbench">
      <div className="wb-tabs" role="tablist" aria-label="Workbench views" onKeyDown={onKey}>
        {WORKBENCH_TABS.map((t) => {
          const c = counts[t];
          return (
            <button
              key={t}
              ref={(el) => {
                tabRefs.current[t] = el;
              }}
              type="button"
              role="tab"
              id={`${base}-tab-${t}`}
              aria-controls={`${base}-panel-${t}`}
              aria-selected={tab === t}
              tabIndex={tab === t ? 0 : -1}
              className="wb-tab"
              onClick={() => setTab(t)}
            >
              {TAB_LABEL[t]}
              {c && (
                <span className="n">
                  <span aria-hidden="true">{c.shown}</span>
                  <span className="sr-only">, {c.spoken}</span>
                </span>
              )}
              {t === "terminal" && live && (
                <>
                  <span className="live-dot" aria-hidden="true" />
                  <span className="sr-only">, a command is running</span>
                </>
              )}
            </button>
          );
        })}
      </div>
      <div
        className="wb-body"
        role="tabpanel"
        id={`${base}-panel-${tab}`}
        aria-labelledby={`${base}-tab-${tab}`}
        tabIndex={0}
      >
        {tab === "plan" && <PlanPanel session={session} facts={facts} />}
        {tab === "changes" && <ChangesPanel session={session} tools={tools} focus={focus ?? null} />}
        {tab === "terminal" && <TerminalPanel session={session} runs={runs} facts={facts} />}
        {tab === "artifacts" && <ArtifactsPanel artifacts={artifacts} />}
      </div>
    </section>
  );
}

// -- Plan -----------------------------------------------------------------------------------

const PLAN_MARK: Record<PlanItem["status"], { icon: string; spoken: string }> = {
  done: { icon: "✓", spoken: "done" },
  in_progress: { icon: "", spoken: "in progress" },
  pending: { icon: "", spoken: "to do" },
  skipped: { icon: "–", spoken: "skipped" },
};

function PlanPanel({ session, facts }: { session: CodingSession; facts: WorkspaceFacts }) {
  const plan = session.plan;
  return (
    <>
      {!plan || plan.items.length === 0 ? (
        <p className="wb-empty">No plan yet. The agent writes one when a task has several steps.</p>
      ) : (
        <>
          {plan.goal && <p className="wb-goal">{plan.goal}</p>}
          <ol className="wb-plan">
            {plan.items.map((item, i) => (
              <li key={i} className={`it ${item.status}`}>
                <span className="box-ic" aria-hidden="true">
                  {PLAN_MARK[item.status].icon}
                </span>
                <span className="txt">
                  {item.text}
                  <span className="sr-only"> ({PLAN_MARK[item.status].spoken})</span>
                </span>
              </li>
            ))}
          </ol>
        </>
      )}
      <Facts facts={facts} />
      <p className="wb-hint">The plan is the agent's own todo list, live from Workflow state.</p>
    </>
  );
}

function Facts({ facts }: { facts: WorkspaceFacts }) {
  const branch = facts.branch
    ? `${facts.branch} · ${count(facts.filesChanged, "file")} changed`
    : `${count(facts.filesChanged, "file")} changed`;
  return (
    <dl className="wb-facts" aria-label="Workspace">
      <dt>branch</dt>
      <dd>{branch}</dd>
      <dt>last command</dt>
      <dd>{facts.lastCommand ?? "none yet"}</dd>
      <dt>exit</dt>
      <dd>{facts.exit}</dd>
    </dl>
  );
}

// -- Changes --------------------------------------------------------------------------------

function ChangesPanel({ session, tools, focus }: { session: CodingSession; tools: LocatedTool[]; focus: WorkbenchFocus | null }) {
  const files = useMemo(() => fileChanges(session.changes, tools), [session.changes, tools]);
  const refs = useRef(new Map<string, HTMLElement>());
  const [flash, setFlash] = useState<{ path: string; at: number } | null>(null);
  const target = focus ? focusedFile(files, focus.path) : null;
  const focusAt = focus?.at ?? null;
  useEffect(() => {
    if (focusAt === null || target === null) return;
    const el = refs.current.get(target);
    if (!el) return;
    const reduce = typeof window.matchMedia === "function" && window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    el.scrollIntoView({ block: "start", behavior: reduce ? "auto" : "smooth" });
    el.focus({ preventScroll: true });
    // Restart the highlight even when the same file is focused again while it still shows.
    if (!reduce && typeof el.animate === "function") {
      el.animate([{ backgroundColor: "rgba(97, 105, 237, .25)" }, { backgroundColor: "transparent" }], { duration: FLASH_MS, easing: "ease-out" });
    }
    setFlash({ path: target, at: focusAt });
    const t = window.setTimeout(() => setFlash((f) => (f?.at === focusAt ? null : f)), FLASH_MS);
    return () => window.clearTimeout(t);
  }, [focusAt, target]);
  const commits = session.changes?.commits ?? [];
  if (files.length === 0 && commits.length === 0) return <p className="wb-empty">No files changed yet.</p>;
  return (
    <>
      {files.map((f) => {
        const s = fileStat(f);
        return (
          <article
            key={f.path}
            ref={(el) => {
              if (el) refs.current.set(f.path, el);
              else refs.current.delete(f.path);
            }}
            tabIndex={-1}
            className={`wb-file${flash?.path === f.path ? " flash" : ""}`}
            aria-label={`${f.path}, ${f.status}`}
          >
            <header className="fh">
              <span className="path">{f.path}</span>
              <span className="status">{f.status}</span>
              <span className="st">
                <span className="a">+{s.added}</span>
                <span className="r">−{s.removed}</span>
                <span className="sr-only">
                  {" "}
                  {count(s.added, "line")} added, {count(s.removed, "line")} removed
                </span>
              </span>
            </header>
            {f.hunks.map((h) => (
              <DiffView key={h.toolId} lines={h.lines} truncated={h.truncated} label={`Diff of ${f.path}`} />
            ))}
            {f.hunks.length === 0 && f.unshown === 0 && <p className="wb-note">No diff reported for this file.</p>}
            {f.unshown > 0 && (
              <p className="wb-note">
                {f.unshown === 1 ? "1 rewrite of this file isn't shown as a diff." : `${f.unshown} rewrites of this file aren't shown as a diff.`}
              </p>
            )}
          </article>
        );
      })}
      {commits.length > 0 ? (
        <div className="wb-commits">
          <p className="wb-hint">{count(commits.length, "commit")} this session</p>
          <ul>
            {commits.map((c) => (
              <li key={c.hash}>
                <span className="mono hash">{c.hash.slice(0, 7)}</span> {c.message}
              </li>
            ))}
          </ul>
        </div>
      ) : (
        <p className="wb-hint">Uncommitted · the diff the commit will contain</p>
      )}
    </>
  );
}

// -- Terminal -------------------------------------------------------------------------------

function useNow(active: boolean): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    if (!active) return;
    const t = window.setInterval(() => setNow(Date.now()), 1000);
    return () => window.clearInterval(t);
  }, [active]);
  return now;
}

function TerminalPanel({ session, runs, facts }: { session: CodingSession; runs: LocatedTool[]; facts: WorkspaceFacts }) {
  const log = useRef(new ProgressLog()).current;
  for (const r of runs) log.observe(r.part.toolId, r.part.progress, r.part.state);

  const worker = session.telemetry.worker;
  const down = sandboxDown(worker);
  const anyRunning = runs.some((r) => r.part.state === "running" || r.part.state === "approved");
  const now = useNow(anyRunning && worker.answering);

  if (runs.length === 0) {
    return (
      <>
        <p className="wb-empty">No commands run yet.</p>
        <Facts facts={facts} />
      </>
    );
  }

  const ordered = [...runs].sort((a, b) => (log.firstSeen(a.part.toolId) ?? 0) - (log.firstSeen(b.part.toolId) ?? 0) || a.seq - b.seq);
  const latest = ordered[ordered.length - 1]!;
  const earlier = ordered.slice(0, -1).reverse();

  const view = (r: LocatedTool) => {
    const first = log.startedAt(r.part.toolId);
    const running = r.part.state === "running" || r.part.state === "approved";
    const badge = exitBadge(r.part, {
      workerAnswering: worker.answering,
      elapsedMs: running && first !== null ? now - first : null,
      sandboxDown: down,
    });
    const lines = runLines(r.part, {
      live: log.text(r.part.toolId),
      workerAnswering: worker.answering,
      pausedAt: hms(worker.since),
      sandboxDown: down,
      reattach: reattachFor(r, session.telemetry.recoveries),
    });
    return { badge, lines, running };
  };

  const now0 = view(latest);
  return (
    <>
      {down && (
        <p className="wb-sandbox" role="status">
          {SANDBOX_DOWN}
        </p>
      )}
      <div className="wb-term-head">
        <span>{now0.running ? "Now" : "Last"} ·</span>
        <span className="mono cmd">{String(latest.part.input.command ?? "")}</span>
        <Badge badge={now0.badge} />
      </div>
      <Term lines={now0.lines} live={now0.running} label={`Output of ${String(latest.part.input.command ?? "the command")}`} />
      {latest.part.state === "failed" && latest.part.error && <p className="wb-note">{latest.part.error}</p>}
      {earlier.length > 0 && (
        <div className="wb-earlier">
          {earlier.map((r, i) => {
            const v = view(r);
            return (
              <details key={r.part.toolId} open={i === 0}>
                <summary className="wb-term-head">
                  <span>Earlier ·</span>
                  <span className="mono cmd">{String(r.part.input.command ?? "")}</span>
                  <Badge badge={v.badge} />
                </summary>
                <Term lines={v.lines} live={false} label={`Output of ${String(r.part.input.command ?? "an earlier command")}`} />
              </details>
            );
          })}
        </div>
      )}
      <Facts facts={facts} />
    </>
  );
}

export function Badge({ badge }: { badge: ExitBadge }) {
  return <span className={`wb-exit ${badge.tone}`}>{badge.text}</span>;
}

function Term({ lines, live, label }: { lines: TermLine[]; live: boolean; label: string }) {
  const ref = useRef<HTMLDivElement>(null);
  const stick = useRef(true);
  useEffect(() => {
    const el = ref.current;
    if (el && live && stick.current) el.scrollTop = el.scrollHeight;
  });
  return (
    <div
      ref={ref}
      className="wb-term"
      role="log"
      aria-label={label}
      aria-live={live ? "polite" : "off"}
      tabIndex={0}
      onScroll={(e) => {
        const el = e.currentTarget;
        stick.current = el.scrollHeight - el.scrollTop - el.clientHeight < 24;
      }}
    >
      {lines.map((l, i) => (
        <div key={i} className={l.kind}>
          {l.kind === "err" && <span className="sr-only">stderr: </span>}
          {l.text || " "}
        </div>
      ))}
    </div>
  );
}

// -- Artifacts ------------------------------------------------------------------------------

function ArtifactsPanel({ artifacts }: { artifacts: ArtifactItem[] }) {
  const [selected, setSelected] = useState<string | null>(null);
  if (artifacts.length === 0) return <p className="wb-empty">No artifacts yet. Files the agent publishes for you to open appear here.</p>;
  const current = artifacts.find((a) => a.id === selected) ?? artifacts[artifacts.length - 1]!;
  return (
    <>
      <ul className="wb-artifacts">
        {artifacts.map((a) => (
          <li key={a.toolId}>
            <button type="button" aria-pressed={a.id === current.id} onClick={() => setSelected(a.id)}>
              <span className="mono">{a.name}</span>
              <span className="wb-hint">{a.mimeType}</span>
            </button>
          </li>
        ))}
      </ul>
      <div className="wb-preview">
        {current.kind === "html" && current.previewUrl && (
          <iframe
            key={current.id}
            title={`Preview of ${current.name}`}
            src={current.previewUrl}
            sandbox={PREVIEW_SANDBOX}
            referrerPolicy="no-referrer"
            loading="lazy"
          />
        )}
        {current.kind === "image" && <img src={current.viewUrl} alt={`Preview of ${current.name}`} />}
        {current.kind === "other" && <p className="wb-note">No preview for {current.mimeType}.</p>}
        <div className="wb-preview-links">
          <a href={current.viewUrl} target="_blank" rel="noopener noreferrer">
            Open ↗
          </a>
          <a href={current.downloadUrl}>Download</a>
        </div>
      </div>
    </>
  );
}
