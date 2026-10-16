// The app shell (PRD U1, U5, U7, U8): boot (reachability and sign-in), the session list, the
// selected session, and the layout grid with its slots.

import { useCallback, useEffect, useMemo, useState, type ReactNode } from "react";

import { Composer } from "../composer/Composer";
import { MetersStrip } from "../meters/MetersStrip";
import {
  api,
  isUnreachable,
  summaryModel,
  useCodingSession,
  type AgentConfig,
  type AuthConfig,
  type CodingSession,
  type Me,
  type ModelChoice,
  type SessionSummary,
} from "../session";
import { Header } from "../shell/Header";
import { useMaxWidth, usePresenterMode } from "../shell/presenter";
import { Rail, Sidebar, type SidebarProps } from "../shell/Sidebar";
import { SignIn } from "../shell/SignIn";
import { Unreachable } from "../shell/Unreachable";
import { ViewLinks, viewDefs } from "../shell/ViewLinks";
import { Transcript } from "../transcript/Transcript";
import { DecisionDock, Workbench, WorkerBanner } from "./slots";

type Boot =
  | { kind: "loading" }
  | { kind: "unreachable" }
  | { kind: "signin"; auth: AuthConfig | null }
  | { kind: "ready"; auth: AuthConfig; me: Me };

export function App() {
  const [boot, setBoot] = useState<Boot>({ kind: "loading" });

  const start = useCallback(async () => {
    let auth: AuthConfig | null = null;
    try {
      auth = await api.getAuthConfig();
      const me = await api.getMe();
      setBoot({ kind: "ready", auth, me });
    } catch (err) {
      if (isUnreachable(err)) setBoot({ kind: "unreachable" });
      else setBoot({ kind: "signin", auth });
    }
  }, []);

  useEffect(() => {
    void start();
  }, [start]);

  switch (boot.kind) {
    case "loading":
      return <main className="page-state" aria-busy="true" />;
    case "unreachable":
      return <Unreachable onRetry={start} />;
    case "signin":
      return <SignIn auth={boot.auth} onSignedIn={start} />;
    case "ready":
      return <Shell me={boot.me} auth={boot.auth} onUnreachable={() => setBoot({ kind: "unreachable" })} />;
  }
}

// -- URL: the selected session is ?s=<id> -------------------------------------------------------

function sessionFromUrl(): string | null {
  return new URLSearchParams(window.location.search).get("s");
}

function setSessionInUrl(id: string | null) {
  const url = new URL(window.location.href);
  if (id) url.searchParams.set("s", id);
  else url.searchParams.delete("s");
  window.history.pushState({}, "", url);
}

// -- the shell ----------------------------------------------------------------------------------

function Shell({ me, auth, onUnreachable }: { me: Me; auth: AuthConfig; onUnreachable(): void }) {
  const [config, setConfig] = useState<AgentConfig | null>(null);
  const [sessions, setSessions] = useState<SessionSummary[] | null>(null);
  const [listError, setListError] = useState<string | null>(null);
  const [selected, setSelected] = useState<string | null>(sessionFromUrl);
  const [creating, setCreating] = useState<"demo" | "empty" | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);

  const fail = useCallback(
    (err: unknown, fallback: string, set: (m: string) => void) => {
      if (isUnreachable(err)) onUnreachable();
      else set(err instanceof Error && err.message ? err.message : fallback);
    },
    [onUnreachable],
  );

  const refresh = useCallback(async () => {
    try {
      const list = await api.listSessions();
      setSessions([...list].sort((a, b) => b.created_at - a.created_at));
      setListError(null);
    } catch (err) {
      fail(err, "Couldn't load your sessions.", setListError);
    }
  }, [fail]);

  useEffect(() => {
    void refresh();
    api.getAgentConfig().then(setConfig, (err) => fail(err, "", () => undefined));
  }, [refresh, fail]);

  useEffect(() => {
    const onPop = () => setSelected(sessionFromUrl());
    window.addEventListener("popstate", onPop);
    return () => window.removeEventListener("popstate", onPop);
  }, []);

  const select = useCallback((id: string | null) => {
    setSessionInUrl(id);
    setSelected(id);
  }, []);

  const create = useCallback(
    async (kind: "demo" | "empty") => {
      setCreating(kind);
      setActionError(null);
      try {
        const created = await api.createSession(kind === "demo" ? { demo: "refund_service" } : {});
        await refresh();
        select(created.session_id);
      } catch (err) {
        fail(err, "Couldn't start a session. Try again.", setActionError);
      } finally {
        setCreating(null);
      }
    },
    [refresh, select, fail],
  );

  const summary = useMemo(() => sessions?.find((s) => s.session_id === selected) ?? null, [sessions, selected]);

  const modelName = useCallback(
    (choice: Partial<ModelChoice> | null | undefined, withProvider = false): string => {
      if (!choice?.provider || !choice.model) return "";
      const entry = config?.models.find((m) => m.provider === choice.provider && m.id === choice.model);
      const name = entry?.display_name ?? choice.model;
      const provider = config?.providers.find((p) => p.provider === choice.provider)?.label ?? choice.provider;
      return withProvider ? `${name} · ${provider}` : name;
    },
    [config],
  );

  const shared: ShellParts = {
    me,
    auth,
    config,
    sessions,
    listError,
    selected,
    summary,
    creating,
    actionError,
    clearActionError: () => setActionError(null),
    select,
    create,
    modelName,
    onUnreachable,
  };

  if (!selected) return <Layout parts={shared} session={null} />;
  return (
    <SessionScreen key={selected} id={selected} summary={summary} render={(session) => <Layout parts={shared} session={session} />} />
  );
}

interface ShellParts {
  me: Me;
  auth: AuthConfig;
  config: AgentConfig | null;
  sessions: SessionSummary[] | null;
  listError: string | null;
  selected: string | null;
  summary: SessionSummary | null;
  creating: "demo" | "empty" | null;
  actionError: string | null;
  clearActionError(): void;
  select(id: string | null): void;
  create(kind: "demo" | "empty"): Promise<void>;
  modelName(choice: Partial<ModelChoice> | null | undefined, withProvider?: boolean): string;
  onUnreachable(): void;
}

function SessionScreen({ id, summary, render }: { id: string; summary: SessionSummary | null; render(s: CodingSession): ReactNode }) {
  const seed = useMemo(() => ({ model: summaryModel(summary), posture: summary?.posture ?? null }), [summary]);
  const session = useCodingSession(id, seed);
  return <>{render(session)}</>;
}

// -- layout -----------------------------------------------------------------------------------

function Layout({ parts, session }: { parts: ShellParts; session: CodingSession | null }) {
  const [presenter, togglePresenter] = usePresenterMode();
  const narrow = useMaxWidth(1600);
  const phone = useMaxWidth(600);
  const [drawer, setDrawer] = useState(false);
  const [sheet, setSheet] = useState(false);
  const rail = (presenter || narrow) && !phone;

  // A stream that gave up: is the app down, or only this session's stream?
  const connection = session?.connection;
  const { onUnreachable } = parts;
  useEffect(() => {
    if (connection !== "error") return;
    api.getAuthConfig().catch((err) => {
      if (isUnreachable(err)) onUnreachable();
    });
  }, [connection, onUnreachable]);

  const views = viewDefs(parts.config, parts.me, parts.selected);
  const sideProps: SidebarProps = {
    sessions: parts.sessions,
    loadError: parts.listError,
    currentId: parts.selected,
    currentStatus: session?.status ?? null,
    modelName: (s) => parts.modelName(s.model),
    creating: parts.creating,
    onSelect: (id) => {
      setDrawer(false);
      parts.select(id);
    },
    onNewDemo: () => {
      setDrawer(false);
      void parts.create("demo");
    },
    onNewEmpty: () => {
      setDrawer(false);
      void parts.create("empty");
    },
    footer: `Signed in as ${parts.me.username}${parts.auth.mode === "catalog" ? " through the catalog" : ""}`,
  };

  const title = parts.summary?.title ?? (parts.selected ? "Session" : null);
  const demo = parts.summary?.demo ?? null;
  const modelLabel = session ? parts.modelName(session.model, true) || null : null;
  const changesCount = session?.changes?.files.length ?? 0;

  return (
    <div className="app">
      <Header
        title={title}
        demo={demo}
        status={session?.status ?? null}
        views={<ViewLinks views={views} />}
        presenter={presenter}
        onPresenter={togglePresenter}
        onMenu={() => setDrawer(true)}
      />
      {session ? <MetersStrip telemetry={session.telemetry} modelLabel={modelLabel} /> : <div className="meters" aria-hidden="true" />}

      <div className={`body${rail ? " rail" : ""}`}>
        <div className="slot-side">{rail ? <Rail {...sideProps} onExpand={() => setDrawer(true)} /> : <Sidebar {...sideProps} />}</div>

        <main className="slot-main" id="main">
          <div className="transcript-slot" data-slot="transcript">
            <div className="col">
              {parts.actionError && (
                <div className="notice" role="alert">
                  <span>{parts.actionError}</span>
                  <button type="button" className="btn ghost small" onClick={parts.clearActionError}>
                    Dismiss
                  </button>
                </div>
              )}
              {session ? (
                <>
                  {session.connection === "error" && (
                    <div className="notice" role="alert">
                      <span>Lost the live connection to this session. Nothing is lost in Temporal.</span>
                      <button type="button" className="btn secondary small" onClick={session.reconnect}>
                        Reconnect
                      </button>
                    </div>
                  )}
                  {session.connection === "reconnecting" && <div className="sys-line">Reconnecting…</div>}
                  <WorkerBanner session={session} />
                  <Transcript session={session} />
                </>
              ) : (
                <Landing creating={parts.creating} onCreate={parts.create} />
              )}
            </div>
          </div>
          {session && (
            <div className="decision-slot" data-slot="decisions">
              <DecisionDock session={session} />
            </div>
          )}
          {session && phone && (
            <div className="slot-sheet" data-slot="sheet">
              <button type="button" className="sheet-grab" onClick={() => setSheet(true)} aria-haspopup="dialog">
                Changes ({changesCount})
              </button>
              <button type="button" className="sheet-grab" onClick={() => setSheet(true)} aria-haspopup="dialog">
                Plan
              </button>
            </div>
          )}
          {session && (
            <div className="composer-slot" data-slot="composer">
              <Composer session={session} />
            </div>
          )}
        </main>

        <div className="slot-bench" data-slot="workbench">
          {session && !phone ? <Workbench session={session} variant="panel" /> : null}
        </div>
      </div>

      {drawer && (
        <>
          <div className="drawer-scrim" onClick={() => setDrawer(false)} aria-hidden="true" />
          <div className="drawer" role="dialog" aria-modal="true" aria-label="Sessions and views">
            <div className="drawer-head">
              <span className="brand">
                <span className="brand-mark" aria-hidden="true">
                  T
                </span>
                Coding agent
              </span>
              <span className="spacer" />
              <button type="button" className="btn ghost small" onClick={() => setDrawer(false)} autoFocus>
                Close
              </button>
            </div>
            <div style={{ display: "grid", gridTemplateRows: "minmax(0,1fr) auto", minHeight: 0 }}>
              <Sidebar {...sideProps} />
              <ViewLinks views={views} className="drawer-views" />
            </div>
          </div>
        </>
      )}

      {sheet && session && phone && (
        <>
          <div className="drawer-scrim" onClick={() => setSheet(false)} aria-hidden="true" />
          <div className="sheet" role="dialog" aria-modal="true" aria-label="Workbench">
            <div className="drawer-head">
              <span>Workbench</span>
              <span className="spacer" />
              <button type="button" className="btn ghost small" onClick={() => setSheet(false)} autoFocus>
                Close
              </button>
            </div>
            <Workbench session={session} variant="sheet" />
          </div>
        </>
      )}
    </div>
  );
}

function Landing({ creating, onCreate }: { creating: "demo" | "empty" | null; onCreate(kind: "demo" | "empty"): Promise<void> }) {
  return (
    <div className="empty">
      <p className="eyebrow">Coding agent on Temporal</p>
      <h1>Start a session</h1>
      <p>A demo session starts the agent in a small refund service with one failing test. Ask it to find the cause, fix it, run the tests and commit.</p>
      <div className="acts">
        <button type="button" className="btn primary" disabled={creating !== null} onClick={() => void onCreate("demo")}>
          {creating === "demo" ? "Setting up the demo…" : "+ New demo session"}
        </button>
        <button type="button" className="btn ghost" disabled={creating !== null} onClick={() => void onCreate("empty")}>
          {creating === "empty" ? "Starting…" : "New empty session"}
        </button>
      </div>
    </div>
  );
}
