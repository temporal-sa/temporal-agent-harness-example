// The app shell (PRD U1, U5, U7, U8): boot (reachability and sign-in), the session list, the
// selected session, and the layout grid with its slots.

import { useCallback, useEffect, useMemo, useState, type ReactNode } from "react";

import { Composer } from "../composer/Composer";
import { MetersStrip } from "../meters/MetersStrip";
import {
  api,
  ApiError,
  errorCopy,
  isUnreachable,
  summaryModel,
  useCodingSession,
  type AgentConfig,
  type AuthConfig,
  type CodingSession,
  type Me,
  type ModelChoice,
  type SessionSummary,
  type WorkbenchFocus,
} from "../session";
import { Dialog } from "../shell/Dialog";
import { Header } from "../shell/Header";
import { useMaxWidth, usePresenterMode } from "../shell/presenter";
import { Rail, Sidebar, type SidebarProps } from "../shell/Sidebar";
import { SignIn } from "../shell/SignIn";
import { Unreachable } from "../shell/Unreachable";
import { ViewLinks, viewDefs } from "../shell/ViewLinks";
import { TranscriptPane } from "../transcript/Transcript";
import type { TranscriptContextValue } from "../transcript/context";
import { postureLabel } from "../composer/choices";
import { McpDialog, mcpReturnNotice } from "../mcp/McpDialog";
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
  // Back from an MCP OAuth sign-in: say how it went, then clear the query (keeping ?s=).
  const [mcpNotice, setMcpNotice] = useState(() => mcpReturnNotice(window.location.search));
  useEffect(() => {
    if (!mcpNotice) return;
    const url = new URL(window.location.href);
    for (const k of ["mcp", "mcp_server", "mcp_error"]) url.searchParams.delete(k);
    window.history.replaceState({}, "", url);
  }, [mcpNotice]);

  const fail = useCallback(
    (err: unknown, fallback: string, set: (m: string) => void) => {
      if (isUnreachable(err)) onUnreachable();
      else set(errorCopy(err, fallback).text);
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

  // A session opened by URL that the list doesn't have (yet): ask for it before opening its
  // stream. The API answers 404 for an unknown id and for another user's session alike; then
  // the app shows a not-found page and never attaches.
  const [lookup, setLookup] = useState<{ id: string; summary: SessionSummary | null; missing: string | null } | null>(null);
  const listed = useMemo(() => sessions?.find((s) => s.session_id === selected) ?? null, [sessions, selected]);
  useEffect(() => {
    if (!selected || listed) return;
    let live = true;
    api.getSession(selected).then(
      (s) => live && setLookup({ id: selected, summary: s, missing: null }),
      (err) => {
        if (!live) return;
        if (err instanceof ApiError && (err.status === 404 || err.status === 403)) {
          setLookup({ id: selected, summary: null, missing: "No such session" });
        } else {
          fail(err, "Couldn't open that session. Try again.", (m) => setLookup({ id: selected, summary: null, missing: m }));
        }
      },
    );
    return () => {
      live = false;
    };
  }, [selected, listed, fail]);
  const found = lookup?.id === selected ? lookup : null;
  const summary = listed ?? found?.summary ?? null;

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
    mcpNotice,
    clearMcpNotice: () => setMcpNotice(null),
    select,
    create,
    refresh,
    modelName,
    onUnreachable,
  };

  if (!selected) return <Layout parts={shared} session={null} />;
  if (!summary) {
    return <Layout parts={shared} session={null} page={found?.missing ? { kind: "missing", message: found.missing } : { kind: "opening" }} />;
  }
  return (
    <SessionScreen key={selected} id={selected} summary={summary} config={config} render={(session) => <Layout parts={shared} session={session} />} />
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
  mcpNotice: { ok: boolean; text: string } | null;
  clearMcpNotice(): void;
  select(id: string | null): void;
  create(kind: "demo" | "empty"): Promise<void>;
  refresh(): Promise<void>;
  modelName(choice: Partial<ModelChoice> | null | undefined, withProvider?: boolean): string;
  onUnreachable(): void;
}

function SessionScreen({
  id,
  summary,
  config,
  render,
}: {
  id: string;
  summary: SessionSummary | null;
  config: AgentConfig | null;
  render(s: CodingSession): ReactNode;
}) {
  const seed = useMemo(
    () => ({
      model: summaryModel(summary),
      posture: summary?.posture ?? null,
      toolRiskClasses: config?.tool_risk_classes ?? null,
      defaultToolRiskClass: config?.default_tool_risk_class ?? null,
      rememberAllowedClasses: config?.remember_allowed_classes ?? null,
    }),
    [summary, config],
  );
  const session = useCodingSession(id, seed);
  return <>{render(session)}</>;
}

// -- layout -----------------------------------------------------------------------------------

type Page = { kind: "landing" } | { kind: "opening" } | { kind: "missing"; message: string };

function Layout({ parts, session, page = { kind: "landing" } }: { parts: ShellParts; session: CodingSession | null; page?: Page }) {
  const [presenter, togglePresenter] = usePresenterMode();
  const narrow = useMaxWidth(1600);
  const phone = useMaxWidth(600);
  const [drawer, setDrawer] = useState(false);
  const [sheet, setSheet] = useState<"changes" | "terminal" | null>(null);
  // The MCP servers dialog; `reconnect` when a transcript chip opened it for one server.
  const [mcp, setMcp] = useState<{ reconnect: string | null } | null>(null);
  const mcpFeature = parts.config?.mcp?.enabled ? parts.config.mcp : null;
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
    footer: footerText(parts.me.username, parts.auth.mode, parts.config?.session_ttl_days ?? null),
  };

  const title = parts.summary?.title ?? null;
  const demo = parts.summary?.demo ?? null;
  const modelLabel = session ? meterModelLabel(session, parts.modelName) : null;
  const changesCount = session?.changes?.files.length ?? 0;
  const commandRunning = session ? runningCommand(session) : false;
  const [focus, setFocus] = useState<WorkbenchFocus | null>(null);
  const { config, modelName } = parts;
  const transcript = useMemo<TranscriptContextValue | null>(
    () =>
      session && {
        session,
        openFile: (path: string) => {
          setFocus({ tab: "changes", path, at: Date.now() });
          if (phone) setSheet("changes");
        },
        modelLabel: (provider: string, model: string) => {
          const name = modelName({ provider: provider as ModelChoice["provider"], model }) || model;
          const p = config?.providers.find((x) => x.provider === provider)?.label ?? provider;
          return p ? `${name} (${p})` : name;
        },
        providerOfModel: (model: string | null) => {
          if (!model || !config) return null;
          const bare = model.includes(":") ? model.split(":").pop()! : model;
          const entry = config.models.find((m) => m.id === bare || m.registry_key === model);
          const provider = entry?.provider ?? (/^claude/.test(bare) ? "anthropic" : /^gpt|^o\d/.test(bare) ? "openai" : /^gemini/.test(bare) ? "google" : null);
          return provider ? config.providers.find((p) => p.provider === provider)?.label ?? provider : null;
        },
        postureLabel: (id: string) => postureLabel(config, id),
        workerAnswering: session.status !== "worker_unresponsive",
        ...(config?.mcp?.enabled ? { openMcp: (server: string) => setMcp({ reconnect: server }) } : {}),
      },
    [session, config, modelName, phone],
  );
  const notices = (
    <>
      {parts.actionError && (
        <div className="notice" role="alert">
          <span>{parts.actionError}</span>
          <button type="button" className="btn ghost small" onClick={parts.clearActionError}>
            Dismiss
          </button>
        </div>
      )}
      {parts.mcpNotice && (
        <div className={`notice${parts.mcpNotice.ok ? "" : " err"}`} role={parts.mcpNotice.ok ? "status" : "alert"}>
          <span>{parts.mcpNotice.text}</span>
          <button type="button" className="btn ghost small" onClick={parts.clearMcpNotice}>
            Dismiss
          </button>
        </div>
      )}
    </>
  );

  const modal = drawer || (sheet !== null && !!session && phone) || (mcp !== null && mcpFeature !== null);

  // The agent applies (or refuses) a model or posture change when its handler ends; then the
  // session list carries the new value, so refetch it once per such reply.
  const settled = session
    ? session.messages.filter((m) => (m.handler === "set_model" || m.handler === "set_posture") && (m.status === "done" || m.status === "error")).length
    : 0;
  const { refresh } = parts;
  useEffect(() => {
    if (settled > 0) void refresh();
  }, [settled, refresh]);
  return (
    <div className="app">
      <div className="app-frame" inert={modal}>
      <Header
        title={title}
        demo={demo}
        status={session?.status ?? null}
        views={<ViewLinks views={views} />}
        tools={
          mcpFeature && (
            <button type="button" className="toggle" aria-haspopup="dialog" onClick={() => setMcp({ reconnect: null })}>
              MCP servers
            </button>
          )
        }
        presenter={presenter}
        onPresenter={togglePresenter}
        onMenu={() => setDrawer(true)}
      />
      {session ? <MetersStrip telemetry={session.telemetry} modelLabel={modelLabel} /> : <div className="meters" aria-hidden="true" />}

      <div className={`body${rail ? " rail" : ""}`}>
        <div className="slot-side">{rail ? <Rail {...sideProps} onExpand={() => setDrawer(true)} /> : <Sidebar {...sideProps} />}</div>

        <main className="slot-main" id="main">
          {/* Pinned above the transcript, so following new output never scrolls them away. */}
          <div className="pinned-slot" data-slot="pinned">
            {notices}
            {session?.connection === "error" && (
              <div className="notice" role="alert">
                <span>Lost the live connection to this session. Nothing is lost in Temporal.</span>
                <button type="button" className="btn secondary small" onClick={session.reconnect}>
                  Reconnect
                </button>
              </div>
            )}
            {session?.connection === "reconnecting" && <div className="sys-line">Reconnecting…</div>}
            {session && <WorkerBanner session={session} temporalUiTemplate={parts.config?.links?.temporal_ui?.url_template ?? null} />}
          </div>
          {session && transcript ? (
            <TranscriptPane value={transcript} />
          ) : (
            <div className="transcript-slot" data-slot="transcript">
              <div className="col">
                {page.kind === "landing" && <Landing creating={parts.creating} onCreate={parts.create} />}
                {page.kind === "opening" && <p className="hint">Opening the session…</p>}
                {page.kind === "missing" && <NotFound message={page.message} onBack={() => parts.select(null)} />}
              </div>
            </div>
          )}
          {session && (
            <div className="decision-slot" data-slot="decisions">
              <DecisionDock session={session} />
            </div>
          )}
          {session && phone && (
            <div className="slot-sheet" data-slot="sheet">
              <button type="button" className="sheet-grab" onClick={() => setSheet("changes")} aria-haspopup="dialog">
                Changes ({changesCount})
              </button>
              <button type="button" className="sheet-grab" onClick={() => setSheet("terminal")} aria-haspopup="dialog">
                Terminal
                {commandRunning && (
                  <>
                    <span aria-hidden="true"> ●</span>
                    <span className="sr-only"> (running)</span>
                  </>
                )}
              </button>
            </div>
          )}
          {session && (
            <div className="composer-slot" data-slot="composer">
              <Composer session={session} config={parts.config} />
            </div>
          )}
        </main>

        <div className="slot-bench" data-slot="workbench">
          {session && !phone ? <Workbench session={session} variant="panel" focus={focus} initialTab={focus ? "changes" : undefined} /> : null}
        </div>
      </div>
      </div>

      {drawer && (
        <Dialog className="drawer" label="Sessions and views" onClose={() => setDrawer(false)}>
          <div className="drawer-head">
            <span className="brand">
              <span className="brand-mark" aria-hidden="true">
                T
              </span>
              Coding agent
            </span>
            <span className="spacer" />
            <button type="button" className="btn ghost small" onClick={() => setDrawer(false)}>
              Close
            </button>
          </div>
          <div style={{ display: "grid", gridTemplateRows: "minmax(0,1fr) auto", minHeight: 0 }}>
            <Sidebar {...sideProps} />
            <ViewLinks views={views} className="drawer-views" />
          </div>
        </Dialog>
      )}

      {mcp && mcpFeature && <McpDialog feature={mcpFeature} reconnect={mcp.reconnect} onClose={() => setMcp(null)} />}

      {sheet && session && phone && (
        <Dialog className="sheet" label="Workbench" onClose={() => setSheet(null)}>
          <div className="drawer-head">
            <span>Workbench</span>
            <span className="spacer" />
            <button type="button" className="btn ghost small" onClick={() => setSheet(null)}>
              Close
            </button>
          </div>
          <Workbench session={session} variant="sheet" focus={focus} initialTab={sheet ?? "changes"} />
        </Dialog>
      )}
    </div>
  );
}

/** "Signed in through the catalog · sessions are kept 15 days" (wireframe sidebar footer). */
export function footerText(username: string, mode: string, ttlDays: number | null): string {
  const who = mode === "catalog" ? `Signed in as ${username} through the catalog` : `Signed in as ${username}`;
  if (ttlDays === null || !(ttlDays > 0)) return who;
  return `${who} · sessions are kept ${ttlDays} ${ttlDays === 1 ? "day" : "days"}`;
}

/** The meters' model: the one the turn runs on, and "next: …" while a change waits. */
export function meterModelLabel(
  session: Pick<CodingSession, "model" | "modelInUse" | "modelAppliesNextTurn">,
  name: (c: Partial<ModelChoice> | null | undefined, withProvider?: boolean) => string,
): string | null {
  const now = name(session.modelInUse ?? session.model, true);
  if (!now) return null;
  if (!session.modelAppliesNextTurn || !session.model) return now;
  const next = name(session.model);
  return next && next !== name(session.modelInUse) ? `${now} · next: ${next}` : now;
}

function runningCommand(session: CodingSession): boolean {
  const agents = [session.rootAgentId ? session.agent(session.rootAgentId) : undefined, ...Object.values(session.subagents)];
  return agents.some((a) =>
    a?.messages.some((m) => m.parts.some((p) => p.type === "tool" && p.toolName === "run_command" && p.state === "running")),
  );
}

function NotFound({ message, onBack }: { message: string; onBack(): void }) {
  return (
    <div className="empty" role="alert">
      <h1>{message}</h1>
      <p>It doesn't exist, or it belongs to someone else.</p>
      <div className="acts">
        <a
          className="btn secondary"
          href={`${import.meta.env.BASE_URL}`}
          onClick={(e) => {
            e.preventDefault();
            onBack();
          }}
        >
          Back to your sessions
        </a>
      </div>
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
          {creating === "demo" ? (
            "Setting up the demo…"
          ) : (
            <>
              <span aria-hidden="true">+ </span>New demo session
            </>
          )}
        </button>
        <button type="button" className="btn ghost" disabled={creating !== null} onClick={() => void onCreate("empty")}>
          {creating === "empty" ? "Starting…" : "New empty session"}
        </button>
      </div>
    </div>
  );
}
