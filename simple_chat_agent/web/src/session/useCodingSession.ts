// useCodingSession: the facade over @temporalio/agent-harness-react (PRD U1). This folder is
// the only place that imports the harness packages.

import {
  createContext,
  createElement,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";
import { HarnessProvider, HttpTransport, SessionStore, useAgentSession } from "@temporalio/agent-harness-react";

import { AGENT_API_BASE, CLIENT_HEADERS } from "./api";
import {
  alwaysApproved as deriveAlwaysApproved,
  currentModel,
  currentPosture,
  modelAppliesNextTurn as deriveModelAppliesNextTurn,
  sessionStatus,
  subagentsOf,
  toolIndex,
} from "./derive";
import { advance, landed, observe, startRecord, stuckInTemporal } from "./sendStates";
import { TelemetryFolder } from "./telemetry";
import type {
  ApproveOptions,
  ChangesState,
  CodingSession,
  ModelChoice,
  PlanState,
  Posture,
  SendKind,
  SendRecord,
  SessionSeed,
  WorkspaceState,
} from "./types";

/** One transport for the app: our API's harness routes, same-origin. */
const transport = new HttpTransport({ baseUrl: AGENT_API_BASE, headers: CLIENT_HEADERS });

// -- provider --------------------------------------------------------------------------------

interface Reconnector {
  reconnect(): void;
}
const ReconnectContext = createContext<Reconnector>({ reconnect: () => undefined });

/** Put once above the app. Shares one connection per session id, and lets `reconnect()`
 *  replace a connection that gave up. */
export function CodingSessionProvider({ children }: { children?: ReactNode }) {
  const [store, setStore] = useState(() => new SessionStore());
  const reconnector = useMemo<Reconnector>(() => ({ reconnect: () => setStore(new SessionStore()) }), []);
  return createElement(
    ReconnectContext.Provider,
    { value: reconnector },
    createElement(HarnessProvider, { store }, children),
  );
}

// -- the hook ----------------------------------------------------------------------------------

let localIds = 0;
const nextLocalId = () => `send-${Date.now().toString(36)}-${++localIds}`;

const asObject = <T,>(v: unknown): T | null => (typeof v === "object" && v !== null ? (v as T) : null);

export function useCodingSession(sessionId: string, seed: SessionSeed = {}): CodingSession {
  const { reconnect } = useContext(ReconnectContext);
  // The core reports a failed submit through onError just before sendMessage resolves null.
  const lastError = useRef<string | null>(null);
  const h = useAgentSession({
    sessionId,
    transport,
    onError: (error: Error) => {
      lastError.current = error.message;
    },
  });
  const latest = useRef(h);
  latest.current = h;

  // Telemetry: folded incrementally from the frames the harness keeps.
  const folder = useRef<{ id: string; folder: TelemetryFolder } | null>(null);
  if (folder.current?.id !== sessionId) folder.current = { id: sessionId, folder: new TelemetryFolder() };
  const activeFolder = folder.current.folder;
  const telemetry = useMemo(() => activeFolder.update(h.frames), [activeFolder, h.frames]);

  // Send records, per session.
  const [sendState, setSendState] = useState<{ id: string; records: readonly SendRecord[] }>({ id: sessionId, records: [] });
  const sends = sendState.id === sessionId ? sendState.records : [];

  const update = useCallback(
    (localId: string, fn: (r: SendRecord) => SendRecord) => {
      setSendState((s) => {
        if (s.id !== sessionId) return s;
        const records = s.records.map((r) => (r.id === localId ? fn(r) : r));
        return { id: s.id, records };
      });
    },
    [sessionId],
  );
  const add = useCallback(
    (record: SendRecord) => {
      setSendState((s) => {
        const records = [...(s.id === sessionId ? s.records : []), record];
        return { id: sessionId, records };
      });
    },
    [sessionId],
  );

  // Advance records from what the stream shows.
  const tools = useMemo(() => toolIndex(h.agents), [h.agents]);
  const rootMessages = h.messages;
  useEffect(() => {
    const byId = new Map(rootMessages.map((m) => [m.id, m]));
    const rejections = new Map(telemetry.rejections.map((r) => [r.requestId, r.message]));
    setSendState((s) => {
      if (s.id !== sessionId) return s;
      const records = observe(
        s.records,
        { message: (id) => byId.get(id), tool: (id) => tools.get(id), rejection: (id) => rejections.get(id) },
        Date.now(),
      );
      if (records === s.records) return s;
      return { id: s.id, records };
    });
  }, [rootMessages, tools, telemetry.rejections, sessionId, sends.length]);

  // A clock that ticks only while something waits in Temporal, for "worker isn't answering".
  const [now, setNow] = useState(() => Date.now());
  const waiting = sends.some((r) => r.phase === "in_temporal");
  useEffect(() => {
    if (!waiting) return;
    const timer = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(timer);
  }, [waiting]);

  // -- actions ---------------------------------------------------------------------------------

  const runMessage = useCallback(
    async (kind: SendKind, handler: string, payload: Record<string, unknown>): Promise<SendRecord> => {
      const record = startRecord(nextLocalId(), kind, Date.now());
      add(record);
      lastError.current = null;
      const reply = await latest.current.sendMessage(handler, payload);
      const at = Date.now();
      const error = lastError.current ?? "Couldn't send. Try again.";
      const settle = (r: SendRecord) => (reply ? landed(r, at, reply.message_id) : advance(r, "failed", at, error));
      update(record.id, settle);
      return settle(record);
    },
    [add, update],
  );

  const answer = useCallback(
    async (toolIds: readonly string[], approved: boolean, options: ApproveOptions = {}): Promise<SendRecord[]> => {
      const remember = approved && options.remember === true;
      const records = toolIds.map((toolId) => startRecord(nextLocalId(), "approval", Date.now(), { toolId, approved, remember }));
      for (const r of records) add(r);
      return Promise.all(
        records.map(async (record) => {
          lastError.current = null;
          const ok = await latest.current.respondToApproval(record.toolId ?? "", {
            approved,
            remember,
            reason: options.reason?.trim() ? options.reason.trim() : null,
          });
          const at = Date.now();
          const error = lastError.current ?? "Couldn't send. Try again.";
          const settle = (r: SendRecord) => (ok ? landed(r, at) : advance(r, "failed", at, error));
          update(record.id, settle);
          return settle(record);
        }),
      );
    },
    [add, update],
  );

  // -- derived ---------------------------------------------------------------------------------

  const states = h.states as Record<string, unknown>;
  const subagents = useMemo(() => subagentsOf(h.agents, h.rootAgentId), [h.agents, h.rootAgentId]);
  const anyBusy = h.agentStatus === "busy" || Object.values(subagents).some((a) => a.agentStatus === "busy");
  const model = useMemo(() => currentModel(h.messages, seed.model ?? null), [h.messages, seed.model]);
  const posture = useMemo(() => currentPosture(h.messages, seed.posture ?? null), [h.messages, seed.posture]);
  const always = useMemo(() => deriveAlwaysApproved(h.agents), [h.agents]);

  const session: CodingSession = {
    sessionId,
    connection: h.connection,
    error: h.error?.message ?? null,
    clearError: () => latest.current.clearError(),
    reconnect,

    status: sessionStatus({
      pendingApprovals: h.pendingApprovals,
      anyBusy,
      worker: telemetry.worker,
      waitingForWorker: stuckInTemporal(sends, now),
    }),
    agentStatus: h.agentStatus,
    anyBusy,

    messages: h.messages,
    unsent: h.pending,
    rootAgentId: h.rootAgentId,
    subagents,
    agent: (id: string) => latest.current.agent(id),

    plan: asObject<PlanState>(states.plan),
    changes: asObject<ChangesState>(states.changes),
    workspace: asObject<WorkspaceState>(states.workspace),

    pendingApprovals: h.pendingApprovals,
    sends,
    sendFor: ({ messageId, toolId }) => {
      for (let i = sends.length - 1; i >= 0; i--) {
        const r = sends[i];
        if (r && ((messageId && r.messageId === messageId) || (toolId && r.toolId === toolId))) return r;
      }
      return undefined;
    },

    telemetry,

    model,
    modelAppliesNextTurn: deriveModelAppliesNextTurn(h.messages, h.agentStatus),
    posture,
    alwaysApproved: always,

    send: (text: string) => runMessage("message", "chat", { text }),
    steer: (text: string) => runMessage("steer", "steer", { text }),
    interrupt: (reason?: string) => runMessage("interrupt", "interrupt", reason ? { reason } : {}),
    setModel: (choice: ModelChoice) => runMessage("set_model", "set_model", { model: { ...choice } }),
    setPosture: (p: Posture) => runMessage("set_posture", "set_posture", { posture: p }),
    approve: (toolIds, options) => answer(toolIds, true, options),
    decline: (toolIds, reason) => answer(toolIds, false, reason === undefined ? {} : { reason }),
  };
  return session;
}
