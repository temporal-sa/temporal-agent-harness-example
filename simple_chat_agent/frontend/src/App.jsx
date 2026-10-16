import { useEffect, useLayoutEffect, useRef, useState } from "react";

import { ArtifactViewer, ArtifactsPanel } from "./components/Artifacts.jsx";
import { AppHeader } from "./components/AppHeader.jsx";
import { Composer } from "./components/Composer.jsx";
import { LoginScreen } from "./components/LoginScreen.jsx";
import { Messages, TurnTraceDrawer } from "./components/Messages.jsx";
import { ToolsWindow } from "./components/ToolsWindow.jsx";
import { artifactNeedsTextFetch, artifactPreviewKind } from "./utils/artifacts.js";
import { jsonHeaders, responseErrorText } from "./utils/http.js";
import { mcpOAuthStartUrl } from "./utils/tools.js";
import {
  applyReplayedStreamEventsInState,
  applyWorkflowStatePatchInState,
  agentSettingsFromConfig,
  applyTranscriptDeltasInState,
  createPendingMessage,
  displayStatus,
  handleStreamEventInState,
  markStreamInterruptedInState,
  mergeReplayedTurnTracesInState,
  normalizeAgentSettings,
  prependTranscriptPageInState,
  restoreActiveStreamTurnInState,
  saveAgentSettings,
  setTurnTraceErrorInState,
  setTurnTraceLoadingInState,
  streamEventNeedsWorkflowStateRefresh,
  temporalUiUrl,
  toggleExpandedTraceInState,
  updateWorkflowStateInState,
} from "./state/chatState.js";
import {
  defaultSystemPrompt,
  defaultMcpFormValues,
  emptyArtifactViewer,
  initialState,
} from "./state/initialState.js";
import {
  coalesceBufferedStreamEvents,
  coalesceStreamEvents,
  latestBufferedStreamCursor,
  takeBufferedStreamEventBatch,
} from "./state/streamBuffer.js";
import { AgentStreamEventKind } from "./state/streamEvents.js";
import {
  TURN_STREAM_FIRST_EVENT_TIMEOUT_MS,
  turnStreamNeedsRecovery,
  turnStreamRecoveryDelay,
} from "./state/turnStream.js";

const TURN_TRACE_CACHE_PREFIX = "simpleChatTurnTraces:v2:";
const TURN_TRACE_CACHE_LIMIT = 12;
const STREAM_SESSION_CACHE_PREFIX = "simpleChatStreamSession:v2:";

function turnTraceCacheKey(workflowId) {
  return `${TURN_TRACE_CACHE_PREFIX}${workflowId}`;
}

function streamSessionCacheKey(workflowId) {
  return `${STREAM_SESSION_CACHE_PREFIX}${workflowId}`;
}

function transcriptDeltaResultStillLive(result) {
  if (!result) return true;
  if (result.status === "responding" || result.status === "starting") return true;
  if (Number(result.pending_messages || 0) > 0) return true;
  return result.active_message_index !== null && result.active_message_index !== undefined;
}

function workflowHasActiveRun(workflowState) {
  if (!workflowState) return false;
  if (workflowState.status === "responding" || workflowState.status === "starting") {
    return true;
  }
  if (Number(workflowState.pending_messages || 0) > 0) return true;
  return (
    workflowState.active_message_index !== null &&
    workflowState.active_message_index !== undefined
  );
}

function loadCachedTurnTraces(workflowId) {
  if (!workflowId) return {};
  try {
    const raw = localStorage.getItem(turnTraceCacheKey(workflowId));
    if (!raw) return {};
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch {
    return {};
  }
}

function saveCachedTurnTraces(workflowId, turnTraces) {
  if (!workflowId) return;
  const entries = Object.entries(turnTraces || {})
    .filter(([, trace]) => trace?.turn)
    .sort(([left], [right]) => Number(right) - Number(left))
    .slice(0, TURN_TRACE_CACHE_LIMIT)
    .sort(([left], [right]) => Number(left) - Number(right));
  try {
    localStorage.setItem(
      turnTraceCacheKey(workflowId),
      JSON.stringify(Object.fromEntries(entries)),
    );
  } catch {
    // Browser cache is best-effort; traces are still available while in memory.
  }
}

function loadCachedStreamSession(workflowId) {
  if (!workflowId) return {};
  try {
    const raw = localStorage.getItem(streamSessionCacheKey(workflowId));
    if (!raw) return {};
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch {
    return {};
  }
}

function saveCachedStreamSession(workflowId, snapshot) {
  if (!workflowId || !snapshot?.streamTurn) return;
  try {
    localStorage.setItem(
      streamSessionCacheKey(workflowId),
      JSON.stringify({
        cursor: snapshot.cursor || "",
        streamTurn: trimCachedStreamTurn(snapshot.streamTurn),
        currentAgentSequence: snapshot.currentAgentSequence ?? null,
        ignoreAgentUntilStart: Boolean(snapshot.ignoreAgentUntilStart),
        updatedAt: new Date().toISOString(),
      }),
    );
  } catch {
    // Browser cache is best-effort; the live stream still owns current state.
  }
}

function clearCachedStreamSession(workflowId) {
  if (!workflowId) return;
  try {
    localStorage.removeItem(streamSessionCacheKey(workflowId));
  } catch {
    // Ignore storage failures; cache cleanup is not required for correctness.
  }
}

function trimCachedStreamTurn(turn) {
  return {
    ...turn,
    segments: (turn.segments || []).map((segment) => {
      const next = { ...segment };
      if (typeof next.text === "string" && next.text.length > 120_000) {
        next.text = next.text.slice(-120_000);
      }
      if (typeof next.thinking === "string" && next.thinking.length > 120_000) {
        next.thinking = next.thinking.slice(-120_000);
      }
      if (Array.isArray(next.events)) {
        next.events = next.events.slice(-12).map(trimCachedStreamEvent);
      }
      return next;
    }),
  };
}

function trimCachedStreamEvent(event) {
  const next = {
    ...event,
    payload: { ...(event.payload || {}) },
  };
  for (const key of ["text", "input_partial", "partial_json"]) {
    if (typeof next.payload[key] === "string" && next.payload[key].length > 80_000) {
      next.payload[key] = next.payload[key].slice(-80_000);
    }
  }
  return next;
}

export default function App() {
  const [state, setState] = useState(initialState);
  const [composerResetToken, setComposerResetToken] = useState(0);
  const stateRef = useRef(state);
  const messageRef = useRef("");
  const turnStreamSourceRef = useRef(null);
  const turnStreamFinishRef = useRef(null);
  const turnStreamTokenRef = useRef(0);
  const activeTurnStreamWorkflowRef = useRef(null);
  const streamCursorRef = useRef("");
  const messagesRef = useRef(null);
  const pinnedToBottomRef = useRef(true);
  const stateLoadRequestRef = useRef(0);
  const restoreScrollAfterPrependRef = useRef(null);
  const olderMessagesRequestRef = useRef(false);
  const workflowStateRefreshTimerRef = useRef(null);
  const workflowStateRefreshRequestRef = useRef(0);
  const streamEventBufferRef = useRef([]);
  const streamEventFrameRef = useRef(null);
  const turnLatencyRef = useRef(null);
  const turnStreamRecoveryAttemptRef = useRef(0);

  useEffect(() => {
    stateRef.current = state;
  }, [state]);

  useEffect(() => {
    if (!state.workflowId) return;
    saveCachedTurnTraces(state.workflowId, state.turnTraces);
  }, [state.workflowId, state.turnTraces]);

  useEffect(() => {
    if (!state.workflowId) return;
    if (state.streamTurn) {
      saveCachedStreamSession(state.workflowId, {
        cursor: streamCursorRef.current,
        streamTurn: state.streamTurn,
        currentAgentSequence: state.currentAgentSequence,
        ignoreAgentUntilStart: state.ignoreAgentUntilStart,
      });
    } else {
      clearCachedStreamSession(state.workflowId);
    }
  }, [
    state.workflowId,
    state.streamTurn,
    state.currentAgentSequence,
    state.ignoreAgentUntilStart,
  ]);

  useEffect(() => {
    const run = { cancelled: false };
    boot(run);
    return () => {
      run.cancelled = true;
      clearWorkflowStateRefresh();
      clearStreamEventFlush();
      clearLiveStreamState();
      closeTurnStream();
    };
  }, []);

  useEffect(() => {
    function handleVisibilityChange() {
      if (document.hidden) return;
      const current = stateRef.current;
      if (current.auth === "app" && current.workflowId) {
        // Browsers may keep delivering SSE callbacks while suspending animation
        // frames in a background tab. Apply anything already received before
        // reconnecting, then replay Redis so events the browser never delivered
        // are folded back into the live projection as well.
        flushStreamEventsNow();
        closeTurnStream();
        reconcileWorkflow(current.workflowId, {
          resumeActive: true,
          restoreActiveStream: true,
        });
      }
    }

    document.addEventListener("visibilitychange", handleVisibilityChange);
    return () => {
      document.removeEventListener("visibilitychange", handleVisibilityChange);
    };
  }, []);

  useLayoutEffect(() => {
    const messages = messagesRef.current;
    if (!messages) return;
    if (restoreScrollAfterPrependRef.current !== null) {
      messages.scrollTop = messages.scrollHeight - restoreScrollAfterPrependRef.current;
      restoreScrollAfterPrependRef.current = null;
      return;
    }
    if (pinnedToBottomRef.current) {
      messages.scrollTop = messages.scrollHeight;
    }
  }, [
    state.workflowState,
    state.localPending,
    state.streamTurn,
    state.turnTraces,
    state.expandedTraceIndex,
    state.draftConversation,
  ]);

  async function boot(run) {
    try {
      const user = await fetchCurrentUser();
      if (!user || run.cancelled) {
        showLogin();
        return;
      }

      const [config, tools, conversations] = await Promise.all([
        loadConfigData(),
        loadToolsData(),
        loadConversationsData(),
      ]);
      if (run.cancelled) return;

      const agentSettings = agentSettingsFromConfig(config);
      setState((previous) => ({
        ...previous,
        auth: "app",
        user,
        config,
        tools,
        conversations,
        agentSettings,
        statusNotice: "",
      }));

      const savedWorkflowId = localStorage.getItem("simpleChatWorkflowId");
      const savedConversation = conversations.find(
        (conversation) => conversation.workflow_id === savedWorkflowId,
      );
      const conversation = savedConversation || conversations[0];
      if (conversation) {
        selectConversation(conversation.workflow_id, {}, conversations);
      } else {
        startDraftConversation();
      }
      showOAuthCallbackStatus();
    } catch (error) {
      setStatusNotice(`failed: ${error}`);
    }
  }

  async function fetchCurrentUser() {
    const response = await fetch("/api/me");
    if (response.status === 401) return null;
    if (!response.ok) throw new Error(await response.text());
    return response.json();
  }

  function clearLiveStreamState() {
    clearWorkflowStateRefresh();
    clearStreamEventFlush();
  }

  function closeTurnStream() {
    turnStreamTokenRef.current += 1;
    activeTurnStreamWorkflowRef.current = null;
    const finish = turnStreamFinishRef.current;
    turnStreamFinishRef.current = null;
    if (finish) finish();
    if (!turnStreamSourceRef.current) return;
    turnStreamSourceRef.current.close();
    turnStreamSourceRef.current = null;
  }

  function beginTurnStream(workflowId) {
    flushStreamEventsNow();
    closeTurnStream();
    clearLiveStreamState();
    turnStreamTokenRef.current += 1;
    const token = turnStreamTokenRef.current;
    activeTurnStreamWorkflowRef.current = workflowId;
    return token;
  }

  function clearStreamEventFlush() {
    streamEventBufferRef.current = [];
    if (streamEventFrameRef.current === null) return;
    window.cancelAnimationFrame(streamEventFrameRef.current);
    streamEventFrameRef.current = null;
  }

  function enqueueStreamEvent(workflowId, event, cursor = "") {
    streamEventBufferRef.current.push({ workflowId, event, cursor });
    if (streamEventFrameRef.current !== null) return;
    streamEventFrameRef.current = window.requestAnimationFrame(flushStreamEvents);
  }

  function flushStreamEventsNow() {
    if (streamEventFrameRef.current !== null) {
      window.cancelAnimationFrame(streamEventFrameRef.current);
      streamEventFrameRef.current = null;
    }
    flushStreamEvents();
  }

  function flushStreamEvents() {
    streamEventFrameRef.current = null;
    const pending = takeBufferedStreamEventBatch(streamEventBufferRef.current);
    if (!pending.length) return;
    const coalesced = coalesceBufferedStreamEvents(pending);

    const activeWorkflowId = stateRef.current.workflowId;
    const appliedCursor = latestBufferedStreamCursor(pending, activeWorkflowId);

    setState((previous) => {
      let next = previous;
      for (const { workflowId, event } of coalesced) {
        if (workflowId && next.workflowId !== workflowId) continue;
        next = handleStreamEventInState(next, event);
      }
      return next;
    });
    if (appliedCursor) streamCursorRef.current = appliedCursor;
    recordFirstRenderedToken(coalesced);

    if (streamEventBufferRef.current.length) {
      streamEventFrameRef.current = window.requestAnimationFrame(flushStreamEvents);
    }
  }

  function clearWorkflowStateRefresh() {
    workflowStateRefreshRequestRef.current += 1;
    if (!workflowStateRefreshTimerRef.current) return;
    clearTimeout(workflowStateRefreshTimerRef.current);
    workflowStateRefreshTimerRef.current = null;
  }

  function scheduleWorkflowStateRefresh(workflowId, options = {}) {
    clearWorkflowStateRefresh();
    const attempt = options.attempt || 0;
    const delay = options.delay ?? 250;
    workflowStateRefreshTimerRef.current = setTimeout(() => {
      workflowStateRefreshTimerRef.current = null;
      if (document.hidden || stateRef.current.workflowId !== workflowId) return;
      refreshWorkflowStatePatch(workflowId, { attempt }).catch((error) => {
        if (stateRef.current.workflowId !== workflowId) return;
        setStatusNotice(`state refresh failed: ${error}`);
      });
    }, delay);
  }

  function nextStateLoadRequest() {
    stateLoadRequestRef.current += 1;
    return stateLoadRequestRef.current;
  }

  function updateComposerMessage(value) {
    messageRef.current = value;
  }

  function clearComposerInput() {
    messageRef.current = "";
    setComposerResetToken((token) => token + 1);
  }

  function showLogin() {
    closeTurnStream();
    clearLiveStreamState();
    nextStateLoadRequest();
    const params = new URLSearchParams(window.location.search);
    const loginError = params.has("oauth_error") ? params.get("oauth_error") || "" : "";
    if (params.has("oauth_error")) history.replaceState({}, "", "/");
    setState((previous) => ({
      ...previous,
      auth: "login",
      loginError,
      loggingIn: false,
      statusNotice: "",
    }));
    configureAuth();
  }

  async function configureAuth() {
    try {
      const response = await fetch("/api/auth/config");
      if (!response.ok) throw new Error(await response.text());
      const body = await response.json();
      const mode = body.mode || "none";
      setState((previous) => ({
        ...previous,
        authMode: mode,
        loginConfigured: mode === "catalog" || mode === "local",
        loginSubtitle: loginSubtitleForMode(mode),
      }));
    } catch (error) {
      setState((previous) => ({
        ...previous,
        authMode: "none",
        loginConfigured: false,
        loginSubtitle: `Could not check auth config: ${error}`,
      }));
    }
  }

  function loginSubtitleForMode(mode) {
    if (mode === "local") return "Local testing mode. Use demo/demo unless overridden.";
    if (mode === "catalog") return "Open this demo from the catalog to sign in.";
    return "Enable local testing auth to run this demo locally.";
  }

  async function loadConversationsData() {
    const response = await fetch("/api/conversations");
    if (response.status === 401) {
      showLogin();
      return [];
    }
    if (!response.ok) throw new Error(await response.text());
    const body = await response.json();
    return body.conversations || [];
  }

  async function loadToolsData() {
    const response = await fetch("/api/tools");
    if (response.status === 401) {
      showLogin();
      return [];
    }
    if (!response.ok) throw new Error(await response.text());
    const body = await response.json();
    return body.tools || [];
  }

  async function loadConfigData() {
    const response = await fetch("/api/config");
    if (response.status === 401) {
      showLogin();
      return {};
    }
    if (!response.ok) throw new Error(await response.text());
    return response.json();
  }

  async function loadWorkflowStateData(workflowId) {
    const started = performance.now();
    const response = await fetch(
      `/api/sessions/${encodeURIComponent(workflowId)}/snapshot?limit=60`,
    );
    if (response.status === 401) {
      showLogin();
      return null;
    }
    if (response.status === 404) {
      await handleMissingWorkflow();
      return null;
    }
    if (!response.ok) throw new Error(await responseErrorText(response));
    const workflowState = await response.json();
    logSessionLoadTiming("snapshot", workflowId, started, response, workflowState);
    return {
      workflowState,
      streamCursor: response.headers.get("x-stream-cursor") || "",
    };
  }

  async function loadActiveStreamReplayData(workflowId, options = {}) {
    const events = [];
    let cursor = options.cursor || "";
    for (let page = 0; page < 50; page += 1) {
      const params = new URLSearchParams({ limit: "5000" });
      if (cursor) params.set("cursor", cursor);
      const response = await fetch(
        `/api/sessions/${encodeURIComponent(workflowId)}/stream/events?${params}`,
        { headers: { "Cache-Control": "no-cache" } },
      );
      if (response.status === 401) {
        showLogin();
        return null;
      }
      if (response.status === 404) {
        await handleMissingWorkflow();
        return null;
      }
      if (!response.ok) throw new Error(await responseErrorText(response));
      const replay = await response.json();
      if (!replay.replay_available) return replay;
      events.push(...(replay.events || []));
      const nextCursor = replay.cursor || cursor;
      if (!replay.has_more || nextCursor === cursor) {
        return { ...replay, cursor: nextCursor, events };
      }
      cursor = nextCursor;
    }
    throw new Error("active stream replay exceeded 250,000 retained events");
  }

  function logSessionLoadTiming(label, workflowId, started, response, workflowState) {
    const elapsed = Math.round(performance.now() - started);
    const serverTiming = response.headers.get("server-timing") || "";
    console.debug("conversation load", {
      label,
      workflowId,
      elapsed,
      serverTiming,
      transcriptStart: workflowState?.transcript_offset ?? 0,
      transcriptCount: workflowState?.transcript?.length ?? 0,
      transcriptTotal: workflowState?.transcript_total ?? 0,
    });
  }

  async function refreshTools() {
    const tools = await loadToolsData();
    setState((previous) => ({ ...previous, tools }));
  }

  async function refreshConversations() {
    const conversations = await loadConversationsData();
    setState((previous) => ({ ...previous, conversations }));
    return conversations;
  }

  function startDraftConversation() {
    closeTurnStream();
    clearLiveStreamState();
    if (stateRef.current.workflowId) clearCachedStreamSession(stateRef.current.workflowId);
    streamCursorRef.current = "";
    nextStateLoadRequest();
    olderMessagesRequestRef.current = false;
    restoreScrollAfterPrependRef.current = null;
    localStorage.removeItem("simpleChatWorkflowId");
    setState((previous) => ({
      ...previous,
      workflowId: null,
      runId: null,
      temporalUiUrl: null,
      workflowState: null,
      workflowStateProjectionRevision: 0,
      workflowTranscriptProjectionRevision: 0,
      olderMessagesLoading: false,
      olderMessagesError: "",
      streamTurn: null,
      turnTraces: {},
      expandedTraceIndex: null,
      streamPanelCollapsed: false,
      currentAgentSequence: null,
      ignoreAgentUntilStart: false,
      localPending: [],
      composerAttachments: [],
      attachmentUploading: false,
      attachmentError: "",
      resolvingApprovals: new Set(),
      draftConversation: true,
      draftSystemPrompt: defaultSystemPrompt,
      artifactViewer: emptyArtifactViewer,
      statusNotice: "",
    }));
  }

  function selectConversation(workflowId, options = {}, conversationsArg = null) {
    const conversations = conversationsArg || stateRef.current.conversations;
    const conversation = conversations.find((item) => item.workflow_id === workflowId);
    if (!conversation) return;
    closeTurnStream();
    clearLiveStreamState();
    const cachedStreamSession = loadCachedStreamSession(conversation.workflow_id);
    streamCursorRef.current = cachedStreamSession.cursor || "";
    const loadRequest = nextStateLoadRequest();
    olderMessagesRequestRef.current = false;
    restoreScrollAfterPrependRef.current = null;
    localStorage.setItem("simpleChatWorkflowId", conversation.workflow_id);
    setState((previous) => ({
      ...previous,
      conversations,
      workflowId: conversation.workflow_id,
      runId: conversation.run_id,
      temporalUiUrl: temporalUiUrl(conversation),
      workflowState: null,
      workflowStateProjectionRevision: 0,
      workflowTranscriptProjectionRevision: 0,
      olderMessagesLoading: false,
      olderMessagesError: "",
      streamTurn: cachedStreamSession.streamTurn || null,
      turnTraces: loadCachedTurnTraces(conversation.workflow_id),
      expandedTraceIndex: null,
      streamPanelCollapsed: false,
      currentAgentSequence: cachedStreamSession.currentAgentSequence ?? null,
      ignoreAgentUntilStart: Boolean(cachedStreamSession.ignoreAgentUntilStart),
      draftConversation: false,
      draftSystemPrompt: defaultSystemPrompt,
      resolvingApprovals: new Set(),
      localPending: options.preserveLocalPending ? previous.localPending : [],
      composerAttachments: [],
      attachmentUploading: false,
      attachmentError: "",
      artifactViewer: emptyArtifactViewer,
      statusNotice: "",
    }));
    loadWorkflowStateAndConnect(conversation.workflow_id, loadRequest, {
      resumeActive: true,
      restoreActiveStream: true,
    }).catch((error) => {
      if (stateLoadRequestRef.current !== loadRequest) return;
      setStatusNotice(`state load failed: ${error}`);
    });
  }

  async function loadWorkflowStateAndConnect(workflowId, loadRequest, options = {}) {
    const loaded = await loadWorkflowStateData(workflowId);
    if (!loaded) return;
    if (
      stateLoadRequestRef.current !== loadRequest ||
      stateRef.current.workflowId !== workflowId
    ) {
      return;
    }
    setState((previous) =>
      previous.workflowId === workflowId
        ? updateWorkflowStateInState(previous, loaded.workflowState)
        : previous,
    );
    const activeRun = workflowHasActiveRun(loaded.workflowState);
    if (!activeRun && !streamCursorRef.current) {
      streamCursorRef.current = loaded.streamCursor;
    }
    if (options.resumeActive && activeRun) {
      let resumeCursor = streamCursorRef.current || loaded.streamCursor;
      if (options.restoreActiveStream) {
        try {
          const replayStartCursor = streamCursorRef.current;
          const replay = await loadActiveStreamReplayData(workflowId, {
            cursor: replayStartCursor,
          });
          if (
            stateLoadRequestRef.current !== loadRequest ||
            stateRef.current.workflowId !== workflowId
          ) {
            return;
          }
          if (replay?.replay_available) {
            resumeCursor = replay.cursor || resumeCursor;
            streamCursorRef.current = resumeCursor;
            const replayEvents = coalesceStreamEvents(replay.events || []);
            setState((previous) =>
              previous.workflowId === workflowId
                ? previous.streamTurn && replayStartCursor
                  ? applyReplayedStreamEventsInState(previous, replayEvents)
                  : restoreActiveStreamTurnInState(previous, replayEvents)
                : previous,
            );
          } else if (replay?.cursor) {
            resumeCursor = replay.cursor;
            streamCursorRef.current = replay.cursor;
          }
        } catch (error) {
          console.warn("active stream replay failed", { workflowId, error });
        }
      }
      resumeActiveTurnStream(workflowId, {
        cursor: resumeCursor,
        afterRevision: loaded.workflowState?.transcript_revision || 0,
      }).catch((error) => {
        setStatusNotice(`stream resume failed: ${error}`);
      });
    }
  }

  function reconcileWorkflow(workflowId, options = {}) {
    if (!workflowId) return;
    flushStreamEventsNow();
    clearWorkflowStateRefresh();
    const loadRequest = nextStateLoadRequest();
    loadWorkflowStateAndConnect(workflowId, loadRequest, options).catch((error) => {
      if (stateLoadRequestRef.current !== loadRequest) return;
      setStatusNotice(`state reconcile failed: ${error}`);
    });
  }

  function deferReconcileWorkflow(workflowId, options = {}) {
    window.setTimeout(() => {
      if (stateRef.current.workflowId === workflowId) {
        reconcileWorkflow(workflowId, options);
      }
    }, 0);
  }

  async function loadOlderMessages() {
    const current = stateRef.current;
    const workflowState = current.workflowState;
    if (
      !current.workflowId ||
      !workflowState ||
      current.olderMessagesLoading ||
      olderMessagesRequestRef.current ||
      !workflowState.transcript_has_more_before
    ) {
      return;
    }

    const before = Number(workflowState.transcript_offset || 0);
    if (before <= 0) return;

    const messages = messagesRef.current;
    restoreScrollAfterPrependRef.current = messages
      ? messages.scrollHeight - messages.scrollTop
      : null;
    olderMessagesRequestRef.current = true;

    setState((previous) => ({
      ...previous,
      olderMessagesLoading: true,
      olderMessagesError: "",
    }));

    try {
      const started = performance.now();
      const response = await fetch(
        `/api/sessions/${encodeURIComponent(current.workflowId)}/messages?before=${before}&limit=60`,
      );
      if (response.status === 401) {
        setState((previous) => ({ ...previous, olderMessagesLoading: false }));
        showLogin();
        return;
      }
      if (response.status === 404) {
        setState((previous) => ({ ...previous, olderMessagesLoading: false }));
        await handleMissingWorkflow();
        return;
      }
      if (!response.ok) throw new Error(await responseErrorText(response));
      const page = await response.json();
      console.debug("conversation load", {
        label: "messages",
        workflowId: current.workflowId,
        elapsed: Math.round(performance.now() - started),
        serverTiming: response.headers.get("server-timing") || "",
        transcriptStart: page.start,
        transcriptCount: page.messages?.length || 0,
        transcriptTotal: page.total,
      });
      setState((previous) =>
        previous.workflowId === current.workflowId
          ? prependTranscriptPageInState(previous, page)
          : previous,
      );
    } catch (error) {
      restoreScrollAfterPrependRef.current = null;
      setState((previous) => ({
        ...previous,
        olderMessagesLoading: false,
        olderMessagesError: String(error),
      }));
    } finally {
      olderMessagesRequestRef.current = false;
    }
  }

  async function applyTurnSettledResult(workflowId, result) {
    if (!result) {
      deferReconcileWorkflow(workflowId, { resumeActive: true });
      return;
    }
    if (result.needs_snapshot) {
      deferReconcileWorkflow(workflowId, { resumeActive: true });
      return;
    }

    setState((previous) =>
      previous.workflowId === workflowId
        ? applyTranscriptDeltasInState(previous, result)
        : previous,
    );

    if (transcriptDeltaResultStillLive(result)) {
      deferReconcileWorkflow(workflowId, { resumeActive: true });
    }
  }

  function turnEventSourceUrl(workflowId, options = {}) {
    const params = new URLSearchParams();
    const cursor = options.cursor || streamCursorRef.current;
    if (cursor) params.set("cursor", cursor);
    params.set(
      "after_revision",
      String(
        Number(
          options.afterRevision ??
            stateRef.current.workflowState?.transcript_revision ??
            0,
        ),
      ),
    );
    const query = params.toString();
    return `/api/sessions/${encodeURIComponent(workflowId)}/events/turn${
      query ? `?${query}` : ""
    }`;
  }

  function parseStreamEventData(message) {
    try {
      return JSON.parse(message.data || "{}");
    } catch (error) {
      setStatusNotice(`stream event parse failed: ${error}`);
      return null;
    }
  }

  function startTurnEventSource(workflowId, options = {}) {
    const token = beginTurnStream(workflowId);
    let source = null;
    let settled = false;
    let reconciled = false;
    let finished = false;
    let accepted = false;
    let firstEventReceived = false;
    let streamErrored = false;
    let firstEventTimer = null;
    let recoveryTimer = null;

    let resolveCompletion;
    let resolveOpened;
    const completion = new Promise((resolve) => {
      resolveCompletion = resolve;
    });
    const opened = new Promise((resolve) => {
      resolveOpened = resolve;
    });

    function finish() {
      if (finished) return;
      finished = true;
      if (firstEventTimer !== null) {
        window.clearTimeout(firstEventTimer);
        firstEventTimer = null;
      }
      if (recoveryTimer !== null) {
        window.clearTimeout(recoveryTimer);
        recoveryTimer = null;
      }
      if (source) source.close();
      if (turnStreamTokenRef.current === token) {
        turnStreamSourceRef.current = null;
        turnStreamFinishRef.current = null;
        activeTurnStreamWorkflowRef.current = null;
      }
      resolveOpened(false);
      resolveCompletion({ settled, reconciled });
    }

    function scheduleRecovery(reason) {
      if (
        finished ||
        recoveryTimer !== null ||
        !turnStreamNeedsRecovery({ settled, reconciled })
      ) {
        return;
      }
      flushStreamEventsNow();
      const attempt = turnStreamRecoveryAttemptRef.current;
      turnStreamRecoveryAttemptRef.current += 1;
      const delay = turnStreamRecoveryDelay(attempt);
      console.warn("turn stream recovery scheduled", {
        workflowId,
        reason,
        attempt,
        delay,
      });
      recoveryTimer = window.setTimeout(() => {
        recoveryTimer = null;
        if (turnStreamTokenRef.current !== token || finished) return;
        finish();
        reconcileWorkflow(workflowId, {
          resumeActive: true,
          restoreActiveStream: true,
        });
      }, delay);
    }

    function armFirstEventWatchdog() {
      accepted = true;
      if (firstEventReceived) return;
      if (finished) {
        const attempt = turnStreamRecoveryAttemptRef.current;
        turnStreamRecoveryAttemptRef.current += 1;
        window.setTimeout(() => {
          if (
            turnStreamTokenRef.current === token &&
            stateRef.current.workflowId === workflowId
          ) {
            reconcileWorkflow(workflowId, {
              resumeActive: true,
              restoreActiveStream: true,
            });
          }
        }, turnStreamRecoveryDelay(attempt));
        return;
      }
      if (streamErrored) {
        scheduleRecovery("stream unavailable after message acceptance");
        return;
      }
      if (firstEventTimer !== null) window.clearTimeout(firstEventTimer);
      firstEventTimer = window.setTimeout(() => {
        firstEventTimer = null;
        scheduleRecovery("no first event after message acceptance");
      }, TURN_STREAM_FIRST_EVENT_TIMEOUT_MS);
    }

    function noteStreamEvent() {
      const first = !firstEventReceived;
      firstEventReceived = true;
      streamErrored = false;
      turnStreamRecoveryAttemptRef.current = 0;
      if (firstEventTimer !== null) {
        window.clearTimeout(firstEventTimer);
        firstEventTimer = null;
      }
      return first;
    }

    turnStreamFinishRef.current = finish;
    source = new EventSource(turnEventSourceUrl(workflowId, options));
    turnStreamSourceRef.current = source;
    source.onopen = () => {
      resolveOpened(true);
      markTurnLatency("streamOpenedAt");
    };

    source.addEventListener("stream", (message) => {
      if (turnStreamTokenRef.current !== token) return;
      const streamEvent = parseStreamEventData(message);
      if (!streamEvent) return;
      const first = noteStreamEvent();
      recordFirstStreamEvent(streamEvent, message.lastEventId);
      enqueueStreamEvent(workflowId, streamEvent, message.lastEventId);
      if (first) flushStreamEventsNow();
      if (
        streamEvent.kind === AgentStreamEventKind.AGENT_START &&
        streamEvent.agent?.kind !== "subagent"
      ) {
        clearWorkflowStateRefresh();
      }
      if (streamEventNeedsWorkflowStateRefresh(streamEvent)) {
        scheduleWorkflowStateRefresh(workflowId);
      }
    });

    source.addEventListener("turn_settled", async (message) => {
      if (turnStreamTokenRef.current !== token) return;
      noteStreamEvent();
      flushStreamEventsNow();
      if (message.lastEventId) streamCursorRef.current = message.lastEventId;
      const body = parseStreamEventData(message);
      if (!body) {
        deferReconcileWorkflow(workflowId, { resumeActive: true });
        finish();
        return;
      }
      if (body.cursor) streamCursorRef.current = body.cursor;
      settled = true;
      await applyTurnSettledResult(workflowId, body.result);
      finishTurnLatency("settled");
      finish();
    });

    source.addEventListener("reconcile", (message) => {
      if (turnStreamTokenRef.current !== token) return;
      noteStreamEvent();
      flushStreamEventsNow();
      if (message.lastEventId) streamCursorRef.current = message.lastEventId;
      reconciled = true;
      finishTurnLatency("reconciled");
      finish();
      deferReconcileWorkflow(workflowId, {
        resumeActive: true,
        restoreActiveStream: true,
      });
    });

    source.onerror = () => {
      flushStreamEventsNow();
      if (turnStreamTokenRef.current !== token) {
        finish();
        return;
      }
      streamErrored = true;
      if (accepted) scheduleRecovery(`EventSource readyState=${source.readyState}`);
    };
    return { completion, finish, opened, armFirstEventWatchdog };
  }

  async function openTurnEventSource(workflowId, options = {}) {
    const turnStream = startTurnEventSource(workflowId, options);
    turnStream.armFirstEventWatchdog();
    return turnStream.completion;
  }

  async function resumeActiveTurnStream(workflowId, options = {}) {
    if (activeTurnStreamWorkflowRef.current === workflowId) return;
    await openTurnEventSource(workflowId, options);
  }

  async function streamChatMessage(workflowId, payload, pendingId) {
    beginTurnLatency(workflowId);
    turnStreamRecoveryAttemptRef.current = 0;
    const initialCursor = streamCursorRef.current;
    const turnStream = startTurnEventSource(workflowId, {
      cursor: initialCursor,
      afterRevision: payload.after_revision,
    });
    if (!initialCursor) await turnStream.opened;
    let response;
    try {
      response = await fetch(`/api/sessions/${encodeURIComponent(workflowId)}/chat`, {
        method: "POST",
        headers: jsonHeaders(),
        body: JSON.stringify(payload),
      });
    } catch (error) {
      turnStream.finish();
      finishTurnLatency("submit_failed");
      throw error;
    }
    markTurnLatency("acceptedAt");
    if (turnLatencyRef.current) {
      turnLatencyRef.current.serverTiming = response.headers.get("Server-Timing") || "";
    }
    if (response.status === 401) {
      turnStream.finish();
      finishTurnLatency("unauthorized");
      showLogin();
      return;
    }
    if (response.status === 404) {
      turnStream.finish();
      finishTurnLatency("workflow_missing");
      await handleMissingWorkflow();
      return;
    }
    if (!response.ok) {
      turnStream.finish();
      finishTurnLatency("submit_failed");
      throw new Error(await responseErrorText(response));
    }

    await response.json();
    markPendingDelivered(pendingId);
    turnStream.armFirstEventWatchdog();
    await turnStream.completion;
  }

  function beginTurnLatency(workflowId) {
    turnLatencyRef.current = {
      workflowId,
      submittedAt: performance.now(),
    };
  }

  function markTurnLatency(field) {
    const timing = turnLatencyRef.current;
    if (timing && timing[field] === undefined) timing[field] = performance.now();
  }

  function recordFirstStreamEvent(event, cursor) {
    const timing = turnLatencyRef.current;
    if (!timing) return;
    markTurnLatency("firstStreamEventAt");
    if (
      timing.firstTokenReceivedAt === undefined &&
      [AgentStreamEventKind.AGENT_TEXT_DELTA, AgentStreamEventKind.AGENT_THINKING_DELTA].includes(
        event.kind,
      )
    ) {
      timing.firstTokenReceivedAt = performance.now();
      timing.providerEmittedAt = event.emitted_at || "";
      const cursorMilliseconds = String(cursor || "").split("-", 1)[0];
      const redisMilliseconds = /^\d+$/.test(cursorMilliseconds)
        ? Number(cursorMilliseconds)
        : Number.NaN;
      const emittedMilliseconds = Date.parse(event.emitted_at || "");
      if (Number.isFinite(redisMilliseconds)) {
        timing.redisToBrowserMs = Date.now() - redisMilliseconds;
      }
      if (Number.isFinite(redisMilliseconds) && Number.isFinite(emittedMilliseconds)) {
        timing.workerToRedisMs = redisMilliseconds - emittedMilliseconds;
      }
    }
  }

  function recordFirstRenderedToken(pending) {
    const timing = turnLatencyRef.current;
    if (!timing || timing.firstTokenRenderedAt !== undefined) return;
    const hasToken = pending.some(({ event }) =>
      [AgentStreamEventKind.AGENT_TEXT_DELTA, AgentStreamEventKind.AGENT_THINKING_DELTA].includes(
        event?.kind,
      ),
    );
    if (!hasToken) return;
    window.requestAnimationFrame(() => markTurnLatency("firstTokenRenderedAt"));
  }

  function finishTurnLatency(outcome) {
    const timing = turnLatencyRef.current;
    if (!timing) return;
    const now = performance.now();
    const duration = (value) =>
      value === undefined ? undefined : Math.round((value - timing.submittedAt) * 10) / 10;
    console.info("[turn-latency]", {
      workflowId: timing.workflowId,
      outcome,
      acceptedMs: duration(timing.acceptedAt),
      streamOpenMs: duration(timing.streamOpenedAt),
      firstEventMs: duration(timing.firstStreamEventAt),
      firstTokenReceivedMs: duration(timing.firstTokenReceivedAt),
      firstTokenRenderedMs: duration(timing.firstTokenRenderedAt),
      settledMs: Math.round((now - timing.submittedAt) * 10) / 10,
      workerToRedisMs: timing.workerToRedisMs,
      redisToBrowserMs: timing.redisToBrowserMs,
      serverTiming: timing.serverTiming || "",
    });
    turnLatencyRef.current = null;
  }

  async function refreshWorkflowStatePatch(workflowId, options = {}) {
    const current = stateRef.current;
    const afterRevision = Number(current.workflowState?.state_revision || 0);
    const requestId = ++workflowStateRefreshRequestRef.current;
    const response = await fetch(
      `/api/sessions/${encodeURIComponent(workflowId)}/state/patch?after_revision=${afterRevision}`,
      {
        headers: { "Cache-Control": "no-cache" },
      },
    );
    if (response.status === 401) {
      showLogin();
      return;
    }
    if (response.status === 404) {
      await handleMissingWorkflow();
      return;
    }
    if (!response.ok) throw new Error(await responseErrorText(response));

    const body = await response.json();
    if (
      stateRef.current.workflowId !== workflowId ||
      requestId !== workflowStateRefreshRequestRef.current
    ) {
      return;
    }

    const patch = body.state || {};
    if (!body.unchanged) {
      setState((previous) =>
        previous.workflowId === workflowId
          ? applyWorkflowStatePatchInState(previous, patch)
          : previous,
      );
    }

    const pendingApprovals = patch.pending_approvals || [];
    const visibleApprovals = stateRef.current.workflowState?.pending_approvals || [];
    if (
      pendingApprovals.length === 0 &&
      visibleApprovals.length === 0 &&
      (options.attempt || 0) < 5
    ) {
      scheduleWorkflowStateRefresh(workflowId, {
        attempt: (options.attempt || 0) + 1,
        delay: 400,
      });
    }
  }

  function toggleTurnTrace(transcriptIndex) {
    const current = stateRef.current;
    const trace = current.turnTraces?.[transcriptIndex];
    setState((previous) => toggleExpandedTraceInState(previous, transcriptIndex));
    if (trace?.turn || trace?.status === "loading") return;
    loadTurnTraceReplay(current.workflowId, transcriptIndex).catch((error) => {
      setState((previous) =>
        previous.workflowId === current.workflowId
          ? setTurnTraceErrorInState(previous, transcriptIndex, error)
          : previous,
      );
    });
  }

  async function loadTurnTraceReplay(workflowId, transcriptIndex) {
    if (!workflowId) return;
    setState((previous) =>
      previous.workflowId === workflowId
        ? setTurnTraceLoadingInState(previous, transcriptIndex)
        : previous,
    );
    const response = await fetch(
      `/api/sessions/${encodeURIComponent(workflowId)}/stream/events?limit=5000`,
      { headers: { "Cache-Control": "no-cache" } },
    );
    if (response.status === 401) {
      showLogin();
      return;
    }
    if (response.status === 404) {
      await handleMissingWorkflow();
      return;
    }
    if (!response.ok) throw new Error(await responseErrorText(response));
    const body = await response.json();
    if (!body.replay_available) {
      throw new Error(body.reason || "stream replay unavailable");
    }

    setState((previous) => {
      if (previous.workflowId !== workflowId) return previous;
      const merged = mergeReplayedTurnTracesInState(
        previous,
        coalesceStreamEvents(body.events || []),
      );
      return merged.turnTraces?.[transcriptIndex]?.turn
        ? merged
        : setTurnTraceErrorInState(
            merged,
            transcriptIndex,
            "trace is no longer available for this turn",
          );
    });
  }

  async function createConversation(initialMessage = null, options = {}) {
    const response = await fetch("/api/sessions", {
      method: "POST",
      headers: jsonHeaders(),
      body: JSON.stringify({
        ...newConversationRequest(),
        initial_message: initialMessage,
      }),
    });
    if (response.status === 401) {
      showLogin();
      return null;
    }
    if (!response.ok) throw new Error(await responseErrorText(response));
    const body = await response.json();
    const conversations = await refreshConversations();
    selectConversation(body.workflow_id, options, conversations);
    return body;
  }

  async function ensureConversationForAttachments() {
    const current = stateRef.current;
    if (current.workflowId) return current.workflowId;
    const body = await createConversation(null);
    return body?.workflow_id || null;
  }

  async function uploadAttachmentFiles(files) {
    const uploadFiles = Array.from(files || []).filter(Boolean);
    if (!uploadFiles.length) return;
    setState((previous) => ({
      ...previous,
      attachmentUploading: true,
      attachmentError: "",
    }));
    try {
      const workflowId = await ensureConversationForAttachments();
      if (!workflowId) return;
      for (const file of uploadFiles) {
        const formData = new FormData();
        formData.append("file", file);
        const response = await fetch(`/api/sessions/${workflowId}/attachments`, {
          method: "POST",
          body: formData,
        });
        if (response.status === 401) {
          showLogin();
          return;
        }
        if (response.status === 404) {
          await handleMissingWorkflow();
          return;
        }
        if (!response.ok) throw new Error(await responseErrorText(response));
        const body = await response.json();
        if (body.attachment) addComposerAttachment(body.attachment);
      }
    } catch (error) {
      setState((previous) => ({
        ...previous,
        attachmentError: String(error),
      }));
    } finally {
      setState((previous) => ({
        ...previous,
        attachmentUploading: false,
      }));
    }
  }

  async function createPasteAttachment(text) {
    const content = String(text || "");
    if (!content) return;
    setState((previous) => ({
      ...previous,
      attachmentUploading: true,
      attachmentError: "",
    }));
    try {
      const workflowId = await ensureConversationForAttachments();
      if (!workflowId) return;
      const response = await fetch(`/api/sessions/${workflowId}/attachments/text`, {
        method: "POST",
        headers: jsonHeaders(),
        body: JSON.stringify({
          name: "pasted-text.txt",
          content,
          mime_type: "text/plain",
        }),
      });
      if (response.status === 401) {
        showLogin();
        return;
      }
      if (response.status === 404) {
        await handleMissingWorkflow();
        return;
      }
      if (!response.ok) throw new Error(await responseErrorText(response));
      const body = await response.json();
      if (body.attachment) addComposerAttachment(body.attachment);
    } catch (error) {
      setState((previous) => ({
        ...previous,
        attachmentError: String(error),
      }));
    } finally {
      setState((previous) => ({
        ...previous,
        attachmentUploading: false,
      }));
    }
  }

  function addComposerAttachment(attachment) {
    const attachmentId = attachment.attachment_id || attachment.artifact_id;
    if (!attachmentId) return;
    setState((previous) => {
      const composerAttachments = previous.composerAttachments || [];
      if (
        composerAttachments.some(
          (existing) =>
            (existing.attachment_id || existing.artifact_id) === attachmentId,
        )
      ) {
        return previous;
      }
      const workflowState = previous.workflowState
        ? {
            ...previous.workflowState,
            attachments: appendUniqueAttachment(
              previous.workflowState.attachments || [],
              attachment,
            ),
          }
        : previous.workflowState;
      return {
        ...previous,
        workflowState,
        composerAttachments: [...composerAttachments, attachment],
      };
    });
  }

  function removeComposerAttachment(attachmentId) {
    setState((previous) => ({
      ...previous,
      composerAttachments: (previous.composerAttachments || []).filter(
        (attachment) =>
          (attachment.attachment_id || attachment.artifact_id) !== attachmentId,
      ),
    }));
  }

  function appendUniqueAttachment(attachments, attachment) {
    const attachmentId = attachment.attachment_id || attachment.artifact_id;
    if (!attachmentId) return attachments;
    if (
      attachments.some(
        (existing) =>
          (existing.attachment_id || existing.artifact_id) === attachmentId,
      )
    ) {
      return attachments;
    }
    return [...attachments, attachment];
  }

  function newConversationRequest() {
    const current = stateRef.current;
    const config = current.config || {};
    const minBudget = config.thinking?.min_budget_tokens || 1024;
    const budgetTokens = Math.max(
      minBudget,
      Number(
        current.agentSettings.thinkingBudgetTokens ||
          config.thinking?.budget_tokens ||
          4096,
      ),
    );
    const agentSettings = {
      model: current.agentSettings.model || config.default_model || "",
      thinkingEnabled: Boolean(current.agentSettings.thinkingEnabled),
      thinkingMode: current.agentSettings.thinkingMode || config.thinking?.mode || "enabled",
      thinkingBudgetTokens: budgetTokens,
      thinkingEffort: current.agentSettings.thinkingEffort || config.thinking?.effort || "max",
    };
    const normalizedSettings = normalizeAgentSettings(agentSettings, config);
    saveAgentSettings(normalizedSettings);
    setState((previous) => ({ ...previous, agentSettings: normalizedSettings }));
    return {
      system_prompt: current.draftSystemPrompt.trim() || defaultSystemPrompt,
      model: normalizedSettings.model,
      thinking: {
        enabled: normalizedSettings.thinkingEnabled,
        mode: normalizedSettings.thinkingMode,
        budget_tokens: budgetTokens,
        effort: normalizedSettings.thinkingEffort,
      },
    };
  }

  async function sendDefault() {
    const busy = stateRef.current.workflowState?.status === "responding";
    const hasAttachments = (stateRef.current.composerAttachments || []).length > 0;
    await sendAction(
      busy && !hasAttachments ? "steer" : "chat",
      busy && !hasAttachments ? "you steering" : "you",
      "sending",
    );
  }

  async function sendAction(action, label, phase) {
    let content = messageRef.current.trim();
    const attachments =
      action === "interrupt" ? [] : [...(stateRef.current.composerAttachments || [])];
    if (!content && action === "interrupt") {
      content = "Stop the current response.";
    }
    if (!content && !attachments.length) return;

    let current = stateRef.current;
    if (!current.workflowId) {
      if (action === "interrupt" || action === "steer" || action === "after-tool") return;
      clearComposerInput();
      setState((previous) => ({
        ...previous,
        composerAttachments: [],
        attachmentError: "",
      }));
      const pending = createPendingMessage(label, content, phase, current, attachments);
      setState((previous) => ({
        ...previous,
        localPending: [...previous.localPending, pending],
      }));
      try {
        const created = await createConversation(null, { preserveLocalPending: true });
        const workflowId = created?.workflow_id;
        if (!workflowId) throw new Error("conversation was not created");
        const attachment_ids = attachments
          .map((attachment) => attachment.attachment_id || attachment.artifact_id)
          .filter(Boolean);
        await streamChatMessage(
          workflowId,
          {
            message: content,
            attachment_ids,
            after_revision: 0,
          },
          pending.id,
        );
        await refreshConversations();
      } catch (error) {
        markPendingFailed(pending.id, error);
      }
      return;
    }

    clearComposerInput();
    setState((previous) => ({
      ...previous,
      composerAttachments: [],
      attachmentError: "",
    }));
    const pending = createPendingMessage(label, content, phase, current, attachments);
    setState((previous) => {
      let next = {
        ...previous,
        localPending: [...previous.localPending, pending],
      };
      if (action === "interrupt") {
        next = markStreamInterruptedInState(next);
        next.ignoreAgentUntilStart = true;
      }
      return next;
    });

    try {
      const attachment_ids = attachments
        .map((attachment) => attachment.attachment_id || attachment.artifact_id)
        .filter(Boolean);
      if (action === "chat") {
        await streamChatMessage(
          current.workflowId,
          {
            message: content,
            attachment_ids,
            after_revision: Number(current.workflowState?.transcript_revision || 0),
          },
          pending.id,
        );
      } else if (action === "steer") {
        await post(`/api/sessions/${current.workflowId}/steer`, {
          message: content,
          mode: "immediate",
          attachment_ids,
        });
      } else if (action === "after-tool") {
        await post(`/api/sessions/${current.workflowId}/steer`, {
          message: content,
          mode: "after_next_tool_result",
          attachment_ids,
        });
      } else if (action === "interrupt") {
        await post(`/api/sessions/${current.workflowId}/interrupt`, { message: content });
      }
      if (action !== "chat") markPendingDelivered(pending.id);
      await refreshConversations();
    } catch (error) {
      markPendingFailed(pending.id, error);
    }
  }

  function markPendingDelivered(pendingId) {
    setState((previous) => ({
      ...previous,
      localPending: previous.localPending.map((pending) =>
        pending.id === pendingId ? { ...pending, phase: "delivered" } : pending,
      ),
    }));
  }

  function markPendingFailed(pendingId, error) {
    setState((previous) => ({
      ...previous,
      localPending: previous.localPending.map((pending) =>
        pending.id === pendingId ? { ...pending, phase: `failed: ${error}` } : pending,
      ),
    }));
  }

  async function post(url, payload) {
    const response = await fetch(url, {
      method: "POST",
      headers: jsonHeaders(),
      body: JSON.stringify(payload),
    });
    if (response.status === 401) {
      showLogin();
    }
    if (response.status === 404) {
      await handleMissingWorkflow();
    }
    if (!response.ok) throw new Error(await responseErrorText(response));
    const contentType = response.headers.get("content-type") || "";
    if (contentType.includes("application/json")) return response.json();
    return {};
  }

  async function handleMissingWorkflow() {
    const current = stateRef.current;
    if (current.recoveringMissingWorkflow) return;
    const missingWorkflowId = current.workflowId;
    setState((previous) => ({ ...previous, recoveringMissingWorkflow: true }));
    try {
      closeTurnStream();
      clearLiveStreamState();
      if (missingWorkflowId) {
        setStatusNotice("Workflow no longer exists; selecting a live chat...");
        if (localStorage.getItem("simpleChatWorkflowId") === missingWorkflowId) {
          localStorage.removeItem("simpleChatWorkflowId");
        }
      }
      const conversations = await refreshConversations();
      const nextConversation = conversations[0];
      if (nextConversation) {
        selectConversation(nextConversation.workflow_id, {}, conversations);
      } else {
        startDraftConversation();
      }
    } finally {
      setState((previous) => ({ ...previous, recoveringMissingWorkflow: false }));
    }
  }

  async function deleteConversation(workflowId) {
    if (!confirm("Delete this chat?")) return;
    const deletingCurrent = stateRef.current.workflowId === workflowId;
    const response = await fetch(`/api/sessions/${workflowId}`, { method: "DELETE" });
    if (response.status === 401) {
      showLogin();
      return;
    }
    if (!response.ok) throw new Error(await responseErrorText(response));

    if (deletingCurrent) {
      closeTurnStream();
      clearLiveStreamState();
      nextStateLoadRequest();
      if (localStorage.getItem("simpleChatWorkflowId") === workflowId) {
        localStorage.removeItem("simpleChatWorkflowId");
      }
      setState((previous) => ({
        ...previous,
        workflowId: null,
        workflowState: null,
        workflowStateProjectionRevision: 0,
        workflowTranscriptProjectionRevision: 0,
        olderMessagesLoading: false,
        olderMessagesError: "",
        streamTurn: null,
        localPending: [],
        artifactViewer: emptyArtifactViewer,
      }));
    }

    const conversations = await refreshConversations();
    if (!deletingCurrent) return;
    const nextConversation = conversations[0];
    if (nextConversation) {
      selectConversation(nextConversation.workflow_id, {}, conversations);
    } else {
      startDraftConversation();
    }
  }

  async function resolveApproval(approvalId, decision) {
    if (stateRef.current.resolvingApprovals.has(approvalId)) return;
    const selectedApproval = (
      stateRef.current.workflowState?.pending_approvals || []
    ).find((approval) => approval.approval_id === approvalId);
    setState((previous) => ({
      ...previous,
      resolvingApprovals: new Set([...previous.resolvingApprovals, approvalId]),
    }));
    try {
      await post(`/api/sessions/${stateRef.current.workflowId}/approvals/${approvalId}`, {
        decision,
      });
      setState((previous) => {
        const resolvingApprovals = new Set(previous.resolvingApprovals);
        resolvingApprovals.delete(approvalId);
        if (!previous.workflowState) {
          return { ...previous, resolvingApprovals };
        }
        return {
          ...previous,
          resolvingApprovals,
          workflowState: {
            ...previous.workflowState,
            pending_approvals: (previous.workflowState.pending_approvals || []).filter(
              (approval) =>
                approval.approval_id !== approvalId &&
                (
                  decision !== "always_allow" ||
                  !selectedApproval?.memory_key ||
                  approval.memory_key !== selectedApproval.memory_key
                ),
            ),
          },
        };
      });
    } catch (error) {
      setState((previous) => {
        const resolvingApprovals = new Set(previous.resolvingApprovals);
        resolvingApprovals.delete(approvalId);
        return {
          ...previous,
          resolvingApprovals,
          statusNotice: `approval failed: ${error}`,
        };
      });
    }
  }

  async function openArtifactViewer(artifact) {
    const previewKind = artifactPreviewKind(artifact);
    setState((previous) => ({
      ...previous,
      artifactViewer: {
        open: true,
        artifact,
        previewKind,
        loading: true,
        error: "",
        text: "",
      },
    }));

    if (!artifactNeedsTextFetch(previewKind)) {
      setState((previous) => ({
        ...previous,
        artifactViewer: {
          ...previous.artifactViewer,
          loading: false,
        },
      }));
      return;
    }

    try {
      const response = await fetch(artifact.view_url);
      if (!response.ok) throw new Error(await responseErrorText(response));
      const text = await response.text();
      setState((previous) => ({
        ...previous,
        artifactViewer: {
          ...previous.artifactViewer,
          loading: false,
          text,
        },
      }));
    } catch (error) {
      setState((previous) => ({
        ...previous,
        artifactViewer: {
          ...previous.artifactViewer,
          loading: false,
          error: String(error),
        },
      }));
    }
  }

  function closeArtifactViewer() {
    setState((previous) => ({
      ...previous,
      artifactViewer: emptyArtifactViewer,
    }));
  }

  async function setMcpServerEnabled(tool, enabled) {
    const serverId = tool.provider.slice("mcp:".length);
    await post(`/api/mcp-servers/${encodeURIComponent(serverId)}/enabled`, { enabled });
    setStatusNotice(`${tool.label} ${enabled ? "enabled" : "disabled"}`);
    await refreshTools();
  }

  async function deleteMcpServer(tool) {
    if (!confirm(`Delete ${tool.label}?`)) return;
    const serverId = tool.provider.slice("mcp:".length);
    const response = await fetch(`/api/mcp-servers/${encodeURIComponent(serverId)}`, {
      method: "DELETE",
    });
    if (response.status === 401) {
      showLogin();
      return;
    }
    if (!response.ok) throw new Error(await responseErrorText(response));
    setStatusNotice(`${tool.label} deleted`);
    await refreshTools();
  }

  async function addHttpMcpServer(event, submittedValues = null) {
    event.preventDefault();
    const values = submittedValues || stateRef.current.mcpFormValues;
    const label = values.label.trim();
    const serverUrl = values.server_url.trim();
    const toolPrefix = values.tool_prefix.trim();
    const authMode = values.auth_mode || "none";
    const bearerToken = values.bearer_token.trim();
    const validationError = mcpFormValidationError({
      label,
      serverUrl,
      toolPrefix,
      authMode,
      bearerToken,
    });
    if (validationError) {
      setState((previous) => ({
        ...previous,
        mcpFormError: validationError,
      }));
      return;
    }

    if (authMode === "oauth") {
      window.location.href = mcpOAuthStartUrl({
        label,
        serverUrl,
        toolPrefix,
      });
      return;
    }

    setState((previous) => ({
      ...previous,
      mcpFormSubmitting: true,
      mcpFormError: "",
    }));
    try {
      const body = await post("/api/mcp-servers", {
        label,
        server_url: serverUrl,
        tool_prefix: toolPrefix,
        auth_mode: authMode,
        bearer_token: authMode === "bearer" ? bearerToken : null,
      });
      setState((previous) => ({
        ...previous,
        mcpFormOpen: false,
        mcpFormSubmitting: false,
        mcpFormError: "",
        mcpFormValues: defaultMcpFormValues,
      }));
      setStatusNotice(`Added MCP server: ${body.server?.label || label}`);
      await refreshTools();
    } catch (error) {
      setState((previous) => ({
        ...previous,
        mcpFormSubmitting: false,
        mcpFormError: String(error),
      }));
    }
  }

  function mcpFormValidationError({
    label,
    serverUrl,
    toolPrefix,
    authMode,
    bearerToken,
  }) {
    if (!label) return "Label is required.";
    if (!serverUrl) return "HTTP URL is required.";
    if (!toolPrefix) return "Tool prefix is required.";
    if (authMode === "bearer" && !bearerToken) return "Bearer token is required.";
    return "";
  }

  function updateAgentSettings(patch) {
    setState((previous) => {
      const agentSettings = normalizeAgentSettings(
        { ...previous.agentSettings, ...patch },
        previous.config || {},
      );
      saveAgentSettings(agentSettings);
      return { ...previous, agentSettings };
    });
  }

  function updateThinkingBudget(value) {
    const minBudget = stateRef.current.config?.thinking?.min_budget_tokens || 1024;
    updateAgentSettings({
      thinkingBudgetTokens: Math.max(minBudget, Number(value || minBudget)),
    });
  }

  function updateMcpFormValues(patch) {
    setState((previous) => ({
      ...previous,
      mcpFormValues: {
        ...previous.mcpFormValues,
        ...patch,
      },
    }));
  }

  function setStatusNotice(statusNotice) {
    setState((previous) => ({ ...previous, statusNotice }));
  }

  function showOAuthCallbackStatus() {
    const params = new URLSearchParams(window.location.search);
    if (params.has("oauth_error")) {
      setStatusNotice(`OAuth failed: ${params.get("oauth_error")}`);
    } else if (params.has("github")) {
      setStatusNotice("GitHub connected");
    } else if (params.has("mcp")) {
      setStatusNotice("MCP server connected");
      refreshTools().catch((error) => {
        setStatusNotice(`tool refresh failed: ${error}`);
      });
    }
    if (params.has("oauth_error") || params.has("github") || params.has("mcp")) {
      history.replaceState({}, "", "/");
    }
  }

  function handleLoginClick(event) {
    event.preventDefault();
    if (state.authMode !== "catalog" || !state.loginConfigured) return;
    const href = event.currentTarget.getAttribute("href");
    setState((previous) => ({ ...previous, loggingIn: true }));
    setTimeout(() => {
      window.location.href = href;
    }, 750);
  }

  async function handleLocalLoginSubmit(event) {
    event.preventDefault();
    const current = stateRef.current;
    if (current.authMode !== "local") return;
    setState((previous) => ({ ...previous, loggingIn: true, loginError: "" }));
    try {
      const response = await fetch("/api/auth/local/login", {
        method: "POST",
        headers: jsonHeaders(),
        body: JSON.stringify({
          username: current.localLoginUsername,
          password: current.localLoginPassword,
        }),
      });
      if (!response.ok) throw new Error(await responseErrorText(response));
      await boot({ cancelled: false });
    } catch (error) {
      setState((previous) => ({
        ...previous,
        loggingIn: false,
        loginError: String(error),
      }));
    }
  }

  const status = displayStatus(state);
  const artifacts = state.workflowState?.artifacts || [];
  const selectedTraceIndex = state.expandedTraceIndex;
  const selectedTrace =
    selectedTraceIndex !== null && selectedTraceIndex !== undefined
      ? state.turnTraces?.[selectedTraceIndex]
      : null;
  const loadingConversation = Boolean(
    state.workflowId &&
      !state.workflowState &&
      !state.draftConversation &&
      state.localPending.length === 0,
  );

  return (
    <>
      <LoginScreen
        hidden={state.auth !== "login"}
        loggingIn={state.loggingIn}
        configured={state.loginConfigured}
        authMode={state.authMode}
        subtitle={state.loginSubtitle}
        error={state.loginError}
        localUsername={state.localLoginUsername}
        localPassword={state.localLoginPassword}
        onLocalUsernameChange={(value) =>
          setState((previous) => ({ ...previous, localLoginUsername: value }))
        }
        onLocalPasswordChange={(value) =>
          setState((previous) => ({ ...previous, localLoginPassword: value }))
        }
        onLocalSubmit={handleLocalLoginSubmit}
        onCatalogLoginClick={handleLoginClick}
      />
      <div className="app" hidden={state.auth !== "app"}>
        <AppHeader
          state={state}
          status={status}
          onNewChat={startDraftConversation}
          onOpenTools={() =>
            setState((previous) => ({ ...previous, toolsWindowOpen: true }))
          }
          onLogout={async () => {
            await post("/api/logout", {});
            localStorage.removeItem("simpleChatWorkflowId");
            clearLiveStreamState();
            setState({
              ...initialState,
              auth: "login",
            });
            configureLoginButton();
          }}
          onUpdateAgentSettings={updateAgentSettings}
          onUpdateThinkingBudget={updateThinkingBudget}
          onSelectConversation={selectConversation}
          onDeleteConversation={(workflowId) => {
            deleteConversation(workflowId).catch((error) => {
              setStatusNotice(`delete failed: ${error}`);
            });
          }}
        />
        <main>
          <section
            className="messages"
            ref={messagesRef}
            onScroll={(event) => {
              const node = event.currentTarget;
              pinnedToBottomRef.current =
                node.scrollHeight - node.scrollTop - node.clientHeight < 80;
              if (node.scrollTop < 180) {
                loadOlderMessages().catch((error) => {
                  setStatusNotice(`older messages failed: ${error}`);
                });
              }
            }}
          >
            <Messages
              workflowId={state.workflowId}
              workflowState={state.workflowState}
              draftConversation={state.draftConversation}
              loadingConversation={loadingConversation}
              olderMessagesLoading={state.olderMessagesLoading}
              olderMessagesError={state.olderMessagesError}
              localPending={state.localPending}
              streamTurn={state.streamTurn}
              turnTraces={state.turnTraces}
              expandedTraceIndex={state.expandedTraceIndex}
              streamPanelCollapsed={state.streamPanelCollapsed}
              resolvingApprovals={state.resolvingApprovals}
              draftSystemPrompt={state.draftSystemPrompt}
              onUpdateDraftSystemPrompt={(draftSystemPrompt) =>
                setState((previous) => ({ ...previous, draftSystemPrompt }))
              }
              onToggleStreamPanel={() =>
                setState((previous) => ({
                  ...previous,
                  streamPanelCollapsed: !previous.streamPanelCollapsed,
                }))
              }
              onToggleTurnTrace={toggleTurnTrace}
              onResolveApproval={resolveApproval}
              onLoadOlderMessages={loadOlderMessages}
            />
          </section>
          <aside className="sidebar">
            <p className="events-title">Sideband Stream</p>
            <div></div>
          </aside>
        </main>
        <ArtifactsPanel artifacts={artifacts} onOpen={openArtifactViewer} />
        <Composer
          temporalUiUrl={state.temporalUiUrl}
          attachments={state.composerAttachments}
          attachmentUploading={state.attachmentUploading}
          attachmentError={state.attachmentError}
          onMessageChange={updateComposerMessage}
          onSend={sendDefault}
          onInterrupt={() => sendAction("interrupt", "you interrupt", "sending")}
          onFilesSelected={uploadAttachmentFiles}
          onPasteAttachment={createPasteAttachment}
          onRemoveAttachment={removeComposerAttachment}
          resetToken={composerResetToken}
        />
        <ToolsWindow
          open={state.toolsWindowOpen}
          tools={state.tools}
          mcpFormOpen={state.mcpFormOpen}
          mcpFormSubmitting={state.mcpFormSubmitting}
          mcpFormError={state.mcpFormError}
          mcpFormValues={state.mcpFormValues}
          onClose={() =>
            setState((previous) => ({
              ...previous,
              toolsWindowOpen: false,
              mcpFormOpen: false,
              mcpFormError: "",
            }))
          }
          onOpenMcpForm={() =>
            setState((previous) => ({
              ...previous,
              mcpFormOpen: true,
              mcpFormError: "",
            }))
          }
          onCancelMcpForm={() =>
            setState((previous) => ({
              ...previous,
              mcpFormOpen: false,
              mcpFormError: "",
              mcpFormValues: defaultMcpFormValues,
            }))
          }
          onUpdateMcpForm={updateMcpFormValues}
          onSubmitMcpForm={addHttpMcpServer}
          onRefreshTools={refreshTools}
          onSetMcpEnabled={setMcpServerEnabled}
          onDeleteMcp={deleteMcpServer}
          setStatusNotice={setStatusNotice}
          post={post}
        />
        <TurnTraceDrawer
          open={selectedTraceIndex !== null && selectedTraceIndex !== undefined}
          trace={selectedTrace}
          transcriptIndex={selectedTraceIndex}
          workflowId={state.workflowId}
          onClose={() =>
            setState((previous) => ({
              ...previous,
              expandedTraceIndex: null,
            }))
          }
        />
        <ArtifactViewer
          viewer={state.artifactViewer}
          onClose={closeArtifactViewer}
        />
      </div>
    </>
  );
}
