import { useEffect, useLayoutEffect, useRef, useState } from "react";

import { ArtifactViewer, ArtifactsPanel } from "./components/Artifacts.jsx";
import { AppHeader } from "./components/AppHeader.jsx";
import { Composer } from "./components/Composer.jsx";
import { LoginScreen } from "./components/LoginScreen.jsx";
import { Messages } from "./components/Messages.jsx";
import { ToolsWindow } from "./components/ToolsWindow.jsx";
import { artifactNeedsTextFetch, artifactPreviewKind } from "./utils/artifacts.js";
import { jsonHeaders, responseErrorText } from "./utils/http.js";
import { mcpOAuthStartUrl } from "./utils/tools.js";
import {
  applyWorkflowStatePatchInState,
  agentSettingsFromConfig,
  applyTranscriptDeltasInState,
  createPendingMessage,
  displayStatus,
  handleStreamEventInState,
  markStreamInterruptedInState,
  normalizeAgentSettings,
  prependTranscriptPageInState,
  saveAgentSettings,
  streamEventNeedsSettledTranscriptDelta,
  streamEventNeedsWorkflowStateRefresh,
  temporalUiUrl,
  updateWorkflowStateInState,
} from "./state/chatState.js";
import {
  defaultMcpFormValues,
  emptyArtifactViewer,
  initialState,
} from "./state/initialState.js";

export default function App() {
  const [state, setState] = useState(initialState);
  const [composerResetToken, setComposerResetToken] = useState(0);
  const stateRef = useRef(state);
  const messageRef = useRef("");
  const eventSourceRef = useRef(null);
  const eventSourceTokenRef = useRef(0);
  const messagesRef = useRef(null);
  const pinnedToBottomRef = useRef(true);
  const stateLoadRequestRef = useRef(0);
  const restoreScrollAfterPrependRef = useRef(null);
  const olderMessagesRequestRef = useRef(false);
  const settledTranscriptTimerRef = useRef(null);
  const settledTranscriptRequestRef = useRef(0);
  const workflowStateRefreshTimerRef = useRef(null);
  const workflowStateRefreshRequestRef = useRef(0);
  const streamEventBufferRef = useRef([]);
  const streamEventFrameRef = useRef(null);

  useEffect(() => {
    stateRef.current = state;
  }, [state]);

  useEffect(() => {
    const run = { cancelled: false };
    boot(run);
    return () => {
      run.cancelled = true;
      clearSettledTranscriptRefresh();
      clearWorkflowStateRefresh();
      clearStreamEventFlush();
      closeEventSource();
    };
  }, []);

  useEffect(() => {
    function handleVisibilityChange() {
      if (document.hidden) {
        closeEventSource();
        return;
      }
      const current = stateRef.current;
      if (current.auth === "app" && current.workflowId) {
        reconcileWorkflow(current.workflowId);
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
    messages
      .querySelectorAll(
        ".stream-current-turn .stream-text, .stream-current-turn .stream-thinking, .stream-agent-segment .stream-text, .stream-agent-segment .stream-thinking",
      )
      .forEach((node) => {
        node.scrollTop = node.scrollHeight;
      });
  }, [state.workflowState, state.localPending, state.streamTurn, state.draftConversation]);

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

  function closeEventSource() {
    eventSourceTokenRef.current += 1;
    clearSettledTranscriptRefresh();
    clearWorkflowStateRefresh();
    clearStreamEventFlush();
    if (!eventSourceRef.current) return;
    eventSourceRef.current.close();
    eventSourceRef.current = null;
  }

  function clearStreamEventFlush() {
    streamEventBufferRef.current = [];
    if (streamEventFrameRef.current === null) return;
    window.cancelAnimationFrame(streamEventFrameRef.current);
    streamEventFrameRef.current = null;
  }

  function enqueueStreamEvent(workflowId, event) {
    streamEventBufferRef.current.push({ workflowId, event });
    if (streamEventFrameRef.current !== null) return;
    streamEventFrameRef.current = window.requestAnimationFrame(flushStreamEvents);
  }

  function flushStreamEvents() {
    streamEventFrameRef.current = null;
    const pending = streamEventBufferRef.current;
    streamEventBufferRef.current = [];
    if (!pending.length) return;

    setState((previous) => {
      let next = previous;
      for (const { workflowId, event } of pending) {
        if (workflowId && next.workflowId !== workflowId) continue;
        next = handleStreamEventInState(next, event);
      }
      return next;
    });

    if (streamEventBufferRef.current.length) {
      streamEventFrameRef.current = window.requestAnimationFrame(flushStreamEvents);
    }
  }

  function clearSettledTranscriptRefresh() {
    if (!settledTranscriptTimerRef.current) return;
    clearTimeout(settledTranscriptTimerRef.current);
    settledTranscriptTimerRef.current = null;
  }

  function scheduleSettledTranscriptRefresh(workflowId, options = {}) {
    clearSettledTranscriptRefresh();
    const attempt = options.attempt || 0;
    const delay = options.delay ?? 300;
    settledTranscriptTimerRef.current = setTimeout(() => {
      settledTranscriptTimerRef.current = null;
      if (document.hidden || stateRef.current.workflowId !== workflowId) return;
      refreshSettledTranscriptDeltas(workflowId, { attempt }).catch((error) => {
        if (stateRef.current.workflowId !== workflowId) return;
        setStatusNotice(`settled transcript failed: ${error}`);
      });
    }, delay);
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
    closeEventSource();
    nextStateLoadRequest();
    const params = new URLSearchParams(window.location.search);
    const loginError = params.has("oauth_error") ? params.get("oauth_error") || "" : "";
    if (params.has("oauth_error")) history.replaceState({}, "", "/");
    setState((previous) => ({
      ...previous,
      auth: "login",
      loginError,
      statusNotice: "",
    }));
    configureLoginButton();
  }

  async function configureLoginButton() {
    try {
      const response = await fetch("/api/auth/google/configured");
      if (!response.ok) throw new Error(await response.text());
      const body = await response.json();
      setState((previous) => ({
        ...previous,
        loginConfigured: Boolean(body.configured),
        loginSubtitle: body.configured
          ? ""
          : "Google OAuth is not configured. Set GOOGLE_OAUTH_CLIENT_ID and GOOGLE_OAUTH_CLIENT_SECRET.",
      }));
    } catch (error) {
      setState((previous) => ({
        ...previous,
        loginConfigured: false,
        loginSubtitle: `Could not check auth config: ${error}`,
      }));
    }
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
    closeEventSource();
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
      streamPanelCollapsed: false,
      currentClaudeSequence: null,
      ignoreClaudeUntilStart: false,
      localPending: [],
      resolvingApprovals: new Set(),
      draftConversation: true,
      artifactViewer: emptyArtifactViewer,
      statusNotice: "",
    }));
  }

  function selectConversation(workflowId, options = {}, conversationsArg = null) {
    const conversations = conversationsArg || stateRef.current.conversations;
    const conversation = conversations.find((item) => item.workflow_id === workflowId);
    if (!conversation) return;
    closeEventSource();
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
      streamTurn: null,
      streamPanelCollapsed: false,
      currentClaudeSequence: null,
      ignoreClaudeUntilStart: false,
      draftConversation: false,
      resolvingApprovals: new Set(),
      localPending: options.preserveLocalPending ? previous.localPending : [],
      artifactViewer: emptyArtifactViewer,
      statusNotice: "",
    }));
    loadWorkflowStateAndConnect(conversation.workflow_id, loadRequest).catch((error) => {
      if (stateLoadRequestRef.current !== loadRequest) return;
      setStatusNotice(`state load failed: ${error}`);
    });
  }

  async function loadWorkflowStateAndConnect(workflowId, loadRequest) {
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
    connectEvents(workflowId, { cursor: loaded.streamCursor });
  }

  function reconcileWorkflow(workflowId) {
    if (!workflowId) return;
    closeEventSource();
    const loadRequest = nextStateLoadRequest();
    loadWorkflowStateAndConnect(workflowId, loadRequest).catch((error) => {
      if (stateLoadRequestRef.current !== loadRequest) return;
      setStatusNotice(`state reconcile failed: ${error}`);
    });
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

  async function refreshSettledTranscriptDeltas(workflowId, options = {}) {
    const current = stateRef.current;
    const afterRevision = Number(current.workflowState?.transcript_revision || 0);
    const requestId = ++settledTranscriptRequestRef.current;
    const response = await fetch(
      `/api/sessions/${encodeURIComponent(workflowId)}/messages/deltas?after_revision=${afterRevision}`,
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
      requestId !== settledTranscriptRequestRef.current
    ) {
      return;
    }
    if (body.needs_snapshot) {
      reconcileWorkflow(workflowId);
      return;
    }

    setState((previous) =>
      previous.workflowId === workflowId
        ? applyTranscriptDeltasInState(previous, body)
        : previous,
    );

    const advanced = Number(body.to_revision || 0) > afterRevision;
    if (!advanced && (options.attempt || 0) < 2) {
      scheduleSettledTranscriptRefresh(workflowId, {
        attempt: (options.attempt || 0) + 1,
        delay: 450,
      });
    }
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

  function connectEvents(workflowId, options = {}) {
    if (!workflowId) return;
    closeEventSource();
    const eventSourceToken = eventSourceTokenRef.current;
    const params = new URLSearchParams();
    if (options.cursor) params.set("cursor", options.cursor);
    const query = params.toString();
    const eventSource = new EventSource(
      `/api/sessions/${encodeURIComponent(workflowId)}/events${query ? `?${query}` : ""}`,
    );
    eventSourceRef.current = eventSource;
    function isCurrentEventSource() {
      return (
        eventSourceRef.current === eventSource &&
        eventSourceTokenRef.current === eventSourceToken &&
        stateRef.current.workflowId === workflowId
      );
    }
    eventSource.addEventListener("state", (event) => {
      if (!isCurrentEventSource()) return;
      const nextState = JSON.parse(event.data);
      setState((previous) => updateWorkflowStateInState(previous, nextState));
    });
    eventSource.addEventListener("stream", (event) => {
      if (!isCurrentEventSource()) return;
      const streamEvent = JSON.parse(event.data);
      enqueueStreamEvent(workflowId, streamEvent);
      if (streamEvent.kind === "claude_start") {
        clearWorkflowStateRefresh();
      }
      if (streamEventNeedsSettledTranscriptDelta(streamEvent)) {
        scheduleSettledTranscriptRefresh(workflowId);
      }
      if (streamEventNeedsWorkflowStateRefresh(streamEvent)) {
        scheduleWorkflowStateRefresh(workflowId);
      }
    });
    eventSource.addEventListener("missing", () => {
      if (!isCurrentEventSource()) return;
      handleMissingWorkflow();
    });
    eventSource.addEventListener("reconcile", () => {
      if (!isCurrentEventSource()) return;
      if (stateRef.current.workflowId === workflowId) {
        reconcileWorkflow(workflowId);
      }
    });
    eventSource.addEventListener("error", () => {
      if (!isCurrentEventSource()) return;
      setState((previous) =>
        previous.statusNotice === "event stream reconnecting..."
          ? previous
          : { ...previous, statusNotice: "event stream reconnecting..." },
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
    await sendAction(busy ? "steer" : "chat", busy ? "you steering" : "you", "sending");
  }

  async function sendAction(action, label, phase) {
    let content = messageRef.current.trim();
    if (!content && action === "interrupt") {
      content = "Stop the current response.";
    }
    if (!content) return;

    const current = stateRef.current;
    if (!current.workflowId) {
      if (action === "interrupt" || action === "steer" || action === "after-tool") return;
      clearComposerInput();
      const pending = createPendingMessage(label, content, phase, current);
      setState((previous) => ({
        ...previous,
        localPending: [...previous.localPending, pending],
      }));
      try {
        await createConversation(content, { preserveLocalPending: true });
        markPendingDelivered(pending.id);
      } catch (error) {
        markPendingFailed(pending.id, error);
      }
      return;
    }

    clearComposerInput();
    const pending = createPendingMessage(label, content, phase, current);
    setState((previous) => {
      let next = {
        ...previous,
        localPending: [...previous.localPending, pending],
      };
      if (action === "interrupt") {
        next = markStreamInterruptedInState(next);
        next.ignoreClaudeUntilStart = true;
      }
      return next;
    });

    try {
      if (action === "chat") {
        await post(`/api/sessions/${current.workflowId}/chat`, { message: content });
      } else if (action === "steer") {
        await post(`/api/sessions/${current.workflowId}/steer`, {
          message: content,
          mode: "immediate",
        });
      } else if (action === "after-tool") {
        await post(`/api/sessions/${current.workflowId}/steer`, {
          message: content,
          mode: "after_next_tool_result",
        });
      } else if (action === "interrupt") {
        await post(`/api/sessions/${current.workflowId}/interrupt`, { message: content });
      }
      markPendingDelivered(pending.id);
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
      closeEventSource();
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
      closeEventSource();
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
              (approval) => approval.approval_id !== approvalId,
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
    if (!state.loginConfigured) return;
    const href = event.currentTarget.getAttribute("href");
    setState((previous) => ({ ...previous, loggingIn: true }));
    setTimeout(() => {
      window.location.href = href;
    }, 750);
  }

  const status = displayStatus(state);
  const artifacts = state.workflowState?.artifacts || [];
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
        subtitle={state.loginSubtitle}
        error={state.loginError}
        onLoginClick={handleLoginClick}
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
            closeEventSource();
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
              workflowState={state.workflowState}
              draftConversation={state.draftConversation}
              loadingConversation={loadingConversation}
              olderMessagesLoading={state.olderMessagesLoading}
              olderMessagesError={state.olderMessagesError}
              localPending={state.localPending}
              streamTurn={state.streamTurn}
              streamPanelCollapsed={state.streamPanelCollapsed}
              resolvingApprovals={state.resolvingApprovals}
              onToggleStreamPanel={() =>
                setState((previous) => ({
                  ...previous,
                  streamPanelCollapsed: !previous.streamPanelCollapsed,
                }))
              }
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
          onMessageChange={updateComposerMessage}
          onSend={sendDefault}
          onInterrupt={() => sendAction("interrupt", "you interrupt", "sending")}
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
        <ArtifactViewer
          viewer={state.artifactViewer}
          onClose={closeArtifactViewer}
        />
      </div>
    </>
  );
}
