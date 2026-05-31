export function updateWorkflowStateInState(previous, nextWorkflowState) {
  const normalized = normalizeWorkflowState(nextWorkflowState, previous.workflowState);
  const previousAssistantCount = previous.workflowState
    ? previous.workflowState.transcript.filter((message) => message.role === "assistant").length
    : 0;
  const nextAssistantCount = normalized.transcript.filter(
    (message) => message.role === "assistant",
  ).length;
  const resolvingApprovals = new Set(previous.resolvingApprovals);
  const pendingApprovalIds = new Set(
    (normalized.pending_approvals || []).map((approval) => approval.approval_id),
  );
  for (const approvalId of resolvingApprovals) {
    if (!pendingApprovalIds.has(approvalId)) resolvingApprovals.delete(approvalId);
  }
  let next = {
    ...previous,
    workflowState: normalized,
    workflowStateProjectionRevision: Math.max(
      previous.workflowStateProjectionRevision || 0,
      Number(normalized.state_revision || 0),
    ),
    workflowTranscriptProjectionRevision: Math.max(
      previous.workflowTranscriptProjectionRevision || 0,
      Number(normalized.transcript_revision || 0),
    ),
    localPending: previous.localPending.filter(
      (pending) => !isAcknowledged(pending, normalized),
    ),
    resolvingApprovals,
    statusNotice: "",
  };
  if (nextAssistantCount > previousAssistantCount) next = markStreamCommittedInState(next);
  if (!hasLiveWorkflowActivity(next, normalized)) next = markStreamCommittedInState(next);
  return next;
}

function normalizeWorkflowState(nextWorkflowState, previousWorkflowState = null) {
  const nextTranscript = nextWorkflowState.transcript || previousWorkflowState?.transcript || [];
  const hasTranscript = Object.prototype.hasOwnProperty.call(
    nextWorkflowState,
    "transcript",
  );
  const transcriptOffset = hasTranscript
    ? Number(nextWorkflowState.transcript_offset || 0)
    : Number(previousWorkflowState?.transcript_offset || 0);
  const transcriptTotal = Number(
    nextWorkflowState.transcript_total ??
      nextWorkflowState.transcript_length ??
      previousWorkflowState?.transcript_total ??
      transcriptOffset + nextTranscript.length,
  );
  return {
    ...nextWorkflowState,
    transcript: nextTranscript,
    transcript_offset: transcriptOffset,
    transcript_total: transcriptTotal,
    transcript_has_more_before:
      nextWorkflowState.transcript_has_more_before ??
      previousWorkflowState?.transcript_has_more_before ??
      transcriptOffset > 0,
    pending_approvals: nextWorkflowState.pending_approvals || [],
    queued_message_indices: nextWorkflowState.queued_message_indices || [],
    artifacts: nextWorkflowState.artifacts || previousWorkflowState?.artifacts || [],
  };
}

export function createPendingMessage(label, content, phase, state) {
  return {
    id: crypto.randomUUID(),
    label,
    content,
    phase,
    transcriptIndex: workflowTranscriptEnd(state.workflowState),
  };
}

export function visibleMessageItems(transcript, localPending, transcriptOffset = 0, transcriptTotal = null) {
  const transcriptEnd = transcriptOffset + transcript.length;
  const total = transcriptTotal ?? transcriptEnd;
  const pendingByIndex = new Map();
  for (const pending of localPending) {
    if (isPendingAcknowledgedByTranscript(pending, transcript)) continue;
    const index = pendingTranscriptIndex(pending, total);
    if (index < transcriptOffset || index > transcriptEnd) continue;
    const pendingAtIndex = pendingByIndex.get(index) || [];
    pendingAtIndex.push(pending);
    pendingByIndex.set(index, pendingAtIndex);
  }

  const items = [];
  for (let index = transcriptOffset; index <= transcriptEnd; index += 1) {
    for (const pending of pendingByIndex.get(index) || []) {
      items.push({ kind: "pending", pending });
    }
    if (index < transcriptEnd) {
      items.push({
        kind: "transcript",
        message: transcript[index - transcriptOffset],
        index,
      });
    }
  }
  return items;
}

export function prependTranscriptPageInState(previous, page) {
  if (!previous.workflowState) return previous;
  return {
    ...previous,
    workflowState: mergeWorkflowTranscriptPage(previous.workflowState, page),
    workflowTranscriptProjectionRevision: Math.max(
      previous.workflowTranscriptProjectionRevision || 0,
      Number(page.revision || 0),
    ),
    olderMessagesLoading: false,
    olderMessagesError: "",
  };
}

function mergeWorkflowTranscriptPage(workflowState, page) {
  const messages = page.messages || page.transcript || [];
  const pageStart = Number(page.start ?? 0);
  const pageEnd = Number(page.end ?? pageStart + messages.length);
  const pageTotal = Number(page.total ?? workflowTranscriptEnd(workflowState));
  const currentTranscript = workflowState.transcript || [];
  const currentStart = Number(workflowState.transcript_offset || 0);
  const currentEnd = currentStart + currentTranscript.length;

  if (!currentTranscript.length) {
    return {
      ...workflowState,
      transcript: messages,
      transcript_offset: pageStart,
      transcript_total: pageTotal,
      transcript_length: pageTotal,
      transcript_has_more_before: pageStart > 0,
    };
  }

  if (pageEnd < currentStart || pageStart > currentEnd) {
    return {
      ...workflowState,
      transcript: messages,
      transcript_offset: pageStart,
      transcript_total: pageTotal,
      transcript_length: pageTotal,
      transcript_has_more_before: pageStart > 0,
    };
  }

  const mergedStart = Math.min(currentStart, pageStart);
  const mergedEnd = Math.max(currentEnd, pageEnd);
  const merged = new Array(mergedEnd - mergedStart);
  currentTranscript.forEach((message, index) => {
    merged[currentStart - mergedStart + index] = message;
  });
  messages.forEach((message, index) => {
    merged[pageStart - mergedStart + index] = message;
  });

  return {
    ...workflowState,
    transcript: merged.filter(Boolean),
    transcript_offset: mergedStart,
    transcript_total: Math.max(pageTotal, workflowState.transcript_total || 0, mergedEnd),
    transcript_length: Math.max(pageTotal, workflowState.transcript_total || 0, mergedEnd),
    transcript_has_more_before: mergedStart > 0,
  };
}

function workflowTranscriptEnd(workflowState) {
  if (!workflowState) return 0;
  const total = Number(workflowState.transcript_total ?? workflowState.transcript_length);
  if (Number.isFinite(total)) return total;
  return Number(workflowState.transcript_offset || 0) + (workflowState.transcript || []).length;
}

function pendingTranscriptIndex(pending, transcriptLength) {
  const index = Number(pending.transcriptIndex);
  if (!Number.isFinite(index)) return transcriptLength;
  return Math.max(0, Math.min(transcriptLength, index));
}

function rawPendingTranscriptIndex(pending) {
  const index = Number(pending.transcriptIndex);
  return Number.isFinite(index) ? index : Number.MAX_SAFE_INTEGER;
}

export function handleStreamEventInState(previous, event) {
  const projectionResult = applyWorkflowProjectionEventInState(previous, event);
  if (projectionResult.handled) return projectionResult.state;

  const artifactResult = applyArtifactStreamEventInState(previous, event);
  if (artifactResult.handled) return artifactResult.state;

  if (!hasLiveWorkflowActivity(previous)) return previous;

  const next = {
    ...previous,
    streamTurn: cloneStreamTurn(previous.streamTurn),
  };
  const sequence = event.payload?.sequence ?? null;

  if (event.kind === "claude_start") {
    next.currentClaudeSequence = sequence;
    next.ignoreClaudeUntilStart = false;
    if (next.workflowState) {
      next.workflowState = { ...next.workflowState, status: "responding" };
    }
    if (isOpenStreamTurn(next.streamTurn)) {
      registerStreamSequence(next.streamTurn, sequence);
      next.streamTurn.status = "streaming";
      next.streamTurn.activeSequence = sequence;
    }
  } else if (event.kind === "claude_text_delta" && event.payload?.text) {
    if (next.ignoreClaudeUntilStart || sequence !== next.currentClaudeSequence) return previous;
    const turn = ensureStreamTurn(next, sequence);
    turn.status = "streaming";
    turn.text += event.payload.text;
  } else if (event.kind === "claude_thinking_start") {
    if (next.ignoreClaudeUntilStart || sequence !== next.currentClaudeSequence) return previous;
    const turn = ensureStreamTurn(next, sequence);
    turn.status = "streaming";
  } else if (event.kind === "claude_thinking_delta" && event.payload?.thinking) {
    if (next.ignoreClaudeUntilStart || sequence !== next.currentClaudeSequence) return previous;
    const turn = ensureStreamTurn(next, sequence);
    turn.status = "streaming";
    turn.thinking += event.payload.thinking;
  } else if (event.kind === "claude_cancelled") {
    if (sequence === next.currentClaudeSequence) {
      return {
        ...markStreamInterruptedInState(next),
        ignoreClaudeUntilStart: true,
      };
    }
  } else if (event.kind === "claude_complete") {
    const terminal = isTerminalClaudeStop(event.payload || {});
    if (!terminal && sequence !== next.currentClaudeSequence) return previous;
    const turn = streamTurnForSequence(next.streamTurn, sequence) || ensureStreamTurn(next, sequence);
    if (turn) {
      finishStreamClaudeTurn(turn, event.payload || {});
      turn.status = terminal ? "complete" : turn.currentEvents.length ? "tooling" : "waiting";
      turn.lastClaudeCompletedAt = new Date().toISOString();
    }
    if (terminal) {
      return settleCompletedClaudeTurnInState(next, event.payload || {});
    }
  } else if (isClaudeToolEvent(event)) {
    const turn = ensureStreamTurn(next, next.currentClaudeSequence);
    appendStreamToolEvent(turn, event);
    if (turn.status !== "complete" && turn.status !== "interrupted") {
      turn.status = "tooling";
    }
  } else if (!event.kind?.startsWith("claude_")) {
    const turn = ensureStreamTurn(next, next.currentClaudeSequence);
    appendStreamToolEvent(turn, event);
    if (turn.status !== "complete" && turn.status !== "interrupted") {
      turn.status = "tooling";
    }
  }
  return next;
}

export function streamEventNeedsDeferredWorkflowReconcile(event) {
  return event.kind === "claude_complete" && event.payload?.stop_reason === "tool_use";
}

function applyWorkflowProjectionEventInState(previous, event) {
  if (!previous.workflowState) return { handled: false, state: previous };
  const payload = event.payload || {};

  if (event.kind === "workflow_state") {
    const revision = Number(payload.revision || 0);
    if (revision && revision <= previous.workflowStateProjectionRevision) {
      return { handled: true, state: previous };
    }
    const { revision: _revision, transcript_revision: _transcriptRevision, ...patch } = payload;
    return {
      handled: true,
      state: updateWorkflowStateInState(previous, {
        ...previous.workflowState,
        ...patch,
        state_revision: revision || previous.workflowState.state_revision || 0,
        transcript_revision: previous.workflowState.transcript_revision || 0,
        transcript: previous.workflowState.transcript || [],
        transcript_offset: previous.workflowState.transcript_offset || 0,
        transcript_total:
          previous.workflowState.transcript_total ||
          previous.workflowState.transcript_length ||
          (previous.workflowState.transcript || []).length,
        transcript_has_more_before: previous.workflowState.transcript_has_more_before || false,
        artifacts: previous.workflowState.artifacts || [],
      }),
    };
  }

  if (event.kind === "workflow_transcript") {
    const revision = Number(payload.revision || 0);
    if (revision && revision <= previous.workflowTranscriptProjectionRevision) {
      return { handled: true, state: previous };
    }
    return {
      handled: true,
      state: updateWorkflowStateInState(previous, {
        ...previous.workflowState,
        transcript: payload.transcript || [],
        transcript_offset: 0,
        transcript_total: (payload.transcript || []).length,
        transcript_has_more_before: false,
        transcript_revision: revision || previous.workflowState.transcript_revision || 0,
        artifacts: previous.workflowState.artifacts || [],
      }),
    };
  }

  if (event.kind === "workflow_transcript_page") {
    const revision = Number(payload.revision || 0);
    if (revision && revision <= previous.workflowTranscriptProjectionRevision) {
      return { handled: true, state: previous };
    }
    const workflowState = mergeWorkflowTranscriptPage(previous.workflowState, payload);
    return {
      handled: true,
      state: updateWorkflowStateInState(previous, {
        ...workflowState,
        transcript_revision: revision || workflowState.transcript_revision || 0,
        artifacts: previous.workflowState.artifacts || [],
      }),
    };
  }

  return { handled: false, state: previous };
}

function applyArtifactStreamEventInState(previous, event) {
  if (event.kind !== "artifact_create_complete" || !previous.workflowState) {
    return { handled: false, state: previous };
  }
  const artifact = event.payload || {};
  if (!artifact.artifact_id) return { handled: true, state: previous };
  const artifacts = previous.workflowState.artifacts || [];
  if (artifacts.some((existing) => existing.artifact_id === artifact.artifact_id)) {
    return { handled: true, state: previous };
  }
  return {
    handled: true,
    state: {
      ...previous,
      workflowState: {
        ...previous.workflowState,
        artifacts: [...artifacts, artifact],
      },
    },
  };
}

function ensureStreamTurn(state, sequence) {
  if (!isOpenStreamTurn(state.streamTurn)) {
    state.streamTurn = createStreamTurn(sequence);
  } else {
    registerStreamSequence(state.streamTurn, sequence);
  }
  return state.streamTurn;
}

function streamTurnForSequence(turn, sequence) {
  if (!isOpenStreamTurn(turn)) return null;
  if (sequence === null) return turn;
  return turn.sequences.includes(sequence) ? turn : null;
}

function isOpenStreamTurn(turn) {
  return Boolean(turn && turn.status !== "complete" && turn.status !== "interrupted");
}

function registerStreamSequence(turn, sequence) {
  if (sequence !== null && !turn.sequences.includes(sequence)) {
    turn.sequences.push(sequence);
  }
}

function createStreamTurn(sequence) {
  return {
    sequence,
    sequences: sequence === null ? [] : [sequence],
    activeSequence: sequence,
    status: "streaming",
    text: "",
    thinking: "",
    finishedTurns: [],
    currentEvents: [],
    startedAt: new Date().toISOString(),
    completedAt: null,
    lastClaudeCompletedAt: null,
    interrupted: false,
  };
}

function cloneStreamTurn(turn) {
  if (!turn) return null;
  return {
    ...turn,
    sequences: [...turn.sequences],
    finishedTurns: turn.finishedTurns.map((finishedTurn) => ({
      ...finishedTurn,
      events: [...(finishedTurn.events || [])],
    })),
    currentEvents: [...turn.currentEvents],
  };
}

function finishStreamClaudeTurn(turn, payload) {
  const text = String(payload.text || turn.text || "").trim();
  const stopReason = payload.stop_reason || "unknown";
  const sequence = payload.sequence ?? turn.activeSequence;
  turn.finishedTurns.push({
    sequence,
    text,
    thinking: String(turn.thinking || "").trim(),
    stopReason,
    usage: payload.usage || null,
    events: turn.currentEvents,
    completedAt: new Date().toISOString(),
  });
  turn.finishedTurns = turn.finishedTurns.slice(-12);
  turn.text = "";
  turn.thinking = "";
  turn.currentEvents = [];
}

function appendStreamToolEvent(turn, event) {
  const finishedTurn = latestFinishedToolUseTurn(turn);
  if (finishedTurn) {
    finishedTurn.events = mergeStreamToolEvent(finishedTurn.events || [], event);
    return;
  }
  turn.currentEvents = mergeStreamToolEvent(turn.currentEvents, event);
}

function isClaudeToolEvent(event) {
  return event.kind?.startsWith("claude_tool_input_");
}

function mergeStreamToolEvent(events, event) {
  if (isPythonSandboxOutputEvent(event)) {
    return mergePythonSandboxOutputEvent(events, event).slice(-5);
  }

  if (isPythonSandboxProgressEvent(event)) {
    return mergePythonSandboxProgressEvent(events, event).slice(-5);
  }

  if (!event.kind?.startsWith("claude_tool_input_")) {
    return [...events, event].slice(-5);
  }

  const key = streamToolInputKey(event);
  const nextEvents = [...events];
  const existingIndex = nextEvents.findIndex(
    (candidate) =>
      candidate.kind?.startsWith("claude_tool_input_") &&
      streamToolInputKey(candidate) === key,
  );
  const existing = existingIndex >= 0 ? nextEvents[existingIndex] : null;
  const merged = mergeToolInputEvent(existing, event, key);
  if (existingIndex >= 0) {
    nextEvents[existingIndex] = merged;
  } else {
    nextEvents.push(merged);
  }
  return nextEvents.slice(-5);
}

function isPythonSandboxOutputEvent(event) {
  return event.kind === "python_sandbox_stdout" || event.kind === "python_sandbox_stderr";
}

function isPythonSandboxProgressEvent(event) {
  return event.kind === "python_sandbox_progress";
}

function isPythonSandboxEvent(event) {
  return event.kind?.startsWith("python_sandbox_");
}

function mergePythonSandboxOutputEvent(events, event) {
  const key = pythonSandboxOutputKey(event);
  const nextEvents = [...events];
  const existingIndex = nextEvents.findIndex(
    (candidate) =>
      isPythonSandboxOutputEvent(candidate) && pythonSandboxOutputKey(candidate) === key,
  );
  if (existingIndex < 0) return [...nextEvents, event];

  const existing = nextEvents[existingIndex];
  const existingPayload = existing.payload || {};
  const payload = event.payload || {};
  nextEvents[existingIndex] = {
    ...existing,
    payload: {
      ...existingPayload,
      ...payload,
      text: String(existingPayload.text || "") + String(payload.text || ""),
    },
  };
  return nextEvents;
}

function mergePythonSandboxProgressEvent(events, event) {
  const key = pythonSandboxInstanceKey(event);
  const nextEvents = [...events];
  const existingIndex = findPythonSandboxProgressTargetIndex(nextEvents, key);
  if (existingIndex < 0) return [...nextEvents, event];

  const existing = nextEvents[existingIndex];
  nextEvents[existingIndex] = {
    ...existing,
    payload: {
      ...(existing.payload || {}),
      ...(event.payload || {}),
    },
  };
  return nextEvents;
}

function findPythonSandboxProgressTargetIndex(events, key) {
  for (let index = events.length - 1; index >= 0; index -= 1) {
    const candidate = events[index];
    if (isPythonSandboxOutputEvent(candidate) && pythonSandboxInstanceKey(candidate) === key) {
      return index;
    }
  }
  for (let index = events.length - 1; index >= 0; index -= 1) {
    const candidate = events[index];
    if (isPythonSandboxEvent(candidate) && pythonSandboxInstanceKey(candidate) === key) {
      return index;
    }
  }
  return -1;
}

function pythonSandboxOutputKey(event) {
  return `${event.kind}:${event.tool_name || ""}:${event.step || ""}`;
}

function pythonSandboxInstanceKey(event) {
  return `${event.tool_name || ""}:${event.step || ""}`;
}

function mergeToolInputEvent(existing, event, key) {
  const existingPayload = existing?.payload || {};
  const payload = event.payload || {};
  const nextPayload = { ...existingPayload, ...payload };
  const existingPartial = String(existingPayload.input_partial || "");

  if (event.kind === "claude_tool_input_delta") {
    nextPayload.input_partial = existingPartial + String(payload.partial_json || "");
    nextPayload.status = "streaming input";
  } else if (event.kind === "claude_tool_input_complete") {
    nextPayload.input_partial = existingPartial;
    nextPayload.status = "input complete";
  } else {
    nextPayload.input_partial = existingPartial;
    nextPayload.status = "building input";
  }

  return {
    ...(existing || event),
    kind: event.kind,
    payload: nextPayload,
    streamToolInputKey: key,
  };
}

function streamToolInputKey(event) {
  return (
    event.streamToolInputKey ||
    event.payload?.tool_use_id ||
    `block:${event.payload?.content_block_index ?? "unknown"}`
  );
}

function latestFinishedToolUseTurn(turn) {
  const latest = turn.finishedTurns[turn.finishedTurns.length - 1];
  if (!latest || latest.stopReason !== "tool_use") return null;
  return latest;
}

function markStreamCommittedInState(state) {
  return {
    ...state,
    streamTurn: null,
    currentClaudeSequence: null,
    ignoreClaudeUntilStart: false,
  };
}

function settleCompletedClaudeTurnInState(state, payload) {
  const workflowState = state.workflowState;
  if (!workflowState) return markStreamCommittedInState(state);

  const transcript = [...(workflowState.transcript || [])];
  const consumedPendingIds = new Set();
  const pendingMessages = [...state.localPending].sort(
    (left, right) => rawPendingTranscriptIndex(left) - rawPendingTranscriptIndex(right),
  );
  const pendingToCommit = pendingMessages.find(transcriptMessageForPending);

  if (pendingToCommit) {
    const message = transcriptMessageForPending(pendingToCommit);
    if (message && !isPendingAcknowledgedByTranscript(pendingToCommit, transcript)) {
      transcript.push(message);
    }
    consumedPendingIds.add(pendingToCommit.id);
  }

  const assistantText = String(payload.text || "").trim();
  if (assistantText && !lastTranscriptMessageMatches(transcript, "assistant", assistantText)) {
    transcript.push({ role: "assistant", content: assistantText });
  }

  const transcriptOffset = Number(workflowState.transcript_offset || 0);
  const transcriptEnd = transcriptOffset + transcript.length;
  const transcriptTotal = Math.max(
    transcriptEnd,
    Number(workflowState.transcript_total || 0),
  );
  const localPending = state.localPending.filter(
    (pending) => !consumedPendingIds.has(pending.id),
  );
  const pendingMessagesCount = localPending.filter(transcriptMessageForPending).length;

  return markStreamCommittedInState({
    ...state,
    workflowState: {
      ...workflowState,
      status: pendingMessagesCount ? "responding" : "idle",
      pending_messages: pendingMessagesCount,
      active_message_index: null,
      queued_message_indices: [],
      transcript,
      transcript_total: transcriptTotal,
      transcript_length: transcriptTotal,
      state_revision: Number(workflowState.state_revision || 0) + 1,
      transcript_revision: Number(workflowState.transcript_revision || 0) + 1,
    },
    localPending,
  });
}

function transcriptMessageForPending(pending) {
  const phase = String(pending.phase || "");
  if (phase.startsWith("failed")) return null;
  if (!String(pending.label || "").startsWith("you")) return null;
  return { role: "user", content: pending.content };
}

function lastTranscriptMessageMatches(transcript, role, content) {
  const last = transcript[transcript.length - 1];
  return last?.role === role && last?.content === content;
}

export function markStreamInterruptedInState(state) {
  return {
    ...state,
    streamTurn: null,
    currentClaudeSequence: null,
  };
}

function hasLiveWorkflowActivity(state, workflowState = state.workflowState) {
  if (!workflowState) return true;
  if (workflowState.status === "responding") return true;
  if (Number(workflowState.pending_messages || 0) > 0) return true;
  return state.localPending.length > 0;
}

function isTerminalClaudeStop(payload) {
  return payload.stop_reason && payload.stop_reason !== "tool_use";
}

function isAcknowledged(pending, workflowState) {
  return isPendingAcknowledgedByTranscript(pending, workflowState.transcript);
}

function isPendingAcknowledgedByTranscript(pending, transcript) {
  return transcript.some((message) => {
    if (message.role === "user" && message.content === pending.content) return true;
    if (message.role === "system" && message.content.includes(pending.content)) return true;
    return false;
  });
}

export function displayStatus(state) {
  if (state.statusNotice) return state.statusNotice;
  const workflowState = state.workflowState;
  const thinkingLabel = workflowState?.thinking?.enabled ? " | thinking" : "";
  const modelLabel = workflowState?.model ? ` | ${workflowState.model}${thinkingLabel}` : "";
  if (state.draftConversation) return "draft | workflow not started";
  if (workflowState) {
    const queued = workflowState.pending_messages
      ? `, queued: ${workflowState.pending_messages}`
      : "";
    return `${workflowState.status}${queued}${modelLabel}`;
  }
  return state.auth === "app" ? "starting..." : "connecting...";
}

export function agentSettingsFromConfig(config) {
  return normalizeAgentSettings(
    {
      model: localStorage.getItem("simpleChatModel") || config.default_model || "",
      thinkingEnabled: localStorage.getItem("simpleChatThinkingEnabled") === "true",
      thinkingMode: localStorage.getItem("simpleChatThinkingMode") || config.thinking?.mode || "enabled",
      thinkingBudgetTokens: Number(
        localStorage.getItem("simpleChatThinkingBudgetTokens") ||
          config.thinking?.budget_tokens ||
          4096,
      ),
      thinkingEffort: localStorage.getItem("simpleChatThinkingEffort") || config.thinking?.effort || "max",
    },
    config,
  );
}

export function agentSettingsFromWorkflowState(workflowState, config) {
  const thinking = workflowState?.thinking || {};
  return normalizeAgentSettings(
    {
      model: workflowState?.model || config.default_model || "",
      thinkingEnabled: Boolean(thinking.enabled),
      thinkingMode: thinking.mode || config.thinking?.mode || "enabled",
      thinkingBudgetTokens: Number(
        thinking.budget_tokens ||
          config.thinking?.budget_tokens ||
          4096,
      ),
      thinkingEffort: thinking.effort || config.thinking?.effort || "max",
    },
    config,
    { allowUnknownModel: true },
  );
}

export function normalizeAgentSettings(agentSettings, config, options = {}) {
  const modelOptions = config.model_options || [];
  const model =
    agentSettings.model && modelOptions.includes(agentSettings.model)
      ? agentSettings.model
      : options.allowUnknownModel && agentSettings.model
        ? agentSettings.model
        : config.default_model || "";
  let thinkingModes = thinkingModesForModel(config, model);
  if (
    options.allowUnknownModel &&
    agentSettings.thinkingMode &&
    !thinkingModes.includes(agentSettings.thinkingMode)
  ) {
    thinkingModes = [agentSettings.thinkingMode, ...thinkingModes];
  }
  const thinkingMode = thinkingModes.includes(agentSettings.thinkingMode)
    ? agentSettings.thinkingMode
    : defaultThinkingModeForModel(config, model);
  const effortOptions = effortOptionsForModel(config, model);
  const thinkingEffort = effortOptions.includes(agentSettings.thinkingEffort)
    ? agentSettings.thinkingEffort
    : defaultEffortForModel(config, model);
  return {
    ...agentSettings,
    model,
    thinkingEnabled: Boolean(agentSettings.thinkingEnabled && thinkingModes.length),
    thinkingMode,
    thinkingEffort,
  };
}

export function saveAgentSettings(agentSettings) {
  localStorage.setItem("simpleChatModel", agentSettings.model);
  localStorage.setItem("simpleChatThinkingEnabled", String(agentSettings.thinkingEnabled));
  localStorage.setItem("simpleChatThinkingMode", agentSettings.thinkingMode);
  localStorage.setItem(
    "simpleChatThinkingBudgetTokens",
    String(agentSettings.thinkingBudgetTokens),
  );
  localStorage.setItem("simpleChatThinkingEffort", agentSettings.thinkingEffort);
}

export function modelOptionsFromConfig(config) {
  if (Array.isArray(config?.models) && config.models.length) {
    return config.models;
  }
  return (config?.model_options || []).map((model) => ({
    id: model,
    display_name: model,
  }));
}

export function modelOptionsForSelection(config, selectedModel) {
  const options = modelOptionsFromConfig(config);
  if (!selectedModel || options.some((model) => model.id === selectedModel)) {
    return options;
  }
  return [
    {
      id: selectedModel,
      display_name: selectedModel,
    },
    ...options,
  ];
}

export function thinkingModesForModel(config, modelId) {
  const model = modelConfigForId(config, modelId);
  const modes = model?.thinking?.modes || [];
  if (model) return modes;
  if (modes.length) return modes;
  return config?.thinking?.mode_options?.length ? config.thinking.mode_options : ["enabled"];
}

export function defaultThinkingModeForModel(config, modelId) {
  const model = modelConfigForId(config, modelId);
  const mode = model?.thinking?.default_mode || config?.thinking?.mode || "enabled";
  return thinkingModesForModel(config, modelId).includes(mode)
    ? mode
    : thinkingModesForModel(config, modelId)[0] || "enabled";
}

export function effortOptionsForModel(config, modelId) {
  const model = modelConfigForId(config, modelId);
  const options = model?.effort_options || [];
  if (options.length) return options;
  return config?.thinking?.effort_options?.length
    ? config.thinking.effort_options
    : ["max"];
}

export function defaultEffortForModel(config, modelId) {
  const model = modelConfigForId(config, modelId);
  const effort = model?.default_effort || config?.thinking?.effort || "max";
  return effortOptionsForModel(config, modelId).includes(effort)
    ? effort
    : effortOptionsForModel(config, modelId).at(-1) || "max";
}

function modelConfigForId(config, modelId) {
  return (config?.models || []).find((model) => model.id === modelId) || null;
}

export function temporalUiUrl(conversation) {
  if (conversation.temporal_ui_url) return conversation.temporal_ui_url;
  const workflow = encodeURIComponent(conversation.workflow_id);
  const run = encodeURIComponent(conversation.run_id || "");
  if (run) {
    return `http://localhost:8233/namespaces/default/workflows/${workflow}/${run}/history`;
  }
  return `http://localhost:8233/namespaces/default/workflows/${workflow}`;
}
