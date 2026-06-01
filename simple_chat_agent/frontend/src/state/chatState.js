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

export function applyWorkflowStatePatchInState(previous, patch) {
  if (!previous.workflowState) return previous;
  const revision = Number(patch.state_revision || patch.revision || 0);
  if (revision && revision <= previous.workflowStateProjectionRevision) return previous;
  const {
    revision: _revision,
    transcript: _transcript,
    transcript_offset: _transcriptOffset,
    transcript_total: _transcriptTotal,
    transcript_has_more_before: _transcriptHasMoreBefore,
    transcript_length: _transcriptLength,
    transcript_revision: _transcriptRevision,
    ...statePatch
  } = patch;

  return updateWorkflowStateInState(previous, {
    ...previous.workflowState,
    ...statePatch,
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
  });
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

export function applyTranscriptDeltasInState(previous, result) {
  if (!previous.workflowState || result?.needs_snapshot) return previous;

  const workflowState = previous.workflowState;
  const transcript = [...(workflowState.transcript || [])];
  const transcriptOffset = Number(workflowState.transcript_offset || 0);
  let transcriptEnd = transcriptOffset + transcript.length;
  let transcriptTotal = Math.max(
    Number(result.transcript_length || 0),
    Number(workflowState.transcript_total || workflowState.transcript_length || 0),
    transcriptEnd,
  );
  const settledIndexes = new Set();

  for (const delta of result.deltas || []) {
    const index = Number(delta.index);
    const message = delta.message;
    if (!Number.isFinite(index) || !message) continue;
    if (index < transcriptOffset) continue;
    if (index > transcriptEnd) {
      return previous;
    }
    if (index === transcriptEnd) {
      transcript.push(message);
      transcriptEnd += 1;
    } else {
      transcript[index - transcriptOffset] = message;
    }
    settledIndexes.add(index);
    transcriptTotal = Math.max(transcriptTotal, index + 1);
  }

  const toRevision = Number(result.to_revision || workflowState.transcript_revision || 0);
  if (toRevision <= Number(workflowState.transcript_revision || 0)) {
    return previous;
  }

  const stateWithSettledPending = {
    ...previous,
    localPending: previous.localPending.filter((pending) => {
      const index = Number(pending.transcriptIndex);
      return !Number.isFinite(index) || !settledIndexes.has(index);
    }),
  };

  return updateWorkflowStateInState(stateWithSettledPending, {
    ...workflowState,
    status: result.status || workflowState.status,
    pending_messages: Number(result.pending_messages ?? workflowState.pending_messages ?? 0),
    active_message_index: result.active_message_index ?? null,
    transcript,
    transcript_offset: transcriptOffset,
    transcript_total: transcriptTotal,
    transcript_length: transcriptTotal,
    transcript_revision: toRevision,
    state_revision: Math.max(
      Number(workflowState.state_revision || 0),
      Number(result.state_revision || 0),
    ),
  });
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

export function handleStreamEventInState(previous, event) {
  const projectionResult = applyWorkflowProjectionEventInState(previous, event);
  if (projectionResult.handled) return projectionResult.state;

  const artifactResult = applyArtifactStreamEventInState(previous, event);
  if (artifactResult.handled) return artifactResult.state;

  const next = {
    ...previous,
    streamTurn: cloneStreamTurn(previous.streamTurn),
  };
  const sequence = claudePayloadSequence(event);

  if (event.kind === "claude_start") {
    next.currentClaudeSequence = sequence;
    next.ignoreClaudeUntilStart = false;
    if (next.workflowState) {
      next.workflowState = { ...next.workflowState, status: "responding" };
    }
    const turn = ensureStreamTurn(next, sequence);
    completeOpenToolSegments(turn);
    const agentSegment = ensureAgentSegment(turn, sequence);
    agentSegment.status = "streaming";
    turn.status = "streaming";
    turn.activeSequence = sequence;
  } else if (event.kind === "claude_text_delta" && event.payload?.text) {
    if (!shouldApplyClaudeStreamEvent(next, sequence)) return previous;
    const activeSequence = activeClaudeSequence(next, sequence);
    adoptClaudeSequence(next, activeSequence);
    const agentSegment = ensureAgentSegment(ensureStreamTurn(next, activeSequence), activeSequence);
    agentSegment.status = "streaming";
    agentSegment.text += event.payload.text;
    next.streamTurn.status = "streaming";
  } else if (event.kind === "claude_thinking_start") {
    if (!shouldApplyClaudeStreamEvent(next, sequence)) return previous;
    const activeSequence = activeClaudeSequence(next, sequence);
    adoptClaudeSequence(next, activeSequence);
    const agentSegment = ensureAgentSegment(ensureStreamTurn(next, activeSequence), activeSequence);
    agentSegment.status = "streaming";
    next.streamTurn.status = "streaming";
  } else if (event.kind === "claude_thinking_delta" && event.payload?.thinking) {
    if (!shouldApplyClaudeStreamEvent(next, sequence)) return previous;
    const activeSequence = activeClaudeSequence(next, sequence);
    adoptClaudeSequence(next, activeSequence);
    const agentSegment = ensureAgentSegment(ensureStreamTurn(next, activeSequence), activeSequence);
    agentSegment.status = "streaming";
    agentSegment.thinking += event.payload.thinking;
    next.streamTurn.status = "streaming";
  } else if (event.kind === "claude_cancelled") {
    if (shouldApplyClaudeStreamEvent(next, sequence)) {
      return {
        ...markStreamInterruptedInState(next),
        ignoreClaudeUntilStart: true,
      };
    }
  } else if (event.kind === "claude_complete") {
    if (!shouldApplyClaudeStreamEvent(next, sequence)) return previous;
    const activeSequence = activeClaudeSequence(next, sequence);
    adoptClaudeSequence(next, activeSequence);
    const terminal = isTerminalClaudeStop(event.payload || {});
    const turn =
      streamTurnForSequence(next.streamTurn, activeSequence) ||
      ensureStreamTurn(next, activeSequence);
    const agentSegment = ensureAgentSegment(turn, activeSequence);
    finishAgentSegment(agentSegment, event.payload || {});
    if (terminal) {
      completeOpenToolSegments(turn);
      turn.status = "complete";
      turn.completedAt = new Date().toISOString();
    } else {
      ensureToolSegment(turn, activeSequence);
      turn.status = "tooling";
    }
    turn.lastClaudeCompletedAt = new Date().toISOString();
  } else if (isClaudeToolEvent(event)) {
    if (!shouldApplyClaudeStreamEvent(next, sequence)) return previous;
    const activeSequence = activeClaudeSequence(next, sequence);
    adoptClaudeSequence(next, activeSequence);
    const turn = ensureStreamTurn(next, activeSequence);
    appendStreamToolEvent(turn, event, activeSequence);
    if (turn.status !== "complete" && turn.status !== "interrupted") {
      turn.status = "tooling";
    }
  } else if (!event.kind?.startsWith("claude_")) {
    const turn = ensureStreamTurn(next, next.currentClaudeSequence);
    appendStreamToolEvent(turn, event, next.currentClaudeSequence);
    if (turn.status !== "complete" && turn.status !== "interrupted") {
      turn.status = "tooling";
    }
  }
  return next;
}

function claudePayloadSequence(event) {
  const sequence = event.payload?.sequence;
  return sequence === undefined ? null : sequence;
}

function activeClaudeSequence(state, eventSequence) {
  return eventSequence ?? state.currentClaudeSequence ?? state.streamTurn?.activeSequence ?? null;
}

function adoptClaudeSequence(state, sequence) {
  if (state.currentClaudeSequence !== null && state.currentClaudeSequence !== undefined) return;
  if (sequence === null || sequence === undefined) return;
  state.currentClaudeSequence = sequence;
}

function shouldApplyClaudeStreamEvent(state, eventSequence) {
  if (state.ignoreClaudeUntilStart) return false;
  if (state.currentClaudeSequence === null || state.currentClaudeSequence === undefined) return true;
  if (eventSequence === null || eventSequence === undefined) return true;
  return eventSequence === state.currentClaudeSequence;
}

export function streamEventNeedsSettledTranscriptDelta(event) {
  return event.kind === "claude_complete" && isTerminalClaudeStop(event.payload || {});
}

export function streamEventNeedsWorkflowStateRefresh(event) {
  return event.kind === "claude_complete" && event.payload?.stop_reason === "tool_use";
}

function applyWorkflowProjectionEventInState(previous, event) {
  if (!previous.workflowState) return { handled: false, state: previous };
  const payload = event.payload || {};

  if (event.kind === "workflow_state") {
    return {
      handled: true,
      state: applyWorkflowStatePatchInState(previous, payload),
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
    segments: [],
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
    segments: (turn.segments || []).map((segment) => ({
      ...segment,
      events: segment.events ? [...segment.events] : undefined,
    })),
  };
}

function ensureAgentSegment(turn, sequence) {
  const normalizedSequence = sequence ?? turn.activeSequence ?? null;
  registerStreamSequence(turn, normalizedSequence);
  turn.activeSequence = normalizedSequence;
  let segment = turn.segments.find(
    (candidate) =>
      candidate.type === "agent" && candidate.sequence === normalizedSequence,
  );
  if (!segment) {
    segment = {
      id: `agent:${normalizedSequence ?? "unknown"}:${turn.segments.length}`,
      type: "agent",
      sequence: normalizedSequence,
      status: "streaming",
      text: "",
      thinking: "",
      stopReason: null,
      usage: null,
      startedAt: new Date().toISOString(),
      completedAt: null,
    };
    turn.segments.push(segment);
  }
  return segment;
}

function ensureToolSegment(turn, sequence) {
  const normalizedSequence = sequence ?? turn.activeSequence ?? null;
  const existing = latestToolSegmentForSequence(turn, normalizedSequence);
  if (existing) return existing;

  const segment = {
    id: `tools:${normalizedSequence ?? "unknown"}:${turn.segments.length}`,
    type: "tools",
    afterSequence: normalizedSequence,
    status: "streaming",
    events: [],
    startedAt: new Date().toISOString(),
    completedAt: null,
  };
  turn.segments.push(segment);
  return segment;
}

function latestToolSegmentForSequence(turn, sequence) {
  for (let index = turn.segments.length - 1; index >= 0; index -= 1) {
    const segment = turn.segments[index];
    if (segment.type !== "tools") continue;
    if (sequence === null || sequence === undefined || segment.afterSequence === sequence) {
      return segment;
    }
  }
  return null;
}

function completeOpenToolSegments(turn) {
  for (const segment of turn.segments) {
    if (segment.type !== "tools" || segment.status === "complete") continue;
    segment.status = "complete";
    segment.completedAt = new Date().toISOString();
  }
}

function finishAgentSegment(segment, payload) {
  const text = String(payload.text || segment.text || "").trim();
  segment.text = text;
  segment.thinking = String(segment.thinking || "").trim();
  segment.status = "complete";
  segment.stopReason = payload.stop_reason || "unknown";
  segment.usage = payload.usage || null;
  segment.completedAt = new Date().toISOString();
}

function appendStreamToolEvent(turn, event, sequence) {
  const toolSegment = ensureToolSegment(turn, sequence);
  toolSegment.events = mergeStreamToolEvent(toolSegment.events || [], event);
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

function markStreamCommittedInState(state) {
  return {
    ...state,
    streamTurn: null,
    currentClaudeSequence: null,
    ignoreClaudeUntilStart: false,
  };
}

function transcriptMessageForPending(pending) {
  const phase = String(pending.phase || "");
  if (phase.startsWith("failed")) return null;
  if (!String(pending.label || "").startsWith("you")) return null;
  return { role: "user", content: pending.content };
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
