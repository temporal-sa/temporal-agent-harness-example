import {
  AGENT_STREAM_EVENT_PREFIX,
  AGENT_TOOL_INPUT_EVENT_PREFIX,
  AgentStreamEventKind,
} from "./streamEvents.js";

export function updateWorkflowStateInState(previous, nextWorkflowState) {
  const normalized = normalizeWorkflowState(nextWorkflowState, previous.workflowState);
  const previousAssistantIndexes = assistantTranscriptIndexes(previous.workflowState);
  const nextAssistantIndexes = assistantTranscriptIndexes(normalized);
  const previousAssistantIndexSet = new Set(previousAssistantIndexes);
  const newAssistantIndex = previous.workflowState
    ? nextAssistantIndexes.find((index) => !previousAssistantIndexSet.has(index))
    : undefined;
  const initialCompletedStreamAssistantIndex =
    !previous.workflowState && previous.streamTurn
      ? nextAssistantIndexes[nextAssistantIndexes.length - 1]
      : undefined;
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
  if (newAssistantIndex !== undefined) {
    next = markStreamCommittedInState(next, { assistantIndex: newAssistantIndex });
  }
  if (!hasLiveWorkflowActivity(next, normalized)) {
    next = markStreamCommittedInState(next, {
      assistantIndex: initialCompletedStreamAssistantIndex,
    });
  }
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
    attachments: previous.workflowState.attachments || [],
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
    attachments:
      nextWorkflowState.attachments || previousWorkflowState?.attachments || [],
  };
}

export function createPendingMessage(label, content, phase, state, attachments = []) {
  return {
    id: crypto.randomUUID(),
    label,
    content,
    attachments: [...attachments],
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

export function toggleExpandedTraceInState(previous, transcriptIndex) {
  const normalizedIndex = Number(transcriptIndex);
  if (!Number.isFinite(normalizedIndex)) return previous;
  return {
    ...previous,
    expandedTraceIndex:
      previous.expandedTraceIndex === normalizedIndex ? null : normalizedIndex,
  };
}

export function setTurnTraceLoadingInState(previous, transcriptIndex) {
  const normalizedIndex = Number(transcriptIndex);
  if (!Number.isFinite(normalizedIndex)) return previous;
  return {
    ...previous,
    turnTraces: {
      ...(previous.turnTraces || {}),
      [normalizedIndex]: {
        ...(previous.turnTraces?.[normalizedIndex] || {}),
        status: "loading",
        error: "",
      },
    },
  };
}

export function setTurnTraceErrorInState(previous, transcriptIndex, error) {
  const normalizedIndex = Number(transcriptIndex);
  if (!Number.isFinite(normalizedIndex)) return previous;
  return {
    ...previous,
    turnTraces: {
      ...(previous.turnTraces || {}),
      [normalizedIndex]: {
        ...(previous.turnTraces?.[normalizedIndex] || {}),
        status: "error",
        error: String(error),
      },
    },
  };
}

export function mergeReplayedTurnTracesInState(previous, events) {
  const replayed = turnTracesFromStreamEvents(
    events || [],
    previous.workflowState,
  );
  if (!Object.keys(replayed).length) return previous;
  return {
    ...previous,
    turnTraces: {
      ...(previous.turnTraces || {}),
      ...replayed,
    },
  };
}

export function restoreActiveStreamTurnInState(previous, events) {
  if (!Array.isArray(events) || !events.length) return previous;
  const { replayState, pendingTerminalCompletion } = replayStreamTurns(
    events,
    previous.workflowState,
  );
  const streamTurn = replayState.streamTurn;
  if (
    !streamTurn ||
    pendingTerminalCompletion ||
    !isOpenStreamTurn(streamTurn) ||
    !(streamTurn.segments || []).length
  ) {
    return previous;
  }
  return {
    ...previous,
    streamTurn: trimTraceTurn(cloneStreamTurn(streamTurn)),
    currentAgentSequence: replayState.currentAgentSequence,
    ignoreAgentUntilStart: replayState.ignoreAgentUntilStart,
  };
}

export function applyReplayedStreamEventsInState(previous, events) {
  let next = previous;
  for (const event of events || []) {
    next = handleStreamEventInState(next, event);
  }
  return next;
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
  const nextStateRevision = Math.max(
    Number(workflowState.state_revision || 0),
    Number(result.state_revision || 0),
  );
  const stateOnlyChanged =
    nextStateRevision > Number(workflowState.state_revision || 0) ||
    (result.status && result.status !== workflowState.status) ||
    Number(result.pending_messages ?? workflowState.pending_messages ?? 0) !==
      Number(workflowState.pending_messages ?? 0) ||
    (result.active_message_index ?? null) !==
      (workflowState.active_message_index ?? null);
  if (toRevision <= Number(workflowState.transcript_revision || 0)) {
    if (stateOnlyChanged) {
      const next = updateWorkflowStateInState(previous, {
        ...workflowState,
        status: result.status || workflowState.status,
        pending_messages: Number(
          result.pending_messages ?? workflowState.pending_messages ?? 0,
        ),
        active_message_index: result.active_message_index ?? null,
        state_revision: nextStateRevision,
      });
      return finalizeSettledStreamInState(next, result);
    }
    return finalizeSettledStreamInState(previous, result);
  }

  const stateWithSettledPending = {
    ...previous,
    localPending: previous.localPending.filter((pending) => {
      const index = Number(pending.transcriptIndex);
      return !Number.isFinite(index) || !settledIndexes.has(index);
    }),
  };

  const next = updateWorkflowStateInState(stateWithSettledPending, {
    ...workflowState,
    status: result.status || workflowState.status,
    pending_messages: Number(result.pending_messages ?? workflowState.pending_messages ?? 0),
    active_message_index: result.active_message_index ?? null,
    transcript,
    transcript_offset: transcriptOffset,
    transcript_total: transcriptTotal,
    transcript_length: transcriptTotal,
    transcript_revision: toRevision,
    state_revision: nextStateRevision,
  });
  return finalizeSettledStreamInState(next, result);
}

function finalizeSettledStreamInState(state, result) {
  if (!result?.settled || transcriptDeltaResultStillLive(result)) return state;
  return markStreamCommittedInState(state, {
    assistantIndex:
      latestAssistantDeltaIndex(result) ?? latestAssistantTranscriptIndex(state.workflowState),
  });
}

function transcriptDeltaResultStillLive(result) {
  if (!result) return true;
  if (result.status === "responding" || result.status === "starting") return true;
  if (Number(result.pending_messages || 0) > 0) return true;
  return result.active_message_index !== null && result.active_message_index !== undefined;
}

function latestAssistantDeltaIndex(result) {
  let assistantIndex = null;
  for (const delta of result?.deltas || []) {
    if (delta?.message?.role !== "assistant") continue;
    const index = Number(delta.index);
    if (Number.isFinite(index)) assistantIndex = index;
  }
  return assistantIndex;
}

function latestAssistantTranscriptIndex(workflowState) {
  const indexes = assistantTranscriptIndexes(workflowState);
  return indexes.length ? indexes[indexes.length - 1] : null;
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

function assistantTranscriptIndexes(workflowState) {
  if (!workflowState) return [];
  const transcript = workflowState.transcript || [];
  const offset = Number(workflowState.transcript_offset || 0);
  const indexes = [];
  transcript.forEach((message, index) => {
    if (message?.role === "assistant") indexes.push(offset + index);
  });
  return indexes;
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
  const base = artifactResult.handled ? artifactResult.state : previous;
  const next = {
    ...base,
    streamTurn: cloneStreamTurn(base.streamTurn),
  };
  const agent = streamAgentForEvent(event);
  const sequence = agentPayloadSequence(event);
  const attempt = agentPayloadAttempt(event);

  if (event.kind === AgentStreamEventKind.AGENT_START) {
    if (agent.kind === "main") {
      next.currentAgentSequence = sequence;
      next.ignoreAgentUntilStart = false;
    }
    if (next.workflowState) {
      next.workflowState = { ...next.workflowState, status: "responding" };
    }
    const turn = ensureStreamTurn(next, agent, sequence);
    completeOpenToolSegments(turn, agent.id);
    const agentSegment = ensureAgentSegment(turn, agent, sequence, attempt);
    recordAgentAttempt(agentSegment, attempt);
    agentSegment.streamAttempt = Number(event.payload?.stream_attempt || 1);
    agentSegment.activityAttempt = Number(event.payload?.activity_attempt || 1);
    agentSegment.status =
      agentSegment.streamAttempt > 1 || agentSegment.activityAttempt > 1
        ? "retrying"
        : "streaming";
    agentSegment.provider = event.payload?.provider || null;
    agentSegment.model = event.payload?.model || null;
    if (agent.kind === "main") turn.status = "streaming";
  } else if (event.kind === AgentStreamEventKind.AGENT_TEXT_DELTA && event.payload?.text) {
    if (!shouldApplyAgentStreamEvent(next, agent, sequence)) return previous;
    const turn = ensureStreamTurn(next, agent, sequence);
    const activeSequence = activeAgentSequence(next, turn, agent, sequence);
    if (agent.kind === "main") adoptAgentSequence(next, activeSequence);
    const agentSegment = ensureAgentSegment(turn, agent, activeSequence, attempt);
    recordAgentAttempt(agentSegment, attempt);
    agentSegment.status = "streaming";
    agentSegment.text += event.payload.text;
    if (agent.kind === "main") next.streamTurn.status = "streaming";
  } else if (event.kind === AgentStreamEventKind.AGENT_THINKING_START) {
    if (!shouldApplyAgentStreamEvent(next, agent, sequence)) return previous;
    const turn = ensureStreamTurn(next, agent, sequence);
    const activeSequence = activeAgentSequence(next, turn, agent, sequence);
    if (agent.kind === "main") adoptAgentSequence(next, activeSequence);
    const agentSegment = ensureAgentSegment(turn, agent, activeSequence, attempt);
    recordAgentAttempt(agentSegment, attempt);
    agentSegment.status = "streaming";
    if (agent.kind === "main") next.streamTurn.status = "streaming";
  } else if (
    event.kind === AgentStreamEventKind.AGENT_THINKING_DELTA &&
    event.payload?.thinking
  ) {
    if (!shouldApplyAgentStreamEvent(next, agent, sequence)) return previous;
    const turn = ensureStreamTurn(next, agent, sequence);
    const activeSequence = activeAgentSequence(next, turn, agent, sequence);
    if (agent.kind === "main") adoptAgentSequence(next, activeSequence);
    const agentSegment = ensureAgentSegment(turn, agent, activeSequence, attempt);
    recordAgentAttempt(agentSegment, attempt);
    agentSegment.status = "streaming";
    agentSegment.thinking += event.payload.thinking;
    if (agent.kind === "main") next.streamTurn.status = "streaming";
  } else if (event.kind === AgentStreamEventKind.AGENT_CANCELLED) {
    if (agent.kind === "main" && shouldApplyAgentStreamEvent(next, agent, sequence)) {
      return {
        ...markStreamInterruptedInState(next),
        ignoreAgentUntilStart: true,
      };
    }
    if (agent.kind !== "main") {
      const turn = ensureStreamTurn(next, agent, sequence);
      const activeSequence = activeAgentSequence(next, turn, agent, sequence);
      const agentSegment = ensureAgentSegment(turn, agent, activeSequence, attempt);
      agentSegment.status = "interrupted";
      agentSegment.completedAt = new Date().toISOString();
      completeOpenToolSegments(turn, agent.id);
    }
  } else if (event.kind === AgentStreamEventKind.AGENT_FAILED) {
    if (!shouldApplyAgentStreamEvent(next, agent, sequence)) return previous;
    const turn = ensureStreamTurn(next, agent, sequence);
    const activeSequence = activeAgentSequence(next, turn, agent, sequence);
    if (agent.kind === "main") adoptAgentSequence(next, activeSequence);
    const agentSegment = ensureAgentSegment(turn, agent, activeSequence, attempt);
    recordAgentAttempt(agentSegment, attempt);
    agentSegment.status = "failed";
    agentSegment.error = event.payload?.error || null;
    agentSegment.provider = event.payload?.provider || agentSegment.provider || null;
    agentSegment.model = event.payload?.model || agentSegment.model || null;
    agentSegment.completedAt = event.emitted_at || new Date().toISOString();
    if (agent.kind === "main") turn.status = "retrying";
  } else if (event.kind === AgentStreamEventKind.AGENT_COMPLETE) {
    if (!shouldApplyAgentStreamEvent(next, agent, sequence)) return previous;
    const turn = ensureStreamTurn(next, agent, sequence);
    const activeSequence = activeAgentSequence(next, turn, agent, sequence);
    if (agent.kind === "main") adoptAgentSequence(next, activeSequence);
    const terminal = isTerminalAgentStop(event.payload || {});
    const agentSegment = ensureAgentSegment(turn, agent, activeSequence, attempt);
    recordAgentAttempt(agentSegment, attempt);
    finishAgentSegment(agentSegment, event.payload || {});
    if (terminal && agent.kind === "main") {
      completeOpenToolSegments(turn);
      turn.status = "complete";
      turn.completedAt = new Date().toISOString();
    } else if (terminal) {
      completeOpenToolSegments(turn, agent.id);
    } else {
      ensureToolSegment(turn, agent, activeSequence);
    }
    if (agent.kind === "main" && !terminal) turn.status = "tooling";
    turn.lastAgentCompletedAt = new Date().toISOString();
  } else if (isAgentToolInputEvent(event)) {
    if (!shouldApplyAgentStreamEvent(next, agent, sequence)) return previous;
    const turn = ensureStreamTurn(next, agent, sequence);
    const activeSequence = activeAgentSequence(next, turn, agent, sequence);
    if (agent.kind === "main") adoptAgentSequence(next, activeSequence);
    appendStreamToolEvent(turn, event, agent, activeSequence);
    if (
      agent.kind === "main" &&
      turn.status !== "complete" &&
      turn.status !== "interrupted"
    ) {
      turn.status = "tooling";
    }
  } else if (!event.kind?.startsWith(AGENT_STREAM_EVENT_PREFIX)) {
    const existingTurn = next.streamTurn;
    const turn =
      existingTurn &&
      (
        isOpenStreamTurn(existingTurn) ||
        sequence === null ||
        sequence === undefined ||
        existingTurn.sequences.includes(sequence)
      )
        ? existingTurn
        : ensureStreamTurn(next, agent, sequence);
    registerStreamSequence(turn, sequence);
    registerAgentSequence(turn, agent, sequence);
    const activeSequence = activeAgentSequence(next, turn, agent, null);
    appendStreamToolEvent(turn, event, agent, activeSequence);
    if (
      agent.kind === "main" &&
      turn.status !== "complete" &&
      turn.status !== "interrupted"
    ) {
      turn.status = "tooling";
    }
  }
  return next;
}

function agentPayloadSequence(event) {
  const sequence = event.payload?.sequence ?? event.payload?.llm_sequence;
  return sequence === undefined ? null : sequence;
}

function agentPayloadAttempt(event) {
  const attempt = Number(event.payload?.attempt);
  return Number.isFinite(attempt) ? attempt : null;
}

function streamAgentForEvent(event) {
  const value = event.agent && typeof event.agent === "object" ? event.agent : {};
  const kind = value.kind === "subagent" ? "subagent" : "main";
  return {
    id: String(value.id || (kind === "main" ? "main" : "subagent:unknown")),
    parentId: value.parent_id ? String(value.parent_id) : null,
    parentToolCallId: value.parent_tool_call_id
      ? String(value.parent_tool_call_id)
      : null,
    kind,
    label: String(value.label || (kind === "main" ? "Main agent" : "Subagent")),
  };
}

function activeAgentSequence(state, turn, agent, eventSequence) {
  if (eventSequence !== null && eventSequence !== undefined) return eventSequence;
  const agentSequence = turn.agentSequences?.[agent.id];
  if (agentSequence !== null && agentSequence !== undefined) return agentSequence;
  if (agent.kind === "main") {
    return state.currentAgentSequence ?? turn.activeSequence ?? null;
  }
  return null;
}

function adoptAgentSequence(state, sequence) {
  if (state.currentAgentSequence !== null && state.currentAgentSequence !== undefined) return;
  if (sequence === null || sequence === undefined) return;
  state.currentAgentSequence = sequence;
}

function shouldApplyAgentStreamEvent(state, agent, eventSequence) {
  if (agent.kind !== "main") return true;
  if (state.ignoreAgentUntilStart) return false;
  if (state.currentAgentSequence === null || state.currentAgentSequence === undefined) return true;
  if (eventSequence === null || eventSequence === undefined) return true;
  return eventSequence === state.currentAgentSequence;
}

export function streamEventNeedsSettledTranscriptDelta(event) {
  return (
    event.kind === AgentStreamEventKind.AGENT_COMPLETE &&
    streamAgentForEvent(event).kind === "main" &&
    isTerminalAgentStop(event.payload || {})
  );
}

export function streamEventNeedsWorkflowStateRefresh(event) {
  if (
    event.payload?.guard_name === "mutating_tool_approval" &&
    (
      event.kind === "harness_tool_guard_start" ||
      event.kind === "harness_tool_guard_complete" ||
      event.kind === "harness_tool_guard_failed"
    )
  ) {
    return true;
  }
  return (
    event.kind === AgentStreamEventKind.AGENT_COMPLETE &&
    streamAgentForEvent(event).kind === "main" &&
    event.payload?.stop_reason === "tool_use"
  );
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

function ensureStreamTurn(state, agent, sequence) {
  if (!isOpenStreamTurn(state.streamTurn)) {
    state.streamTurn = createStreamTurn(agent, sequence);
  } else {
    registerStreamSequence(state.streamTurn, sequence);
    registerAgentSequence(state.streamTurn, agent, sequence);
  }
  return state.streamTurn;
}

function isOpenStreamTurn(turn) {
  return Boolean(turn && turn.status !== "complete" && turn.status !== "interrupted");
}

function registerStreamSequence(turn, sequence) {
  if (sequence !== null && !turn.sequences.includes(sequence)) {
    turn.sequences.push(sequence);
  }
}

function registerAgentSequence(turn, agent, sequence) {
  if (!turn.agentSequences) turn.agentSequences = {};
  if (sequence !== null && sequence !== undefined) {
    turn.agentSequences[agent.id] = sequence;
  } else if (!Object.prototype.hasOwnProperty.call(turn.agentSequences, agent.id)) {
    turn.agentSequences[agent.id] = null;
  }
  if (agent.kind === "main" && sequence !== null && sequence !== undefined) {
    turn.activeSequence = sequence;
  }
}

function createStreamTurn(agent, sequence) {
  return {
    sequence,
    sequences: sequence === null ? [] : [sequence],
    agentSequences: {
      [agent.id]: sequence,
    },
    activeSequence: agent.kind === "main" ? sequence : null,
    status: "streaming",
    segments: [],
    startedAt: new Date().toISOString(),
    completedAt: null,
    lastAgentCompletedAt: null,
    interrupted: false,
  };
}

function cloneStreamTurn(turn) {
  if (!turn) return null;
  return {
    ...turn,
    sequences: [...turn.sequences],
    agentSequences: { ...(turn.agentSequences || {}) },
    segments: (turn.segments || []).map((segment) => ({
      ...segment,
      events: segment.events ? [...segment.events] : undefined,
    })),
  };
}

function ensureAgentSegment(turn, agent, sequence, attempt = null) {
  const normalizedSequence =
    sequence ?? turn.agentSequences?.[agent.id] ?? (agent.kind === "main" ? turn.activeSequence : null);
  registerStreamSequence(turn, normalizedSequence);
  registerAgentSequence(turn, agent, normalizedSequence);
  let segment = agentSegmentForSequence(
    turn,
    agent.id,
    normalizedSequence,
    attempt,
  );
  if (!segment) {
    segment = {
      id: `agent:${agent.id}:${normalizedSequence ?? "unknown"}:${turn.segments.length}`,
      type: "agent",
      agentId: agent.id,
      parentAgentId: agent.parentId,
      parentToolCallId: agent.parentToolCallId,
      agentKind: agent.kind,
      agentLabel: agent.label,
      sequence: normalizedSequence,
      status: "streaming",
      text: "",
      thinking: "",
      stopReason: null,
      usage: null,
      attempt,
      startedAt: new Date().toISOString(),
      completedAt: null,
    };
    turn.segments.push(segment);
  }
  return segment;
}

function agentSegmentForSequence(turn, agentId, sequence, attempt = null) {
  return turn.segments.find(
    (candidate) =>
      candidate.type === "agent" &&
      (candidate.agentId || "main") === agentId &&
      candidate.sequence === sequence &&
      (
        attempt === null ||
        attempt === undefined ||
        candidate.attempt === attempt
      ),
  );
}

function recordAgentAttempt(segment, attempt) {
  if (attempt === null || attempt === undefined) return;
  segment.attempt = attempt;
}

function ensureToolSegment(turn, agent, sequence) {
  const normalizedSequence =
    sequence ?? turn.agentSequences?.[agent.id] ?? (agent.kind === "main" ? turn.activeSequence : null);
  registerStreamSequence(turn, normalizedSequence);
  registerAgentSequence(turn, agent, normalizedSequence);
  const existing = latestToolSegmentForSequence(turn, agent.id, normalizedSequence);
  if (existing && existing.status !== "complete") return existing;

  const segment = {
    id: `tools:${agent.id}:${normalizedSequence ?? "unknown"}:${turn.segments.length}`,
    type: "tools",
    agentId: agent.id,
    parentAgentId: agent.parentId,
    agentKind: agent.kind,
    agentLabel: agent.label,
    afterSequence: normalizedSequence,
    status: "streaming",
    events: [],
    startedAt: new Date().toISOString(),
    completedAt: null,
  };
  turn.segments.push(segment);
  return segment;
}

function latestToolSegmentForSequence(turn, agentId, sequence) {
  for (let index = turn.segments.length - 1; index >= 0; index -= 1) {
    const segment = turn.segments[index];
    if (segment.type !== "tools") continue;
    if ((segment.agentId || "main") !== agentId) continue;
    if (sequence === null || sequence === undefined || segment.afterSequence === sequence) {
      return segment;
    }
  }
  return null;
}

function completeOpenToolSegments(turn, agentId = null) {
  for (const segment of turn.segments) {
    if (segment.type !== "tools" || segment.status === "complete") continue;
    if (agentId !== null && (segment.agentId || "main") !== agentId) continue;
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
  segment.terminal = isTerminalAgentStop(payload);
  segment.stopDetails = payload.stop_details || null;
  segment.usage = payload.usage || null;
  segment.provider = payload.provider || segment.provider || null;
  segment.model = payload.model || segment.model || null;
  segment.completedAt = new Date().toISOString();
}

function appendStreamToolEvent(turn, event, agent, sequence) {
  const toolSegment = ensureToolSegment(turn, agent, sequence);
  toolSegment.events = mergeStreamToolEvent(toolSegment.events || [], event);
}

function isAgentToolInputEvent(event) {
  return event.kind?.startsWith(AGENT_TOOL_INPUT_EVENT_PREFIX);
}

function mergeStreamToolEvent(events, event) {
  if (isPythonSandboxOutputEvent(event)) {
    return mergePythonSandboxOutputEvent(events, event).slice(-200);
  }

  if (isPythonSandboxProgressEvent(event)) {
    return mergePythonSandboxProgressEvent(events, event).slice(-200);
  }

  if (!event.kind?.startsWith(AGENT_TOOL_INPUT_EVENT_PREFIX)) {
    return [...events, event].slice(-200);
  }

  const key = streamToolInputKey(event);
  const nextEvents = [...events];
  const existingIndex = nextEvents.findIndex(
    (candidate) =>
      candidate.kind?.startsWith(AGENT_TOOL_INPUT_EVENT_PREFIX) &&
      streamToolInputKey(candidate) === key,
  );
  const existing = existingIndex >= 0 ? nextEvents[existingIndex] : null;
  const merged = mergeToolInputEvent(existing, event, key);
  if (existingIndex >= 0) {
    nextEvents[existingIndex] = merged;
  } else {
    nextEvents.push(merged);
  }
  return nextEvents.slice(-200);
}

function isPythonSandboxOutputEvent(event) {
  return event.kind === "python_sandbox_stdout" ||
    event.kind === "python_sandbox_stderr" ||
    event.kind === "workspace_shell_stdout" ||
    event.kind === "workspace_shell_stderr";
}

function isPythonSandboxProgressEvent(event) {
  return event.kind === "python_sandbox_progress" || event.kind === "workspace_shell_progress";
}

function isPythonSandboxEvent(event) {
  return event.kind?.startsWith("python_sandbox_") ||
    event.kind?.startsWith("workspace_shell_");
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

  if (event.kind === AgentStreamEventKind.AGENT_TOOL_INPUT_DELTA) {
    nextPayload.input_partial = existingPartial + String(payload.partial_json || "");
    nextPayload.status = "streaming input";
  } else if (event.kind === AgentStreamEventKind.AGENT_TOOL_INPUT_COMPLETE) {
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

function markStreamCommittedInState(state, options = {}) {
  const trace = turnTraceFromStreamTurn(
    state.streamTurn,
    state.workflowState,
    options.assistantIndex,
  );
  const turnTraces = trace
    ? {
        ...(state.turnTraces || {}),
        [trace.transcriptIndex]: trace,
      }
    : state.turnTraces;
  return {
    ...state,
    turnTraces,
    streamTurn: null,
    currentAgentSequence: null,
    ignoreAgentUntilStart: false,
  };
}

function turnTraceFromStreamTurn(turn, workflowState, assistantIndex) {
  if (!turn || assistantIndex === undefined || assistantIndex === null) return null;
  const normalizedIndex = Number(assistantIndex);
  if (!Number.isFinite(normalizedIndex)) return null;
  const traceTurn = cloneStreamTurn(turn);
  if (!traceTurn || !(traceTurn.segments || []).length) return null;
  return {
    status: "ready",
    source: "live",
    transcriptIndex: normalizedIndex,
    capturedAt: new Date().toISOString(),
    turn: trimTraceTurn(traceTurn),
  };
}

function turnTracesFromStreamEvents(events, workflowState) {
  const assistantIndexes = assistantTranscriptIndexes(workflowState);
  if (!assistantIndexes.length || !Array.isArray(events) || !events.length) {
    return {};
  }

  const { completedTurns } = replayStreamTurns(events, workflowState);

  const traces = {};
  const start = Math.max(0, completedTurns.length - assistantIndexes.length);
  const visibleTurns = completedTurns.slice(start);
  visibleTurns.forEach((turn, index) => {
    const transcriptIndex = assistantIndexes[index];
    if (transcriptIndex === undefined || !turn) return;
    traces[transcriptIndex] = {
      status: "ready",
      source: "replay",
      transcriptIndex,
      capturedAt: new Date().toISOString(),
      turn,
    };
  });
  return traces;
}

function replayStreamTurns(events, workflowState) {
  let replayState = {
    workflowState: workflowState || {
      transcript: [],
      transcript_offset: 0,
      transcript_total: 0,
      pending_approvals: [],
      queued_message_indices: [],
      artifacts: [],
    },
    workflowStateProjectionRevision: 0,
    workflowTranscriptProjectionRevision: 0,
    localPending: [],
    resolvingApprovals: new Set(),
    streamTurn: null,
    currentAgentSequence: null,
    ignoreAgentUntilStart: false,
    turnTraces: {},
  };
  const completedTurns = [];
  let pendingTerminalCompletion = false;

  for (const event of events) {
    if (isWorkflowProjectionEvent(event)) continue;
    if (
      pendingTerminalCompletion &&
      event.kind === AgentStreamEventKind.AGENT_START &&
      streamAgentForEvent(event).kind === "main" &&
      replayState.streamTurn
    ) {
      completedTurns.push(trimTraceTurn(cloneStreamTurn(replayState.streamTurn)));
      replayState = {
        ...replayState,
        streamTurn: null,
        currentAgentSequence: null,
        ignoreAgentUntilStart: false,
      };
      pendingTerminalCompletion = false;
    }

    replayState = handleStreamEventInState(replayState, event);
    if (
      event.kind === AgentStreamEventKind.AGENT_COMPLETE &&
      streamAgentForEvent(event).kind === "main" &&
      isTerminalAgentStop(event.payload || {})
    ) {
      pendingTerminalCompletion = true;
    }
    if (
      event.kind === "turn_settled" &&
      pendingTerminalCompletion &&
      replayState.streamTurn
    ) {
      completedTurns.push(trimTraceTurn(cloneStreamTurn(replayState.streamTurn)));
      replayState = {
        ...replayState,
        streamTurn: null,
        currentAgentSequence: null,
        ignoreAgentUntilStart: false,
      };
      pendingTerminalCompletion = false;
    }
  }
  if (pendingTerminalCompletion && replayState.streamTurn) {
    completedTurns.push(trimTraceTurn(cloneStreamTurn(replayState.streamTurn)));
  }
  return { replayState, completedTurns, pendingTerminalCompletion };
}

function isWorkflowProjectionEvent(event) {
  return (
    event.kind === "workflow_state" ||
    event.kind === "workflow_transcript" ||
    event.kind === "workflow_transcript_page" ||
    event.kind === "artifact_create_complete"
  );
}

function trimTraceTurn(turn) {
  if (!turn) return null;
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
        next.events = next.events.slice(-200).map(trimTraceEvent);
      }
      return next;
    }),
  };
}

function trimTraceEvent(event) {
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

function transcriptMessageForPending(pending) {
  const phase = String(pending.phase || "");
  if (phase.startsWith("failed")) return null;
  if (!String(pending.label || "").startsWith("you")) return null;
  return {
    role: "user",
    content: pending.content,
    attachments: [...(pending.attachments || [])],
  };
}

export function markStreamInterruptedInState(state) {
  return {
    ...state,
    streamTurn: null,
    currentAgentSequence: null,
  };
}

function hasLiveWorkflowActivity(state, workflowState = state.workflowState) {
  if (!workflowState) return true;
  if (workflowState.status === "responding") return true;
  if (Number(workflowState.pending_messages || 0) > 0) return true;
  return state.localPending.length > 0;
}

function isTerminalAgentStop(payload) {
  return payload.stop_reason && payload.stop_reason !== "tool_use";
}

function isAcknowledged(pending, workflowState) {
  return isPendingAcknowledgedByTranscript(pending, workflowState.transcript);
}

function isPendingAcknowledgedByTranscript(pending, transcript) {
  const pendingAttachmentIds = attachmentIds(pending.attachments || []);
  return transcript.some((message) => {
    if (
      message.role === "user" &&
      message.content === pending.content &&
      attachmentIdsEqual(pendingAttachmentIds, attachmentIds(message.attachments || []))
    ) {
      return true;
    }
    if (message.role === "system" && message.content.includes(pending.content)) return true;
    return false;
  });
}

function attachmentIds(attachments) {
  return (attachments || [])
    .map((attachment) => String(attachment.attachment_id || attachment.artifact_id || ""))
    .filter(Boolean)
    .sort();
}

function attachmentIdsEqual(left, right) {
  if (left.length !== right.length) return false;
  return left.every((value, index) => value === right[index]);
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
