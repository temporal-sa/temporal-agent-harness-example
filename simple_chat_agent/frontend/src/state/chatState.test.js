import assert from "node:assert/strict";
import test from "node:test";

import {
  applyReplayedStreamEventsInState,
  handleStreamEventInState,
  restoreActiveStreamTurnInState,
  streamEventNeedsSettledTranscriptDelta,
  streamEventNeedsWorkflowStateRefresh,
} from "./chatState.js";

function initialState() {
  return {
    workflowState: {
      status: "responding",
      transcript: [],
      transcript_offset: 0,
      transcript_total: 0,
      pending_approvals: [],
      queued_message_indices: [],
      artifacts: [],
    },
    workflowStateProjectionRevision: 0,
    workflowTranscriptProjectionRevision: 0,
    streamTurn: null,
    currentAgentSequence: null,
    ignoreAgentUntilStart: false,
    localPending: [],
    resolvingApprovals: new Set(),
    turnTraces: {},
  };
}

function event(kind, agent, payload = {}, extra = {}) {
  return {
    stream_id: "chat-1",
    tool_name: "agent",
    step: null,
    kind,
    payload,
    sequence: 1,
    agent,
    tool_call_id: null,
    ...extra,
  };
}

test("reconnect replay extends the existing live turn instead of rebuilding it", () => {
  const main = { id: "chat-1", parent_id: null, kind: "main", label: "Main agent" };
  let state = handleStreamEventInState(
    initialState(),
    event(AgentStart, main, { sequence: 1, provider: "claude" }),
  );
  state = handleStreamEventInState(
    state,
    event(AgentTextDelta, main, { sequence: 1, text: "Hello" }),
  );

  const replayed = applyReplayedStreamEventsInState(state, [
    event(AgentTextDelta, main, { sequence: 1, text: " again" }),
  ]);
  const agentSegment = replayed.streamTurn.segments.find(
    (segment) => segment.type === "agent" && segment.agentId === "chat-1",
  );

  assert.equal(agentSegment.text, "Hello again");
  assert.equal(replayed.currentAgentSequence, 1);
});

test("concurrent subagents with colliding turn numbers retain separate traces", () => {
  const main = { id: "chat-1", parent_id: null, kind: "main", label: "Main agent" };
  const sharks = {
    id: "chat-1-subagent-sharks",
    parent_id: "chat-1",
    kind: "subagent",
    label: "Research shark population changes",
  };
  const policy = {
    id: "chat-1-subagent-policy",
    parent_id: "chat-1",
    kind: "subagent",
    label: "Research shark conservation policy",
  };

  let state = initialState();
  state = handleStreamEventInState(
    state,
    event(AgentStart, main, { sequence: 1, provider: "claude" }),
  );
  state = handleStreamEventInState(
    state,
    event(AgentComplete, main, {
      sequence: 1,
      provider: "claude",
      stop_reason: "tool_use",
      text: "I will delegate this research.",
    }),
  );
  state = handleStreamEventInState(
    state,
    event(AgentStart, sharks, { sequence: 1, provider: "claude" }),
  );
  state = handleStreamEventInState(
    state,
    event(AgentStart, policy, { sequence: 1, provider: "claude" }),
  );
  state = handleStreamEventInState(
    state,
    event(AgentTextDelta, sharks, {
      sequence: 1,
      provider: "claude",
      text: "Population findings",
    }),
  );
  state = handleStreamEventInState(
    state,
    event(AgentTextDelta, policy, {
      sequence: 1,
      provider: "claude",
      text: "Policy findings",
    }),
  );
  state = handleStreamEventInState(
    state,
    event(AgentComplete, sharks, {
      sequence: 1,
      provider: "claude",
      stop_reason: "end_turn",
      text: "Population findings complete.",
    }),
  );

  assert.notEqual(state.streamTurn.status, "complete");
  const childSegments = state.streamTurn.segments.filter(
    (segment) => segment.type === "agent" && segment.agentKind === "subagent",
  );
  assert.equal(childSegments.length, 2);
  assert.equal(
    childSegments.find((segment) => segment.agentId === sharks.id).text,
    "Population findings complete.",
  );
  assert.equal(
    childSegments.find((segment) => segment.agentId === policy.id).text,
    "Policy findings",
  );

  state = handleStreamEventInState(
    state,
    event(AgentStart, main, { sequence: 2, provider: "claude" }),
  );
  state = handleStreamEventInState(
    state,
    event(AgentTextDelta, main, {
      sequence: 2,
      provider: "claude",
      text: "Synthesizing delegated findings",
    }),
  );

  const resumedMain = state.streamTurn.segments.find(
    (segment) =>
      segment.type === "agent" && segment.agentKind === "main" && segment.sequence === 2,
  );
  assert.equal(resumedMain.text, "Synthesizing delegated findings");
});

test("refresh replay restores the main agent and every in-flight subagent", () => {
  const main = { id: "chat-1", parent_id: null, kind: "main", label: "Main agent" };
  const children = ["sharks", "policy", "habitats"].map((topic) => ({
    id: `chat-1-subagent-${topic}`,
    parent_id: "chat-1",
    kind: "subagent",
    label: `Research ${topic}`,
  }));
  const events = [
    event(AgentStart, main, { sequence: 1, provider: "claude" }),
    event(AgentComplete, main, {
      sequence: 1,
      provider: "claude",
      stop_reason: "tool_use",
      text: "I will delegate this research.",
    }),
    ...children.map((child, index) =>
      event("agent_tool_input_complete", main, {
        sequence: 1,
        tool_use_id: `create-subagent-${index}`,
        tool_name: "create_subagent",
        input: { task: child.label },
      }),
    ),
    ...children.map((child) =>
      event(AgentStart, child, { sequence: 1, provider: "claude" }),
    ),
  ];

  const state = restoreActiveStreamTurnInState(initialState(), events);
  const agentSegments = state.streamTurn.segments.filter(
    (segment) => segment.type === "agent",
  );

  assert.equal(state.streamTurn.status, "tooling");
  assert.equal(state.currentAgentSequence, 1);
  assert.equal(
    agentSegments.find((segment) => segment.agentKind === "main").agentId,
    main.id,
  );
  assert.deepEqual(
    agentSegments
      .filter((segment) => segment.agentKind === "subagent")
      .map((segment) => segment.agentId),
    children.map((child) => child.id),
  );
});

test("only main-agent completions trigger workflow reconciliation boundaries", () => {
  const main = { id: "chat-1", parent_id: null, kind: "main", label: "Main agent" };
  const child = {
    id: "chat-1-subagent",
    parent_id: "chat-1",
    kind: "subagent",
    label: "Research one topic",
  };
  const mainToolUse = event(AgentComplete, main, { stop_reason: "tool_use" });
  const childToolUse = event(AgentComplete, child, { stop_reason: "tool_use" });
  const mainDone = event(AgentComplete, main, { stop_reason: "end_turn" });
  const childDone = event(AgentComplete, child, { stop_reason: "end_turn" });

  assert.equal(streamEventNeedsWorkflowStateRefresh(mainToolUse), true);
  assert.equal(streamEventNeedsWorkflowStateRefresh(childToolUse), false);
  assert.equal(streamEventNeedsSettledTranscriptDelta(mainDone), true);
  assert.equal(streamEventNeedsSettledTranscriptDelta(childDone), false);
});

test("approval guard lifecycle triggers workflow state reconciliation", () => {
  const child = {
    id: "chat-1-subagent",
    parent_id: "chat-1",
    kind: "subagent",
    label: "Research one topic",
  };
  const approvalGuardStart = event("harness_tool_guard_start", child, {
    guard_name: "mutating_tool_approval",
    tool_name: "python_sandbox",
    tool_type: "mutating",
    status: "running",
  });
  const approvalGuardComplete = event("harness_tool_guard_complete", child, {
    guard_name: "mutating_tool_approval",
    tool_name: "python_sandbox",
    tool_type: "mutating",
    status: "passed",
  });
  const unrelatedGuard = event("harness_tool_guard_start", child, {
    guard_name: "some_other_guard",
    tool_name: "python_sandbox",
    tool_type: "mutating",
    status: "running",
  });

  assert.equal(streamEventNeedsWorkflowStateRefresh(approvalGuardStart), true);
  assert.equal(streamEventNeedsWorkflowStateRefresh(approvalGuardComplete), true);
  assert.equal(streamEventNeedsWorkflowStateRefresh(unrelatedGuard), false);
});

test("failed model attempts remain visible when Temporal retries the call", () => {
  const main = { id: "chat-1", parent_id: null, kind: "main", label: "Main agent" };
  let state = initialState();

  state = handleStreamEventInState(
    state,
    event(AgentStart, main, {
      sequence: 1,
      provider: "claude",
      model: "claude-sonnet",
      stream_attempt: 1,
      activity_attempt: 1,
      attempt: 1001,
    }),
  );
  state = handleStreamEventInState(
    state,
    event(AgentFailed, main, {
      sequence: 1,
      provider: "claude",
      model: "claude-sonnet",
      stream_attempt: 1,
      activity_attempt: 1,
      attempt: 1001,
      error: { type: "APIConnectionError", message: "connection reset" },
    }),
  );
  state = handleStreamEventInState(
    state,
    event(AgentStart, main, {
      sequence: 1,
      provider: "claude",
      model: "claude-sonnet",
      stream_attempt: 1,
      activity_attempt: 2,
      attempt: 1002,
    }),
  );
  state = handleStreamEventInState(
    state,
    event(AgentComplete, main, {
      sequence: 1,
      provider: "claude",
      model: "claude-sonnet",
      stream_attempt: 1,
      activity_attempt: 2,
      attempt: 1002,
      stop_reason: "end_turn",
      text: "Recovered response",
    }),
  );

  const attempts = state.streamTurn.segments.filter(
    (segment) => segment.type === "agent" && segment.sequence === 1,
  );
  assert.equal(attempts.length, 2);
  assert.equal(attempts[0].status, "failed");
  assert.equal(attempts[0].error.message, "connection reset");
  assert.equal(attempts[1].status, "complete");
  assert.equal(attempts[1].activityAttempt, 2);
  assert.equal(attempts[1].text, "Recovered response");
});

test("post-LLM guard lifecycle stays attached after provider completion", () => {
  const main = { id: "chat-1", parent_id: null, kind: "main", label: "Main agent" };
  let state = initialState();
  state = handleStreamEventInState(
    state,
    event(AgentStart, main, { sequence: 1, attempt: 1001 }),
  );
  state = handleStreamEventInState(
    state,
    event(AgentComplete, main, {
      sequence: 1,
      attempt: 1001,
      stop_reason: "end_turn",
      text: "Done",
    }),
  );
  const completedTurn = state.streamTurn;
  state = handleStreamEventInState(
    state,
    event("harness_llm_guard_complete", main, {
      operation_id: "chat-1:llm:1:guard:post:0:good_place",
      parent_operation_id: "chat-1:llm:1",
      guard_name: "good_place",
      timing: "post",
      llm_sequence: 1,
      status: "passed",
    }),
  );

  assert.equal(state.streamTurn.startedAt, completedTurn.startedAt);
  assert.equal(
    state.streamTurn.segments
      .filter((segment) => segment.type === "tools")
      .flatMap((segment) => segment.events)
      .some((streamEvent) => streamEvent.kind === "harness_llm_guard_complete"),
    true,
  );
});

test("streamed tool input starts a new segment after pre-LLM guards close", () => {
  const main = { id: "chat-1", parent_id: null, kind: "main", label: "Main agent" };
  let state = initialState();
  state = handleStreamEventInState(
    state,
    event("harness_llm_guard_complete", main, {
      operation_id: "chat-1:llm:2:guard:pre:0:good_place",
      guard_name: "good_place",
      timing: "pre",
      llm_sequence: 2,
      status: "passed",
    }),
  );
  state = handleStreamEventInState(
    state,
    event(AgentStart, main, { sequence: 2, provider: "claude" }),
  );
  state = handleStreamEventInState(
    state,
    event(AgentTextDelta, main, {
      sequence: 2,
      text: "I will build the report.",
    }),
  );
  state = handleStreamEventInState(
    state,
    event("agent_tool_input_start", main, {
      sequence: 2,
      tool_use_id: "artifact-tool-1",
      tool_name: "create_artifact",
    }),
  );
  state = handleStreamEventInState(
    state,
    event("agent_tool_input_delta", main, {
      sequence: 2,
      tool_use_id: "artifact-tool-1",
      tool_name: "create_artifact",
      partial_json: '{"name":"report.html","content":"<html>',
    }),
  );

  const toolSegments = state.streamTurn.segments.filter(
    (segment) => segment.type === "tools" && segment.afterSequence === 2,
  );
  assert.equal(toolSegments.length, 2);
  assert.equal(toolSegments[0].status, "complete");
  assert.equal(toolSegments[1].status, "streaming");
  assert.equal(toolSegments[1].events[0].kind, "agent_tool_input_delta");
});

const AgentStart = "agent_start";
const AgentTextDelta = "agent_text_delta";
const AgentComplete = "agent_complete";
const AgentFailed = "agent_failed";
