import { useMemo } from "react";

import { AgentRuntime } from "./AgentRuntime.jsx";
import { AgentStreamEventKind } from "../state/streamEvents.js";

export function StreamPanel({
  turn,
  waiting = false,
  collapsed,
  onToggle,
  embedded = false,
  inputText = "",
  workflowId = "",
}) {
  const timeline = useMemo(() => normalizeStreamTimeline(turn), [turn]);
  const agents = useMemo(() => streamAgents(timeline), [timeline]);

  if ((!turn || !timeline.segments.length) && !waiting) return null;

  return (
    <section
      className={`runtime-stream-panel ${timeline.status}${collapsed ? " collapsed" : ""}${
        embedded ? " embedded" : ""
      }`}
    >
      <header className="runtime-panel-header">
        <div>
          <span className="runtime-panel-mark" />
          <strong>Agent runtime</strong>
          <small>{panelStatus(timeline, agents)}</small>
        </div>
        {onToggle ? (
          <button type="button" onClick={onToggle}>
            {collapsed ? "Open runtime" : "Collapse"}
          </button>
        ) : null}
      </header>
      {collapsed ? (
        <button type="button" className="runtime-collapsed-preview" onClick={onToggle}>
          <span className={`runtime-dot ${panelTone(timeline.status)}`} />
          <strong>{agents.find((agent) => agent.kind === "main")?.latestAction || "Agent activity"}</strong>
          <small>Open live runtime</small>
        </button>
      ) : (
        <AgentRuntime
          timeline={timeline}
          agents={agents}
          inputText={inputText}
          workflowId={workflowId}
        />
      )}
    </section>
  );
}

export function normalizeStreamTimeline(turn) {
  if (!turn) return { status: "waiting", segments: [] };
  if (turn.segments) {
    return {
      ...turn,
      segments: turn.segments.map((segment) => ({
        ...segment,
        agentId: segment.agentId || "main",
        parentAgentId: segment.parentAgentId || null,
        agentKind: segment.agentKind === "subagent" ? "subagent" : "main",
        agentLabel:
          segment.agentLabel ||
          (segment.agentKind === "subagent" ? "Subagent" : "Main agent"),
      })),
    };
  }

  const segments = [];
  for (const finishedTurn of turn.finishedTurns || []) {
    segments.push({
      id: `agent:main:${finishedTurn.sequence ?? "unknown"}:${segments.length}`,
      type: "agent",
      agentId: "main",
      parentAgentId: null,
      agentKind: "main",
      agentLabel: "Main agent",
      sequence: finishedTurn.sequence ?? null,
      status: "complete",
      terminal: finishedTurn.stopReason !== "tool_use",
      text: finishedTurn.text || "",
      thinking: finishedTurn.thinking || "",
      stopReason: finishedTurn.stopReason || "unknown",
      stopDetails: finishedTurn.stopDetails || null,
      usage: finishedTurn.usage || null,
      completedAt: finishedTurn.completedAt || null,
    });
    if (finishedTurn.events?.length) {
      segments.push({
        id: `tools:main:${finishedTurn.sequence ?? "unknown"}:${segments.length}`,
        type: "tools",
        agentId: "main",
        parentAgentId: null,
        agentKind: "main",
        agentLabel: "Main agent",
        afterSequence: finishedTurn.sequence ?? null,
        status: "complete",
        events: finishedTurn.events,
        completedAt: finishedTurn.completedAt || null,
      });
    }
  }
  if (turn.text || turn.thinking) {
    segments.push({
      id: `agent:main:${turn.activeSequence ?? "unknown"}:${segments.length}`,
      type: "agent",
      agentId: "main",
      parentAgentId: null,
      agentKind: "main",
      agentLabel: "Main agent",
      sequence: turn.activeSequence ?? null,
      status: turn.status === "complete" ? "complete" : "streaming",
      text: turn.text || "",
      thinking: turn.thinking || "",
      stopReason: null,
      usage: null,
      completedAt: null,
    });
  }
  if (turn.currentEvents?.length) {
    segments.push({
      id: `tools:main:${turn.activeSequence ?? "unknown"}:${segments.length}`,
      type: "tools",
      agentId: "main",
      parentAgentId: null,
      agentKind: "main",
      agentLabel: "Main agent",
      afterSequence: turn.activeSequence ?? null,
      status: turn.status === "complete" ? "complete" : "streaming",
      events: turn.currentEvents,
      completedAt: null,
    });
  }
  return { ...turn, segments };
}

export function streamAgents(timeline) {
  const agents = new Map();
  for (const segment of timeline.segments || []) {
    const kind = segment.agentKind === "subagent" ? "subagent" : "main";
    const id = String(segment.agentId || (kind === "main" ? "main" : "subagent:unknown"));
    if (!agents.has(id)) {
      agents.set(id, {
        id,
        parentId: segment.parentAgentId || null,
        parentToolCallId: segment.parentToolCallId || null,
        kind,
        label: String(segment.agentLabel || (kind === "main" ? "Main agent" : "Subagent")),
        segments: [],
        events: [],
        startedAt: segment.startedAt || null,
        completedAt: null,
      });
    }
    const agent = agents.get(id);
    agent.segments.push(segment);
    agent.events.push(...(segment.events || []));
    if (!agent.startedAt && segment.startedAt) agent.startedAt = segment.startedAt;
    if (segment.completedAt) agent.completedAt = segment.completedAt;
  }

  const startedToolCalls = new Set(
    [...agents.values()]
      .filter((agent) => agent.kind === "subagent" && agent.parentToolCallId)
      .map((agent) => agent.parentToolCallId),
  );
  const startedTasks = new Set(
    [...agents.values()]
      .filter(
        (agent) => agent.kind === "subagent" && !agent.parentToolCallId,
      )
      .map((agent) => `${agent.parentId || ""}:${agent.label}`),
  );
  for (const segment of timeline.segments || []) {
    if (segment.type !== "tools") continue;
    for (const event of segment.events || []) {
      if (event.kind !== AgentStreamEventKind.AGENT_TOOL_INPUT_COMPLETE) continue;
      if (event.payload?.tool_name !== "create_subagent") continue;
      const task = String(event.payload?.input?.task || "").trim();
      const parentId = String(segment.agentId || "main");
      const parentToolCallId = String(
        event.payload?.tool_use_id || event.tool_call_id || "",
      );
      const taskKey = `${parentId}:${task}`;
      if (
        !task ||
        (parentToolCallId
          ? startedToolCalls.has(parentToolCallId)
          : startedTasks.has(taskKey))
      ) continue;
      const id = `queued:${parentToolCallId || agents.size}`;
      agents.set(id, {
        id,
        parentId,
        parentToolCallId: parentToolCallId || null,
        kind: "subagent",
        label: task,
        segments: [],
        events: [event],
        startedAt: segment.startedAt || null,
        completedAt: null,
        queued: true,
      });
      if (parentToolCallId) startedToolCalls.add(parentToolCallId);
      else startedTasks.add(taskKey);
    }
  }

  return [...agents.values()]
    .sort((left, right) => {
      if (left.kind === right.kind) return 0;
      return left.kind === "main" ? -1 : 1;
    })
    .map((agent) => {
    const agentSegments = agent.segments.filter((segment) => segment.type === "agent");
    const latestAgentSegment = agentSegments[agentSegments.length - 1] || null;
    const hasLiveSegment = agent.segments.some((segment) =>
      ["streaming", "retrying"].includes(segment.status),
    );
    let status = "waiting";
    if (agent.queued) status = "queued";
    else if (
      ["interrupted", "failed"].includes(latestAgentSegment?.status) ||
      ["error", "failed", "cancelled", "refusal"].includes(
        String(latestAgentSegment?.stopReason || "").toLowerCase(),
      )
    ) status = "error";
    else if (latestAgentSegment?.terminal) status = "complete";
    else if (latestAgentSegment?.status === "retrying") status = "retrying";
    else if (hasLiveSegment || (agent.kind === "main" && timeline.status === "tooling")) status = "running";

    const latestSegment = agent.segments[agent.segments.length - 1] || null;
    const latestEvent = agent.events[agent.events.length - 1] || null;
    let latestAction = "Waiting for activity…";
    if (status === "queued") latestAction = "Queued for delegation";
    else if (status === "complete") latestAction = "Completed the response";
    else if (status === "error") latestAction = "Stopped with an error";
    else if (latestSegment?.type === "agent") {
      const thinking = compactText(latestSegment.thinking, 120);
      const text = compactText(latestSegment.text, 120);
      latestAction = thinking ? `Reasoning · ${thinking}` : text ? `Drafting · ${text}` : "Model active";
    } else if (latestEvent) {
      latestAction = eventSummary(latestEvent);
    }

      return {
        ...agent,
        status,
        latestAction,
        turnCount: agentSegments.length,
        toolCount: new Set(agent.events.map((event, index) =>
          event.tool_call_id || event.payload?.tool_use_id || event.payload?.operation_id || `${event.kind}:${index}`,
        )).size,
      };
    });
}

function eventSummary(event) {
  const payload = event.payload || {};
  const name = String(payload.tool_name || event.tool_name || "tool");
  const input = payload.input || {};
  const value = input.query || input.url || input.task || input.path || payload.query || payload.path;
  return value ? `${name} · ${compactText(value, 110)}` : `${name} · ${humanize(event.kind)}`;
}

function panelStatus(timeline, agents) {
  if (!(timeline.segments || []).length) return "dispatching";
  const subagents = agents.filter((agent) => agent.kind === "subagent").length;
  const events = (timeline.segments || []).reduce(
    (count, segment) => count + (segment.events?.length || 0),
    0,
  );
  const state = timeline.status === "complete" ? "complete" : timeline.status === "interrupted" ? "stopped" : "live";
  return `${state} · ${subagents} subagent${subagents === 1 ? "" : "s"} · ${events} events`;
}

function panelTone(status) {
  if (status === "complete") return "complete";
  if (status === "interrupted") return "error";
  return "running";
}

function humanize(value) {
  return String(value || "activity").replace(/^agent_/, "").replace(/_/g, " ");
}

function compactText(value, limit) {
  const text = String(value || "").replace(/\s+/g, " ").trim();
  if (text.length <= limit) return text;
  return `${text.slice(0, limit - 1)}…`;
}
