import {
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from "react";

import {
  AGENT_TOOL_INPUT_EVENT_PREFIX,
  AgentStreamEventKind,
} from "../state/streamEvents.js";

export function AgentRuntime({
  timeline,
  agents,
  inputText = "",
  workflowId = "",
}) {
  const mainAgent = agents.find((agent) => agent.kind === "main") || agents[0] || null;
  const [activeAgentId, setActiveAgentId] = useState(mainAgent?.id || null);
  const activeAgent =
    agents.find((agent) => agent.id === activeAgentId) || mainAgent || agents[0] || null;
  const navigationRows = useMemo(
    () => buildAgentNavigationRows(agents, activeAgent?.id),
    [activeAgent?.id, agents],
  );
  const frames = useMemo(
    () => buildRuntimeFrames(timeline, activeAgent),
    [timeline, activeAgent],
  );
  const [cursor, setCursor] = useState(() => Math.max(0, frames.length - 1));
  const [following, setFollowing] = useState(true);
  const [playing, setPlaying] = useState(false);
  const [selectedCard, setSelectedCard] = useState(null);
  const [toolResultRequests, setToolResultRequests] = useState({});
  const toolResultPendingRef = useRef(new Set());
  const toolGridRef = useRef(null);
  const modelStreamRef = useRef(null);
  const modelStreamContentRef = useRef(null);
  const lastAgentIdRef = useRef(activeAgent?.id || null);
  const finalFrame = Math.max(0, frames.length - 1);
  const visibleCursor = following ? finalFrame : Math.min(cursor, finalFrame);
  const projectedModel = useMemo(
    () => projectRuntimeFrame(timeline, activeAgent, frames, visibleCursor),
    [timeline, activeAgent, frames, visibleCursor],
  );
  const model = useMemo(
    () => projectSubagentTools(projectedModel, agents, activeAgent, following),
    [activeAgent, agents, following, projectedModel],
  );
  const modelStream = modelStreamForProjection(model);
  const showModelStream = Boolean(modelStream.text);
  const selectedTool = selectedCard?.type === "tool"
    ? model.tools.find((tool) => tool.id === selectedCard.id) || null
    : null;
  const resultWorkflowId = activeAgent?.kind === "subagent"
    ? activeAgent.id
    : workflowId;
  const selectedResultKey = selectedTool && resultWorkflowId
    ? `${resultWorkflowId}:${selectedTool.id}`
    : null;
  const selectedToolResult = selectedResultKey
    ? toolResultRequests[selectedResultKey] || null
    : null;
  const layoutVersion = model.tools
    .map((tool) => `${tool.id}:${tool.status}:${guardVersion(tool.guards)}`)
    .join("|");

  useFluidToolLayout(toolGridRef, layoutVersion);

  useEffect(() => {
    if (lastAgentIdRef.current === activeAgent?.id) return;
    lastAgentIdRef.current = activeAgent?.id || null;
    setCursor(finalFrame);
    setFollowing(true);
    setPlaying(false);
    setSelectedCard(null);
  }, [activeAgent?.id, finalFrame]);

  useEffect(() => {
    if (!following) return;
    setCursor(finalFrame);
  }, [finalFrame, following]);

  useLiveStreamAutoscroll(
    modelStreamRef,
    modelStreamContentRef,
    following && showModelStream,
    `${activeAgent?.id || "agent"}:${modelStream.kind}`,
    modelStream.text,
  );

  useEffect(() => {
    if (!selectedTool || !selectedResultKey || !workflowId) return undefined;
    if (!["done", "failed"].includes(selectedTool.status)) return undefined;
    if (toolResultRequests[selectedResultKey] || toolResultPendingRef.current.has(selectedResultKey)) {
      return undefined;
    }

    toolResultPendingRef.current.add(selectedResultKey);
    setToolResultRequests((current) => ({
      ...current,
      [selectedResultKey]: { status: "loading" },
    }));
    const url =
      `/api/sessions/${encodeURIComponent(workflowId)}` +
      `/agents/${encodeURIComponent(resultWorkflowId)}` +
      `/tools/${encodeURIComponent(selectedTool.id)}/result`;
    fetch(url, {
      headers: { "Cache-Control": "no-cache" },
    })
      .then(async (response) => {
        if (response.status === 404) return { status: "missing" };
        if (!response.ok) {
          const message = await response.text();
          throw new Error(message || `Tool result request failed (${response.status})`);
        }
        return { status: "loaded", data: await response.json() };
      })
      .then((result) => {
        setToolResultRequests((current) => ({
          ...current,
          [selectedResultKey]: result,
        }));
      })
      .catch((error) => {
        setToolResultRequests((current) => ({
          ...current,
          [selectedResultKey]: { status: "error", error: error.message },
        }));
      })
      .finally(() => {
        toolResultPendingRef.current.delete(selectedResultKey);
      });

    return undefined;
  }, [
    resultWorkflowId,
    selectedResultKey,
    selectedTool?.status,
    workflowId,
  ]);

  useEffect(() => {
    if (!playing) return undefined;
    if (visibleCursor >= finalFrame) {
      setPlaying(false);
      return undefined;
    }
    const timer = window.setTimeout(() => {
      setCursor((current) => Math.min(finalFrame, current + 1));
    }, 620);
    return () => window.clearTimeout(timer);
  }, [finalFrame, playing, visibleCursor]);

  if (!activeAgent || !frames.length) {
    return <div className="runtime-empty">Waiting for agent activity…</div>;
  }

  const inspected = inspectedRuntimeCard(
    selectedCard,
    model,
    selectedToolResult,
  );
  const isLive = !["complete", "interrupted"].includes(timeline.status);
  const showIngress = activeAgent.kind === "main" && visibleCursor <= 1;

  function seek(nextCursor) {
    const next = Math.max(0, Math.min(finalFrame, Number(nextCursor)));
    setCursor(next);
    setFollowing(next === finalFrame);
    setPlaying(false);
  }

  function selectAgent(agentId) {
    setActiveAgentId(agentId);
    setCursor(Number.MAX_SAFE_INTEGER);
    setFollowing(true);
    setPlaying(false);
    setSelectedCard(null);
  }

  return (
    <div className="runtime-visualizer">
      {navigationRows.length ? (
        <div className="runtime-agent-navigator" aria-label="Subagent hierarchy">
          {navigationRows.map((row, depth) => (
            <div
              key={row.parent.id}
              className="runtime-agent-row"
              aria-label={`Delegation depth ${depth + 1}`}
            >
              <button
                type="button"
                className={`runtime-agent-parent${
                  row.parent.id === activeAgent.id ? " active" : ""
                }`}
                onClick={() => selectAgent(row.parent.id)}
                aria-current={row.parent.id === activeAgent.id ? "page" : undefined}
              >
                <span className={`runtime-dot ${runtimeStatus(row.parent.status)}`} />
                <span>
                  {row.parent.kind === "main"
                    ? "Main agent"
                    : compactText(row.parent.label, 42)}
                </span>
                <small>{runtimeStatus(row.parent.status)}</small>
              </button>
              <span className="runtime-agent-branch" aria-hidden="true">›</span>
              <div
                className="runtime-agent-switcher"
                aria-label={`Subagents of ${row.parent.label || "Main agent"}`}
              >
                {row.children.map((agent) => {
                  const active = agent.id === activeAgent.id;
                  const selected = agent.id === row.selectedChildId;
                  return (
                    <button
                      key={agent.id}
                      type="button"
                      className={`${selected ? "selected" : ""}${
                        active ? " active" : ""
                      }`}
                      onClick={() => selectAgent(agent.id)}
                      aria-current={active ? "page" : undefined}
                      aria-pressed={selected}
                    >
                      <span className={`runtime-dot ${runtimeStatus(agent.status)}`} />
                      <span>{compactText(agent.label, 42)}</span>
                      <small>{runtimeStatus(agent.status)}</small>
                    </button>
                  );
                })}
              </div>
            </div>
          ))}
        </div>
      ) : null}

      <div className={`runtime-stage${showIngress ? " has-ingress" : ""}`}>
        <div className="runtime-grid" aria-hidden="true" />
        <div className="runtime-stage-label">
          <span>Agent runtime</span>
          <i />
        </div>

        {showIngress ? (
          <button
            type="button"
            className="runtime-ingress-card"
            onClick={() => setSelectedCard({ type: "ingress", id: "ingress" })}
          >
            <strong>Message in</strong>
            <span><i /> Received</span>
            <p>{compactText(inputText || "Conversation turn accepted", 96)}</p>
          </button>
        ) : null}

        <section className={`runtime-agent-shell ${runtimeStatus(model.agentStatus)}`}>
          <header className="runtime-agent-header">
            <div className="runtime-agent-title">
              <span className="runtime-agent-glyph" />
              <strong>{activeAgent.kind === "main" ? "Agent" : "Subagent"}</strong>
              {activeAgent.kind === "subagent" ? (
                <small title={activeAgent.label}>{compactText(activeAgent.label, 48)}</small>
              ) : null}
            </div>
            <div className={`runtime-run-pill ${runtimeStatus(model.agentStatus)}`}>
              <i />
              <span>{runtimeStatusLabel(model.agentStatus)}</span>
              <b>·</b>
              <span>Turn {model.turn || "—"}</span>
            </div>
          </header>

          <div className="runtime-agent-body">
            <div className="runtime-signal-line" aria-hidden="true"><i /></div>
            <button
              type="button"
              className={`runtime-model-card ${model.modelStatus}`}
              onClick={() => setSelectedCard({ type: "model", id: "model" })}
              aria-label={`Model ${model.modelStatus}`}
            >
              <span className="runtime-model-icon"><i /></span>
              <strong>Model</strong>
              <small>{modelStatusLabel(model.modelStatus)}</small>
              <RuntimeGuardBundle guards={model.modelGuards} owner="model" />
            </button>

            <div ref={toolGridRef} className="runtime-tool-bay">
              {showModelStream ? (
                <div className={`runtime-model-stream ${modelStream.kind}`}>
                  <div className="runtime-model-stream-heading">
                    <span><i /> {modelStream.label}</span>
                    <small>{following ? "Following live" : "Replay"}</small>
                  </div>
                  <p ref={modelStreamRef} aria-live="polite" aria-atomic="false">
                    <span ref={modelStreamContentRef}>{modelStream.text}</span>
                  </p>
                </div>
              ) : null}
              {model.tools.length ? (
                model.tools.map((tool) => (
                  <button
                    key={tool.id}
                    type="button"
                    data-runtime-card={tool.id}
                    className={`runtime-tool-card ${tool.kind || "tool"} ${tool.status}${
                      selectedCard?.type === "tool" && selectedCard.id === tool.id
                        ? " selected"
                        : ""
                    }`}
                    onClick={() => setSelectedCard({ type: "tool", id: tool.id })}
                  >
                    <div className="runtime-tool-heading">
                      <strong title={tool.name}>{compactText(tool.name, 24)}</strong>
                      <small>#{shortId(tool.id)}</small>
                    </div>
                    <span className="runtime-tool-status"><i /> {toolStatusLabel(tool.status)}</span>
                    {tool.preview ? <p>{compactText(tool.preview, 88)}</p> : null}
                    <RuntimeGuardBundle guards={tool.guards} owner="tool" />
                  </button>
                ))
              ) : !showModelStream ? (
                <div className="runtime-tool-empty">
                  {model.modelStatus === "complete"
                    ? "Run complete"
                    : model.modelStatus === "failed"
                      ? "Run stopped"
                      : "Tools appear here when requested"}
                </div>
              ) : null}
            </div>
          </div>
        </section>
      </div>

      <div className="runtime-transport">
        <div className="runtime-transport-buttons">
          <button type="button" onClick={() => seek(visibleCursor - 1)} disabled={!visibleCursor} aria-label="Previous event">‹</button>
          <button
            type="button"
            onClick={() => {
              if (visibleCursor >= finalFrame) setCursor(0);
              setFollowing(false);
              setPlaying((current) => !current);
            }}
            aria-label={playing ? "Pause replay" : "Play replay"}
          >
            {playing ? "Ⅱ" : "▶"}
          </button>
          <button type="button" onClick={() => seek(visibleCursor + 1)} disabled={visibleCursor >= finalFrame} aria-label="Next event">›</button>
        </div>
        <div className="runtime-scrubber">
          <input
            type="range"
            min="0"
            max={finalFrame}
            value={visibleCursor}
            onChange={(event) => seek(event.currentTarget.value)}
            aria-label="Runtime replay position"
          />
          <div className="runtime-frame-marks" aria-hidden="true">
            {frames.map((frame, index) => (
              <i
                key={frame.id}
                className={`${frame.tone}${index <= visibleCursor ? " passed" : ""}`}
                style={{ left: `${finalFrame ? (index / finalFrame) * 100 : 0}%` }}
              />
            ))}
          </div>
        </div>
        <div className="runtime-transport-state">
          <strong>{playing ? "Playing" : following && isLive ? "Live" : "Paused"}</strong>
          <span>· click a card</span>
          <small>{visibleCursor + 1} / {frames.length} · t{model.turn || "—"}</small>
          {!following ? (
            <button type="button" onClick={() => { setCursor(finalFrame); setFollowing(true); setPlaying(false); }}>
              {isLive ? "Jump live" : "Latest"}
            </button>
          ) : null}
        </div>
      </div>

      {inspected ? (
        <RuntimeInspector card={inspected} onClose={() => setSelectedCard(null)} />
      ) : null}
    </div>
  );
}

export function buildAgentNavigationRows(agents, activeAgentId) {
  const mainAgent = agents.find((agent) => agent.kind === "main") || agents[0] || null;
  if (!mainAgent) return [];

  const agentsById = new Map(agents.map((agent) => [agent.id, agent]));
  const activeAgent = agentsById.get(activeAgentId) || mainAgent;
  const reversePath = [];
  const visited = new Set();
  let current = activeAgent;
  while (current && !visited.has(current.id)) {
    visited.add(current.id);
    reversePath.push(current);
    if (current.id === mainAgent.id) break;
    current = agentsById.get(current.parentId) || null;
  }

  const reachedMain = reversePath[reversePath.length - 1]?.id === mainAgent.id;
  const path = reachedMain ? reversePath.reverse() : [mainAgent];
  const pathIndexById = new Map(path.map((agent, index) => [agent.id, index]));
  const childrenByParentId = new Map();
  for (const agent of agents) {
    if (agent.kind !== "subagent" || !agent.parentId) continue;
    const children = childrenByParentId.get(agent.parentId) || [];
    children.push(agent);
    childrenByParentId.set(agent.parentId, children);
  }

  return path.flatMap((parent) => {
    const children = childrenByParentId.get(parent.id) || [];
    if (!children.length) return [];
    const pathIndex = pathIndexById.get(parent.id);
    return [{
      parent,
      children,
      selectedChildId: path[pathIndex + 1]?.id || null,
    }];
  });
}

function useLiveStreamAutoscroll(viewportRef, contentRef, enabled, streamId, content) {
  useLayoutEffect(() => {
    const viewport = viewportRef.current;
    const content = contentRef.current;
    if (!enabled || !viewport || !content) return undefined;

    let animationFrame = null;
    const scheduleScroll = () => {
      if (animationFrame !== null) window.cancelAnimationFrame(animationFrame);
      animationFrame = window.requestAnimationFrame(() => {
        animationFrame = null;
        scrollStreamToBottom(viewport);
      });
    };

    scrollStreamToBottom(viewport);
    scheduleScroll();

    const resizeObserver = typeof ResizeObserver === "function"
      ? new ResizeObserver(scheduleScroll)
      : null;
    resizeObserver?.observe(content);

    const mutationObserver = typeof MutationObserver === "function"
      ? new MutationObserver(scheduleScroll)
      : null;
    mutationObserver?.observe(content, {
      characterData: true,
      childList: true,
      subtree: true,
    });

    return () => {
      if (animationFrame !== null) window.cancelAnimationFrame(animationFrame);
      resizeObserver?.disconnect();
      mutationObserver?.disconnect();
    };
  }, [content, contentRef, enabled, streamId, viewportRef]);
}

function RuntimeGuardBundle({ guards, owner }) {
  if (!guards?.length) return null;
  return (
    <span className={`runtime-guard-bundle ${owner}`} aria-label={`${owner} guards`}>
      {guards.map((guard) => (
        <span key={guard.id} className={`runtime-guard-row ${guard.status}`}>
          <i />
          <b title={guard.name}>{compactText(guard.name, owner === "model" ? 18 : 24)}</b>
          <em>{guard.timing || "guard"}</em>
          <small>{guardStatusLabel(guard.status)}</small>
        </span>
      ))}
    </span>
  );
}

export function scrollStreamToBottom(viewport) {
  if (!viewport) return;
  viewport.scrollTop = Math.max(0, viewport.scrollHeight - viewport.clientHeight);
}

function RuntimeInspector({ card, onClose }) {
  const detail = displayValue(card.detail || {});
  const rawEvents = card.rawEvents || [];
  const rawDetail = displayValue(rawEvents.length ? rawEvents : card.detail || {});
  const result = card.result === undefined || card.result === null
    ? null
    : displayValue(card.result);
  return (
    <aside className={`runtime-inspector ${card.status || "waiting"}`}>
      <div className="runtime-inspector-summary">
        <span>{card.kind}</span>
        <strong>{card.label}</strong>
        {card.summary ? <p>{card.summary}</p> : null}
      </div>
      <div className="runtime-inspector-body">
        {card.error ? (
          <section className="runtime-inspector-outcome failed">
            <span>Failure</span>
            <strong>{card.error.type || "Tool error"}</strong>
            <p>{card.error.message || displayValue(card.error)}</p>
          </section>
        ) : null}
        {card.resultRequestStatus === "loading" ? (
          <p className="runtime-inspector-notice">Loading the durable result from Temporal history…</p>
        ) : null}
        {card.resultRequestStatus === "error" ? (
          <p className="runtime-inspector-notice error">
            Could not load the durable result: {card.resultRequestError}
          </p>
        ) : null}
        {card.resultRequestStatus === "missing" && result === null ? (
          <p className="runtime-inspector-notice">No durable result was found for this tool call.</p>
        ) : null}
        {result !== null ? (
          <section className="runtime-inspector-outcome result">
            <span>Result {card.resultSource ? `· ${card.resultSource}` : ""}</span>
            <pre>{result}</pre>
            {card.resultTruncated ? (
              <p>Stream preview truncated; loading the durable result provides the complete output.</p>
            ) : null}
          </section>
        ) : !card.error && card.kind !== "Tool" ? (
          <pre>{detail}</pre>
        ) : null}
        <details className="runtime-inspector-raw">
          <summary>{rawEvents.length ? `Raw events (${rawEvents.length})` : "Raw details"}</summary>
          <pre>{rawDetail}</pre>
        </details>
      </div>
      <button type="button" onClick={onClose}>Close details</button>
    </aside>
  );
}

function displayValue(value) {
  if (typeof value === "string") return value;
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

export function buildRuntimeFrames(_timeline, agent) {
  if (!agent) return [];
  const frames = [
    { id: `${agent.id}:ingress`, kind: "ingress", tone: "ingress", turn: 0 },
  ];
  let ordinal = 0;
  for (const group of runtimeTurnGroups(agent.segments || [])) {
    const llmGuardEvents = group.toolSegments.flatMap((segment) =>
      (segment.events || []).filter(isLlmGuardEvent),
    );
    const preGuards = llmGuardEvents.filter(
      (event) => String(event.payload?.timing || "pre") !== "post",
    );
    const postGuards = llmGuardEvents.filter(
      (event) => String(event.payload?.timing || "") === "post",
    );

    for (const event of preGuards) {
      frames.push(modelGuardFrame(agent.id, group.turn, event, ordinal));
      ordinal += 1;
    }

    for (const segment of group.agentSegments) {
      if (String(segment.thinking || "").trim()) {
        frames.push({
          id: `${segment.id}:thinking`,
          kind: "model",
          tone: "model",
          turn: group.turn,
          modelStatus: "thinking",
          segment,
        });
      }
      frames.push({
        id: `${segment.id}:model`,
        kind: "model",
        tone: segment.status === "failed" ? "failed" : "model",
        turn: group.turn,
        modelStatus: modelStatusForSegment(segment),
        segment,
      });
    }

    for (const event of postGuards) {
      frames.push(modelGuardFrame(agent.id, group.turn, event, ordinal));
      ordinal += 1;
    }

    for (const segment of group.toolSegments) {
      const toolFrames = [];
      let pendingEvents = [];
      let currentFrame = null;
      for (const event of (segment.events || []).filter((candidate) => !isLlmGuardEvent(candidate))) {
        if (!isSemanticToolEvent(event)) {
          pendingEvents.push(event);
          continue;
        }
        currentFrame = {
          id: `${segment.id}:event:${ordinal}`,
          kind: "tool-event",
          tone: eventTone(event),
          turn: group.turn,
          event,
          events: [...pendingEvents, event],
          ordinal,
        };
        pendingEvents = [];
        toolFrames.push(currentFrame);
        ordinal += 1;
      }
      if (pendingEvents.length) {
        if (currentFrame) currentFrame.events.push(...pendingEvents);
      }
      frames.push(...toolFrames);
      if (segment.status === "complete" && toolFrames.length &&
          toolSegmentNeedsSettledFrame(segment.events || [])) {
        frames.push({
          id: `${segment.id}:settled`,
          kind: "tools-settled",
          tone: "done",
          turn: group.turn,
          segment,
        });
      }
    }
  }
  return frames;
}

function runtimeTurnGroups(segments) {
  const groups = new Map();
  for (const segment of segments) {
    const turn = Number(segment.sequence ?? segment.afterSequence ?? 0);
    const key = String(turn);
    if (!groups.has(key)) {
      groups.set(key, { turn, agentSegments: [], toolSegments: [] });
    }
    const group = groups.get(key);
    if (segment.type === "agent") group.agentSegments.push(segment);
    else group.toolSegments.push(segment);
  }
  return [...groups.values()];
}

function modelGuardFrame(agentId, turn, event, ordinal) {
  return {
    id: `${agentId}:model-guard:${event.payload?.operation_id || ordinal}:${event.kind}`,
    kind: "model-guard",
    tone: eventTone(event),
    turn,
    event,
    ordinal,
  };
}

function isLlmGuardEvent(event) {
  return String(event?.kind || "").startsWith("harness_llm_guard_");
}

function isSemanticToolEvent(event) {
  const kind = String(event?.kind || "");
  if (kind.startsWith(AGENT_TOOL_INPUT_EVENT_PREFIX)) return true;
  if (/^harness_tool_(start|complete|failed)$/.test(kind)) return true;
  if (kind.startsWith("harness_tool_guard_")) return true;
  if (kind.endsWith("_activity_failed") || kind.endsWith("_activity_rejected")) return true;
  if (kind.endsWith("_activity_start") && Number(event.payload?.activity_attempt || 1) > 1) {
    return true;
  }
  if (kind.startsWith("artifact_create") ||
      kind.startsWith("python_sandbox_") ||
      kind.startsWith("workspace_shell_")) return true;
  return false;
}

function toolSegmentNeedsSettledFrame(events) {
  const tools = new Map();
  for (const [ordinal, event] of events.entries()) {
    const update = runtimeToolUpdate(event, ordinal, tools);
    if (update) tools.set(update.id, update);
  }
  return [...tools.values()].some((tool) =>
    ["requested", "running", "waiting"].includes(tool.status),
  );
}

export function projectRuntimeFrame(timeline, agent, frames, cursor) {
  const tools = new Map();
  const modelGuards = new Map();
  let toolTurn = null;
  let modelTurn = null;
  let turn = Number(agent?.segments?.[0]?.sequence || timeline?.activeSequence || 0);
  let modelStatus = "dormant";
  let modelBaseStatus = "dormant";
  let currentSegment = null;
  const visibleFrames = frames.slice(0, Math.max(0, cursor) + 1);

  for (const frame of visibleFrames) {
    if (frame.kind === "model") {
      const nextTurn = frame.turn || turn;
      if (modelTurn !== nextTurn) modelGuards.clear();
      modelTurn = nextTurn;
      turn = nextTurn;
      modelStatus = frame.modelStatus;
      modelBaseStatus = frame.modelStatus;
      currentSegment = frame.segment;
      continue;
    }
    if (frame.kind === "model-guard") {
      const nextTurn = frame.turn || turn;
      if (modelTurn !== nextTurn) modelGuards.clear();
      modelTurn = nextTurn;
      turn = nextTurn;
      const guard = runtimeGuardUpdate(frame.event, modelGuards.get(runtimeGuardId(frame.event)));
      modelGuards.set(guard.id, guard);
      if (guard.status === "failed") modelStatus = "failed";
      else if (guard.status === "running" || guard.status === "waiting") modelStatus = "guarding";
      else modelStatus = modelBaseStatus;
      continue;
    }
    if (frame.kind === "tool-event") {
      const nextToolTurn = frame.turn || turn;
      if (toolTurn !== null && nextToolTurn !== toolTurn) tools.clear();
      toolTurn = nextToolTurn;
      turn = nextToolTurn;
      for (const event of frame.events || [frame.event]) {
        const update = runtimeToolUpdate(event, frame.ordinal, tools);
        if (update) tools.set(update.id, update);
      }
      const modelIsStreamingToolInput =
        frame.event.kind === AgentStreamEventKind.AGENT_TOOL_INPUT_START ||
        frame.event.kind === AgentStreamEventKind.AGENT_TOOL_INPUT_DELTA;
      modelStatus = modelIsStreamingToolInput ? "active" : "dormant";
      modelBaseStatus = modelStatus;
      continue;
    }
    if (frame.kind === "tools-settled") {
      const nextToolTurn = frame.turn || turn;
      if (toolTurn !== null && nextToolTurn !== toolTurn) tools.clear();
      toolTurn = nextToolTurn;
      turn = nextToolTurn;
      for (const [id, tool] of tools) {
        if (["requested", "running", "waiting"].includes(tool.status)) {
          tools.set(id, { ...tool, status: "done", executionStatus: "done" });
        }
      }
      modelStatus = "dormant";
      modelBaseStatus = "dormant";
    }
  }

  return {
    turn,
    modelStatus,
    modelGuards: [...modelGuards.values()],
    currentSegment,
    tools: [...tools.values()],
    agentStatus:
      cursor >= frames.length - 1
        ? runtimeStatus(agent?.status || timeline?.status)
        : modelStatus === "failed"
          ? "error"
          : "running",
  };
}

export function projectSubagentTools(model, agents, activeAgent, enabled = true) {
  if (!enabled || activeAgent?.kind !== "main") return model;
  const tools = new Map(model.tools.map((tool) => [tool.id, tool]));
  const subagents = agents.filter(
    (agent) => agent.kind === "subagent" && agent.parentId === activeAgent.id,
  );

  for (const subagent of subagents) {
    const id = String(subagent.parentToolCallId || `subagent:${subagent.id}`);
    const existing = tools.get(id);
    const status = subagentToolStatus(subagent.status);
    if (!existing && !["requested", "running", "waiting"].includes(status)) {
      continue;
    }
    tools.set(id, {
      ...existing,
      id,
      name: "create_subagent",
      status,
      executionStatus: status,
      preview: subagent.label,
      guards: existing?.guards || [],
      detail: {
        ...(existing?.detail || {}),
        tool_call_id: id,
        tool_name: "create_subagent",
        latest_event: "subagent_status",
        child_agent_id: subagent.id,
        child_agent_status: subagent.status,
        child_task: subagent.label,
        child_turns: subagent.turnCount || 0,
        child_tools: subagent.toolCount || 0,
      },
    });
  }

  return { ...model, tools: [...tools.values()] };
}

function runtimeToolUpdate(event, ordinal, tools) {
  const kind = String(event?.kind || "");
  if (!kind || isLlmGuardEvent(event)) return null;
  const payload = event.payload || {};
  const isInput = kind.startsWith(AGENT_TOOL_INPUT_EVENT_PREFIX);
  const isToolLifecycle = /^harness_tool_(start|complete|failed)$/.test(kind);
  const isToolGuard = kind.startsWith("harness_tool_guard_");
  const isToolActivity = kind.startsWith("harness_tool_activity_");
  const isToolDetail = isToolActivity;
  const isArtifact = kind.startsWith("artifact_create");
  const isPython = kind.startsWith("python_sandbox_");
  const isWorkspaceShell = kind.startsWith("workspace_shell_");
  if (isToolGuard) return runtimeToolGuardUpdate(event, ordinal, tools);
  const rawId =
    payload.tool_use_id ||
    event.tool_call_id ||
    (isToolActivity ? payload.parent_operation_id : payload.operation_id) ||
    payload.run_id ||
    `${payload.tool_name || event.tool_name || kind}:${payload.llm_sequence ?? payload.sequence ?? ordinal}`;
  const id = String(rawId);
  const existing = tools.get(id);
  if (isToolActivity && !existing && !payload.tool_name) return null;
  if (!isInput && !isToolLifecycle && !isToolDetail && !isArtifact && !isPython && !isWorkspaceShell && !event.tool_name && !payload.tool_name) {
    return null;
  }
  const name = String(
    payload.tool_name ||
      event.tool_name ||
      existing?.name ||
      (isArtifact
        ? "create_artifact"
        : isWorkspaceShell
          ? "workspace_shell"
          : isPython
            ? "python_sandbox"
            : "tool"),
  );
  const nextStatus = toolStatusForEvent(kind, payload, existing?.status);
  const input = payload.input ?? payload.input_partial ?? payload.input_preview;
  const preview = input === undefined
    ? existing?.preview || ""
    : compactValue(input);
  const events = [...(existing?.events || []), event];
  const guards = existing?.guards || [];
  const outcome = toolOutcomeForEvents(events);
  return {
    id,
    name,
    kind: existing?.kind || "tool",
    status: nextStatus,
    executionStatus: nextStatus,
    preview,
    events,
    guards,
    error: outcome.error,
    resultPreview: outcome.result,
    resultTruncated: outcome.truncated,
    detail: {
      tool_call_id: id,
      tool_name: name,
      input: input ?? existing?.detail?.input,
      guards: guards.map((guard) => guard.detail),
      latest_event: kind,
      latest_payload: payload,
      event_count: events.length,
    },
  };
}

export function toolOutcomeForEvents(events) {
  let error = null;
  let result = null;
  let truncated = false;

  for (let index = (events || []).length - 1; index >= 0; index -= 1) {
    const payload = events[index]?.payload || {};
    if (error === null && payload.error !== undefined && payload.error !== null) {
      error = normalizedToolError(payload.error);
    }
    if (result === null && typeof payload.result_preview === "string") {
      result = parseResultPreview(payload.result_preview);
      truncated = Boolean(payload.result_truncated);
    }
  }
  return { error, result, truncated };
}

function normalizedToolError(error) {
  if (error && typeof error === "object") {
    return {
      type: String(error.type || "ToolError"),
      message: String(error.message || JSON.stringify(error)),
    };
  }
  return { type: "ToolError", message: String(error) };
}

function parseResultPreview(preview) {
  try {
    return JSON.parse(preview);
  } catch {
    return preview;
  }
}

function runtimeToolGuardUpdate(event, ordinal, tools) {
  const payload = event.payload || {};
  const parentId = String(
    payload.parent_operation_id ||
    payload.tool_use_id ||
    event.tool_call_id ||
    `${payload.tool_name || "tool"}:${payload.llm_sequence ?? ordinal}`,
  );
  const existing = tools.get(parentId);
  const guardId = runtimeGuardId(event);
  const previousGuard = existing?.guards?.find((guard) => guard.id === guardId);
  const guard = runtimeGuardUpdate(event, previousGuard);
  const guards = [...(existing?.guards || []).filter((candidate) => candidate.id !== guard.id), guard];
  const executionStatus = existing?.executionStatus || existing?.status || "requested";
  const status = guard.status === "failed"
    ? "failed"
    : guard.status === "waiting"
      ? "waiting"
      : executionStatus;
  const events = [...(existing?.events || []), event];
  const name = String(payload.tool_name || existing?.name || event.tool_name || "tool");
  const preview = existing?.preview || compactValue(payload.input || "");
  return {
    id: parentId,
    name,
    kind: existing?.kind || "tool",
    status,
    executionStatus,
    preview,
    events,
    guards,
    detail: {
      ...(existing?.detail || {}),
      tool_call_id: parentId,
      tool_name: name,
      input: existing?.detail?.input,
      guards: guards.map((candidate) => candidate.detail),
      latest_event: event.kind,
      latest_payload: payload,
      event_count: events.length,
    },
  };
}

function runtimeGuardId(event) {
  const payload = event?.payload || {};
  return String(
    payload.operation_id ||
    `${payload.parent_operation_id || "guard"}:${payload.timing || "guard"}:${payload.guard_name || event?.kind}`,
  );
}

function runtimeGuardUpdate(event, existing = null) {
  const payload = event?.payload || {};
  const status = guardStatusForEvent(event?.kind, payload, existing?.status);
  const events = [...(existing?.events || []), event];
  const name = String(payload.guard_name || event?.tool_name || "guard");
  return {
    id: runtimeGuardId(event),
    name,
    timing: String(payload.timing || "guard"),
    status,
    reason: payload.reason || existing?.reason || null,
    events,
    detail: {
      guard_name: name,
      guard_timing: payload.timing || null,
      operation_id: runtimeGuardId(event),
      parent_operation_id: payload.parent_operation_id || null,
      status,
      reason: payload.reason || existing?.reason || null,
      latest_event: event?.kind || null,
      latest_payload: payload,
      event_count: events.length,
    },
  };
}

function guardStatusForEvent(kind, payload, fallback = "running") {
  const normalized = String(payload.status || "").toLowerCase();
  if (String(kind || "").endsWith("_failed") ||
      String(kind || "").endsWith("_rejected") ||
      ["failed", "error", "rejected", "blocked", "cancelled"].includes(normalized)) {
    return "failed";
  }
  if (String(kind || "").endsWith("_complete")) return "done";
  if (String(kind || "").endsWith("_start") &&
      String(payload.guard_name || "").includes("approval")) return "waiting";
  if (String(kind || "").endsWith("_start")) return "running";
  return fallback || "running";
}

function toolStatusForEvent(kind, payload, previous = "requested") {
  if (kind === AgentStreamEventKind.AGENT_TOOL_INPUT_START ||
      kind === AgentStreamEventKind.AGENT_TOOL_INPUT_DELTA) return "building";
  if (kind === AgentStreamEventKind.AGENT_TOOL_INPUT_COMPLETE) return "requested";
  if (kind === "harness_tool_start" || kind.endsWith("_activity_start")) return "running";
  if (kind === "harness_tool_complete") return normalizeToolStatus(payload.status, "done");
  if (kind === "artifact_create_complete") return "done";
  if (kind.endsWith("_failed") || kind.endsWith("_rejected")) return "failed";
  if (kind.endsWith("_activity_complete")) return "running";
  if (kind.startsWith("python_sandbox_") || kind.startsWith("workspace_shell_")) return "running";
  return previous || "requested";
}

function normalizeToolStatus(value, fallback) {
  const status = String(value || "").toLowerCase();
  if (["done", "complete", "completed", "passed", "success"].includes(status)) return "done";
  if (["failed", "error", "rejected", "cancelled", "blocked"].includes(status)) return "failed";
  if (["waiting", "approval", "pending_approval"].includes(status)) return "waiting";
  if (["running", "streaming", "retrying", "active"].includes(status)) return "running";
  return fallback || "requested";
}

function subagentToolStatus(status) {
  const normalized = runtimeStatus(status);
  if (normalized === "running") return "running";
  if (normalized === "complete") return "done";
  if (normalized === "error") return "failed";
  if (normalized === "waiting") return "waiting";
  return "requested";
}

function modelStatusForSegment(segment) {
  if (segment.status === "failed" || segment.status === "interrupted") return "failed";
  if (segment.status === "retrying") return "thinking";
  if (segment.status === "streaming") {
    return String(segment.thinking || "").trim() && !String(segment.text || "").trim()
      ? "thinking"
      : "active";
  }
  if (segment.terminal) return "complete";
  if (segment.stopReason === "tool_use") {
    return String(segment.text || "").trim() ? "active" : "dormant";
  }
  return segment.status === "complete" ? "complete" : "active";
}

export function modelStreamForProjection(model) {
  const response = String(model?.currentSegment?.text || "").trim();
  if (response) {
    return { kind: "response", label: "Response stream", text: response };
  }
  const thinking = String(model?.currentSegment?.thinking || "").trim();
  if (thinking) {
    return { kind: "thinking", label: "Thinking stream", text: thinking };
  }
  return { kind: "idle", label: "Model stream", text: "" };
}

export function inspectedRuntimeCard(selected, model, resultRequest = null) {
  if (!selected) return null;
  if (selected.type === "ingress") {
    return { kind: "Ingress", label: "Message received", status: "received", detail: {} };
  }
  if (selected.type === "model") {
    const segment = model.currentSegment;
    return {
      kind: "Model",
      label: segment?.model || segment?.provider || "Agent model",
      status: model.modelStatus,
      summary: segment?.text ? compactText(segment.text, 220) : compactText(segment?.thinking, 220),
      detail: {
        ...(segment || {}),
        guards: model.modelGuards?.map((guard) => guard.detail) || [],
      },
    };
  }
  const tool = model.tools.find((candidate) => candidate.id === selected.id);
  if (!tool) return null;
  const durable = resultRequest?.status === "loaded" ? resultRequest.data : null;
  const durableResult = durable?.result;
  const durableError = durable?.error || (
    durableResult && typeof durableResult === "object" && !Array.isArray(durableResult)
      ? durableResult.error
      : null
  );
  return {
    kind: "Tool",
    label: tool.name,
    status: tool.status,
    summary: tool.preview,
    detail: tool.detail,
    error: durableError ? normalizedToolError(durableError) : tool.error,
    result: durableResult === undefined ? tool.resultPreview : durableResult,
    resultTruncated: durableResult === undefined && tool.resultTruncated,
    resultRequestStatus: resultRequest?.status || null,
    resultRequestError: resultRequest?.error || null,
    resultSource: durable?.source || (tool.resultPreview !== null ? "stream preview" : null),
    rawEvents: tool.events || [],
  };
}

function useFluidToolLayout(ref, version) {
  const positionsRef = useRef(new Map());
  useLayoutEffect(() => {
    const container = ref.current;
    if (!container) return;
    const reduceMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    const next = new Map();
    for (const element of container.querySelectorAll("[data-runtime-card]")) {
      const id = element.getAttribute("data-runtime-card");
      const rect = element.getBoundingClientRect();
      next.set(id, rect);
      const previous = positionsRef.current.get(id);
      if (!previous || reduceMotion || typeof element.animate !== "function") continue;
      const deltaX = previous.left - rect.left;
      const deltaY = previous.top - rect.top;
      if (Math.abs(deltaX) < 1 && Math.abs(deltaY) < 1) continue;
      element.animate(
        [
          { transform: `translate(${deltaX}px, ${deltaY}px)` },
          { transform: "translate(0, 0)" },
        ],
        { duration: 360, easing: "cubic-bezier(.2,.8,.2,1)" },
      );
    }
    positionsRef.current = next;
  }, [ref, version]);
}

function eventTone(event) {
  const kind = String(event?.kind || "");
  if (kind.endsWith("_failed") || kind.endsWith("_rejected")) return "failed";
  if (kind.endsWith("_complete")) return "done";
  if (kind.startsWith(AGENT_TOOL_INPUT_EVENT_PREFIX)) return "requested";
  return "running";
}

function runtimeStatus(status) {
  const value = String(status || "waiting").toLowerCase();
  if (["streaming", "tooling", "running", "retrying", "active"].includes(value)) return "running";
  if (["complete", "completed", "done"].includes(value)) return "complete";
  if (["failed", "error", "cancelled", "interrupted"].includes(value)) return "error";
  return value === "queued" ? "queued" : "waiting";
}

function runtimeStatusLabel(status) {
  const value = runtimeStatus(status);
  return value === "complete" ? "Complete" : value === "error" ? "Stopped" : value === "waiting" ? "Waiting" : value === "queued" ? "Queued" : "Running";
}

function modelStatusLabel(status) {
  return ({ dormant: "Dormant", active: "Responding", thinking: "Thinking", guarding: "Guarding", complete: "Complete", failed: "Stopped" })[status] || "Dormant";
}

export function toolStatusLabel(status) {
  return ({
    building: "Building input…",
    requested: "Requested",
    running: "Running",
    waiting: "Waiting",
    done: "Done",
    failed: "Failed",
  })[status] || "Requested";
}

function guardStatusLabel(status) {
  return ({ running: "Checking", waiting: "Waiting", done: "Passed", failed: "Blocked" })[status] || "Checking";
}

function guardVersion(guards) {
  return (guards || []).map((guard) => `${guard.id}:${guard.status}`).join(",");
}

function shortId(value) {
  const compact = String(value || "tool").replace(/[^a-zA-Z0-9]/g, "");
  return (compact.slice(-5) || "tool").toLowerCase();
}

function compactValue(value) {
  if (typeof value === "string") return value;
  try {
    return JSON.stringify(value);
  } catch (_error) {
    return String(value);
  }
}

function compactText(value, limit) {
  const text = String(value || "").replace(/\s+/g, " ").trim();
  if (text.length <= limit) return text;
  return `${text.slice(0, Math.max(0, limit - 1))}…`;
}
