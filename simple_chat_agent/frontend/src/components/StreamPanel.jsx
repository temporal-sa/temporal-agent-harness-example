import { CodeBlock, MarkdownContent } from "./MarkdownContent.jsx";
import { inferCodeLanguage } from "../utils/code.js";

export function StreamPanel({ turn, collapsed, onToggle }) {
  if (!turn) return null;
  const timeline = normalizeStreamTimeline(turn);
  if (!timeline.segments.length) return null;
  return (
    <section className={`stream-panel ${timeline.status}${collapsed ? " collapsed" : ""}`}>
      <div className="stream-panel-header">
        <div className="stream-panel-title">
          Streaming visibility
          <span className="stream-panel-status">{streamPanelStatus(timeline)}</span>
        </div>
        <button type="button" className="stream-panel-toggle" onClick={onToggle}>
          {collapsed ? "Expand" : "Collapse"}
        </button>
      </div>
      <div className="stream-panel-body">
        {collapsed ? (
          <div className="stream-preview">{streamPanelPreview(timeline)}</div>
        ) : (
          <StreamPanelBody timeline={timeline} />
        )}
      </div>
    </section>
  );
}

function StreamPanelBody({ timeline }) {
  return (
    <div className="stream-timeline">
      {timeline.segments.map((segment) =>
        segment.type === "agent" ? (
          <AgentStreamSegment key={segment.id} segment={segment} />
        ) : (
          <ToolStreamSegment key={segment.id} segment={segment} />
        ),
      )}
    </div>
  );
}

function AgentStreamSegment({ segment }) {
  const complete = segment.status === "complete";
  const text = String(segment.text || "").trim();
  const thinking = String(segment.thinking || "").trim();
  return (
    <div className={`stream-agent-segment ${complete ? "complete" : "streaming"}`}>
      <div className="stream-finished-title">
        Claude turn {segment.sequence ?? ""} {complete ? "complete" : "streaming"}
        {complete && segment.stopReason ? ` | ${segment.stopReason}` : ""}
      </div>
      {thinking ? <div className="stream-thinking">{thinking}</div> : null}
      {text ? (
        complete ? (
          <MarkdownContent content={text} />
        ) : (
          <div className="stream-text">{text}</div>
        )
      ) : (
        <div className="stream-preview">
          {complete
            ? `Completed without text (${segment.stopReason || "unknown"}).`
            : "Waiting for streamed tokens..."}
        </div>
      )}
    </div>
  );
}

function ToolStreamSegment({ segment }) {
  const complete = segment.status === "complete";
  return (
    <div className={`stream-tool-segment ${complete ? "complete" : "streaming"}`}>
      <div className="stream-finished-title">
        Tool activity after Claude turn {segment.afterSequence ?? ""}{" "}
        {complete ? "complete" : "streaming"}
      </div>
      {segment.events?.length ? (
        <StreamToolList events={segment.events} active={!complete} />
      ) : (
        <div className="stream-preview">Waiting for tool activity...</div>
      )}
    </div>
  );
}

function StreamToolList({ events, active }) {
  return (
    <div className="stream-tool-list">
      {events.slice(-5).map((event, index) => (
        <StreamToolEvent
          key={streamToolEventKey(event, index)}
          event={event}
          active={active}
        />
      ))}
    </div>
  );
}

function StreamToolEvent({ event, active }) {
  const payloadText = streamToolPayloadText(event);
  const status = event.payload?.status || sandboxProgressStatus(event.payload);
  return (
    <div
      className={`stream-tool-event${
        event.kind?.startsWith("claude_tool_input_") ? " input-streaming" : ""
      }`}
    >
      <div className="stream-tool-name">
        {streamToolLabel(event)}
        {status ? <span className="stream-tool-status">{status}</span> : null}
      </div>
      <div className="stream-tool-payload">
        <CodeBlock
          source={payloadText}
          languageHint={streamToolLanguage(event, payloadText)}
          compact
          highlight={!active}
          wrapLongLines
        />
      </div>
    </div>
  );
}

function streamToolPayloadText(event) {
  const payload = event.payload || {};
  if (event.kind?.startsWith("claude_tool_input_")) {
    if (event.kind === "claude_tool_input_complete") {
      return truncateStreamText(
        formatStreamValue(payload.input ?? payload.input_partial ?? payload.input_preview),
      );
    }
    return truncateStreamText(String(payload.input_partial || payload.partial_json || ""));
  }

  if (
    typeof payload.text === "string" &&
    (event.kind?.includes("stdout") || event.kind?.includes("stderr"))
  ) {
    return truncateStreamText(payload.text);
  }

  return truncateStreamText(formatStreamValue(payload));
}

function streamToolLanguage(event, payloadText) {
  if (event.kind === "claude_tool_input_complete") return "json";
  if (event.kind?.includes("stdout") || event.kind?.includes("stderr")) {
    return inferCodeLanguage(payloadText) || "text";
  }
  return inferCodeLanguage(payloadText) || "json";
}

function streamPanelStatus(turn) {
  const count = streamToolEvents(turn).length;
  const toolText = count === 1 ? "1 tool event" : `${count} tool events`;
  const turnCount = turn.segments.filter((segment) => segment.type === "agent").length;
  const turnText = turnCount === 1 ? "1 Claude turn" : `${turnCount} Claude turns`;
  if (turn.status === "interrupted") return `interrupted | ${toolText}`;
  if (turn.status === "complete") return `complete | ${turnText} | ${toolText}`;
  if (turn.status === "tooling") return `tool activity | ${turnText} | ${toolText}`;
  if (turn.status === "waiting") return `finalizing | ${turnText} | ${toolText}`;
  return `streaming | ${turnText} | ${toolText}`;
}

function streamPanelPreview(turn) {
  const activeAgent = latestAgentSegment(turn, { active: true });
  const latestAgent = latestAgentSegment(turn);
  const latestEvent = streamToolEvents(turn).at(-1);
  const text = String(activeAgent?.text || "").trim();
  const thinking = String(activeAgent?.thinking || "").trim();
  if (text) return text.replace(/\s+/g, " ").slice(-240);
  if (thinking) return thinking.replace(/\s+/g, " ").slice(-240);
  if (latestAgent?.text) return latestAgent.text.replace(/\s+/g, " ").slice(-240);
  if (latestAgent?.thinking) {
    return latestAgent.thinking.replace(/\s+/g, " ").slice(-240);
  }
  if (latestEvent) return `${streamToolLabel(latestEvent)} | ${latestEvent.kind}`;
  return streamPanelStatus(turn);
}

function normalizeStreamTimeline(turn) {
  if (turn.segments) return turn;

  const segments = [];
  for (const finishedTurn of turn.finishedTurns || []) {
    segments.push({
      id: `agent:${finishedTurn.sequence ?? "unknown"}:${segments.length}`,
      type: "agent",
      sequence: finishedTurn.sequence ?? null,
      status: "complete",
      text: finishedTurn.text || "",
      thinking: finishedTurn.thinking || "",
      stopReason: finishedTurn.stopReason || "unknown",
      usage: finishedTurn.usage || null,
      completedAt: finishedTurn.completedAt || null,
    });
    if (finishedTurn.events?.length) {
      segments.push({
        id: `tools:${finishedTurn.sequence ?? "unknown"}:${segments.length}`,
        type: "tools",
        afterSequence: finishedTurn.sequence ?? null,
        status: "complete",
        events: finishedTurn.events,
        completedAt: finishedTurn.completedAt || null,
      });
    }
  }
  if (turn.text || turn.thinking) {
    segments.push({
      id: `agent:${turn.activeSequence ?? "unknown"}:${segments.length}`,
      type: "agent",
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
      id: `tools:${turn.activeSequence ?? "unknown"}:${segments.length}`,
      type: "tools",
      afterSequence: turn.activeSequence ?? null,
      status: turn.status === "complete" ? "complete" : "streaming",
      events: turn.currentEvents,
      completedAt: null,
    });
  }
  return { ...turn, segments };
}

function latestAgentSegment(turn, options = {}) {
  for (let index = turn.segments.length - 1; index >= 0; index -= 1) {
    const segment = turn.segments[index];
    if (segment.type !== "agent") continue;
    if (options.active && segment.status === "complete") continue;
    return segment;
  }
  return null;
}

function streamToolEvents(turn) {
  return turn.segments.flatMap((segment) => segment.events || []);
}

function streamToolEventKey(event, index) {
  if (event.streamToolInputKey) return `input:${event.streamToolInputKey}`;
  if (event.payload?.tool_use_id) return `tool-use:${event.payload.tool_use_id}`;
  const sequence = event.sequence ?? event.payload?.sequence ?? index;
  return `${event.kind}:${event.tool_name || ""}:${event.step || ""}:${sequence}`;
}

function streamToolLabel(event) {
  if (event.kind === "python_sandbox_stdout") return "python_sandbox stdout";
  if (event.kind === "python_sandbox_stderr") return "python_sandbox stderr";
  if (event.kind === "python_sandbox_progress") return "python_sandbox progress";
  const payloadToolName = event.payload?.tool_name;
  const name = payloadToolName || event.tool_name || "stream";
  return event.step ? `${name}:${event.step}` : name;
}

function sandboxProgressStatus(payload = {}) {
  const elapsed = Number(payload.elapsed_seconds);
  const timeout = Number(payload.timeout_seconds);
  if (!Number.isFinite(elapsed) || !Number.isFinite(timeout) || timeout <= 0) {
    return "";
  }
  return `${elapsed}s / ${timeout}s`;
}

function formatStreamValue(value) {
  if (typeof value === "string") return value;
  try {
    return JSON.stringify(value, null, 2);
  } catch (_error) {
    return String(value);
  }
}

function truncateStreamText(value) {
  const text = String(value || "");
  if (text.length <= 4000) return text;
  return text.slice(-4000);
}
