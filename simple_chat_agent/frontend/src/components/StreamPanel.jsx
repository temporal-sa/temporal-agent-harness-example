import { CodeBlock, MarkdownContent } from "./MarkdownContent.jsx";
import { inferCodeLanguage } from "../utils/code.js";

export function StreamPanel({ turn, collapsed, onToggle }) {
  if (!turn) return null;
  if (
    !turn.text &&
    !turn.thinking &&
    turn.currentEvents.length === 0 &&
    turn.finishedTurns.length === 0
  ) {
    return null;
  }
  return (
    <section className={`stream-panel ${turn.status}${collapsed ? " collapsed" : ""}`}>
      <div className="stream-panel-header">
        <div className="stream-panel-title">
          Streaming visibility
          <span className="stream-panel-status">{streamPanelStatus(turn)}</span>
        </div>
        <button type="button" className="stream-panel-toggle" onClick={onToggle}>
          {collapsed ? "Expand" : "Collapse"}
        </button>
      </div>
      <div className="stream-panel-body">
        {collapsed ? (
          <div className="stream-preview">{streamPanelPreview(turn)}</div>
        ) : (
          <StreamPanelBody turn={turn} />
        )}
      </div>
    </section>
  );
}

function StreamPanelBody({ turn }) {
  return (
    <>
      {turn.finishedTurns.length ? (
        <div className="stream-finished-list">
          {turn.finishedTurns.map((finishedTurn, index) => (
            <FinishedStreamTurn
              key={`${finishedTurn.sequence ?? "turn"}-${index}`}
              finishedTurn={finishedTurn}
            />
          ))}
        </div>
      ) : null}
      {turn.text ? (
        <div className="stream-current-turn">
          <div className="stream-finished-title">
            Claude turn {turn.activeSequence ?? ""} streaming
          </div>
          {turn.thinking ? <div className="stream-thinking">{turn.thinking}</div> : null}
          <div className="stream-text">{turn.text}</div>
          {turn.currentEvents.length ? <StreamToolList events={turn.currentEvents} /> : null}
        </div>
      ) : null}
      {!turn.text && turn.thinking ? (
        <div className="stream-current-turn">
          <div className="stream-finished-title">
            Claude turn {turn.activeSequence ?? ""} thinking
          </div>
          <div className="stream-thinking">{turn.thinking}</div>
        </div>
      ) : null}
      {!turn.text && turn.currentEvents.length ? (
        <StreamToolList events={turn.currentEvents} />
      ) : null}
      {!turn.text &&
      !turn.thinking &&
      !turn.currentEvents.length &&
      !turn.finishedTurns.length ? (
        <div className="stream-preview">Waiting for streamed tokens or tool activity...</div>
      ) : null}
    </>
  );
}

function FinishedStreamTurn({ finishedTurn }) {
  return (
    <div className="stream-finished-turn">
      <div className="stream-finished-title">
        Claude turn {finishedTurn.sequence ?? ""} complete | {finishedTurn.stopReason}
      </div>
      {finishedTurn.thinking ? (
        <div className="stream-thinking">{finishedTurn.thinking}</div>
      ) : null}
      {finishedTurn.text ? (
        <MarkdownContent content={finishedTurn.text} />
      ) : (
        <div>Completed without text ({finishedTurn.stopReason}).</div>
      )}
      {finishedTurn.events?.length ? <StreamToolList events={finishedTurn.events} /> : null}
    </div>
  );
}

function StreamToolList({ events }) {
  return (
    <div className="stream-tool-list">
      {events.slice(-5).map((event, index) => (
        <StreamToolEvent key={`${event.kind}-${index}`} event={event} />
      ))}
    </div>
  );
}

function StreamToolEvent({ event }) {
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
  if (event.kind?.includes("stdout") || event.kind?.includes("stderr")) return "text";
  return inferCodeLanguage(payloadText) || "json";
}

function streamPanelStatus(turn) {
  const count =
    turn.currentEvents.length +
    turn.finishedTurns.reduce(
      (total, finishedTurn) => total + (finishedTurn.events?.length || 0),
      0,
    );
  const toolText = count === 1 ? "1 tool event" : `${count} tool events`;
  const turnCount = turn.finishedTurns.length;
  const turnText = turnCount === 1 ? "1 Claude turn" : `${turnCount} Claude turns`;
  if (turn.status === "interrupted") return `interrupted | ${toolText}`;
  if (turn.status === "complete") return `complete | ${turnText} | ${toolText}`;
  if (turn.status === "tooling") return `tool activity | ${turnText} | ${toolText}`;
  if (turn.status === "waiting") return `finalizing | ${turnText} | ${toolText}`;
  return `streaming | ${turnText} | ${toolText}`;
}

function streamPanelPreview(turn) {
  const text = turn.text.trim();
  const thinking = String(turn.thinking || "").trim();
  const latestEvent = turn.currentEvents[turn.currentEvents.length - 1];
  if (text) return text.replace(/\s+/g, " ").slice(-240);
  if (thinking) return thinking.replace(/\s+/g, " ").slice(-240);
  const latestFinished = turn.finishedTurns[turn.finishedTurns.length - 1];
  if (latestFinished?.text) return latestFinished.text.replace(/\s+/g, " ").slice(-240);
  if (latestFinished?.thinking) {
    return latestFinished.thinking.replace(/\s+/g, " ").slice(-240);
  }
  if (latestEvent) return `${streamToolLabel(latestEvent)} | ${latestEvent.kind}`;
  return streamPanelStatus(turn);
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
