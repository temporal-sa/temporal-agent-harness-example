import { ApprovalsPanel } from "./ApprovalsPanel.jsx";
import { MarkdownContent } from "./MarkdownContent.jsx";
import { StreamPanel } from "./StreamPanel.jsx";
import { visibleMessageItems } from "../state/chatState.js";
import { formatBytes } from "../utils/format.js";

export function Messages({
  workflowId,
  workflowState,
  draftConversation,
  loadingConversation,
  olderMessagesLoading,
  olderMessagesError,
  localPending,
  streamTurn,
  turnTraces,
  expandedTraceIndex,
  streamPanelCollapsed,
  resolvingApprovals,
  draftSystemPrompt,
  onUpdateDraftSystemPrompt,
  onToggleStreamPanel,
  onToggleTurnTrace,
  onResolveApproval,
  onLoadOlderMessages,
}) {
  const transcript = workflowState?.transcript || [];
  const transcriptOffset = workflowState?.transcript_offset || 0;
  const transcriptTotal = workflowState?.transcript_total ?? transcriptOffset + transcript.length;
  const activeMessageValue = workflowState?.active_message_index;
  const activeMessageIndex =
    activeMessageValue === null || activeMessageValue === undefined
      ? null
      : Number(activeMessageValue);
  const activeMessage = Number.isFinite(activeMessageIndex)
    ? transcript[activeMessageIndex - transcriptOffset]
    : null;
  const messageItems = visibleMessageItems(
    transcript,
    localPending,
    transcriptOffset,
    transcriptTotal,
  );
  const waitingForRuntime =
    workflowState?.status === "responding" ||
    localPending.some((pending) =>
      ["sending", "delivered"].includes(String(pending?.phase || "")),
    );
  const hasContent = workflowState || localPending.length > 0;
  if (loadingConversation) {
    return <ConversationLoading />;
  }
  return (
    <>
      {!hasContent ? (
        draftConversation ? (
          <DraftEmptyState
            systemPrompt={draftSystemPrompt}
            onUpdateSystemPrompt={onUpdateDraftSystemPrompt}
          />
        ) : (
          <div className="empty">Starting a Temporal workflow...</div>
        )
      ) : null}
      {workflowState?.transcript_has_more_before ? (
        <HistoryLoader
          loading={olderMessagesLoading}
          error={olderMessagesError}
          onLoad={onLoadOlderMessages}
        />
      ) : null}
      {messageItems.map((item) => (
        item.kind === "pending" ? (
          <Bubble
            key={item.pending.id}
            kind="pending"
            label={item.pending.label}
            content={`${item.pending.content || "Attached files"} (${item.pending.phase})`}
            attachments={item.pending.attachments || []}
          />
        ) : (
          <MessageBubble
            key={`transcript-${item.index}`}
            message={item.message}
            index={item.index}
            workflowState={workflowState}
            turnTrace={turnTraces?.[item.index]}
            expandedTraceIndex={expandedTraceIndex}
            onToggleTurnTrace={onToggleTurnTrace}
          />
        )
      ))}
      <StreamPanel
        turn={streamTurn}
        waiting={waitingForRuntime}
        collapsed={streamPanelCollapsed}
        onToggle={onToggleStreamPanel}
        inputText={activeMessage?.role === "user" ? activeMessage.content : ""}
        workflowId={workflowId}
      />
      <ApprovalsPanel
        workflowState={workflowState}
        resolvingApprovals={resolvingApprovals}
        onResolve={onResolveApproval}
      />
    </>
  );
}

export function TurnTraceDrawer({
  trace,
  transcriptIndex,
  workflowId,
  open,
  onClose,
}) {
  return (
    <section
      className={`turn-trace-drawer-shell${open ? " open" : ""}`}
      hidden={!open}
      aria-hidden={!open}
      onClick={(event) => {
        if (event.target === event.currentTarget) onClose();
      }}
    >
      <div className="turn-trace-drawer" role="dialog" aria-modal="true">
        <div className="turn-trace-drawer-header">
          <div className="turn-trace-drawer-title">
            <span>Assistant Run Details</span>
            {transcriptIndex !== null && transcriptIndex !== undefined ? (
              <small>Transcript index {transcriptIndex}</small>
            ) : null}
          </div>
          <button type="button" onClick={onClose}>
            Close
          </button>
        </div>
        <div className="turn-trace-drawer-body">
          <TurnTraceDetails trace={trace} workflowId={workflowId} />
        </div>
      </div>
    </section>
  );
}

function DraftEmptyState({ systemPrompt, onUpdateSystemPrompt }) {
  return (
    <div className="empty draft-empty">
      <label className="draft-system-prompt">
        <span>System Prompt</span>
        <textarea
          rows={2}
          value={systemPrompt}
          onChange={(event) => onUpdateSystemPrompt(event.currentTarget.value)}
          aria-label="System prompt"
          spellCheck="true"
        />
      </label>
      <p>Type your first message to start a Temporal workflow.</p>
    </div>
  );
}

function HistoryLoader({ loading, error, onLoad }) {
  return (
    <div className="history-loader">
      <button type="button" disabled={loading} onClick={onLoad}>
        {loading ? "Loading earlier messages..." : "Load earlier messages"}
      </button>
      {error ? <span>{error}</span> : null}
    </div>
  );
}

function ConversationLoading() {
  return (
    <div className="conversation-loading" role="status" aria-live="polite">
      <div className="conversation-loading-label">Loading</div>
      <img
        className="temporal-loading-animation"
        src="/static/animated/temporal-logo-animation-inverted-transparent.gif"
        alt=""
        aria-hidden="true"
      />
    </div>
  );
}

function MessageBubble({
  message,
  index,
  workflowState,
  turnTrace,
  expandedTraceIndex,
  onToggleTurnTrace,
}) {
  if (message.role === "user") {
    if (workflowState.active_message_index === index) {
      return (
        <Bubble
          kind="pending"
          label="you -> agent"
          content={`${message.content || "Attached files"} (delivered)`}
          attachments={message.attachments || []}
        />
      );
    }
    if ((workflowState.queued_message_indices || []).includes(index)) {
      return (
        <Bubble
          kind="pending"
          label="you"
          content={`${message.content || "Attached files"} (queued)`}
          attachments={message.attachments || []}
        />
      );
    }
    return (
      <Bubble
        kind="user"
        label="you"
        content={message.content}
        attachments={message.attachments || []}
      />
    );
  }
  if (message.role === "assistant") {
    return (
      <Bubble
        kind="assistant"
        label="assistant"
        content={message.content}
        trace={turnTrace}
        traceExpanded={expandedTraceIndex === index}
        onToggleTrace={() => onToggleTurnTrace(index)}
      />
    );
  }
  return <Bubble kind="system" label="system" content={message.content} />;
}

function Bubble({
  kind,
  label,
  content,
  attachments = [],
  trace,
  traceExpanded,
  onToggleTrace,
}) {
  const canShowTrace = kind === "assistant" && onToggleTrace;
  return (
    <div className={`bubble ${kind}${traceExpanded ? " detail-open" : ""}`}>
      <div className="bubble-header">
        <span className="label">{label}</span>
        {canShowTrace ? (
          <button
            type="button"
            className="bubble-trace-toggle"
            onClick={onToggleTrace}
          >
            {traceExpanded ? "Hide details" : "Details"}
          </button>
        ) : null}
      </div>
      {content ? <MarkdownContent content={content} /> : null}
      <AttachmentChips attachments={attachments} />
    </div>
  );
}

function AttachmentChips({ attachments }) {
  if (!attachments?.length) return null;
  return (
    <div className="message-attachments">
      {attachments.map((attachment) => (
        <a
          key={attachment.attachment_id || attachment.artifact_id}
          className="message-attachment-chip"
          href={attachment.view_url || `/api/attachments/${attachment.attachment_id}`}
          target="_blank"
          rel="noreferrer"
        >
          <span>{attachment.name || "attachment"}</span>
          <small>
            {attachment.content_kind || "file"} | {formatBytes(attachment.size_bytes || 0)}
          </small>
        </a>
      ))}
    </div>
  );
}

function TurnTraceDetails({ trace, workflowId }) {
  if (!trace || trace.status === "loading") {
    return <div className="turn-trace-status">Loading turn details...</div>;
  }
  if (trace.status === "error") {
    return <div className="turn-trace-status error">{trace.error}</div>;
  }
  if (!trace.turn) {
    return <div className="turn-trace-status">No turn details available.</div>;
  }
  return (
    <div className="turn-trace-panel">
      <StreamPanel
        turn={trace.turn}
        collapsed={false}
        embedded
        workflowId={workflowId}
      />
    </div>
  );
}
