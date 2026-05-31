import { ApprovalsPanel } from "./ApprovalsPanel.jsx";
import { MarkdownContent } from "./MarkdownContent.jsx";
import { StreamPanel } from "./StreamPanel.jsx";
import { visibleMessageItems } from "../state/chatState.js";

export function Messages({
  workflowState,
  draftConversation,
  loadingConversation,
  olderMessagesLoading,
  olderMessagesError,
  localPending,
  streamTurn,
  streamPanelCollapsed,
  resolvingApprovals,
  onToggleStreamPanel,
  onResolveApproval,
  onLoadOlderMessages,
}) {
  const transcript = workflowState?.transcript || [];
  const transcriptOffset = workflowState?.transcript_offset || 0;
  const transcriptTotal = workflowState?.transcript_total ?? transcriptOffset + transcript.length;
  const messageItems = visibleMessageItems(
    transcript,
    localPending,
    transcriptOffset,
    transcriptTotal,
  );
  const hasContent = workflowState || localPending.length > 0;
  if (loadingConversation) {
    return <ConversationLoading />;
  }
  return (
    <>
      {!hasContent ? (
        <div className="empty">
          {draftConversation
            ? "Type your first message to start a Temporal workflow."
            : "Starting a Temporal workflow..."}
        </div>
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
            content={`${item.pending.content} (${item.pending.phase})`}
          />
        ) : (
          <MessageBubble
            key={`transcript-${item.index}`}
            message={item.message}
            index={item.index}
            workflowState={workflowState}
          />
        )
      ))}
      <StreamPanel
        turn={streamTurn}
        collapsed={streamPanelCollapsed}
        onToggle={onToggleStreamPanel}
      />
      <ApprovalsPanel
        workflowState={workflowState}
        resolvingApprovals={resolvingApprovals}
        onResolve={onResolveApproval}
      />
    </>
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

function MessageBubble({ message, index, workflowState }) {
  if (message.role === "user") {
    if (workflowState.active_message_index === index) {
      return <Bubble kind="pending" label="you -> agent" content={`${message.content} (delivered)`} />;
    }
    if ((workflowState.queued_message_indices || []).includes(index)) {
      return <Bubble kind="pending" label="you" content={`${message.content} (queued)`} />;
    }
    return <Bubble kind="user" label="you" content={message.content} />;
  }
  if (message.role === "assistant") {
    return <Bubble kind="assistant" label="assistant" content={message.content} />;
  }
  return <Bubble kind="system" label="system" content={message.content} />;
}

function Bubble({ kind, label, content }) {
  return (
    <div className={`bubble ${kind}`}>
      <span className="label">{label}</span>
      <MarkdownContent content={content} />
    </div>
  );
}
