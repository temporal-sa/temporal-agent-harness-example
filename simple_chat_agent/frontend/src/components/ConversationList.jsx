export function ConversationList({
  conversations,
  currentWorkflowId,
  draftConversation,
  onNewDraft,
  onSelect,
  onDelete,
}) {
  return (
    <div>
      {draftConversation ? (
        <div className="conversation-row">
          <button type="button" className="conversation-item active" onClick={onNewDraft}>
            New chat
          </button>
        </div>
      ) : null}
      {conversations.map((conversation) => (
        <div className="conversation-row" key={conversation.workflow_id}>
          <button
            type="button"
            className={`conversation-item${
              conversation.workflow_id === currentWorkflowId ? " active" : ""
            }`}
            onClick={() => onSelect(conversation.workflow_id)}
          >
            {conversation.title || "New chat"}
          </button>
          <button
            type="button"
            className="conversation-delete"
            title="Delete chat"
            aria-label="Delete chat"
            onClick={(event) => {
              event.stopPropagation();
              onDelete(conversation.workflow_id);
            }}
          >
            <svg
              viewBox="0 0 24 24"
              width="16"
              height="16"
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
              strokeLinecap="round"
              strokeLinejoin="round"
              aria-hidden="true"
            >
              <rect width="20" height="5" x="2" y="3" rx="1" />
              <path d="M4 8v11a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8" />
              <path d="M10 12h4" />
            </svg>
          </button>
        </div>
      ))}
      {conversations.length === 0 && !draftConversation ? (
        <div className="tool-meta">No chats yet.</div>
      ) : null}
    </div>
  );
}
