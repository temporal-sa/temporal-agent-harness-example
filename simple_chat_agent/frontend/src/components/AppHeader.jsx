import { ConversationList } from "./ConversationList.jsx";
import {
  agentSettingsFromWorkflowState,
  defaultThinkingModeForModel,
  effortOptionsForModel,
  modelOptionsForSelection,
  thinkingModesForModel,
} from "../state/chatState.js";

export function AppHeader({
  state,
  status,
  onNewChat,
  onOpenTools,
  onLogout,
  onUpdateAgentSettings,
  onUpdateThinkingBudget,
  onSelectConversation,
  onDeleteConversation,
}) {
  const settingsLocked = Boolean(state.workflowId && !state.draftConversation);
  const displayedSettings =
    settingsLocked && state.workflowState
      ? agentSettingsFromWorkflowState(state.workflowState, state.config || {})
      : state.agentSettings;
  const selectedModel = displayedSettings.model || state.config?.default_model || "";
  const modelOptions = modelOptionsForSelection(state.config, selectedModel);
  const baseThinkingModes = thinkingModesForModel(state.config, selectedModel);
  const thinkingModes =
    displayedSettings.thinkingMode && !baseThinkingModes.includes(displayedSettings.thinkingMode)
      ? [displayedSettings.thinkingMode, ...baseThinkingModes]
      : baseThinkingModes;
  const baseEffortOptions = effortOptionsForModel(state.config, selectedModel);
  const effortOptions =
    displayedSettings.thinkingEffort && !baseEffortOptions.includes(displayedSettings.thinkingEffort)
      ? [displayedSettings.thinkingEffort, ...baseEffortOptions]
      : baseEffortOptions;
  const adaptiveThinking =
    displayedSettings.thinkingMode === "adaptive" &&
    thinkingModes.includes("adaptive");

  return (
    <header>
      <div className="header-left">
        <h1 aria-label="Simple Chat Agent"></h1>
        {state.user?.temporal_ui_workflows_url ? (
          <a
            className="temporal-link"
            href={state.user.temporal_ui_workflows_url}
            target="_blank"
            rel="noreferrer"
          >
            Temporal UI
          </a>
        ) : null}
      </div>
      <div className="side-panel">
        <section className="side-section">
          <div className="side-actions">
            <button className="primary" type="button" onClick={onNewChat}>
              New Chat
            </button>
            <button type="button" onClick={onOpenTools}>
              Tools
            </button>
            <button type="button" onClick={onLogout}>
              Logout
            </button>
          </div>
        </section>
        <section className="side-section">
          <p className="side-section-title">Agent</p>
          <div className="agent-settings">
            <div className="agent-field">
              <label htmlFor="modelSelect">Model</label>
              <select
                id="modelSelect"
                value={selectedModel}
                disabled={settingsLocked}
                onChange={(event) =>
                  onUpdateAgentSettings({ model: event.currentTarget.value })
                }
              >
                {modelOptions.map((model) => (
                  <option key={model.id} value={model.id}>
                    {model.display_name || model.id}
                  </option>
                ))}
              </select>
            </div>
            <label className="agent-toggle" htmlFor="thinkingEnabled">
              <input
                id="thinkingEnabled"
                type="checkbox"
                checked={displayedSettings.thinkingEnabled}
                disabled={settingsLocked || !thinkingModes.length}
                onChange={(event) =>
                  onUpdateAgentSettings({
                    thinkingEnabled: event.currentTarget.checked,
                  })
                }
              />
              Extended thinking
            </label>
            <div
              className="agent-field"
              id="thinkingModeField"
              hidden={!displayedSettings.thinkingEnabled || thinkingModes.length <= 1}
            >
              <label htmlFor="thinkingMode">Mode</label>
              <select
                id="thinkingMode"
                disabled={settingsLocked}
                value={
                  displayedSettings.thinkingMode ||
                  defaultThinkingModeForModel(state.config, selectedModel)
                }
                onChange={(event) =>
                  onUpdateAgentSettings({ thinkingMode: event.currentTarget.value })
                }
              >
                {thinkingModes.map((mode) => (
                  <option key={mode} value={mode}>
                    {thinkingModeLabel(mode)}
                  </option>
                ))}
              </select>
            </div>
            <div
              className="agent-field"
              id="thinkingBudgetField"
              hidden={!displayedSettings.thinkingEnabled || adaptiveThinking}
            >
              <label htmlFor="thinkingBudget">Budget tokens</label>
              <input
                id="thinkingBudget"
                type="number"
                min={state.config?.thinking?.min_budget_tokens || 1024}
                step="1024"
                value={displayedSettings.thinkingBudgetTokens}
                disabled={settingsLocked}
                onChange={(event) => onUpdateThinkingBudget(event.currentTarget.value)}
              />
            </div>
            <div
              className="agent-field"
              id="thinkingEffortField"
              hidden={!displayedSettings.thinkingEnabled || !adaptiveThinking}
            >
              <label htmlFor="thinkingEffort">Effort</label>
              <select
                id="thinkingEffort"
                value={displayedSettings.thinkingEffort}
                disabled={settingsLocked}
                onChange={(event) =>
                  onUpdateAgentSettings({ thinkingEffort: event.currentTarget.value })
                }
              >
                {effortOptions.map((effort) => (
                  <option key={effort} value={effort}>
                    {effort}
                  </option>
                ))}
              </select>
            </div>
          </div>
        </section>
        <section className="side-section">
          <p className="side-section-title">Chats</p>
          <ConversationList
            conversations={state.conversations}
            currentWorkflowId={state.workflowId}
            draftConversation={state.draftConversation}
            onNewDraft={onNewChat}
            onSelect={onSelectConversation}
            onDelete={onDeleteConversation}
          />
        </section>
      </div>
      <div className="status">{status}</div>
    </header>
  );
}

function thinkingModeLabel(mode) {
  if (mode === "adaptive") return "Adaptive effort";
  if (mode === "enabled") return "Token budget";
  return mode;
}
