    const state = {
      user: null,
      config: null,
      agentSettings: {
        model: "",
        thinkingEnabled: false,
        thinkingBudgetTokens: 4096,
        thinkingEffort: "medium",
      },
      conversations: [],
      tools: [],
      workflowId: null,
      runId: null,
      temporalUiUrl: null,
      workflowState: null,
      eventSource: null,
      streamTurn: null,
      streamPanelCollapsed: false,
      currentClaudeSequence: null,
      ignoreClaudeUntilStart: false,
      localPending: [],
      resolvingApprovals: new Set(),
      lastAssistantCount: 0,
      recoveringMissingWorkflow: false,
      toolsWindowOpen: false,
      artifactViewer: {
        open: false,
        artifact: null,
        loading: false,
        error: "",
        text: "",
        objectUrl: "",
      },
      draftConversation: true,
      mcpFormOpen: false,
      mcpFormSubmitting: false,
      mcpFormError: "",
      mcpFormValues: {
        label: "",
        server_url: "",
        tool_prefix: "",
        auth_mode: "none",
        bearer_token: "",
      },
    };

    const appRootEl = document.getElementById("appRoot");
    const loginScreenEl = document.getElementById("loginScreen");
    const loginGoogleEl = document.getElementById("loginGoogle");
    const loginSubtitleEl = document.getElementById("loginSubtitle");
    const loginErrorEl = document.getElementById("loginError");
    const conversationListEl = document.getElementById("conversationList");
    const toolsOverlayEl = document.getElementById("toolsOverlay");
    const toolsWindowBodyEl = document.getElementById("toolsWindowBody");
    const artifactsSidebarEl = document.getElementById("artifactsSidebar");
    const artifactViewerOverlayEl = document.getElementById("artifactViewerOverlay");
    const messagesEl = document.getElementById("messages");
    const eventsEl = document.getElementById("events");
    const statusEl = document.getElementById("status");
    const temporalLinkEl = document.getElementById("temporalLink");
    const chatWorkflowLinkEl = document.getElementById("chatWorkflowLink");
    const inputEl = document.getElementById("message");
    const formEl = document.getElementById("composer");
    const modelSelectEl = document.getElementById("modelSelect");
    const thinkingEnabledEl = document.getElementById("thinkingEnabled");
    const thinkingBudgetEl = document.getElementById("thinkingBudget");
    const thinkingBudgetFieldEl = document.getElementById("thinkingBudgetField");
    const thinkingEffortEl = document.getElementById("thinkingEffort");
    const thinkingEffortFieldEl = document.getElementById("thinkingEffortField");

    boot().catch((err) => {
      statusEl.textContent = `failed: ${err}`;
    });

    async function boot() {
      const authenticated = await refreshUser();
      if (!authenticated) {
        showLogin();
        return;
      }

      showApp();
      await Promise.all([loadConfig(), loadTools(), loadConversations()]);

      const savedWorkflowId = localStorage.getItem("simpleChatWorkflowId");
      const savedConversation = state.conversations.find((conversation) => conversation.workflow_id === savedWorkflowId);
      const conversation = savedConversation || state.conversations[0];
      if (conversation) {
        selectConversation(conversation.workflow_id);
      } else {
        startDraftConversation();
      }
      showOAuthCallbackStatus();
    }

    async function refreshUser() {
      const response = await fetch("/api/me");
      if (response.status === 401) return false;
      if (!response.ok) throw new Error(await response.text());
      state.user = await response.json();
      applyUserTemporalLink();
      return true;
    }

    // The header "Temporal UI" link points at all of the signed-in user's
    // workflows (filtered by the UserEmail search attribute). The per-chat link
    // (bottom of the chat pane) points at the specific chat workflow.
    function applyUserTemporalLink() {
      const url = state.user?.temporal_ui_workflows_url;
      if (url) {
        temporalLinkEl.href = url;
        temporalLinkEl.style.display = "inline-flex";
      } else {
        temporalLinkEl.removeAttribute("href");
        temporalLinkEl.style.display = "none";
      }
    }

    function showLogin() {
      appRootEl.hidden = true;
      loginScreenEl.hidden = false;
      if (state.eventSource) state.eventSource.close();
      const params = new URLSearchParams(window.location.search);
      if (params.has("oauth_error")) {
        loginErrorEl.textContent = params.get("oauth_error");
        history.replaceState({}, "", "/");
      } else {
        loginErrorEl.textContent = "";
      }
      configureLoginButton();
    }

    async function configureLoginButton() {
      try {
        const response = await fetch("/api/auth/google/configured");
        if (!response.ok) throw new Error(await response.text());
        const body = await response.json();
        if (!body.configured) {
          loginGoogleEl.setAttribute("aria-disabled", "true");
          loginSubtitleEl.textContent = "Google OAuth is not configured. Set GOOGLE_OAUTH_CLIENT_ID and GOOGLE_OAUTH_CLIENT_SECRET.";
          return;
        }
        loginGoogleEl.removeAttribute("aria-disabled");
        loginSubtitleEl.textContent = "";
      } catch (err) {
        loginSubtitleEl.textContent = `Could not check auth config: ${err}`;
      }
    }

    function showApp() {
      loginScreenEl.hidden = true;
      appRootEl.hidden = false;
    }

    async function loadConversations() {
      const response = await fetch("/api/conversations");
      if (response.status === 401) {
        showLogin();
        return;
      }
      if (!response.ok) throw new Error(await response.text());
      const body = await response.json();
      state.conversations = body.conversations || [];
      renderSidebar();
    }

    async function loadTools() {
      const response = await fetch("/api/tools");
      if (response.status === 401) {
        showLogin();
        return;
      }
      if (!response.ok) throw new Error(await response.text());
      const body = await response.json();
      state.tools = body.tools || [];
      renderSidebar();
      renderToolsWindow();
    }

    async function loadConfig() {
      const response = await fetch("/api/config");
      if (response.status === 401) {
        showLogin();
        return;
      }
      if (!response.ok) throw new Error(await response.text());
      state.config = await response.json();
      const savedModel = localStorage.getItem("simpleChatModel");
      const modelOptions = state.config.model_options || [];
      state.agentSettings.model = (
        savedModel && modelOptions.includes(savedModel)
      ) ? savedModel : state.config.default_model;
      state.agentSettings.thinkingEnabled =
        localStorage.getItem("simpleChatThinkingEnabled") === "true";
      state.agentSettings.thinkingBudgetTokens = Number(
        localStorage.getItem("simpleChatThinkingBudgetTokens") ||
        state.config.thinking?.budget_tokens ||
        4096
      );
      const savedEffort = localStorage.getItem("simpleChatThinkingEffort");
      const effortOptions = state.config.thinking?.effort_options || ["medium"];
      state.agentSettings.thinkingEffort = (
        savedEffort && effortOptions.includes(savedEffort)
      ) ? savedEffort : state.config.thinking?.effort || "medium";
      renderAgentSettings();
    }

    function renderAgentSettings() {
      const config = state.config || {};
      const modelOptions = config.model_options || [];
      modelSelectEl.replaceChildren();
      for (const model of modelOptions) {
        const option = document.createElement("option");
        option.value = model;
        option.textContent = model;
        modelSelectEl.append(option);
      }
      modelSelectEl.value = state.agentSettings.model || config.default_model || "";
      thinkingEnabledEl.checked = state.agentSettings.thinkingEnabled;
      const effortOptions = config.thinking?.effort_options || ["medium"];
      thinkingEffortEl.replaceChildren();
      for (const effort of effortOptions) {
        const option = document.createElement("option");
        option.value = effort;
        option.textContent = effort;
        thinkingEffortEl.append(option);
      }
      thinkingEffortEl.value = state.agentSettings.thinkingEffort || config.thinking?.effort || "medium";
      const minBudget = config.thinking?.min_budget_tokens || 1024;
      thinkingBudgetEl.min = String(minBudget);
      thinkingBudgetEl.value = String(
        Math.max(minBudget, state.agentSettings.thinkingBudgetTokens || minBudget)
      );
      const adaptive = selectedModelUsesAdaptiveThinking();
      thinkingBudgetFieldEl.hidden = !state.agentSettings.thinkingEnabled || adaptive;
      thinkingEffortFieldEl.hidden = !state.agentSettings.thinkingEnabled || !adaptive;
    }

    function newConversationRequest() {
      const config = state.config || {};
      const minBudget = config.thinking?.min_budget_tokens || 1024;
      const budgetTokens = Math.max(
        minBudget,
        Number(
          thinkingBudgetEl.value ||
          state.agentSettings.thinkingBudgetTokens ||
          config.thinking?.budget_tokens ||
          4096
        ),
      );
      state.agentSettings.model =
        modelSelectEl.value || state.agentSettings.model || config.default_model;
      state.agentSettings.thinkingEnabled = thinkingEnabledEl.checked;
      state.agentSettings.thinkingBudgetTokens = budgetTokens;
      state.agentSettings.thinkingEffort = thinkingEffortEl.value || state.agentSettings.thinkingEffort || "medium";
      saveAgentSettings();
      return {
        model: state.agentSettings.model,
        thinking: {
          enabled: state.agentSettings.thinkingEnabled,
          budget_tokens: budgetTokens,
          effort: state.agentSettings.thinkingEffort,
        },
      };
    }

    function saveAgentSettings() {
      localStorage.setItem("simpleChatModel", state.agentSettings.model);
      localStorage.setItem(
        "simpleChatThinkingEnabled",
        String(state.agentSettings.thinkingEnabled),
      );
      localStorage.setItem(
        "simpleChatThinkingBudgetTokens",
        String(state.agentSettings.thinkingBudgetTokens),
      );
      localStorage.setItem(
        "simpleChatThinkingEffort",
        state.agentSettings.thinkingEffort,
      );
    }

    function selectedModelUsesAdaptiveThinking() {
      const model = modelSelectEl.value || state.agentSettings.model || "";
      return (state.config?.thinking?.adaptive_model_prefixes || []).some((prefix) => (
        model.startsWith(prefix)
      ));
    }

    function startDraftConversation() {
      if (state.eventSource) {
        state.eventSource.close();
        state.eventSource = null;
      }
      state.workflowId = null;
      state.runId = null;
      state.temporalUiUrl = null;
      state.workflowState = null;
      state.streamTurn = null;
      state.streamPanelCollapsed = false;
      state.currentClaudeSequence = null;
      state.ignoreClaudeUntilStart = false;
      state.localPending = [];
      state.resolvingApprovals.clear();
      state.draftConversation = true;
      closeArtifactViewer();
      localStorage.removeItem("simpleChatWorkflowId");
      chatWorkflowLinkEl.style.display = "none";
      chatWorkflowLinkEl.removeAttribute("href");
      renderSidebar();
      render();
    }

    async function createConversation(initialMessage = null, options = {}) {
      const response = await fetch("/api/sessions", {
        method: "POST",
        headers: jsonHeaders(),
        body: JSON.stringify({
          ...newConversationRequest(),
          initial_message: initialMessage,
        }),
      });
      if (response.status === 401) {
        showLogin();
        return;
      }
      if (!response.ok) throw new Error(await response.text());
      const body = await response.json();
      await loadConversations();
      selectConversation(body.workflow_id, options);
      return body;
    }

    function selectConversation(workflowId, options = {}) {
      const conversation = state.conversations.find((item) => item.workflow_id === workflowId);
      if (!conversation) return;
      if (state.eventSource) state.eventSource.close();
      state.workflowId = conversation.workflow_id;
      state.runId = conversation.run_id;
      state.temporalUiUrl = temporalUiUrl(conversation);
      state.workflowState = null;
      state.streamTurn = null;
      state.streamPanelCollapsed = false;
      state.currentClaudeSequence = null;
      state.ignoreClaudeUntilStart = false;
      state.draftConversation = false;
      state.resolvingApprovals.clear();
      if (!options.preserveLocalPending) {
        state.localPending = [];
      }
      closeArtifactViewer();
      localStorage.setItem("simpleChatWorkflowId", state.workflowId);
      if (state.temporalUiUrl) {
        chatWorkflowLinkEl.href = state.temporalUiUrl;
        chatWorkflowLinkEl.style.display = "inline-flex";
      } else {
        chatWorkflowLinkEl.style.display = "none";
      }
      renderSidebar();
      render();
      connectEvents();
    }

    function connectEvents() {
      if (!state.workflowId) return;
      state.eventSource = new EventSource(`/api/sessions/${state.workflowId}/events`);
      state.eventSource.addEventListener("state", (event) => {
        const nextState = JSON.parse(event.data);
        updateWorkflowState(nextState);
      });
      state.eventSource.addEventListener("stream", (event) => {
        handleStreamEvent(JSON.parse(event.data));
      });
      state.eventSource.addEventListener("missing", async () => {
        await handleMissingWorkflow();
      });
      state.eventSource.addEventListener("error", () => {
        statusEl.textContent = "event stream reconnecting...";
      });
    }

    document.getElementById("newChat").addEventListener("click", () => startDraftConversation());
    modelSelectEl.addEventListener("change", () => {
      state.agentSettings.model = modelSelectEl.value;
      saveAgentSettings();
      renderAgentSettings();
    });
    thinkingEnabledEl.addEventListener("change", () => {
      state.agentSettings.thinkingEnabled = thinkingEnabledEl.checked;
      saveAgentSettings();
      renderAgentSettings();
    });
    function updateThinkingBudget() {
      const minBudget = state.config?.thinking?.min_budget_tokens || 1024;
      state.agentSettings.thinkingBudgetTokens = Math.max(
        minBudget,
        Number(thinkingBudgetEl.value || minBudget),
      );
      saveAgentSettings();
      renderAgentSettings();
    }
    thinkingBudgetEl.addEventListener("change", updateThinkingBudget);
    thinkingEffortEl.addEventListener("change", () => {
      state.agentSettings.thinkingEffort = thinkingEffortEl.value;
      saveAgentSettings();
    });
    document.getElementById("toolsButton").addEventListener("click", () => {
      state.toolsWindowOpen = true;
      renderToolsWindow();
    });
    document.getElementById("closeTools").addEventListener("click", () => {
      state.toolsWindowOpen = false;
      state.mcpFormOpen = false;
      state.mcpFormError = "";
      renderToolsWindow();
    });
    toolsOverlayEl.addEventListener("click", (event) => {
      if (event.target !== toolsOverlayEl) return;
      state.toolsWindowOpen = false;
      state.mcpFormOpen = false;
      state.mcpFormError = "";
      renderToolsWindow();
    });
    artifactViewerOverlayEl.addEventListener("click", (event) => {
      if (event.target !== artifactViewerOverlayEl) return;
      closeArtifactViewer();
    });
    document.getElementById("logout").addEventListener("click", async () => {
      await post("/api/logout", {});
      localStorage.removeItem("simpleChatWorkflowId");
      state.user = null;
      state.conversations = [];
      state.workflowId = null;
      state.workflowState = null;
      state.draftConversation = true;
      closeArtifactViewer();
      state.toolsWindowOpen = false;
      state.mcpFormOpen = false;
      showLogin();
    });
    formEl.addEventListener("submit", (event) => {
      event.preventDefault();
      sendDefault();
    });
    document.getElementById("interrupt").addEventListener("click", () => sendAction("interrupt", "you interrupt", "sending"));
    loginGoogleEl.addEventListener("click", (event) => {
      event.preventDefault();
      if (loginGoogleEl.getAttribute("aria-disabled") === "true") return;
      const href = loginGoogleEl.getAttribute("href");
      // Hide the button and play the Ziggy "flying in" animation once, then
      // redirect (6 frames x 90ms = 540ms + a brief hold on the last frame).
      loginGoogleEl.closest(".login-card").classList.add("logging-in");
      setTimeout(() => {
        window.location.href = href;
      }, 750);
    });
    inputEl.addEventListener("keydown", (event) => {
      if (event.key === "Enter" && !event.shiftKey) {
        event.preventDefault();
        sendDefault();
      }
    });

    function sendDefault() {
      const busy = state.workflowState?.status === "responding";
      sendAction(busy ? "steer" : "chat", busy ? "you steering" : "you", "sending");
    }

    async function sendAction(action, label, phase) {
      let message = inputEl.value.trim();
      if (!message && action === "interrupt") {
        message = "Stop the current response.";
      }
      if (!message) return;
      if (!state.workflowId) {
        if (action === "interrupt" || action === "steer" || action === "after-tool") return;
        inputEl.value = "";
        const pending = { id: crypto.randomUUID(), label, content: message, phase };
        state.localPending.push(pending);
        render();
        try {
          await createConversation(message, { preserveLocalPending: true });
        } catch (err) {
          pending.phase = `failed: ${err}`;
          render();
        }
        return;
      }
      inputEl.value = "";
      const pending = { id: crypto.randomUUID(), label, content: message, phase };
      state.localPending.push(pending);
      if (action === "interrupt") {
        markStreamInterrupted();
        state.ignoreClaudeUntilStart = true;
      }
      render();

      try {
        if (action === "chat") {
          await post(`/api/sessions/${state.workflowId}/chat`, { message });
        } else if (action === "steer") {
          await post(`/api/sessions/${state.workflowId}/steer`, { message, mode: "immediate" });
        } else if (action === "after-tool") {
          await post(`/api/sessions/${state.workflowId}/steer`, { message, mode: "after_next_tool_result" });
        } else if (action === "interrupt") {
          await post(`/api/sessions/${state.workflowId}/interrupt`, { message });
        }
        await loadConversations();
      } catch (err) {
        pending.phase = `failed: ${err}`;
        render();
      }
    }

    async function post(url, payload) {
      const response = await fetch(url, { method: "POST", headers: jsonHeaders(), body: JSON.stringify(payload) });
      if (response.status === 401) {
        showLogin();
      }
      if (response.status === 404) {
        await handleMissingWorkflow();
      }
      if (!response.ok) throw new Error(await responseErrorText(response));
      const contentType = response.headers.get("content-type") || "";
      if (contentType.includes("application/json")) return await response.json();
      return {};
    }

    async function responseErrorText(response) {
      const text = await response.text();
      try {
        const body = JSON.parse(text);
        if (typeof body.detail === "string") return body.detail;
        if (body.detail) return JSON.stringify(body.detail);
      } catch (_err) {
      }
      return text || `${response.status} ${response.statusText}`;
    }

    async function handleMissingWorkflow() {
      if (state.recoveringMissingWorkflow) return;
      state.recoveringMissingWorkflow = true;
      const missingWorkflowId = state.workflowId;
      try {
        if (state.eventSource) {
          state.eventSource.close();
          state.eventSource = null;
        }
        if (missingWorkflowId) {
          statusEl.textContent = "Workflow no longer exists; selecting a live chat...";
          if (localStorage.getItem("simpleChatWorkflowId") === missingWorkflowId) {
            localStorage.removeItem("simpleChatWorkflowId");
          }
        }

        await loadConversations();
        const nextConversation = state.conversations[0];
        if (nextConversation) {
          selectConversation(nextConversation.workflow_id);
        } else {
          startDraftConversation();
        }
      } finally {
        state.recoveringMissingWorkflow = false;
      }
    }

    async function deleteConversation(workflowId) {
      if (!confirm("Delete this chat?")) return;
      const response = await fetch(`/api/sessions/${workflowId}`, { method: "DELETE" });
      if (response.status === 401) {
        showLogin();
        return;
      }
      if (!response.ok) throw new Error(await response.text());

      if (state.workflowId === workflowId) {
        if (state.eventSource) {
          state.eventSource.close();
          state.eventSource = null;
        }
        if (localStorage.getItem("simpleChatWorkflowId") === workflowId) {
          localStorage.removeItem("simpleChatWorkflowId");
        }
        state.workflowId = null;
        state.workflowState = null;
        state.streamTurn = null;
        state.localPending = [];
        closeArtifactViewer();
      }

      await loadConversations();
      if (!state.workflowId) {
        const nextConversation = state.conversations[0];
        if (nextConversation) {
          selectConversation(nextConversation.workflow_id);
        } else {
          startDraftConversation();
        }
      }
    }

    async function resolveApproval(approvalId, decision) {
      // Guard against double-submits: the approval card stays on screen until the
      // workflow state refreshes (which the resolution itself triggers), so a fast
      // second click would otherwise re-POST an already-resolved approval and the
      // workflow would log "Approval is no longer pending" for each extra click.
      // Mark it resolving, hide the card optimistically, and only restore on error.
      if (state.resolvingApprovals.has(approvalId)) return;
      state.resolvingApprovals.add(approvalId);
      render();
      try {
        await post(`/api/sessions/${state.workflowId}/approvals/${approvalId}`, { decision });
      } catch (err) {
        state.resolvingApprovals.delete(approvalId);
        statusEl.textContent = `approval failed: ${err}`;
        render();
      }
    }

    function renderSidebar() {
      const conversationFragment = document.createDocumentFragment();
      if (state.draftConversation) {
        const row = document.createElement("div");
        row.className = "conversation-row";
        const button = document.createElement("button");
        button.type = "button";
        button.className = "conversation-item active";
        button.textContent = "New chat";
        button.addEventListener("click", () => startDraftConversation());
        row.append(button);
        conversationFragment.append(row);
      }
      for (const conversation of state.conversations) {
        const row = document.createElement("div");
        row.className = "conversation-row";
        const button = document.createElement("button");
        button.type = "button";
        button.className = `conversation-item${conversation.workflow_id === state.workflowId ? " active" : ""}`;
        button.textContent = conversation.title || "New chat";
        button.addEventListener("click", () => selectConversation(conversation.workflow_id));

        const deleteButton = document.createElement("button");
        deleteButton.type = "button";
        deleteButton.className = "conversation-delete";
        deleteButton.innerHTML =
          '<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><rect width="20" height="5" x="2" y="3" rx="1"/><path d="M4 8v11a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8"/><path d="M10 12h4"/></svg>';
        deleteButton.title = "Delete chat";
        deleteButton.setAttribute("aria-label", "Delete chat");
        deleteButton.addEventListener("click", (event) => {
          event.stopPropagation();
          deleteConversation(conversation.workflow_id).catch((err) => {
            statusEl.textContent = `delete failed: ${err}`;
          });
        });

        row.append(button, deleteButton);
        conversationFragment.append(row);
      }
      if (state.conversations.length === 0 && !state.draftConversation) {
        const empty = document.createElement("div");
        empty.className = "tool-meta";
        empty.textContent = "No chats yet.";
        conversationFragment.append(empty);
      }
      conversationListEl.replaceChildren(conversationFragment);
    }

    function renderApprovalsPanel() {
      // Hide approvals that are mid-resolution so the card clears the instant the
      // user clicks, rather than lingering until the next workflow-state refresh.
      const approvals = (state.workflowState?.pending_approvals || []).filter(
        (approval) => !state.resolvingApprovals.has(approval.approval_id),
      );
      if (approvals.length === 0) return null;

      const panel = document.createElement("section");
      panel.className = "approval-panel";

      const header = document.createElement("div");
      header.className = "approval-panel-header";
      const title = document.createElement("span");
      title.textContent = "Approval Required";
      const count = document.createElement("span");
      count.className = "approval-panel-count";
      count.textContent = `${approvals.length} pending`;
      header.append(title, count);
      panel.append(header);

      for (const approval of approvals) {
        panel.append(renderApprovalCard(approval));
      }

      return panel;
    }

    function renderApprovalCard(approval) {
      const card = document.createElement("div");
      card.className = "approval-card";

      const title = document.createElement("div");
      title.className = "approval-title";
      title.textContent = approval.summary || approval.tool_name;
      card.append(title);

      const meta = document.createElement("div");
      meta.className = "approval-meta";
      meta.append(
        approvalMetaRow("Tool", approval.tool_name),
        approvalMetaRow("Scope", approval.memory_key || "one time"),
      );
      card.append(meta);

      const details = document.createElement("div");
      details.className = "approval-details bubble-content";
      renderApprovalArgs(details, approval.tool_args || {});
      card.append(details);

      const actions = document.createElement("div");
      actions.className = "approval-actions";
      actions.append(
        approvalButton("Allow", approval.approval_id, "allow", "allow"),
        approvalButton("Always Allow", approval.approval_id, "always_allow", "always"),
        approvalButton("Deny", approval.approval_id, "deny", "deny"),
      );
      card.append(actions);

      return card;
    }

    function approvalMetaRow(label, value) {
      const row = document.createElement("div");
      const labelNode = document.createElement("strong");
      labelNode.textContent = `${label}: `;
      row.append(labelNode, document.createTextNode(value || "unknown"));
      return row;
    }

    function renderApprovalArgs(container, args) {
      if (typeof args.code === "string") {
        container.append(createCodeBlock(args.code, "python"));
        const rest = { ...args };
        delete rest.code;
        if (Object.keys(rest).length > 0) {
          container.append(createCodeBlock(JSON.stringify(rest, null, 2), "json"));
        }
        return;
      }

      if (typeof args.content === "string" && typeof args.name === "string") {
        const metadata = { ...args };
        delete metadata.content;
        container.append(createCodeBlock(JSON.stringify(metadata, null, 2), "json"));
        const truncated = args.content.length > 12000;
        const preview = truncated
          ? `${args.content.slice(0, 12000)}\n...[truncated for approval preview]`
          : args.content;
        container.append(createCodeBlock(preview, languageFromFileName(args.name)));
        return;
      }

      container.append(createCodeBlock(JSON.stringify(args, null, 2), "json"));
    }

    function renderToolsWindow() {
      toolsOverlayEl.hidden = !state.toolsWindowOpen;
      if (!state.toolsWindowOpen) {
        toolsWindowBodyEl.replaceChildren();
        return;
      }

      const fragment = document.createDocumentFragment();
      const builtInTools = state.tools.filter((tool) => !tool.provider?.startsWith("mcp:"));
      const mcpTools = state.tools.filter((tool) => tool.provider?.startsWith("mcp:"));

      fragment.append(renderBuiltInToolsSection(builtInTools));
      fragment.append(renderMcpToolsSection(mcpTools));
      toolsWindowBodyEl.replaceChildren(fragment);
    }

    function renderBuiltInToolsSection(tools) {
      const section = document.createElement("section");
      section.className = "tools-section";
      section.append(toolsSectionHeader("Built-in tools"));

      const grid = document.createElement("div");
      grid.className = "tools-grid";
      for (const tool of tools) {
        grid.append(renderBuiltInToolCard(tool));
      }
      section.append(grid);
      return section;
    }

    function renderMcpToolsSection(tools) {
      const section = document.createElement("section");
      section.className = "tools-section";

      const actions = document.createElement("div");
      actions.className = "tools-section-actions";
      const addMcpButton = document.createElement("button");
      addMcpButton.type = "button";
      addMcpButton.textContent = "Add HTTP MCP";
      addMcpButton.addEventListener("click", () => {
        state.mcpFormOpen = true;
        state.mcpFormError = "";
        renderToolsWindow();
      });
      actions.append(addMcpButton);
      section.append(toolsSectionHeader("MCP servers", actions));

      if (state.mcpFormOpen) {
        section.append(renderMcpForm());
      }

      const grid = document.createElement("div");
      grid.className = "tools-grid";
      for (const tool of tools) {
        grid.append(renderMcpToolCard(tool));
      }
      if (tools.length === 0) {
        const empty = document.createElement("div");
        empty.className = "tool-meta";
        empty.textContent = "No MCP servers connected.";
        grid.append(empty);
      }
      section.append(grid);
      return section;
    }

    function toolsSectionHeader(titleText, actions = null) {
      const header = document.createElement("div");
      header.className = "tools-section-header";
      const title = document.createElement("div");
      title.className = "tools-section-title";
      title.textContent = titleText;
      header.append(title);
      if (actions) header.append(actions);
      return header;
    }

    function renderBuiltInToolCard(tool) {
      const card = baseToolCard(tool, {
        status: tool.connected ? "Connected" : "Disconnected",
        connected: Boolean(tool.connected),
        disabled: false,
      });

      if (tool.provider === "github") {
        const actions = document.createElement("div");
        actions.className = "tool-actions";
        const action = document.createElement("button");
        action.type = "button";
        action.textContent = tool.connected ? "Disconnect" : "Connect";
        action.disabled = !tool.configured;
        action.addEventListener("click", async () => {
          if (tool.connected) {
            await post("/api/tools/github/disconnect", {});
            statusEl.textContent = "GitHub disconnected";
            await loadTools();
          } else {
            window.location.href = "/oauth/github/start";
          }
        });
        actions.append(action);
        card.append(actions);
      }

      return card;
    }

    function renderMcpToolCard(tool) {
      const connected = Boolean(tool.connected);
      const enabled = Boolean(tool.enabled);
      const card = baseToolCard(tool, {
        status: connected ? (enabled ? "Enabled" : "Disabled") : "Disconnected",
        connected: connected && enabled,
        disabled: !enabled,
      });

      const actions = document.createElement("div");
      actions.className = "tool-actions";
      const toggle = document.createElement("button");
      toggle.type = "button";
      toggle.textContent = enabled ? "Disable" : "Enable";
      toggle.addEventListener("click", async () => {
        await setMcpServerEnabled(tool, !enabled);
      });
      if (tool.auth_mode === "oauth") {
        const reconnect = document.createElement("button");
        reconnect.type = "button";
        reconnect.textContent = "Reconnect";
        reconnect.addEventListener("click", () => {
          window.location.href = mcpOAuthStartUrl({
            label: tool.label,
            serverUrl: tool.server_url || tool.login || "",
            toolPrefix: tool.tool_prefix || "",
            serverId: tool.server_id || tool.provider.slice("mcp:".length),
          });
        });
        actions.append(reconnect);
      }
      const remove = document.createElement("button");
      remove.type = "button";
      remove.className = "danger";
      remove.textContent = "Delete";
      remove.addEventListener("click", async () => {
        await deleteMcpServer(tool);
      });
      actions.append(toggle, remove);
      card.append(actions);
      return card;
    }

    function baseToolCard(tool, { status, connected, disabled }) {
      const card = document.createElement("div");
      card.className = `tool-card${connected ? " connected" : ""}${disabled ? " disabled" : ""}`;

      const title = document.createElement("div");
      title.className = "tool-title";
      const label = document.createElement("span");
      label.className = "tool-label";
      label.textContent = tool.label;
      const statusNode = document.createElement("span");
      statusNode.className = "tool-status";
      statusNode.textContent = status;
      title.append(label, statusNode);
      card.append(title);

      const meta = document.createElement("div");
      meta.className = "tool-meta";
      if (!tool.configured) {
        meta.textContent = "Set GITHUB_OAUTH_CLIENT_ID and GITHUB_OAUTH_CLIENT_SECRET.";
      } else if (tool.provider?.startsWith("mcp:")) {
        meta.textContent = `${tool.login || "HTTP MCP"} | ${tool.available_tools?.length || 0} tools | ${tool.scopes}`;
      } else if (tool.connected && tool.login) {
        meta.textContent = `@${tool.login} | ${tool.scopes || "no scopes returned"}`;
      } else {
        meta.textContent = `Scopes: ${tool.scopes || "none"}`;
      }
      card.append(meta);

      if (tool.available_tools?.length) {
        const chips = document.createElement("div");
        chips.className = "tool-chip-list";
        for (const toolName of tool.available_tools.slice(0, 8)) {
          const chip = document.createElement("span");
          chip.className = "tool-chip";
          chip.textContent = toolName;
          chips.append(chip);
        }
        if (tool.available_tools.length > 8) {
          const chip = document.createElement("span");
          chip.className = "tool-chip";
          chip.textContent = `+${tool.available_tools.length - 8}`;
          chips.append(chip);
        }
        card.append(chips);
      }

      return card;
    }

    async function setMcpServerEnabled(tool, enabled) {
      const serverId = tool.provider.slice("mcp:".length);
      await post(`/api/mcp-servers/${encodeURIComponent(serverId)}/enabled`, { enabled });
      statusEl.textContent = `${tool.label} ${enabled ? "enabled" : "disabled"}`;
      await loadTools();
    }

    async function deleteMcpServer(tool) {
      if (!confirm(`Delete ${tool.label}?`)) return;
      const serverId = tool.provider.slice("mcp:".length);
      const response = await fetch(`/api/mcp-servers/${encodeURIComponent(serverId)}`, { method: "DELETE" });
      if (response.status === 401) {
        showLogin();
        return;
      }
      if (!response.ok) throw new Error(await responseErrorText(response));
      statusEl.textContent = `${tool.label} deleted`;
      await loadTools();
    }

    function renderMcpForm() {
      const values = state.mcpFormValues;
      const form = document.createElement("form");
      form.className = "mcp-form";
      form.append(
        mcpField("Label", "label", "Temporal docs", "text", values.label),
        mcpField("HTTP URL", "server_url", "https://example.com/mcp", "text", values.server_url),
        mcpField("Tool prefix", "tool_prefix", "temporal", "text", values.tool_prefix),
        mcpAuthField(values.auth_mode),
        mcpField("Bearer token", "bearer_token", "", "password", values.bearer_token),
      );

      const bearerField = form.querySelector('[data-field="bearer_token"]');
      const authMode = form.querySelector('[name="auth_mode"]');
      bearerField.hidden = authMode.value !== "bearer";
      authMode.addEventListener("change", () => {
        bearerField.hidden = authMode.value !== "bearer";
      });

      const labelInput = form.querySelector('[name="label"]');
      const prefixInput = form.querySelector('[name="tool_prefix"]');
      let prefixTouched = false;
      prefixInput.addEventListener("input", () => {
        prefixTouched = true;
      });
      labelInput.addEventListener("input", () => {
        if (!prefixTouched) prefixInput.value = toolPrefixFromLabel(labelInput.value);
      });

      if (state.mcpFormError) {
        const error = document.createElement("div");
        error.className = "mcp-error";
        error.textContent = state.mcpFormError;
        form.append(error);
      }

      const actions = document.createElement("div");
      actions.className = "mcp-form-actions";
      const submit = document.createElement("button");
      submit.type = "submit";
      submit.className = "primary";
      submit.textContent = state.mcpFormSubmitting ? "Adding..." : "Add";
      submit.disabled = state.mcpFormSubmitting;
      const cancel = document.createElement("button");
      cancel.type = "button";
      cancel.textContent = "Cancel";
      cancel.disabled = state.mcpFormSubmitting;
      cancel.addEventListener("click", () => {
        state.mcpFormOpen = false;
        state.mcpFormError = "";
        resetMcpFormValues();
        renderToolsWindow();
      });
      actions.append(submit, cancel);
      form.append(actions);

      form.addEventListener("submit", (event) => {
        event.preventDefault();
        addHttpMcpServer(form).catch((err) => {
          state.mcpFormError = String(err);
          state.mcpFormSubmitting = false;
          renderToolsWindow();
        });
      });

      return form;
    }

    function mcpField(label, name, placeholder, type = "text", value = "") {
      const field = document.createElement("div");
      field.className = "mcp-field";
      field.dataset.field = name;
      const labelNode = document.createElement("label");
      labelNode.textContent = label;
      const input = document.createElement("input");
      input.name = name;
      input.type = type;
      input.placeholder = placeholder;
      input.value = value;
      input.required = name !== "bearer_token";
      field.append(labelNode, input);
      return field;
    }

    function mcpAuthField(value = "none") {
      const field = document.createElement("div");
      field.className = "mcp-field";
      const labelNode = document.createElement("label");
      labelNode.textContent = "Auth";
      const select = document.createElement("select");
      select.name = "auth_mode";
      const none = document.createElement("option");
      none.value = "none";
      none.textContent = "No auth";
      const oauth = document.createElement("option");
      oauth.value = "oauth";
      oauth.textContent = "OAuth authorization";
      const bearer = document.createElement("option");
      bearer.value = "bearer";
      bearer.textContent = "Bearer token";
      select.append(none, oauth, bearer);
      select.value = value;
      field.append(labelNode, select);
      return field;
    }

    async function addHttpMcpServer(form) {
      const formData = new FormData(form);
      const label = String(formData.get("label") || "").trim();
      const serverUrl = String(formData.get("server_url") || "").trim();
      const toolPrefix = String(formData.get("tool_prefix") || "").trim();
      const authMode = String(formData.get("auth_mode") || "none");
      const bearerToken = String(formData.get("bearer_token") || "").trim();
      state.mcpFormValues = {
        label,
        server_url: serverUrl,
        tool_prefix: toolPrefix,
        auth_mode: authMode,
        bearer_token: bearerToken,
      };

      if (authMode === "oauth") {
        window.location.href = mcpOAuthStartUrl({
          label,
          serverUrl,
          toolPrefix,
        });
        return;
      }

      state.mcpFormSubmitting = true;
      state.mcpFormError = "";
      renderToolsWindow();

      const body = await post("/api/mcp-servers", {
        label,
        server_url: serverUrl,
        tool_prefix: toolPrefix,
        auth_mode: authMode,
        bearer_token: authMode === "bearer" ? bearerToken : null,
      });
      state.mcpFormOpen = false;
      state.mcpFormSubmitting = false;
      state.mcpFormError = "";
      resetMcpFormValues();
      statusEl.textContent = `Added MCP server: ${body.server?.label || label}`;
      await loadTools();
    }

    function mcpOAuthStartUrl({ label, serverUrl, toolPrefix, serverId = "" }) {
      const params = new URLSearchParams({
        label,
        server_url: serverUrl,
        tool_prefix: toolPrefix,
      });
      if (serverId) params.set("server_id", serverId);
      return `/api/mcp-servers/oauth/start?${params.toString()}`;
    }

    function resetMcpFormValues() {
      state.mcpFormValues = {
        label: "",
        server_url: "",
        tool_prefix: "",
        auth_mode: "none",
        bearer_token: "",
      };
    }

    function toolPrefixFromLabel(label) {
      return label.toLowerCase().replace(/[^a-z0-9_-]+/g, "_").replace(/^_+|_+$/g, "") || "mcp";
    }

    function approvalButton(label, approvalId, decision, className = "") {
      const button = document.createElement("button");
      button.type = "button";
      button.textContent = label;
      if (className) button.className = className;
      button.addEventListener("click", () => resolveApproval(approvalId, decision));
      return button;
    }

    function temporalUiUrl(conversation) {
      if (conversation.temporal_ui_url) return conversation.temporal_ui_url;
      const workflow = encodeURIComponent(conversation.workflow_id);
      const run = encodeURIComponent(conversation.run_id || "");
      if (run) {
        return `http://localhost:8233/namespaces/default/workflows/${workflow}/${run}/history`;
      }
      return `http://localhost:8233/namespaces/default/workflows/${workflow}`;
    }

    function showOAuthCallbackStatus() {
      const params = new URLSearchParams(window.location.search);
      if (params.has("oauth_error")) {
        statusEl.textContent = `OAuth failed: ${params.get("oauth_error")}`;
      } else if (params.has("github")) {
        statusEl.textContent = "GitHub connected";
      } else if (params.has("mcp")) {
        statusEl.textContent = "MCP server connected";
        loadTools().catch((err) => {
          statusEl.textContent = `tool refresh failed: ${err}`;
        });
      }
      if (params.has("oauth_error") || params.has("github") || params.has("mcp")) {
        history.replaceState({}, "", "/");
      }
    }

    function updateWorkflowState(nextState) {
      const previousAssistantCount = state.workflowState
        ? state.workflowState.transcript.filter((m) => m.role === "assistant").length
        : 0;
      const nextAssistantCount = nextState.transcript.filter((m) => m.role === "assistant").length;
      state.workflowState = nextState;
      state.localPending = state.localPending.filter((pending) => !isAcknowledged(pending, nextState));
      // Once the workflow confirms an approval is no longer pending, stop tracking
      // it as resolving (it has left the panel for good).
      const pendingApprovalIds = new Set(
        (nextState.pending_approvals || []).map((approval) => approval.approval_id),
      );
      for (const approvalId of state.resolvingApprovals) {
        if (!pendingApprovalIds.has(approvalId)) state.resolvingApprovals.delete(approvalId);
      }
      if (nextAssistantCount > previousAssistantCount) markStreamCommitted();
      render();
    }

    function handleStreamEvent(event) {
      const sequence = event.payload?.sequence ?? null;
      if (event.kind === "claude_start") {
        state.currentClaudeSequence = sequence;
        state.ignoreClaudeUntilStart = false;
        if (isOpenStreamTurn(state.streamTurn)) {
          registerStreamSequence(state.streamTurn, sequence);
          state.streamTurn.status = "streaming";
          state.streamTurn.activeSequence = sequence;
        }
      } else if (event.kind === "claude_text_delta" && event.payload?.text) {
        if (state.ignoreClaudeUntilStart) return;
        if (sequence !== state.currentClaudeSequence) return;
        const turn = ensureStreamTurn(sequence);
        turn.status = "streaming";
        turn.text += event.payload.text;
      } else if (event.kind === "claude_thinking_start") {
        if (state.ignoreClaudeUntilStart) return;
        if (sequence !== state.currentClaudeSequence) return;
        const turn = ensureStreamTurn(sequence);
        turn.status = "streaming";
      } else if (event.kind === "claude_thinking_delta" && event.payload?.thinking) {
        if (state.ignoreClaudeUntilStart) return;
        if (sequence !== state.currentClaudeSequence) return;
        const turn = ensureStreamTurn(sequence);
        turn.status = "streaming";
        turn.thinking += event.payload.thinking;
      } else if (event.kind === "claude_cancelled") {
        if (sequence === state.currentClaudeSequence) {
          markStreamInterrupted();
          state.ignoreClaudeUntilStart = true;
        }
      } else if (event.kind === "claude_complete") {
        if (sequence !== state.currentClaudeSequence) return;
        const turn = ensureStreamTurn(sequence);
        if (turn) {
          finishStreamClaudeTurn(turn, event.payload || {});
          turn.status = turn.currentEvents.length ? "tooling" : "waiting";
          turn.lastClaudeCompletedAt = new Date().toISOString();
        }
      } else if (isClaudeToolEvent(event)) {
        const turn = ensureStreamTurn(state.currentClaudeSequence);
        appendStreamToolEvent(turn, event);
        if (turn.status !== "complete" && turn.status !== "interrupted") {
          turn.status = "tooling";
        }
      } else if (!event.kind?.startsWith("claude_")) {
        const turn = ensureStreamTurn(state.currentClaudeSequence);
        appendStreamToolEvent(turn, event);
        if (turn.status !== "complete" && turn.status !== "interrupted") {
          turn.status = "tooling";
        }
      }
      render();
    }

    function ensureStreamTurn(sequence) {
      if (!isOpenStreamTurn(state.streamTurn)) {
        state.streamTurn = createStreamTurn(sequence);
      } else {
        registerStreamSequence(state.streamTurn, sequence);
      }
      return state.streamTurn;
    }

    function streamTurnForSequence(sequence) {
      if (!isOpenStreamTurn(state.streamTurn)) return null;
      if (sequence === null) return state.streamTurn;
      return state.streamTurn.sequences.includes(sequence) ? state.streamTurn : null;
    }

    function isOpenStreamTurn(turn) {
      return Boolean(
        turn &&
        turn.status !== "complete" &&
        turn.status !== "interrupted"
      );
    }

    function registerStreamSequence(turn, sequence) {
      if (sequence !== null && !turn.sequences.includes(sequence)) {
        turn.sequences.push(sequence);
      }
    }

    function createStreamTurn(sequence) {
      return {
        sequence,
        sequences: sequence === null ? [] : [sequence],
        activeSequence: sequence,
        status: "streaming",
        text: "",
        thinking: "",
        finishedTurns: [],
        currentEvents: [],
        startedAt: new Date().toISOString(),
        completedAt: null,
        lastClaudeCompletedAt: null,
        interrupted: false,
      };
    }

    function finishStreamClaudeTurn(turn, payload) {
      const text = String(payload.text || turn.text || "").trim();
      const stopReason = payload.stop_reason || "unknown";
      const sequence = payload.sequence ?? turn.activeSequence;
      turn.finishedTurns.push({
        sequence,
        text,
        thinking: String(turn.thinking || "").trim(),
        stopReason,
        usage: payload.usage || null,
        events: turn.currentEvents,
        completedAt: new Date().toISOString(),
      });
      turn.finishedTurns = turn.finishedTurns.slice(-12);
      turn.text = "";
      turn.thinking = "";
      turn.currentEvents = [];
    }

    function appendStreamToolEvent(turn, event) {
      const finishedTurn = latestFinishedToolUseTurn(turn);
      if (finishedTurn) {
        finishedTurn.events = mergeStreamToolEvent(finishedTurn.events || [], event);
        return;
      }

      turn.currentEvents = mergeStreamToolEvent(turn.currentEvents, event);
    }

    function isClaudeToolEvent(event) {
      return event.kind?.startsWith("claude_tool_input_");
    }

    function mergeStreamToolEvent(events, event) {
      if (!event.kind?.startsWith("claude_tool_input_")) {
        return [...events, event].slice(-5);
      }

      const key = streamToolInputKey(event);
      const nextEvents = [...events];
      const existingIndex = nextEvents.findIndex((candidate) => (
        candidate.kind?.startsWith("claude_tool_input_") &&
        streamToolInputKey(candidate) === key
      ));
      const existing = existingIndex >= 0 ? nextEvents[existingIndex] : null;
      const merged = mergeToolInputEvent(existing, event, key);
      if (existingIndex >= 0) {
        nextEvents[existingIndex] = merged;
      } else {
        nextEvents.push(merged);
      }
      return nextEvents.slice(-5);
    }

    function mergeToolInputEvent(existing, event, key) {
      const existingPayload = existing?.payload || {};
      const payload = event.payload || {};
      const nextPayload = { ...existingPayload, ...payload };
      const existingPartial = String(existingPayload.input_partial || "");

      if (event.kind === "claude_tool_input_delta") {
        nextPayload.input_partial = existingPartial + String(payload.partial_json || "");
        nextPayload.status = "streaming input";
      } else if (event.kind === "claude_tool_input_complete") {
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

    function latestFinishedToolUseTurn(turn) {
      const latest = turn.finishedTurns[turn.finishedTurns.length - 1];
      if (!latest || latest.stopReason !== "tool_use") return null;
      return latest;
    }

    function markStreamCommitted() {
      state.streamTurn = null;
      state.currentClaudeSequence = null;
      state.ignoreClaudeUntilStart = false;
    }

    function markStreamInterrupted() {
      state.streamTurn = null;
      state.currentClaudeSequence = null;
    }

    function isAcknowledged(pending, workflowState) {
      return workflowState.transcript.some((message) => {
        if (message.role === "user" && message.content === pending.content) return true;
        if (message.role === "system" && message.content.includes(pending.content)) return true;
        return false;
      });
    }

    function render() {
      const workflowState = state.workflowState;
      const thinkingLabel = workflowState?.thinking?.enabled ? " | thinking" : "";
      const modelLabel = workflowState?.model ? ` | ${workflowState.model}${thinkingLabel}` : "";
      statusEl.textContent = state.draftConversation
        ? "draft | workflow not started"
        : workflowState
        ? `${workflowState.status}${workflowState.pending_messages ? `, queued: ${workflowState.pending_messages}` : ""}${modelLabel}`
        : "starting...";
      renderSidebar();
      renderArtifactsPanel();
      renderArtifactViewer();

      const fragment = document.createDocumentFragment();
      if (!workflowState && state.localPending.length === 0) {
        const empty = document.createElement("div");
        empty.className = "empty";
        empty.textContent = state.draftConversation
          ? "Type your first message to start a Temporal workflow."
          : "Starting a Temporal workflow...";
        fragment.append(empty);
      }

      for (const [index, message] of (workflowState?.transcript || []).entries()) {
        fragment.append(renderMessage(message, index, workflowState));
      }
      for (const pending of state.localPending) {
        fragment.append(bubble("pending", pending.label, `${pending.content} (${pending.phase})`));
      }
      const streamPanel = renderStreamPanel();
      if (streamPanel) {
        fragment.append(streamPanel);
      }
      const approvalsPanel = renderApprovalsPanel();
      if (approvalsPanel) {
        fragment.append(approvalsPanel);
      }

      // Capture whether we were pinned to the bottom BEFORE swapping content;
      // replaceChildren resets scroll, so re-pin only if the user was already
      // at the bottom. This keeps live output in view without yanking the user
      // back down when they've scrolled up to read history (and avoids the
      // jump-to-top-then-scroll churn during the burst of re-renders an
      // approval resolution triggers).
      const pinnedToBottom =
        messagesEl.scrollHeight - messagesEl.scrollTop - messagesEl.clientHeight < 80;
      messagesEl.replaceChildren(fragment);
      if (pinnedToBottom) {
        messagesEl.scrollTop = messagesEl.scrollHeight;
      }
      // The live streaming text/thinking boxes have their own max-height scroll
      // region; keep them pinned to the latest output as text streams in.
      messagesEl
        .querySelectorAll(
          ".stream-current-turn .stream-text, .stream-current-turn .stream-thinking",
        )
        .forEach((el) => {
          el.scrollTop = el.scrollHeight;
        });
      eventsEl.replaceChildren();
    }

    function renderMessage(message, index, workflowState) {
      if (message.role === "user") {
        if (workflowState.active_message_index === index) {
          return bubble("pending", "you -> agent", `${message.content} (delivered)`);
        }
        if (workflowState.queued_message_indices.includes(index)) {
          return bubble("pending", "you", `${message.content} (queued)`);
        }
        return bubble("user", "you", message.content);
      }
      if (message.role === "assistant") return bubble("assistant", "assistant", message.content);
      return bubble("system", "system", message.content);
    }

    function renderArtifactsPanel() {
      const artifacts = state.workflowState?.artifacts || [];
      const panel = document.createElement("section");
      panel.className = "artifact-panel";

      const header = document.createElement("div");
      header.className = "artifact-panel-header";
      const title = document.createElement("span");
      title.textContent = "Artifacts";
      const count = document.createElement("span");
      count.className = "artifact-panel-count";
      count.textContent = artifacts.length === 1 ? "1 file" : `${artifacts.length} files`;
      header.append(title, count);
      panel.append(header);

      if (artifacts.length === 0) {
        const empty = document.createElement("div");
        empty.className = "artifact-empty";
        empty.textContent = "Artifacts created by the agent will appear here.";
        panel.append(empty);
        artifactsSidebarEl.replaceChildren(panel);
        return;
      }

      const list = document.createElement("div");
      list.className = "artifact-list";
      for (const artifact of [...artifacts].reverse()) {
        list.append(renderArtifactCard(artifact));
      }
      panel.append(list);
      artifactsSidebarEl.replaceChildren(panel);
    }

    function renderArtifactCard(artifact) {
      const card = document.createElement("article");
      card.className = "artifact-card";

      const name = document.createElement("div");
      name.className = "artifact-name";
      name.textContent = artifact.name || artifact.artifact_id;

      const meta = document.createElement("div");
      meta.className = "artifact-meta";
      meta.textContent = `${artifact.mime_type || "application/octet-stream"} | ${formatBytes(artifact.size_bytes || 0)}`;

      const actions = document.createElement("div");
      actions.className = "artifact-actions";
      actions.append(artifactViewButton(artifact));
      actions.append(artifactLink(artifact.download_url, "Download", true));

      card.append(name, meta, actions);
      return card;
    }

    function artifactViewButton(artifact) {
      const button = document.createElement("button");
      button.type = "button";
      button.textContent = "View";
      button.addEventListener("click", () => {
        openArtifactViewer(artifact).catch((err) => {
          state.artifactViewer.error = String(err);
          state.artifactViewer.loading = false;
          renderArtifactViewer();
        });
      });
      return button;
    }

    function artifactLink(url, label, download) {
      const link = document.createElement("a");
      link.href = url;
      link.textContent = label;
      if (download) {
        link.setAttribute("download", "");
      } else {
        link.target = "_blank";
        link.rel = "noreferrer";
      }
      return link;
    }

    async function openArtifactViewer(artifact) {
      closeArtifactObjectUrl();
      state.artifactViewer = {
        open: true,
        artifact,
        loading: true,
        error: "",
        text: "",
        objectUrl: "",
      };
      renderArtifactViewer();

      if (isImageArtifact(artifact) || isPdfArtifact(artifact)) {
        state.artifactViewer.loading = false;
        renderArtifactViewer();
        return;
      }

      const response = await fetch(artifact.view_url);
      if (!response.ok) throw new Error(await responseErrorText(response));
      state.artifactViewer.text = await response.text();
      state.artifactViewer.loading = false;
      renderArtifactViewer();
    }

    function closeArtifactViewer() {
      closeArtifactObjectUrl();
      state.artifactViewer = {
        open: false,
        artifact: null,
        loading: false,
        error: "",
        text: "",
        objectUrl: "",
      };
      renderArtifactViewer();
    }

    function closeArtifactObjectUrl() {
      if (state.artifactViewer?.objectUrl) {
        URL.revokeObjectURL(state.artifactViewer.objectUrl);
      }
    }

    function renderArtifactViewer() {
      const viewer = state.artifactViewer;
      artifactViewerOverlayEl.hidden = !viewer.open;
      if (!viewer.open || !viewer.artifact) {
        artifactViewerOverlayEl.replaceChildren();
        return;
      }

      const artifact = viewer.artifact;
      const shell = document.createElement("div");
      shell.className = "artifact-viewer";

      const header = document.createElement("div");
      header.className = "artifact-viewer-header";

      const title = document.createElement("div");
      title.className = "artifact-viewer-title";
      const name = document.createElement("div");
      name.className = "artifact-viewer-name";
      name.textContent = artifact.name || artifact.artifact_id;
      const meta = document.createElement("div");
      meta.className = "artifact-viewer-meta";
      meta.textContent = `${artifact.mime_type || "application/octet-stream"} | ${formatBytes(artifact.size_bytes || 0)}`;
      title.append(name, meta);

      const actions = document.createElement("div");
      actions.className = "artifact-viewer-actions";
      actions.append(artifactLink(artifact.download_url, "Download", true));
      const close = document.createElement("button");
      close.type = "button";
      close.textContent = "Close";
      close.addEventListener("click", closeArtifactViewer);
      actions.append(close);

      header.append(title, actions);
      shell.append(header);

      const body = document.createElement("div");
      body.className = "artifact-viewer-body";
      renderArtifactViewerBody(body, viewer);
      shell.append(body);

      artifactViewerOverlayEl.replaceChildren(shell);
    }

    function renderArtifactViewerBody(body, viewer) {
      const artifact = viewer.artifact;
      if (viewer.loading) {
        const loading = document.createElement("div");
        loading.className = "empty";
        loading.textContent = "Loading artifact...";
        body.append(loading);
        return;
      }
      if (viewer.error) {
        const error = document.createElement("div");
        error.className = "artifact-viewer-error";
        error.textContent = viewer.error;
        body.append(error);
        return;
      }
      if (isImageArtifact(artifact)) {
        const image = document.createElement("img");
        image.className = "artifact-viewer-image";
        image.src = artifact.view_url;
        image.alt = artifact.name || "Artifact";
        body.append(image);
        return;
      }
      if (isPdfArtifact(artifact)) {
        const frame = document.createElement("iframe");
        frame.className = "artifact-viewer-frame";
        frame.src = artifact.view_url;
        body.append(frame);
        return;
      }

      const content = document.createElement("div");
      content.className = "bubble-content";
      if (isMarkdownArtifact(artifact)) {
        content.classList.add("artifact-markdown");
        renderFormattedContent(content, viewer.text);
      } else {
        content.append(createCodeBlock(viewer.text, languageFromFileName(artifact.name)));
      }
      body.append(content);
    }

    function isImageArtifact(artifact) {
      const mimeType = artifact?.mime_type || "";
      return mimeType.startsWith("image/") && mimeType !== "image/svg+xml";
    }

    function isPdfArtifact(artifact) {
      return artifact?.mime_type === "application/pdf";
    }

    function isMarkdownArtifact(artifact) {
      const mimeType = String(artifact?.mime_type || "").toLowerCase();
      const name = String(artifact?.name || artifact?.artifact_id || "").toLowerCase();
      return (
        mimeType === "text/markdown" ||
        mimeType === "text/x-markdown" ||
        name.endsWith(".md") ||
        name.endsWith(".markdown")
      );
    }

    function formatBytes(size) {
      if (!Number.isFinite(size)) return "0 B";
      if (size < 1024) return `${size} B`;
      if (size < 1024 * 1024) return `${(size / 1024).toFixed(1)} KB`;
      return `${(size / 1024 / 1024).toFixed(1)} MB`;
    }

    function languageFromFileName(name) {
      const extension = String(name || "").split(".").pop()?.toLowerCase();
      const languages = {
        bash: "bash",
        css: "css",
        html: "html",
        js: "javascript",
        json: "json",
        md: "markdown",
        markdown: "markdown",
        py: "python",
        sh: "bash",
        sql: "sql",
        ts: "typescript",
        xml: "xml",
        yaml: "yaml",
        yml: "yaml",
      };
      return languages[extension] || null;
    }

    function bubble(kind, label, content) {
      const node = document.createElement("div");
      node.className = `bubble ${kind}`;
      const labelNode = document.createElement("span");
      labelNode.className = "label";
      labelNode.textContent = label;
      const contentNode = document.createElement("div");
      contentNode.className = "bubble-content";
      renderFormattedContent(contentNode, content);
      node.append(labelNode, contentNode);
      return node;
    }

    function renderStreamPanel() {
      const turn = state.streamTurn;
      if (!turn) return null;
      if (!turn.text && !turn.thinking && turn.currentEvents.length === 0 && turn.finishedTurns.length === 0) return null;

      const collapsed = state.streamPanelCollapsed;
      const node = document.createElement("section");
      node.className = `stream-panel ${turn.status}${collapsed ? " collapsed" : ""}`;

      const header = document.createElement("div");
      header.className = "stream-panel-header";

      const title = document.createElement("div");
      title.className = "stream-panel-title";
      title.textContent = "Streaming visibility";
      const status = document.createElement("span");
      status.className = "stream-panel-status";
      status.textContent = streamPanelStatus(turn);
      title.append(status);

      const toggle = document.createElement("button");
      toggle.type = "button";
      toggle.className = "stream-panel-toggle";
      toggle.textContent = collapsed ? "Expand" : "Collapse";
      toggle.addEventListener("click", () => {
        state.streamPanelCollapsed = !state.streamPanelCollapsed;
        render();
      });

      header.append(title, toggle);
      node.append(header);

      const body = document.createElement("div");
      body.className = "stream-panel-body";

      if (collapsed) {
        const preview = document.createElement("div");
        preview.className = "stream-preview";
        preview.textContent = streamPanelPreview(turn);
        body.append(preview);
        node.append(body);
        return node;
      }

      if (turn.finishedTurns.length) {
        const finishedList = document.createElement("div");
        finishedList.className = "stream-finished-list";
        for (const finishedTurn of turn.finishedTurns) {
          finishedList.append(renderFinishedStreamTurn(finishedTurn));
        }
        body.append(finishedList);
      }

      if (turn.text) {
        const currentTurn = document.createElement("div");
        currentTurn.className = "stream-current-turn";

        const title = document.createElement("div");
        title.className = "stream-finished-title";
        title.textContent = `Claude turn ${turn.activeSequence ?? ""} streaming`;
        currentTurn.append(title);

        if (turn.thinking) {
          const thinking = document.createElement("div");
          thinking.className = "stream-thinking";
          thinking.textContent = turn.thinking;
          currentTurn.append(thinking);
        }

        const text = document.createElement("div");
        text.className = "stream-text";
        text.textContent = turn.text;
        currentTurn.append(text);

        if (turn.currentEvents.length) {
          currentTurn.append(renderStreamToolList(turn.currentEvents));
        }

        body.append(currentTurn);
      }

      if (!turn.text && turn.thinking) {
        const currentThinking = document.createElement("div");
        currentThinking.className = "stream-current-turn";
        const title = document.createElement("div");
        title.className = "stream-finished-title";
        title.textContent = `Claude turn ${turn.activeSequence ?? ""} thinking`;
        const text = document.createElement("div");
        text.className = "stream-thinking";
        text.textContent = turn.thinking;
        currentThinking.append(title, text);
        body.append(currentThinking);
      }

      if (!turn.text && turn.currentEvents.length) {
        body.append(renderStreamToolList(turn.currentEvents));
      }

      if (!turn.text && !turn.thinking && !turn.currentEvents.length && !turn.finishedTurns.length) {
        const preview = document.createElement("div");
        preview.className = "stream-preview";
        preview.textContent = "Waiting for streamed tokens or tool activity...";
        body.append(preview);
      }

      node.append(body);
      return node;
    }

    function renderFinishedStreamTurn(finishedTurn) {
      const node = document.createElement("div");
      node.className = "stream-finished-turn";
      const title = document.createElement("div");
      title.className = "stream-finished-title";
      title.textContent = `Claude turn ${finishedTurn.sequence ?? ""} complete | ${finishedTurn.stopReason}`;
      if (finishedTurn.thinking) {
        const thinking = document.createElement("div");
        thinking.className = "stream-thinking";
        thinking.textContent = finishedTurn.thinking;
        node.append(title, thinking);
      } else {
        node.append(title);
      }
      const text = document.createElement("div");
      text.textContent = finishedTurn.text || `Completed without text (${finishedTurn.stopReason}).`;
      node.append(text);
      if (finishedTurn.events?.length) {
        node.append(renderStreamToolList(finishedTurn.events));
      }
      return node;
    }

    function renderStreamToolList(events) {
      const toolList = document.createElement("div");
      toolList.className = "stream-tool-list";
      for (const event of events.slice(-5)) {
        toolList.append(renderStreamToolEvent(event));
      }
      return toolList;
    }

    function renderStreamToolEvent(event) {
      const node = document.createElement("div");
      node.className = "stream-tool-event";
      if (event.kind?.startsWith("claude_tool_input_")) {
        node.classList.add("input-streaming");
      }

      const name = document.createElement("div");
      name.className = "stream-tool-name";
      name.textContent = streamToolLabel(event);
      node.append(name);

      const payload = document.createElement("div");
      payload.className = "stream-tool-payload";
      payload.textContent = streamToolPayloadText(event);
      node.append(payload);

      return node;
    }

    function streamToolPayloadText(event) {
      const payload = event.payload || {};
      if (event.kind?.startsWith("claude_tool_input_")) {
        const status = payload.status || "building input";
        if (event.kind === "claude_tool_input_complete") {
          return `${status}:\n${truncateStreamText(formatStreamValue(payload.input ?? payload.input_partial ?? payload.input_preview))}`;
        }
        const partial = payload.input_partial || payload.partial_json || "";
        return `${status}:\n${truncateStreamText(String(partial))}`;
      }

      return `${event.kind}: ${truncateStreamText(formatStreamValue(payload))}`;
    }

    function streamPanelStatus(turn) {
      const count = turn.currentEvents.length + turn.finishedTurns.reduce(
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
      if (latestFinished?.text) {
        return latestFinished.text.replace(/\s+/g, " ").slice(-240);
      }
      if (latestFinished?.thinking) {
        return latestFinished.thinking.replace(/\s+/g, " ").slice(-240);
      }
      if (latestEvent) {
        return `${streamToolLabel(latestEvent)} | ${latestEvent.kind}`;
      }
      return streamPanelStatus(turn);
    }

    function streamToolLabel(event) {
      const payloadToolName = event.payload?.tool_name;
      const name = payloadToolName || event.tool_name || "stream";
      return event.step ? `${name}:${event.step}` : name;
    }

    function formatStreamValue(value) {
      if (typeof value === "string") return value;
      try {
        return JSON.stringify(value, null, 2);
      } catch (_err) {
        return String(value);
      }
    }

    function truncateStreamText(value) {
      const text = String(value || "");
      if (text.length <= 4000) return text;
      return text.slice(-4000);
    }

    function renderFormattedContent(container, content) {
      const lines = String(content || "").replace(/\r\n/g, "\n").split("\n");
      let paragraphLines = [];
      let listNode = null;
      let listType = null;
      let codeLines = null;
      let codeLanguage = null;

      function flushParagraph() {
        if (paragraphLines.length === 0) return;
        const paragraph = document.createElement("p");
        paragraphLines.forEach((line, index) => {
          if (index > 0) paragraph.append(document.createElement("br"));
          renderInline(paragraph, line);
        });
        container.append(paragraph);
        paragraphLines = [];
      }

      function flushList() {
        if (!listNode) return;
        container.append(listNode);
        listNode = null;
        listType = null;
      }

      function flushCode() {
        if (codeLines === null) return;
        const source = codeLines.join("\n");
        container.append(createCodeBlock(source, codeLanguage));
        codeLines = null;
        codeLanguage = null;
      }

      for (let lineIndex = 0; lineIndex < lines.length; lineIndex += 1) {
        const line = lines[lineIndex];
        const fence = line.trim().match(/^```(?:\s*([A-Za-z0-9_+.#-]+))?.*$/);
        if (fence) {
          if (codeLines === null) {
            flushParagraph();
            flushList();
            codeLines = [];
            codeLanguage = fence[1] || null;
          } else {
            flushCode();
          }
          continue;
        }

        if (codeLines !== null) {
          codeLines.push(line);
          continue;
        }

        if (isMarkdownTableAt(lines, lineIndex)) {
          flushParagraph();
          flushList();
          lineIndex = renderMarkdownTable(container, lines, lineIndex) - 1;
          continue;
        }

        if (line.trim() === "") {
          flushParagraph();
          flushList();
          continue;
        }

        if (/^\s*-{3,}\s*$/.test(line)) {
          flushParagraph();
          flushList();
          container.append(document.createElement("hr"));
          continue;
        }

        const heading = line.match(/^(#{1,4})\s+(.+)$/);
        if (heading) {
          flushParagraph();
          flushList();
          const headingNode = document.createElement("div");
          headingNode.className = "md-heading";
          renderInline(headingNode, heading[2]);
          container.append(headingNode);
          continue;
        }

        if (/^\s*>\s?/.test(line)) {
          flushParagraph();
          flushList();
          const quoteLines = [];
          let quoteIndex = lineIndex;
          while (quoteIndex < lines.length && /^\s*>\s?/.test(lines[quoteIndex])) {
            quoteLines.push(lines[quoteIndex].replace(/^\s*>\s?/, ""));
            quoteIndex += 1;
          }
          const quote = document.createElement("blockquote");
          renderFormattedContent(quote, quoteLines.join("\n"));
          container.append(quote);
          lineIndex = quoteIndex - 1;
          continue;
        }

        const unordered = line.match(/^\s*[-*]\s+(.+)$/);
        const ordered = line.match(/^\s*\d+[.)]\s+(.+)$/);
        if (unordered || ordered) {
          flushParagraph();
          const nextType = unordered ? "ul" : "ol";
          if (!listNode || listType !== nextType) {
            flushList();
            listNode = document.createElement(nextType);
            listType = nextType;
          }
          const item = document.createElement("li");
          renderInline(item, unordered ? unordered[1] : ordered[1]);
          listNode.append(item);
          continue;
        }

        flushList();
        paragraphLines.push(line);
      }

      flushParagraph();
      flushList();
      flushCode();
    }

    function isMarkdownTableAt(lines, index) {
      const header = lines[index] || "";
      const separator = lines[index + 1] || "";
      return header.includes("|") && isMarkdownTableSeparator(separator);
    }

    function isMarkdownTableSeparator(line) {
      const cells = splitMarkdownTableRow(line);
      return (
        cells.length >= 2 &&
        cells.every((cell) => /^:?-{3,}:?$/.test(cell.trim()))
      );
    }

    function renderMarkdownTable(container, lines, startIndex) {
      const tableWrap = document.createElement("div");
      tableWrap.className = "markdown-table-wrap";
      const table = document.createElement("table");
      const head = document.createElement("thead");
      const body = document.createElement("tbody");

      const headerRow = document.createElement("tr");
      for (const cell of splitMarkdownTableRow(lines[startIndex])) {
        const th = document.createElement("th");
        renderInline(th, cell.trim());
        headerRow.append(th);
      }
      head.append(headerRow);

      let index = startIndex + 2;
      while (index < lines.length && lines[index].includes("|") && lines[index].trim() !== "") {
        const row = document.createElement("tr");
        for (const cell of splitMarkdownTableRow(lines[index])) {
          const td = document.createElement("td");
          renderInline(td, cell.trim());
          row.append(td);
        }
        body.append(row);
        index += 1;
      }

      table.append(head, body);
      tableWrap.append(table);
      container.append(tableWrap);
      return index;
    }

    function splitMarkdownTableRow(line) {
      let value = String(line || "").trim();
      if (value.startsWith("|")) value = value.slice(1);
      if (value.endsWith("|")) value = value.slice(0, -1);
      return value.split("|").map((cell) => cell.trim());
    }

    function createCodeBlock(source, languageHint = null) {
      const pre = document.createElement("pre");
      const code = document.createElement("code");
      const language = normalizeCodeLanguage(languageHint) || inferCodeLanguage(source);
      if (language) {
        pre.dataset.language = language;
        code.className = `language-${language}`;
        renderHighlightedCode(code, source, language);
      } else {
        code.textContent = source;
      }
      pre.append(code);
      return pre;
    }

    function renderInline(parent, text) {
      const pattern = /(\[[^\]]+\]\(https?:\/\/[^)\s]+\)|`[^`]+`|\*\*[^*]+\*\*|\*[^*]+\*)/g;
      let index = 0;
      for (const match of text.matchAll(pattern)) {
        if (match.index > index) {
          parent.append(document.createTextNode(text.slice(index, match.index)));
        }
        const token = match[0];
        if (token.startsWith("[") && token.includes("](")) {
          const link = token.match(/^\[([^\]]+)\]\((https?:\/\/[^)\s]+)\)$/);
          if (link) {
            const anchor = document.createElement("a");
            anchor.href = link[2];
            anchor.target = "_blank";
            anchor.rel = "noreferrer";
            anchor.textContent = link[1];
            parent.append(anchor);
          } else {
            parent.append(document.createTextNode(token));
          }
        } else if (token.startsWith("`")) {
          const code = document.createElement("code");
          code.textContent = token.slice(1, -1);
          parent.append(code);
        } else if (token.startsWith("**")) {
          const strong = document.createElement("strong");
          strong.textContent = token.slice(2, -2);
          parent.append(strong);
        } else {
          const emphasis = document.createElement("em");
          emphasis.textContent = token.slice(1, -1);
          parent.append(emphasis);
        }
        index = match.index + token.length;
      }
      if (index < text.length) {
        parent.append(document.createTextNode(text.slice(index)));
      }
    }

    function renderHighlightedCode(parent, source, language) {
      const rules = highlightRules(language);
      let index = 0;

      while (index < source.length) {
        const chunk = source.slice(index);
        let matched = false;

        for (const [className, rule] of rules) {
          const match = chunk.match(rule);
          if (!match) continue;

          const text = match[0];
          if (!text) continue;

          if (className === null) {
            parent.append(document.createTextNode(text));
          } else {
            const span = document.createElement("span");
            span.className = className;
            span.textContent = text;
            parent.append(span);
          }
          index += text.length;
          matched = true;
          break;
        }

        if (!matched) {
          parent.append(document.createTextNode(source[index]));
          index += 1;
        }
      }
    }

    function highlightRules(language) {
      const common = [
        [null, /^\s+/],
        ["hl-number", /^\b(?:0x[\da-fA-F]+|\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)\b/],
        ["hl-operator", /^[{}()[\].,;:+\-*/%<>=!&|^~?]+/],
      ];

      if (language === "python") {
        return [
          [null, /^\s+/],
          ["hl-comment", /^#[^\n]*/],
          ["hl-string", /^(?:(?:[rubfRUBF]{0,3})(?:"{3}[\s\S]*?"{3}|'{3}[\s\S]*?'{3}|"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'))/],
          ["hl-keyword", wordRule("and|as|assert|async|await|break|class|continue|def|del|elif|else|except|False|finally|for|from|global|if|import|in|is|lambda|None|nonlocal|not|or|pass|raise|return|True|try|while|with|yield")],
          ["hl-function", wordRule("abs|all|any|bool|dict|enumerate|filter|float|int|len|list|map|max|min|open|print|range|set|str|sum|super|tuple|zip")],
          ...common.slice(1),
        ];
      }

      if (language === "javascript" || language === "typescript") {
        return [
          [null, /^\s+/],
          ["hl-comment", /^\/\/[^\n]*/],
          ["hl-comment", /^\/\*[\s\S]*?\*\//],
          ["hl-string", /^`(?:\\.|[^`\\])*`/],
          ["hl-string", /^"(?:\\.|[^"\\])*"/],
          ["hl-string", /^'(?:\\.|[^'\\])*'/],
          ["hl-keyword", wordRule("async|await|break|case|catch|class|const|continue|debugger|default|delete|do|else|export|extends|false|finally|for|from|function|if|import|in|instanceof|let|new|null|of|return|static|super|switch|this|throw|true|try|typeof|undefined|var|void|while|with|yield")],
          ["hl-type", wordRule("interface|type|implements|private|protected|public|readonly|enum|namespace|abstract|declare")],
          ["hl-function", /^[A-Za-z_$][\w$]*(?=\s*\()/],
          ...common.slice(1),
        ];
      }

      if (language === "json") {
        return [
          [null, /^\s+/],
          ["hl-property", /^"(?:\\.|[^"\\])*"(?=\s*:)/],
          ["hl-string", /^"(?:\\.|[^"\\])*"/],
          ["hl-keyword", wordRule("true|false|null")],
          ...common.slice(1),
        ];
      }

      if (language === "bash") {
        return [
          [null, /^\s+/],
          ["hl-comment", /^#[^\n]*/],
          ["hl-string", /^"(?:\\.|[^"\\])*"/],
          ["hl-string", /^'(?:\\.|[^'\\])*'/],
          ["hl-property", /^\$[A-Za-z_][\w]*/],
          ["hl-keyword", wordRule("alias|case|do|done|elif|else|esac|export|fi|for|function|if|in|local|readonly|return|set|shift|source|then|unalias|unset|while")],
          ["hl-function", /^[A-Za-z_][\w.-]*(?=\s)/],
          ...common.slice(1),
        ];
      }

      if (language === "sql") {
        return [
          [null, /^\s+/],
          ["hl-comment", /^--[^\n]*/],
          ["hl-comment", /^\/\*[\s\S]*?\*\//],
          ["hl-string", /^'(?:''|[^'])*'/],
          ["hl-keyword", wordRule("alter|and|as|avg|by|case|count|create|delete|desc|distinct|drop|else|end|from|group|having|in|inner|insert|into|is|join|left|limit|max|min|not|null|offset|on|or|order|outer|right|select|set|sum|table|then|update|values|view|when|where", "i")],
          ...common.slice(1),
        ];
      }

      if (language === "html" || language === "xml") {
        return [
          [null, /^\s+/],
          ["hl-comment", /^<!--[\s\S]*?-->/],
          ["hl-tag", /^<!doctype[^>]*>/i],
          ["hl-tag", /^<\/?[A-Za-z][\w:-]*/],
          ["hl-attr", /^[A-Za-z_:][\w:.-]*(?=\=)/],
          ["hl-string", /^"(?:\\.|[^"\\])*"/],
          ["hl-string", /^'(?:\\.|[^'\\])*'/],
          ["hl-operator", /^\/?>/],
          ...common.slice(1),
        ];
      }

      if (language === "css") {
        return [
          [null, /^\s+/],
          ["hl-comment", /^\/\*[\s\S]*?\*\//],
          ["hl-string", /^"(?:\\.|[^"\\])*"/],
          ["hl-string", /^'(?:\\.|[^'\\])*'/],
          ["hl-property", /^--?[A-Za-z_][\w-]*(?=\s*:)/],
          ["hl-keyword", /^@[A-Za-z-]+/],
          ["hl-number", /^\b\d+(?:\.\d+)?(?:px|rem|em|vh|vw|%|s|ms)?\b/],
          ["hl-operator", /^[{}()[\].,;:+\-*/%<>=!&|^~?]+/],
        ];
      }

      if (language === "yaml") {
        return [
          [null, /^\s+/],
          ["hl-comment", /^#[^\n]*/],
          ["hl-property", /^[A-Za-z_][\w.-]*(?=\s*:)/],
          ["hl-string", /^"(?:\\.|[^"\\])*"/],
          ["hl-string", /^'(?:\\.|[^'\\])*'/],
          ["hl-keyword", wordRule("true|false|null|yes|no|on|off")],
          ...common.slice(1),
        ];
      }

      if (language === "markdown") {
        return [
          [null, /^[\s\S]+/],
        ];
      }

      return [
        [null, /^\s+/],
        ["hl-comment", /^#[^\n]*/],
        ["hl-comment", /^\/\/[^\n]*/],
        ["hl-comment", /^\/\*[\s\S]*?\*\//],
        ["hl-string", /^`(?:\\.|[^`\\])*`/],
        ["hl-string", /^"(?:\\.|[^"\\])*"/],
        ["hl-string", /^'(?:\\.|[^'\\])*'/],
        ["hl-function", /^[A-Za-z_$][\w$]*(?=\s*\()/],
        ...common.slice(1),
      ];
    }

    function wordRule(words, flags = "") {
      return new RegExp(`^\\b(?:${words})\\b`, flags);
    }

    function normalizeCodeLanguage(language) {
      if (!language) return null;
      const normalized = language.toLowerCase();
      const aliases = {
        bash: "bash",
        cjs: "javascript",
        css: "css",
        html: "html",
        javascript: "javascript",
        js: "javascript",
        json: "json",
        jsonc: "json",
        jsx: "javascript",
        markdown: "markdown",
        md: "markdown",
        mjs: "javascript",
        py: "python",
        python: "python",
        sh: "bash",
        shell: "bash",
        sql: "sql",
        ts: "typescript",
        tsx: "typescript",
        typescript: "typescript",
        xml: "xml",
        yaml: "yaml",
        yml: "yaml",
        zsh: "bash",
      };
      return aliases[normalized] || null;
    }

    function inferCodeLanguage(source) {
      const trimmed = source.trim();
      if (!trimmed) return null;
      if ((trimmed.startsWith("{") || trimmed.startsWith("[")) && looksLikeJson(trimmed)) return "json";
      if (/^\s*(from\s+\w+\s+import|import\s+\w+|def\s+\w+|class\s+\w+|async\s+def\s+\w+)\b/m.test(source)) return "python";
      if (/\b(print|range|len)\s*\(/.test(source) && /(^|\n)\s*#/.test(source)) return "python";
      if (/\b(const|let|function|console\.log|=>|import\s+.+\s+from)\b/.test(source)) return "javascript";
      if (/^#!.*\b(?:bash|sh|zsh)\b/m.test(source) || /\b(?:echo|curl|export|chmod|sudo)\b/.test(source)) return "bash";
      if (/\bselect\b[\s\S]+\bfrom\b/i.test(source)) return "sql";
      if (/^\s*</.test(source) && /<\/?[A-Za-z][\s\S]*>/.test(source)) return "html";
      if (/^[\s\S]*\{[\s\S]*:[\s\S]*\}/.test(source) && /[.#]?[A-Za-z][\w-]*\s*\{/.test(source)) return "css";
      if (/^[A-Za-z_][\w.-]*\s*:/m.test(source)) return "yaml";
      return null;
    }

    function looksLikeJson(source) {
      try {
        JSON.parse(source);
        return true;
      } catch (_err) {
        return false;
      }
    }

    function jsonHeaders() {
      return { "content-type": "application/json" };
    }
  
