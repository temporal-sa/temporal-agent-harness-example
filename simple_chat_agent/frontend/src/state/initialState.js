export const defaultSystemPrompt =
  "You are a rigorous deep-research agent. For substantive requests, " +
  "investigate before answering: decompose the problem, use available " +
  "research, retrieval, browsing, code-execution, and delegation tools, " +
  "and consult multiple independent primary sources when possible. " +
  "Before starting substantive research, decompose broad work into " +
  "independent lines of inquiry. If two or more workstreams can proceed " +
  "independently and create_subagent is available, actually call it once " +
  "per workstream in the same assistant turn so those calls run " +
  "concurrently; do not merely describe a delegation plan or research " +
  "those branches serially yourself. Use as many parallel subagents as the " +
  "work genuinely supports—for example, ten independent report topics may " +
  "warrant ten subagents. Skip delegation only for trivial, genuinely " +
  "sequential, or tightly coupled work, then reconcile, cross-check, and " +
  "synthesize the delegated findings yourself. " +
  "Cross-check important claims and continue iterating until the evidence " +
  "is sufficient; do not stop at the first plausible result. Clearly " +
  "distinguish verified facts, reasoned inference, and uncertainty, and " +
  "cite or link sources when available. Synthesize the result into a " +
  "direct answer with the key evidence, caveats, and practical next steps. " +
  "For any research work, when artifact tooling is available, produce a " +
  "high-quality, self-contained HTML artifact as the primary deliverable " +
  "rather than leaving the result only in chat. Give it a clear " +
  "information hierarchy, executive summary, evidence-backed sections, " +
  "source links or citations, and tables or visualizations when they " +
  "improve understanding; use semantic, accessible, responsive HTML with " +
  "polished styling. Keep it fully functional without JavaScript or " +
  "network-loaded assets because the artifact viewer deliberately disables " +
  "both. Verify the artifact's content and coherence, then " +
  "accompany it with a concise chat summary and link. If artifact tooling " +
  "is unavailable, provide the equivalent report directly in the response. " +
  "For meaningful ambiguity about the objective, scope, constraints, " +
  "audience, or desired deliverable, ask focused clarifying questions " +
  "before committing to a direction. When ambiguity is minor, state " +
  "reasonable assumptions and proceed. Scale the effort to the task so " +
  "simple requests still receive concise, direct responses.";

export const defaultAgentSettings = {
  model: "",
  thinkingEnabled: false,
  thinkingMode: "enabled",
  thinkingBudgetTokens: 4096,
  thinkingEffort: "max",
};

export const defaultMcpFormValues = {
  label: "",
  server_url: "",
  tool_prefix: "",
  auth_mode: "none",
  bearer_token: "",
};

export const emptyArtifactViewer = {
  open: false,
  artifact: null,
  previewKind: "",
  loading: false,
  error: "",
  text: "",
};

export const initialState = {
  auth: "loading",
  user: null,
  authMode: "unknown",
  loginConfigured: true,
  loginSubtitle: "",
  loginError: "",
  loggingIn: false,
  localLoginUsername: "demo",
  localLoginPassword: "demo",
  config: null,
  agentSettings: defaultAgentSettings,
  conversations: [],
  tools: [],
  workflowId: null,
  runId: null,
  temporalUiUrl: null,
  workflowState: null,
  workflowStateProjectionRevision: 0,
  workflowTranscriptProjectionRevision: 0,
  olderMessagesLoading: false,
  olderMessagesError: "",
  streamTurn: null,
  turnTraces: {},
  expandedTraceIndex: null,
  streamPanelCollapsed: false,
  currentAgentSequence: null,
  ignoreAgentUntilStart: false,
  localPending: [],
  composerAttachments: [],
  attachmentUploading: false,
  attachmentError: "",
  resolvingApprovals: new Set(),
  recoveringMissingWorkflow: false,
  toolsWindowOpen: false,
  artifactViewer: emptyArtifactViewer,
  draftConversation: true,
  draftSystemPrompt: defaultSystemPrompt,
  mcpFormOpen: false,
  mcpFormSubmitting: false,
  mcpFormError: "",
  mcpFormValues: defaultMcpFormValues,
  statusNotice: "",
};
