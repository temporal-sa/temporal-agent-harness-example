// The coding session facade's public types (PRD U1). Everything a component needs from a
// session is typed here; components import from "../session" and never from the harness
// packages, so an SDK upgrade touches only this folder.
//
// Slots that Eng W fills (web/src/{workbench,decisions,recovery}). Each gets the whole facade
// as `session` and reads what it needs; the shell renders them in these places:
//
//   web/src/workbench/index.ts   export function Workbench(props: WorkbenchProps)
//                                 right-hand panel (1920: 34em, 1366: 30em) and, at <= 600px,
//                                 the bottom sheet (variant "sheet").
//   web/src/decisions/index.ts   export function DecisionDock(props: DecisionDockProps)
//                                 above the composer: stacked approval card(s), checkpoint and
//                                 spend-cap cards. Groups `session.pendingApprovals` itself.
//   web/src/recovery/index.ts    export function WorkerBanner(props: WorkerBannerProps)
//                                 top of the transcript column while `session.worker.answering`
//                                 is false.
//                                 export function RecoveryDivider(props: RecoveryDividerProps)
//                                 rendered by the transcript (Eng U) at the point in a turn
//                                 where a worker was lost, one per `TurnRecovery`.
//                                 export function SendStateTrail(props: SendStateTrailProps)
//                                 Sending → In Temporal → Accepted → Running, for one record.
//                                 export function JudgeCard(props: JudgeCardProps)  (decisions/)
//                                 rendered by the transcript above the tool row it decided.
//
// The Workbench also gets `focus` (path chips in the transcript open a file in Changes).
//
// Ask Eng U (through the PM) for anything missing here rather than reaching into the harness.

import type {
  AgentView,
  ConnectionStatus,
  Evaluation,
  HarnessMessage,
  MessagePart,
  ModelInteractionPart,
  ReplyDeltaPart,
  SubagentPart,
  ThoughtSummaryPart,
  ToolPart,
  ToolState,
  WaitingToolCall,
} from "@temporalio/agent-harness-react";

// -- re-exported harness shapes (read-only views of the stream) ---------------------------

/** One inbound message to an agent and everything it produced, in stream order. */
export type Message = HarnessMessage;
export type {
  Evaluation,
  MessagePart,
  ModelInteractionPart,
  ReplyDeltaPart,
  SubagentPart,
  ThoughtSummaryPart,
  ToolPart,
  ToolState,
};
/** One agent in the session's tree (root or subagent at any depth). */
export type AgentNode = AgentView;
/** The approval classes the agent sorts tools into today (config `risk_classes` is the list
 *  of record; a class not listed here is still passed through as a string). */
export type RiskClass = "read_only" | "workspace_edit" | "shell" | "network" | "external_side_effect";

/** A gated tool call waiting on a person, anywhere in the tree. Raw: Eng W groups them.
 *  `riskClass`: config `tool_risk_classes[toolName]`, else `default_tool_risk_class`; null
 *  only while config hasn't loaded. `rememberAllowed`: its class is in config
 *  `remember_allowed_classes` (offer "Always approve"). */
export type PendingApproval = WaitingToolCall & {
  /** Null only while config hasn't loaded. */
  riskClass: RiskClass | (string & {}) | null;
  rememberAllowed: boolean;
  /** The Auto mode judge is deciding it (its latest evaluation is running: from
   *  auto_approval_evaluation_started until _ended, _error or _superseded). A person can still
   *  answer. */
  judging: boolean;
  /** The judge's latest finished evaluation (ended, superseded or error), once there is one;
   *  e.g. an escalation's reason. */
  evaluation: Evaluation | null;
};

// -- config and catalog (GET /api/agent/config) ---------------------------------------------

export type Provider = "anthropic" | "openai" | "google";
export type Effort = "low" | "medium" | "high";
export type Posture = "ask_everything" | "trust_safe" | "auto" | "never_ask";

/** A provider plus a model id from that provider's allowlist (contracts.ModelChoice). */
export interface ModelChoice {
  provider: Provider;
  model: string;
  effort?: Effort | null;
}

export interface CatalogModel {
  provider: Provider;
  id: string;
  registry_key: string;
  display_name: string;
  tier: string;
  max_input_tokens: number | null;
  max_output_tokens: number | null;
  effort_options: Effort[];
  default_effort: Effort | null;
}

export interface CatalogProvider {
  provider: Provider;
  label: string;
  default_model: string;
  models: CatalogModel[];
}

export interface PostureOption {
  id: Posture;
  label: string;
  description: string;
}

export interface AgentCatalog {
  environment: string;
  default_model: ModelChoice;
  providers: CatalogProvider[];
  models: CatalogModel[];
  effort_options: Effort[];
  default_effort: Effort | null;
  /** Only the postures allowed in this environment ("never_ask" is absent in the catalog). */
  postures: PostureOption[];
  default_posture: Posture;
  research_tools: string[];
  judge: string;
  demos: { id: string; label: string }[];
  model_source: string;
}

// -- observable state (simple_chat_agent/agent/state.py) ------------------------------------
// Generated from the agent's JSON Schema by Eng F's `npm run gen` (web/src/generated).

import type { ChangedFile, ChangesState, Commit, PlanItem, PlanState, WorkspaceState } from "../generated";
export type { ChangedFile, ChangesState, Commit, PlanItem, PlanState, WorkspaceState };
export type PlanStatus = PlanItem["status"];
export type FileStatus = ChangedFile["status"];

// -- connection and status ------------------------------------------------------------------

/** The stream's state (harness ConnectionStatus): connecting, live, idle (caught up with an
 *  idle agent), reconnecting, closed (the workflow finished), error (retries exhausted;
 *  a send or `reconnect()` tries again), stopped. */
export type ConnectionState = ConnectionStatus;

/** The header's status pill, most urgent first. */
export type SessionStatus =
  /** A tool call waits on a person (pink). */
  | "waiting_for_you"
  /** A turn is open and the bridge says no worker is answering, or a send has sat in
   *  Temporal with no worker taking it (yellow). */
  | "worker_unresponsive"
  /** A turn is open (green). */
  | "running"
  | "idle";

// -- send states (PRD U4) -------------------------------------------------------------------

/** What a local action was. Messages and approvals are Signals that land in Temporal before
 *  any worker runs them (PRD S8). */
export type SendKind = "message" | "steer" | "interrupt" | "set_model" | "set_posture" | "approval" | "forget_always_allowed";

/**
 * - `sending`: clicked; the request hasn't returned.
 * - `in_temporal`: the API returned: the Signal is in workflow history, waiting for a worker.
 * - `accepted`: the agent admitted it (message: `message_accepted` on the stream; approval:
 *   `tool_approval_resolved`).
 * - `running`: its handler (message) or the approved tool is running.
 * - `done`: finished (handler returned; tool ended; a decline settled).
 * - `failed`: the request failed, or the agent rejected it. `error` says why.
 * Phases only move forward; a skipped phase gets the same timestamp as the next one.
 */
export type SendPhase = "sending" | "in_temporal" | "accepted" | "running" | "done" | "failed";

export interface SendRecord {
  /** Local id, stable for the record's life. */
  id: string;
  kind: SendKind;
  /** The harness message id once the API returned it (all kinds but approval). */
  messageId: string | null;
  /** The tool call it answers (approval only). */
  toolId: string | null;
  /** approval only: approve or decline, and whether "Always approve" was asked for. */
  approved: boolean | null;
  remember: boolean;
  phase: SendPhase;
  /** When each phase was entered (ms since epoch, browser clock). */
  at: Partial<Record<SendPhase, number>>;
  /** Raw failure text (server message, agent reason). Show `errorCopy(record)` instead. */
  error: string | null;
  /** The API error code or x_rejected code, when there is one (feeds `errorCopy`). Optional
   *  only so records built before it existed stay valid; the facade always sets it. */
  errorCode?: string | null;
  /** The HTTP status of a failed request (0: the app wasn't reachable). */
  errorStatus?: number | null;
}

// -- telemetry folded from our x_* frames (simple_chat_agent/api/agent_telemetry.py) -------

/** Token counts. `null` means the provider didn't report it, never 0. */
export interface TokenCounts {
  input: number | null;
  output: number | null;
  /** Part of `input` read from the provider's prompt cache. */
  cacheRead: number | null;
  cacheWrite: number | null;
  thinking: number | null;
}

/** One model call's usage (one `x_usage` id; a confirmed frame replaces the live one). */
export interface ModelCallUsage extends TokenCounts {
  id: string;
  agentId: string | null;
  turnId: string | null;
  turnNumber: number | null;
  messageId: string | null;
  callIndex: number | null;
  model: string | null;
  provider: string | null;
  attempt: number | null;
  /** Read back from workflow history (true) or live from the stream (false). */
  confirmed: boolean;
}

export interface UsageTotals extends TokenCounts {
  calls: number;
  /** Every call's usage is confirmed from history. */
  confirmed: boolean;
  /** cacheRead / input over the calls that report both; null when none do. */
  cacheShare: number | null;
}

export interface WorkCounts {
  modelCalls: number;
  toolCalls: number;
  tokensIn: number;
  tokensOut: number;
}

/** One `x_recovery` id: a model call or tool that ran again because a worker was lost. */
export interface Recovery {
  id: string;
  agentId: string | null;
  turnId: string | null;
  turnNumber: number | null;
  kind: "model_call" | "tool_call" | "command_reattached" | string;
  callIndex: number | null;
  activityId: string | null;
  toolName: string | null;
  attempt: number | null;
  cause: string | null;
  detectedAt: number | null;
  lostAt: number | null;
  lostWorker: string | null;
  resumedOn: string | null;
  kept: WorkCounts;
  repeated: { modelCalls: number; toolCalls: number; resentInputTokensEstimate: number | null };
  /** Until confirmed from history, the UI says "Checking…". */
  confirmed: boolean;
  /** History showed it wasn't a lost worker (a slow call, a provider error). Not shown. */
  retracted: boolean;
}

/** The recoveries of one turn, as the transcript's divider shows them. */
export interface TurnRecovery {
  agentId: string | null;
  turnId: string | null;
  turnNumber: number | null;
  recoveries: Recovery[];
  /** The most work kept by any recovery in the turn (kept counts are cumulative in a turn). */
  kept: WorkCounts;
  repeatedModelCalls: number;
  repeatedToolCalls: number;
  lostAt: number | null;
  confirmed: boolean;
}

export interface RecoveryTotals {
  /** Turns with at least one non-retracted recovery. */
  count: number;
  /** Sum over those turns of each turn's `kept`. */
  kept: WorkCounts;
  confirmed: boolean;
}

/** Worker liveness for the banner, from `x_worker` (bridge silence + Describe). */
export interface WorkerHealth {
  /** False while the bridge reports "suspect". */
  answering: boolean;
  reason: string | null;
  /** When the current state began (ms since epoch). */
  since: number | null;
  pollers: number | null;
  agentId: string | null;
  turnId: string | null;
}

/** A front-door request the agent refused (`x_rejected`). */
export interface Rejection {
  requestId: string;
  kind: "approval" | "message" | null;
  toolId: string | null;
  messageId: string | null;
  /** unknown_approval, already_resolved, unknown_function, malformed_message, busy,
   *  session_closed or rejected. */
  code: string | null;
  /** Kept for the 3.1 shape: same as `code`. */
  type: string | null;
  /** The agent's reason, for the card. */
  message: string;
}

/** The workspace sandbox (executor), from the sandbox notices a tool publishes on its
 *  progress channel while Temporal retries an executor that isn't answering. Answering until
 *  a notice says otherwise; cleared by an "up" notice or when that tool call ends. */
export interface SandboxHealth {
  answering: boolean;
  /** The activity attempt the notice is about. */
  attempt: number | null;
  maxAttempts: number | null;
  /** False on the last attempt: Temporal won't retry it. */
  willRetry: boolean | null;
  /** When Temporal tries again (ms since epoch, approximate). */
  nextRetryAt: number | null;
  reason: string | null;
  /** The tool's own sentence for the UI. */
  message: string | null;
  /** The tool call the notice came from. */
  toolId: string | null;
}

/** One chunk of a running tool's live output. Untagged (legacy) output is "stdout"; "note"
 *  is a line from us, not the command (reattach, dropped output, limit reached). */
export interface ToolProgressChunk {
  text: string;
  stream: "stdout" | "stderr" | "note";
  /** ms since epoch, from the frame. */
  at: number | null;
}

export interface SessionTelemetry {
  /** Every model call, in the order first seen. */
  calls: ModelCallUsage[];
  /** Session totals over every agent and provider. */
  totals: UsageTotals;
  /** The same totals per provider ("unknown" while a live frame hasn't named it). */
  byProvider: Record<string, UsageTotals>;
  recoveries: Recovery[];
  turnRecoveries: TurnRecovery[];
  recoveryTotals: RecoveryTotals;
  worker: WorkerHealth;
  /** The current Temporal run id per agent id (root and subagents), once the bridge reports it. */
  runIds: Readonly<Record<string, string>>;
  /** The same by workflow id (x_worker `runs`, `workflow_id` + `run_id`). */
  runIdsByWorkflow: Readonly<Record<string, string>>;
  rejections: Rejection[];
  /** Frame types we don't know, by count. Ignored otherwise. */
  unknownFrames: Record<string, number>;
}

// -- the facade -----------------------------------------------------------------------------

export interface ApproveOptions {
  /** "Always approve" (honored for workspace edits only; the agent narrows it otherwise). */
  remember?: boolean;
  reason?: string;
}

export interface CodingSession {
  sessionId: string;
  connection: ConnectionState;
  /** The last connection or action failure, until `clearError()`. */
  error: string | null;
  clearError(): void;
  /** Re-open a stream that gave up (connection "error"). */
  reconnect(): void;

  status: SessionStatus;
  /** Whether the root agent has a turn open. */
  agentStatus: "idle" | "busy";
  /** Whether any agent in the tree has a turn open. */
  anyBusy: boolean;

  /** The root agent's messages, including control messages (steer, set_model, ...). */
  messages: readonly Message[];
  /** Sends this browser made that the stream hasn't admitted yet, and failed sends. */
  unsent: readonly Message[];
  /** Drop a failed send from `unsent`. */
  dismissUnsent(localId: string): void;
  rootAgentId: string | null;
  /** Every subagent in the tree by agent id (the root excluded). */
  subagents: Readonly<Record<string, AgentNode>>;
  /** Any agent by id, root included. */
  agent(agentId: string): AgentNode | undefined;

  plan: PlanState | null;
  changes: ChangesState | null;
  workspace: WorkspaceState | null;

  /** Raw, in the order they started waiting. */
  pendingApprovals: readonly PendingApproval[];
  /** Every live output chunk per tool id, accumulated (the harness keeps only the latest). */
  toolProgress: Readonly<Record<string, string>>;
  toolProgressChunks: Readonly<Record<string, readonly ToolProgressChunk[]>>;
  /** The sandbox's health, from tool progress notices. */
  sandbox: SandboxHealth;
  /** Same as `telemetry.runIds`; the root's is `runIds[rootAgentId]`. */
  runIds: Readonly<Record<string, string>>;
  /** The session workflow's current run id (stream, else GET status), for Temporal UI links. */
  runId: string | null;
  /** A workflow's current run id (the session's or a subagent's), when known. */
  runIdFor(workflowId: string): string | null;
  /** Local actions, oldest first. */
  sends: readonly SendRecord[];
  /** The latest send record for a message id or tool id, if this browser sent it. */
  sendFor(target: { messageId?: string; toolId?: string }): SendRecord | undefined;

  telemetry: SessionTelemetry;

  /** The model the next turn uses (session start, then the latest set_model). */
  model: ModelChoice | null;
  /** True while a set_model sent during the open turn waits for the next turn. */
  modelAppliesNextTurn: boolean;
  /** The model the open turn actually runs on (the previous one while a change waits). */
  modelInUse: ModelChoice | null;
  posture: Posture | null;
  /** Tool names approved with "Always approve" this session (root agent), emptied once a
   *  forget_always_allowed is admitted. */
  alwaysApproved: readonly string[];

  /** When one model call ran (from the stream's timestamps), by its agent, message and its
   *  position among that message's model_interaction parts (0-based). */
  modelCallTiming(agentId: string, messageId: string, index: number): ModelCallTiming | undefined;
  /** A page of a command's stored output (S7), by the `log_ref` in its result. */
  readLog(logRef: string, options?: { offset?: number; stream?: "stdout" | "stderr" | "both" }): Promise<LogPage>;
  /** Upload a file to the session; mention it in the next message so the agent reads it. */
  attach(file: File): Promise<Attachment>;

  /** Send a chat message; `attachments` are upload results (at most 20 are sent). */
  send(text: string, attachments?: readonly Attachment[]): Promise<SendRecord>;
  /** Forget this session's "Always approve" rules. Calls already waiting stay waiting. */
  forgetAlwaysAllowed(): Promise<SendRecord>;
  steer(text: string): Promise<SendRecord>;
  interrupt(reason?: string): Promise<SendRecord>;
  setModel(choice: ModelChoice): Promise<SendRecord>;
  setPosture(posture: Posture): Promise<SendRecord>;
  /** One answer per tool id, each through the Signal front door with its own SendRecord. */
  approve(toolIds: readonly string[], options?: ApproveOptions): Promise<SendRecord[]>;
  decline(toolIds: readonly string[], reason?: string): Promise<SendRecord[]>;
}

/** What `useCodingSession` needs besides the session id. */
export interface SessionSeed {
  /** From the session summary: the model and posture it started with. */
  model?: ModelChoice | null;
  posture?: Posture | null;
  /** Config `tool_risk_classes`, `default_tool_risk_class` and `remember_allowed_classes`. */
  toolRiskClasses?: Readonly<Record<string, string>> | null;
  defaultToolRiskClass?: string | null;
  rememberAllowedClasses?: readonly string[] | null;
}

export interface ModelCallTiming {
  /** ms since epoch. */
  startedAt: number;
  firstThoughtAt: number | null;
  /** The first reply text or tool request out of the call. */
  firstOutputAt: number | null;
  endedAt: number | null;
}

/** GET /api/agent/sessions/{id}/logs/{log_ref}. */
export interface LogPage {
  text: string;
  offset: number;
  next_offset: number | null;
  total_chars: number | null;
  finished: boolean;
  exit_code: number | null;
}

/** POST /api/agent/sessions/{id}/attachments → `attachment`. */
export interface Attachment {
  attachment_id: string;
  name: string;
  mime_type: string;
  size_bytes: number;
  content_kind: string;
  text_preview?: string | null;
}

// -- slot props (Eng W) ---------------------------------------------------------------------

/** Open a file in the Changes tab. `at` changes on every request, so the same path can be
 *  asked for twice. */
export interface WorkbenchFocus {
  tab: "changes";
  path: string;
  at: number;
}

export interface WorkbenchProps {
  session: CodingSession;
  variant: "panel" | "sheet";
  focus?: WorkbenchFocus | null;
}
export interface JudgeCardProps {
  /** The tool call the judge decided, and the evaluation to show. */
  part: ToolPart;
  evaluation: Evaluation;
}
export interface DecisionDockProps {
  session: CodingSession;
}
export interface WorkerBannerProps {
  session: CodingSession;
}
export interface RecoveryDividerProps {
  recovery: TurnRecovery;
}
export interface SendStateTrailProps {
  record: SendRecord;
}
