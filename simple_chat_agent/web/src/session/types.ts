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
/** A gated tool call waiting on a person, anywhere in the tree. Raw: Eng W groups them. */
export type PendingApproval = WaitingToolCall;

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
// Hand-written until Eng F's generated types land in web/src/generated/; then these become
// re-exports of the generated ones.

export type PlanStatus = "pending" | "in_progress" | "done" | "skipped";
export interface PlanItem {
  text: string;
  status: PlanStatus;
}
export interface PlanState {
  goal: string;
  items: PlanItem[];
}

export type FileStatus = "added" | "modified" | "deleted" | "renamed";
export interface ChangedFile {
  path: string;
  status: FileStatus;
  added: number;
  removed: number;
  tool_id: string;
}
export interface Commit {
  hash: string;
  message: string;
}
export interface ChangesState {
  files: ChangedFile[];
  commits: Commit[];
}

export interface WorkspaceState {
  last_command: string;
  last_exit_code: number | null;
  commands_run: number;
}

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
export type SendKind = "message" | "steer" | "interrupt" | "set_model" | "set_posture" | "approval";

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
  error: string | null;
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

/** A front-door request the agent refused (`x_rejected`; not emitted by the bridge yet). */
export interface Rejection {
  requestId: string;
  type: string | null;
  message: string;
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
  /** Local actions, oldest first. */
  sends: readonly SendRecord[];
  /** The latest send record for a message id or tool id, if this browser sent it. */
  sendFor(target: { messageId?: string; toolId?: string }): SendRecord | undefined;

  telemetry: SessionTelemetry;

  /** The model the next turn uses (session start, then the latest set_model). */
  model: ModelChoice | null;
  /** True while a set_model sent during the open turn waits for the next turn. */
  modelAppliesNextTurn: boolean;
  posture: Posture | null;
  /** Tool names approved with "Always approve" this session (from resolved approvals). */
  alwaysApproved: readonly string[];

  send(text: string): Promise<SendRecord>;
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
}

// -- slot props (Eng W) ---------------------------------------------------------------------

export interface WorkbenchProps {
  session: CodingSession;
  variant: "panel" | "sheet";
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
