// Plain JSON calls to our API outside the harness transport: auth, config, the session list
// and session creation. All same-origin. None of these reach a Query-backed path: sessions
// come from Redis, config is static, `/api/me` reads the cookie or catalog headers.

import type { AgentCatalog, ModelChoice, Posture } from "./types";

/** Sent on every request. The API admits a state-changing request on a same-origin Origin or
 *  this header; sending both keeps the dev proxy and odd browsers working. */
export const CLIENT_HEADERS: Readonly<Record<string, string>> = { "X-Requested-With": "coding-agent-web" };

export const AGENT_API_BASE = "/api/agent/";

export class ApiError extends Error {
  readonly status: number;
  readonly code: string | null;
  constructor(message: string, status: number, code: string | null = null) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.code = code;
  }
  /** The API itself didn't answer (network failure or a gateway error in front of it). */
  get unreachable(): boolean {
    return this.status === 0 || this.status === 502 || this.status === 503 || this.status === 504;
  }
}

/** A thrown error that means "the app is down", as opposed to a refusal. */
export function isUnreachable(error: unknown): boolean {
  if (error instanceof ApiError) return error.unreachable;
  const status = (error as { status?: unknown } | null)?.status;
  if (typeof status === "number") return status === 502 || status === 503 || status === 504;
  return error instanceof TypeError; // fetch's network failure
}

async function request<T>(path: string, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(init.headers);
  for (const [k, v] of Object.entries(CLIENT_HEADERS)) headers.set(k, v);
  if (init.body !== undefined && !headers.has("Content-Type")) headers.set("Content-Type", "application/json");
  let response: Response;
  try {
    response = await fetch(path, { ...init, headers, credentials: "same-origin" });
  } catch (err) {
    throw new ApiError(err instanceof Error ? err.message : String(err), 0);
  }
  const text = await response.text();
  let body: unknown = null;
  if (text) {
    try {
      body = JSON.parse(text);
    } catch {
      body = null;
    }
  }
  if (!response.ok) {
    const b = (body ?? {}) as { message?: unknown; detail?: unknown; error?: unknown };
    const message =
      typeof b.message === "string" ? b.message : typeof b.detail === "string" ? b.detail : `${response.status} ${response.statusText}`;
    throw new ApiError(message, response.status, typeof b.error === "string" ? b.error : null);
  }
  return body as T;
}

// -- auth ------------------------------------------------------------------------------------

export interface AuthConfig {
  mode: "catalog" | "local";
  local_enabled: boolean;
}

export interface Me {
  user_id: string;
  username: string;
  temporal_ui_workflows_url: string | null;
}

export const getAuthConfig = () => request<AuthConfig>("/api/auth/config");
export const getMe = () => request<Me>("/api/me");
export const localLogin = (username: string, password: string) =>
  request<{ status: string; username: string }>("/api/auth/local/login", {
    method: "POST",
    body: JSON.stringify({ username, password }),
  });

// -- agent config ------------------------------------------------------------------------------

export interface ViewLink {
  enabled: boolean;
  /** `{session_id}` / `{workflow_id}` placeholders, substituted URL-encoded. */
  url_template: string | null;
  reason: string | null;
}

export interface PostureMenuOption {
  id: Posture;
  label: string;
  description: string;
  available: boolean;
  reason: string | null;
  default: boolean;
}

/** `GET /api/agent/config`. The P3 additions (links, posture_options, auto_mode) are optional
 *  so an older API still renders. */
export interface AgentConfig extends AgentCatalog {
  links?: { console?: ViewLink; temporal_ui?: ViewLink };
  posture_options?: PostureMenuOption[];
  auto_mode?: { available: boolean; judge: string; judge_model: string | null; reason: string | null };
}

export const getAgentConfig = () => request<AgentConfig>(`${AGENT_API_BASE}config`);

export function fillLink(link: ViewLink | undefined, sessionId: string | null): string | null {
  if (!link?.enabled || !link.url_template) return null;
  if (link.url_template.includes("{") && !sessionId) return null;
  const id = encodeURIComponent(sessionId ?? "");
  return link.url_template.replaceAll("{session_id}", id).replaceAll("{workflow_id}", id);
}

// -- sessions ----------------------------------------------------------------------------------

export interface SessionSummary {
  workflow_id: string;
  session_id: string;
  created_at: number;
  title: string;
  label: string;
  closed: boolean;
  execution_status?: string;
  model: Partial<ModelChoice>;
  posture: Posture | null;
  environment: string | null;
  demo: string | null;
}

export const listSessions = () => request<SessionSummary[]>(`${AGENT_API_BASE}sessions`);
export const getSession = (id: string) => request<SessionSummary>(`${AGENT_API_BASE}sessions/${encodeURIComponent(id)}`);

export interface CreateSessionBody {
  demo?: "refund_service";
  title?: string;
  model?: ModelChoice;
  posture?: Posture;
}

export const createSession = (body: CreateSessionBody) =>
  request<SessionSummary>(`${AGENT_API_BASE}sessions`, { method: "POST", body: JSON.stringify(body) });

/** A session summary's model, if it names one. */
export function summaryModel(s: Pick<SessionSummary, "model"> | null | undefined): ModelChoice | null {
  const m = s?.model;
  if (!m?.provider || !m.model) return null;
  return { provider: m.provider, model: m.model, effort: m.effort ?? null };
}
