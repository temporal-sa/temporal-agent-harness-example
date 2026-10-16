// The mocked API for the `mocked` project: every JSON route the app calls, answered with
// page.route in the shapes simple_chat_agent/api/agent_routes.py and main.py return. The
// attach stream is served by mock-server.mjs (scripted per session through its control routes).

import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import type { APIRequestContext, Page, Request, Route } from "@playwright/test";

import type { Frame } from "./frames";

const here = dirname(fileURLToPath(import.meta.url));
/** GET /api/agent/config as the API builds it for AGENT_ENVIRONMENT=local (generated from
 *  agent_routes.config() with no keys, AGENT_ENVIRONMENT=local). */
export const AGENT_CONFIG: Record<string, unknown> = JSON.parse(readFileSync(join(here, "../fixtures/agent-config.json"), "utf8"));

export interface Summary {
  workflow_id: string;
  session_id: string;
  created_at: number;
  title: string;
  label: string;
  closed: boolean;
  model: { provider: string; model: string; effort?: string | null };
  posture: string | null;
  environment: string | null;
  demo: string | null;
}

export function summary(id: string, over: Partial<Summary> = {}): Summary {
  return {
    workflow_id: id,
    session_id: id,
    created_at: Date.now() / 1000 - 120,
    title: "Refund service (planted bug)",
    label: "Refund service (planted bug)",
    closed: false,
    model: { provider: "anthropic", model: "claude-sonnet-5-5", effort: "medium" },
    posture: "auto",
    environment: "local",
    demo: "refund_service",
    ...over,
  };
}

export interface Captured {
  method: string;
  path: string;
  body: Record<string, unknown> | null;
  at: number;
}

export interface MockOptions {
  /** Sessions in the list (the first is opened when `open` is set). */
  sessions?: Summary[];
  /** Start signed out: /api/me answers 401 until the local login succeeds. */
  signedOut?: boolean;
  /** Delay (ms) before POST messages / approve answer. */
  postDelayMs?: number;
  /** What POST /api/agent/messages answers with; default 202-style landed reply. */
  onMessage?(body: Record<string, unknown>, messageId: string): Promise<void> | void;
  onApprove?(body: Record<string, unknown>): Promise<void> | void;
  /** A session created through POST /sessions. */
  created?: Summary;
}

let ids = 0;

export class MockApi {
  readonly calls: Captured[] = [];
  sessions: Summary[];
  signedIn: boolean;
  status = { current_turn: 1, turn_active: true, pending_turns: [] as unknown[], run_id: "run-1", worker: null as unknown };

  constructor(
    private readonly page: Page,
    private readonly request: APIRequestContext,
    private readonly baseURL: string,
    private readonly opts: MockOptions = {},
  ) {
    this.sessions = [...(opts.sessions ?? [])];
    this.signedIn = !opts.signedOut;
  }

  /** Requests of one route, by method and a path prefix. */
  bodies(method: string, path: string): Record<string, unknown>[] {
    return this.calls.filter((c) => c.method === method && c.path.startsWith(path)).map((c) => c.body ?? {});
  }

  // -- the scripted stream --------------------------------------------------------------------

  async script(sessionId: string, frames: Frame[]): Promise<void> {
    const r = await this.request.put(`${this.baseURL}/__mock/sessions/${encodeURIComponent(sessionId)}/frames`, { data: { frames } });
    if (!r.ok()) throw new Error(`mock script failed: ${r.status()}`);
  }

  async push(sessionId: string, frames: Frame[]): Promise<void> {
    const r = await this.request.post(`${this.baseURL}/__mock/sessions/${encodeURIComponent(sessionId)}/frames`, { data: { frames } });
    if (!r.ok()) throw new Error(`mock push failed: ${r.status()}`);
  }

  // -- routes -----------------------------------------------------------------------------------

  async install(): Promise<this> {
    await this.page.route(/\/api\//, (route) => this.handle(route));
    return this;
  }

  private json(route: Route, status: number, body: unknown) {
    return route.fulfill({ status, contentType: "application/json", headers: { "Cache-Control": "no-store" }, body: JSON.stringify(body) });
  }

  private record(req: Request, path: string): Captured {
    let body: Record<string, unknown> | null = null;
    try {
      body = req.postDataJSON() as Record<string, unknown> | null;
    } catch {
      body = null;
    }
    const c = { method: req.method(), path, body, at: Date.now() };
    this.calls.push(c);
    return c;
  }

  private async handle(route: Route): Promise<void> {
    const req = route.request();
    const url = new URL(req.url());
    const path = url.pathname;
    const method = req.method();
    const call = this.record(req, path);

    // The stream: served by the mock server.
    if (path === "/api/agent/attach") return route.fallback();

    if (path === "/api/auth/config") return this.json(route, 200, { mode: "local", local_enabled: true });
    if (path === "/api/me") {
      if (!this.signedIn) return this.json(route, 401, { detail: "Not signed in" });
      return this.json(route, 200, { user_id: "local:demo", username: "demo", temporal_ui_workflows_url: null });
    }
    if (path === "/api/auth/local/login" && method === "POST") {
      this.signedIn = true;
      return this.json(route, 200, { status: "ok", user_id: "local:demo", username: "demo" });
    }
    if (path === "/api/agent/config") return this.json(route, 200, AGENT_CONFIG);

    if (path === "/api/agent/sessions" && method === "GET") return this.json(route, 200, this.sessions);
    if (path === "/api/agent/sessions" && method === "POST") {
      const created = this.opts.created ?? summary(`sess-created-${++ids}`);
      this.sessions = [created, ...this.sessions.filter((s) => s.session_id !== created.session_id)];
      return this.json(route, 200, { ...created, seed: { fixture: "refund_service", files: 9, commit: "abc1234" } });
    }
    let m = /^\/api\/agent\/sessions\/([^/]+)$/.exec(path);
    if (m && method === "GET") {
      const s = this.sessions.find((x) => x.session_id === decodeURIComponent(m![1]!));
      return s ? this.json(route, 200, s) : this.json(route, 404, { error: "not_found", message: "Not found." });
    }
    m = /^\/api\/agent\/sessions\/([^/]+)\/logs\/([^/]+)$/.exec(path);
    if (m) return this.json(route, 200, { text: "..........\n10 passed in 19.6s\n", offset: 0, next_offset: null, total_chars: 31, finished: true, exit_code: 0 });
    m = /^\/api\/agent\/sessions\/([^/]+)\/attachments$/.exec(path);
    if (m && method === "POST") {
      return this.json(route, 200, { attachment: { attachment_id: `att-${++ids}`, name: "notes.txt", mime_type: "text/plain", size_bytes: 12, content_kind: "text", text_preview: "hello" } });
    }
    m = /^\/api\/agent\/status\/([^/]+)$/.exec(path);
    if (m) return this.json(route, 200, this.status);
    m = /^\/api\/agent\/workflow-status\/([^/]+)$/.exec(path);
    if (m) return this.json(route, 200, { workflow_id: decodeURIComponent(m[1]!), execution_status: "RUNNING", closed: false });

    if (path === "/api/agent/messages" && method === "POST") {
      if (this.opts.postDelayMs) await new Promise((r) => setTimeout(r, this.opts.postDelayMs));
      const messageId = `msg-sent-${++ids}`;
      await this.opts.onMessage?.(call.body ?? {}, messageId);
      return this.json(route, 200, { message_id: messageId, turn_id: "", turn_number: 0, accepted_offset: 0, disposition: "queued", landed: true });
    }
    if (path === "/api/agent/approve" && method === "POST") {
      if (this.opts.postDelayMs) await new Promise((r) => setTimeout(r, this.opts.postDelayMs));
      await this.opts.onApprove?.(call.body ?? {});
      return this.json(route, 200, { tool_id: call.body?.tool_id ?? null, accepted: true });
    }
    return this.json(route, 404, { error: "not_mocked", message: `${method} ${path} is not mocked` });
  }
}
