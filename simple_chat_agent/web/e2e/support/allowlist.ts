// Check 3c: every request the product view makes must be on this list. Each entry names the
// API route that serves it (simple_chat_agent/api/agent_routes.py, main.py) and what backs it.
// None of these routes issues a Temporal Query on its normal path; see `queryNote`.

export interface AllowedRoute {
  method: string;
  path: RegExp;
  route: string;
  backing: string;
  /** Set when the route can reach a Query on some path (server-side). */
  queryNote?: string;
}

const ID = "[^/]+";

export const ALLOWED: readonly AllowedRoute[] = [
  { method: "GET", path: /^\/app\/?$/, route: "GET /app/", backing: "static (web/dist/index.html)" },
  { method: "GET", path: /^\/app\/assets\/[\w.-]+$/, route: "GET /app/assets/*", backing: "static (bundled JS, CSS, fonts)" },
  { method: "GET", path: /^\/favicon\.ico$/, route: "GET /favicon.ico", backing: "browser default request" },
  { method: "GET", path: /^\/api\/auth\/config$/, route: "GET /api/auth/config", backing: "process config" },
  { method: "GET", path: /^\/api\/me$/, route: "GET /api/me", backing: "session cookie / catalog headers" },
  { method: "POST", path: /^\/api\/auth\/local\/login$/, route: "POST /api/auth/local/login", backing: "local credentials" },
  { method: "GET", path: /^\/api\/agent\/config$/, route: "GET /api/agent/config", backing: "static catalog and environment" },
  {
    method: "GET",
    path: /^\/api\/agent\/sessions$/,
    route: "GET /api/agent/sessions",
    backing: "Redis session directory",
    queryNote:
      "SessionDirectory.list() falls back to _rebuild(), which runs the `list_sessions` Query on the user's CodingSessions workflow when the user's Redis index is empty (rate-limited to once per REBUILD_BACKOFF_SECONDS per user).",
  },
  { method: "POST", path: /^\/api\/agent\/sessions$/, route: "POST /api/agent/sessions", backing: "Redis + StartWorkflow (+ Describe on retry), executor seed" },
  {
    method: "GET",
    path: new RegExp(`^/api/agent/sessions/${ID}$`),
    route: "GET /api/agent/sessions/{id}",
    backing: "Redis session directory",
    queryNote: "SessionDirectory.get() calls the same _rebuild() (`list_sessions` Query) when the user's Redis index is gone.",
  },
  { method: "POST", path: new RegExp(`^/api/agent/sessions/${ID}/attachments$`), route: "POST /api/agent/sessions/{id}/attachments", backing: "app store" },
  { method: "GET", path: new RegExp(`^/api/agent/sessions/${ID}/logs/${ID}$`), route: "GET /api/agent/sessions/{id}/logs/{ref}", backing: "executor log read" },
  { method: "GET", path: /^\/api\/agent\/attach$/, route: "GET /api/agent/attach", backing: "Redis stream (bridge; Describe only)" },
  { method: "POST", path: /^\/api\/agent\/messages$/, route: "POST /api/agent/messages", backing: "Signal enqueue_message" },
  { method: "POST", path: /^\/api\/agent\/approve$/, route: "POST /api/agent/approve", backing: "Signal enqueue_approval (subagent: Update)" },
  { method: "GET", path: new RegExp(`^/api/agent/status/${ID}$`), route: "GET /api/agent/status/{id}", backing: "Redis status meta" },
  { method: "GET", path: new RegExp(`^/api/agent/workflow-status/${ID}$`), route: "GET /api/agent/workflow-status/{id}", backing: "Describe" },
];

/** Paths that are known to issue Queries (the old frontend's routes). Called out by name. */
export const QUERY_BACKED: readonly RegExp[] = [
  /^\/api\/chats?\b/, // main.py: SimpleChatWorkflow.state / snapshot / transcript_page, UserChatsWorkflow.list_chats
  /^\/api\/conversations?\b/,
  /^\/console\/api\//, // the harness console's own routes
  /^\/api\/agent\/agent-interface\//,
];

export interface SeenRequest {
  method: string;
  url: string;
  resourceType: string;
}

export interface Verdict {
  request: SeenRequest;
  allowed: AllowedRoute | null;
  reason: string | null;
}

export function judge(r: SeenRequest, origin: string | readonly string[]): Verdict {
  const origins = typeof origin === "string" ? [origin] : origin;
  const u = new URL(r.url);
  if (u.protocol === "data:" || u.protocol === "blob:" || u.protocol === "about:") return { request: r, allowed: ALLOWED[0]!, reason: null };
  if (!origins.includes(u.origin)) return { request: r, allowed: null, reason: `other origin ${u.origin}` };
  const hit = ALLOWED.find((a) => a.method === r.method && a.path.test(u.pathname));
  if (hit) return { request: r, allowed: hit, reason: null };
  const query = QUERY_BACKED.some((q) => q.test(u.pathname));
  return { request: r, allowed: null, reason: query ? "QUERY-BACKED path" : "not on the allowlist" };
}
