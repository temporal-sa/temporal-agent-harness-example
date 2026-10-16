// The mocked project's server: the built app (web/dist) at /app/, and a scripted SSE stream at
// /api/agent/attach. Every other /api route is mocked in the page with page.route (see
// mock-api.ts); attach falls through to here because page.route can only fulfill a whole body,
// and a real attach stays open while a turn runs.
//
// Tests script a session's stream through control routes (never called by the app):
//   PUT  /__mock/sessions/:id/frames   {frames: [{event, data}]}  replace the session's frames
//   POST /__mock/sessions/:id/frames   {frames: [...]}            append and push to open streams
//   GET  /__mock/sessions/:id/attaches                             how many attaches were opened
//
// Frames get `resume_offset` here, the way the bridge stores them: a harness frame advances the
// root stream position by one and carries `event_offset = resume_offset - 1`; our x_* frames sit
// at the position of the frame before them and carry no event_offset (agent_telemetry.py).
//
//   node e2e/support/mock-server.mjs   (E2E_MOCK_PORT, default 5179; E2E_WEB_DIST, default web/dist)

import { createServer } from "node:http";
import { readFile, stat } from "node:fs/promises";
import { extname, join, normalize, resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
// E2E_WEB_DIST: serve another build (e.g. one made from a clean checkout).
const DIST = resolve(process.env.E2E_WEB_DIST || resolve(here, "../../dist"));
const PORT = Number(process.env.E2E_MOCK_PORT ?? 5179);

const TYPES = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".woff2": "font/woff2",
  ".map": "application/json",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".json": "application/json",
};

/** session id -> { frames: [{event, data}], clients: Set<res>, attaches: number } */
const sessions = new Map();

function session(id) {
  let s = sessions.get(id);
  if (!s) {
    s = { frames: [], clients: new Set(), attaches: 0, position: 0, offsets: new Map() };
    sessions.set(id, s);
  }
  return s;
}

const isTelemetry = (event) => event.startsWith("x_");

/** Stamp a frame with its stream position, as the bridge stores it. */
function place(s, frame) {
  const data = { ...frame.data };
  if (!isTelemetry(frame.event) && data.resume_offset === undefined) {
    s.position += 1;
    // event_offset is the frame's offset in its own agent's log.
    const agent = String(data.agent_id ?? "");
    const next = s.offsets.get(agent) ?? 0;
    s.offsets.set(agent, next + 1);
    if (data.event_offset === undefined) data.event_offset = next;
  }
  if (data.resume_offset === undefined) data.resume_offset = s.position;
  return { event: frame.event, data };
}

function sse(frame) {
  return `event: ${frame.event}\ndata: ${JSON.stringify(frame.data)}\n\n`;
}

async function body(req) {
  const chunks = [];
  for await (const c of req) chunks.push(c);
  const text = Buffer.concat(chunks).toString("utf8");
  return text ? JSON.parse(text) : {};
}

function json(res, status, value) {
  res.writeHead(status, { "Content-Type": "application/json", "Cache-Control": "no-store" });
  res.end(JSON.stringify(value));
}

async function serveStatic(res, path) {
  const rel = normalize(path.replace(/^\/app\/?/, "")).replace(/^(\.\.(\/|\\|$))+/, "");
  let file = join(DIST, rel || "index.html");
  if (!file.startsWith(DIST)) return json(res, 404, { error: "not_found" });
  try {
    const st = await stat(file);
    if (st.isDirectory()) file = join(file, "index.html");
  } catch {
    // SPA fallback, like the API's /app/{path} route (assets/ 404s there).
    if (rel.startsWith("assets/")) return json(res, 404, { error: "not_found" });
    file = join(DIST, "index.html");
  }
  try {
    const content = await readFile(file);
    res.writeHead(200, { "Content-Type": TYPES[extname(file)] ?? "application/octet-stream", "Cache-Control": "no-cache" });
    res.end(content);
  } catch {
    json(res, 503, { error: "not_built", message: "Run npm run build first." });
  }
}

const server = createServer(async (req, res) => {
  const url = new URL(req.url ?? "/", `http://${req.headers.host}`);
  const path = url.pathname;
  try {
    if (path === "/__mock/health") return json(res, 200, { ok: true });

    const control = /^\/__mock\/sessions\/([^/]+)\/(frames|attaches)$/.exec(path);
    if (control) {
      const s = session(decodeURIComponent(control[1]));
      if (control[2] === "attaches") return json(res, 200, { attaches: s.attaches, open: s.clients.size });
      const { frames = [] } = await body(req);
      if (req.method === "PUT") {
        s.frames = [];
        s.position = 0;
        s.offsets = new Map();
      }
      const placed = frames.map((f) => place(s, f));
      s.frames.push(...placed);
      if (req.method === "POST") for (const client of s.clients) for (const f of placed) client.write(sse(f));
      return json(res, 200, { count: s.frames.length, position: s.position });
    }

    if (path === "/api/agent/attach") {
      const id = url.searchParams.get("session_id") ?? "";
      const from = Number(url.searchParams.get("from_offset") ?? 0);
      if (!sessions.has(id)) return json(res, 404, { error: "not_found", message: "No such session." });
      const s = session(id);
      s.attaches += 1;
      res.writeHead(200, { "Content-Type": "text/event-stream", "Cache-Control": "no-store", Connection: "keep-alive" });
      // Replay includes the frames at from_offset; the client dedups (agent_routes._frames).
      for (const f of s.frames) if (f.data.resume_offset >= from) res.write(sse(f));
      s.clients.add(res);
      const keepAlive = setInterval(() => res.write(": keep-alive\n\n"), 10_000);
      req.on("close", () => {
        clearInterval(keepAlive);
        s.clients.delete(res);
      });
      return;
    }

    if (path === "/app") {
      res.writeHead(307, { Location: `/app/${url.search}` });
      return res.end();
    }
    if (path.startsWith("/app/")) return serveStatic(res, path);
    // Everything else under /api is mocked in the page; reaching here is a gap in the mock.
    return json(res, 404, { error: "not_mocked", message: `${req.method} ${path} is not mocked.` });
  } catch (err) {
    return json(res, 500, { error: "mock_server", message: String(err) });
  }
});

server.listen(PORT, "127.0.0.1", () => {
  console.log(`mock server on http://127.0.0.1:${PORT}/app/ (dist: ${DIST})`);
});
