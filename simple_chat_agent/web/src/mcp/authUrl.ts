// Where the browser may go to sign in to an MCP server (P5). The URL comes from the server's
// OAuth metadata, through our API, so check it before leaving the page: https only, plus http
// on this machine (a local test server). Never javascript:, data: or anything else.

const LOOPBACK = new Set(["localhost", "127.0.0.1", "[::1]"]);

/** The URL to open, or null if it isn't one we'll navigate to. */
export function safeAuthUrl(raw: unknown): string | null {
  if (typeof raw !== "string" || raw.length === 0) return null;
  let url: URL;
  try {
    url = new URL(raw);
  } catch {
    return null;
  }
  if (url.protocol === "https:") return url.href;
  if (url.protocol === "http:" && LOOPBACK.has(url.hostname)) return url.href;
  return null;
}
