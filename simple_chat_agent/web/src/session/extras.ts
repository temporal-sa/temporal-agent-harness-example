// P4 extras the facade derives from the stream (PRD A8, A11, A12, provider web search). Pure,
// so each rule is tested without React or a connection. The shapes come from
// docs/phases/P4-ui-contract.md.

import type { Message, Rejection, ToolPart } from "./types";

const isObj = (v: unknown): v is Record<string, unknown> => typeof v === "object" && v !== null && !Array.isArray(v);
const str = (v: unknown): string => (typeof v === "string" ? v : "");

// -- Code Mode (run_script) -----------------------------------------------------------------

export const SCRIPT_TOOL = "run_script";

/** The script tool id a host call belongs to (`<script id>/<n>`, n from 1), else null. */
export function hostCallParent(toolId: string): string | null {
  const m = /^(.+)\/(\d+)$/.exec(toolId);
  return m && Number(m[2]) >= 1 ? m[1]! : null;
}

/** A host call's position under its script (1-based), else null. */
export function hostCallIndex(toolId: string): number | null {
  const m = /\/(\d+)$/.exec(toolId);
  return m ? Number(m[1]) : null;
}

/**
 * Split a message's parts into the parts the transcript draws as rows and each script's host
 * calls (by script tool id, in call order). A host call whose script isn't in the message
 * stays a row of its own.
 */
export function nestHostCalls<P extends Message["parts"][number]>(parts: readonly P[]): { parts: P[]; hostCalls: Map<string, ToolPart[]> } {
  const ids = new Set<string>();
  for (const p of parts) if (p.type === "tool") ids.add(p.toolId);
  const hostCalls = new Map<string, ToolPart[]>();
  const rest: P[] = [];
  for (const p of parts) {
    const parent = p.type === "tool" ? hostCallParent(p.toolId) : null;
    if (p.type === "tool" && parent && ids.has(parent)) {
      const list = hostCalls.get(parent) ?? [];
      list.push(p);
      hostCalls.set(parent, list);
    } else {
      rest.push(p);
    }
  }
  for (const list of hostCalls.values()) list.sort((a, b) => (hostCallIndex(a.toolId) ?? 0) - (hostCallIndex(b.toolId) ?? 0));
  return { parts: rest, hostCalls };
}

/** A script that failed to type-check or raised comes back as tool output "Script error (...)". */
export function scriptError(part: Pick<ToolPart, "toolName" | "output">): string | null {
  if (part.toolName !== SCRIPT_TOOL) return null;
  const out = part.output ?? "";
  return out.startsWith("Script error") ? out : null;
}

// -- MCP --------------------------------------------------------------------------------------

export const MCP_REAUTH = "mcp_reauth_required:";

/** `mcp__<server>__<tool>` → its server and tool, else null. */
export function mcpTool(toolName: string): { server: string; tool: string } | null {
  // Server names are 1-24 of a-z, 0-9 and "-" (agent_mcp.py), so the first "__" ends one.
  const m = /^mcp__([a-z][a-z0-9-]*)__(.+)$/.exec(toolName);
  return m ? { server: m[1]!, tool: m[2]! } : null;
}

/** How a tool's name reads: "local-echo · echo" for an MCP tool, the name otherwise. */
export function toolDisplayName(toolName: string): string {
  const m = mcpTool(toolName);
  return m ? `${m.server} · ${m.tool}` : toolName;
}

/** A tool's own arguments: MCP calls wrap them as `{"arguments": {...}}`. */
export function toolArgs(toolName: string, input: Record<string, unknown> | null | undefined): Record<string, unknown> {
  const i = input ?? {};
  if (mcpTool(toolName) && isObj(i.arguments)) return i.arguments;
  return i;
}

/** The server to reconnect when a failed call says it needs reauth, else null. */
export function reauthServer(part: Pick<ToolPart, "toolName" | "state" | "error">): string | null {
  if (part.state !== "failed" || !(part.error ?? "").startsWith(MCP_REAUTH)) return null;
  const named = /MCP server '([^']+)'/.exec(part.error ?? "")?.[1];
  return mcpTool(part.toolName)?.server ?? named ?? null;
}

// -- web search -------------------------------------------------------------------------------

export interface WebSource {
  title: string;
  url: string;
  domain: string;
}

/** The reply's sources (ChatReply.sources): http(s) links only, at most 20, no repeats. */
export function replySources(output: unknown): WebSource[] {
  if (!isObj(output) || !Array.isArray(output.sources)) return [];
  const out: WebSource[] = [];
  const seen = new Set<string>();
  for (const s of output.sources) {
    if (!isObj(s)) continue;
    let url: URL;
    try {
      url = new URL(str(s.url));
    } catch {
      continue;
    }
    if (url.protocol !== "http:" && url.protocol !== "https:") continue;
    if (seen.has(url.href)) continue;
    seen.add(url.href);
    const domain = url.hostname.replace(/^www\./, "");
    out.push({ url: url.href, domain, title: str(s.title).trim() || domain });
    if (out.length >= 20) break;
  }
  return out;
}

/** Whether web search is on for the next turn: the start value, then each set_web_search the
 *  agent admitted (its ack's value when it has replied). */
export function webSearchOn(messages: readonly Message[], seed: boolean): boolean {
  let on = seed;
  for (const m of messages) {
    if (m.handler !== "set_web_search" || m.acceptedAt === null || m.status === "error") continue;
    const out = isObj(m.output) ? m.output : null;
    if (m.status === "done" && out && typeof out.web_search === "boolean") on = out.web_search;
    else if (isObj(m.input) && typeof m.input.enabled === "boolean") on = m.input.enabled;
  }
  return on;
}

// -- callbacks (ask_user, pick_files) -----------------------------------------------------------

export interface ChoiceOption {
  id: string;
  label: string;
  description: string | null;
}

export type CallbackRequest =
  | { kind: "ask_user"; question: string; options: ChoiceOption[] }
  | { kind: "pick_files"; prompt: string; maxFiles: number }
  | { kind: "other"; toolName: string };

export const ASK_MAX_OPTIONS = 6;
export const PICK_MAX_FILES = 10;
export const PICK_MAX_BYTES = 5 * 1024 * 1024;

/** What a waiting callback asks for, from its tool input. */
export function callbackRequest(toolName: string, input: Record<string, unknown> | null | undefined): CallbackRequest {
  const i = input ?? {};
  if (toolName === "ask_user") {
    const options: ChoiceOption[] = [];
    for (const o of Array.isArray(i.options) ? i.options : []) {
      if (!isObj(o) || !str(o.id)) continue;
      options.push({ id: str(o.id), label: str(o.label) || str(o.id), description: str(o.description) || null });
      if (options.length >= ASK_MAX_OPTIONS) break;
    }
    return { kind: "ask_user", question: str(i.question), options };
  }
  if (toolName === "pick_files") {
    const n = typeof i.max_files === "number" && Number.isFinite(i.max_files) ? Math.floor(i.max_files) : 5;
    return { kind: "pick_files", prompt: str(i.prompt), maxFiles: Math.min(PICK_MAX_FILES, Math.max(1, n)) };
  }
  return { kind: "other", toolName };
}

/** Files the picker can take: within the count and size limits. The rest, with why. */
export function screenFiles<F extends { name: string; size: number }>(
  files: readonly F[],
  room: number,
): { accepted: F[]; refused: { file: F; reason: string }[] } {
  const accepted: F[] = [];
  const refused: { file: F; reason: string }[] = [];
  for (const f of files) {
    if (f.size > PICK_MAX_BYTES) refused.push({ file: f, reason: "Over 5 MB. Each file can be at most 5 MB." });
    else if (accepted.length >= room) refused.push({ file: f, reason: "Over the limit for this request." });
    else accepted.push(f);
  }
  return { accepted, refused };
}

/** This browser's last answer to one callback. Sent with our own request id, which the
 *  agent's `x_rejected` echoes when it refuses the answer. */
export interface CallbackSend {
  toolId: string;
  requestId: string;
  phase: "sending" | "sent" | "failed";
  action: "answer" | "decline";
  error: string | null;
  errorCode: string | null;
  errorStatus: number | null;
}

export type CallbackStatus =
  | { state: "open" }
  | { state: "sending"; action: "answer" | "decline" }
  | { state: "sent"; action: "answer" | "decline" }
  | { state: "failed"; error: string | null; code: string | null; status: number | null }
  | { state: "rejected"; code: string | null; reason: string };

/** Where one waiting callback stands: open, sending, sent (waiting for the agent), failed to
 *  send, or refused by the agent (`x_rejected` kind callback; the card stays open for another
 *  answer). */
export function callbackStatus(send: CallbackSend | undefined, rejections: readonly Rejection[]): CallbackStatus {
  if (!send) return { state: "open" };
  if (send.phase === "sending") return { state: "sending", action: send.action };
  if (send.phase === "failed") return { state: "failed", error: send.error, code: send.errorCode, status: send.errorStatus };
  const r = rejections.find((x) => x.requestId === send.requestId);
  if (r) return { state: "rejected", code: r.code, reason: r.message };
  return { state: "sent", action: send.action };
}
