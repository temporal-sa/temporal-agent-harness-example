// Walks the session's agent tree for the workbench, decisions and recovery views. Pure.
// Everything here reads the facade (`../session`) only.

import type { AgentNode, CodingSession, Message, ToolPart } from "../session";

/** A tool call with where it came from. `seq` is its position in a stable walk of the tree:
 *  the root's messages first, then each subagent's, each in stream order. */
export interface LocatedTool {
  part: ToolPart;
  agentId: string;
  /** null for the root agent; "explorer", "reviewer", ... for a subagent. */
  agentKey: string | null;
  messageId: string;
  /** The message's handler status: an open message is "accepted" or "running". */
  messageStatus: Message["status"];
  turnNumber: number | null;
  seq: number;
}

export type SessionTree = Pick<CodingSession, "rootAgentId" | "subagents" | "agent">;

/** The root (if known) and every subagent, root first. */
export function agentsOf(session: SessionTree): { id: string; node: AgentNode }[] {
  const out: { id: string; node: AgentNode }[] = [];
  const root = session.rootAgentId ? session.agent(session.rootAgentId) : undefined;
  if (root && session.rootAgentId) out.push({ id: session.rootAgentId, node: root });
  for (const [id, node] of Object.entries(session.subagents)) out.push({ id, node });
  return out;
}

/** Every tool call in the tree, root first, each agent's in stream order. */
export function toolsOf(session: SessionTree): LocatedTool[] {
  const out: LocatedTool[] = [];
  let seq = 0;
  for (const { id, node } of agentsOf(session)) {
    for (const m of node.messages) {
      for (const p of m.parts) {
        if (p.type !== "tool") continue;
        out.push({
          part: p,
          agentId: id,
          agentKey: node.agentKey,
          messageId: m.id,
          messageStatus: m.status,
          turnNumber: m.turnNumber,
          seq: seq++,
        });
      }
    }
  }
  return out;
}

/** "main agent", or the subagent's kind ("Explorer"). */
export function agentLabel(agentKey: string | null): string {
  if (!agentKey) return "main agent";
  const words = agentKey.replace(/[_-]+/g, " ").trim();
  return words ? words[0]!.toUpperCase() + words.slice(1) : "a subagent";
}

/** A string argument, or "" when absent or not a string. */
export function strArg(input: Record<string, unknown> | null | undefined, key: string): string {
  const v = input?.[key];
  return typeof v === "string" ? v : "";
}

/** Whether any agent in the tree is mid model call or mid tool call. */
export function anythingRunning(session: SessionTree): boolean {
  for (const { node } of agentsOf(session)) {
    for (const m of node.messages) {
      if (m.status !== "running" && m.status !== "accepted") continue;
      for (const p of m.parts) {
        if (p.type === "tool" && (p.state === "running" || p.state === "awaiting_callback")) return true;
        if (p.type === "model_interaction" && p.status === "running") return true;
      }
    }
  }
  return false;
}
