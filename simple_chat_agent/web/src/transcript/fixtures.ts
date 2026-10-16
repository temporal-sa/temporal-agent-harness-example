// Test fixtures: harness message parts as the client projection builds them.
import type { Message, MessagePart, ModelInteractionPart, ToolPart } from "../session";

export const call = (status: "running" | "done" = "done", over: Partial<ModelInteractionPart> = {}): ModelInteractionPart => ({
  type: "model_interaction",
  model: "claude-sonnet-5-5",
  status,
  usage: null,
  ...over,
});
export const text = (t: string): MessagePart => ({ type: "reply_delta", text: t });
export const thought = (t: string): MessagePart => ({ type: "thought_summary", text: t });
export const tool = (name: string, over: Partial<ToolPart> = {}): ToolPart => ({
  type: "tool",
  toolId: `${name}-${Math.random().toString(36).slice(2, 7)}`,
  toolName: name,
  state: "done",
  input: {},
  output: null,
  error: null,
  progress: null,
  approval: null,
  evaluations: [],
  callback: null,
  ...over,
});
export const message = (parts: MessagePart[], over: Partial<Message> = {}): Message =>
  ({
    id: "m1",
    handler: "chat",
    input: { text: "test_refund fails. Fix it." },
    output: null,
    status: "done",
    disposition: "started",
    turnNumber: 1,
    parts,
    error: null,
    acceptedAt: 1_760_000_000,
    startedAt: 1_760_000_000,
    endedAt: 1_760_000_010,
    ...over,
  }) as Message;
