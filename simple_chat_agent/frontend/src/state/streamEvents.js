export const AgentStreamEventKind = Object.freeze({
  AGENT_START: "agent_start",
  AGENT_TEXT_DELTA: "agent_text_delta",
  AGENT_THINKING_START: "agent_thinking_start",
  AGENT_THINKING_DELTA: "agent_thinking_delta",
  AGENT_TOOL_INPUT_START: "agent_tool_input_start",
  AGENT_TOOL_INPUT_DELTA: "agent_tool_input_delta",
  AGENT_TOOL_INPUT_COMPLETE: "agent_tool_input_complete",
  AGENT_COMPLETE: "agent_complete",
  AGENT_CANCELLED: "agent_cancelled",
  AGENT_FAILED: "agent_failed",
});

export const AGENT_STREAM_EVENT_PREFIX = "agent_";
export const AGENT_TOOL_INPUT_EVENT_PREFIX = "agent_tool_input_";
