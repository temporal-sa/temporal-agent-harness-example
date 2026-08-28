import assert from "node:assert/strict";
import test from "node:test";

import {
  coalesceBufferedStreamEvents,
  latestBufferedStreamCursor,
} from "./streamBuffer.js";

test("the reconnect cursor advances only through events applied to the active workflow", () => {
  const pending = [
    { workflowId: "chat-1", cursor: "100-0" },
    { workflowId: "chat-2", cursor: "200-0" },
    { workflowId: "chat-1", cursor: "101-0" },
  ];

  assert.equal(latestBufferedStreamCursor(pending, "chat-1"), "101-0");
  assert.equal(latestBufferedStreamCursor(pending, "chat-2"), "200-0");
  assert.equal(latestBufferedStreamCursor(pending, "chat-3"), "");
});

test("adjacent model deltas are coalesced without losing the newest cursor", () => {
  const pending = [
    {
      workflowId: "chat-1",
      cursor: "100-0",
      event: {
        kind: "agent_text_delta",
        agent: { id: "main" },
        payload: { operation_id: "main:llm:1", sequence: 1, text: "Hel" },
      },
    },
    {
      workflowId: "chat-1",
      cursor: "101-0",
      event: {
        kind: "agent_text_delta",
        agent: { id: "main" },
        payload: { operation_id: "main:llm:1", sequence: 1, text: "lo" },
      },
    },
  ];

  const coalesced = coalesceBufferedStreamEvents(pending);

  assert.equal(coalesced.length, 1);
  assert.equal(coalesced[0].event.payload.text, "Hello");
  assert.equal(coalesced[0].cursor, "101-0");
});

test("deltas from different operations retain their boundary", () => {
  const pending = [
    {
      workflowId: "chat-1",
      cursor: "100-0",
      event: {
        kind: "agent_text_delta",
        payload: { operation_id: "main:llm:1", sequence: 1, text: "one" },
      },
    },
    {
      workflowId: "chat-1",
      cursor: "101-0",
      event: {
        kind: "agent_text_delta",
        payload: { operation_id: "main:llm:2", sequence: 2, text: "two" },
      },
    },
  ];

  assert.equal(coalesceBufferedStreamEvents(pending).length, 2);
});
