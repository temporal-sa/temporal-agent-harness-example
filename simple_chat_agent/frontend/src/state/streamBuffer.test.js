import assert from "node:assert/strict";
import test from "node:test";

import {
  coalesceBufferedStreamEvents,
  coalesceStreamEvents,
  latestBufferedStreamCursor,
  takeBufferedStreamEventBatch,
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

test("interleaved tool input deltas coalesce by tool without losing content", () => {
  const toolDelta = (toolUseId, partialJson, cursor) => ({
    workflowId: "chat-1",
    cursor,
    event: {
      kind: "agent_tool_input_delta",
      agent: { id: "main" },
      payload: {
        operation_id: "main:llm:1",
        sequence: 1,
        tool_use_id: toolUseId,
        partial_json: partialJson,
      },
    },
  });
  const pending = [
    toolDelta("tool-1", '{"task":"one', "100-0"),
    toolDelta("tool-2", '{"task":"two', "101-0"),
    toolDelta("tool-1", ' complete"}', "102-0"),
    toolDelta("tool-2", ' complete"}', "103-0"),
  ];

  const coalesced = coalesceBufferedStreamEvents(pending);

  assert.equal(coalesced.length, 2);
  assert.equal(coalesced[0].event.payload.partial_json, '{"task":"one complete"}');
  assert.equal(coalesced[0].cursor, "102-0");
  assert.equal(coalesced[1].event.payload.partial_json, '{"task":"two complete"}');
  assert.equal(coalesced[1].cursor, "103-0");
});

test("non-delta events remain barriers between coalesced delta bursts", () => {
  const events = [
    { kind: "agent_text_delta", payload: { sequence: 1, text: "one" } },
    { kind: "agent_start", payload: { sequence: 2 } },
    { kind: "agent_text_delta", payload: { sequence: 1, text: "two" } },
  ];

  assert.deepEqual(coalesceStreamEvents(events), events);
});

test("render batches leave excess events queued for later animation frames", () => {
  const pending = Array.from({ length: 125 }, (_, index) => ({ cursor: `${index}-0` }));

  const first = takeBufferedStreamEventBatch(pending, 100);

  assert.equal(first.length, 100);
  assert.equal(pending.length, 25);
  assert.equal(pending[0].cursor, "100-0");
});
