export const STREAM_EVENT_RENDER_BATCH_SIZE = 100;

export function latestBufferedStreamCursor(pending, workflowId) {
  let cursor = "";
  for (const item of pending || []) {
    if (item?.workflowId && item.workflowId !== workflowId) continue;
    if (item?.cursor) cursor = item.cursor;
  }
  return cursor;
}

export function coalesceBufferedStreamEvents(pending) {
  const coalesced = [];
  const mergeIndexes = new Map();
  for (const item of pending || []) {
    const mergeKey = deltaMergeKey(item);
    if (!mergeKey) {
      mergeIndexes.clear();
      coalesced.push(item);
      continue;
    }

    const previousIndex = mergeIndexes.get(mergeKey);
    const previous = previousIndex === undefined ? null : coalesced[previousIndex];
    const mergedEvent = previous ? mergeDeltaEvents(previous, item) : null;
    if (mergedEvent) {
      coalesced[previousIndex] = {
        ...previous,
        event: mergedEvent,
        cursor: item.cursor || previous.cursor,
      };
    } else {
      mergeIndexes.set(mergeKey, coalesced.length);
      coalesced.push(item);
    }
  }
  return coalesced;
}

export function coalesceStreamEvents(events) {
  return coalesceBufferedStreamEvents(
    (events || []).map((event) => ({ event })),
  ).map((item) => item.event);
}

export function takeBufferedStreamEventBatch(
  pending,
  limit = STREAM_EVENT_RENDER_BATCH_SIZE,
) {
  return pending.splice(0, Math.max(1, limit));
}

function mergeDeltaEvents(leftItem, rightItem) {
  if (leftItem?.workflowId !== rightItem?.workflowId) return null;
  const left = leftItem?.event;
  const right = rightItem?.event;
  if (!left || !right || left.kind !== right.kind) return null;
  const contentKey = {
    agent_text_delta: "text",
    agent_thinking_delta: "thinking",
    agent_tool_input_delta: "partial_json",
  }[left.kind];
  if (!contentKey) return null;
  if (!sameStreamIdentity(left, right)) return null;
  const leftText = left.payload?.[contentKey];
  const rightText = right.payload?.[contentKey];
  if (typeof leftText !== "string" || typeof rightText !== "string") return null;
  return {
    ...left,
    payload: {
      ...left.payload,
      [contentKey]: leftText + rightText,
    },
  };
}

function deltaMergeKey(item) {
  const event = item?.event;
  const contentKey = {
    agent_text_delta: "text",
    agent_thinking_delta: "thinking",
    agent_tool_input_delta: "partial_json",
  }[event?.kind];
  if (!contentKey || typeof event?.payload?.[contentKey] !== "string") return null;
  return JSON.stringify([
    item.workflowId || "",
    event.kind,
    event.tool_name || "",
    event.tool_call_id || "",
    event.agent || null,
    event.payload?.provider,
    event.payload?.operation_id,
    event.payload?.sequence,
    event.payload?.attempt,
    event.payload?.stream_attempt,
    event.payload?.activity_attempt,
    event.payload?.tool_use_id,
    event.payload?.content_block_index,
  ]);
}

function sameStreamIdentity(left, right) {
  const keys = [
    "provider",
    "operation_id",
    "sequence",
    "attempt",
    "stream_attempt",
    "activity_attempt",
    "tool_use_id",
    "content_block_index",
  ];
  if (left.tool_name !== right.tool_name || left.tool_call_id !== right.tool_call_id) {
    return false;
  }
  if (JSON.stringify(left.agent || null) !== JSON.stringify(right.agent || null)) {
    return false;
  }
  return keys.every((key) => left.payload?.[key] === right.payload?.[key]);
}
