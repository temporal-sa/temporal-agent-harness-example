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
  for (const item of pending || []) {
    const previous = coalesced.at(-1);
    const mergedEvent = previous ? mergeDeltaEvents(previous, item) : null;
    if (mergedEvent) {
      coalesced[coalesced.length - 1] = {
        ...previous,
        event: mergedEvent,
        cursor: item.cursor || previous.cursor,
      };
    } else {
      coalesced.push(item);
    }
  }
  return coalesced;
}

function mergeDeltaEvents(leftItem, rightItem) {
  if (leftItem?.workflowId !== rightItem?.workflowId) return null;
  const left = leftItem?.event;
  const right = rightItem?.event;
  if (!left || !right || left.kind !== right.kind) return null;
  const contentKey = {
    agent_text_delta: "text",
    agent_thinking_delta: "thinking",
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

function sameStreamIdentity(left, right) {
  const keys = [
    "provider",
    "operation_id",
    "sequence",
    "attempt",
    "stream_attempt",
    "activity_attempt",
  ];
  if (left.tool_name !== right.tool_name || left.tool_call_id !== right.tool_call_id) {
    return false;
  }
  if (JSON.stringify(left.agent || null) !== JSON.stringify(right.agent || null)) {
    return false;
  }
  return keys.every((key) => left.payload?.[key] === right.payload?.[key]);
}
