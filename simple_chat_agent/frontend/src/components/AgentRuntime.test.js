import assert from "node:assert/strict";
import test from "node:test";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";

import { createServer } from "vite";

test("the runtime appears immediately while an accepted turn awaits stream events", async (t) => {
  const server = await createServer({
    server: { middlewareMode: true },
    appType: "custom",
  });
  t.after(() => server.close());
  const { StreamPanel } = await server.ssrLoadModule("/src/components/StreamPanel.jsx");

  const markup = renderToStaticMarkup(
    createElement(StreamPanel, {
      turn: null,
      waiting: true,
      collapsed: false,
    }),
  );

  assert.match(markup, /Agent runtime/);
  assert.match(markup, /dispatching/);
  assert.match(markup, /Waiting for agent activity/);
});

test("the agent switcher always keeps the main agent first", async (t) => {
  const server = await createServer({
    server: { middlewareMode: true },
    appType: "custom",
  });
  t.after(() => server.close());
  const { streamAgents } = await server.ssrLoadModule("/src/components/StreamPanel.jsx");
  const agents = streamAgents({
    status: "tooling",
    segments: [
      {
        id: "agent:child:1",
        type: "agent",
        agentId: "child",
        parentAgentId: "main",
        agentKind: "subagent",
        agentLabel: "Research one topic",
        sequence: 1,
        status: "streaming",
      },
      {
        id: "agent:main:2",
        type: "agent",
        agentId: "main",
        agentKind: "main",
        agentLabel: "Main agent",
        sequence: 2,
        status: "streaming",
      },
    ],
  });

  assert.deepEqual(agents.map((agent) => agent.id), ["main", "child"]);
});

test("subagent navigation follows only the selected lineage", async (t) => {
  const server = await createServer({
    server: { middlewareMode: true },
    appType: "custom",
  });
  t.after(() => server.close());
  const { buildAgentNavigationRows } = await server.ssrLoadModule(
    "/src/components/AgentRuntime.jsx",
  );
  const agents = [
    { id: "main", kind: "main", label: "Main agent", status: "running" },
    { id: "a", parentId: "main", kind: "subagent", label: "Child A", status: "running" },
    { id: "b", parentId: "main", kind: "subagent", label: "Child B", status: "complete" },
    { id: "a1", parentId: "a", kind: "subagent", label: "Child A1", status: "complete" },
    { id: "a2", parentId: "a", kind: "subagent", label: "Child A2", status: "running" },
    { id: "b1", parentId: "b", kind: "subagent", label: "Child B1", status: "queued" },
  ];

  function summarize(activeAgentId) {
    return buildAgentNavigationRows(agents, activeAgentId).map((row) => ({
      parent: row.parent.id,
      children: row.children.map((agent) => agent.id),
      selected: row.selectedChildId,
    }));
  }

  assert.deepEqual(summarize("main"), [
    { parent: "main", children: ["a", "b"], selected: null },
  ]);
  assert.deepEqual(summarize("a"), [
    { parent: "main", children: ["a", "b"], selected: "a" },
    { parent: "a", children: ["a1", "a2"], selected: null },
  ]);
  assert.deepEqual(summarize("a2"), [
    { parent: "main", children: ["a", "b"], selected: "a" },
    { parent: "a", children: ["a1", "a2"], selected: "a2" },
  ]);
  assert.deepEqual(summarize("b"), [
    { parent: "main", children: ["a", "b"], selected: "b" },
    { parent: "b", children: ["b1"], selected: null },
  ]);
});

test("nested subagents appear in their parent row while queued", async (t) => {
  const server = await createServer({
    server: { middlewareMode: true },
    appType: "custom",
  });
  t.after(() => server.close());
  const { streamAgents } = await server.ssrLoadModule("/src/components/StreamPanel.jsx");
  const agents = streamAgents({
    status: "tooling",
    segments: [
      {
        id: "agent:main:1",
        type: "agent",
        agentId: "main",
        agentKind: "main",
        agentLabel: "Main agent",
        sequence: 1,
        status: "complete",
      },
      {
        id: "agent:child:1",
        type: "agent",
        agentId: "child",
        parentAgentId: "main",
        parentToolCallId: "main-tool",
        agentKind: "subagent",
        agentLabel: "Parent research",
        sequence: 1,
        status: "streaming",
      },
      {
        id: "tools:child:1",
        type: "tools",
        agentId: "child",
        parentAgentId: "main",
        agentKind: "subagent",
        agentLabel: "Parent research",
        afterSequence: 1,
        status: "streaming",
        events: [
          {
            kind: "agent_tool_input_complete",
            payload: {
              tool_use_id: "nested-tool",
              tool_name: "create_subagent",
              input: { task: "Nested research" },
            },
          },
          {
            kind: "agent_tool_input_complete",
            payload: {
              tool_use_id: "nested-tool-2",
              tool_name: "create_subagent",
              input: { task: "Nested research" },
            },
          },
        ],
      },
    ],
  });

  const queued = agents.find((agent) => agent.id === "queued:nested-tool");
  assert.ok(queued);
  assert.equal(queued.parentId, "child");
  assert.equal(queued.parentToolCallId, "nested-tool");
  assert.equal(queued.status, "queued");
  assert.equal(
    agents.find((agent) => agent.id === "queued:nested-tool-2")?.parentId,
    "child",
  );
});

test("the runtime projects requested, running, failed, recovered, and completed tools", async (t) => {
  const server = await createServer({
    server: { middlewareMode: true },
    appType: "custom",
  });
  t.after(() => server.close());
  const {
    buildRuntimeFrames,
    projectRuntimeFrame,
    projectSubagentTools,
    toolStatusLabel,
  } = await server.ssrLoadModule("/src/components/AgentRuntime.jsx");

  const agent = {
    id: "main",
    kind: "main",
    label: "Main agent",
    status: "running",
    segments: [
      {
        id: "agent:main:1",
        type: "agent",
        sequence: 1,
        status: "complete",
        terminal: false,
        stopReason: "tool_use",
      },
      {
        id: "tools:main:1",
        type: "tools",
        afterSequence: 1,
        status: "complete",
        events: [
          {
            kind: "agent_tool_input_start",
            payload: {
              tool_use_id: "tool-1",
              tool_name: "file_search",
              sequence: 1,
            },
          },
          {
            kind: "agent_tool_input_delta",
            payload: {
              tool_use_id: "tool-1",
              tool_name: "file_search",
              sequence: 1,
              partial_json: '{"query":"Temporal',
            },
          },
          {
            kind: "agent_tool_input_complete",
            payload: {
              tool_use_id: "tool-1",
              tool_name: "file_search",
              sequence: 1,
              input: { query: "Temporal agents" },
            },
          },
          {
            kind: "harness_tool_start",
            payload: {
              operation_id: "tool-1",
              tool_name: "file_search",
              llm_sequence: 1,
              status: "running",
            },
          },
          {
            kind: "harness_tool_guard_start",
            payload: {
              operation_id: "tool-1:guard:pre:0:policy",
              parent_operation_id: "tool-1",
              tool_name: "file_search",
              guard_name: "policy",
              timing: "pre",
              status: "running",
            },
          },
          {
            kind: "harness_tool_guard_complete",
            payload: {
              operation_id: "tool-1:guard:pre:0:policy",
              parent_operation_id: "tool-1",
              tool_name: "file_search",
              guard_name: "policy",
              timing: "pre",
              status: "passed",
            },
          },
          {
            kind: "harness_tool_activity_failed",
            payload: {
              operation_id: "tool-1:activity:run",
              parent_operation_id: "tool-1",
              tool_name: "file_search",
              activity_attempt: 1,
              error: { message: "temporary failure" },
            },
          },
          {
            kind: "harness_tool_activity_start",
            payload: {
              operation_id: "tool-1:activity:run",
              parent_operation_id: "tool-1",
              tool_name: "file_search",
              activity_attempt: 2,
              status: "running",
            },
          },
          {
            kind: "harness_tool_complete",
            payload: {
              operation_id: "tool-1",
              tool_name: "file_search",
              llm_sequence: 1,
              status: "complete",
            },
          },
        ],
      },
      {
        id: "agent:main:2",
        type: "agent",
        sequence: 2,
        status: "streaming",
        thinking: "I should combine the tool results before answering.",
        text: "Synthesizing the result",
      },
    ],
  };
  const timeline = { status: "streaming", activeSequence: 2 };
  const frames = buildRuntimeFrames(timeline, agent);

  function atEvent(kind) {
    const index = frames.findIndex(
      (frame) => frame.kind === "tool-event" && frame.event.kind === kind,
    );
    assert.notEqual(index, -1);
    return projectRuntimeFrame(timeline, agent, frames, index);
  }

  assert.equal(atEvent("agent_tool_input_start").modelStatus, "active");
  assert.equal(atEvent("agent_tool_input_delta").modelStatus, "active");
  assert.equal(atEvent("agent_tool_input_complete").modelStatus, "dormant");
  assert.equal(atEvent("agent_tool_input_start").tools[0].status, "building");
  assert.equal(atEvent("agent_tool_input_delta").tools[0].status, "building");
  assert.equal(atEvent("agent_tool_input_complete").tools[0].status, "requested");
  assert.equal(toolStatusLabel("building"), "Building input…");
  assert.equal(atEvent("harness_tool_start").tools[0].status, "running");
  const guardedTool = atEvent("harness_tool_guard_start").tools[0];
  assert.equal(guardedTool.id, "tool-1");
  assert.equal(guardedTool.status, "running");
  assert.deepEqual(
    guardedTool.guards.map((guard) => ({
      id: guard.id,
      name: guard.name,
      timing: guard.timing,
      status: guard.status,
    })),
    [{
      id: "tool-1:guard:pre:0:policy",
      name: "policy",
      timing: "pre",
      status: "running",
    }],
  );
  assert.equal(
    atEvent("harness_tool_guard_complete").tools[0].guards[0].status,
    "done",
  );
  assert.equal(atEvent("harness_tool_activity_failed").tools[0].status, "failed");
  assert.equal(atEvent("harness_tool_activity_start").tools[0].status, "running");
  assert.equal(atEvent("harness_tool_complete").tools[0].status, "done");

  const current = projectRuntimeFrame(timeline, agent, frames, frames.length - 1);
  assert.equal(current.turn, 2);
  assert.equal(current.modelStatus, "active");
  assert.match(current.currentSegment.thinking, /combine the tool results/);
  assert.deepEqual(
    current.tools.map((tool) => ({ id: tool.id, name: tool.name, status: tool.status })),
    [
      { id: "tool-1", name: "file_search", status: "done" },
    ],
  );

  const child = {
    id: "child-1",
    parentId: "main",
    parentToolCallId: "subagent-tool-1",
    kind: "subagent",
    label: "Research shark population changes",
    status: "running",
    turnCount: 2,
    toolCount: 1,
  };
  const withSubagent = projectSubagentTools(current, [agent, child], agent);
  assert.deepEqual(
    withSubagent.tools.map((tool) => ({
      id: tool.id,
      name: tool.name,
      status: tool.status,
      preview: tool.preview,
    })),
    [
      {
        id: "tool-1",
        name: "file_search",
        status: "done",
        preview: '{"query":"Temporal agents"}',
      },
      {
        id: "subagent-tool-1",
        name: "create_subagent",
        status: "running",
        preview: "Research shark population changes",
      },
    ],
  );
  const completedChild = { ...child, status: "complete" };
  assert.deepEqual(
    projectSubagentTools(current, [agent, completedChild], agent).tools.map(
      (tool) => tool.id,
    ),
    ["tool-1"],
  );
  assert.equal(projectSubagentTools(current, [agent, child], agent, false), current);
});

test("the final response retains create_artifact without reviving old subagents", async (t) => {
  const server = await createServer({
    server: { middlewareMode: true },
    appType: "custom",
  });
  t.after(() => server.close());
  const {
    buildRuntimeFrames,
    projectRuntimeFrame,
    projectSubagentTools,
  } = await server.ssrLoadModule("/src/components/AgentRuntime.jsx");
  const agent = {
    id: "main",
    kind: "main",
    status: "complete",
    segments: [
      {
        id: "agent:main:4",
        type: "agent",
        sequence: 4,
        status: "complete",
        terminal: false,
        stopReason: "tool_use",
      },
      {
        id: "tools:main:4",
        type: "tools",
        afterSequence: 4,
        status: "complete",
        events: [
          {
            kind: "agent_tool_input_complete",
            payload: {
              tool_use_id: "artifact-tool-1",
              tool_name: "create_artifact",
              sequence: 4,
              input: { filename: "report.html" },
            },
          },
          {
            kind: "harness_tool_complete",
            payload: {
              operation_id: "artifact-tool-1",
              tool_name: "create_artifact",
              llm_sequence: 4,
              status: "complete",
            },
          },
        ],
      },
      {
        id: "agent:main:5",
        type: "agent",
        sequence: 5,
        status: "complete",
        terminal: true,
        stopReason: "end_turn",
        text: "Your report is ready.",
      },
    ],
  };
  const timeline = { status: "complete", activeSequence: 5 };
  const frames = buildRuntimeFrames(timeline, agent);
  const finalModel = projectRuntimeFrame(timeline, agent, frames, frames.length - 1);
  const completedChild = {
    id: "child-1",
    parentId: "main",
    parentToolCallId: "old-subagent-tool",
    kind: "subagent",
    label: "Old completed research",
    status: "complete",
    turnCount: 3,
    toolCount: 2,
  };
  const projected = projectSubagentTools(
    finalModel,
    [agent, completedChild],
    agent,
  );

  assert.equal(projected.turn, 5);
  assert.equal(projected.modelStatus, "complete");
  assert.deepEqual(
    projected.tools.map((tool) => ({ id: tool.id, name: tool.name, status: tool.status })),
    [{ id: "artifact-tool-1", name: "create_artifact", status: "done" }],
  );
});

test("model streams transition from thinking to response and pin to newest content", async (t) => {
  const server = await createServer({
    server: { middlewareMode: true },
    appType: "custom",
  });
  t.after(() => server.close());
  const { modelStreamForProjection, scrollStreamToBottom } = await server.ssrLoadModule(
    "/src/components/AgentRuntime.jsx",
  );
  assert.deepEqual(
    modelStreamForProjection({ currentSegment: { thinking: "Considering tools", text: "" } }),
    { kind: "thinking", label: "Thinking stream", text: "Considering tools" },
  );
  assert.deepEqual(
    modelStreamForProjection({
      currentSegment: { thinking: "Considering tools", text: "Here is the answer" },
    }),
    { kind: "response", label: "Response stream", text: "Here is the answer" },
  );
  const viewport = {
    clientHeight: 132,
    scrollHeight: 412,
    scrollTop: 0,
  };

  scrollStreamToBottom(viewport);

  assert.equal(viewport.scrollTop, 280);
});

test("a large tool argument stream keeps the model active after its pre-guard", async (t) => {
  const server = await createServer({
    server: { middlewareMode: true },
    appType: "custom",
  });
  t.after(() => server.close());
  const { buildRuntimeFrames, projectRuntimeFrame } = await server.ssrLoadModule(
    "/src/components/AgentRuntime.jsx",
  );
  const agent = {
    id: "main",
    kind: "main",
    status: "running",
    segments: [
      {
        id: "guards:main:2",
        type: "tools",
        agentId: "main",
        agentKind: "main",
        afterSequence: 2,
        status: "complete",
        events: [{
          kind: "harness_llm_guard_complete",
          payload: {
            operation_id: "main:llm:2:guard:pre:0:policy",
            guard_name: "policy",
            timing: "pre",
            status: "passed",
          },
        }],
      },
      {
        id: "agent:main:2",
        type: "agent",
        agentId: "main",
        agentKind: "main",
        sequence: 2,
        status: "streaming",
        text: "I will build the report.",
      },
      {
        id: "tools:main:2",
        type: "tools",
        agentId: "main",
        agentKind: "main",
        afterSequence: 2,
        status: "streaming",
        events: [{
          kind: "agent_tool_input_delta",
          payload: {
            tool_use_id: "artifact-tool-1",
            tool_name: "create_artifact",
            sequence: 2,
            input_partial: '{"name":"report.html","content":"<html>',
          },
        }],
      },
    ],
  };
  const timeline = { status: "tooling", activeSequence: 2 };
  const frames = buildRuntimeFrames(timeline, agent);
  const projected = projectRuntimeFrame(timeline, agent, frames, frames.length - 1);

  assert.equal(frames.some((frame) => frame.kind === "tools-settled"), false);
  assert.equal(projected.modelStatus, "active");
  assert.deepEqual(
    projected.tools.map((tool) => ({ name: tool.name, status: tool.status })),
    [{ name: "create_artifact", status: "building" }],
  );
});

test("tool outcomes retain failure reasons and prefer durable results", async (t) => {
  const server = await createServer({
    server: { middlewareMode: true },
    appType: "custom",
  });
  t.after(() => server.close());
  const { inspectedRuntimeCard, toolOutcomeForEvents } = await server.ssrLoadModule(
    "/src/components/AgentRuntime.jsx",
  );
  const events = [
    {
      kind: "fetch_complete",
      payload: {
        status: 403,
        error: "HTTP 403: Forbidden",
        blocked_reason: "forbidden",
      },
    },
    {
      kind: "harness_tool_complete",
      payload: {
        status: "failed",
        result_preview: '{"error":"HTTP 403: Forbidden","status":403}',
        result_truncated: false,
      },
    },
  ];
  const outcome = toolOutcomeForEvents(events);
  assert.deepEqual(outcome.error, {
    type: "ToolError",
    message: "HTTP 403: Forbidden",
  });
  assert.deepEqual(outcome.result, {
    error: "HTTP 403: Forbidden",
    status: 403,
  });

  const model = {
    tools: [
      {
        id: "tool-1",
        name: "fetch_url",
        status: "failed",
        preview: '{"url":"https://example.test"}',
        detail: { latest_event: "harness_tool_complete" },
        error: outcome.error,
        resultPreview: outcome.result,
        resultTruncated: false,
        events,
      },
    ],
  };
  const inspected = inspectedRuntimeCard(
    { type: "tool", id: "tool-1" },
    model,
    {
      status: "loaded",
      data: {
        source: "temporal_history",
        status: "failed",
        error: "HTTP 403: Forbidden",
        result: {
          error: "HTTP 403: Forbidden",
          blocked_reason: "forbidden",
          status: 403,
        },
      },
    },
  );
  assert.equal(inspected.resultSource, "temporal_history");
  assert.equal(inspected.result.blocked_reason, "forbidden");
  assert.equal(inspected.error.message, "HTTP 403: Forbidden");
  assert.equal(inspected.rawEvents.length, 2);
});

test("LLM guards are bundled with the model and replay precedes its tools", async (t) => {
  const server = await createServer({
    server: { middlewareMode: true },
    appType: "custom",
  });
  t.after(() => server.close());
  const { buildRuntimeFrames, projectRuntimeFrame } = await server.ssrLoadModule(
    "/src/components/AgentRuntime.jsx",
  );
  const preStart = {
    kind: "harness_llm_guard_start",
    payload: {
      operation_id: "chat:llm:1:guard:pre:policy",
      parent_operation_id: "chat:llm:1",
      guard_name: "prompt_policy",
      timing: "pre",
      status: "running",
      llm_sequence: 1,
    },
  };
  const preComplete = {
    ...preStart,
    kind: "harness_llm_guard_complete",
    payload: { ...preStart.payload, status: "passed" },
  };
  const postStart = {
    kind: "harness_llm_guard_start",
    payload: {
      operation_id: "chat:llm:1:guard:post:policy",
      parent_operation_id: "chat:llm:1",
      guard_name: "response_policy",
      timing: "post",
      status: "running",
      llm_sequence: 1,
    },
  };
  const postComplete = {
    ...postStart,
    kind: "harness_llm_guard_complete",
    payload: { ...postStart.payload, status: "passed" },
  };
  const agent = {
    id: "main",
    kind: "main",
    status: "running",
    segments: [
      {
        id: "tools:main:1",
        type: "tools",
        afterSequence: 1,
        status: "complete",
        events: [
          preStart,
          preComplete,
          {
            kind: "agent_tool_input_complete",
            payload: { tool_use_id: "tool-1", tool_name: "search_web", input: { query: "Temporal" } },
          },
          postStart,
          postComplete,
          {
            kind: "harness_tool_start",
            payload: { operation_id: "tool-1", tool_name: "search_web", status: "running" },
          },
          {
            kind: "search_start",
            tool_name: "search_web",
            tool_call_id: "tool-1",
            payload: { tool_name: "search_web" },
          },
          {
            kind: "harness_tool_complete",
            payload: { operation_id: "tool-1", tool_name: "search_web", status: "complete" },
          },
        ],
      },
      {
        id: "agent:main:1",
        type: "agent",
        sequence: 1,
        status: "complete",
        terminal: false,
        stopReason: "tool_use",
        thinking: "I should search.",
        text: "I’ll check that.",
      },
    ],
  };
  const timeline = { status: "tooling", activeSequence: 1 };
  const frames = buildRuntimeFrames(timeline, agent);

  assert.deepEqual(
    frames.map((frame) => frame.kind),
    [
      "ingress",
      "model-guard",
      "model-guard",
      "model",
      "model",
      "model-guard",
      "model-guard",
      "tool-event",
      "tool-event",
      "tool-event",
    ],
  );
  assert.equal(
    frames.some((frame) => frame.event?.kind === "search_start"),
    false,
  );
  const preGuardModel = projectRuntimeFrame(timeline, agent, frames, 1);
  assert.equal(preGuardModel.modelStatus, "guarding");
  assert.deepEqual(
    preGuardModel.modelGuards.map((guard) => ({ name: guard.name, status: guard.status })),
    [{ name: "prompt_policy", status: "running" }],
  );
  const responseFrame = frames.findIndex(
    (frame) => frame.kind === "model" && frame.modelStatus === "active",
  );
  assert.equal(projectRuntimeFrame(timeline, agent, frames, responseFrame).currentSegment.text, "I’ll check that.");
  const postGuardFrame = frames.findIndex(
    (frame) => frame.kind === "model-guard" && frame.event === postComplete,
  );
  assert.deepEqual(
    projectRuntimeFrame(timeline, agent, frames, postGuardFrame).modelGuards.map(
      (guard) => ({ name: guard.name, status: guard.status }),
    ),
    [
      { name: "prompt_policy", status: "done" },
      { name: "response_policy", status: "done" },
    ],
  );
});

test("a historical terminal turn projects a completed model", async (t) => {
  const server = await createServer({
    server: { middlewareMode: true },
    appType: "custom",
  });
  t.after(() => server.close());
  const { buildRuntimeFrames, projectRuntimeFrame } = await server.ssrLoadModule(
    "/src/components/AgentRuntime.jsx",
  );
  const agent = {
    id: "main",
    kind: "main",
    status: "complete",
    segments: [
      {
        id: "agent:main:1",
        type: "agent",
        sequence: 1,
        status: "complete",
        terminal: true,
        stopReason: "end_turn",
        text: "Done",
      },
    ],
  };
  const timeline = { status: "complete" };
  const frames = buildRuntimeFrames(timeline, agent);
  const model = projectRuntimeFrame(timeline, agent, frames, frames.length - 1);
  assert.equal(model.agentStatus, "complete");
  assert.equal(model.modelStatus, "complete");
  assert.equal(model.turn, 1);
});
