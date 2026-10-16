// P0 step 0.8 / P2 step 2.1a: the official @temporalio/agent-harness-client (0.1.0,
// unpatched) against our agent API routes (simple_chat_agent/api/agent_routes.py).
//
//     node contract.mjs http://127.0.0.1:<port>                  # dev app (X-Test-User)
//     CONTRACT_LOGIN=user:password node contract.mjs <base>      # real app, local login
//
// Creates and lists the session through HttpTransport itself (startSession / listSessions,
// the harness CreateSessionRequest shape), then drives it entirely through the client:
// AgentSessionCore + HttpTransport + PlainSessionState. Prints a JSON summary; exits 0 on
// success, 1 on failure.

import {
  AgentSessionCore,
  HttpError,
  HttpTransport,
  PlainSessionState,
  startSession,
  waitingCalls
} from "@temporalio/agent-harness-client";

const base = (process.argv[2] ?? "").replace(/\/+$/, "");
if (!base) {
  console.error("usage: node contract.mjs http://127.0.0.1:<port>");
  process.exit(2);
}
const LOGIN = process.env.CONTRACT_LOGIN ?? "";
// Set in main(): the dev app's test header, or the real app's session cookie.
let USER = { "X-Test-User": "alice" };
const TIMEOUT_MS = Number(process.env.CONTRACT_TIMEOUT_MS ?? 240_000);

const summary = { ok: false, checks: {}, session_id: null };
const clientErrors = [];
const connections = [];
// Stream-end semantics (transport.ts: an attach "ends when the agent is idle and caught up";
// session.ts: one that ends with a turn open was cut off). Every message this client sent:
const sentIds = new Set();
// Connection went "idle" while a sent message had not finished: the server ended an attach early.
const prematureIdle = [];

function check(name, ok, detail) {
  summary.checks[name] = detail === undefined ? ok : { ok, detail };
  if (!ok) summary.failed = [...(summary.failed ?? []), name];
}

function waitFor(state, predicate, what, timeoutMs = TIMEOUT_MS) {
  return new Promise((resolve, reject) => {
    if (predicate()) return resolve();
    const timer = setTimeout(() => {
      unsubscribe();
      reject(new Error(`timed out waiting for ${what}`));
    }, timeoutMs);
    const unsubscribe = state.subscribe(() => {
      if (!predicate()) return;
      clearTimeout(timer);
      unsubscribe();
      resolve();
    });
  });
}

const toolParts = (state) =>
  state.messages.flatMap((m) => m.parts.filter((p) => p.type === "tool"));

async function login() {
  const [username, ...rest] = LOGIN.split(":");
  const response = await fetch(`${base}/api/auth/local/login`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ username, password: rest.join(":") })
  });
  if (!response.ok) throw new Error(`local login: ${response.status} ${await response.text()}`);
  const cookie = (response.headers.getSetCookie?.() ?? [response.headers.get("set-cookie") ?? ""])
    .map((c) => c.split(";")[0])
    .filter(Boolean)
    .join("; ");
  if (!cookie) throw new Error("local login set no cookie");
  return { Cookie: cookie };
}

async function expectHttpError(what, promise, status, code) {
  try {
    await promise;
    check(what, false, "no error");
  } catch (error) {
    const ok = error instanceof HttpError && error.status === status && error.code === code;
    check(what, ok, ok ? undefined : `${error.status} ${error.code} ${error.message}`);
  }
}

async function main() {
  summary.mode = LOGIN ? "real-app-local-login" : "dev-app";
  if (LOGIN) USER = await login();

  // 1. Create and list the session through HttpTransport (the harness CreateSessionRequest).
  const transport = new HttpTransport({ baseUrl: `${base}/api/agent/`, headers: USER });
  const created = await startSession(transport, { workflowType: "CodingAgent" }, {
    data: { model: { provider: "anthropic", model: "claude-haiku-5-5", effort: "low" }, title: "contract" }
  });
  const sessionId = created.workflow_id;
  summary.session_id = sessionId;
  check(
    "create_summary_shape",
    typeof sessionId === "string" &&
      created.agent_workflow_type === "CodingAgent" &&
      typeof created.created_at === "number" &&
      created.label === "contract" &&
      created.closed === false,
    JSON.stringify(created)
  );
  const listed = await transport.listSessions();
  check("list_has_session", listed.some((s) => s.workflow_id === sessionId && s.label === "contract"), listed.length);

  // Harness error bodies reach the client as HttpError codes.
  const anonymous = new HttpTransport({ baseUrl: `${base}/api/agent/` });
  await expectHttpError("unauthenticated_401", anonymous.listSessions(), 401, "unauthorized");
  await expectHttpError("unknown_session_404", transport.agentStatus("coding-agent-0000000000000000"), 404, "not_found");
  await expectHttpError(
    "unknown_agent_422",
    transport.createSession({ agent_workflow_type: "NoSuchAgent" }),
    422,
    "unknown_agent"
  );

  // 2. The client's session over our routes.
  const state = new PlainSessionState();
  let lastConnection = null;
  state.subscribe(() => {
    if (state.connection !== lastConnection) {
      lastConnection = state.connection;
      connections.push(state.connection);
      if (state.connection === "idle") {
        const unfinished = [...sentIds].filter((id) => {
          const m = state.messages.find((x) => x.id === id);
          return !m || (m.status !== "done" && m.status !== "error");
        });
        if (unfinished.length > 0) prematureIdle.push(unfinished);
      }
    }
  });
  const session = new AgentSessionCore(
    {
      sessionId,
      transport,
      idlePollMs: 1_000,
      onError: (error) => clientErrors.push(error.message),
      schedule: (flush) => setTimeout(flush, 0)
    },
    state
  );
  session.start();

  try {
    // The UI attaches on open and sends later. Sending while that first attach is still in
    // flight hits a client-side race (session.ts #ensureLive: the wake is dropped while an
    // attach is open, so an empty attach that ends after the send leaves the client idle until
    // the next status poll). Not ours to fix; wait for the first attach to settle instead.
    await waitFor(state, () => state.connection === "idle", "the first attach to settle", 30_000);

    // 3. Send through the client.
    const reply = await session.sendMessage("chat", {
      text: "Create notes/c.txt containing 'contract', then read it back."
    });
    if (!reply) throw new Error(`sendMessage failed: ${state.error?.message}`);
    summary.message_id = reply.message_id;
    sentIds.add(reply.message_id);

    // 4. Wait for the gated write_file, approve it through the client.
    const approvalFor = () =>
      waitingCalls(state.agents, (n) => session.handlesCallback(n)).approvals.find(
        (c) => c.part.toolName === "write_file"
      );
    await waitFor(state, () => approvalFor() !== undefined, "a write_file approval");
    const approval = approvalFor();
    summary.approved_tool_id = approval.part.toolId;
    const approved = await session.respondToApproval(approval.part.toolId, { approved: true });
    check("approve_accepted", approved);

    // 5. Wait until the message finishes and the client has gone idle.
    const message = () => state.messages.find((m) => m.id === reply.message_id);
    await waitFor(
      state,
      () =>
        ["done", "error"].includes(message()?.status) &&
        state.agentStatus === "idle" &&
        state.connection === "idle",
      "the turn to finish and the connection to go idle"
    );

    // 6. Assertions.
    const m = message();
    const outputText = typeof m.output?.text === "string" ? m.output.text : JSON.stringify(m.output);
    summary.output_text = outputText?.slice(0, 200);
    check("message_done", m.status === "done", m.status);
    check("output_mentions_contract", /contract/i.test(outputText ?? ""));

    const chats = state.messages.filter((x) => x.handler === "chat");
    check("one_chat_message", chats.length === 1, chats.length);
    check("pending_empty", state.pending.length === 0, state.pending.map((p) => `${p.id}:${p.status}`));

    const tools = toolParts(state).map((p) => `${p.toolName}:${p.state}`);
    summary.tools = tools;
    check("write_file_done", toolParts(state).some((p) => p.toolName === "write_file" && p.state === "done"));
    check("read_file_done", toolParts(state).some((p) => p.toolName === "read_file" && p.state === "done"));
    check(
      "write_file_approved",
      toolParts(state).some((p) => p.toolName === "write_file" && p.approval?.approved === true)
    );

    check("connection_healthy", state.connection !== "error", state.connection);
    check("no_client_error", state.error === undefined && clientErrors.length === 0, [
      ...(state.error ? [state.error.message] : []),
      ...clientErrors
    ]);
    // Every frame's resume_offset is non-decreasing (attach/re-attach never went backwards).
    const offsets = state.frames.map((f) => f.data.resume_offset);
    check("offsets_monotonic", offsets.every((o, i) => i === 0 || o >= offsets[i - 1]));

    // 7. Two sends back to back: the second arrives while the first's turn is open, so it is
    //    queued. Both must finish on the same attach without the client reconnecting or going
    //    idle early (session.ts sendMessage: "once admitted, the message is visible to the
    //    server's idle check, so a stream opened now cannot end before it").
    const r2 = await session.sendMessage("chat", { text: "Reply with just the word 'second'." });
    if (r2) sentIds.add(r2.message_id);
    const r3 = await session.sendMessage("chat", { text: "Reply with just the word 'third'." });
    if (r3) sentIds.add(r3.message_id);
    check("queued_sends_accepted", Boolean(r2 && r3));
    const finished = (id) => ["done", "error"].includes(state.messages.find((x) => x.id === id)?.status);
    await waitFor(
      state,
      () => [...sentIds].every(finished) && state.agentStatus === "idle" && state.connection === "idle",
      "the queued messages to finish"
    );
    summary.dispositions = state.messages.map((x) => x.disposition);
    check("three_chat_messages", state.messages.filter((x) => x.handler === "chat").length === 3);
    check("all_done", [...sentIds].every((id) => state.messages.find((x) => x.id === id)?.status === "done"));
    check("pending_empty_after_queue", state.pending.length === 0);
    check("never_reconnected", !connections.includes("reconnecting") && !connections.includes("error"), connections);
    check("no_premature_idle", prematureIdle.length === 0, prematureIdle);
    check("no_client_error_after_queue", state.error === undefined && clientErrors.length === 0, clientErrors);
    summary.frames = state.frames.length;
  } finally {
    session.stop();
  }
  summary.connections = connections;
  summary.ok = !summary.failed;
}

try {
  await main();
} catch (error) {
  summary.error = error instanceof Error ? error.message : String(error);
  summary.connections = connections;
  summary.client_errors = clientErrors;
}
console.log(JSON.stringify(summary, null, 2));
process.exit(summary.ok ? 0 : 1);
