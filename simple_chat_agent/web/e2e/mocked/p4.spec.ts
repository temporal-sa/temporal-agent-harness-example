// P4 surfaces (PRD A8, A11, A12, provider web search), one mocked test each: the Code Mode
// script row and "Approve script", the ask_user card, the pick_files card, the web search
// toggle and sources list, and the MCP servers dialog with its Reconnect chip.

import { expect, test } from "../support/fixtures";
import { openScripted, sessionId } from "../support/open";
import {
  accepted,
  approvalRequested,
  approvalResolved,
  callbackResolved,
  callbackWaits,
  closeTurn,
  ev,
  modelCall,
  openTurn,
  toolEnd,
  toolError,
  toolRequested,
  toolStart,
  turn,
  xRejected,
} from "../support/frames";

const USAGE = { input: 1000, output: 100 };

test("Code Mode: Approve script, then host calls nest under the script and fold past 5", async ({ page, mockApi }) => {
  const api = await mockApi();
  const t = turn(1, "msg-1");
  const script = "import pathlib\nfiles = list(pathlib.Path('/workspace').iterdir())\nprint(len(files))";
  const id = "toolu_script";
  const s = await openScripted(
    page,
    api,
    [...openTurn(t, "Count the files"), ...modelCall(t, 0, USAGE), toolRequested(t, id, "run_script", { script }), approvalRequested(t, id, "run_script", { script })],
    { session_id: sessionId("p4-script"), posture: "ask_everything" },
  );
  const card = page.getByRole("region", { name: "Needs approval" });
  await expect(card.getByText("Run a script")).toBeVisible();
  await expect(card.getByText("files = list(pathlib.Path('/workspace').iterdir())")).toBeVisible();
  await expect(card.getByRole("button", { name: /Always approve/ })).toHaveCount(0);
  await card.getByRole("button", { name: "Approve script" }).click();
  await expect.poll(() => api.bodies("POST", "/api/agent/approve")).toEqual([expect.objectContaining({ tool_id: id, approved: true })]);

  const hosts = Array.from({ length: 7 }, (_, i) => [toolStart(t, `${id}/${i + 1}`, "read_file", { path: `f${i}.py` }), toolEnd(t, `${id}/${i + 1}`, "read_file", "x")]).flat();
  await api.push(s.session_id, [approvalResolved(t, id, "run_script", true), toolStart(t, id, "run_script", { script }), ...hosts, toolEnd(t, id, "run_script", "7")]);
  const fold = page.getByRole("button", { name: "7 host calls" });
  await expect(fold).toBeVisible();
  await fold.click();
  await expect(page.locator(".host-calls .tool.nest")).toHaveCount(7);
  await page.getByRole("button", { name: /Show the script \(3 lines\)/ }).click();
  await expect(page.locator(".script-src")).toContainText("iterdir()");
});

test("ask_user: options answer, a refusal shows inline, the card survives a reload and closes when resolved", async ({ page, mockApi }) => {
  const api = await mockApi();
  const t = turn(1, "msg-1");
  const input = { question: "Which approach?", options: [{ id: "fix", label: "Fix the rounding", description: "Smallest change" }, { id: "rewrite", label: "Rewrite refunds.py" }] };
  const s = await openScripted(page, api, [...openTurn(t, "Fix it"), ...modelCall(t, 0, USAGE), ...callbackWaits(t, "toolu_ask", "ask_user", input)], {
    session_id: sessionId("p4-ask"),
  });
  const card = page.getByRole("region", { name: "Question from the agent" });
  await expect(card.getByText("Which approach?")).toBeVisible();
  await expect(page.getByText("Waiting for you")).toBeVisible();
  await page.reload();
  await expect(card.getByRole("button", { name: /Fix the rounding/ })).toBeVisible();

  await card.getByRole("button", { name: /Fix the rounding/ }).click();
  await expect.poll(() => api.bodies("POST", "/api/agent/callback-result").length).toBe(1);
  const body = api.bodies("POST", "/api/agent/callback-result")[0]!;
  expect(body).toMatchObject({ session_id: s.session_id, tool_id: "toolu_ask", result: { option_id: "fix" } });
  await expect(card.getByText("Sent. Waiting for the agent to take it.")).toBeVisible();

  await api.push(s.session_id, [xRejected(String(body.request_id), { kind: "callback", toolId: "toolu_ask", code: "invalid_answer", reason: "That option wasn't offered." })]);
  await expect(card.getByRole("alert")).toContainText("That option wasn't offered.");
  await expect(card.getByRole("button", { name: /Rewrite refunds\.py/ })).toBeEnabled();

  await api.push(s.session_id, [callbackResolved(t, "toolu_ask", "ask_user", "ok"), toolEnd(t, "toolu_ask", "ask_user", '{"option_id": "fix"}')]);
  await expect(card).toHaveCount(0);
});

test("pick_files: chooses, uploads with limits, and sends the uploaded paths", async ({ page, mockApi }) => {
  const api = await mockApi();
  const t = turn(1, "msg-1");
  const s = await openScripted(page, api, [...openTurn(t, "Read my CSV"), ...modelCall(t, 0, USAGE), ...callbackWaits(t, "toolu_pick", "pick_files", { prompt: "Pick the sales CSV", max_files: 1 })], {
    session_id: sessionId("p4-pick"),
  });
  const card = page.getByRole("region", { name: "Files requested by the agent" });
  await expect(card.getByText("Pick the sales CSV")).toBeVisible();
  await card.getByLabel("Files for the agent").setInputFiles([
    { name: "sales.csv", mimeType: "text/csv", buffer: Buffer.from("a,b\n1,2\n") },
    { name: "extra.csv", mimeType: "text/csv", buffer: Buffer.from("x\n") },
  ]);
  await expect(card.getByText("Uploaded", { exact: true })).toBeVisible();
  await expect(card.getByText(/Not uploaded\. Over the limit/)).toBeVisible();
  expect(api.bodies("POST", "/api/agent/sessions/")).toHaveLength(1);
  await card.getByRole("button", { name: "Send 1 file" }).click();
  await expect.poll(() => api.bodies("POST", "/api/agent/callback-result")).toEqual([
    expect.objectContaining({ session_id: s.session_id, tool_id: "toolu_pick", result: { paths: ["uploads/sales.csv"] } }),
  ]);
});

test("web search: the toggle sends set_web_search, its ack shows, and the reply lists its sources", async ({ page, mockApi }) => {
  const sid = sessionId("p4-search");
  const t = turn(1, "msg-1");
  const ack = turn(1, "msg-ack");
  let api!: Awaited<ReturnType<typeof mockApi>>;
  api = await mockApi({
    config: { web_search: { available: true, default: false } },
    onMessage: async (b, messageId) => {
      const m = b.message as { type?: string; payload?: Record<string, unknown> };
      if (m?.type !== "set_web_search") return;
      const tt = { ...ack, messageId };
      await api.push(sid, [
        accepted(tt, "set_web_search", m.payload ?? {}, "queued"),
        ev(tt, "message_handler_start"),
        ev(tt, "message_handler_end", { output: { ok: true, setting: "web_search", web_search: true, detail: "Web search is on from the next turn." } }),
      ]);
    },
  });
  const reply = "Temporal is a durable execution platform.";
  await openScripted(
    page,
    api,
    [
      ...openTurn(t, "What is Temporal?"),
      ...modelCall(t, 0, USAGE, { text: reply }),
      ...closeTurn(t, { text: reply, sources: [{ title: "Temporal docs", url: "https://docs.temporal.io/" }, { title: "x", url: "javascript:alert(1)" }] }),
    ],
    { session_id: sid },
  );
  const sources = page.getByRole("list", { name: "Sources" });
  await expect(sources.getByRole("link", { name: "Temporal docs" })).toHaveAttribute("rel", "noopener noreferrer");
  await expect(sources.getByRole("listitem")).toHaveCount(1);
  await expect(sources).toContainText("docs.temporal.io");

  const toggle = page.getByRole("button", { name: "Web search" });
  await expect(toggle).toHaveAttribute("aria-pressed", "false");
  await toggle.click();
  await expect(toggle).toHaveAttribute("aria-pressed", "true");
  await expect(page.getByRole("status").filter({ hasText: "Web search on" })).toBeVisible();
  await expect(page.getByText("Web search is on from the next turn.")).toBeVisible();
});

test("MCP: the dialog lists, adds and closes on Esc; a reauth error offers Reconnect", async ({ page, mockApi }) => {
  const api = await mockApi({
    config: { mcp: { enabled: true, reason: null, max_servers: 5, auth_modes: ["none", "bearer", "oauth"] } },
    mcpServers: [{ name: "docs", url: "https://mcp.example.com/mcp", auth_mode: "bearer", has_credentials: true, added_at: 1, tool_prefix: "mcp__docs__" }],
  });
  const t = turn(1, "msg-1");
  const err = "mcp_reauth_required: MCP server 'docs' needs you to reconnect. Open MCP settings and choose Reconnect, then try again.";
  await openScripted(
    page,
    api,
    [
      ...openTurn(t, "Search the docs"),
      ...modelCall(t, 0, USAGE),
      toolRequested(t, "toolu_mcp", "mcp__docs__search", { arguments: { query: "retry policy" } }),
      toolStart(t, "toolu_mcp", "mcp__docs__search", { arguments: { query: "retry policy" } }),
      toolError(t, "toolu_mcp", "mcp__docs__search", err),
    ],
    { session_id: sessionId("p4-mcp") },
  );
  await expect(page.locator(".tool-row .name").filter({ hasText: "docs · search" })).toContainText('{"query":"retry policy"}');

  const open = page.getByRole("button", { name: "MCP servers" });
  await open.click();
  const dialog = page.getByRole("dialog", { name: "MCP servers" });
  await expect(dialog.getByText("https://mcp.example.com/mcp")).toBeVisible();
  await expect(dialog.getByText("Connected")).toBeVisible();
  await dialog.getByLabel("Name").fill("local-echo");
  await dialog.getByLabel("URL").fill("http://127.0.0.1:8765/mcp");
  await dialog.getByRole("button", { name: "Add server" }).click();
  await expect(dialog.getByText("http://127.0.0.1:8765/mcp")).toBeVisible();
  expect(api.bodies("POST", "/api/agent/mcp/servers")).toEqual([{ name: "local-echo", url: "http://127.0.0.1:8765/mcp", auth_mode: "none" }]);
  await page.keyboard.press("Escape");
  await expect(dialog).toHaveCount(0);
  await expect(open).toBeFocused();

  await page.getByRole("button", { name: "Reconnect docs" }).click();
  await expect(dialog).toBeVisible();
  await expect(dialog.getByRole("textbox", { name: "Bearer token" })).toBeFocused();
});
