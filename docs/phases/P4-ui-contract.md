PM:

# P4 UI contract (from Eng C and Eng M)

All routes are under `/api/agent`. They are owner-scoped, require the CSRF header, and return errors as `{"error": code, "message": text}`.

## Code Mode (`run_script`)

- **The tool:** `run_script(script)`. It always asks for approval. In Auto mode the judge may approve it.
- **Approve script:** the normal approve call for the script's own tool id: `POST /approve {"session_id", "tool_id": "<run_script id>", "approved": true}`. That one answer covers every host call the script makes. Don't offer "Always approve" for this tool.
- **Host calls:**
  - They arrive as `tool_start` then `tool_end` or `tool_error`, with `tool_id` = `<script tool_id>/<n>` (n starts at 1).
  - They have no request or approval events.
  - Nest them under the script row. A script can make many calls, so collapse past about 5 (e.g. "62 host calls").
- **Errors:** a script error comes back as tool output starting `"Script error (...)"`. Show it as a failed run, not as an app error.

## `ask_user` (callback)

- **Request:** `callback_requested` with `tool_input: {"question", "options": [{"id", "label", "description"}]}`. The harness facade exposes it under `pendingCallbacks`.
- **Answer:** `POST /callback-result {"session_id", "tool_id", "result": {"option_id"}, "request_id"}`.
- **Decline:** send `"error": "..."` instead of `result`.
- **Bad answers:** `x_rejected` with `kind: "callback"` and code `invalid_answer`, `invalid_result`, `unknown_callback` or `already_resolved`. The card stays open.
- **Timeout:** after 30 minutes the model gets a tool error and the card closes.

## `pick_files` (callback)

- **Request:** `callback_requested` with `tool_input: {"prompt", "max_files"}`.
- **Upload each file:** `POST /sessions/{sid}/callbacks/{tool_id}/files`, multipart field `file`. The response is `{"path": "uploads/x.csv", "name", "size"}`.
- **Answer:** `{"result": {"paths": [...]}}`. Paths must be ones uploaded for that tool id.
- **Limits:** at most 10 files, 5 MB each. Names are cleaned to `[A-Za-z0-9._-]`, at most 100 characters.

## Provider web search

- **Config:** `GET /config` returns `"web_search": {"available", "default": false}`.
- **Toggle:**
  - Send message type `set_web_search` with payload `{"enabled": bool}`. It applies from the next turn.
  - The reply is `SettingsAck`-like `{ok, setting: "web_search", web_search, detail}`. Show `detail` as a transcript line.
  - Refused with 422 `web_search_not_allowed` where unavailable.
- **Sources:** in the reply payload as `"sources": [{"title", "url"}]` (http(s) only, at most 20). Render them as a sources list under the reply. Links open in a new tab with `rel="noopener noreferrer"`.

## MCP (local only)

- **Config:** `GET /config` returns `"mcp": {"enabled", "reason", "max_servers", "auth_modes": ["none", "bearer", "oauth"]}`. When disabled, hide the settings entry or show it disabled with the reason.
- **List:** `GET /mcp/servers` returns `{"enabled", "reason", "servers": [{"name", "url", "auth_mode", "has_credentials", "added_at", "tool_prefix"}]}`.
- **Add or replace:**
  - `POST /mcp/servers {"name", "url", "auth_mode": "none" | "bearer", "bearer_token"?}` returns `{"server": {..., "status", "tools": [{"name", "mcp_name"}]}}`.
  - Errors: 403 `mcp_disabled`; 422 `invalid_name` (1–24 characters of a-z, 0-9 and `-`, starting with a letter), `blocked_destination`, `token_required` or `too_many_servers`; 401 `mcp_auth_required`; 502 `mcp_unreachable`.
  - Sending the same name again replaces the server, which is how a bearer reconnect works.
- **Remove:** `DELETE /mcp/servers/{name}` returns `{"removed": true, "name"}`.
- **Status:** `GET /mcp/servers/{name}/status` returns the server plus `"status": "connected" | "needs_reauth" | "unreachable" | "blocked"`, `code`, `message` and `tools`.
- **OAuth:**
  - `POST /mcp/oauth/start {"name", "url"}` returns `{"authorization_url", "flow_id"}`. Open the URL; this serves both "add" and "Reconnect".
  - The callback redirects to `/app/?mcp=connected&mcp_server=<name>`, or `/app/?mcp_error=<text>`. Show a short notice and clear the query.
- **In the stream:** tools are named `mcp__<server>__<tool>`. Show the server and tool name nicely, e.g. "local-echo · echo".
  - `tool_approval_requested` and `tool_start` wrap the args as `{"arguments": {...}}`, so unwrap them.
  - The risk class falls back to config `default_tool_risk_class` (`external_side_effect`), so there's no "Always approve" for MCP tools.
- **Reauth:** a `tool_error` whose message starts `mcp_reauth_required:` shows a "Reconnect" chip that opens the dialog's Reconnect for that server.
- **Sessions:** the server turns MCP on for new sessions when config enables it. The browser doesn't send `mcp` on create, and the create body refuses unknown fields.
