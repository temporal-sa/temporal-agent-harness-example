PM:

# P4 phase handoff: demo extras

- **Parent docs:**
  - `docs/PRD.md` v1.2, rows A8, A11, A12, D5 and D7, and the "provider web search as a sources list" candidate.
  - `docs/phases/P3-report.md`.
- **Branch:** `refactor/official-harness`. It stays local.
- **Owner go:** `/yes`, 2026-10-16, with the instruction **"reduce the testing. This is taking too long and burning too many tokens."**
- **Next phase:** P5. Don't start it without the owner's go-ahead.

## Lean test plan (owner instruction)

- **Unit tests** cover new logic only. During development, run only the affected test files. Run the full unit suite once, before your final commit.
- **Live checks:** one live run per feature, on Claude Haiku 5.5 at effort low (Sonnet only where Haiku can't do the feature). Rerun only if it fails.
- **No behaviour evals.** No multi-run model statistics.
- **UI:** typecheck and unit tests. Add at most one mocked e2e test per new UI surface. No live Playwright per feature.
- **End of phase:** one full regression run and one demo pass (P4 beats only, run once). Then one combined review (security plus UX). A re-review happens only if it finds a blocker.

## Decisions for P4 (PM, within the approved plan)

1. **MCP is local only** in release 1 (D7). It's off in the catalog: config hides the feature, and the API refuses registration.
2. **Provider web search** uses the provider's built-in search through Pydantic AI's built-in web-search tool.
   - It runs inside the provider, so our approval gate can't see its queries.
   - So it is **off by default**. A per-session "Web search" toggle in the composer turns it on, and the transcript notes when it's on.
   - It's available locally. In the catalog it's off until the owner says otherwise.
   - Results render as a **sources list** under the reply.
3. **Code Mode** follows A11: at most 16 host calls per script and concurrency 4. "Approve script" approves its host calls once. Every host call shows as a nested row.
4. **Callbacks** follow A12: `ask_user` gets choice cards, validated against the option ids offered; `pick_files` uses click-to-choose and uploads through our API with size, count and name limits. Both wait durably across a reload and a worker restart.
5. **`basic_file_agent`** is rewritten as a minimal harness example of about 100 lines (D5), with a README section.

## Scope and owners

| Step | Rows | Work | Owner |
|---|---|---|---|
| 4.1 | A8 | MCP. Registration per user, stored in the `CodingSessions` settings; reuse the old app's OAuth helpers (`api/mcp_helpers.py`), resolving tokens only for the session owner. Tool listing by an activity, cached in state. Per-tool harness tools `mcp__<server>__<tool>` calling one `mcp_call` activity. Per-tool approval and always-allow; the judge never sees server descriptions; the SSRF guard applies to server URLs. A "Reconnect" error when reauth is needed. API routes. | Eng M |
| 4.2 | D5 | `basic_file_agent` rewrite | Eng M |
| 4.3 | A11 | Code Mode on the agent (`code_mode_tool`, `/workspace` VFS through the executor) | Eng C |
| 4.4 | A12 | `ask_user` and `pick_files` callback tools on the agent, plus the API's `callback-result` and upload path for `pick_files` | Eng C |
| 4.5 | — | Provider web search with sources (agent side and config flag) | Eng C |
| 4.6 | A8, A11, A12, 4.5 | UI:<br>• `ask_user` choice cards and the `pick_files` picker (they live in the decision dock)<br>• MCP settings dialog and Reconnect chip<br>• Code Mode script row with nested host calls and "Approve script"<br>• the web-search toggle and sources list | Eng V |
| 4.7 | — | Final regression, one demo pass of the P4 beats, combined review, report | Lead |

## Validation (one check each)

| Check | Pass when |
|---|---|
| MCP | A registered local MCP server's tool is called with approval and shows in transcript and console. A cross-user `auth_ref` is refused (unit). Reauth shows "Reconnect". |
| Code Mode | A script reading many workspace files completes, and its host calls are visible. A type-check error goes back to the model. |
| `ask_user` | The card appears, survives a reload, and the answer reaches the agent. An answer not among the offered ids is refused (unit). |
| `pick_files` | Files chosen in the browser land in the workspace within the limits, and the agent reads them. |
| Web search | With the toggle on, the reply shows a sources list. With it off, no search happens. |
| `basic_file_agent` | It runs end to end against the local stack once. |

## Rules

- No patching. The worker is versioned (PINNED), so change workflow code directly.
- Private harness internals stay in `harness_ext.py` only.
- Browser paths issue no Queries.
- Never print key values.
- Stage only your own files.
- Commit messages start with your role, for example "Eng M: P4 …".

## File ownership

| Owner | Files |
|---|---|
| Eng M | `simple_chat_agent/agent/mcp*.py` (new), `simple_chat_agent/agent/sessions.py` (MCP settings), `simple_chat_agent/api/mcp_helpers.py`, the MCP routes in `agent_routes.py`, `basic_file_agent/**`, and their tests |
| Eng C | `simple_chat_agent/agent/{workflow.py,runtime.py,model_registry.py,harness_ext.py,approvals.py}`, `agent/tools/{code_mode,callbacks}*.py` (new), the callback and upload routes in `agent_routes.py`, and their tests |
| Eng V | `simple_chat_agent/web/src/**` and `web/e2e/mocked/**` |

**Shared files:**
- `agent_routes.py` is edited by both Eng M and Eng C. Each owns only their own routes.
- `workflow.py` is Eng C's. Eng M may make one small edit there: the call that adds MCP tools to the per-run toolset. Keep the logic in `agent/mcp.py`, and re-read the file before editing, because Eng C works in it in parallel.
- After agent contract changes, run `make gen` and commit the generated types.
