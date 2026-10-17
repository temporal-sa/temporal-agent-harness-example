PM:

# P4 report: demo extras

- **Date:** 2026-10-16
- **Branch:** `refactor/official-harness` (local). Commits 4c51183..157f50f.
- **Status: every feature passes its check.** Run on the lean test plan the owner asked for.
- **Handoff:** `docs/phases/P4-handoff.md`. **UI contract:** `docs/phases/P4-ui-contract.md`.

## Where it stands

| Feature | Result | Evidence |
|---|---|---|
| MCP (A8, local only) | **Pass** | Live: a local echo server was added in the dialog ("Connected · 2 tools"). The agent called "local-echo · echo"; it was approved and returned `echo: hello from review`. Unit tests cover the cross-user `auth_ref` refusal, the SSRF refusal and the catalog refusal. Reauth raises `mcp_reauth_required`, which shows a "Reconnect" chip. |
| Code Mode (A11) | **Pass** | Live: a 20-file read finished in 2.0 s with every host call nested, under one "Approve script". Script errors go back to the model, which then fixed its script. Limits: 16 host calls, 4 at a time, 64 file reads. |
| `ask_user` (A12) | **Pass** | Live: the card survived a page reload, and the answer reached the agent. With the Worker stopped, an answer that wasn't one of the options was refused (`invalid_answer`) and the right answer then applied. |
| `pick_files` (A12) | **Pass** | Live: a CSV was uploaded into `uploads/`, and the agent read it and computed the sum (42). |
| Provider web search | **Pass** | Live: with the toggle on, the reply carried 10 sources, shown as a list. With it off, no search ran. Off by default, refused in the catalog. |
| `basic_file_agent` (D5) | **Pass** | Rewritten on the official harness (about 150 lines, over the ~100 target) and run once end to end. |

## Review (one combined pass, per the lean plan)

- **No blockers.**
- **One major, fixed:** the `pick_files` upload route accepted files for tool ids with no pending pick, so each new id lifted the cap. Fixed in 157f50f:
  - per-tool and per-session limits are reserved atomically before writing (50 files and 50 MB per session);
  - a failed write gives its slot back;
  - closed sessions are refused.
- **Minor items, deferred to P5** (listed below).
- **Checked and sound:**
  - Code Mode's sandbox, its read-only mount and its scope to the session's own workspace.
  - Callback validation and name cleaning.
  - MCP: the SSRF guard with IP pinning and no redirects, token ownership, OAuth state and PKCE, and server descriptions kept away from the judge.
  - Web search: off by default.
  - The UI: no XSS paths.

## Totals

| Suite | Result |
|---|---|
| Python unit | 832 pass (one run), plus the review-fix tests |
| Web unit | 218 pass |
| Build | Pass |
| Playwright, mocked | 83 pass (1 skipped by design), including one test per new surface |
| Live | One check per feature by the engineers, plus one reviewer walkthrough of all five beats (156k tokens, about 70% read from the provider cache) |
| Integration suites | Not rerun (lean plan); P4 left the P0–P3 paths alone apart from additive fields |

## Cost per demo run

- **P3 core script:** about 125k input and 5k output tokens.
- **P4 extras walkthrough:** about 155k input and 1.5k output, of which about 70% was a cache read.

These go into the README in P5.

## Deferred to P5

| Item |
|---|
| Accept `pick_files` uploads only while that pick is pending (an API-side mirror of open callbacks). The per-session caps bound it for now. |
| The local MCP loopback exception opens all of 127.0.0.0/8 on every allowed port. Narrow it to the named ports (local only; the catalog is unaffected). |
| An abandoned MCP OAuth flow waits forever. Add a timeout and a per-user cap. |
| A failed bearer re-add overwrites a working token. Discover first, then store. |
| Check the scheme of the OAuth URL in the browser. |
| MCP approval cards show "Always approve" greyed out; hide it. Hide "Reconnect" for servers with no sign-in. |
| The web-search toggle reads "off" after a reload. Carry `web_search` in the session summary. |
| The MCP tool list is refetched after continue-as-new rather than carried over. |
| `basic_file_agent` is about 150 lines; two docs still describe the old example. |
| Write the demo cost into the README. |

## Next

P5, hardening and release prep:
- the security checklist;
- swap `/app/` to `/` and the worker entrypoint;
- delete `agent_harness/` and the old app and frontend;
- Docker and catalog notes;
- a dry run of the cutover runbook;
- the deferred items above.

**Don't start without the owner's go-ahead.**
