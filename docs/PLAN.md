# Plan on a page: Advanced AI Agent on the Temporal Agent Harness

Built from `docs/PRD.md` v1.0 (approved) (2026-10-16). If the PRD version changes, this page is out of date.

**What we're building.** We're rebuilding the demo as a **coding agent** on Temporal's official agent harness, and deleting our hand-rolled harness.
- **The agent:** it works in a sandboxed workspace. It reads code, edits it, runs tests and commits, and it can use Claude, GPT or Gemini, switchable between turns.
- **Approvals:** risky actions wait for you. An optional auto mode lets a judge approve routine steps, and anything unclear still comes to you.
- **The product view:** it follows the buildkit design language and looks like Cursor or claude.ai. You see thinking, tool steps, live diffs, a terminal, a plan, artifacts and token meters.
- **Crashes:** when a worker dies, the screen says exactly what was kept and what had to be repeated.
- **The harness console:** one click away, showing the raw agent loop.

**What we're not building (first release).** No GitHub tools or Good Place filter. No `git push`. No arbitrary MCP servers in the catalog. No light theme, and no migration of old chats.

**Order of work.**

| Phase | What | Done when |
|---|---|---|
| P0 | Short spike on the six riskiest unknowns | Go/no-go memo; each unknown has a fallback |
| P1 | The agent | A scripted fix-the-bug task passes in at least 4 of 5 runs on each provider; security tests pass |
| P2 | API and streaming | Works with three tabs and with workers down; one user can't see another's sessions |
| P3 | Product view | You approve the wireframes first; then the 7-minute demo script passes twice in a row |
| P4 | Extras | Code Mode, browser-answered questions and file picking, MCP, web-search sources |
| P5 | Hardening | Security checklist with tests, docs, image, cutover dry run |

Release is a separate step and needs `/go release`.

**How we'll know it works.**
- The 7-minute demo script runs end to end: find and fix a bug, survive a worker crash, get a human approval, switch to GPT for review, open the harness console.
- After a crash, the transcript shows each reply exactly once.
- The token numbers match Temporal's history.

**Answered:** the scope (core first; extras in P4; GitHub and Good Place cut), the tech advisor's six recommendations, the judge (ours active; Jev when a key exists), and keys (the catalog brokers them).

**What we need from you** (I'll ask one at a time; each has a default):
1. Smaller items with defaults: MCP off in the catalog, harness console on once it's proven safe, catalog audience is employees only, keep a minimal starter example.
2. At release only: ending the old chats (can't be undone).
