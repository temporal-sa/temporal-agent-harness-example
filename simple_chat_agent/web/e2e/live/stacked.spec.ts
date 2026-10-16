// Check 3e against the real stack needs five approval-gated calls in flight at once. The dev
// worker registers TEST_TOOLS, but the only one (wait_seconds) is inherently safe, so it never
// asks; and POST /api/agent/sessions never sets CodingAgentInit.test_tools. A model can't be
// relied on to issue exactly 3 edits and 2 identical commands in one response, so the live half
// is skipped and 3e is covered by e2e/mocked/stacked.spec.ts (the card, the counts, and the
// request bodies: each tool id answered exactly once). Exactly-once application on the
// workflow side is covered by the P2 front-door tests (tests/test_session_signals.py).

import { test } from "../support/fixtures";

test("3e stacked approvals against the real stack", async () => {
  test.skip(true, "No approval-gated test tool on the dev worker; covered by the mocked stacked.spec.ts");
});
