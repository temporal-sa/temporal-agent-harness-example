// The scripted streams render as the wireframes describe: a running turn (Frame 1), the
// worker-lost banner and the recovery divider (Frames 2 and 3, the UI half of 3f), and an
// x_rejected already_resolved answer.

import { expect, test } from "../support/fixtures";
import { approvalResolved, turn, xRejected } from "../support/frames";
import { openScripted, sessionId } from "../support/open";
import { DEMO_PROMPT, FIX_PATH, KEPT, commitWaits, recovery, workerBack, workerLost, working, workingEnds } from "../support/scenarios";

test("running turn: explorers, the fix as a diff, the judge's approval, pytest running", async ({ page, mockApi }) => {
  const api = await mockApi();
  await openScripted(page, api, working(), { session_id: sessionId("working") });

  await expect(page.getByText(DEMO_PROMPT)).toBeVisible();
  await expect(page.getByRole("status").filter({ hasText: "Running" }).first()).toBeVisible();
  // Two explorers, side by side, each with its findings.
  await expect(page.getByText("Where is the refund amount computed?")).toBeVisible();
  await expect(page.getByText("What does test_refund expect?")).toBeVisible();
  await expect(page.getByText("test_refund expects 9.45, not 9.")).toBeVisible();
  // The inline diff of the fix.
  await expect(page.getByLabel(`Changes to ${FIX_PATH} in this call`)).toContainText("return round(amount * 0.9, 2)");
  // The judge approved pytest, with its confidence and reason.
  await expect(page.getByText("confidence 0.93")).toBeVisible();
  await expect(page.getByText("Running the project's own test suite is read-only.")).toBeVisible();
  // pytest runs: Stop replaces Send.
  await expect(page.getByRole("button", { name: /^Stop/ })).toBeVisible();
  await expect(page.getByRole("button", { name: "Send" })).toHaveCount(0);
  // Workbench: Plan and Changes.
  await expect(page.getByRole("tab", { name: /Changes/ })).toContainText("1");
});

test("a call the judge approved is not labelled as approved by you", async ({ page, mockApi }) => {
  // GAP (Eng U, transcript/ToolRow.tsx StateBadge): a finished call whose approval came from
  // the Auto-mode judge shows "Approved by you". The harness publishes the judge's
  // tool_approval_resolved like a person's (agent_workflow.py `_resolve_and_publish`, reason
  // "judge …"), so the badge must look at the evaluations, not only `approval.approved`.
  const api = await mockApi();
  await openScripted(page, api, working(), { session_id: sessionId("judge-badge") });
  await expect(page.getByText("confidence 0.96")).toBeVisible();
  await expect(page.getByText(/approved by you/i)).toHaveCount(0, { timeout: 2000 });
});

test("worker lost: banner, then the divider goes from Checking… to confirmed numbers", async ({ page, mockApi }) => {
  const api = await mockApi();
  const id = sessionId("recovery");
  const root = turn(1, "msg-1");
  await openScripted(page, api, working(root), { session_id: id });
  await expect(page.getByText(DEMO_PROMPT)).toBeVisible();

  const lostAt = Math.floor(Date.now() / 1000);
  await api.push(id, [workerLost(root, lostAt)]);
  const banner = page.getByRole("status", { name: "Worker isn't answering" });
  await expect(banner).toBeVisible();
  await expect(banner).toContainText("Saved in Temporal:");
  await expect(banner).toContainText("for another Worker to pick up the test run");
  await expect(page.getByRole("status").filter({ hasText: "Worker isn't answering" }).first()).toBeVisible();

  // Another worker takes over: the live (unconfirmed) recovery marker.
  await api.push(id, [workerBack(root), recovery(root, lostAt, false)]);
  await expect(banner).toHaveCount(0);
  const divider = page.getByRole("region", { name: /^(Another Worker took over · checking|Recovered · Worker lost at)/ });
  await expect(divider).toBeVisible();
  await expect(divider).toContainText("Checking…");
  await expect(page.getByText("Work kept after worker loss (this session)")).toBeVisible();

  // Confirmed from history: the numbers replace "Checking…".
  await api.push(id, [recovery(root, lostAt, true)]);
  await expect(divider).not.toContainText("Checking…");
  await expect(divider).toContainText(`${KEPT.model_calls} model calls`);
  await expect(divider).toContainText(`${KEPT.tool_calls} tool calls plus the running command`);
  await expect(divider).toContainText("Repeated nothing");
  await expect(page.getByText("Recoveries").locator("..")).toContainText("1");

  // The turn finishes; the reply appears once.
  const reply = "The tests pass after the recovery.";
  await api.push(id, workingEnds(root, reply));
  await expect(page.getByText(reply)).toHaveCount(1);
});

test("x_rejected already_resolved: the answer settles quietly, no error", async ({ page, mockApi }) => {
  const root = turn(1, "msg-1");
  let id = "";
  const api = await mockApi({
    // The approval lands, but another tab answered first: the agent rejects ours as
    // already_resolved and the stream carries the other tab's resolution.
    onApprove: async (body) => {
      await api.push(id, [
        approvalResolved(root, String(body.tool_id), "git_commit", true),
        xRejected("req-dup", {
          kind: "approval",
          toolId: String(body.tool_id),
          code: "already_resolved",
          reason: "This tool call was already approved, so this decision was not applied.",
        }),
      ]);
    },
  });
  id = sessionId("rejected");
  await openScripted(page, api, commitWaits(root), { session_id: id });
  const card = page.getByRole("region", { name: "Needs approval" });
  await expect(card).toBeVisible();
  await card.getByRole("button", { name: "Approve", exact: true }).click();
  await expect(card).toHaveCount(0);
  await expect(page.getByRole("alert")).toHaveCount(0);
  await expect(page.getByText(/Not sent|didn't reach Temporal/)).toHaveCount(0);
  expect(api.bodies("POST", "/api/agent/approve")).toHaveLength(1);
});
