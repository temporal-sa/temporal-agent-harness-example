// The "Worker isn't answering" banner's copy (PRD U6). It says what Temporal has saved and
// what is waiting, from the stream (durable) and the bridge's x_worker frame (Describe, never a
// Query). Pure.

import type { CodingSession } from "../session";
import { agentsOf, strArg, type SessionTree } from "../workbench/tree";
import { count, hms } from "./format";

export interface BannerView {
  show: boolean;
  /** "The worker running this turn stopped responding at 14:02:11." */
  lead: string;
  saved: string | null;
  waiting: string;
  /** The workflow whose pending Activity to open: a subagent's when it is the one stuck. */
  workflowId: string | null;
  /** Its current run id (from the facade), for the deep link to its pending Activities. */
  runId: string | null;
}

const TESTS = /\b(pytest|py\.test|tox|nox|jest|vitest|mocha|rspec|phpunit|(npm|pnpm|yarn|bun) (run )?test|go test|cargo test|mvn test|gradle test|make test|unittest)\b/;

export function commandNoun(command: string): string {
  return TESTS.test(command) ? "the test run" : "the command";
}

type BannerInput = SessionTree &
  Pick<CodingSession, "sessionId" | "status" | "anyBusy" | "telemetry" | "runIdFor"> &
  Partial<Pick<CodingSession, "toolProgress">>;

export function bannerView(s: BannerInput): BannerView {
  const worker = s.telemetry.worker;
  const show = (s.anyBusy && !worker.answering) || s.status === "worker_unresponsive";
  let models = 0;
  let tools = 0;
  let running: { kind: "tool"; name: string; command: string; output: boolean } | { kind: "model" } | null = null;
  let openTurn = false;
  for (const { node } of agentsOf(s)) {
    for (const m of node.messages) {
      if (m.status !== "running") continue;
      openTurn = true;
      for (const p of m.parts) {
        if (p.type === "model_interaction") {
          if (p.status === "done") models++;
          else running ??= { kind: "model" };
        } else if (p.type === "tool") {
          if (p.state === "done") tools++;
          else if (p.state === "running" || p.state === "approved") {
            const cmd = strArg(p.input, "command");
            running = { kind: "tool", name: p.toolName, command: cmd, output: !!(s.toolProgress?.[p.toolId] || p.progress) };
          }
        }
      }
    }
  }

  const agent = worker.agentId ? s.agent(worker.agentId) : undefined;
  const workflowId = agent?.workflowId ?? s.sessionId;
  const runId = s.runIdFor(workflowId);
  const at = hms(worker.since);

  if (!openTurn) {
    return {
      show,
      lead: "No Worker has picked up your message yet.",
      saved: "your message, in the Workflow's history",
      waiting: "for a Worker to start the turn. It runs when one returns.",
      workflowId,
      runId,
    };
  }

  const savedParts: string[] = [];
  if (models) savedParts.push(count(models, "model call"));
  if (tools) savedParts.push(count(tools, "tool call"));
  let thing = "the next step";
  if (running?.kind === "tool") {
    const isCommand = running.name === "run_command";
    thing = isCommand ? commandNoun(running.command) : running.name;
    if (isCommand && running.output) savedParts.push(`${thing}'s output so far`);
  } else if (running?.kind === "model") {
    thing = "the model call";
  }
  const saved = savedParts.length
    ? savedParts.length === 1
      ? savedParts[0]!
      : `${savedParts.slice(0, -1).join(", ")} and ${savedParts[savedParts.length - 1]}`
    : null;
  return {
    show,
    lead: at ? `The Worker running this turn stopped responding at ${at}.` : "The Worker running this turn stopped responding.",
    saved,
    waiting: `for another Worker to pick up ${thing}. Messages you send now are stored in Temporal and run when a Worker returns.`,
    workflowId,
    runId,
  };
}

/**
 * Fill a Temporal UI url template (`…/namespaces/<ns>/workflows/{workflow_id}`) for one
 * workflow. With its run id, the link opens that run's pending Activities; without one, the
 * Workflow's page.
 */
export function temporalUiUrl(template: string | null | undefined, workflowId: string | null, runId: string | null = null): string | null {
  if (!template || !workflowId) return null;
  const id = encodeURIComponent(workflowId);
  const url = template.replaceAll("{workflow_id}", id).replaceAll("{session_id}", id);
  if (!runId) return url;
  const cut = url.search(/[?#]/);
  const [base, rest] = cut < 0 ? [url, ""] : [url.slice(0, cut), url.slice(cut)];
  return `${base.replace(/\/+$/, "")}/${encodeURIComponent(runId)}/pending-activities${rest}`;
}
