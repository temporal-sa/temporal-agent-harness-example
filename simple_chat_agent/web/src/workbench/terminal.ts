// The Terminal tab's model: every run_command call as a run with its output, an exit badge,
// and the markers for a paused stream, a reattached run and a sandbox that isn't answering.
// Pure, apart from RunClock, which remembers when this browser saw each run start.

import type { Recovery, SandboxHealth, ToolPart, ToolProgressChunk } from "../session";
import { strArg, type LocatedTool } from "./tree";

export const COMMAND_TOOLS: ReadonlySet<string> = new Set(["run_command", "workspace.run_command"]);

export const SANDBOX_DOWN = "The sandbox isn't answering. Temporal will retry the command.";

// -- final output ---------------------------------------------------------------------------

/** run_command's result text (CommandResult.model_text), parsed. */
export interface CommandOutput {
  exitCode: number | null;
  durationS: number | null;
  timedOut: boolean;
  reattached: boolean;
  attempt: number | null;
  error: string | null;
  stdout: string;
  stderr: string;
  truncated: boolean;
}

export function parseCommandOutput(output: string | null): CommandOutput | null {
  if (!output) return null;
  const rows = output.replace(/\r\n/g, "\n").split("\n");
  const status = rows[1] ?? "";
  const m = /^exit (\d+|\?)(?: · ([\d.]+)s)?(.*)$/.exec(status);
  if (!m) return null;
  const rest = m[3] ?? "";
  const att = /reattached \(attempt (\d+)\)/.exec(rest);
  const res: CommandOutput = {
    exitCode: m[1] === "?" ? null : Number(m[1]),
    durationS: m[2] ? Number(m[2]) : null,
    timedOut: rest.includes("timed out"),
    reattached: !!att,
    attempt: att ? Number(att[1]) : null,
    error: null,
    stdout: "",
    stderr: "",
    truncated: false,
  };
  // Sections after the status line: "error: …", "stdout:\n…", "stderr:\n…", then the
  // "[output truncated; …]" and "changed: …" trailers.
  let section: "none" | "stdout" | "stderr" = "none";
  const out: string[] = [];
  const err: string[] = [];
  const body = rows.slice(2);
  // Trailers are the last lines; strip them first so they don't land in stderr.
  while (body.length) {
    const last = body[body.length - 1] ?? "";
    if (last.startsWith("[output truncated;")) {
      res.truncated = true;
      body.pop();
    } else if (last.startsWith("changed: ")) {
      body.pop();
    } else break;
  }
  for (const row of body) {
    if (section === "none" && row.startsWith("error: ")) {
      res.error = row.slice(7);
      continue;
    }
    if (row === "stdout:" && section === "none") {
      section = "stdout";
      continue;
    }
    if (row === "stderr:" && section !== "stderr") {
      section = "stderr";
      continue;
    }
    if (section === "stdout") out.push(row);
    else if (section === "stderr") err.push(row);
  }
  res.stdout = out.join("\n");
  res.stderr = err.join("\n");
  return res;
}

// -- exit badge -----------------------------------------------------------------------------

export type BadgeTone = "ok" | "fail" | "run" | "wait" | "muted" | "error";

export interface ExitBadge {
  text: string;
  tone: BadgeTone;
}

/** "27 passed", "1 failed, 13 passed": the test runner's summary, if the output has one. */
export function testSummary(text: string): { passed: number | null; failed: number | null; errors: number | null } {
  const num = (re: RegExp) => {
    let last: number | null = null;
    for (const m of text.matchAll(re)) last = Number(m[1]);
    return last;
  };
  return {
    passed: num(/\b(\d+) passed\b/g),
    failed: num(/\b(\d+) failed\b/g),
    errors: num(/\b(\d+) errors?\b(?! omitted)/g),
  };
}

export function clock(ms: number): string {
  const s = Math.max(0, Math.floor(ms / 1000));
  return `${Math.floor(s / 60)}:${String(s % 60).padStart(2, "0")}`;
}

export function exitBadge(
  part: Pick<ToolPart, "state" | "output">,
  opts: { workerAnswering: boolean; elapsedMs?: number | null; sandboxDown?: boolean } = { workerAnswering: true },
): ExitBadge {
  switch (part.state) {
    case "requested":
      return { text: "queued", tone: "muted" };
    case "awaiting_approval":
    case "evaluating":
      return { text: "waiting for approval", tone: "wait" };
    case "denied":
      return { text: "declined", tone: "muted" };
    case "failed":
      return { text: "failed", tone: "error" };
    case "approved":
    case "running":
    case "awaiting_callback": {
      if (!opts.workerAnswering) return { text: "paused · waiting for a Worker", tone: "wait" };
      if (opts.sandboxDown) return { text: "waiting for the sandbox", tone: "wait" };
      const t = opts.elapsedMs != null ? ` ${clock(opts.elapsedMs)}` : "";
      return { text: `running${t}`, tone: "run" };
    }
    case "done": {
      const out = parseCommandOutput(part.output);
      if (!out) return { text: "finished", tone: "muted" };
      if (out.timedOut) return { text: "timed out", tone: "fail" };
      if (out.exitCode === null) return { text: "exit not reported", tone: "muted" };
      const sum = testSummary(`${out.stdout}\n${out.stderr}`);
      if (out.exitCode === 0) {
        return { text: sum.passed !== null ? `exit 0 · ${sum.passed} passed` : "exit 0", tone: "ok" };
      }
      const failing = (sum.failed ?? 0) > 0 || (sum.errors ?? 0) > 0;
      return { text: failing ? `exit ${out.exitCode} · tests failing` : `exit ${out.exitCode}`, tone: "fail" };
    }
  }
}

// -- run clock ------------------------------------------------------------------------------

/**
 * When this browser first saw each run, and when it saw it start running. Live output comes
 * from the facade (`toolProgressChunks`); this only times runs. Idempotent per observation, so
 * React's double render in development is safe.
 */
export class RunClock {
  #runs = new Map<string, { firstSeen: number; sawBeforeStart: boolean; startedAt: number | null }>();

  observe(toolId: string, state?: ToolPart["state"], now: number = Date.now()): void {
    const before = state === "requested" || state === "awaiting_approval" || state === "evaluating" || state === "approved";
    let r = this.#runs.get(toolId);
    if (!r) {
      r = { firstSeen: now, sawBeforeStart: before, startedAt: null };
      this.#runs.set(toolId, r);
    }
    if (before) r.sawBeforeStart = true;
    if (state === "running" && r.startedAt === null && r.sawBeforeStart) r.startedAt = now;
  }

  firstSeen(toolId: string): number | null {
    return this.#runs.get(toolId)?.firstSeen ?? null;
  }

  /** When this browser saw the run start, or null if it was already running when first seen
   *  (after a reload): then the elapsed time isn't known. */
  startedAt(toolId: string): number | null {
    return this.#runs.get(toolId)?.startedAt ?? null;
  }
}

// -- lines ----------------------------------------------------------------------------------

export type TermLineKind = "cmd" | "out" | "err" | "ok" | "muted" | "pause" | "reattach";

export interface TermLine {
  kind: TermLineKind;
  text: string;
}

const REATTACH_NOTE = /^\[worker restarted; reattaching to the command \(attempt (\d+)\)\]$/;
const MUTED_NOTE = /^\[(live output limit reached|earlier output dropped|\.\.\. \d+ chars omitted)/;

export function reattachLine(resumedOn: string | null): string {
  return resumedOn
    ? `── reattached on ${resumedOn} · same run, not restarted ──`
    : "── reattached after the Worker was lost · same run, not restarted ──";
}

export function pauseLine(since: string | null): string {
  return since ? `── stream paused at ${since} · waiting for a Worker ──` : "── stream paused · waiting for a Worker ──";
}

/** The run_command recovery telemetry reports for this run, if any. */
export function reattachFor(run: Pick<LocatedTool, "agentId" | "turnNumber">, recoveries: readonly Recovery[]): Recovery | null {
  let found: Recovery | null = null;
  for (const r of recoveries) {
    if (r.retracted) continue;
    const isCommand = r.kind === "command_reattached" || (r.toolName !== null && COMMAND_TOOLS.has(r.toolName));
    if (!isCommand) continue;
    if (r.agentId && run.agentId && r.agentId !== run.agentId) continue;
    if (r.turnNumber !== null && run.turnNumber !== null && r.turnNumber !== run.turnNumber) continue;
    found = r;
  }
  return found;
}

function textLines(text: string, kind: "out" | "err", passedLine: boolean): TermLine[] {
  if (!text) return [];
  const rows = text.replace(/\r\n/g, "\n").split("\n");
  if (rows[rows.length - 1] === "") rows.pop();
  return rows.map((row) => {
    if (MUTED_NOTE.test(row)) return { kind: "muted", text: row };
    if (passedLine && kind === "out" && /^=*\s*\d+ passed\b/.test(row) && !/failed|error/.test(row)) return { kind: "ok", text: row.replace(/^=+\s*|\s*=+$/g, "") };
    return { kind, text: row };
  });
}

export interface RunLineContext {
  /** The run's live output chunks (`session.toolProgressChunks[toolId]`). */
  live: readonly ToolProgressChunk[];
  workerAnswering: boolean;
  /** HH:MM:SS the worker stopped answering. */
  pausedAt: string | null;
  /** The sandbox isn't answering for this run: the attempt and next try, for the detail line. */
  sandbox: Pick<SandboxHealth, "attempt" | "nextRetryAt"> | null;
  now?: number;
  /** Telemetry's reattach for this run (resumed_on names the new worker). */
  reattach: Recovery | null;
}

/** The lines the Terminal shows for one run. */
export function runLines(part: Pick<ToolPart, "state" | "output" | "input">, ctx: RunLineContext): TermLine[] {
  const lines: TermLine[] = [{ kind: "cmd", text: `$ ${strArg(part.input, "command")}` }];
  const running = part.state === "running" || part.state === "approved" || part.state === "awaiting_callback";
  if (part.state === "done") {
    const out = parseCommandOutput(part.output);
    if (out?.reattached || ctx.reattach) lines.push({ kind: "reattach", text: reattachLine(ctx.reattach?.resumedOn ?? null) });
    if (out) {
      if (out.error && !out.timedOut) lines.push({ kind: "muted", text: out.error });
      lines.push(...textLines(out.stdout, "out", out.exitCode === 0));
      lines.push(...textLines(out.stderr, "err", false));
      if (out.truncated) lines.push({ kind: "muted", text: "Output shortened. The full log stays in the sandbox." });
    } else if (part.output) {
      lines.push(...textLines(part.output, "out", false));
    }
    return lines;
  }
  // Live: each line takes its stream from the chunks it came from (stderr wins); our own
  // "note" lines read muted, and the reattach note becomes the reattach marker.
  let sawReattach = false;
  for (const row of liveRows(ctx.live)) {
    const text = row.text;
    if (REATTACH_NOTE.test(text.trim())) {
      sawReattach = true;
      lines.push({ kind: "reattach", text: reattachLine(ctx.reattach?.resumedOn ?? null) });
    } else if (text === "" && lines[lines.length - 1]?.kind === "reattach") {
      continue;
    } else if (row.kind === "note" || MUTED_NOTE.test(text)) {
      lines.push({ kind: "muted", text });
    } else {
      lines.push({ kind: row.kind, text });
    }
  }
  if (!sawReattach && ctx.reattach && running) lines.push({ kind: "reattach", text: reattachLine(ctx.reattach.resumedOn) });
  if (running && !ctx.workerAnswering) lines.push({ kind: "pause", text: pauseLine(ctx.pausedAt) });
  if (running && ctx.sandbox) {
    lines.push({ kind: "pause", text: SANDBOX_DOWN });
    const detail = sandboxDetail(ctx.sandbox, ctx.now ?? Date.now());
    if (detail) lines.push({ kind: "muted", text: detail });
  }
  if (part.state === "failed") lines.push({ kind: "muted", text: "The command didn't finish." });
  return lines;
}

export type LiveRowKind = "out" | "err" | "note";

/**
 * Split live chunks into lines. A line is stderr if any part of it came from stderr; else a
 * note if it came from a "note" chunk (our own lines: reattach, dropped output, the live
 * limit); else stdout.
 */
export function liveRows(chunks: readonly ToolProgressChunk[]): { text: string; kind: LiveRowKind }[] {
  const rows: { text: string; kind: LiveRowKind }[] = [];
  let cur: { text: string; kind: LiveRowKind } = { text: "", kind: "out" };
  let started = false;
  const mark = (stream: ToolProgressChunk["stream"]) => {
    if (stream === "stderr") cur.kind = "err";
    else if (stream === "note" && cur.kind === "out" && !started) cur.kind = "note";
  };
  for (const c of chunks) {
    const parts = c.text.replace(/\r\n/g, "\n").split("\n");
    parts.forEach((part, i) => {
      if (i > 0) {
        rows.push(cur);
        cur = { text: "", kind: "out" };
        started = false;
      }
      if (part) {
        mark(c.stream);
        if (c.stream !== "note") started = true;
        cur.text += part;
      }
    });
  }
  if (cur.text !== "") rows.push(cur);
  return rows;
}

/** The sandbox (workspace executor) isn't answering for this run (`session.sandbox`). When the
 *  notice names a tool id, only that run shows it. */
export function sandboxDown(sandbox: Pick<SandboxHealth, "answering" | "toolId">, toolId?: string): boolean {
  if (sandbox.answering) return false;
  return !sandbox.toolId || toolId === undefined || sandbox.toolId === toolId;
}

/** "attempt 2 · next try in 5s". */
export function sandboxDetail(s: Pick<SandboxHealth, "attempt" | "nextRetryAt">, now: number): string | null {
  const parts: string[] = [];
  if (s.attempt !== null) parts.push(`attempt ${s.attempt}`);
  if (s.nextRetryAt !== null) {
    const secs = Math.ceil((s.nextRetryAt - now) / 1000);
    parts.push(secs > 0 ? `next try in ${secs}s` : "trying again now");
  }
  return parts.length ? parts.join(" · ") : null;
}
