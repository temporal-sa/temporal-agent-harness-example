// Above the composer (PRD U4): the stacked approval card (owner decisions A and B), then the
// checkpoint and spend-cap cards.

import { useEffect, useId, useMemo, useRef, useState, type ReactNode } from "react";

import type { CodingSession, DecisionDockProps, SendRecord } from "../session";
import { SendStateTrail } from "../recovery/SendStateTrail";
import { currentBranch, DETACHED } from "../workbench/changes";
import { DiffView } from "../workbench/DiffView";
import { agentLabel, anythingRunning, toolsOf } from "../workbench/tree";
import {
  ALWAYS_APPROVE_WHY,
  alwaysApproveLabel,
  alwaysApproveReason,
  argSummary,
  callTitle,
  editStat,
  reviewOf,
  toolBadge,
  type Risk,
} from "./describe";
import {
  answersFrom,
  approveAllTargets,
  buildStack,
  cardItems,
  EMPTY_CARD,
  groupTargets,
  isAnswered,
  itemOf,
  reconcile,
  type Answer,
  type ApprovalItem,
  type CardState,
  type Fold,
  type Group,
} from "./grouping";
import { answeredText, groupTitle, stackHeading } from "./labels";
import { StopCards } from "./StopCards";
import "./decisions.css";

export function DecisionDock({ session }: DecisionDockProps) {
  return (
    <div className="dc-dock">
      <ApprovalCard session={session} />
      <StopCards session={session} />
    </div>
  );
}

export const PAUSED_LINE = "The agent is paused in Temporal and nothing is running.";
export const SURVIVES_LINE = "This request survives reloads and worker restarts.";
export const STILL_RUNNING_LINE = "Other work in this turn keeps running while these wait.";
export const DECLINE_HINT = "Optional. The agent sees your reason with the declined call.";

// -- the card -------------------------------------------------------------------------------

interface DeclineTarget {
  key: string;
  ids: string[];
  label: string;
}

export function ApprovalCard({ session }: { session: CodingSession }) {
  const pending = useMemo(() => session.pendingApprovals.map(itemOf), [session.pendingApprovals]);
  const answers = useMemo(() => answersFrom(session.sends), [session.sends]);
  const [hover, setHover] = useState(false);

  // Card membership: stable order, held arrivals while the pointer is on the card.
  const prev = useRef<CardState>(EMPTY_CARD);
  const card = reconcile(prev.current, pending, (id) => isAnswered(answers.get(id)), hover);
  useEffect(() => {
    prev.current = card;
  });

  // Arrivals after the first render get a one-off highlight.
  const initial = useRef<Set<string> | null>(null);
  if (initial.current === null) initial.current = new Set(card.order);

  const items = cardItems(card);
  const stack = buildStack(items, answers);
  const [open, setOpen] = useState<ReadonlySet<string>>(new Set());
  const [declining, setDeclining] = useState<DeclineTarget | null>(null);
  const toggle = (key: string) =>
    setOpen((s) => {
      const n = new Set(s);
      if (n.has(key)) n.delete(key);
      else n.add(key);
      return n;
    });

  if (items.length === 0) return null;

  const tools = toolsOf(session);
  const paused = !anythingRunning(session);
  const worker = session.telemetry.worker.answering;
  const isWaiting = (id: string) => !isAnswered(answers.get(id)) && pending.some((p) => p.toolId === id);
  const approve = (ids: readonly string[], remember = false) => {
    const live = ids.filter(isWaiting);
    if (live.length) void session.approve(live, remember ? { remember: true } : undefined);
  };
  const decline = (ids: readonly string[], reason: string) => {
    const live = ids.filter(isWaiting);
    if (live.length) void session.decline(live, reason.trim() || undefined);
    setDeclining(null);
  };
  const latest = latestRecord(items, answers);
  const agentKey = (it: ApprovalItem) => (it.agentId === session.rootAgentId ? null : (session.agent(it.agentId)?.agentKey ?? null));

  const declineForm = (key: string) =>
    declining?.key === key ? (
      <DeclineForm target={declining} onCancel={() => setDeclining(null)} onDecline={(reason) => decline(declining.ids, reason)} />
    ) : null;

  const pausedLine = (
    <p className="paused">
      <span className="dot you" aria-hidden="true" />
      {paused ? PAUSED_LINE : STILL_RUNNING_LINE}
      {items.length === 1 && paused ? ` ${SURVIVES_LINE}` : ""}
    </p>
  );

  // One call: the plain card (Frame 4 and "File edit · Trust safe tools").
  if (items.length === 1 && card.held.length === 0) {
    const it = items[0]!;
    const a = answers.get(it.toolId);
    const answered = isAnswered(a);
    const badge = toolBadge(it.toolName, it.risk);
    const files = session.changes?.files.length ?? 0;
    const branch = currentBranch(session.workspace, tools);
    return (
      <section className="dc-card" aria-label="Needs approval" onPointerEnter={() => setHover(true)} onPointerLeave={() => setHover(false)}>
        <div className="top">
          <span className={`badge ${badge.kind}`}>Needs approval</span>
          <span className="t">{callTitle(it.toolName, it.input, { branch: branch === DETACHED ? null : branch, files })}</span>
          {it.agentId !== session.rootAgentId && <span className="hint">from {agentLabel(agentKey(it))}</span>}
        </div>
        <ReviewBody item={it} />
        {pausedLine}
        <div className="acts">
          {answered && a ? (
            <span className={`badge ${a.approved ? "good" : "leaving"}`}>{answeredText(a.approved, a.record.remember)}</span>
          ) : (
            <>
              <button type="button" className="btn green small" onClick={() => approve([it.toolId])}>
                Approve
              </button>
              <AlwaysButton risk={it.risk} allowed={it.rememberAllowed} onClick={() => approve([it.toolId], true)} />
              <button
                type="button"
                className="btn ghost small"
                aria-expanded={declining?.key === "one"}
                onClick={() => setDeclining({ key: "one", ids: [it.toolId], label: "Decline" })}
              >
                Decline
              </button>
            </>
          )}
          <span className="spacer" />
          {latest && <SendStateTrail record={latest} workerAnswering={worker} />}
        </div>
        {declineForm("one")}
        <SendError answer={a} />
        <p className="why">{it.rememberAllowed ? alwaysWhy(it.risk) : `${alwaysApproveReason(it.risk)}.`}</p>
      </section>
    );
  }

  // Several calls: one stacked card.
  const w = stack.waitingIds.length;
  return (
    <section
      className="dc-card stacked"
      aria-label={stackHeading(w)}
      onPointerEnter={() => setHover(true)}
      onPointerLeave={() => setHover(false)}
    >
      <div className="top">
        <span className="badge edit">{w} need approval</span>
        <span className="t" aria-live="polite">
          {stackHeading(w)}
        </span>
        <span className="spacer" />
        <button type="button" className="btn green small all" disabled={w === 0} onClick={() => approve(approveAllTargets(stack))}>
          Approve all {w}
        </button>
      </div>
      <ul className="stack">
        {stack.groups.map((g) => (
          <GroupRows
            key={g.toolName}
            group={g}
            open={open}
            toggle={toggle}
            answers={answers}
            fresh={(id) => !initial.current!.has(id)}
            agentKey={agentKey}
            onApprove={approve}
            onDecline={(target) => setDeclining(target)}
            declining={declining}
            declineForm={declineForm}
          />
        ))}
      </ul>
      {card.held.length > 0 && (
        <p className="held" role="status">
          {card.held.length === 1 ? "1 more call is waiting." : `${card.held.length} more calls are waiting.`} It joins this card when you move the pointer
          off it.{" "}
          <button type="button" className="linkish" onClick={() => setHover(false)}>
            Show now
          </button>
        </p>
      )}
      {pausedLine}
      {latest && (
        <div className="acts end">
          <SendStateTrail record={latest} workerAnswering={worker} />
        </div>
      )}
    </section>
  );
}

function latestRecord(items: readonly ApprovalItem[], answers: ReadonlyMap<string, Answer>): SendRecord | null {
  let best: SendRecord | null = null;
  for (const it of items) {
    const r = answers.get(it.toolId)?.record;
    if (r && (!best || (r.at.sending ?? 0) >= (best.at.sending ?? 0))) best = r;
  }
  return best;
}

// -- pieces ---------------------------------------------------------------------------------

function alwaysWhy(risk: Risk): string {
  return risk === "workspace_edit" || risk === null
    ? ALWAYS_APPROVE_WHY
    : `Always approve lasts for this session. You can undo it from the posture menu.`;
}

function AlwaysButton({ risk, allowed, onClick }: { risk: Risk; allowed: boolean; onClick(): void }) {
  const id = useId();
  if (allowed) {
    return (
      <button type="button" className="btn secondary small" onClick={onClick}>
        {alwaysApproveLabel(risk)}
      </button>
    );
  }
  const reason = alwaysApproveReason(risk);
  return (
    <>
      <button type="button" className="btn ghost small off" aria-disabled="true" aria-describedby={id} title={reason} onClick={(e) => e.preventDefault()}>
        Always approve
      </button>
      <span id={id} className="sr-only">
        Not available: {reason}.
      </span>
    </>
  );
}

function ReviewBody({ item }: { item: Pick<ApprovalItem, "toolName" | "input"> }) {
  const r = reviewOf(item.toolName, item.input);
  if (r.kind === "diff") {
    return (
      <>
        <DiffView lines={r.lines} framed label={`Proposed change to ${String(item.input.path ?? "the file")}`} />
        {r.note && <p className="why">{r.note}</p>}
      </>
    );
  }
  return <pre className="args">{r.text}</pre>;
}

function SendError({ answer }: { answer: Answer | undefined }) {
  if (!answer || answer.phase !== "failed") return null;
  return (
    <p className="err" role="alert">
      Your answer didn't reach Temporal{answer.error ? `: ${answer.error}` : ""}. Try again.
    </p>
  );
}

function DeclineForm({ target, onCancel, onDecline }: { target: DeclineTarget; onCancel(): void; onDecline(reason: string): void }) {
  const [reason, setReason] = useState("");
  const id = useId();
  return (
    <form
      className="decline"
      onSubmit={(e) => {
        e.preventDefault();
        onDecline(reason);
      }}
      onKeyDown={(e) => {
        if (e.key === "Escape") {
          e.stopPropagation();
          onCancel();
        }
      }}
    >
      <label htmlFor={id}>Reason for the agent</label>
      <textarea id={id} rows={2} value={reason} onChange={(e) => setReason(e.target.value)} placeholder="Don't install anything. Use what's in the repo." autoFocus />
      <p className="hint">{DECLINE_HINT}</p>
      <div className="acts">
        <button type="submit" className="btn ghost small">
          {target.label}
        </button>
        <button type="button" className="btn ghost small quiet" onClick={onCancel}>
          Cancel
        </button>
      </div>
    </form>
  );
}

interface GroupRowsProps {
  group: Group;
  open: ReadonlySet<string>;
  toggle(key: string): void;
  answers: ReadonlyMap<string, Answer>;
  fresh(id: string): boolean;
  agentKey(it: ApprovalItem): string | null;
  onApprove(ids: readonly string[], remember?: boolean): void;
  onDecline(target: DeclineTarget): void;
  declining: DeclineTarget | null;
  declineForm(key: string): ReactNode;
}

function GroupRows(p: GroupRowsProps) {
  const { group: g } = p;
  const key = `g:${g.toolName}`;
  const expanded = p.open.has(key);
  const n = g.waitingIds.length;
  const title = groupTitle(g);
  const badge = toolBadge(g.toolName, g.risk);
  const panel = useId();
  return (
    <li className={`grp-wrap${expanded ? " open" : ""}`}>
      <div className="grp">
        <button type="button" className="chev" aria-expanded={expanded} aria-controls={panel} onClick={() => p.toggle(key)}>
          <span aria-hidden="true">{expanded ? "▾" : "▸"}</span>
          <span className={`badge ${badge.kind}`}>{badge.text}</span>
          <span className="title">
            {title.text} {title.code && <span className="mono">{title.code}</span>} {title.fold && <span className="hint">{title.fold}</span>}
          </span>
          <span className="sr-only">{expanded ? ", hide calls" : ", review each call"}</span>
        </button>
        <span className="spacer" />
        {n > 0 && (
          <span className="grp-acts">
            <button type="button" className="btn green small" onClick={() => p.onApprove(groupTargets(g))}>
              Approve {n}
            </button>
            <AlwaysButton risk={g.risk} allowed={g.rememberAllowed} onClick={() => p.onApprove(groupTargets(g), true)} />
            <button
              type="button"
              className="btn ghost small"
              aria-expanded={p.declining?.key === key}
              onClick={() => p.onDecline({ key, ids: groupTargets(g), label: `Decline ${n}` })}
            >
              Decline {n}
            </button>
          </span>
        )}
      </div>
      {p.declineForm(key)}
      {expanded && (
        <ul className="items" id={panel}>
          {!g.rememberAllowed && n > 0 && <li className="why">{alwaysApproveReason(g.risk)}.</li>}
          {g.folds.map((f) => (
            <FoldRows key={f.key} fold={f} {...p} />
          ))}
        </ul>
      )}
    </li>
  );
}

function itemLabel(it: Pick<ApprovalItem, "toolName" | "input">): string {
  return argSummary(it.toolName, it.input) || it.toolName;
}

function FoldRows(p: GroupRowsProps & { fold: Fold }) {
  const { fold: f } = p;
  const key = `f:${f.key}`;
  const expanded = p.open.has(key);
  const panel = useId();
  const label = itemLabel(f);
  const stat = editStat(f.toolName, f.input);

  if (f.items.length === 1) {
    const it = f.items[0]!;
    return (
      <li className={p.fresh(it.toolId) ? "arrived" : undefined}>
        <ItemRow
          item={it}
          label={label}
          hint={[stat, `from ${agentLabel(p.agentKey(it))}`].filter(Boolean).join(" · ")}
          expanded={expanded}
          panel={panel}
          onToggle={() => p.toggle(key)}
          {...p}
        />
        {expanded && (
          <div className="review" id={panel}>
            <ReviewBody item={it} />
          </div>
        )}
        {p.declineForm(`i:${it.toolId}`)}
      </li>
    );
  }

  const n = f.waiting.length;
  return (
    <li className={f.items.some((i) => p.fresh(i.toolId)) ? "arrived" : undefined}>
      <div className={`item${n === 0 ? " done" : ""}`}>
        <button type="button" className="chev" aria-expanded={expanded} aria-controls={panel} onClick={() => p.toggle(key)}>
          <span aria-hidden="true">{expanded ? "▾" : "▸"}</span>
          <span className="mono label">{label}</span>
          <span className="hint">
            × {f.items.length}, same arguments{stat ? ` · ${stat}` : ""}
          </span>
        </button>
        <span className="spacer" />
        {n > 0 ? (
          <span className="item-acts">
            <button type="button" className="ok" aria-label={`Approve ${n} identical calls: ${label}`} title={`Approve ${n}`} onClick={() => p.onApprove(f.waiting.map((i) => i.toolId))}>
              ✓<span className="n">{n}</span>
            </button>
            <button
              type="button"
              className="no"
              aria-label={`Decline ${n} identical calls: ${label}`}
              title={`Decline ${n}`}
              onClick={() => p.onDecline({ key: key, ids: f.waiting.map((i) => i.toolId), label: `Decline ${n}` })}
            >
              ✕<span className="n">{n}</span>
            </button>
          </span>
        ) : (
          <span className="badge good">All answered</span>
        )}
      </div>
      {p.declineForm(key)}
      {expanded && (
        <div className="review" id={panel}>
          <ReviewBody item={f} />
          <ul className="members">
            {f.items.map((it, i) => (
              <li key={it.toolId} className={p.fresh(it.toolId) ? "arrived" : undefined}>
                <ItemRow item={it} label={`Call ${i + 1}`} hint={`from ${agentLabel(p.agentKey(it))}`} {...p} />
                {p.declineForm(`i:${it.toolId}`)}
              </li>
            ))}
          </ul>
        </div>
      )}
    </li>
  );
}

function ItemRow(
  p: GroupRowsProps & { item: ApprovalItem; label: string; hint: string; expanded?: boolean; panel?: string; onToggle?(): void },
) {
  const a = p.answers.get(p.item.toolId);
  const answered = isAnswered(a);
  const name = p.onToggle ? (
    <button type="button" className="chev" aria-expanded={p.expanded} aria-controls={p.panel} onClick={p.onToggle}>
      <span aria-hidden="true">{p.expanded ? "▾" : "▸"}</span>
      <span className="mono label">{p.label}</span>
      <span className="hint">{p.hint}</span>
    </button>
  ) : (
    <span className="chev static">
      <span className="mono label">{p.label}</span>
      <span className="hint">{p.hint}</span>
    </span>
  );
  return (
    <>
      <div className={`item${answered ? " done" : ""}`}>
        {name}
        <span className="spacer" />
        {answered && a ? (
          <>
            <span className={`badge ${a.approved ? "good" : "leaving"}`}>{answeredText(a.approved, a.record.remember)}</span>
            {a.phase !== "done" && <SendStateTrail record={a.record} />}
          </>
        ) : (
          <span className="item-acts">
            <button type="button" className="ok" aria-label={`Approve ${p.label}`} title="Approve" onClick={() => p.onApprove([p.item.toolId])}>
              ✓
            </button>
            <button
              type="button"
              className="no"
              aria-label={`Decline ${p.label}`}
              title="Decline"
              onClick={() => p.onDecline({ key: `i:${p.item.toolId}`, ids: [p.item.toolId], label: "Decline" })}
            >
              ✕
            </button>
          </span>
        )}
      </div>
      <SendError answer={a} />
    </>
  );
}

