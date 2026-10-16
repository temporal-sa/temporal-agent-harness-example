// Above the composer (PRD U4): the stacked approval card (owner decisions A and B), then the
// checkpoint and spend-cap cards.

import { useEffect, useId, useLayoutEffect, useMemo, useRef, useState, type FocusEvent, type ReactNode } from "react";

import { errorCopy, type CodingSession, type DecisionDockProps, type SendRecord } from "../session";
import { FailedNote, SendStateTrail } from "../recovery/SendStateTrail";
import { usePhone } from "../recovery/usePhone";
import { currentBranch, DETACHED } from "../workbench/changes";
import { DiffView } from "../workbench/DiffView";
import { agentLabel, anythingRunning, toolsOf } from "../workbench/tree";
import {
  ALWAYS_APPROVE_WHY,
  alwaysApproveLabel,
  alwaysApproveReason,
  argSummary,
  callNoun,
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
import { answeredText, groupTitle, SAME, stackHeading } from "./labels";
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
export const SURVIVES_LINE = "This request survives reloads and Worker restarts.";
export const STILL_RUNNING_LINE = "Other work in this turn keeps running while these wait.";

// -- the card -------------------------------------------------------------------------------

interface DeclineTarget {
  key: string;
  ids: string[];
  /** The submit button's text, and its accessible name. */
  label: string;
  name?: string;
}

export function ApprovalCard({ session }: { session: CodingSession }) {
  const pending = useMemo(() => session.pendingApprovals.map(itemOf), [session.pendingApprovals]);
  const answers = useMemo(() => answersFrom(session.sends), [session.sends]);
  // Arrivals are held while the pointer or keyboard focus is in the card, so nothing moves
  // under either (owner decision B). "Show now" lets them join at once.
  const [hover, setHover] = useState(false);
  const [focusIn, setFocusIn] = useState(false);
  const [joinNow, setJoinNow] = useState(false);
  const hold = (hover || focusIn) && !joinNow;

  // Card membership: stable order, held arrivals.
  const prev = useRef<CardState>(EMPTY_CARD);
  const card = reconcile(prev.current, pending, (id) => isAnswered(answers.get(id)), hold);
  useEffect(() => {
    prev.current = card;
    if (joinNow && card.held.length === 0) setJoinNow(false);
  });

  // Arrivals after the first render get a one-off highlight.
  const initial = useRef<Set<string> | null>(null);
  if (initial.current === null) initial.current = new Set(card.order);

  const items = cardItems(card);
  const stack = buildStack(items, answers);
  const [open, setOpen] = useState<ReadonlySet<string>>(new Set());
  const [declining, setDeclining] = useState<DeclineTarget | null>(null);
  // On a phone the stacked card starts as one line (heading and Approve all) plus a "Review"
  // toggle, so the transcript keeps its room.
  const phone = usePhone();
  const [phoneOpen, setPhoneOpen] = useState(false);
  // While the pointer or focus is on the card, its height is locked: opening a group, the
  // decline form or an answer then scrolls the list instead of growing the card, which sits on
  // the composer and would push its header (and Approve all) up (owner decision B).
  const [lockH, setLockH] = useState<number | null>(null);
  // Not on a phone: there the card starts collapsed and its header is already pinned, and a
  // tap's compatibility mouse events would lock the collapsed height, leaving the opened list
  // no room (its rows then fell under the shell's sheet strip).
  const lockable = (hover || focusIn) && !phone;
  useLayoutEffect(() => {
    if (!lockable) {
      if (lockH !== null) setLockH(null);
      return;
    }
    if (lockH === null && root.current) setLockH(root.current.getBoundingClientRect().height);
  }, [lockable, lockH]);
  // Wheel over the card scrolls its list, never the transcript behind it.
  useEffect(() => {
    const el = root.current;
    if (!el) return;
    const onWheel = (e: WheelEvent) => {
      if (Math.abs(e.deltaY) <= Math.abs(e.deltaX)) return;
      e.preventDefault();
      const body = el.querySelector<HTMLElement>(".dc-body");
      if (body) body.scrollTop += e.deltaMode === 1 ? e.deltaY * 16 : e.deltaY;
    };
    el.addEventListener("wheel", onWheel, { passive: false });
    return () => el.removeEventListener("wheel", onWheel);
  });
  const toggle = (key: string) =>
    setOpen((s) => {
      const n = new Set(s);
      if (n.has(key)) n.delete(key);
      else n.add(key);
      return n;
    });

  // Focus after an answer: the answered control goes away, so move focus on (M3) instead of
  // letting it fall to <body>.
  const root = useRef<HTMLElement | null>(null);
  const refocus = useRef<{ row: string; at: number } | null>(null);
  const focusWasInside = useRef(false);
  useLayoutEffect(() => {
    const el = root.current;
    const active = document.activeElement;
    // Lost: nothing focused, the focused control went away, or it became disabled (Approve all
    // once nothing waits).
    const lost = !active || active === document.body || !active.isConnected || (active as HTMLButtonElement).disabled === true;
    if (!el) {
      if (focusIn) setFocusIn(false);
      if (hover) setHover(false);
      // The card left (everything settled) while focus was in it: hand focus to the composer.
      if (focusWasInside.current && lost) {
        focusWasInside.current = false;
        document.querySelector<HTMLElement>('[data-slot="composer"] textarea')?.focus();
      }
      return;
    }
    const r = refocus.current;
    if (!r) return;
    if (Date.now() - r.at > 3000) {
      refocus.current = null;
      return;
    }
    if (!lost && el.contains(active)) return; // the answer hasn't rendered yet
    refocus.current = null;
    nextFocus(el, r.row)?.focus();
  });

  if (items.length === 0) return null;

  const tools = toolsOf(session);
  const paused = !anythingRunning(session);
  const worker = session.telemetry.worker.answering;
  const isWaiting = (id: string) => !isAnswered(answers.get(id)) && pending.some((p) => p.toolId === id);
  const approve = (row: string, ids: readonly string[], remember = false) => {
    const live = ids.filter(isWaiting);
    refocus.current = { row, at: Date.now() };
    if (live.length) void session.approve(live, remember ? { remember: true } : undefined);
  };
  const decline = (row: string, ids: readonly string[], reason: string) => {
    const live = ids.filter(isWaiting);
    refocus.current = { row, at: Date.now() };
    if (live.length) void session.decline(live, reason.trim() || undefined);
    setDeclining(null);
  };
  const latest = latestRecord(items, answers);
  const agentKey = (it: ApprovalItem) => (it.agentId === session.rootAgentId ? null : (session.agent(it.agentId)?.agentKey ?? null));

  const declineForm = (key: string) =>
    declining?.key === key ? (
      <DeclineForm
        target={declining}
        onCancel={() => {
          refocus.current = { row: key, at: Date.now() };
          setDeclining(null);
        }}
        onDecline={(reason) => decline(key, declining.ids, reason)}
      />
    ) : null;

  const pausedLine = (
    <p className="paused">
      <span className="dot you" aria-hidden="true" />
      {paused ? PAUSED_LINE : STILL_RUNNING_LINE}
      {items.length === 1 && paused ? ` ${SURVIVES_LINE}` : ""}
    </p>
  );

  const cardProps = {
    ref: (el: HTMLElement | null) => {
      root.current = el;
    },
    style: lockH !== null ? { height: `${lockH}px` } : undefined,
    onPointerEnter: () => setHover(true),
    onPointerLeave: () => setHover(false),
    onFocus: () => {
      focusWasInside.current = true;
      setFocusIn(true);
    },
    onBlur: (e: FocusEvent<HTMLElement>) => {
      const next = e.relatedTarget as Node | null;
      if (next && e.currentTarget.contains(next)) return;
      const leave = () => {
        focusWasInside.current = false;
        setFocusIn(false);
      };
      if (next) return leave();
      // No new target: either focus left for nowhere (a click on the page), or the focused
      // control went away because it was answered. In the second case keep holding until the
      // refocus above lands, so a held arrival doesn't join in between.
      const card = e.currentTarget;
      window.setTimeout(() => {
        if (refocus.current) return;
        if (!card.isConnected || !card.contains(document.activeElement)) leave();
      }, 0);
    },
  };
  const footer = <CardFooter held={card.held.length} onShow={() => setJoinNow(true)} trail={latest} workerAnswering={worker} />;

  // One call: the plain card (Frame 4 and "File edit · Trust safe tools").
  if (items.length === 1) {
    const it = items[0]!;
    const a = answers.get(it.toolId);
    const answered = isAnswered(a);
    const badge = toolBadge(it.toolName, it.risk);
    const files = session.changes?.files.length ?? 0;
    const branch = currentBranch(session.workspace, tools);
    const from = agentKey(it);
    return (
      <section className="dc-card single" aria-label="Needs approval" {...cardProps}>
        <div className="top">
          <span className={`badge ${badge.kind}`}>Needs approval</span>
          <span className="t" tabIndex={-1} data-heading="">
            {callTitle(it.toolName, it.input, { branch: branch === DETACHED ? null : branch, files })}
          </span>
          {from && <span className="hint">from {agentLabel(from)}</span>}
        </div>
        {pausedLine}
        <div className="dc-body">
          <ReviewBody item={it} />
        </div>
        <div className="acts" data-row="one">
          {answered && a ? (
            <span className={`badge ${a.approved ? "good" : "leaving"}`}>{answeredText(a.approved, a.record.remember)}</span>
          ) : (
            <>
              <button type="button" className="btn green small" data-answer="item" onClick={() => approve("one", [it.toolId])}>
                Approve
              </button>
              <AlwaysButton risk={it.risk} toolName={it.toolName} allowed={it.rememberAllowed} onClick={() => approve("one", [it.toolId], true)} />
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
        </div>
        {declineForm("one")}
        <SendError answer={a} />
        <p className="why">{it.rememberAllowed ? alwaysWhy(it.risk) : `${alwaysApproveReason(it.risk, it.toolName)}.`}</p>
        {footer}
      </section>
    );
  }

  // Several calls: one stacked card. A column: the header (Approve all) and the paused line at
  // the top, the footer at the bottom, and only the list of calls scrolls.
  const w = stack.waitingIds.length;
  const compact = phone && !phoneOpen;
  return (
    <section className={`dc-card stacked${compact ? " compact" : ""}`} aria-label={stackHeading(w)} {...cardProps}>
      <div className="top" data-row="all">
        <span className="badge edit">{w} need approval</span>
        <span className="t" aria-live="polite" tabIndex={-1} data-heading="">
          {stackHeading(w)}
        </span>
        <span className="spacer" />
        <button type="button" className="btn green small all" data-answer="all" disabled={w === 0} onClick={() => approve("all", approveAllTargets(stack))}>
          Approve all {w}
        </button>
      </div>
      {phone && (
        <button type="button" className="dc-review" aria-expanded={phoneOpen} onClick={() => setPhoneOpen((o) => !o)}>
          <span aria-hidden="true">{phoneOpen ? "▾" : "▸"}</span> {phoneOpen ? "Hide the calls" : `Review ${w === 1 ? "the call" : `the ${stack.total} calls`}`}
        </button>
      )}
      {!compact && pausedLine}
      {!compact && (
        <div className="dc-body">
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
        </div>
      )}
      {(!compact || card.held.length > 0 || latest) && footer}
    </section>
  );
}

/**
 * Where focus goes after an answer from row `row`: the next waiting call in the same group,
 * else that group's row, else the next answer control on the card, else the card's heading.
 */
export function nextFocus(card: HTMLElement, row: string): HTMLElement | null {
  const anchor = card.querySelector<HTMLElement>(`[data-row="${CSS.escape(row)}"]`);
  const usable = (list: Iterable<HTMLElement>) => [...list].filter((b) => !(b as HTMLButtonElement).disabled && b.isConnected);
  const after = (b: HTMLElement) => !!anchor && !anchor.contains(b) && !!(anchor.compareDocumentPosition(b) & Node.DOCUMENT_POSITION_FOLLOWING);
  const group = anchor?.closest<HTMLElement>(".grp-wrap");
  if (group) {
    const inGroup = usable(group.querySelectorAll<HTMLElement>('[data-answer="item"]'));
    const next = inGroup.find(after) ?? inGroup[0];
    if (next) return next;
    if (group.querySelector('[data-answer="group"]')) return group.querySelector<HTMLElement>("[data-group-toggle]");
  }
  const all = usable(card.querySelectorAll<HTMLElement>('[data-answer="item"], [data-answer="group"]'));
  return all.find(after) ?? all[0] ?? card.querySelector<HTMLElement>("[data-heading]");
}

/**
 * The card's last row, a fixed height whatever it shows: held arrivals ("1 more call is
 * waiting."), else the delivery trail of the latest answer. The card sits on the composer and
 * grows upward, so a row that appeared would push every button above it (owner decision B).
 */
function CardFooter({ held, onShow, trail, workerAnswering }: { held: number; onShow(): void; trail: SendRecord | null; workerAnswering: boolean }) {
  return (
    <div className="dc-foot">
      <p className="held" role="status">
        {held > 0 && (
          <>
            {held === 1 ? "1 more call is waiting." : `${held} more calls are waiting.`}
            <span className="sr-only"> It joins this card when you move the pointer or focus off it.</span>{" "}
            <button type="button" className="linkish" onClick={onShow}>
              Show now
            </button>
          </>
        )}
      </p>
      {held === 0 && trail && <SendStateTrail record={trail} workerAnswering={workerAnswering} />}
    </div>
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

function AlwaysButton({ risk, toolName, allowed, onClick, name }: { risk: Risk; toolName: string; allowed: boolean; onClick(): void; name?: string }) {
  const id = useId();
  if (allowed) {
    return (
      <button type="button" className="btn secondary small" aria-label={name} onClick={onClick}>
        {alwaysApproveLabel(risk)}
      </button>
    );
  }
  const reason = alwaysApproveReason(risk, toolName);
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
  const r = answer.record;
  const copy = errorCopy({ code: r.errorCode ?? null, status: r.errorStatus ?? null, message: r.error }, "Check your connection and try again.");
  return <FailedNote className="err" copy={{ text: `Your answer didn't reach Temporal. ${copy.text}`, detail: copy.detail }} />;
}

export const DECLINE_PLACEHOLDER = "Reason (optional). The agent sees this.";

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
      <textarea id={id} rows={2} value={reason} onChange={(e) => setReason(e.target.value)} placeholder={DECLINE_PLACEHOLDER} autoFocus />
      <div className="acts">
        <button type="submit" className="btn ghost small" aria-label={target.name}>
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
  onApprove(row: string, ids: readonly string[], remember?: boolean): void;
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
  const noun = callNoun(g.toolName, n);
  // One fold (every call identical, or a single call): opening the group shows that fold's
  // review and calls directly, with no second level to open.
  const only = g.folds.length === 1 ? g.folds[0]! : null;
  return (
    <li className={`grp-wrap${expanded ? " open" : ""}`}>
      <div className="grp" data-row={key}>
        <button type="button" className="chev" data-group-toggle="" aria-expanded={expanded} aria-controls={panel} onClick={() => p.toggle(key)}>
          <span aria-hidden="true">{expanded ? "▾" : "▸"}</span>
          <span className={`badge ${badge.kind}`}>{badge.text}</span>
          <span className="title">
            {title.text} {title.code && <span className="mono code">{title.code}</span>} {title.fold && <span className="hint">{title.fold}</span>}
          </span>
          <span className="sr-only">{expanded ? ", hide calls" : ", review each call"}</span>
        </button>
        <span className="spacer" />
        {n > 0 ? (
          <span className="grp-acts">
            <button type="button" className="btn green small" data-answer="group" aria-label={`Approve ${n} ${noun}`} onClick={() => p.onApprove(key, groupTargets(g))}>
              Approve {n}
            </button>
            <AlwaysButton
              risk={g.risk}
              toolName={g.toolName}
              allowed={g.rememberAllowed}
              name={`${alwaysApproveLabel(g.risk)}, approving these ${n}`}
              onClick={() => p.onApprove(key, groupTargets(g), true)}
            />
            <button
              type="button"
              className="btn ghost small"
              aria-label={`Decline ${n} ${noun}`}
              aria-expanded={p.declining?.key === key}
              onClick={() => p.onDecline({ key, ids: groupTargets(g), label: `Decline ${n}`, name: `Decline ${n} ${noun}` })}
            >
              Decline {n}
            </button>
          </span>
        ) : (
          <span className="badge good">All answered</span>
        )}
      </div>
      {p.declineForm(key)}
      {expanded && (
        <div className="items-wrap" id={panel}>
          {!g.rememberAllowed && n > 0 && <p className="why">{alwaysApproveReason(g.risk, g.toolName)}.</p>}
          {only ? (
            <FoldBody {...p} fold={only} />
          ) : (
            <ul className="items">
              {g.folds.map((f) => (
                <FoldRows key={f.key} {...p} fold={f} />
              ))}
            </ul>
          )}
        </div>
      )}
    </li>
  );
}

function itemLabel(it: Pick<ApprovalItem, "toolName" | "input">): string {
  return argSummary(it.toolName, it.input) || it.toolName;
}

function fromHint(p: GroupRowsProps, it: ApprovalItem): string | null {
  const key = p.agentKey(it);
  return key ? `from ${agentLabel(key)}` : null;
}

/** A fold opened in place: its arguments or diff, then each call when there are several. */
function FoldBody(p: GroupRowsProps & { fold: Fold }) {
  const { fold: f } = p;
  const label = itemLabel(f);
  if (f.items.length === 1) {
    return (
      <div className="review">
        <ReviewBody item={f.items[0]!} />
        <SendError answer={p.answers.get(f.items[0]!.toolId)} />
      </div>
    );
  }
  return (
    <div className="review">
      <ReviewBody item={f} />
      <ul className="members">
        {f.items.map((it, i) => (
          <li key={it.toolId} className={p.fresh(it.toolId) ? "arrived" : undefined}>
            <ItemRow
              item={it}
              label={`Call ${i + 1}`}
              name={`call ${i + 1} of ${f.items.length}: ${label}`}
              hint={fromHint(p, it)}
              {...p}
            />
            {p.declineForm(`i:${it.toolId}`)}
          </li>
        ))}
      </ul>
    </div>
  );
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
          name={`${callNoun(f.toolName, 1)}: ${label}`}
          hint={[stat, fromHint(p, it)].filter(Boolean).join(" · ") || null}
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
      <div className={`item${n === 0 ? " done" : ""}`} data-row={key}>
        <button type="button" className="chev" aria-expanded={expanded} aria-controls={panel} onClick={() => p.toggle(key)}>
          <span aria-hidden="true">{expanded ? "▾" : "▸"}</span>
          <span className="mono label">{label}</span>
          <span className="hint">
            × {f.items.length}, {SAME}
            {stat ? ` · ${stat}` : ""}
          </span>
        </button>
        <span className="spacer" />
        {n > 0 ? (
          <span className="item-acts">
            <button
              type="button"
              className="ok"
              data-answer="item"
              aria-label={`Approve ${n} identical calls: ${label}`}
              title={`Approve ${n}`}
              onClick={() => p.onApprove(key, f.waiting.map((i) => i.toolId))}
            >
              ✓<span className="n">{n}</span>
            </button>
            <button
              type="button"
              className="no"
              aria-label={`Decline ${n} identical calls: ${label}`}
              title={`Decline ${n}`}
              onClick={() => p.onDecline({ key, ids: f.waiting.map((i) => i.toolId), label: `Decline ${n}`, name: `Decline ${n} identical calls: ${label}` })}
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
        <div id={panel}>
          <FoldBody {...p} fold={f} />
        </div>
      )}
    </li>
  );
}

/**
 * One call. Answered, it keeps its row and its height: a badge replaces the two buttons, and
 * the delivery trail shows once, in the card's footer (M1).
 */
function ItemRow(
  p: GroupRowsProps & { item: ApprovalItem; label: string; name: string; hint: string | null; expanded?: boolean; panel?: string; onToggle?(): void },
) {
  const a = p.answers.get(p.item.toolId);
  const answered = isAnswered(a);
  const row = `i:${p.item.toolId}`;
  const inner = (
    <>
      <span className="mono label">{p.label}</span>
      {p.hint && <span className="hint">{p.hint}</span>}
    </>
  );
  const name = p.onToggle ? (
    <button type="button" className="chev" aria-expanded={p.expanded} aria-controls={p.panel} onClick={p.onToggle}>
      <span aria-hidden="true">{p.expanded ? "▾" : "▸"}</span>
      {inner}
    </button>
  ) : (
    <span className="chev static">{inner}</span>
  );
  return (
    <>
      <div className={`item${answered ? " done" : ""}`} data-row={row}>
        {name}
        <span className="spacer" />
        <span className="item-acts">
          {answered && a ? (
            <span className={`badge ${a.approved ? "good" : "leaving"}`}>{answeredText(a.approved, a.record.remember)}</span>
          ) : (
            <>
              <button type="button" className="ok" data-answer="item" aria-label={`Approve ${p.name}`} title="Approve" onClick={() => p.onApprove(row, [p.item.toolId])}>
                ✓
              </button>
              <button
                type="button"
                className="no"
                aria-label={`Decline ${p.name}`}
                title="Decline"
                onClick={() => p.onDecline({ key: row, ids: [p.item.toolId], label: "Decline", name: `Decline ${p.name}` })}
              >
                ✕
              </button>
            </>
          )}
        </span>
      </div>
      <SendError answer={a} />
    </>
  );
}
