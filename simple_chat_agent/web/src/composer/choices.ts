// Pure helpers for the composer's model picker and posture menu.

import type { AgentConfig, Effort, ModelChoice, Posture, PostureMenuOption, SendRecord } from "../session";

export function modelName(config: AgentConfig | null, choice: Pick<ModelChoice, "provider" | "model"> | null): string {
  if (!choice) return "Model";
  return config?.models.find((m) => m.provider === choice.provider && m.id === choice.model)?.display_name ?? choice.model;
}

export function providerName(config: AgentConfig | null, provider: string): string {
  return config?.providers.find((p) => p.provider === provider)?.label ?? provider;
}

export interface PickerState {
  /** The choice the button shows: what was just picked, else the session's model. */
  shown: ModelChoice | null;
  /** "applies next turn", "Sending…", "Couldn't change the model" or null. */
  note: string | null;
  failed: boolean;
}

/** The picker acknowledges a pick at once (the pending choice and "Sending…"), then says
 *  "applies next turn" until the next turn starts with it. */
export function pickerState(
  current: ModelChoice | null,
  appliesNextTurn: boolean,
  pending: { choice: ModelChoice; record: SendRecord | undefined } | null,
): PickerState {
  if (pending && pending.record?.phase === "failed") {
    return { shown: current, note: "Couldn't change the model", failed: true };
  }
  if (pending && (!pending.record || pending.record.phase === "sending")) {
    return { shown: pending.choice, note: "Sending…", failed: false };
  }
  if (pending && pending.record && pending.record.phase === "in_temporal") {
    return { shown: pending.choice, note: "applies next turn", failed: false };
  }
  return { shown: current, note: appliesNextTurn ? "applies next turn" : null, failed: false };
}

export function choiceLabel(config: AgentConfig | null, choice: ModelChoice | null): string {
  if (!choice) return "Model";
  const effort = choice.effort ? ` · effort ${choice.effort}` : "";
  return `${modelName(config, choice)}${effort}`;
}

export function effortsFor(config: AgentConfig | null, choice: Pick<ModelChoice, "provider" | "model"> | null): Effort[] {
  const entry = choice ? config?.models.find((m) => m.provider === choice.provider && m.id === choice.model) : undefined;
  return entry?.effort_options ?? config?.effort_options ?? [];
}

/** The posture menu's rows: `posture_options` from config (every posture, greyed with a
 *  reason when it can't be chosen here), else the allowed `postures`. */
export function postureOptions(config: AgentConfig | null): PostureMenuOption[] {
  if (config?.posture_options?.length) return config.posture_options;
  return (config?.postures ?? []).map((p) => ({
    id: p.id,
    label: p.label,
    description: p.description,
    available: true,
    reason: null,
    default: p.id === config?.default_posture,
  }));
}

export function postureLabel(config: AgentConfig | null, id: Posture | string | null): string {
  if (!id) return "Approvals";
  return postureOptions(config).find((p) => p.id === id)?.label ?? id.replace(/_/g, " ");
}

/** "file edits" for tools config classes as workspace edits; other names as they are. */
export function alwaysApprovedText(tools: readonly string[], config: Pick<AgentConfig, "tool_risk_classes"> | null): string {
  const parts = new Set<string>();
  for (const t of tools) parts.add(config?.tool_risk_classes?.[t] === "workspace_edit" ? "file edits" : t);
  return [...parts].join(", ");
}
