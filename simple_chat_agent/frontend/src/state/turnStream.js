export const TURN_STREAM_FIRST_EVENT_TIMEOUT_MS = 2000;
export const TURN_STREAM_MAX_RECOVERY_DELAY_MS = 5000;

export function turnStreamRecoveryDelay(attempt) {
  const normalizedAttempt = Math.max(0, Number(attempt) || 0);
  return Math.min(250 * (2 ** normalizedAttempt), TURN_STREAM_MAX_RECOVERY_DELAY_MS);
}

export function turnStreamNeedsRecovery({ settled, reconciled }) {
  return !settled && !reconciled;
}
