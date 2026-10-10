import assert from "node:assert/strict";
import test from "node:test";

import {
  TURN_STREAM_FIRST_EVENT_TIMEOUT_MS,
  TURN_STREAM_MAX_RECOVERY_DELAY_MS,
  turnStreamNeedsRecovery,
  turnStreamRecoveryDelay,
} from "./turnStream.js";

test("first-event recovery starts quickly and backs off to a bound", () => {
  assert.equal(TURN_STREAM_FIRST_EVENT_TIMEOUT_MS, 2000);
  assert.equal(turnStreamRecoveryDelay(0), 250);
  assert.equal(turnStreamRecoveryDelay(1), 500);
  assert.equal(turnStreamRecoveryDelay(20), TURN_STREAM_MAX_RECOVERY_DELAY_MS);
});

test("terminal stream events suppress reconnect recovery", () => {
  assert.equal(turnStreamNeedsRecovery({ settled: false, reconciled: false }), true);
  assert.equal(turnStreamNeedsRecovery({ settled: true, reconciled: false }), false);
  assert.equal(turnStreamNeedsRecovery({ settled: false, reconciled: true }), false);
});
