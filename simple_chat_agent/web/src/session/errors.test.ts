import { describe, expect, it } from "vitest";
import { errorCopy } from "./errors";

describe("error copy (UX review M9)", () => {
  it("maps known codes to copy that says what happened and what to do, keeping the raw text apart", () => {
    expect(errorCopy({ code: "seed_failed", status: 502, message: "executor said 500: traceback…" })).toEqual({
      text: "Couldn't set up the demo workspace. Nothing was started; try again in a moment.",
      detail: "executor said 500: traceback…",
    });
    expect(errorCopy({ code: "busy", message: "MidTurnRejected" }).text).toMatch(/^The agent is busy/);
    expect(errorCopy({ code: "executor_unavailable" }).text).toBe("The sandbox isn't answering. Temporal will retry the command; try again shortly.");
  });

  it("covers the API's id checks", () => {
    expect(errorCopy({ code: "invalid_attachment", status: 422 }).text).toBe("That file can't be attached, or it's no longer available. Attach it again.");
    expect(errorCopy({ code: "invalid_request_id", status: 422 }).text).toBe("The app sent a request the server couldn't match. Reload the page and try again.");
  });

  it("falls back on the HTTP status, then on the caller's sentence", () => {
    expect(errorCopy({ status: 503, message: "Bad Gateway" }).text).toMatch(/^The app's API isn't answering/);
    expect(errorCopy({ status: 0, message: "Failed to fetch" }).text).toMatch(/^Couldn't reach the app/);
    expect(errorCopy(new TypeError("Failed to fetch")).text).toMatch(/^Couldn't reach the app/);
    expect(errorCopy({ status: 418, message: "teapot" }, "Couldn't attach this file.")).toEqual({ text: "Couldn't attach this file.", detail: "teapot" });
    expect(errorCopy(null).text).toBe("Something went wrong. Try again.");
  });
});
