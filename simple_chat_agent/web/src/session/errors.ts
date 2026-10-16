// What to say when something fails (buildkit guide 09; UX review M9). Every message says what
// happened, what Temporal does next when it does anything, and what you can do. The server's
// raw text is kept as `detail`, for a disclosure or a tooltip, never as the headline.

export interface ErrorCopy {
  text: string;
  /** The raw server or agent text, when it adds anything. */
  detail: string | null;
}

export interface ErrorLike {
  code?: string | null;
  status?: number | null;
  message?: string | null;
}

const BY_CODE: Record<string, string> = {
  // API (agent_routes.py)
  not_found: "No such session. It doesn't exist, or it belongs to someone else.",
  session_closed: "This session is closed. Start a new one.",
  model_not_allowed: "That model isn't available here. Pick another one.",
  posture_not_allowed: "That approval setting isn't available here. Pick another one.",
  seed_failed: "Couldn't set up the demo workspace. Nothing was started; try again in a moment.",
  creating: "This session is still being set up. Try again in a few seconds.",
  session_exists: "That session id is already taken.",
  too_large: "That file is too large to attach.",
  invalid_attachment: "That file can't be attached, or it's no longer available. Attach it again.",
  output_expired: "This output is no longer kept.",
  executor_unavailable: "The sandbox isn't answering. Temporal will retry the command; try again shortly.",
  worker_unavailable: "No Worker is running this subagent right now. Try again when one is back.",
  not_supported: "This can't be sent to a subagent; send it to the session.",
  tool_approval_error: "That call was already answered or no longer needs an answer.",
  not_implemented: "This isn't available yet.",
  invalid_request: "The app sent something the server didn't accept. Reload the page and try again.",
  invalid_request_id: "The app sent a request the server couldn't match. Reload the page and try again.",
  // The agent's front door (x_rejected)
  unknown_approval: "That call no longer needs an answer.",
  already_resolved: "That call was already answered.",
  unknown_function: "The agent doesn't accept that request.",
  malformed_message: "The agent couldn't read that request. Reload the page and try again.",
  busy: "The agent is busy with a turn that doesn't take this now. Try again when it finishes.",
  rejected: "The agent refused this request.",
  // Explained turn failures (ChatReply.error)
  SpendCapReached: "This session reached its usage limit. Start a new session or try again tomorrow.",
  SpendGuardUnavailable: "The usage limit can't be checked right now, so the turn stopped. Try again shortly.",
  ProviderRejected: "The model provider rejected the request. Try another model.",
  ContextOverflow: "This conversation no longer fits the model's context window. Start a new session.",
};

const BY_STATUS = (status: number): string | null => {
  if (status === 0) return "Couldn't reach the app. Check your connection and try again; your session is safe in Temporal.";
  if (status === 401) return "You're signed out. Reload the page to sign in again.";
  if (status === 403 || status === 404) return BY_CODE.not_found ?? null;
  if (status === 413) return BY_CODE.too_large ?? null;
  if (status >= 502 && status <= 504) return "The app's API isn't answering. Your session is safe in Temporal; try again shortly.";
  if (status >= 500) return "The app hit an error. Your session is safe in Temporal; try again.";
  return null;
};

/** User-facing copy for a failure, with the raw text kept apart. */
export function errorCopy(error: ErrorLike | unknown, fallback = "Something went wrong. Try again."): ErrorCopy {
  const e = (typeof error === "object" && error !== null ? error : {}) as ErrorLike;
  // fetch rejects with a TypeError when the network or the server is unreachable.
  const status = error instanceof TypeError ? 0 : e.status;
  const raw = typeof e.message === "string" && e.message.trim() ? e.message.trim() : null;
  const text = (e.code && BY_CODE[e.code]) || (typeof status === "number" ? BY_STATUS(status) : null) || fallback;
  return { text, detail: raw && raw !== text ? raw : null };
}
