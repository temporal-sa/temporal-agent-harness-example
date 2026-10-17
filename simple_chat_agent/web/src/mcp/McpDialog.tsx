// The "MCP servers" dialog (PRD A8; local only): list, add, remove, status and Reconnect for
// this user's MCP servers. A modal Dialog: focus moves in, Tab stays inside, Esc closes it, and
// focus returns to the opener. OAuth (add or Reconnect) leaves for the provider and comes back
// to /?mcp=connected (see `mcpReturnNotice`).

import { useCallback, useEffect, useId, useRef, useState, type FormEvent } from "react";

import { api, ApiError, type McpFeature } from "../session";
import type { McpAuthMode, McpServer, McpStatus } from "../session/api";
import { Dialog } from "../shell/Dialog";

const STATUS_TEXT: Record<McpStatus, string> = {
  connected: "Connected",
  needs_reauth: "Needs you to reconnect",
  unreachable: "Not reachable",
  blocked: "Blocked address",
};

const AUTH_TEXT: Record<McpAuthMode, string> = { none: "No sign-in", bearer: "Bearer token", oauth: "OAuth" };

/** Copy for the MCP routes' error codes (P4 UI contract). */
export function mcpErrorText(err: unknown): string {
  const code = err instanceof ApiError ? err.code : null;
  switch (code) {
    case "mcp_disabled":
      return "MCP servers are off on this server.";
    case "invalid_name":
      return "Use 1 to 24 lowercase letters, digits or dashes, starting with a letter.";
    case "blocked_destination":
      return "That address isn't allowed. Use a public https URL, or a local port this server allows.";
    case "token_required":
      return "Enter the bearer token.";
    case "too_many_servers":
      return "You've reached the limit of MCP servers. Remove one first.";
    case "mcp_auth_required":
      return "The server asks you to sign in. Choose OAuth, or a bearer token.";
    case "mcp_unreachable":
      return "Couldn't reach the server. Check the URL and that it's running.";
    default:
      return err instanceof Error && err.message ? err.message : "Something went wrong. Try again.";
  }
}

/** The notice for the OAuth return query, or null when the URL has none. */
export function mcpReturnNotice(search: string): { ok: boolean; text: string } | null {
  const q = new URLSearchParams(search);
  if (q.get("mcp") === "connected") {
    const name = q.get("mcp_server");
    return { ok: true, text: name ? `Connected the MCP server ${name}.` : "Connected the MCP server." };
  }
  const error = q.get("mcp_error");
  if (error !== null) return { ok: false, text: `Couldn't connect the MCP server. ${error}`.trim() };
  return null;
}

interface Draft {
  name: string;
  url: string;
  auth: McpAuthMode;
  token: string;
}

const EMPTY: Draft = { name: "", url: "", auth: "none", token: "" };

export function McpDialog({ feature, reconnect, onClose, onChanged }: { feature: McpFeature; reconnect?: string | null; onClose(): void; onChanged?(servers: McpServer[]): void }) {
  const [servers, setServers] = useState<McpServer[] | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [busy, setBusy] = useState<string | null>(null);
  const [rowError, setRowError] = useState<{ name: string; text: string } | null>(null);
  const [draft, setDraft] = useState<Draft>(EMPTY);
  const [formError, setFormError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const nameRef = useRef<HTMLInputElement>(null);
  const tokenRef = useRef<HTMLInputElement>(null);
  const ids = { name: useId(), url: useId(), auth: useId(), token: useId() };
  const changed = useRef(onChanged);
  changed.current = onChanged;
  const modes = feature.auth_modes ?? ["none", "bearer", "oauth"];

  const load = useCallback(async () => {
    try {
      const list = await api.listMcpServers();
      setServers(list.servers);
      setLoadError(null);
      changed.current?.(list.servers);
      // Each server's live status, one call each.
      for (const s of list.servers) {
        api.mcpServerStatus(s.name).then(
          (st) => setServers((cur) => cur?.map((x) => (x.name === s.name ? { ...x, ...st } : x)) ?? cur),
          () => undefined,
        );
      }
    } catch (err) {
      setLoadError(mcpErrorText(err));
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  // Reconnect with a bearer token: focus the token field once it renders.
  const [focusToken, setFocusToken] = useState(false);
  useEffect(() => {
    if (!focusToken || !tokenRef.current) return;
    tokenRef.current.focus();
    setFocusToken(false);
  }, [focusToken, draft.auth]);

  // A control that went away or got disabled (an added or removed server, a busy button) would
  // drop focus to the page, outside the dialog: keep it in the form.
  useEffect(() => {
    const a = document.activeElement;
    if (!a || a === document.body || !a.isConnected || (a as HTMLButtonElement).disabled) nameRef.current?.focus();
  }, [servers, busy, saving]);

  async function oauth(name: string, url: string) {
    const r = await api.startMcpOAuth({ name, url });
    window.location.assign(r.authorization_url);
  }

  async function submit(e: FormEvent) {
    e.preventDefault();
    setFormError(null);
    const name = draft.name.trim();
    const url = draft.url.trim();
    if (!name || !url) {
      setFormError("Enter a name and a URL.");
      return;
    }
    setSaving(true);
    try {
      if (draft.auth === "oauth") {
        await oauth(name, url);
        return;
      }
      await api.addMcpServer({ name, url, auth_mode: draft.auth, ...(draft.auth === "bearer" ? { bearer_token: draft.token } : {}) });
      setDraft(EMPTY);
      await load();
    } catch (err) {
      setFormError(mcpErrorText(err));
    } finally {
      setSaving(false);
    }
  }

  async function remove(name: string) {
    setBusy(name);
    setRowError(null);
    try {
      await api.removeMcpServer(name);
      await load();
    } catch (err) {
      setRowError({ name, text: mcpErrorText(err) });
    } finally {
      setBusy(null);
    }
  }

  async function reconnectServer(s: McpServer) {
    setRowError(null);
    if (s.auth_mode === "bearer") {
      // A new token replaces the server (same name).
      setDraft({ name: s.name, url: s.url, auth: "bearer", token: "" });
      setFocusToken(true);
      return;
    }
    setBusy(s.name);
    try {
      if (s.auth_mode === "oauth") await oauth(s.name, s.url);
      else {
        await api.addMcpServer({ name: s.name, url: s.url, auth_mode: "none" });
        await load();
      }
    } catch (err) {
      setRowError({ name: s.name, text: mcpErrorText(err) });
    } finally {
      setBusy(null);
    }
  }

  // Opened from a "Reconnect" chip: start that server's Reconnect once the list is in.
  const started = useRef(false);
  useEffect(() => {
    if (!reconnect || started.current || !servers) return;
    const s = servers.find((x) => x.name === reconnect);
    if (!s) return;
    started.current = true;
    if (s.auth_mode === "bearer") void reconnectServer(s);
    else document.getElementById(`mcp-reconnect-${s.name}`)?.focus();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [reconnect, servers]);

  const full = feature.max_servers !== undefined && servers !== null && servers.length >= feature.max_servers && !servers.some((s) => s.name === draft.name.trim());

  return (
    <Dialog className="mcp-dialog" label="MCP servers" onClose={onClose}>
      <div className="drawer-head">
        <span>MCP servers</span>
        <span className="spacer" />
        <button type="button" className="btn ghost small" onClick={onClose}>
          Close
        </button>
      </div>
      <div className="mcp-body">
        <p className="hint">Tools from these servers are offered to new sessions. Each call asks for your approval.</p>
        {loadError && (
          <p className="err" role="alert">
            {loadError}
          </p>
        )}
        {servers === null && !loadError && <p className="hint">Loading…</p>}
        {servers && servers.length === 0 && <p className="hint">No MCP servers yet.</p>}
        {servers && servers.length > 0 && (
          <ul className="servers" aria-label="Your MCP servers">
            {servers.map((s) => (
              <li key={s.name} className="server">
                <div className="row">
                  <strong className="mono">{s.name}</strong>
                  <span className="hint">{AUTH_TEXT[s.auth_mode] ?? s.auth_mode}</span>
                  {s.status && <span className={`status ${s.status}`}>{STATUS_TEXT[s.status] ?? s.status}</span>}
                  {s.tools && <span className="hint">{s.tools.length === 1 ? "1 tool" : `${s.tools.length} tools`}</span>}
                  <span className="spacer" />
                  <button
                    type="button"
                    id={`mcp-reconnect-${s.name}`}
                    className="btn secondary small"
                    disabled={busy === s.name}
                    aria-label={`Reconnect ${s.name}`}
                    onClick={() => void reconnectServer(s)}
                  >
                    Reconnect
                  </button>
                  <button type="button" className="btn ghost small" disabled={busy === s.name} aria-label={`Remove ${s.name}`} onClick={() => void remove(s.name)}>
                    Remove
                  </button>
                </div>
                <span className="url">{s.url}</span>
                {s.message && s.status !== "connected" && <span className="hint">{s.message}</span>}
                {rowError?.name === s.name && (
                  <p className="err" role="alert">
                    {rowError.text}
                  </p>
                )}
              </li>
            ))}
          </ul>
        )}
        <form onSubmit={(e) => void submit(e)} aria-label="Add an MCP server">
          <h2>Add a server</h2>
          <label htmlFor={ids.name}>
            Name
            <input
              ref={nameRef}
              id={ids.name}
              value={draft.name}
              autoComplete="off"
              spellCheck={false}
              placeholder="local-echo"
              onChange={(e) => setDraft({ ...draft, name: e.target.value })}
            />
          </label>
          <label htmlFor={ids.url}>
            URL
            <input id={ids.url} value={draft.url} type="url" autoComplete="off" spellCheck={false} placeholder="https://" onChange={(e) => setDraft({ ...draft, url: e.target.value })} />
          </label>
          <label htmlFor={ids.auth}>
            Sign-in
            <select id={ids.auth} value={draft.auth} onChange={(e) => setDraft({ ...draft, auth: e.target.value as McpAuthMode })}>
              {modes.map((m) => (
                <option key={m} value={m}>
                  {AUTH_TEXT[m]}
                </option>
              ))}
            </select>
          </label>
          {draft.auth === "bearer" && (
            <label htmlFor={ids.token}>
              Bearer token
              <input ref={tokenRef} id={ids.token} type="password" autoComplete="off" value={draft.token} onChange={(e) => setDraft({ ...draft, token: e.target.value })} />
            </label>
          )}
          {formError && (
            <p className="err" role="alert">
              {formError}
            </p>
          )}
          <div className="acts">
            <button type="submit" className="btn primary small" disabled={saving || full}>
              {saving ? "Connecting…" : draft.auth === "oauth" ? "Sign in and add" : "Add server"}
            </button>
            {full && <span className="hint">You've reached the limit of {feature.max_servers} servers.</span>}
          </div>
        </form>
      </div>
    </Dialog>
  );
}
