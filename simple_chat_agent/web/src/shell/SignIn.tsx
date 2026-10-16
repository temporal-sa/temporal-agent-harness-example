import { useState, type FormEvent } from "react";
import { ApiError, api, type AuthConfig } from "../session";

/** Shown when the API says we're not signed in. Local login posts to the API, which sets the
 *  session cookie; catalog sign-in happens at the catalog's ingress, so we can only explain. */
export function SignIn({ auth, onSignedIn }: { auth: AuthConfig | null; onSignedIn(): void }) {
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const local = auth?.mode === "local" && auth.local_enabled;

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const data = new FormData(event.currentTarget);
    setBusy(true);
    setError("");
    try {
      await api.localLogin(String(data.get("username") ?? ""), String(data.get("password") ?? ""));
      onSignedIn();
    } catch (err) {
      setError(err instanceof ApiError && err.status === 401 ? "That username and password didn't match." : "Couldn't sign in. Try again.");
    } finally {
      setBusy(false);
    }
  }

  return (
    <main className="page-state">
      <div className="card">
        <h1>Sign in to continue</h1>
        {local ? (
          <form onSubmit={submit}>
            <label>
              Username
              <input name="username" autoComplete="username" required />
            </label>
            <label>
              Password
              <input name="password" type="password" autoComplete="current-password" required />
            </label>
            <button type="submit" className="btn primary" disabled={busy}>
              {busy ? "Signing in…" : "Sign in"}
            </button>
            <p className="form-error" role="alert">
              {error}
            </p>
          </form>
        ) : (
          <>
            <p>Your sign-in didn't reach the app. Reload the page; if it keeps happening, sign in again through the catalog.</p>
            <button type="button" className="btn secondary small" onClick={() => window.location.reload()}>
              Reload
            </button>
          </>
        )}
      </div>
    </main>
  );
}
