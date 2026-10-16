import { useEffect, useState } from "react";

const RETRY_SECONDS = 5;

/** Full-page state when the API doesn't answer (PRD U1). Retries on its own every 5 s. */
export function Unreachable({ onRetry }: { onRetry(): void }) {
  const [left, setLeft] = useState(RETRY_SECONDS);
  useEffect(() => {
    const timer = setInterval(() => {
      setLeft((n) => {
        if (n <= 1) {
          onRetry();
          return RETRY_SECONDS;
        }
        return n - 1;
      });
    }, 1000);
    return () => clearInterval(timer);
  }, [onRetry]);
  return (
    <main className="page-state">
      <div className="card" role="alert">
        <h1>Can't reach the app</h1>
        <p>The app's API isn't answering. Workflows keep running in Temporal. Retrying every {RETRY_SECONDS} seconds.</p>
        <button type="button" className="btn secondary small" onClick={onRetry}>
          Retry now
        </button>
        <span className="hint" aria-live="off">
          Next try in {left}s
        </span>
      </div>
    </main>
  );
}
