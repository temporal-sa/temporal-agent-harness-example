// A failure in the transcript: plain copy that says what happened and what comes next, with the
// raw text from the server or the agent behind a "Details" disclosure (UX review M9).

export function ErrorNote({ text, detail, alert = false }: { text: string; detail?: string | null; alert?: boolean }) {
  return (
    <div className="error-note" role={alert ? "alert" : undefined}>
      <span>{text}</span>
      {detail && detail !== text && (
        <details>
          <summary>Details</summary>
          <pre>{detail}</pre>
        </details>
      )}
    </div>
  );
}
