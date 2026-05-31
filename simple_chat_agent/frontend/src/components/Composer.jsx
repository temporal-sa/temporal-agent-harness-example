import { useEffect, useState } from "react";

export function Composer({
  temporalUiUrl,
  onMessageChange,
  onSend,
  onInterrupt,
  resetToken,
}) {
  const [message, setMessage] = useState("");

  useEffect(() => {
    setMessage("");
  }, [resetToken]);

  function updateMessage(value) {
    setMessage(value);
    onMessageChange(value);
  }

  return (
    <form
      className="composer"
      onSubmit={(event) => {
        event.preventDefault();
        onSend();
      }}
    >
      <textarea
        value={message}
        onChange={(event) => updateMessage(event.currentTarget.value)}
        onKeyDown={(event) => {
          if (event.key === "Enter" && !event.shiftKey) {
            event.preventDefault();
            onSend();
          }
        }}
        placeholder="Type to chat. While responding, Send becomes steering."
      ></textarea>
      <button className="primary" type="submit">
        Send
      </button>
      {temporalUiUrl ? (
        <a
          className="temporal-link"
          href={temporalUiUrl}
          target="_blank"
          rel="noreferrer"
        >
          Workflow
        </a>
      ) : null}
      <button type="button" onClick={onInterrupt}>
        Interrupt
      </button>
    </form>
  );
}
