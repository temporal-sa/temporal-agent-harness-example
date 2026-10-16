import { useEffect, useState } from "react";

// Presenter mode: a larger root font size, remembered per browser. Storage can be missing or
// throw (private windows, blocked site data); the toggle still works for the page's life.
const KEY = "coding-agent:presenter";

function read(): boolean {
  try {
    return window.localStorage.getItem(KEY) === "on";
  } catch {
    return false;
  }
}

export function usePresenterMode(): [boolean, () => void] {
  const [on, setOn] = useState(read);
  useEffect(() => {
    document.documentElement.dataset.presenter = on ? "on" : "off";
    try {
      window.localStorage.setItem(KEY, on ? "on" : "off");
    } catch {
      // Not remembered; fine.
    }
  }, [on]);
  return [on, () => setOn((v) => !v)];
}

/** True when the window is at most `px` wide (CSS pixels). */
export function useMaxWidth(px: number): boolean {
  const query = `(max-width: ${px}px)`;
  const [match, setMatch] = useState(() => typeof window !== "undefined" && window.matchMedia(query).matches);
  useEffect(() => {
    const mq = window.matchMedia(query);
    const on = () => setMatch(mq.matches);
    on();
    mq.addEventListener("change", on);
    return () => mq.removeEventListener("change", on);
  }, [query]);
  return match;
}
