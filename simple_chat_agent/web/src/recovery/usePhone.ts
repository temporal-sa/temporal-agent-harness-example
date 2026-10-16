// Phone width (the shell's sheet breakpoint), for the compact approval card and Worker banner.

import { useEffect, useState } from "react";

export const PHONE_QUERY = "(max-width: 600px)";

export function usePhone(): boolean {
  const get = () => typeof window !== "undefined" && typeof window.matchMedia === "function" && window.matchMedia(PHONE_QUERY).matches;
  const [phone, setPhone] = useState(get);
  useEffect(() => {
    if (typeof window.matchMedia !== "function") return;
    const m = window.matchMedia(PHONE_QUERY);
    const on = () => setPhone(m.matches);
    on();
    m.addEventListener?.("change", on);
    return () => m.removeEventListener?.("change", on);
  }, []);
  return phone;
}
