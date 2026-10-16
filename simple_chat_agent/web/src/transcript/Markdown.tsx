// The agent's reply as markdown, with smoothed typing while it streams (PRD U2). Raw HTML in
// the text is not rendered (react-markdown's default); images become links, so a reply can't
// make the browser fetch from another host.

import { memo, useEffect, useRef, useState } from "react";
import ReactMarkdown, { type Components } from "react-markdown";
import remarkGfm from "remark-gfm";

const components: Components = {
  a: ({ href, children }) => (
    <a href={href} target="_blank" rel="noopener noreferrer">
      {children}
    </a>
  ),
  img: ({ src, alt }) => (
    <a href={typeof src === "string" ? src : undefined} target="_blank" rel="noopener noreferrer">
      {alt || "image"}
    </a>
  ),
};

export const Markdown = memo(function Markdown({ text }: { text: string }) {
  return (
    <ReactMarkdown remarkPlugins={[remarkGfm]} components={components}>
      {text}
    </ReactMarkdown>
  );
});

function prefersReducedMotion(): boolean {
  return typeof window !== "undefined" && window.matchMedia?.("(prefers-reduced-motion: reduce)").matches === true;
}

/**
 * Reveal `target` a little at a time while `streaming`, so bursts from the stream read as
 * steady typing. It catches up faster the further behind it is (about 8 frames to close any
 * gap), and shows everything at once when not streaming or with reduced motion.
 */
export function useSmoothText(target: string, streaming: boolean): string {
  const [shown, setShown] = useState(streaming ? "" : target);
  const shownRef = useRef(shown);
  shownRef.current = shown;
  useEffect(() => {
    if (!streaming || prefersReducedMotion() || !target.startsWith(shownRef.current)) {
      setShown(target);
      return;
    }
    let frame = 0;
    const step = () => {
      const current = shownRef.current;
      if (current.length >= target.length) return;
      const gap = target.length - current.length;
      const next = target.slice(0, current.length + Math.max(1, Math.ceil(gap / 8)));
      shownRef.current = next;
      setShown(next);
      frame = requestAnimationFrame(step);
    };
    frame = requestAnimationFrame(step);
    return () => cancelAnimationFrame(frame);
  }, [target, streaming]);
  return streaming ? shown : target;
}

export function StreamingMarkdown({ text, streaming }: { text: string; streaming: boolean }) {
  const shown = useSmoothText(text, streaming);
  return (
    <div className="answer">
      <Markdown text={shown} />
      {streaming && <span className="caret" aria-hidden="true" />}
    </div>
  );
}
