import { memo, useEffect, useRef, useState } from "react";
import ReactMarkdown from "react-markdown";
import SyntaxHighlighter from "react-syntax-highlighter/dist/esm/prism-async-light.js";
import oneDark from "react-syntax-highlighter/dist/esm/styles/prism/one-dark.js";
import remarkGfm from "remark-gfm";
import { inferCodeLanguage, normalizeCodeLanguage, syntaxHighlighterLanguage } from "../utils/code.js";

export const MarkdownContent = memo(function MarkdownContent({ content, className = "" }) {
  return (
    <div className={`bubble-content${className ? ` ${className}` : ""}`}>
      <ReactMarkdown remarkPlugins={[remarkGfm]} components={markdownComponents}>
        {String(content || "")}
      </ReactMarkdown>
    </div>
  );
});

const markdownComponents = {
  a({ href, children }) {
    return (
      <a href={href} target="_blank" rel="noreferrer">
        {children}
      </a>
    );
  },
  h1({ children }) {
    return <div className="md-heading">{children}</div>;
  },
  h2({ children }) {
    return <div className="md-heading">{children}</div>;
  },
  h3({ children }) {
    return <div className="md-heading">{children}</div>;
  },
  h4({ children }) {
    return <div className="md-heading">{children}</div>;
  },
  table({ children }) {
    return (
      <div className="markdown-table-wrap">
        <table>{children}</table>
      </div>
    );
  },
  pre({ children }) {
    return <>{children}</>;
  },
  code({ className, children, node, ...props }) {
    const source = String(children || "").replace(/\n$/, "");
    const language = codeLanguageFromClassName(className);
    const spansMultipleLines =
      node?.position?.start?.line && node?.position?.end?.line
        ? node.position.start.line !== node.position.end.line
        : false;
    if (language || source.includes("\n") || spansMultipleLines) {
      return <CodeBlock source={source} languageHint={language} />;
    }
    return (
      <code className={className || undefined} {...props}>
        {children}
      </code>
    );
  },
};

function codeLanguageFromClassName(className = "") {
  const match = String(className).match(/language-([A-Za-z0-9_+.#-]+)/);
  return normalizeCodeLanguage(match?.[1] || "");
}

export function CodeBlock({
  source,
  languageHint = null,
  showLineNumbers = false,
  compact = false,
}) {
  const [copied, setCopied] = useState(false);
  const [highlightVisible, setHighlightVisible] = useState(false);
  const blockRef = useRef(null);
  const code = String(source ?? "");
  const language = normalizeCodeLanguage(languageHint) || inferCodeLanguage(code);
  const displayLanguage = language || "text";
  const highlighterLanguage = syntaxHighlighterLanguage(displayLanguage);
  const lineCount = code ? code.split("\n").length : 1;

  useEffect(() => {
    setHighlightVisible(false);
    if (highlighterLanguage === "text") return undefined;
    const node = blockRef.current;
    if (!node) return undefined;
    if (!("IntersectionObserver" in window)) {
      setHighlightVisible(true);
      return undefined;
    }

    const observer = new IntersectionObserver(
      ([entry]) => {
        if (!entry.isIntersecting) return;
        setHighlightVisible(true);
        observer.disconnect();
      },
      { rootMargin: "640px 0px" },
    );
    observer.observe(node);
    return () => observer.disconnect();
  }, [code, highlighterLanguage]);

  async function copyCode() {
    try {
      await navigator.clipboard.writeText(code);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1200);
    } catch (_error) {
      setCopied(false);
    }
  }

  return (
    <div className={`code-block${compact ? " compact" : ""}`} ref={blockRef}>
      <div className="code-block-header">
        <span>{displayLanguage}</span>
        <button type="button" onClick={copyCode}>
          {copied ? "Copied" : "Copy"}
        </button>
      </div>
      {highlighterLanguage === "text" || !highlightVisible ? (
        <pre className="plain-code">
          <code>{code}</code>
        </pre>
      ) : (
        <SyntaxHighlighter
          language={highlighterLanguage}
          style={oneDark}
          showLineNumbers={showLineNumbers || lineCount >= 12}
          wrapLongLines={false}
          customStyle={{
            margin: 0,
            padding: "12px",
            background: "transparent",
            fontSize: "0.88rem",
            lineHeight: 1.5,
          }}
          codeTagProps={{
            style: {
              fontFamily:
                'ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace',
            },
          }}
        >
          {code}
        </SyntaxHighlighter>
      )}
    </div>
  );
}
