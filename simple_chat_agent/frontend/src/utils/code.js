export function languageFromFileName(name) {
  const extension = String(name || "").split(".").pop()?.toLowerCase();
  const languages = {
    bash: "bash",
    c: "c",
    cc: "cpp",
    cpp: "cpp",
    cs: "csharp",
    css: "css",
    csv: "csv",
    diff: "diff",
    dockerfile: "docker",
    env: "dotenv",
    go: "go",
    graphql: "graphql",
    h: "c",
    hpp: "cpp",
    html: "html",
    ini: "ini",
    java: "java",
    js: "javascript",
    json: "json",
    jsonl: "json",
    jsx: "jsx",
    kt: "kotlin",
    kts: "kotlin",
    log: "text",
    md: "markdown",
    markdown: "markdown",
    mjs: "javascript",
    php: "php",
    plist: "xml",
    py: "python",
    rb: "ruby",
    rs: "rust",
    sh: "bash",
    sql: "sql",
    svg: "xml",
    swift: "swift",
    toml: "toml",
    ts: "typescript",
    tsx: "tsx",
    txt: "text",
    xml: "xml",
    yaml: "yaml",
    yml: "yaml",
  };
  const lowered = String(name || "").toLowerCase();
  if (lowered.endsWith("dockerfile") || lowered.includes("dockerfile.")) return "docker";
  return languages[extension] || null;
}

export function languageFromMimeType(mimeType) {
  const normalized = String(mimeType || "").toLowerCase().split(";")[0].trim();
  const languages = {
    "application/graphql": "graphql",
    "application/javascript": "javascript",
    "application/json": "json",
    "application/ld+json": "json",
    "application/sql": "sql",
    "application/toml": "toml",
    "application/typescript": "typescript",
    "application/x-sh": "bash",
    "application/x-yaml": "yaml",
    "application/xml": "xml",
    "application/yaml": "yaml",
    "image/svg+xml": "xml",
    "text/css": "css",
    "text/csv": "csv",
    "text/html": "html",
    "text/javascript": "javascript",
    "text/markdown": "markdown",
    "text/plain": null,
    "text/tab-separated-values": "tsv",
    "text/xml": "xml",
    "text/x-markdown": "markdown",
    "text/yaml": "yaml",
  };
  return languages[normalized] || null;
}

export function normalizeCodeLanguage(language) {
  if (!language) return null;
  const normalized = language.toLowerCase();
  const aliases = {
    bash: "bash",
    cjs: "javascript",
    csharp: "csharp",
    csv: "csv",
    docker: "docker",
    dockerfile: "docker",
    dotenv: "dotenv",
    env: "dotenv",
    graphql: "graphql",
    ini: "ini",
    css: "css",
    diff: "diff",
    go: "go",
    html: "html",
    java: "java",
    javascript: "javascript",
    js: "javascript",
    json: "json",
    jsonc: "json",
    jsx: "jsx",
    kotlin: "kotlin",
    markdown: "markdown",
    md: "markdown",
    mjs: "javascript",
    php: "php",
    py: "python",
    python: "python",
    rb: "ruby",
    ruby: "ruby",
    rs: "rust",
    rust: "rust",
    sh: "bash",
    shell: "bash",
    sql: "sql",
    swift: "swift",
    text: "text",
    toml: "toml",
    ts: "typescript",
    tsx: "tsx",
    tsv: "tsv",
    typescript: "typescript",
    xml: "xml",
    yaml: "yaml",
    yml: "yaml",
    zsh: "bash",
  };
  return aliases[normalized] || null;
}

export function syntaxHighlighterLanguage(language) {
  const normalized = normalizeCodeLanguage(language) || "text";
  const aliases = {
    csharp: "csharp",
    csv: "text",
    docker: "docker",
    dotenv: "bash",
    html: "markup",
    text: "text",
    tsv: "text",
    xml: "markup",
  };
  return aliases[normalized] || normalized;
}

export function inferCodeLanguage(source) {
  source = String(source || "");
  const trimmed = source.trim();
  if (!trimmed) return null;
  if ((trimmed.startsWith("{") || trimmed.startsWith("[")) && looksLikeJson(trimmed)) {
    return "json";
  }
  if (/^\s*(from\s+\w+\s+import|import\s+\w+|def\s+\w+|class\s+\w+|async\s+def\s+\w+)\b/m.test(source)) {
    return "python";
  }
  if (/\b(print|range|len)\s*\(/.test(source) && /(^|\n)\s*#/.test(source)) {
    return "python";
  }
  if (/\b(const|let|function|console\.log|=>|import\s+.+\s+from)\b/.test(source)) {
    return "javascript";
  }
  if (/^#!.*\b(?:bash|sh|zsh)\b/m.test(source) || /\b(?:echo|curl|export|chmod|sudo)\b/.test(source)) {
    return "bash";
  }
  if (/\bselect\b[\s\S]+\bfrom\b/i.test(source)) return "sql";
  if (/^\s*</.test(source) && /<\/?[A-Za-z][\s\S]*>/.test(source)) return "html";
  if (/^[\s\S]*\{[\s\S]*:[\s\S]*\}/.test(source) && /[.#]?[A-Za-z][\w-]*\s*\{/.test(source)) {
    return "css";
  }
  if (/^[A-Za-z_][\w.-]*\s*:/m.test(source)) return "yaml";
  return null;
}

function looksLikeJson(source) {
  try {
    JSON.parse(source);
    return true;
  } catch (_error) {
    return false;
  }
}
