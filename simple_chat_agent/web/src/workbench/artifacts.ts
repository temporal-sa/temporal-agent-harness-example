// Artifacts the agent created, read from create_artifact results. Pure.
// The result text is "Created artifact <name> (artifact_id=<id>, <mime>, <n> bytes). View: …".

import type { LocatedTool } from "./tree";

export interface ArtifactItem {
  toolId: string;
  id: string;
  name: string;
  mimeType: string;
  size: number | null;
  /** Same-origin paths built from the id, never taken from the text. */
  viewUrl: string;
  previewUrl: string | null;
  downloadUrl: string;
  kind: "html" | "image" | "other";
}

const RESULT = /^Created artifact (.+?) \(artifact_id=([A-Za-z0-9_-]{1,128}), ([\w.+-]+\/[\w.+-]+), (\d+) bytes\)/;

export function parseArtifact(toolId: string, output: string | null): ArtifactItem | null {
  const m = RESULT.exec(output ?? "");
  if (!m) return null;
  const [, name = "", id = "", mime = "", size = ""] = m;
  const lower = mime.toLowerCase();
  const html = lower === "text/html" || lower === "application/xhtml+xml" || /\.x?html?$/i.test(name);
  const base = `/api/artifacts/${encodeURIComponent(id)}`;
  return {
    toolId,
    id,
    name,
    mimeType: mime,
    size: size ? Number(size) : null,
    viewUrl: base,
    previewUrl: html ? `${base}/preview` : null,
    downloadUrl: `${base}/download`,
    kind: html ? "html" : lower.startsWith("image/") ? "image" : "other",
  };
}

export function artifactsOf(tools: readonly LocatedTool[]): ArtifactItem[] {
  const out: ArtifactItem[] = [];
  for (const t of tools) {
    if (t.part.toolName !== "create_artifact" || t.part.state !== "done") continue;
    const a = parseArtifact(t.part.toolId, t.part.output);
    if (a) out.push(a);
  }
  return out;
}

/** The iframe sandbox for an HTML preview: scripts may run, but with an opaque origin (no
 *  allow-same-origin), so the page can't read the app's cookies or call its API. */
export const PREVIEW_SANDBOX = "allow-scripts";
