import { languageFromFileName, languageFromMimeType } from "./code.js";

export function artifactPreviewKind(artifact) {
  if (isImageArtifact(artifact)) return "image";
  if (isPdfArtifact(artifact)) return "pdf";
  if (isAudioArtifact(artifact)) return "audio";
  if (isVideoArtifact(artifact)) return "video";
  if (isMarkdownArtifact(artifact)) return "markdown";
  if (artifactLanguage(artifact)) return "code";
  if (isTextArtifact(artifact)) return "text";
  return "binary";
}

export function artifactNeedsTextFetch(previewKind) {
  return ["markdown", "code", "text"].includes(previewKind);
}

export function artifactKindLabel(kind) {
  const labels = {
    audio: "audio",
    binary: "file",
    code: "code",
    image: "image",
    markdown: "md",
    pdf: "pdf",
    text: "text",
    video: "video",
  };
  return labels[kind] || "file";
}

function isImageArtifact(artifact) {
  const mimeType = String(artifact?.mime_type || "").toLowerCase();
  return mimeType.startsWith("image/") && mimeType !== "image/svg+xml";
}

function isPdfArtifact(artifact) {
  return String(artifact?.mime_type || "").toLowerCase() === "application/pdf";
}

function isAudioArtifact(artifact) {
  return String(artifact?.mime_type || "").toLowerCase().startsWith("audio/");
}

function isVideoArtifact(artifact) {
  return String(artifact?.mime_type || "").toLowerCase().startsWith("video/");
}

function isMarkdownArtifact(artifact) {
  const mimeType = String(artifact?.mime_type || "").toLowerCase();
  const name = String(artifact?.name || artifact?.artifact_id || "").toLowerCase();
  return (
    mimeType === "text/markdown" ||
    mimeType === "text/x-markdown" ||
    name.endsWith(".md") ||
    name.endsWith(".markdown")
  );
}

function isTextArtifact(artifact) {
  const mimeType = String(artifact?.mime_type || "").toLowerCase();
  const name = String(artifact?.name || artifact?.artifact_id || "").toLowerCase();
  if (mimeType.startsWith("text/")) return true;
  if (
    [
      "application/json",
      "application/ld+json",
      "application/javascript",
      "application/typescript",
      "application/xml",
      "application/yaml",
      "application/x-yaml",
      "application/toml",
      "application/sql",
      "application/graphql",
      "image/svg+xml",
    ].includes(mimeType)
  ) {
    return true;
  }
  return /\.(csv|env|graphql|ini|log|sql|svg|toml|txt)$/i.test(name);
}

export function artifactLanguage(artifact) {
  return (
    languageFromMimeType(artifact?.mime_type) ||
    languageFromFileName(artifact?.name || artifact?.artifact_id)
  );
}
