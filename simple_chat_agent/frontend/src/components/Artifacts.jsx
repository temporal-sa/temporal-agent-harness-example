import { CodeBlock, MarkdownContent } from "./MarkdownContent.jsx";
import { artifactKindLabel, artifactLanguage, artifactPreviewKind } from "../utils/artifacts.js";
import { formatBytes } from "../utils/format.js";

export function ArtifactsPanel({ artifacts, onOpen }) {
  return (
    <aside className="artifacts-sidebar">
      <section className="artifact-panel">
        <div className="artifact-panel-header">
          <span>Artifacts</span>
          <span className="artifact-panel-count">
            {artifacts.length === 1 ? "1 file" : `${artifacts.length} files`}
          </span>
        </div>
        {artifacts.length === 0 ? (
          <div className="artifact-empty">Artifacts created by the agent will appear here.</div>
        ) : (
          <div className="artifact-list">
            {[...artifacts].reverse().map((artifact) => (
              <ArtifactCard key={artifact.artifact_id} artifact={artifact} onOpen={onOpen} />
            ))}
          </div>
        )}
      </section>
    </aside>
  );
}

function ArtifactCard({ artifact, onOpen }) {
  const kind = artifactPreviewKind(artifact);
  return (
    <article className="artifact-card">
      <div className="artifact-card-title">
        <div className="artifact-name">{artifact.name || artifact.artifact_id}</div>
        <span className={`artifact-kind ${kind}`}>{artifactKindLabel(kind)}</span>
      </div>
      <div className="artifact-meta">
        {artifact.mime_type || "application/octet-stream"} |{" "}
        {formatBytes(artifact.size_bytes || 0)}
      </div>
      <div className="artifact-actions">
        <button type="button" onClick={() => onOpen(artifact)}>
          View
        </button>
        <ArtifactLink url={artifact.view_url} label="Raw" />
        <ArtifactLink url={artifact.download_url} label="Download" download />
      </div>
    </article>
  );
}

function ArtifactLink({ url, label, download = false }) {
  return download ? (
    <a href={url} download="">
      {label}
    </a>
  ) : (
    <a href={url} target="_blank" rel="noreferrer">
      {label}
    </a>
  );
}

export function ArtifactViewer({ viewer, onClose }) {
  return (
    <section
      className="artifact-viewer-overlay"
      hidden={!viewer.open}
      onClick={(event) => {
        if (event.target === event.currentTarget) onClose();
      }}
    >
      {viewer.open && viewer.artifact ? (
        <div className="artifact-viewer">
          <div className="artifact-viewer-header">
            <div className="artifact-viewer-title">
              <div className="artifact-viewer-name">
                {viewer.artifact.name || viewer.artifact.artifact_id}
              </div>
              <div className="artifact-viewer-meta">
                {viewer.artifact.mime_type || "application/octet-stream"} |{" "}
                {formatBytes(viewer.artifact.size_bytes || 0)}
              </div>
            </div>
            <div className="artifact-viewer-actions">
              <ArtifactLink
                url={viewer.artifact.download_url}
                label="Download"
                download
              />
              <button type="button" onClick={onClose}>
                Close
              </button>
            </div>
          </div>
          <div className="artifact-viewer-body">
            <ArtifactViewerBody viewer={viewer} />
          </div>
        </div>
      ) : null}
    </section>
  );
}

function ArtifactViewerBody({ viewer }) {
  const artifact = viewer.artifact;
  const previewKind = viewer.previewKind || artifactPreviewKind(artifact);
  if (viewer.loading) return <div className="empty">Loading artifact...</div>;
  if (viewer.error) return <div className="artifact-viewer-error">{viewer.error}</div>;
  if (previewKind === "image") {
    return (
      <img
        className="artifact-viewer-image"
        src={artifact.view_url}
        alt={artifact.name || "Artifact"}
      />
    );
  }
  if (previewKind === "pdf") {
    return (
      <iframe
        className="artifact-viewer-frame"
        src={artifact.view_url}
        title={artifact.name || "Artifact PDF"}
      ></iframe>
    );
  }
  if (previewKind === "audio") {
    return <audio className="artifact-viewer-media" controls src={artifact.view_url}></audio>;
  }
  if (previewKind === "video") {
    return <video className="artifact-viewer-media" controls src={artifact.view_url}></video>;
  }
  if (previewKind === "markdown") {
    return <MarkdownContent className="artifact-markdown" content={viewer.text} />;
  }
  if (previewKind === "code" || previewKind === "text") {
    return (
      <div className="artifact-code-preview">
        <CodeBlock
          source={viewer.text}
          languageHint={artifactLanguage(artifact) || "text"}
          showLineNumbers
        />
      </div>
    );
  }
  return (
    <div className="bubble-content">
      <div className="artifact-unsupported">
        <div className="artifact-unsupported-title">No inline preview available.</div>
        <div>
          Download the file or open the raw artifact if your browser can handle this
          format.
        </div>
      </div>
    </div>
  );
}
