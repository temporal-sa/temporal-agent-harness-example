import assert from "node:assert/strict";
import test from "node:test";

import {
  artifactKindLabel,
  artifactNeedsTextFetch,
  artifactPreviewKind,
} from "./artifacts.js";

test("HTML MIME types and file names select the rendered preview", () => {
  assert.equal(
    artifactPreviewKind({ name: "report.bin", mime_type: "text/html" }),
    "html",
  );
  assert.equal(
    artifactPreviewKind({ name: "report.xhtml", mime_type: "text/plain" }),
    "html",
  );
  assert.equal(artifactKindLabel("html"), "html");
  assert.equal(artifactNeedsTextFetch("html"), false);
});

test("non-HTML source artifacts keep their existing preview kinds", () => {
  assert.equal(
    artifactPreviewKind({ name: "report.md", mime_type: "text/markdown" }),
    "markdown",
  );
  assert.equal(
    artifactPreviewKind({ name: "data.json", mime_type: "application/json" }),
    "code",
  );
});
