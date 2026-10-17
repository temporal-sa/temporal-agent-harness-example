from __future__ import annotations

import unittest
from dataclasses import replace
from datetime import UTC, datetime
from unittest.mock import Mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from simple_chat_agent.api.artifacts import artifact_response, create_artifacts_router
from simple_chat_agent.api.auth import AuthenticatedUser
from simple_chat_agent.common.attachments import USER_ATTACHMENT_SOURCE
from simple_chat_agent.common.store import ArtifactRecord


class ArtifactResponseTests(unittest.TestCase):
    def setUp(self) -> None:
        self.store = Mock()
        self.store.read_artifact_bytes.return_value = (
            b"<!doctype html><style>body{color:navy}</style><h1>Report</h1>"
        )
        self.artifact = ArtifactRecord(
            artifact_id="artifact-1",
            user_id="user-1",
            conversation_id="conversation-1",
            workflow_id="workflow-1",
            name="report.html",
            mime_type="text/html",
            size_bytes=64,
            path="/tmp/report.html",
            metadata={},
            created_at=datetime.now(UTC).isoformat(),
        )
        self.store.get_artifact.return_value = self.artifact
        app = FastAPI()
        app.include_router(
            create_artifacts_router(
                store=lambda: self.store,
                current_user=lambda request: AuthenticatedUser(
                    user_id="user-1",
                    username="researcher@example.com",
                ),
            )
        )
        self.client = TestClient(app)

    def test_html_preview_is_renderable_but_strictly_sandboxed(self) -> None:
        response = artifact_response(
            self.store,
            self.artifact,
            disposition="html-preview",
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["content-type"], "text/html; charset=utf-8")
        self.assertEqual(response.headers["x-content-type-options"], "nosniff")
        self.assertEqual(response.headers["referrer-policy"], "no-referrer")
        self.assertEqual(response.headers["cache-control"], "private, no-store")
        self.assertIn("inline;", response.headers["content-disposition"])
        policy = response.headers["content-security-policy"]
        self.assertIn("sandbox", policy)
        self.assertIn("default-src 'none'", policy)
        self.assertIn("script-src 'none'", policy)
        self.assertIn("connect-src 'none'", policy)
        self.assertIn("style-src 'unsafe-inline'", policy)
        self.assertEqual(response.body, self.store.read_artifact_bytes.return_value)

    def test_html_preview_route_returns_the_sandboxed_document(self) -> None:
        response = self.client.get("/api/artifacts/artifact-1/preview")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["content-type"], "text/html; charset=utf-8")
        self.assertIn("sandbox", response.headers["content-security-policy"])
        self.store.get_artifact.assert_called_with(
            user_id="user-1",
            artifact_id="artifact-1",
        )

    def test_html_preview_route_rejects_non_html_artifacts(self) -> None:
        self.store.get_artifact.return_value = replace(
            self.artifact,
            name="report.json",
            mime_type="application/json",
        )

        response = self.client.get("/api/artifacts/artifact-1/preview")

        self.assertEqual(response.status_code, 415)
        self.assertEqual(response.json()["detail"], "Artifact is not an HTML document")

    def test_normal_inline_route_still_serves_html_as_plain_text(self) -> None:
        response = artifact_response(
            self.store,
            self.artifact,
            disposition="inline",
        )

        self.assertEqual(
            response.headers["content-type"],
            "text/plain; charset=utf-8",
        )
        self.assertNotIn("content-security-policy", response.headers)

    def test_download_and_user_uploads(self) -> None:
        response = self.client.get("/api/artifacts/artifact-1/download")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.headers["content-disposition"].startswith("attachment;"))
        # A user's own upload (an attachment) is never served as an artifact.
        self.store.get_artifact.return_value = replace(self.artifact, metadata={"source": USER_ATTACHMENT_SOURCE})
        self.assertEqual(self.client.get("/api/artifacts/artifact-1").status_code, 404)
        self.store.get_artifact.return_value = None
        self.assertEqual(self.client.get("/api/artifacts/artifact-1/download").status_code, 404)


if __name__ == "__main__":
    unittest.main()
