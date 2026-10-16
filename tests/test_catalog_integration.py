from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from fastapi import HTTPException
from starlette.requests import Request

from simple_chat_agent.api.auth import SESSION_COOKIE, AuthenticatedUser, create_session_token
from simple_chat_agent.api.main import _current_user
from simple_chat_agent.api.auth import user_from_catalog_headers
from simple_chat_agent.worker.main import _worker_deployment_config
from simple_chat_agent.worker.user_chats_workflow import user_chats_workflow_id
from simple_chat_agent.common.external_storage import simple_chat_data_converter
from simple_chat_agent.api.temporal_ui import temporal_ui_url, temporal_ui_user_workflows_url
from urllib.parse import parse_qs, urlsplit


class CatalogIntegrationTests(unittest.TestCase):
    def test_catalog_workflow_links_use_the_standard_cloud_address(self) -> None:
        with patch.dict(os.environ, {"TEMPORAL_ADDRESS": "demo.a2dd6.tmprl.cloud:7233"}, clear=True):
            link = temporal_ui_url(namespace="demo.a2dd6", workflow_id="chat", run_id="run")
            self.assertEqual(urlsplit(link).netloc, "cloud.temporal.io")

    def test_user_workflow_filter_matches_the_full_keyword_email(self) -> None:
        link = temporal_ui_user_workflows_url("user@temporal.io", namespace="demo.a2dd6", search_attr_name="UserEmail")
        self.assertEqual(parse_qs(urlsplit(link).query)["query"], ['UserEmail = "user@temporal.io"'])

    def test_catalog_cannot_fall_back_to_pod_local_payload_storage(self) -> None:
        with patch.dict(os.environ, {"SIMPLE_CHAT_REQUIRE_SHARED_STORAGE": "true"}, clear=True):
            with self.assertRaisesRegex(RuntimeError, "SIMPLE_CHAT_S3_BUCKET"):
                simple_chat_data_converter()

    def test_catalog_identity_is_stable_and_isolated_by_task_queue(self) -> None:
        headers = {"x-temporal-auth-subject": "google-subject", "x-temporal-auth-email": " User@temporal.io "}
        user = user_from_catalog_headers(headers, task_queue="advanced-ai-agent")
        again = user_from_catalog_headers(headers, task_queue="advanced-ai-agent")
        workspace = user_from_catalog_headers(headers, task_queue="advanced-ai-agent-workspace-123")
        self.assertEqual(user, again)
        self.assertEqual(user.username, "user@temporal.io")
        self.assertNotEqual(user.user_id, workspace.user_id)
        self.assertNotEqual(user_chats_workflow_id(user.user_id), user_chats_workflow_id(workspace.user_id))

    def test_catalog_auth_does_not_accept_an_app_session_cookie(self) -> None:
        with patch.dict(os.environ, {"SIMPLE_CHAT_AUTH_MODE": "catalog"}):
            token = create_session_token(AuthenticatedUser("old-user", "old@example.com"))
            request = Request({"type": "http", "headers": [(b"cookie", f"{SESSION_COOKIE}={token}".encode())]})
            with self.assertRaises(HTTPException) as error:
                _current_user(request)
            self.assertEqual(error.exception.status_code, 401)

    def test_worker_uses_the_controller_deployment_identity(self) -> None:
        with patch.dict(os.environ, {
            "TEMPORAL_DEPLOYMENT_NAME": "advanced-ai-agent-workspace-123",
            "TEMPORAL_WORKER_BUILD_ID": "git-abc123",
            "SIMPLE_CHAT_WORKER_VERSION": "old-local-version",
        }, clear=True):
            config = _worker_deployment_config("workspace-queue")
            self.assertEqual(config.version.deployment_name, "advanced-ai-agent-workspace-123")
            self.assertEqual(config.version.build_id, "git-abc123")


if __name__ == "__main__":
    unittest.main()
