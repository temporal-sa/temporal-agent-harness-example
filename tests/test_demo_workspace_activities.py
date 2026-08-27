from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from simple_chat_agent.worker.demo_workspace_activities import (
    _common_env,
    _redis_deployment,
    _service,
)
from simple_chat_agent.worker.demo_workspace_workflow import (
    ProvisionDemoWorkspaceRequest,
)


class DemoWorkspaceRedisTests(unittest.TestCase):
    def test_workspace_env_wires_redis_without_copying_the_token_value(self) -> None:
        request = ProvisionDemoWorkspaceRequest(
            namespace="workspace-1",
            temporal_namespace="namespace",
            host="workspace.example.com",
            url="https://workspace.example.com",
            task_queue="workspace-queue",
            workflow_prefix="workspace-",
            control_workflow_id="controller",
            parent_public_url="https://parent.example.com",
            source_namespace="source",
            source_secret_name="source-secret",
            tls_secret_name="source-tls",
            source_web_deployment="web",
            source_api_deployment="api",
            source_worker_deployment="worker",
            user_email="user@example.com",
        )
        with patch.dict(os.environ, {}, clear=True):
            env = _common_env(request)

        token = env[0]
        redis_url = env[1]
        self.assertEqual(token["name"], "SIMPLE_CHAT_STREAM_TOKEN")
        self.assertEqual(
            token["valueFrom"]["secretKeyRef"],
            {
                "name": "agent-harness-workspace-secrets",
                "key": "SIMPLE_CHAT_STREAM_TOKEN",
            },
        )
        self.assertEqual(redis_url["name"], "SIMPLE_CHAT_REDIS_URL")
        self.assertEqual(
            redis_url["value"],
            "redis://:$(SIMPLE_CHAT_STREAM_TOKEN)@agent-harness-redis:6379/0",
        )

    def test_workspace_env_wires_durable_artifacts_without_oauth_table(self) -> None:
        request = ProvisionDemoWorkspaceRequest(
            namespace="workspace-1",
            temporal_namespace="namespace",
            host="workspace.example.com",
            url="https://workspace.example.com",
            task_queue="workspace-queue",
            workflow_prefix="workspace-",
            control_workflow_id="controller",
            parent_public_url="https://parent.example.com",
            source_namespace="source",
            source_secret_name="source-secret",
            tls_secret_name="source-tls",
            source_web_deployment="web",
            source_api_deployment="api",
            source_worker_deployment="worker",
            user_email="user@example.com",
        )
        with patch.dict(
            os.environ,
            {
                "SIMPLE_CHAT_S3_BUCKET": "artifact-bucket",
                "SIMPLE_CHAT_ARTIFACTS_TABLE": "artifact-table",
                "SIMPLE_CHAT_DYNAMODB_TABLE": "oauth-table",
            },
            clear=True,
        ):
            env = _common_env(request)

        values = {
            item["name"]: item["value"]
            for item in env
            if "value" in item
        }
        self.assertEqual(values["SIMPLE_CHAT_S3_BUCKET"], "artifact-bucket")
        self.assertEqual(values["SIMPLE_CHAT_ARTIFACTS_TABLE"], "artifact-table")
        self.assertNotIn("SIMPLE_CHAT_DYNAMODB_TABLE", values)

    def test_workspace_redis_is_password_protected_and_ephemeral(self) -> None:
        deployment = _redis_deployment("workspace-1")
        pod_spec = deployment["spec"]["template"]["spec"]
        container = pod_spec["containers"][0]

        self.assertEqual(deployment["metadata"]["name"], "agent-harness-redis")
        self.assertIn("requirepass", container["args"][0])
        self.assertEqual(container["securityContext"]["runAsUser"], 999)
        self.assertEqual(pod_spec["volumes"][0], {"name": "data", "emptyDir": {}})
        self.assertEqual(
            container["env"][0]["valueFrom"]["secretKeyRef"]["key"],
            "SIMPLE_CHAT_STREAM_TOKEN",
        )

    def test_workspace_redis_service_uses_the_redis_port_name(self) -> None:
        service = _service(
            "agent-harness-redis",
            "redis",
            6379,
            6379,
            port_name="redis",
        )
        self.assertEqual(
            service["spec"]["ports"],
            [{"name": "redis", "port": 6379, "targetPort": 6379}],
        )
