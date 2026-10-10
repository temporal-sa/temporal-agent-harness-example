from __future__ import annotations

import asyncio
import json
import os
import ssl
import time
from typing import Any
from urllib.error import HTTPError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

from temporalio import activity

from simple_chat_agent.common.external_storage import purge_workflow_payloads
from simple_chat_agent.worker.demo_workspace_workflow import (
    ProvisionDemoWorkspaceRequest,
)
from simple_chat_agent.worker.shutdown import (
    WORKER_GRACEFUL_SHUTDOWN_SECONDS_ENV,
    kubernetes_termination_grace_period_seconds,
    worker_graceful_shutdown_seconds,
)
from simple_chat_agent.worker.tools.research import (
    GOOGLE_RESEARCH_API_KEY_NAMES,
    GOOGLE_SEARCH_GROUNDING_MODEL,
    SEARXNG_BASE_URL,
)

WORKSPACE_LABEL = "agent-harness-demo-workspace"
WORKSPACE_SECRET = "agent-harness-workspace-secrets"
WORKSPACE_TLS_SECRET = "agent-harness-workspace-tls"
WORKSPACE_SERVICE_ACCOUNT = "agent-harness-workspace"
WORKSPACE_HTTPS_REDIRECT_MIDDLEWARE = "agent-harness-https-redirect"
WEB_SERVICE = "agent-harness-web"
API_SERVICE = "agent-harness-api"
REDIS_SERVICE = "agent-harness-redis"
REDIS_IMAGE = "redis:7.4-alpine"

SECRET_DENY_PREFIXES = (
    "AWS_",
    "GITHUB_",
    "PYTHON_SANDBOX_LAMBDA_",
)
SECRET_DENY_NAMES = {
    "SIMPLE_CHAT_DYNAMODB_TABLE",
    "SIMPLE_CHAT_ARTIFACTS_TABLE",
    "GITHUB_OAUTH_CLIENT_ID",
    "GITHUB_OAUTH_CLIENT_SECRET",
    "GITHUB_OAUTH_REDIRECT_URI",
    "GITHUB_OAUTH_SCOPES",
    "GOOGLE_OAUTH_CLIENT_ID",
    "GOOGLE_OAUTH_CLIENT_SECRET",
    "GOOGLE_OAUTH_REDIRECT_URI",
}


@activity.defn(name="provision_demo_workspace")
async def provision_demo_workspace(request: ProvisionDemoWorkspaceRequest) -> dict[str, Any]:
    source_images = await resolve_demo_workspace_images(request)
    await create_demo_workspace_namespace(request)
    await configure_demo_workspace(request)
    await deploy_demo_workspace_workloads(request, source_images)
    for deployment_name in [
        "agent-harness-redis",
        "agent-harness-web",
        "agent-harness-api",
        "agent-harness-worker",
    ]:
        await wait_demo_workspace_deployment(request.namespace, deployment_name)
    return {"namespace": request.namespace, "url": request.url}


@activity.defn(name="resolve_demo_workspace_images")
async def resolve_demo_workspace_images(
    request: ProvisionDemoWorkspaceRequest,
) -> dict[str, str]:
    client = KubernetesClient.in_cluster()
    return {
        "web": _deployment_image(client, request.source_namespace, request.source_web_deployment),
        "api": _deployment_image(client, request.source_namespace, request.source_api_deployment),
        "worker": _deployment_image(
            client,
            request.source_namespace,
            request.source_worker_deployment,
        ),
    }


@activity.defn(name="create_demo_workspace_namespace")
async def create_demo_workspace_namespace(
    request: ProvisionDemoWorkspaceRequest,
) -> dict[str, str]:
    client = KubernetesClient.in_cluster()
    client.upsert_namespace(_namespace(request.namespace))
    return {"namespace": request.namespace}


@activity.defn(name="configure_demo_workspace")
async def configure_demo_workspace(request: ProvisionDemoWorkspaceRequest) -> dict[str, str]:
    client = KubernetesClient.in_cluster()
    client.upsert_secret(
        request.namespace,
        _sanitized_secret(
            client.get_secret(request.source_namespace, request.source_secret_name),
            name=WORKSPACE_SECRET,
        ),
    )
    if request.tls_secret_name:
        client.upsert_secret(
            request.namespace,
            _copied_secret(
                client.get_secret(request.source_namespace, request.tls_secret_name),
                name=WORKSPACE_TLS_SECRET,
            ),
        )
    client.upsert_service_account(
        request.namespace,
        _service_account(request.service_account_role_arn),
    )
    return {"namespace": request.namespace}


@activity.defn(name="deploy_demo_workspace_workloads")
async def deploy_demo_workspace_workloads(
    request: ProvisionDemoWorkspaceRequest,
    source_images: dict[str, str],
) -> dict[str, str]:
    client = KubernetesClient.in_cluster()
    client.upsert_service(request.namespace, _service(WEB_SERVICE, "web", 80, 8080))
    client.upsert_service(request.namespace, _service(API_SERVICE, "api", 80, 8000))
    client.upsert_service(
        request.namespace,
        _service(REDIS_SERVICE, "redis", 6379, 6379, port_name="redis"),
    )

    common_env = _common_env(request)
    client.upsert_deployment(request.namespace, _redis_deployment(request.namespace))
    client.upsert_deployment(
        request.namespace,
        _web_deployment(source_images["web"], request.namespace),
    )
    client.upsert_deployment(
        request.namespace,
        _api_deployment(source_images["api"], request.namespace, common_env),
    )
    client.upsert_deployment(
        request.namespace,
        _worker_deployment(source_images["worker"], request.namespace, common_env),
    )
    client.upsert_middleware(request.namespace, _https_redirect_middleware())
    client.upsert_ingress_route(request.namespace, _ingress_route(request.host, secure=True))
    client.upsert_ingress_route(request.namespace, _ingress_route(request.host, secure=False))
    return {"namespace": request.namespace, "url": request.url}


@activity.defn(name="wait_demo_workspace_deployment")
async def wait_demo_workspace_deployment(
    namespace: str,
    deployment_name: str,
) -> dict[str, Any]:
    client = KubernetesClient.in_cluster()
    await _wait_for_deployment(
        client,
        namespace,
        deployment_name,
        timeout_seconds=180,
    )
    return {"namespace": namespace, "deployment": deployment_name, "ready": True}


@activity.defn(name="crash_demo_workspace")
async def crash_demo_workspace(namespace: str) -> dict[str, Any]:
    client = KubernetesClient.in_cluster()
    pods = client.list_pods(namespace)
    deleted = 0
    for pod in pods:
        name = pod.get("metadata", {}).get("name")
        if not name:
            continue
        client.delete_pod(namespace, name)
        deleted += 1
    return {"namespace": namespace, "deleted_pods": deleted}


@activity.defn(name="delete_demo_workspace")
async def delete_demo_workspace(namespace: str) -> dict[str, Any]:
    client = KubernetesClient.in_cluster()
    client.delete_namespace(namespace)
    return {"namespace": namespace, "deleted": True}


@activity.defn(name="purge_demo_workspace_payloads")
async def purge_demo_workspace_payloads(
    temporal_namespace: str,
    workflow_ids: list[str],
) -> dict[str, Any]:
    deleted = 0
    for workflow_id in workflow_ids:
        deleted += await asyncio.to_thread(
            purge_workflow_payloads,
            namespace=temporal_namespace,
            workflow_id=workflow_id,
        )
    return {"workflow_count": len(workflow_ids), "deleted_payloads": deleted}


class KubernetesClient:
    def __init__(self, *, base_url: str, token: str, cafile: str) -> None:
        self._base_url = base_url.rstrip("/")
        self._token = token
        self._context = ssl.create_default_context(cafile=cafile)

    @classmethod
    def in_cluster(cls) -> "KubernetesClient":
        token_path = "/var/run/secrets/kubernetes.io/serviceaccount/token"
        ca_path = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
        with open(token_path, "r", encoding="utf-8") as token_file:
            token = token_file.read().strip()
        return cls(
            base_url=os.environ.get(
                "KUBERNETES_SERVICE_URL",
                "https://kubernetes.default.svc",
            ),
            token=token,
            cafile=ca_path,
        )

    def request(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
        *,
        content_type: str = "application/json",
        not_found_ok: bool = False,
        conflict_ok: bool = False,
    ) -> dict[str, Any] | None:
        data = None if body is None else json.dumps(body).encode("utf-8")
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Accept": "application/json",
        }
        if data is not None:
            headers["Content-Type"] = content_type
        req = Request(
            f"{self._base_url}{path}",
            data=data,
            headers=headers,
            method=method,
        )
        try:
            with urlopen(req, context=self._context, timeout=30) as response:
                payload = response.read()
                if not payload:
                    return None
                return json.loads(payload.decode("utf-8"))
        except HTTPError as err:
            if err.code == 404 and not_found_ok:
                return None
            if err.code == 409 and conflict_ok:
                return None
            detail = err.read().decode("utf-8", errors="replace")
            raise RuntimeError(
                f"Kubernetes {method} {path} failed with {err.code}: {detail}"
            ) from err

    def upsert_namespace(self, body: dict[str, Any]) -> None:
        name = body["metadata"]["name"]
        self._upsert("/api/v1/namespaces", f"/api/v1/namespaces/{name}", body)

    def get_secret(self, namespace: str, name: str) -> dict[str, Any]:
        return self.request(
            "GET",
            f"/api/v1/namespaces/{quote(namespace)}/secrets/{quote(name)}",
        ) or {}

    def upsert_secret(self, namespace: str, body: dict[str, Any]) -> None:
        name = body["metadata"]["name"]
        base = f"/api/v1/namespaces/{quote(namespace)}/secrets"
        self._upsert(base, f"{base}/{quote(name)}", body)

    def upsert_service_account(self, namespace: str, body: dict[str, Any]) -> None:
        name = body["metadata"]["name"]
        base = f"/api/v1/namespaces/{quote(namespace)}/serviceaccounts"
        self._upsert(base, f"{base}/{quote(name)}", body)

    def upsert_service(self, namespace: str, body: dict[str, Any]) -> None:
        name = body["metadata"]["name"]
        base = f"/api/v1/namespaces/{quote(namespace)}/services"
        self._upsert(base, f"{base}/{quote(name)}", body)

    def upsert_deployment(self, namespace: str, body: dict[str, Any]) -> None:
        name = body["metadata"]["name"]
        base = f"/apis/apps/v1/namespaces/{quote(namespace)}/deployments"
        self._upsert(base, f"{base}/{quote(name)}", body)

    def upsert_ingress_route(self, namespace: str, body: dict[str, Any]) -> None:
        name = body["metadata"]["name"]
        base = (
            "/apis/traefik.containo.us/v1alpha1/"
            f"namespaces/{quote(namespace)}/ingressroutes"
        )
        self._upsert(base, f"{base}/{quote(name)}", body)

    def upsert_middleware(self, namespace: str, body: dict[str, Any]) -> None:
        name = body["metadata"]["name"]
        base = (
            "/apis/traefik.containo.us/v1alpha1/"
            f"namespaces/{quote(namespace)}/middlewares"
        )
        self._upsert(base, f"{base}/{quote(name)}", body)

    def get_deployment(self, namespace: str, name: str) -> dict[str, Any]:
        return self.request(
            "GET",
            f"/apis/apps/v1/namespaces/{quote(namespace)}/deployments/{quote(name)}",
        ) or {}

    def list_pods(self, namespace: str) -> list[dict[str, Any]]:
        params = urlencode({"labelSelector": f"app={WORKSPACE_LABEL}"})
        response = self.request(
            "GET",
            f"/api/v1/namespaces/{quote(namespace)}/pods?{params}",
            not_found_ok=True,
        )
        return list((response or {}).get("items", []))

    def delete_pod(self, namespace: str, name: str) -> None:
        self.request(
            "DELETE",
            f"/api/v1/namespaces/{quote(namespace)}/pods/{quote(name)}",
            body={"gracePeriodSeconds": 0},
            not_found_ok=True,
        )

    def delete_namespace(self, namespace: str) -> None:
        self.request(
            "DELETE",
            f"/api/v1/namespaces/{quote(namespace)}",
            not_found_ok=True,
        )

    def _upsert(self, collection_path: str, item_path: str, body: dict[str, Any]) -> None:
        created = self.request("POST", collection_path, body, conflict_ok=True)
        if created is not None:
            return
        patch = _patch_body(body)
        self.request(
            "PATCH",
            item_path,
            patch,
            content_type="application/merge-patch+json",
        )


def _deployment_image(client: KubernetesClient, namespace: str, name: str) -> str:
    deployment = client.get_deployment(namespace, name)
    containers = deployment.get("spec", {}).get("template", {}).get("spec", {}).get(
        "containers",
        [],
    )
    if not containers or not containers[0].get("image"):
        raise RuntimeError(f"Could not find image for deployment {namespace}/{name}")
    return str(containers[0]["image"])


async def _wait_for_deployment(
    client: KubernetesClient,
    namespace: str,
    name: str,
    *,
    timeout_seconds: int,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        deployment = client.get_deployment(namespace, name)
        spec_replicas = int(deployment.get("spec", {}).get("replicas") or 1)
        status = deployment.get("status", {})
        available = int(status.get("availableReplicas") or 0)
        generation = deployment.get("metadata", {}).get("generation")
        observed = status.get("observedGeneration")
        activity.heartbeat(
            {
                "deployment": name,
                "available": available,
                "replicas": spec_replicas,
            }
        )
        if available >= spec_replicas and (
            generation is None or observed is None or observed >= generation
        ):
            return
        await asyncio.sleep(5)
    raise TimeoutError(f"Timed out waiting for deployment {namespace}/{name}")


def _namespace(name: str) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "name": name,
            "labels": {
                "app": WORKSPACE_LABEL,
                "managed-by": "simple-chat-agent",
            },
        },
    }


def _sanitized_secret(secret: dict[str, Any], *, name: str) -> dict[str, Any]:
    data = secret.get("data", {}) or {}
    sanitized = {
        key: value
        for key, value in data.items()
        if key not in SECRET_DENY_NAMES
        and not any(key.startswith(prefix) for prefix in SECRET_DENY_PREFIXES)
    }
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": name, "labels": {"app": WORKSPACE_LABEL}},
        "type": secret.get("type") or "Opaque",
        "data": sanitized,
    }


def _copied_secret(secret: dict[str, Any], *, name: str) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": name, "labels": {"app": WORKSPACE_LABEL}},
        "type": secret.get("type") or "Opaque",
        "data": secret.get("data", {}) or {},
    }


def _service_account(role_arn: str = "") -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "name": WORKSPACE_SERVICE_ACCOUNT,
        "labels": {"app": WORKSPACE_LABEL},
    }
    if role_arn:
        metadata["annotations"] = {"eks.amazonaws.com/role-arn": role_arn}
    return {
        "apiVersion": "v1",
        "kind": "ServiceAccount",
        "metadata": metadata,
        "automountServiceAccountToken": False,
    }


def _common_env(request: ProvisionDemoWorkspaceRequest) -> list[dict[str, Any]]:
    values = {
        "SIMPLE_CHAT_DEMO_WORKSPACE": "1",
        "SIMPLE_CHAT_DEMO_WORKSPACES_ENABLED": "0",
        "SIMPLE_CHAT_GITHUB_TOOLS_ENABLED": "0",
        "SIMPLE_CHAT_TASK_QUEUE": request.task_queue,
        "SIMPLE_CHAT_WORKER_VERSIONING_ENABLED": "0",
        WORKER_GRACEFUL_SHUTDOWN_SECONDS_ENV: str(worker_graceful_shutdown_seconds()),
        "SIMPLE_CHAT_WORKFLOW_PREFIX": request.workflow_prefix,
        "SIMPLE_CHAT_PUBLIC_URL": request.url,
        "SIMPLE_CHAT_DEMO_PARENT_WORKFLOW_ID": request.control_workflow_id,
        "SIMPLE_CHAT_DEMO_PARENT_PUBLIC_URL": request.parent_public_url,
        # Temporary crash-demo namespaces do not provision a credential-free
        # workspace executor or its NetworkPolicy/PVC yet.
        "SIMPLE_CHAT_WORKSPACE_SHELL_ENABLED": "0",
    }
    if os.environ.get("SIMPLE_CHAT_S3_BUCKET"):
        values["SIMPLE_CHAT_S3_BUCKET"] = os.environ["SIMPLE_CHAT_S3_BUCKET"]
    if os.environ.get("SIMPLE_CHAT_ARTIFACTS_TABLE"):
        values["SIMPLE_CHAT_ARTIFACTS_TABLE"] = os.environ[
            "SIMPLE_CHAT_ARTIFACTS_TABLE"
        ]
    if os.environ.get("SIMPLE_CHAT_EXTERNAL_STORAGE_THRESHOLD_BYTES"):
        values["SIMPLE_CHAT_EXTERNAL_STORAGE_THRESHOLD_BYTES"] = os.environ[
            "SIMPLE_CHAT_EXTERNAL_STORAGE_THRESHOLD_BYTES"
        ]
    if os.environ.get(SEARXNG_BASE_URL):
        values[SEARXNG_BASE_URL] = os.environ[SEARXNG_BASE_URL]
    for name in GOOGLE_RESEARCH_API_KEY_NAMES:
        if os.environ.get(name):
            values[name] = os.environ[name]
    if os.environ.get(GOOGLE_SEARCH_GROUNDING_MODEL):
        values[GOOGLE_SEARCH_GROUNDING_MODEL] = os.environ[
            GOOGLE_SEARCH_GROUNDING_MODEL
        ]
    if request.search_attr_name:
        values["SIMPLE_CHAT_USER_EMAIL_SEARCH_ATTR"] = request.search_attr_name
    if os.environ.get("SIMPLE_CHAT_GOOD_PLACE") is not None:
        values["SIMPLE_CHAT_GOOD_PLACE"] = os.environ["SIMPLE_CHAT_GOOD_PLACE"]
    return [
        {
            "name": "SIMPLE_CHAT_STREAM_TOKEN",
            "valueFrom": {
                "secretKeyRef": {
                    "name": WORKSPACE_SECRET,
                    "key": "SIMPLE_CHAT_STREAM_TOKEN",
                }
            },
        },
        {
            "name": "SIMPLE_CHAT_REDIS_URL",
            "value": (
                f"redis://:$(SIMPLE_CHAT_STREAM_TOKEN)@{REDIS_SERVICE}:6379/0"
            ),
        },
        *[{"name": key, "value": value} for key, value in values.items()],
    ]


def _web_deployment(image: str, namespace: str) -> dict[str, Any]:
    return _deployment(
        name="agent-harness-web",
        namespace=namespace,
        component="web",
        image=image,
        command=[
            "uvicorn",
            "simple_chat_agent.frontend.server:app",
            "--host",
            "0.0.0.0",
            "--port",
            "8080",
            "--proxy-headers",
            "--forwarded-allow-ips",
            "*",
        ],
        ports=[{"name": "http", "containerPort": 8080}],
        env=[],
        env_from=[],
        resources={
            "requests": {"cpu": "50m", "memory": "128Mi"},
            "limits": {"cpu": "250m", "memory": "256Mi"},
        },
        readiness_port=8080,
    )


def _api_deployment(
    image: str,
    namespace: str,
    common_env: list[dict[str, Any]],
) -> dict[str, Any]:
    return _deployment(
        name="agent-harness-api",
        namespace=namespace,
        component="api",
        image=image,
        command=[
            "uvicorn",
            "simple_chat_agent.api.main:app",
            "--host",
            "0.0.0.0",
            "--port",
            "8000",
            "--proxy-headers",
            "--forwarded-allow-ips",
            "*",
        ],
        ports=[{"name": "http", "containerPort": 8000}],
        env=common_env,
        env_from=[{"secretRef": {"name": WORKSPACE_SECRET}}],
        resources={
            "requests": {"cpu": "250m", "memory": "512Mi"},
            "limits": {"cpu": "1", "memory": "1Gi"},
        },
        readiness_port=8000,
        volume_mounts=[{"name": "chat-data", "mountPath": "/app/.simple_chat_agent"}],
        volumes=[{"name": "chat-data", "emptyDir": {}}],
        automount_service_account_token=True,
    )


def _worker_deployment(
    image: str,
    namespace: str,
    common_env: list[dict[str, Any]],
) -> dict[str, Any]:
    return _deployment(
        name="agent-harness-worker",
        namespace=namespace,
        component="worker",
        image=image,
        command=["python", "-m", "simple_chat_agent.worker.main"],
        ports=[{"name": "codec", "containerPort": 8001}],
        env=common_env,
        env_from=[{"secretRef": {"name": WORKSPACE_SECRET}}],
        resources={
            "requests": {"cpu": "250m", "memory": "512Mi"},
            "limits": {"cpu": "1", "memory": "1Gi"},
        },
        readiness_port=8001,
        volume_mounts=[{"name": "chat-data", "mountPath": "/app/.simple_chat_agent"}],
        volumes=[{"name": "chat-data", "emptyDir": {}}],
        automount_service_account_token=True,
        termination_grace_period_seconds=(
            kubernetes_termination_grace_period_seconds()
        ),
    )


def _redis_deployment(namespace: str) -> dict[str, Any]:
    labels = {
        "app": WORKSPACE_LABEL,
        "workspace": namespace,
        "component": "redis",
    }
    secret_ref = {
        "secretKeyRef": {
            "name": WORKSPACE_SECRET,
            "key": "SIMPLE_CHAT_STREAM_TOKEN",
        }
    }
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": REDIS_SERVICE, "labels": labels},
        "spec": {
            "replicas": 1,
            "strategy": {"type": "Recreate"},
            "selector": {"matchLabels": labels},
            "template": {
                "metadata": {"labels": labels},
                "spec": {
                    "automountServiceAccountToken": False,
                    "securityContext": {"fsGroup": 999},
                    "containers": [
                        {
                            "name": "redis",
                            "image": REDIS_IMAGE,
                            "imagePullPolicy": "IfNotPresent",
                            "command": ["/bin/sh", "-ec"],
                            "args": [
                                "umask 077\n"
                                "printf 'requirepass %s\\n' "
                                '"$SIMPLE_CHAT_STREAM_TOKEN" '
                                "> /run/redis/redis.conf\n"
                                "exec redis-server /run/redis/redis.conf "
                                "--appendonly yes --appendfsync everysec --dir /data"
                            ],
                            "env": [
                                {
                                    "name": "SIMPLE_CHAT_STREAM_TOKEN",
                                    "valueFrom": secret_ref,
                                },
                                {"name": "REDISCLI_AUTH", "valueFrom": secret_ref},
                            ],
                            "ports": [{"name": "redis", "containerPort": 6379}],
                            "volumeMounts": [
                                {"name": "data", "mountPath": "/data"},
                                {"name": "config", "mountPath": "/run/redis"},
                            ],
                            "readinessProbe": {
                                "exec": {"command": ["redis-cli", "ping"]},
                                "initialDelaySeconds": 3,
                                "periodSeconds": 5,
                            },
                            "livenessProbe": {
                                "exec": {"command": ["redis-cli", "ping"]},
                                "initialDelaySeconds": 15,
                                "periodSeconds": 10,
                            },
                            "resources": {
                                "requests": {"cpu": "50m", "memory": "64Mi"},
                                "limits": {"cpu": "500m", "memory": "256Mi"},
                            },
                            "securityContext": {
                                "allowPrivilegeEscalation": False,
                                "capabilities": {"drop": ["ALL"]},
                                "runAsNonRoot": True,
                                "runAsUser": 999,
                            },
                        }
                    ],
                    "volumes": [
                        {"name": "data", "emptyDir": {}},
                        {"name": "config", "emptyDir": {"medium": "Memory"}},
                    ],
                },
            },
        },
    }


def _deployment(
    *,
    name: str,
    namespace: str,
    component: str,
    image: str,
    command: list[str],
    ports: list[dict[str, Any]],
    env: list[dict[str, Any]],
    env_from: list[dict[str, Any]],
    resources: dict[str, Any],
    readiness_port: int,
    volume_mounts: list[dict[str, Any]] | None = None,
    volumes: list[dict[str, Any]] | None = None,
    automount_service_account_token: bool = False,
    termination_grace_period_seconds: int | None = None,
) -> dict[str, Any]:
    labels = {
        "app": WORKSPACE_LABEL,
        "workspace": namespace,
        "component": component,
    }
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": name, "labels": labels},
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": labels},
            "template": {
                "metadata": {"labels": labels},
                "spec": {
                    "serviceAccountName": WORKSPACE_SERVICE_ACCOUNT,
                    "automountServiceAccountToken": automount_service_account_token,
                    **(
                        {
                            "terminationGracePeriodSeconds": (
                                termination_grace_period_seconds
                            )
                        }
                        if termination_grace_period_seconds is not None
                        else {}
                    ),
                    "containers": [
                        {
                            "name": component,
                            "image": image,
                            "imagePullPolicy": "IfNotPresent",
                            "command": command,
                            "ports": ports,
                            "envFrom": env_from,
                            "env": env,
                            "readinessProbe": {
                                "tcpSocket": {"port": readiness_port},
                                "initialDelaySeconds": 5,
                                "periodSeconds": 10,
                            },
                            "resources": resources,
                            **({"volumeMounts": volume_mounts} if volume_mounts else {}),
                        }
                    ],
                    **({"volumes": volumes} if volumes else {}),
                },
            },
        },
    }


def _service(
    name: str,
    component: str,
    port: int,
    target_port: int,
    *,
    port_name: str = "http",
) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": name, "labels": {"app": WORKSPACE_LABEL}},
        "spec": {
            "type": "ClusterIP",
            "selector": {"app": WORKSPACE_LABEL, "component": component},
            "ports": [
                {"name": port_name, "port": port, "targetPort": target_port}
            ],
        },
    }


def _ingress_route(host: str, *, secure: bool) -> dict[str, Any]:
    suffix = "" if secure else "-http"
    entrypoint = "websecure" if secure else "web"
    routes = [
        _route(host, "/api", API_SERVICE),
        _route(host, "/oauth", API_SERVICE),
        _route(host, "/internal", API_SERVICE),
        {
            "kind": "Rule",
            "match": f"Host(`{host}`)",
            "services": [
                {
                    "name": WEB_SERVICE,
                    "passHostHeader": True,
                    "port": 80,
                }
            ],
        },
    ]
    if not secure:
        for route in routes:
            route["middlewares"] = [{"name": WORKSPACE_HTTPS_REDIRECT_MIDDLEWARE}]
    route = {
        "apiVersion": "traefik.containo.us/v1alpha1",
        "kind": "IngressRoute",
        "metadata": {
            "name": f"agent-harness-workspace{suffix}",
            "labels": {"app": WORKSPACE_LABEL},
        },
        "spec": {
            "entryPoints": [entrypoint],
            "routes": routes,
        },
    }
    if secure:
        route["spec"]["tls"] = {"secretName": WORKSPACE_TLS_SECRET}
    return route


def _https_redirect_middleware() -> dict[str, Any]:
    return {
        "apiVersion": "traefik.containo.us/v1alpha1",
        "kind": "Middleware",
        "metadata": {
            "name": WORKSPACE_HTTPS_REDIRECT_MIDDLEWARE,
            "labels": {"app": WORKSPACE_LABEL},
        },
        "spec": {
            "redirectScheme": {
                "scheme": "https",
                "permanent": True,
            }
        },
    }


def _route(host: str, prefix: str, service: str) -> dict[str, Any]:
    return {
        "kind": "Rule",
        "match": f"Host(`{host}`) && PathPrefix(`{prefix}`)",
        "services": [{"name": service, "passHostHeader": True, "port": 80}],
    }


def _patch_body(body: dict[str, Any]) -> dict[str, Any]:
    patched = json.loads(json.dumps(body))
    metadata = patched.setdefault("metadata", {})
    metadata.pop("resourceVersion", None)
    metadata.pop("uid", None)
    metadata.pop("creationTimestamp", None)
    metadata.pop("managedFields", None)
    return patched
