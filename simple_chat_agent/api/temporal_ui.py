from __future__ import annotations

import os
from urllib.parse import quote


def temporal_ui_base_url() -> str:
    # Explicit override always wins.
    explicit = os.environ.get("TEMPORAL_UI_URL")
    if explicit:
        return explicit
    # Otherwise derive from the environment: a Temporal Cloud endpoint maps to
    # the Cloud Web UI; anything else falls back to the local dev server.
    endpoint = os.environ.get("TEMPORAL_ADDRESS", os.environ.get("TEMPORAL_ENDPOINT", ""))
    if "tmprl.cloud" in endpoint:
        return "https://cloud.temporal.io"
    return "http://localhost:8233"


def temporal_ui_user_workflows_url(
    email: str,
    *,
    namespace: str,
    search_attr_name: str,
) -> str | None:
    """Temporal UI workflow-list URL filtered by the user search attribute.

    Returns None when the search attribute is not configured.
    """
    if not search_attr_name or not email:
        return None
    base_url = temporal_ui_base_url().rstrip("/")
    namespace_path = quote(namespace, safe="")
    # UserEmail is a Keyword attribute, matching the Workflow's typed value.
    query = quote(f'{search_attr_name} = "{email}"', safe="")
    return f"{base_url}/namespaces/{namespace_path}/workflows?query={query}"


def temporal_ui_url(*, namespace: str, workflow_id: str, run_id: str) -> str:
    base_url = temporal_ui_base_url().rstrip("/")
    namespace_path = quote(namespace, safe="")
    workflow_path = quote(workflow_id, safe="")
    run_path = quote(run_id, safe="")
    if run_path:
        return (
            f"{base_url}/namespaces/{namespace_path}/workflows/"
            f"{workflow_path}/{run_path}/timeline"
        )
    return f"{base_url}/namespaces/{namespace_path}/workflows/{workflow_path}"
