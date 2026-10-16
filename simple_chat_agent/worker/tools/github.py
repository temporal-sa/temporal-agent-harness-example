from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Callable
from datetime import timedelta
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlencode, urlparse
from urllib.request import Request, urlopen

from temporalio import activity as temporal_activity
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

from agent_harness.streaming import StreamContext
from agent_harness.tools import ToolContext, ToolResult, tool
from agent_harness.tool_types import ToolType
from simple_chat_agent.common.store import app_store

GITHUB_MUTATION_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=2),
    maximum_interval=timedelta(seconds=15),
    maximum_attempts=3,
)
GITHUB_IDEMPOTENCY_MARKER_PREFIX = "agentharnessidempotency"


class GitHubProvider:
    def __init__(
        self,
        connection_id: Callable[[], str | None],
    ) -> None:
        self._connection_id = connection_id

    @tool(
        name="github_authenticated_user",
        description="Return the GitHub user currently authorized for this chat.",
        tool_type=ToolType.READ,
    )
    async def authenticated_user(self, ctx: ToolContext) -> ToolResult:
        connection_id = self._require_connection_id()
        payload = await ctx.activity(
            _github_get_authenticated_user_activity,
            args={"connection_id": connection_id},
        )
        return ToolResult(payload=payload, error="error" in payload)

    @tool(
        name="github_list_repositories",
        description="List repositories visible to the authorized GitHub user.",
        tool_type=ToolType.READ,
    )
    async def list_repositories(
        self,
        ctx: ToolContext,
        visibility: str = "all",
        affiliation: str = "owner,collaborator,organization_member",
        max_results: int = 20,
        page: int = 1,
    ) -> ToolResult:
        connection_id = self._require_connection_id()
        payload = await ctx.activity(
            _github_list_repositories_activity,
            args={
                "connection_id": connection_id,
                "visibility": visibility,
                "affiliation": affiliation,
                "max_results": max_results,
                "page": page,
            },
        )
        return ToolResult(payload=payload, error="error" in payload)

    @tool(
        name="github_list_issues",
        description=(
            "List issues for a GitHub repository visible to the authorized "
            "GitHub user."
        ),
        tool_type=ToolType.READ,
    )
    async def list_issues(
        self,
        ctx: ToolContext,
        owner: str,
        repo: str,
        state: str = "open",
        max_results: int = 20,
        page: int = 1,
    ) -> ToolResult:
        connection_id = self._require_connection_id()
        payload = await ctx.activity(
            _github_list_issues_activity,
            args={
                "connection_id": connection_id,
                "owner": owner,
                "repo": repo,
                "state": state,
                "max_results": max_results,
                "page": page,
            },
        )
        return ToolResult(payload=payload, error="error" in payload)

    @tool(
        name="github_open_issue",
        description=(
            "Open a new issue in a GitHub repository visible to the authorized "
            "GitHub user. This mutates GitHub state by creating an issue."
        ),
        tool_type=ToolType.MUTATING,
        pre_guards=["mutating_tool_approval"],
    )
    async def open_issue(
        self,
        ctx: ToolContext,
        owner: str,
        repo: str,
        title: str,
        body: str = "",
        labels: list[str] | None = None,
    ) -> ToolResult:
        connection_id = self._require_connection_id()
        issue_labels = labels or []
        idempotency_key = ctx.idempotency_key(
            owner.lower(),
            repo.lower(),
            title.strip(),
            body or "",
            sorted(issue_labels),
        )
        payload = await ctx.activity(
            _github_open_issue_activity,
            args={
                "connection_id": connection_id,
                "owner": owner,
                "repo": repo,
                "title": title,
                "body": body,
                "labels": issue_labels,
                "idempotency_key": idempotency_key,
            },
            start_to_close_timeout=timedelta(seconds=45),
            schedule_to_close_timeout=timedelta(minutes=3),
            retry_policy=GITHUB_MUTATION_RETRY_POLICY,
        )
        return ToolResult(payload=payload, error="error" in payload)

    def _require_connection_id(self) -> str:
        connection_id = self._connection_id()
        if connection_id is None:
            raise ValueError("GitHub is not connected for this chat.")
        return connection_id


async def _github_get_authenticated_user_activity(
    connection_id: str,
    *,
    stream: StreamContext,
) -> dict[str, Any]:
    await stream.emit({}, kind="github_user_start")
    payload = await asyncio.to_thread(_github_api_get, connection_id, "/user")
    if "error" in payload:
        return payload

    user = {
        "login": payload.get("login"),
        "id": payload.get("id"),
        "name": payload.get("name"),
        "company": payload.get("company"),
        "blog": payload.get("blog"),
        "location": payload.get("location"),
        "public_repos": payload.get("public_repos"),
    }
    await stream.emit({"login": user["login"]}, kind="github_user_complete")
    return {"user": user}


async def _github_list_repositories_activity(
    connection_id: str,
    visibility: str,
    affiliation: str,
    max_results: int,
    page: int,
    *,
    stream: StreamContext,
) -> dict[str, Any]:
    max_results = _bounded_max_results(max_results)
    page = _bounded_page(page)
    await stream.emit(
        {"visibility": visibility, "max_results": max_results, "page": page},
        kind="github_repositories_start",
    )
    response = await asyncio.to_thread(
        _github_api_get_with_metadata,
        connection_id,
        "/user/repos",
        {
            "visibility": visibility,
            "affiliation": affiliation,
            "sort": "updated",
            "per_page": str(max_results),
            "page": str(page),
        },
    )
    if "error" in response:
        return response

    payload = response["data"]

    repositories = [
        {
            "full_name": repo.get("full_name"),
            "description": repo.get("description"),
            "private": repo.get("private"),
            "fork": repo.get("fork"),
            "html_url": repo.get("html_url"),
            "language": repo.get("language"),
            "open_issues_count": repo.get("open_issues_count"),
            "updated_at": repo.get("updated_at"),
        }
        for repo in payload
        if isinstance(repo, dict)
    ]
    await stream.emit(
        {"count": len(repositories), "page": page},
        kind="github_repositories_complete",
    )
    return {
        "repositories": repositories,
        "pagination": response["pagination"],
        "rate_limit": response["rate_limit"],
    }


async def _github_list_issues_activity(
    connection_id: str,
    owner: str,
    repo: str,
    state: str,
    max_results: int,
    page: int,
    *,
    stream: StreamContext,
) -> dict[str, Any]:
    max_results = _bounded_max_results(max_results)
    page = _bounded_page(page)
    await stream.emit(
        {
            "repo": f"{owner}/{repo}",
            "state": state,
            "max_results": max_results,
            "page": page,
        },
        kind="github_issues_start",
    )
    response = await asyncio.to_thread(
        _github_api_get_with_metadata,
        connection_id,
        f"/repos/{owner}/{repo}/issues",
        {
            "state": state,
            "per_page": str(max_results),
            "page": str(page),
        },
    )
    if "error" in response:
        return response

    payload = response["data"]

    issues = [
        {
            "number": issue.get("number"),
            "title": issue.get("title"),
            "state": issue.get("state"),
            "html_url": issue.get("html_url"),
            "user": (issue.get("user") or {}).get("login"),
            "created_at": issue.get("created_at"),
            "updated_at": issue.get("updated_at"),
            "pull_request": "pull_request" in issue,
        }
        for issue in payload
        if isinstance(issue, dict)
    ]
    await stream.emit(
        {"count": len(issues), "page": page},
        kind="github_issues_complete",
    )
    return {
        "issues": issues,
        "pagination": response["pagination"],
        "rate_limit": response["rate_limit"],
    }


async def _find_issue_by_idempotency_marker(
    *,
    connection_id: str,
    owner: str,
    repo: str,
    marker: str,
    stream: StreamContext,
) -> dict[str, Any]:
    attempt = _activity_attempt()
    search_attempts = 2 if attempt > 1 else 1
    for search_attempt in range(1, search_attempts + 1):
        await stream.emit(
            {
                "repo": f"{owner}/{repo}",
                "attempt": attempt,
                "search_attempt": search_attempt,
            },
            kind="github_open_issue_idempotency_search_start",
        )
        result = await asyncio.to_thread(
            _github_search_issue_by_marker,
            connection_id,
            owner,
            repo,
            marker,
        )
        if "error" in result:
            return result
        if result.get("issue") is not None:
            await stream.emit(
                {
                    "repo": f"{owner}/{repo}",
                    "found": True,
                    "number": result["issue"].get("number"),
                },
                kind="github_open_issue_idempotency_search_complete",
            )
            return result
        if search_attempt < search_attempts:
            await asyncio.sleep(2)

    await stream.emit(
        {"repo": f"{owner}/{repo}", "found": False},
        kind="github_open_issue_idempotency_search_complete",
    )
    return {"issue": None}


def _github_search_issue_by_marker(
    connection_id: str,
    owner: str,
    repo: str,
    marker: str,
) -> dict[str, Any]:
    query = f"repo:{owner}/{repo} is:issue in:body {marker}"
    response = _github_api_get_with_metadata(
        connection_id,
        "/search/issues",
        {
            "q": query,
            "per_page": "10",
        },
    )
    if "error" in response:
        return response

    payload = response.get("data")
    if not isinstance(payload, dict):
        return {"error": "GitHub issue search returned an unexpected response."}
    if payload.get("incomplete_results"):
        return {
            "error": (
                "GitHub issue search returned incomplete results; refusing to "
                "create a potentially duplicate issue."
            )
        }

    items = payload.get("items")
    if not isinstance(items, list):
        return {"issue": None}
    for item in items:
        if not isinstance(item, dict):
            continue
        if "pull_request" in item:
            continue
        body = item.get("body")
        if isinstance(body, str) and marker not in body:
            continue
        return {
            "issue": _issue_summary(item),
            "pagination": response["pagination"],
            "rate_limit": response["rate_limit"],
        }
    return {
        "issue": None,
        "pagination": response["pagination"],
        "rate_limit": response["rate_limit"],
    }


async def _github_open_issue_activity(
    connection_id: str,
    owner: str,
    repo: str,
    title: str,
    body: str,
    labels: list[str],
    idempotency_key: str,
    *,
    stream: StreamContext,
) -> dict[str, Any]:
    marker = _github_issue_idempotency_marker(idempotency_key)
    await stream.emit(
        {
            "repo": f"{owner}/{repo}",
            "title": title,
            "labels": labels,
            "attempt": _activity_attempt(),
        },
        kind="github_open_issue_start",
    )

    existing = await _find_issue_by_idempotency_marker(
        connection_id=connection_id,
        owner=owner,
        repo=repo,
        marker=marker,
        stream=stream,
    )
    if "error" in existing:
        return existing
    if existing.get("issue") is not None:
        issue = existing["issue"]
        await stream.emit(
            {
                "repo": f"{owner}/{repo}",
                "number": issue.get("number"),
                "url": issue.get("html_url"),
                "reused_existing": True,
            },
            kind="github_open_issue_complete",
        )
        return {
            "issue": issue,
            "idempotency": {
                "reused_existing": True,
                "method": "github_issue_body_marker",
            },
        }

    payload = await asyncio.to_thread(
        _github_api_request,
        connection_id,
        f"/repos/{owner}/{repo}/issues",
        method="POST",
        payload=_issue_payload(
            title=title,
            body=_issue_body_with_idempotency_marker(body, marker),
            labels=labels,
        ),
    )
    if "error" in payload:
        return payload

    issue = _issue_summary(payload)
    await stream.emit(
        {
            "repo": f"{owner}/{repo}",
            "number": issue["number"],
            "url": issue["html_url"],
            "reused_existing": False,
        },
        kind="github_open_issue_complete",
    )
    return {
        "issue": issue,
        "idempotency": {
            "reused_existing": False,
            "method": "github_issue_body_marker",
        },
    }


def _issue_summary(issue: dict[str, Any]) -> dict[str, Any]:
    return {
        "number": issue.get("number"),
        "title": issue.get("title"),
        "state": issue.get("state"),
        "html_url": issue.get("html_url"),
        "user": (issue.get("user") or {}).get("login"),
        "created_at": issue.get("created_at"),
        "updated_at": issue.get("updated_at"),
    }


def _github_api_get(
    connection_id: str,
    path: str,
    query: dict[str, str] | None = None,
) -> Any:
    return _github_api_request(connection_id, path, query=query)


def _github_api_get_with_metadata(
    connection_id: str,
    path: str,
    query: dict[str, str] | None = None,
) -> dict[str, Any]:
    return _github_api_request(
        connection_id,
        path,
        query=query,
        include_metadata=True,
    )


def _github_api_request(
    connection_id: str,
    path: str,
    query: dict[str, str] | None = None,
    *,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
    include_metadata: bool = False,
) -> Any:
    connection = app_store().get_oauth_connection_by_id(connection_id)
    if connection is None:
        raise ApplicationError(
            "GitHub connection was not found.",
            type="GitHubConnectionNotFound",
            non_retryable=True,
        )

    url = f"https://api.github.com{path}"
    if query:
        url = f"{url}?{urlencode(query)}"
    data = None
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {connection.access_token}",
        "User-Agent": "temporal-agent-harness-example/0.1",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = Request(
        url,
        data=data,
        headers=headers,
        method=method,
    )
    try:
        with urlopen(request, timeout=15) as response:
            raw = response.read().decode("utf-8")
            headers = response.headers
    except HTTPError as err:
        details = _read_http_error(err)
        retryable = (
            err.code in {408, 409, 425, 429}
            or err.code >= 500
            or (err.code == 403 and "rate limit" in details.lower())
        )
        raise ApplicationError(
            f"GitHub API HTTP {err.code}",
            details,
            type="GitHubApiError",
            non_retryable=not retryable,
        ) from err
    except URLError as err:
        raise ApplicationError(
            f"GitHub API error: {err.reason}",
            type="GitHubApiError",
        ) from err

    if not raw:
        data: Any = {}
    else:
        data = json.loads(raw)

    if not include_metadata:
        return data

    return {
        "data": data,
        "pagination": _pagination_from_headers(headers, query or {}),
        "rate_limit": _rate_limit_from_headers(headers),
    }


def _issue_payload(
    *,
    title: str,
    body: str,
    labels: list[str],
) -> dict[str, Any]:
    issue: dict[str, Any] = {"title": title}
    if body:
        issue["body"] = body
    if labels:
        issue["labels"] = labels
    return issue


def _github_issue_idempotency_marker(idempotency_key: str) -> str:
    digest = hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()
    return f"{GITHUB_IDEMPOTENCY_MARKER_PREFIX}{digest}"


def _issue_body_with_idempotency_marker(body: str, marker: str) -> str:
    marker_comment = f"<!-- {marker} -->"
    cleaned = body.rstrip()
    if marker in cleaned:
        return cleaned
    if not cleaned:
        return marker_comment
    return f"{cleaned}\n\n{marker_comment}"


def _activity_attempt() -> int:
    try:
        return int(temporal_activity.info().attempt)
    except RuntimeError:
        return 1


def _read_http_error(err: HTTPError) -> str:
    try:
        return err.read().decode("utf-8", errors="replace")
    except Exception:
        return err.reason


def _bounded_max_results(max_results: int) -> int:
    return max(1, min(max_results, 100))


def _bounded_page(page: int) -> int:
    return max(1, page)


def _pagination_from_headers(
    headers: Any,
    query: dict[str, str],
) -> dict[str, Any]:
    links = _parse_link_header(headers.get("Link", ""))
    page = _int_or_none(query.get("page")) or 1
    per_page = _int_or_none(query.get("per_page"))

    return {
        "page": page,
        "per_page": per_page,
        "has_next_page": "next" in links,
        "has_previous_page": "prev" in links,
        "next_page": _page_from_url(links.get("next")),
        "previous_page": _page_from_url(links.get("prev")),
        "first_page": _page_from_url(links.get("first")),
        "last_page": _page_from_url(links.get("last")),
    }


def _rate_limit_from_headers(headers: Any) -> dict[str, Any]:
    reset_epoch = _int_or_none(headers.get("X-RateLimit-Reset"))
    return {
        "limit": _int_or_none(headers.get("X-RateLimit-Limit")),
        "remaining": _int_or_none(headers.get("X-RateLimit-Remaining")),
        "used": _int_or_none(headers.get("X-RateLimit-Used")),
        "resource": headers.get("X-RateLimit-Resource"),
        "reset_epoch_seconds": reset_epoch,
    }


def _parse_link_header(header: str) -> dict[str, str]:
    links: dict[str, str] = {}
    for part in header.split(","):
        url_part, separator, rel_part = part.strip().partition(";")
        if not separator:
            continue
        url = url_part.strip()
        if not url.startswith("<") or not url.endswith(">"):
            continue

        rel = ""
        for param in rel_part.split(";"):
            name, param_separator, value = param.strip().partition("=")
            if param_separator and name == "rel":
                rel = value.strip('"')
                break
        if rel:
            links[rel] = url[1:-1]
    return links


def _page_from_url(url: str | None) -> int | None:
    if not url:
        return None
    values = parse_qs(urlparse(url).query).get("page")
    if not values:
        return None
    return _int_or_none(values[0])


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
