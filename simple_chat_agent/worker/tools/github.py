from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlencode, urlparse
from urllib.request import Request, urlopen

from claude_harness.streaming import StreamContext
from claude_harness.tools import ToolContext, ToolResult, tool
from claude_harness.tool_types import ToolType
from simple_chat_agent.common.store import app_store


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
        payload = await ctx.activity(
            _github_open_issue_activity,
            args={
                "connection_id": connection_id,
                "owner": owner,
                "repo": repo,
                "title": title,
                "body": body,
                "labels": labels or [],
            },
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


async def _github_open_issue_activity(
    connection_id: str,
    owner: str,
    repo: str,
    title: str,
    body: str,
    labels: list[str],
    *,
    stream: StreamContext,
) -> dict[str, Any]:
    await stream.emit(
        {
            "repo": f"{owner}/{repo}",
            "title": title,
            "labels": labels,
        },
        kind="github_open_issue_start",
    )
    payload = await asyncio.to_thread(
        _github_api_request,
        connection_id,
        f"/repos/{owner}/{repo}/issues",
        method="POST",
        payload=_issue_payload(title=title, body=body, labels=labels),
    )
    if "error" in payload:
        return payload

    issue = {
        "number": payload.get("number"),
        "title": payload.get("title"),
        "state": payload.get("state"),
        "html_url": payload.get("html_url"),
        "user": (payload.get("user") or {}).get("login"),
        "created_at": payload.get("created_at"),
    }
    await stream.emit(
        {
            "repo": f"{owner}/{repo}",
            "number": issue["number"],
            "url": issue["html_url"],
        },
        kind="github_open_issue_complete",
    )
    return {"issue": issue}


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
        return {"error": "GitHub connection was not found."}

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
        return {
            "error": f"GitHub API HTTP {err.code}",
            "details": _read_http_error(err),
        }
    except URLError as err:
        return {"error": f"GitHub API error: {err.reason}"}

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
