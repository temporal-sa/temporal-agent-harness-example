from __future__ import annotations

import json
import os
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


GITHUB_PROVIDER = "github"


class GitHubOAuthError(Exception):
    pass


def github_oauth_configured() -> bool:
    return bool(
        os.environ.get("GITHUB_OAUTH_CLIENT_ID")
        and os.environ.get("GITHUB_OAUTH_CLIENT_SECRET")
    )


def github_authorize_url(*, state: str) -> str:
    client_id = os.environ["GITHUB_OAUTH_CLIENT_ID"]
    redirect_uri = github_redirect_uri()
    scopes = github_scopes()
    return "https://github.com/login/oauth/authorize?" + urlencode(
        {
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "scope": scopes,
            "state": state,
        }
    )


def exchange_github_code(code: str) -> dict[str, Any]:
    client_id = os.environ.get("GITHUB_OAUTH_CLIENT_ID")
    client_secret = os.environ.get("GITHUB_OAUTH_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise GitHubOAuthError("GitHub OAuth is not configured.")

    request = Request(
        "https://github.com/login/oauth/access_token",
        data=urlencode(
            {
                "client_id": client_id,
                "client_secret": client_secret,
                "code": code,
                "redirect_uri": github_redirect_uri(),
            }
        ).encode("utf-8"),
        headers={
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": "temporal-agent-harness-example/0.1",
        },
        method="POST",
    )

    payload = _send_json_request(request)
    if "error" in payload:
        description = payload.get("error_description") or payload["error"]
        raise GitHubOAuthError(str(description))
    return payload


def fetch_github_user(access_token: str) -> dict[str, Any]:
    request = Request(
        "https://api.github.com/user",
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {access_token}",
            "User-Agent": "temporal-agent-harness-example/0.1",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    return _send_json_request(request)


def github_redirect_uri() -> str:
    return os.environ.get(
        "GITHUB_OAUTH_REDIRECT_URI",
        "http://127.0.0.1:8000/oauth/github/callback",
    )


def github_scopes() -> str:
    return os.environ.get(
        "GITHUB_OAUTH_SCOPES",
        "read:user,user:email,public_repo",
    )


def _send_json_request(request: Request) -> dict[str, Any]:
    try:
        with urlopen(request, timeout=15) as response:
            raw = response.read().decode("utf-8")
    except HTTPError as err:
        details = err.read().decode("utf-8", errors="replace")
        raise GitHubOAuthError(f"GitHub HTTP {err.code}: {details}") from err
    except URLError as err:
        raise GitHubOAuthError(f"GitHub request failed: {err.reason}") from err

    if not raw:
        return {}
    return json.loads(raw)
