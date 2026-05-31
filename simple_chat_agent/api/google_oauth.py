from __future__ import annotations

import base64
import json
import os
import time
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


GOOGLE_PROVIDER = "google"
GOOGLE_AUTHORIZE_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_ISSUERS = ("https://accounts.google.com", "accounts.google.com")


class GoogleOAuthError(Exception):
    pass


@dataclass(frozen=True)
class GoogleIdentity:
    subject: str
    email: str
    name: str | None
    picture: str | None
    hosted_domain: str | None


def google_oauth_configured() -> bool:
    return bool(
        os.environ.get("GOOGLE_OAUTH_CLIENT_ID")
        and os.environ.get("GOOGLE_OAUTH_CLIENT_SECRET")
    )


def google_authorize_url(*, state: str, redirect_uri: str) -> str:
    client_id = os.environ["GOOGLE_OAUTH_CLIENT_ID"]
    params = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": "openid email profile",
        "state": state,
        "access_type": "online",
        "prompt": "select_account",
    }
    allowed_domain = google_allowed_domain()
    if allowed_domain:
        params["hd"] = allowed_domain
    return f"{GOOGLE_AUTHORIZE_URL}?{urlencode(params)}"


def exchange_google_code(code: str, *, redirect_uri: str) -> dict[str, Any]:
    client_id = os.environ.get("GOOGLE_OAUTH_CLIENT_ID")
    client_secret = os.environ.get("GOOGLE_OAUTH_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise GoogleOAuthError("Google OAuth is not configured.")

    request = Request(
        GOOGLE_TOKEN_URL,
        data=urlencode(
            {
                "client_id": client_id,
                "client_secret": client_secret,
                "code": code,
                "grant_type": "authorization_code",
                "redirect_uri": redirect_uri,
            }
        ).encode("utf-8"),
        headers={
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        method="POST",
    )

    payload = _send_json_request(request)
    if "error" in payload:
        description = payload.get("error_description") or payload["error"]
        raise GoogleOAuthError(str(description))
    return payload


def identity_from_id_token(id_token: str) -> GoogleIdentity:
    # Signature verification is intentionally skipped: the ID token was just
    # received directly from Google's token endpoint over TLS, which Google's
    # docs treat as sufficient for trusting the token's contents.
    claims = _decode_id_token_claims(id_token)

    client_id = os.environ.get("GOOGLE_OAUTH_CLIENT_ID")
    if claims.get("iss") not in GOOGLE_ISSUERS:
        raise GoogleOAuthError("ID token has unexpected issuer.")
    if client_id and claims.get("aud") != client_id:
        raise GoogleOAuthError("ID token audience does not match this client.")

    exp = claims.get("exp")
    if not isinstance(exp, int) or exp < int(time.time()):
        raise GoogleOAuthError("ID token is expired.")

    email = claims.get("email")
    subject = claims.get("sub")
    if not isinstance(email, str) or not isinstance(subject, str):
        raise GoogleOAuthError("ID token is missing identity claims.")
    if claims.get("email_verified") is not True:
        raise GoogleOAuthError("Google account email is not verified.")

    allowed_domain = google_allowed_domain()
    if allowed_domain:
        hd = claims.get("hd")
        email_domain = email.rsplit("@", 1)[-1].lower()
        if hd != allowed_domain and email_domain != allowed_domain.lower():
            raise GoogleOAuthError(
                f"Only {allowed_domain} accounts are allowed to sign in."
            )

    name = claims.get("name") if isinstance(claims.get("name"), str) else None
    picture = (
        claims.get("picture") if isinstance(claims.get("picture"), str) else None
    )
    hosted_domain = claims.get("hd") if isinstance(claims.get("hd"), str) else None
    return GoogleIdentity(
        subject=subject,
        email=email,
        name=name,
        picture=picture,
        hosted_domain=hosted_domain,
    )


def google_redirect_uri_from_base(base_url: str) -> str:
    explicit = os.environ.get("GOOGLE_OAUTH_REDIRECT_URI", "").strip()
    if explicit:
        return explicit
    return f"{base_url.rstrip('/')}/oauth/google/callback"


def google_allowed_domain() -> str | None:
    value = os.environ.get("GOOGLE_OAUTH_ALLOWED_DOMAIN", "temporal.io").strip()
    return value or None


def _decode_id_token_claims(id_token: str) -> dict[str, Any]:
    parts = id_token.split(".")
    if len(parts) != 3:
        raise GoogleOAuthError("ID token is malformed.")
    try:
        padded = parts[1] + "=" * (-len(parts[1]) % 4)
        payload = base64.urlsafe_b64decode(padded)
        return json.loads(payload)
    except (ValueError, json.JSONDecodeError) as err:
        raise GoogleOAuthError("ID token payload could not be decoded.") from err


def _send_json_request(request: Request) -> dict[str, Any]:
    try:
        with urlopen(request, timeout=15) as response:
            raw = response.read().decode("utf-8")
    except HTTPError as err:
        details = err.read().decode("utf-8", errors="replace")
        raise GoogleOAuthError(f"Google HTTP {err.code}: {details}") from err
    except URLError as err:
        raise GoogleOAuthError(f"Google request failed: {err.reason}") from err

    if not raw:
        return {}
    return json.loads(raw)
