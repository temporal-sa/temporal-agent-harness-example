from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
from dataclasses import dataclass
from typing import Any


SESSION_COOKIE = "simple_chat_session"
DEFAULT_SESSION_SECONDS = 60 * 60 * 24 * 7


@dataclass(frozen=True)
class AuthenticatedUser:
    user_id: str
    username: str


class AuthError(Exception):
    pass


def authenticate_user(username: str, password: str) -> AuthenticatedUser | None:
    configured_username = os.environ.get("SIMPLE_CHAT_USERNAME", "demo")
    configured_password = os.environ.get("SIMPLE_CHAT_PASSWORD", "demo")

    if not hmac.compare_digest(username, configured_username):
        return None
    if not hmac.compare_digest(password, configured_password):
        return None

    return AuthenticatedUser(
        user_id=_user_id_for_username(username),
        username=username,
    )


def create_session_token(
    user: AuthenticatedUser,
    *,
    ttl_seconds: int = DEFAULT_SESSION_SECONDS,
) -> str:
    now = int(time.time())
    return _encode_jwt(
        {
            "sub": user.user_id,
            "username": user.username,
            "iat": now,
            "exp": now + ttl_seconds,
        },
        _session_secret(),
    )


def user_from_session_token(token: str) -> AuthenticatedUser:
    payload = _decode_jwt(token, _session_secret())
    user_id = payload.get("sub")
    username = payload.get("username")
    if not isinstance(user_id, str) or not isinstance(username, str):
        raise AuthError("Session token is missing user identity")
    return AuthenticatedUser(user_id=user_id, username=username)


def _user_id_for_username(username: str) -> str:
    digest = hashlib.sha256(username.encode("utf-8")).hexdigest()[:24]
    return f"user_{digest}"


def _session_secret() -> bytes:
    secret = os.environ.get("SIMPLE_CHAT_JWT_SECRET")
    if secret is None:
        secret = "dev-only-simple-chat-secret"
    return secret.encode("utf-8")


def _encode_jwt(payload: dict[str, Any], secret: bytes) -> str:
    header = {"alg": "HS256", "typ": "JWT"}
    signing_input = ".".join(
        [
            _base64url_json(header),
            _base64url_json(payload),
        ]
    )
    signature = hmac.new(
        secret,
        signing_input.encode("ascii"),
        hashlib.sha256,
    ).digest()
    return f"{signing_input}.{_base64url_encode(signature)}"


def _decode_jwt(token: str, secret: bytes) -> dict[str, Any]:
    parts = token.split(".")
    if len(parts) != 3:
        raise AuthError("Invalid session token")

    signing_input = f"{parts[0]}.{parts[1]}"
    expected_signature = hmac.new(
        secret,
        signing_input.encode("ascii"),
        hashlib.sha256,
    ).digest()
    actual_signature = _base64url_decode(parts[2])
    if not hmac.compare_digest(actual_signature, expected_signature):
        raise AuthError("Invalid session signature")

    payload = json.loads(_base64url_decode(parts[1]))
    exp = payload.get("exp")
    if not isinstance(exp, int) or exp < int(time.time()):
        raise AuthError("Session token expired")
    return payload


def _base64url_json(value: dict[str, Any]) -> str:
    raw = json.dumps(value, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return _base64url_encode(raw)


def _base64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _base64url_decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(f"{value}{padding}")
