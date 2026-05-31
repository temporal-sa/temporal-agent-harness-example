from __future__ import annotations

import os
from typing import Any, Literal

import jwt
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from google.protobuf.json_format import MessageToDict, ParseDict
from temporalio.api.common.v1 import Payloads
from temporalio.converter import DataConverter

from simple_chat_agent.common.external_storage import simple_chat_data_converter

DEFAULT_CODEC_SERVER_HOST = "127.0.0.1"
DEFAULT_CODEC_SERVER_PORT = 8001

# Temporal Cloud signs the access tokens it forwards from the Web UI (when the
# namespace's Codec Server endpoint has "Pass access token" enabled). Verify
# them against this JWKS endpoint. The email of the requesting user is carried
# in the claim below once the signature is validated.
DEFAULT_CODEC_JWKS_URL = "https://login.tmprl.cloud/.well-known/jwks.json"
TEMPORAL_CLOUD_EMAIL_CLAIM = "https://saas-api.tmprl.cloud/user/email"

CodecOperation = Literal["encode", "decode"]


def codec_auth_enabled() -> bool:
    return os.environ.get("SIMPLE_CHAT_CODEC_AUTH_ENABLED", "0").lower() not in {
        "0",
        "false",
        "no",
        "off",
    }


def codec_jwks_url() -> str:
    return os.environ.get("SIMPLE_CHAT_CODEC_JWKS_URL", DEFAULT_CODEC_JWKS_URL)


def codec_allowed_origins() -> list[str]:
    raw = os.environ.get("SIMPLE_CHAT_CODEC_ALLOWED_ORIGINS", "*")
    origins = [origin.strip() for origin in raw.split(",") if origin.strip()]
    return origins or ["*"]


def codec_server_url() -> str:
    return f"http://{codec_server_host()}:{codec_server_port()}"


def codec_server_host() -> str:
    return os.environ.get("SIMPLE_CHAT_CODEC_SERVER_HOST", DEFAULT_CODEC_SERVER_HOST)


def codec_server_port() -> int:
    return int(
        os.environ.get("SIMPLE_CHAT_CODEC_SERVER_PORT", str(DEFAULT_CODEC_SERVER_PORT))
    )


def codec_server_enabled() -> bool:
    value = os.environ.get("SIMPLE_CHAT_CODEC_SERVER_ENABLED", "1")
    return value.lower() not in {"0", "false", "no", "off"}


def _build_jwks_client() -> jwt.PyJWKClient | None:
    if not codec_auth_enabled():
        return None
    # PyJWKClient caches signing keys in-process, so the JWKS endpoint is only
    # fetched on the first request and when a new key id is seen.
    return jwt.PyJWKClient(codec_jwks_url())


def _require_temporal_cloud_user(jwks_client: jwt.PyJWKClient | None):
    async def dependency(request: Request) -> str | None:
        if jwks_client is None:
            return None

        header = request.headers.get("authorization", "")
        scheme, _, token = header.partition(" ")
        if scheme.lower() != "bearer" or not token.strip():
            raise HTTPException(
                status_code=401,
                detail="Missing bearer access token.",
            )

        try:
            signing_key = jwks_client.get_signing_key_from_jwt(token)
            claims = jwt.decode(
                token,
                signing_key.key,
                algorithms=["RS256"],
                # Temporal Cloud tokens are not minted for this server, so the
                # audience is not ours to validate; the signature + expiry plus
                # the trusted JWKS source are what authenticate the caller.
                options={"verify_aud": False},
            )
        except jwt.PyJWTError as err:
            raise HTTPException(
                status_code=401,
                detail=f"Invalid access token: {type(err).__name__}: {err}",
            ) from err

        return claims.get(TEMPORAL_CLOUD_EMAIL_CLAIM)

    return dependency


def create_codec_app(
    data_converter: DataConverter | None = None,
) -> FastAPI:
    converter = data_converter or simple_chat_data_converter()
    app = FastAPI(title="Simple Chat Temporal Codec Server")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=codec_allowed_origins(),
        allow_methods=["*"],
        allow_headers=["*"],
    )

    jwks_client = _build_jwks_client()
    require_user = _require_temporal_cloud_user(jwks_client)

    @app.get("/")
    async def root() -> dict[str, str]:
        return {
            "status": "ok",
            "encode": "/encode",
            "decode": "/decode",
        }

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/encode")
    async def encode(
        request: dict[str, Any],
        _user: str | None = Depends(require_user),
    ) -> dict[str, Any]:
        return await _transform_payloads(converter, request, "encode")

    @app.post("/decode")
    async def decode(
        request: dict[str, Any],
        _user: str | None = Depends(require_user),
    ) -> dict[str, Any]:
        return await _transform_payloads(converter, request, "decode")

    return app


async def _transform_payloads(
    converter: DataConverter,
    request: dict[str, Any],
    operation: CodecOperation,
) -> dict[str, Any]:
    payloads = _payloads_from_request(request)
    try:
        if operation == "encode":
            await converter._transform_outbound_payloads(payloads)
        else:
            await converter._transform_inbound_payloads(payloads)
    except Exception as err:
        raise HTTPException(
            status_code=400,
            detail=f"Payload {operation} failed: {type(err).__name__}: {err}",
        ) from err

    return _payloads_to_response(payloads)


def _payloads_from_request(request: dict[str, Any]) -> Payloads:
    if not isinstance(request, dict):
        raise HTTPException(status_code=400, detail="Codec request must be an object.")
    if "payloads" not in request:
        raise HTTPException(
            status_code=400,
            detail="Codec request must contain a 'payloads' field.",
        )

    payloads = Payloads()
    try:
        ParseDict(request, payloads, ignore_unknown_fields=True)
    except Exception as err:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid payloads request: {type(err).__name__}: {err}",
        ) from err
    return payloads


def _payloads_to_response(payloads: Payloads) -> dict[str, Any]:
    return MessageToDict(
        payloads,
        preserving_proto_field_name=True,
        always_print_fields_with_no_presence=True,
    )
