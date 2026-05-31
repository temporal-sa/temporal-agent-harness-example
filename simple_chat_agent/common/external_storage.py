from __future__ import annotations

import asyncio
import base64
import contextlib
import fcntl
import hashlib
import json
import os
import urllib.parse
from pathlib import Path
from typing import Any, Mapping, Sequence

from temporalio.api.common.v1 import Payload
from temporalio.contrib.aws.s3driver import S3StorageDriver, S3StorageDriverClient
from temporalio.converter import (
    DataConverter,
    ExternalStorage,
    StorageDriver,
    StorageDriverClaim,
    StorageDriverRetrieveContext,
    StorageDriverStoreContext,
)


DEFAULT_EXTERNAL_STORAGE_PATH = ".simple_chat_agent/external_payloads.json"
DEFAULT_EXTERNAL_STORAGE_THRESHOLD_BYTES = 1024

# Workflow type used to build the S3 key prefix when purging a chat's payloads.
SIMPLE_CHAT_WORKFLOW_TYPE = "SimpleChatWorkflow"


class JsonFileStorageDriver(StorageDriver):
    def __init__(self, path: str | None = None) -> None:
        self._path = Path(
            path
            or os.environ.get(
                "SIMPLE_CHAT_EXTERNAL_STORAGE_PATH",
                DEFAULT_EXTERNAL_STORAGE_PATH,
            )
        )
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._lock_path = self._path.with_suffix(f"{self._path.suffix}.lock")

    def name(self) -> str:
        return "simple-chat-json-file"

    def type(self) -> str:
        return "json-file"

    async def store(
        self,
        context: StorageDriverStoreContext,
        payloads: Sequence[Payload],
    ) -> list[StorageDriverClaim]:
        del context
        with self._locked():
            db = self._read_db()
            claims: list[StorageDriverClaim] = []

            for payload in payloads:
                encoded = payload.SerializeToString()
                key = hashlib.sha256(encoded).hexdigest()
                db.setdefault(
                    key,
                    {
                        "payload": base64.b64encode(encoded).decode("ascii"),
                        "size": str(payload.ByteSize()),
                    },
                )
                claims.append(StorageDriverClaim(claim_data={"key": key}))

            self._write_db(db)
            return claims

    async def retrieve(
        self,
        context: StorageDriverRetrieveContext,
        claims: Sequence[StorageDriverClaim],
    ) -> list[Payload]:
        del context
        with self._locked():
            db = self._read_db()
            payloads: list[Payload] = []

            for claim in claims:
                key = claim.claim_data.get("key")
                if not key or key not in db:
                    raise KeyError(f"External payload not found: {key}")

                payload = Payload()
                payload.ParseFromString(base64.b64decode(db[key]["payload"]))
                payloads.append(payload)

            return payloads

    @contextlib.contextmanager
    def _locked(self):
        with self._lock_path.open("w", encoding="utf-8") as lock_file:
            fcntl.flock(lock_file, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock_file, fcntl.LOCK_UN)

    def _read_db(self) -> dict[str, dict[str, str]]:
        if not self._path.exists():
            return {}
        with self._path.open("r", encoding="utf-8") as file:
            raw = json.load(file)
        if not isinstance(raw, dict):
            raise ValueError(f"External storage file is not a JSON object: {self._path}")
        return {
            str(key): _storage_row(value)
            for key, value in raw.items()
        }

    def _write_db(self, db: dict[str, dict[str, str]]) -> None:
        temp_path = self._path.with_suffix(f"{self._path.suffix}.tmp")
        with temp_path.open("w", encoding="utf-8") as file:
            json.dump(db, file, sort_keys=True, separators=(",", ":"))
        temp_path.replace(self._path)


class _Boto3S3StorageDriverClient(S3StorageDriverClient):
    """Sync-boto3-backed client for the official S3StorageDriver.

    boto3 clients are not async context managers and are cheap to keep around,
    so this keeps simple_chat_data_converter() synchronous (no async client
    lifecycle to thread through the worker/web startup) and shares the same
    boto3 stack used to purge payloads. Calls run off the event loop.
    """

    def __init__(self) -> None:
        import boto3

        self._client = boto3.client("s3")

    def describe(self) -> Mapping[str, str]:
        region = self._client.meta.region_name
        return {"client_region": region} if region else {}

    async def object_exists(self, *, bucket: str, key: str) -> bool:
        from botocore.exceptions import ClientError

        def _head() -> bool:
            try:
                self._client.head_object(Bucket=bucket, Key=key)
                return True
            except ClientError as err:
                code = err.response.get("Error", {}).get("Code")
                if code in {"404", "NoSuchKey", "NotFound"}:
                    return False
                raise

        return await asyncio.to_thread(_head)

    async def put_object(self, *, bucket: str, key: str, data: bytes) -> None:
        await asyncio.to_thread(
            lambda: self._client.put_object(Bucket=bucket, Key=key, Body=data)
        )

    async def get_object(self, *, bucket: str, key: str) -> bytes:
        def _get() -> bytes:
            response = self._client.get_object(Bucket=bucket, Key=key)
            return response["Body"].read()

        return await asyncio.to_thread(_get)


def _s3_bucket() -> str | None:
    bucket = os.environ.get("SIMPLE_CHAT_S3_BUCKET", "").strip()
    return bucket or None


def simple_chat_data_converter() -> DataConverter:
    threshold = int(
        os.environ.get(
            "SIMPLE_CHAT_EXTERNAL_STORAGE_THRESHOLD_BYTES",
            str(DEFAULT_EXTERNAL_STORAGE_THRESHOLD_BYTES),
        )
    )
    bucket = _s3_bucket()
    if bucket is not None:
        # Durable, pod-restart-safe, shared storage (production / k8s).
        driver: StorageDriver = S3StorageDriver(
            client=_Boto3S3StorageDriverClient(),
            bucket=bucket,
        )
    else:
        # Local dev: on-disk JSON file, no AWS required.
        driver = JsonFileStorageDriver()
    return DataConverter(
        external_storage=ExternalStorage(
            drivers=[driver],
            payload_size_threshold=threshold,
        )
    )


def purge_workflow_payloads(
    *,
    namespace: str,
    workflow_id: str,
    workflow_type: str = SIMPLE_CHAT_WORKFLOW_TYPE,
) -> int:
    """Delete all S3 objects offloaded for a workflow (across runs).

    Mirrors the official S3StorageDriver key layout
    ``v0/ns/<ns>/wt/<type>/wi/<id>/ri/<run>/d/sha256/<hash>`` and deletes the
    ``.../wi/<id>/`` prefix. No-op (returns 0) when S3 storage is not configured.
    """
    bucket = _s3_bucket()
    if bucket is None:
        return 0

    import boto3

    def _quote(value: str) -> str:
        return urllib.parse.quote(value, safe="")

    prefix = (
        f"v0/ns/{_quote(namespace)}"
        f"/wt/{_quote(workflow_type)}/wi/{_quote(workflow_id)}/"
    )

    client = boto3.client("s3")
    paginator = client.get_paginator("list_objects_v2")
    deleted = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objects = [{"Key": item["Key"]} for item in page.get("Contents", [])]
        if not objects:
            continue
        client.delete_objects(Bucket=bucket, Delete={"Objects": objects})
        deleted += len(objects)
    return deleted


def _storage_row(value: Any) -> dict[str, str]:
    if not isinstance(value, dict):
        raise ValueError("External storage row must be a JSON object")
    payload = value.get("payload")
    size = value.get("size")
    if not isinstance(payload, str) or not isinstance(size, str):
        raise ValueError("External storage row must contain payload and size strings")
    return {"payload": payload, "size": size}
