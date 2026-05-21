from __future__ import annotations

import base64
import contextlib
import fcntl
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Sequence

from temporalio.api.common.v1 import Payload
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


def simple_chat_data_converter() -> DataConverter:
    threshold = int(
        os.environ.get(
            "SIMPLE_CHAT_EXTERNAL_STORAGE_THRESHOLD_BYTES",
            str(DEFAULT_EXTERNAL_STORAGE_THRESHOLD_BYTES),
        )
    )
    return DataConverter(
        external_storage=ExternalStorage(
            drivers=[JsonFileStorageDriver()],
            payload_size_threshold=threshold,
        )
    )


def _storage_row(value: Any) -> dict[str, str]:
    if not isinstance(value, dict):
        raise ValueError("External storage row must be a JSON object")
    payload = value.get("payload")
    size = value.get("size")
    if not isinstance(payload, str) or not isinstance(size, str):
        raise ValueError("External storage row must contain payload and size strings")
    return {"payload": payload, "size": size}
