from __future__ import annotations

import json
import os
import re
import sqlite3
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4


@dataclass(frozen=True)
class ConversationRecord:
    workflow_id: str
    run_id: str
    title: str
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class OAuthConnectionRecord:
    connection_id: str
    user_id: str
    provider: str
    access_token: str
    token_type: str
    scope: str
    provider_user_id: str | None
    provider_user_login: str | None
    metadata: dict[str, Any]
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class ArtifactRecord:
    artifact_id: str
    user_id: str
    conversation_id: str
    workflow_id: str
    name: str
    mime_type: str
    size_bytes: int
    path: str
    metadata: dict[str, Any]
    created_at: str


class AppStore:
    def __init__(
        self,
        path: str | None = None,
        *,
        artifact_dir: str | None = None,
    ) -> None:
        self._path = Path(
            path
            or os.environ.get(
                "SIMPLE_CHAT_DB_PATH",
                ".simple_chat_agent/simple_chat.sqlite3",
            )
        )
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._artifact_dir = Path(
            artifact_dir
            or os.environ.get(
                "SIMPLE_CHAT_ARTIFACT_DIR",
                str(self._path.parent / "artifacts"),
            )
        )
        self._artifact_dir.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()
        # When configured, OAuth connections (GitHub/MCP tokens) live in DynamoDB
        # so they survive redeploys and are shared across pods. Transient
        # oauth_states and other tables stay in SQLite. Unset => all SQLite
        # (local dev / file storage).
        table_name = dynamo_oauth_table_name()
        self._oauth_dynamo: DynamoOAuthStore | None = (
            DynamoOAuthStore(table_name) if table_name else None
        )
        # Artifacts: bytes in S3, metadata in DynamoDB when configured (durable,
        # shared across pods); otherwise local disk + SQLite.
        artifacts_table = dynamo_artifacts_table_name()
        artifacts_bucket = _artifact_s3_bucket()
        self._artifact_store: S3DynamoArtifactStore | None = (
            S3DynamoArtifactStore(table_name=artifacts_table, bucket=artifacts_bucket)
            if artifacts_table and artifacts_bucket
            else None
        )

    def read_artifact_bytes(self, artifact: ArtifactRecord) -> bytes:
        return read_artifact_bytes(artifact)

    def record_conversation(
        self,
        *,
        user_id: str,
        workflow_id: str,
        run_id: str,
        title: str,
    ) -> None:
        now = _now()
        with self._connect() as conn:
            conn.execute(
                """
                insert into conversations (
                    user_id, workflow_id, run_id, title, created_at, updated_at
                )
                values (?, ?, ?, ?, ?, ?)
                on conflict(workflow_id) do update set
                    run_id = excluded.run_id,
                    title = excluded.title,
                    updated_at = excluded.updated_at
                """,
                (user_id, workflow_id, run_id, title, now, now),
            )

    def list_conversations(self, user_id: str) -> list[ConversationRecord]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                select workflow_id, run_id, title, created_at, updated_at
                from conversations
                where user_id = ?
                order by updated_at desc
                """,
                (user_id,),
            ).fetchall()
        return [
            ConversationRecord(
                workflow_id=row["workflow_id"],
                run_id=row["run_id"],
                title=row["title"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )
            for row in rows
        ]

    def get_conversation(
        self, *, user_id: str, workflow_id: str
    ) -> ConversationRecord | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                select workflow_id, run_id, title, created_at, updated_at
                from conversations
                where user_id = ? and workflow_id = ?
                """,
                (user_id, workflow_id),
            ).fetchone()
        if row is None:
            return None
        return ConversationRecord(
            workflow_id=row["workflow_id"],
            run_id=row["run_id"],
            title=row["title"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def touch_conversation(
        self,
        *,
        user_id: str,
        workflow_id: str,
        title: str | None = None,
    ) -> None:
        now = _now()
        with self._connect() as conn:
            if title is None:
                conn.execute(
                    """
                    update conversations
                    set updated_at = ?
                    where user_id = ? and workflow_id = ?
                    """,
                    (now, user_id, workflow_id),
                )
            else:
                conn.execute(
                    """
                    update conversations
                    set title = ?, updated_at = ?
                    where user_id = ? and workflow_id = ?
                    """,
                    (title, now, user_id, workflow_id),
                )

    def delete_conversation(self, *, user_id: str, workflow_id: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "delete from conversations where user_id = ? and workflow_id = ?",
                (user_id, workflow_id),
            )

    def create_oauth_state(
        self,
        *,
        user_id: str,
        provider: str,
        metadata: dict[str, str] | None = None,
    ) -> str:
        state = uuid4().hex
        with self._connect() as conn:
            conn.execute(
                """
                insert into oauth_states (
                    state, user_id, provider, metadata_json, created_at
                )
                values (?, ?, ?, ?, ?)
                """,
                (state, user_id, provider, json.dumps(metadata or {}), _now()),
            )
        return state

    def consume_oauth_state(
        self,
        *,
        state: str,
        provider: str,
        max_age: timedelta = timedelta(minutes=10),
    ) -> tuple[str, dict[str, str]] | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                select user_id, metadata_json, created_at
                from oauth_states
                where state = ? and provider = ?
                """,
                (state, provider),
            ).fetchone()
            conn.execute(
                "delete from oauth_states where state = ? and provider = ?",
                (state, provider),
            )

        if row is None:
            return None

        created_at = datetime.fromisoformat(row["created_at"])
        if created_at < datetime.now(UTC) - max_age:
            return None

        metadata = json.loads(row["metadata_json"])
        return row["user_id"], metadata

    def upsert_oauth_connection(
        self,
        *,
        user_id: str,
        provider: str,
        access_token: str,
        token_type: str,
        scope: str,
        provider_user_id: str | None,
        provider_user_login: str | None,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        if self._oauth_dynamo is not None:
            return self._oauth_dynamo.upsert_oauth_connection(
                user_id=user_id,
                provider=provider,
                access_token=access_token,
                token_type=token_type,
                scope=scope,
                provider_user_id=provider_user_id,
                provider_user_login=provider_user_login,
                metadata=metadata,
            )
        existing = self.get_oauth_connection(user_id=user_id, provider=provider)
        connection_id = (
            existing.connection_id
            if existing is not None
            else f"{provider}_{uuid4().hex}"
        )
        now = _now()
        created_at = existing.created_at if existing is not None else now
        metadata_json = json.dumps(metadata if metadata is not None else {})

        with self._connect() as conn:
            conn.execute(
                """
                insert into oauth_connections (
                    connection_id, user_id, provider, access_token, token_type,
                    scope, provider_user_id, provider_user_login, metadata_json,
                    created_at, updated_at
                )
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                on conflict(user_id, provider) do update set
                    connection_id = excluded.connection_id,
                    access_token = excluded.access_token,
                    token_type = excluded.token_type,
                    scope = excluded.scope,
                    provider_user_id = excluded.provider_user_id,
                    provider_user_login = excluded.provider_user_login,
                    metadata_json = excluded.metadata_json,
                    updated_at = excluded.updated_at
                """,
                (
                    connection_id,
                    user_id,
                    provider,
                    access_token,
                    token_type,
                    scope,
                    provider_user_id,
                    provider_user_login,
                    metadata_json,
                    created_at,
                    now,
                ),
            )

        return connection_id

    def get_oauth_connection(
        self, *, user_id: str, provider: str
    ) -> OAuthConnectionRecord | None:
        if self._oauth_dynamo is not None:
            return self._oauth_dynamo.get_oauth_connection(
                user_id=user_id, provider=provider
            )
        with self._connect() as conn:
            row = conn.execute(
                """
                select *
                from oauth_connections
                where user_id = ? and provider = ?
                """,
                (user_id, provider),
            ).fetchone()
        return _oauth_connection_from_row(row)

    def get_oauth_connection_by_id(
        self, connection_id: str
    ) -> OAuthConnectionRecord | None:
        if self._oauth_dynamo is not None:
            return self._oauth_dynamo.get_oauth_connection_by_id(connection_id)
        with self._connect() as conn:
            row = conn.execute(
                """
                select *
                from oauth_connections
                where connection_id = ?
                """,
                (connection_id,),
            ).fetchone()
        return _oauth_connection_from_row(row)

    def get_oauth_connection_by_provider(
        self, provider: str
    ) -> OAuthConnectionRecord | None:
        if self._oauth_dynamo is not None:
            return self._oauth_dynamo.get_oauth_connection_by_provider(provider)
        with self._connect() as conn:
            rows = conn.execute(
                """
                select *
                from oauth_connections
                where provider = ?
                order by updated_at desc
                limit 2
                """,
                (provider,),
            ).fetchall()

        if len(rows) > 1:
            raise ValueError(f"Multiple OAuth connections found for provider: {provider}")
        return _oauth_connection_from_row(rows[0] if rows else None)

    def delete_oauth_connection(self, *, user_id: str, provider: str) -> None:
        if self._oauth_dynamo is not None:
            self._oauth_dynamo.delete_oauth_connection(
                user_id=user_id, provider=provider
            )
            return
        with self._connect() as conn:
            conn.execute(
                "delete from oauth_connections where user_id = ? and provider = ?",
                (user_id, provider),
            )

    def create_artifact(
        self,
        *,
        user_id: str,
        conversation_id: str,
        workflow_id: str,
        name: str,
        mime_type: str,
        content: bytes,
        metadata: dict[str, Any] | None = None,
    ) -> ArtifactRecord:
        if self._artifact_store is not None:
            return self._artifact_store.create_artifact(
                user_id=user_id,
                conversation_id=conversation_id,
                workflow_id=workflow_id,
                name=name,
                mime_type=mime_type,
                content=content,
                metadata=metadata,
            )
        artifact_id = f"artifact_{uuid4().hex}"
        safe_name = _safe_artifact_name(name)
        artifact_path = self._artifact_dir / f"{artifact_id}-{safe_name}"
        artifact_path.write_bytes(content)

        now = _now()
        metadata_json = json.dumps(metadata if metadata is not None else {})
        with self._connect() as conn:
            conn.execute(
                """
                insert into artifacts (
                    artifact_id, user_id, conversation_id, workflow_id, name,
                    mime_type, size_bytes, path, metadata_json, created_at
                )
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    artifact_id,
                    user_id,
                    conversation_id,
                    workflow_id,
                    safe_name,
                    mime_type,
                    len(content),
                    str(artifact_path),
                    metadata_json,
                    now,
                ),
            )

        return ArtifactRecord(
            artifact_id=artifact_id,
            user_id=user_id,
            conversation_id=conversation_id,
            workflow_id=workflow_id,
            name=safe_name,
            mime_type=mime_type,
            size_bytes=len(content),
            path=str(artifact_path),
            metadata=json.loads(metadata_json),
            created_at=now,
        )

    def get_artifact(
        self,
        *,
        user_id: str,
        artifact_id: str,
    ) -> ArtifactRecord | None:
        if self._artifact_store is not None:
            return self._artifact_store.get_artifact(
                user_id=user_id, artifact_id=artifact_id
            )
        with self._connect() as conn:
            row = conn.execute(
                """
                select *
                from artifacts
                where user_id = ? and artifact_id = ?
                """,
                (user_id, artifact_id),
            ).fetchone()
        return _artifact_from_row(row)

    def list_artifacts(
        self,
        *,
        user_id: str,
        workflow_id: str,
    ) -> list[ArtifactRecord]:
        if self._artifact_store is not None:
            return self._artifact_store.list_artifacts(
                user_id=user_id, workflow_id=workflow_id
            )
        with self._connect() as conn:
            rows = conn.execute(
                """
                select *
                from artifacts
                where user_id = ? and workflow_id = ?
                order by created_at asc
                """,
                (user_id, workflow_id),
            ).fetchall()
        artifacts: list[ArtifactRecord] = []
        for row in rows:
            artifact = _artifact_from_row(row)
            if artifact is not None:
                artifacts.append(artifact)
        return artifacts

    def delete_artifacts_for_conversation(
        self,
        *,
        user_id: str,
        workflow_id: str,
    ) -> None:
        if self._artifact_store is not None:
            self._artifact_store.delete_artifacts_for_conversation(
                user_id=user_id, workflow_id=workflow_id
            )
            return
        with self._connect() as conn:
            rows = conn.execute(
                """
                select path
                from artifacts
                where user_id = ? and workflow_id = ?
                """,
                (user_id, workflow_id),
            ).fetchall()
            conn.execute(
                "delete from artifacts where user_id = ? and workflow_id = ?",
                (user_id, workflow_id),
            )

        for row in rows:
            with suppress(OSError):
                Path(row["path"]).unlink(missing_ok=True)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._path)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                create table if not exists conversations (
                    user_id text not null,
                    workflow_id text primary key,
                    run_id text not null,
                    title text not null,
                    created_at text not null,
                    updated_at text not null
                );

                create index if not exists conversations_user_updated_idx
                on conversations(user_id, updated_at desc);

                create table if not exists oauth_states (
                    state text primary key,
                    user_id text not null,
                    provider text not null,
                    metadata_json text not null,
                    created_at text not null
                );

                create table if not exists oauth_connections (
                    connection_id text primary key,
                    user_id text not null,
                    provider text not null,
                    access_token text not null,
                    token_type text not null,
                    scope text not null,
                    provider_user_id text,
                    provider_user_login text,
                    metadata_json text not null default '{}',
                    created_at text not null,
                    updated_at text not null,
                    unique(user_id, provider)
                );

                create table if not exists artifacts (
                    artifact_id text primary key,
                    user_id text not null,
                    conversation_id text not null,
                    workflow_id text not null,
                    name text not null,
                    mime_type text not null,
                    size_bytes integer not null,
                    path text not null,
                    metadata_json text not null default '{}',
                    created_at text not null
                );

                create index if not exists artifacts_user_workflow_created_idx
                on artifacts(user_id, workflow_id, created_at);
                """
            )
            self._ensure_oauth_metadata_column(conn)

    def _ensure_oauth_metadata_column(self, conn: sqlite3.Connection) -> None:
        rows = conn.execute("pragma table_info(oauth_connections)").fetchall()
        columns = {row["name"] for row in rows}
        if "metadata_json" not in columns:
            conn.execute(
                "alter table oauth_connections "
                "add column metadata_json text not null default '{}'"
            )


_DYNAMO_TABLE_CACHE: dict[str, Any] = {}


def dynamo_oauth_table_name() -> str | None:
    name = os.environ.get("SIMPLE_CHAT_DYNAMODB_TABLE", "").strip()
    return name or None


def _dynamo_table(name: str) -> Any:
    table = _DYNAMO_TABLE_CACHE.get(name)
    if table is None:
        import boto3

        table = boto3.resource("dynamodb").Table(name)
        _DYNAMO_TABLE_CACHE[name] = table
    return table


class DynamoOAuthStore:
    """DynamoDB-backed OAuth connection store: durable and shared across pods.

    Key schema: partition key ``user_id``, sort key ``provider``. Plus GSIs
    ``connection-id-index`` (``connection_id``) and ``provider-index``
    (``provider``) for the MCP resolver's by-id / by-provider lookups. Used when
    SIMPLE_CHAT_DYNAMODB_TABLE is set; otherwise the app uses local SQLite.
    """

    CONNECTION_ID_INDEX = "connection-id-index"
    PROVIDER_INDEX = "provider-index"

    def __init__(self, table_name: str) -> None:
        self._table_name = table_name

    @property
    def _table(self) -> Any:
        return _dynamo_table(self._table_name)

    def upsert_oauth_connection(
        self,
        *,
        user_id: str,
        provider: str,
        access_token: str,
        token_type: str,
        scope: str,
        provider_user_id: str | None,
        provider_user_login: str | None,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        existing = self.get_oauth_connection(user_id=user_id, provider=provider)
        connection_id = (
            existing.connection_id
            if existing is not None
            else f"{provider}_{uuid4().hex}"
        )
        now = _now()
        item = {
            "user_id": user_id,
            "provider": provider,
            "connection_id": connection_id,
            "access_token": access_token,
            "token_type": token_type,
            "scope": scope,
            "metadata_json": json.dumps(metadata if metadata is not None else {}),
            "created_at": existing.created_at if existing is not None else now,
            "updated_at": now,
        }
        if provider_user_id is not None:
            item["provider_user_id"] = provider_user_id
        if provider_user_login is not None:
            item["provider_user_login"] = provider_user_login
        self._table.put_item(Item=item)
        return connection_id

    def get_oauth_connection(
        self, *, user_id: str, provider: str
    ) -> OAuthConnectionRecord | None:
        response = self._table.get_item(
            Key={"user_id": user_id, "provider": provider},
            ConsistentRead=True,
        )
        return _oauth_connection_from_item(response.get("Item"))

    def get_oauth_connection_by_id(
        self, connection_id: str
    ) -> OAuthConnectionRecord | None:
        from boto3.dynamodb.conditions import Key

        response = self._table.query(
            IndexName=self.CONNECTION_ID_INDEX,
            KeyConditionExpression=Key("connection_id").eq(connection_id),
            Limit=1,
        )
        items = response.get("Items", [])
        return _oauth_connection_from_item(items[0] if items else None)

    def get_oauth_connection_by_provider(
        self, provider: str
    ) -> OAuthConnectionRecord | None:
        from boto3.dynamodb.conditions import Key

        response = self._table.query(
            IndexName=self.PROVIDER_INDEX,
            KeyConditionExpression=Key("provider").eq(provider),
            Limit=2,
        )
        items = response.get("Items", [])
        if len(items) > 1:
            raise ValueError(
                f"Multiple OAuth connections found for provider: {provider}"
            )
        return _oauth_connection_from_item(items[0] if items else None)

    def delete_oauth_connection(self, *, user_id: str, provider: str) -> None:
        self._table.delete_item(Key={"user_id": user_id, "provider": provider})


def _oauth_connection_from_item(
    item: dict[str, Any] | None,
) -> OAuthConnectionRecord | None:
    if not item:
        return None
    return OAuthConnectionRecord(
        connection_id=item["connection_id"],
        user_id=item["user_id"],
        provider=item["provider"],
        access_token=item["access_token"],
        token_type=item["token_type"],
        scope=item.get("scope", ""),
        provider_user_id=item.get("provider_user_id"),
        provider_user_login=item.get("provider_user_login"),
        metadata=json.loads(item.get("metadata_json") or "{}"),
        created_at=item["created_at"],
        updated_at=item["updated_at"],
    )


def dynamo_artifacts_table_name() -> str | None:
    name = os.environ.get("SIMPLE_CHAT_ARTIFACTS_TABLE", "").strip()
    return name or None


def _artifact_s3_bucket() -> str | None:
    name = os.environ.get("SIMPLE_CHAT_S3_BUCKET", "").strip()
    return name or None


_S3_CLIENT_CACHE: dict[str, Any] = {}


def _s3_client() -> Any:
    client = _S3_CLIENT_CACHE.get("client")
    if client is None:
        import boto3

        client = boto3.client("s3")
        _S3_CLIENT_CACHE["client"] = client
    return client


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    bucket, _, key = uri[len("s3://") :].partition("/")
    return bucket, key


class S3DynamoArtifactStore:
    """Durable artifact storage: bytes in S3, metadata in DynamoDB.

    All access happens in activities (worker) or the web layer — never in
    workflow code — so there is no workflow determinism risk. Used when
    SIMPLE_CHAT_ARTIFACTS_TABLE + SIMPLE_CHAT_S3_BUCKET are set; otherwise the
    app uses local disk + SQLite.
    """

    WORKFLOW_INDEX = "workflow-index"

    def __init__(self, *, table_name: str, bucket: str) -> None:
        self._table_name = table_name
        self._bucket = bucket

    @property
    def _table(self) -> Any:
        return _dynamo_table(self._table_name)

    def create_artifact(
        self,
        *,
        user_id: str,
        conversation_id: str,
        workflow_id: str,
        name: str,
        mime_type: str,
        content: bytes,
        metadata: dict[str, Any] | None = None,
    ) -> ArtifactRecord:
        artifact_id = f"artifact_{uuid4().hex}"
        safe_name = _safe_artifact_name(name)
        key = f"artifacts/{user_id}/{workflow_id}/{artifact_id}-{safe_name}"
        _s3_client().put_object(
            Bucket=self._bucket, Key=key, Body=content, ContentType=mime_type
        )
        now = _now()
        path = f"s3://{self._bucket}/{key}"
        metadata_json = json.dumps(metadata if metadata is not None else {})
        self._table.put_item(
            Item={
                "user_id": user_id,
                "artifact_id": artifact_id,
                "workflow_id": workflow_id,
                "conversation_id": conversation_id,
                "name": safe_name,
                "mime_type": mime_type,
                "size_bytes": len(content),
                "path": path,
                "metadata_json": metadata_json,
                "created_at": now,
            }
        )
        return ArtifactRecord(
            artifact_id=artifact_id,
            user_id=user_id,
            conversation_id=conversation_id,
            workflow_id=workflow_id,
            name=safe_name,
            mime_type=mime_type,
            size_bytes=len(content),
            path=path,
            metadata=json.loads(metadata_json),
            created_at=now,
        )

    def get_artifact(
        self, *, user_id: str, artifact_id: str
    ) -> ArtifactRecord | None:
        response = self._table.get_item(
            Key={"user_id": user_id, "artifact_id": artifact_id},
            ConsistentRead=True,
        )
        return _artifact_from_item(response.get("Item"))

    def list_artifacts(
        self, *, user_id: str, workflow_id: str
    ) -> list[ArtifactRecord]:
        from boto3.dynamodb.conditions import Key

        response = self._table.query(
            IndexName=self.WORKFLOW_INDEX,
            KeyConditionExpression=Key("workflow_id").eq(workflow_id),
        )
        records = [
            record
            for record in (
                _artifact_from_item(item) for item in response.get("Items", [])
            )
            if record is not None and record.user_id == user_id
        ]
        records.sort(key=lambda record: record.created_at)
        return records

    def delete_artifacts_for_conversation(
        self, *, user_id: str, workflow_id: str
    ) -> None:
        from boto3.dynamodb.conditions import Key

        response = self._table.query(
            IndexName=self.WORKFLOW_INDEX,
            KeyConditionExpression=Key("workflow_id").eq(workflow_id),
        )
        for item in response.get("Items", []):
            if item.get("user_id") != user_id:
                continue
            with suppress(Exception):
                bucket, key = _parse_s3_uri(str(item.get("path", "")))
                if bucket and key:
                    _s3_client().delete_object(Bucket=bucket, Key=key)
            self._table.delete_item(
                Key={"user_id": item["user_id"], "artifact_id": item["artifact_id"]}
            )


def _artifact_from_item(item: dict[str, Any] | None) -> ArtifactRecord | None:
    if not item:
        return None
    return ArtifactRecord(
        artifact_id=item["artifact_id"],
        user_id=item["user_id"],
        conversation_id=item.get("conversation_id", ""),
        workflow_id=item["workflow_id"],
        name=item["name"],
        mime_type=item["mime_type"],
        size_bytes=int(item.get("size_bytes", 0)),
        path=item["path"],
        metadata=json.loads(item.get("metadata_json") or "{}"),
        created_at=item["created_at"],
    )


def read_artifact_bytes(artifact: ArtifactRecord) -> bytes:
    """Read an artifact's bytes from S3 (s3:// path) or local disk."""
    if artifact.path.startswith("s3://"):
        bucket, key = _parse_s3_uri(artifact.path)
        response = _s3_client().get_object(Bucket=bucket, Key=key)
        return response["Body"].read()
    path = Path(artifact.path)
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(artifact.path)
    return path.read_bytes()


def app_store() -> AppStore:
    return AppStore()


def _oauth_connection_from_row(row: sqlite3.Row | None) -> OAuthConnectionRecord | None:
    if row is None:
        return None
    return OAuthConnectionRecord(
        connection_id=row["connection_id"],
        user_id=row["user_id"],
        provider=row["provider"],
        access_token=row["access_token"],
        token_type=row["token_type"],
        scope=row["scope"],
        provider_user_id=row["provider_user_id"],
        provider_user_login=row["provider_user_login"],
        metadata=json.loads(row["metadata_json"] or "{}"),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _artifact_from_row(row: sqlite3.Row | None) -> ArtifactRecord | None:
    if row is None:
        return None
    return ArtifactRecord(
        artifact_id=row["artifact_id"],
        user_id=row["user_id"],
        conversation_id=row["conversation_id"],
        workflow_id=row["workflow_id"],
        name=row["name"],
        mime_type=row["mime_type"],
        size_bytes=row["size_bytes"],
        path=row["path"],
        metadata=json.loads(row["metadata_json"] or "{}"),
        created_at=row["created_at"],
    )


def _safe_artifact_name(name: str) -> str:
    leaf_name = Path(name.strip()).name
    safe_name = re.sub(r"[^A-Za-z0-9._ -]+", "_", leaf_name).strip(" .")
    return safe_name[:160] or "artifact.txt"


def _now() -> str:
    return datetime.now(UTC).isoformat()
