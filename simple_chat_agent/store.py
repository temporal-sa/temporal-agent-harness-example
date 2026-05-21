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

    def delete_oauth_connection(self, *, user_id: str, provider: str) -> None:
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
