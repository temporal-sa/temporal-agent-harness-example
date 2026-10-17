from __future__ import annotations

import hashlib
import mimetypes
from dataclasses import dataclass, field
from pathlib import PurePath
from typing import Any, Literal

from .store import AppStore, ArtifactRecord, artifact_expires_at

# A user's upload as the API describes it (attachment_dict).
AttachmentContentKind = Literal[
    "text",
    "code",
    "markdown",
    "image",
    "pdf",
    "binary",
    "paste",
]


@dataclass(frozen=True)
class AttachmentRef:
    attachment_id: str
    name: str
    mime_type: str
    size_bytes: int
    content_kind: AttachmentContentKind = "binary"
    source: str = "user_attachment"
    text_preview: str = ""
    text_chars: int | None = None
    expires_at: str = ""
    metadata: dict = field(default_factory=dict)


USER_ATTACHMENT_SOURCE = "user_attachment"
MAX_ATTACHMENT_BYTES = 10_000_000
MAX_PASTE_BYTES = 2_000_000
TEXT_PREVIEW_CHARS = 1_000
TEXT_ATTACHMENT_KINDS = frozenset({"text", "code", "markdown", "paste"})
MARKDOWN_SUFFIXES = frozenset({".md", ".markdown"})
CODE_SUFFIXES = frozenset(
    {
        ".c",
        ".cc",
        ".cpp",
        ".css",
        ".go",
        ".h",
        ".hpp",
        ".html",
        ".java",
        ".js",
        ".json",
        ".jsx",
        ".mermaid",
        ".mjs",
        ".mmd",
        ".py",
        ".rb",
        ".rs",
        ".sh",
        ".sql",
        ".toml",
        ".ts",
        ".tsx",
        ".xml",
        ".yaml",
        ".yml",
    }
)
TEXT_SUFFIXES = frozenset({".csv", ".env", ".graphql", ".ini", ".log", ".svg", ".txt"})

AttachmentUploadKind = Literal["file", "paste"]


class AttachmentValidationError(ValueError):
    pass


def create_user_attachment(
    store: AppStore,
    *,
    user_id: str,
    conversation_id: str,
    workflow_id: str,
    name: str,
    content: bytes,
    mime_type: str | None = None,
    upload_kind: AttachmentUploadKind = "file",
) -> ArtifactRecord:
    normalized_name = _safe_upload_name(name, upload_kind=upload_kind)
    content_kind, normalized_mime = sniff_attachment_text_kind(
        name=normalized_name,
        mime_type=mime_type or "",
        content=content,
        upload_kind=upload_kind,
    )
    max_bytes = MAX_PASTE_BYTES if upload_kind == "paste" else MAX_ATTACHMENT_BYTES
    if not content:
        raise AttachmentValidationError("Attachment content is empty.")
    if len(content) > max_bytes:
        raise AttachmentValidationError(
            f"Attachment is too large. Max bytes: {max_bytes}."
        )

    text_preview, text_chars = text_preview_for_bytes(
        content,
        content_kind=content_kind,
        mime_type=normalized_mime,
    )
    return store.create_artifact(
        user_id=user_id,
        conversation_id=conversation_id,
        workflow_id=workflow_id,
        name=normalized_name,
        mime_type=normalized_mime,
        content=content,
        metadata={
            "source": USER_ATTACHMENT_SOURCE,
            "upload_kind": upload_kind,
            "content_kind": content_kind,
            "sha256": hashlib.sha256(content).hexdigest(),
            "text_preview": text_preview,
            "text_chars": text_chars,
            "extraction_status": "ready" if text_chars is not None else "none",
        },
    )


def artifact_is_user_attachment(artifact: ArtifactRecord) -> bool:
    return artifact.metadata.get("source") == USER_ATTACHMENT_SOURCE


def attachment_ref_from_artifact(artifact: ArtifactRecord) -> AttachmentRef:
    metadata = dict(artifact.metadata or {})
    return AttachmentRef(
        attachment_id=artifact.artifact_id,
        name=artifact.name,
        mime_type=artifact.mime_type,
        size_bytes=artifact.size_bytes,
        content_kind=_metadata_content_kind(metadata),
        source=USER_ATTACHMENT_SOURCE,
        text_preview=str(metadata.get("text_preview") or ""),
        text_chars=(
            int(metadata["text_chars"])
            if metadata.get("text_chars") is not None
            else None
        ),
        expires_at=artifact_expires_at(artifact),
        metadata=metadata,
    )


def attachment_dict(artifact: ArtifactRecord) -> dict[str, Any]:
    ref = attachment_ref_from_artifact(artifact)
    return {
        "attachment_id": ref.attachment_id,
        "artifact_id": ref.attachment_id,
        "conversation_id": artifact.conversation_id,
        "workflow_id": artifact.workflow_id,
        "name": ref.name,
        "mime_type": ref.mime_type,
        "size_bytes": ref.size_bytes,
        "content_kind": ref.content_kind,
        "source": ref.source,
        "text_preview": ref.text_preview,
        "text_chars": ref.text_chars,
        "expires_at": ref.expires_at,
        "created_at": artifact.created_at,
        "metadata": ref.metadata,
        "view_url": f"/api/attachments/{ref.attachment_id}",
        "download_url": f"/api/attachments/{ref.attachment_id}/download",
    }


def detect_content_kind(
    *,
    name: str,
    mime_type: str,
    upload_kind: AttachmentUploadKind = "file",
) -> AttachmentContentKind:
    if upload_kind == "paste":
        return "paste"
    normalized_mime = mime_type.lower().split(";", 1)[0].strip()
    suffix = PurePath(name).suffix.lower()
    if normalized_mime == "application/pdf":
        return "pdf"
    if normalized_mime.startswith("image/"):
        return "image"
    if normalized_mime in {"text/markdown", "text/x-markdown"} or suffix in MARKDOWN_SUFFIXES:
        return "markdown"
    if suffix in CODE_SUFFIXES:
        return "code"
    if normalized_mime.startswith("text/") or normalized_mime in {
        "application/json",
        "application/ld+json",
        "application/javascript",
        "application/sql",
        "application/toml",
        "application/typescript",
        "application/xml",
        "application/yaml",
        "application/x-yaml",
    } or suffix in TEXT_SUFFIXES:
        return "text"
    return "binary"


def text_like_content_kind(kind: str | None) -> bool:
    return kind in TEXT_ATTACHMENT_KINDS


def sniff_attachment_text_kind(
    *,
    name: str,
    mime_type: str,
    content: bytes,
    upload_kind: AttachmentUploadKind = "file",
) -> tuple[AttachmentContentKind, str]:
    normalized_mime = _normalize_mime_type(mime_type, name, content)
    content_kind = detect_content_kind(
        name=name,
        mime_type=normalized_mime,
        upload_kind=upload_kind,
    )
    if text_like_content_kind(content_kind):
        return content_kind, normalized_mime
    if _looks_like_text(content):
        return "text", "text/plain"
    return content_kind, normalized_mime


def text_preview_for_bytes(
    content: bytes,
    *,
    content_kind: AttachmentContentKind,
    mime_type: str,
    max_chars: int = TEXT_PREVIEW_CHARS,
) -> tuple[str, int | None]:
    if not text_like_content_kind(content_kind):
        return "", None
    text = decode_text_attachment(content, mime_type=mime_type)
    normalized = " ".join(text.split())
    preview = normalized[:max_chars]
    return preview, len(text)


def decode_text_attachment(content: bytes, *, mime_type: str = "") -> str:
    encoding = "utf-8"
    for part in mime_type.split(";")[1:]:
        key, _, value = part.strip().partition("=")
        if key.lower() == "charset" and value:
            encoding = value.strip()
            break
    try:
        return content.decode(encoding)
    except (LookupError, UnicodeDecodeError):
        return content.decode("utf-8", errors="replace")


def _metadata_content_kind(metadata: dict[str, Any]) -> AttachmentContentKind:
    kind = metadata.get("content_kind")
    if kind in {
        "text",
        "code",
        "markdown",
        "image",
        "pdf",
        "binary",
        "paste",
    }:
        return kind
    return "binary"


def _safe_upload_name(name: str, *, upload_kind: AttachmentUploadKind) -> str:
    candidate = PurePath(name or "").name.strip()
    if not candidate:
        return "pasted-text.txt" if upload_kind == "paste" else "attachment"
    return candidate


def _normalize_mime_type(
    mime_type: str | None,
    name: str,
    content: bytes,
) -> str:
    candidate = (mime_type or "").strip() or None
    if not candidate or candidate == "application/octet-stream":
        guessed, _ = mimetypes.guess_type(name)
        candidate = guessed or candidate
    if not candidate or candidate == "application/octet-stream":
        if _looks_like_text(content):
            candidate = "text/plain"
        else:
            candidate = "application/octet-stream"
    return candidate


def _looks_like_text(content: bytes) -> bool:
    if not content:
        return False
    sample = content[:2048]
    if b"\x00" in sample:
        return False
    try:
        sample.decode("utf-8")
    except UnicodeDecodeError:
        return False
    return True
