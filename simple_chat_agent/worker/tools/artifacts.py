from __future__ import annotations

import base64
import re
from collections.abc import Callable
from typing import Any, Literal

from claude_harness.streaming import StreamContext
from claude_harness.tool_types import ToolType
from claude_harness.tools import ToolContext, ToolResult, tool

CREATE_ARTIFACT_TOOL = "create_artifact"
MAX_ARTIFACT_BYTES = 2_000_000

ArtifactEncoding = Literal["text", "base64"]


class ArtifactProvider:
    def __init__(
        self,
        *,
        user_ref: Callable[[], str | None],
        conversation_id: Callable[[], str | None],
        workflow_id: Callable[[], str],
    ) -> None:
        self._user_ref = user_ref
        self._conversation_id = conversation_id
        self._workflow_id = workflow_id

    @tool(
        name=CREATE_ARTIFACT_TOOL,
        description=(
            "Create a persistent file artifact that the user can view and "
            "download from the chat UI. Use this when the user asks you to "
            "write, save, export, or create a file. The Python sandbox cannot "
            "persist files; use this tool for durable file output. Pass text "
            "content directly with encoding='text', or pass base64 content "
            "with encoding='base64' for binary files. File name should be the name of the file only without path - paths are not supported."
        ),
        tool_type=ToolType.MUTATING,
        pre_guards=["mutating_tool_approval"],
    )
    async def create_artifact(
        self,
        ctx: ToolContext,
        name: str,
        content: str,
        mime_type: str = "text/plain",
        encoding: ArtifactEncoding = "text",
    ) -> ToolResult:
        user_ref = self._user_ref()
        conversation_id = self._conversation_id()
        if user_ref is None or conversation_id is None:
            return ToolResult(
                payload={"error": "Artifact identity context is not available."},
                error=True,
            )

        payload = await ctx.activity(
            _create_artifact_activity,
            args={
                "user_id": user_ref,
                "conversation_id": conversation_id,
                "workflow_id": self._workflow_id(),
                "name": name,
                "content": content,
                "mime_type": mime_type,
                "encoding": encoding,
            },
        )
        return ToolResult(payload=payload, error="error" in payload)


async def _create_artifact_activity(
    user_id: str,
    conversation_id: str,
    workflow_id: str,
    name: str,
    content: str,
    mime_type: str,
    encoding: ArtifactEncoding,
    *,
    stream: StreamContext,
) -> dict[str, Any]:
    await stream.emit(
        {
            "name": name,
            "mime_type": mime_type,
            "encoding": encoding,
            "chars": len(content),
        },
        kind="artifact_create_start",
    )

    try:
        artifact_bytes = _decode_content(content, encoding)
        _validate_artifact(name=name, mime_type=mime_type, content=artifact_bytes)
    except ValueError as err:
        payload = {"error": str(err), "type": "ArtifactValidationError"}
        await stream.emit(payload, kind="artifact_create_rejected")
        return payload

    from simple_chat_agent.common.store import AppStore

    artifact = AppStore().create_artifact(
        user_id=user_id,
        conversation_id=conversation_id,
        workflow_id=workflow_id,
        name=name,
        mime_type=mime_type,
        content=artifact_bytes,
        metadata={"encoding": encoding},
    )
    payload = {
        "artifact": {
            "artifact_id": artifact.artifact_id,
            "name": artifact.name,
            "mime_type": artifact.mime_type,
            "size_bytes": artifact.size_bytes,
            "view_url": f"/api/artifacts/{artifact.artifact_id}",
            "download_url": f"/api/artifacts/{artifact.artifact_id}/download",
            "created_at": artifact.created_at,
        }
    }
    await stream.emit(payload["artifact"], kind="artifact_create_complete")
    return payload


def _decode_content(content: str, encoding: ArtifactEncoding) -> bytes:
    if encoding == "text":
        return content.encode("utf-8")
    if encoding == "base64":
        try:
            return base64.b64decode(content, validate=True)
        except ValueError as err:
            raise ValueError("Artifact content is not valid base64.") from err
    raise ValueError(f"Unsupported artifact encoding: {encoding}")


def _validate_artifact(*, name: str, mime_type: str, content: bytes) -> None:
    if not name.strip():
        raise ValueError("Artifact name is required.")
    if "/" in name or "\\" in name:
        raise ValueError("Artifact name must be a file name, not a path.")
    if not _valid_mime_type(mime_type):
        raise ValueError("Artifact mime_type must be a valid MIME type.")
    if not content:
        raise ValueError("Artifact content is empty.")
    if len(content) > MAX_ARTIFACT_BYTES:
        raise ValueError(f"Artifact is too large. Max bytes: {MAX_ARTIFACT_BYTES}.")


def _valid_mime_type(mime_type: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z0-9.+_-]+/[A-Za-z0-9.+_-]+", mime_type))
