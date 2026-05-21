from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


McpAuthMode = Literal["none", "bearer", "oauth"]


@dataclass(frozen=True)
class HttpMcpToolConfig:
    name: str
    description: str
    input_schema: dict[str, Any]
    public_name: str | None = None


@dataclass(frozen=True)
class HttpMcpServerConfig:
    server_id: str
    label: str
    server_url: str
    tool_prefix: str
    auth_ref: str | None = None
    auth_mode: McpAuthMode = "none"
    enabled: bool = True
    tools: list[HttpMcpToolConfig] = field(default_factory=list)

    def public_tool_name(self, tool_name: str) -> str:
        from .mcp import public_mcp_tool_name

        return public_mcp_tool_name(self.tool_prefix, tool_name)
