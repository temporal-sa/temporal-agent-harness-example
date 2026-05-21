from __future__ import annotations

import inspect
import re
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, cast

from .tools import ToolContext, ToolResult, ToolSet
from .tool_types import ToolType
from .mcp_types import HttpMcpServerConfig, HttpMcpToolConfig


McpAuthResolver = Callable[[str], Mapping[str, str] | Awaitable[Mapping[str, str]]]
McpHttpAuthResolver = Callable[[str, str], Any | Awaitable[Any | None]]


class HttpMcpProvider:
    def __init__(self, config: HttpMcpServerConfig) -> None:
        self._config = config
        self._tool_name_by_public_name: dict[str, str] = {}

    @property
    def config(self) -> HttpMcpServerConfig:
        return self._config

    def register(self, tools: ToolSet) -> None:
        if not self._config.enabled:
            return

        for mcp_tool in self._config.tools:
            public_name = mcp_tool.public_name or self._config.public_tool_name(
                mcp_tool.name
            )
            self._tool_name_by_public_name[public_name] = mcp_tool.name
            tools.add_dynamic_tool(
                name=public_name,
                description=_mcp_tool_description(self._config, mcp_tool),
                input_schema=mcp_tool.input_schema,
                tool_type=ToolType.READ,
                fn=self._make_tool_runner(public_name),
            )

    def tool_names(self) -> list[str]:
        if not self._config.enabled:
            return []
        return [
            tool.public_name or self._config.public_tool_name(tool.name)
            for tool in self._config.tools
        ]

    def _make_tool_runner(self, public_name: str):
        async def run_mcp_tool(
            ctx: ToolContext, args: dict[str, Any]
        ) -> ToolResult:
            mcp_tool_name = self._tool_name_by_public_name[public_name]
            payload = await ctx.activity(
                call_http_mcp_tool,
                step=public_name,
                args={
                    "server_id": self._config.server_id,
                    "server_url": self._config.server_url,
                    "auth_ref": self._config.auth_ref,
                    "tool_name": mcp_tool_name,
                    "arguments": args,
                },
            )
            return ToolResult(payload=payload, error=bool(payload.get("is_error")))

        return run_mcp_tool


async def discover_http_mcp_tools(
    *,
    server_url: str,
    tool_prefix: str,
    auth_ref: str | None = None,
    auth_headers: Mapping[str, str] | None = None,
    http_auth: Any | None = None,
) -> list[HttpMcpToolConfig]:
    tools = await _list_http_mcp_tools(
        server_url=server_url,
        auth_ref=auth_ref,
        auth_headers=auth_headers,
        http_auth=http_auth,
    )
    return [
        HttpMcpToolConfig(
            name=tool["name"],
            description=tool.get("description") or f"MCP tool {tool['name']}",
            input_schema=cast(dict[str, Any], tool.get("input_schema") or {}),
            public_name=public_mcp_tool_name(tool_prefix, tool["name"]),
        )
        for tool in tools
    ]


async def call_http_mcp_tool(
    server_id: str,
    server_url: str,
    auth_ref: str | None,
    tool_name: str,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    result = await _call_http_mcp_tool(
        server_url=server_url,
        auth_ref=auth_ref,
        tool_name=tool_name,
        arguments=arguments,
    )
    result["server_id"] = server_id
    result["tool_name"] = tool_name
    return result


def configure_mcp_auth_resolver(resolver: McpAuthResolver | None) -> None:
    global _mcp_auth_resolver
    _mcp_auth_resolver = resolver


def configure_mcp_http_auth_resolver(resolver: McpHttpAuthResolver | None) -> None:
    global _mcp_http_auth_resolver
    _mcp_http_auth_resolver = resolver


def public_mcp_tool_name(tool_prefix: str, tool_name: str) -> str:
    prefix = _sanitize_tool_name(tool_prefix)
    name = _sanitize_tool_name(tool_name)
    public_name = f"{prefix}__{name}" if prefix else name
    return public_name[:64]


async def _list_http_mcp_tools(
    *,
    server_url: str,
    auth_ref: str | None,
    auth_headers: Mapping[str, str] | None = None,
    http_auth: Any | None = None,
) -> list[dict[str, Any]]:
    from mcp import ClientSession
    from mcp.client.streamable_http import (
        create_mcp_http_client,
        streamable_http_client,
    )

    resolved_http_auth = http_auth or await _http_auth_for_mcp(auth_ref, server_url)
    headers = {} if resolved_http_auth is not None else await _headers_for_mcp(
        auth_ref, auth_headers
    )
    async with create_mcp_http_client(
        headers=dict(headers),
        auth=resolved_http_auth,
    ) as http_client:
        async with streamable_http_client(
            server_url,
            http_client=http_client,
        ) as (read_stream, write_stream, _get_session_id):
            async with ClientSession(read_stream, write_stream) as session:
                await session.initialize()
                response = await session.list_tools()

    return [
        {
            "name": tool.name,
            "description": tool.description or "",
            "input_schema": dict(tool.inputSchema),
        }
        for tool in response.tools
    ]


async def _call_http_mcp_tool(
    *,
    server_url: str,
    auth_ref: str | None,
    tool_name: str,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    from mcp import ClientSession
    from mcp.client.streamable_http import (
        create_mcp_http_client,
        streamable_http_client,
    )

    http_auth = await _http_auth_for_mcp(auth_ref, server_url)
    headers = {} if http_auth is not None else await _headers_for_mcp(auth_ref)
    async with create_mcp_http_client(
        headers=dict(headers),
        auth=http_auth,
    ) as http_client:
        async with streamable_http_client(
            server_url,
            http_client=http_client,
        ) as (read_stream, write_stream, _get_session_id):
            async with ClientSession(read_stream, write_stream) as session:
                await session.initialize()
                result = await session.call_tool(tool_name, arguments=arguments)

    return {
        "content": [block.model_dump(by_alias=True) for block in result.content],
        "structured_content": result.structuredContent,
        "is_error": result.isError,
        "meta": result.meta,
    }


async def _headers_for_mcp(
    auth_ref: str | None,
    explicit_headers: Mapping[str, str] | None = None,
) -> Mapping[str, str]:
    if explicit_headers is not None:
        return explicit_headers
    if auth_ref is None:
        return {}
    if _mcp_auth_resolver is None:
        raise RuntimeError("No MCP auth resolver configured for HTTP MCP auth_ref")

    headers = _mcp_auth_resolver(auth_ref)
    if inspect.isawaitable(headers):
        headers = await headers
    return headers


async def _http_auth_for_mcp(auth_ref: str | None, server_url: str) -> Any | None:
    if auth_ref is None or _mcp_http_auth_resolver is None:
        return None

    auth = _mcp_http_auth_resolver(auth_ref, server_url)
    if inspect.isawaitable(auth):
        auth = await auth
    return auth


def _mcp_tool_description(
    server: HttpMcpServerConfig,
    tool: HttpMcpToolConfig,
) -> str:
    description = tool.description or f"MCP tool {tool.name}"
    return f"{description}\n\nMCP server: {server.label} ({server.server_id})."


def _sanitize_tool_name(value: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9_-]+", "_", value.strip())
    sanitized = sanitized.strip("_-")
    return sanitized or "mcp"


_mcp_auth_resolver: McpAuthResolver | None = None
_mcp_http_auth_resolver: McpHttpAuthResolver | None = None
