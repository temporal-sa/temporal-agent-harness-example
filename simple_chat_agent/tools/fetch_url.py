from __future__ import annotations

import asyncio
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from claude_harness.streaming import StreamContext
from claude_harness.tool_types import ToolType
from claude_harness.tools import ToolContext, ToolResult, tool


@tool(
    name="fetch_url",
    description=(
        "Fetch the text content at an http or https URL. Use this when the "
        "user asks about a specific web page or asks you to retrieve a URL."
    ),
    tool_type=ToolType.READ,
)
async def fetch_url(
    ctx: ToolContext,
    url: str,
    max_chars: int = 4000,
) -> ToolResult:
    payload = await ctx.activity(
        _fetch_url_activity,
        args={"url": url, "max_chars": max_chars},
    )
    return ToolResult(payload=payload, error="error" in payload)


async def _fetch_url_activity(
    url: str,
    max_chars: int = 4000,
    *,
    stream: StreamContext,
) -> dict[str, object]:
    await stream.emit({"url": url, "max_chars": max_chars}, kind="fetch_start")

    result = await asyncio.to_thread(_fetch_url_sync, url, max_chars)

    await stream.emit(
        {
            "url": result.get("final_url", result.get("url", url)),
            "status": result.get("status"),
            "error": result.get("error"),
            "truncated": result.get("truncated"),
        },
        kind="fetch_complete",
    )

    return result


def _fetch_url_sync(url: str, max_chars: int) -> dict[str, object]:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return {
            "error": "Only http and https URLs are supported.",
            "url": url,
        }
    if max_chars < 1:
        return {
            "error": "max_chars must be at least 1.",
            "url": url,
        }

    request = Request(
        url,
        headers={
            "User-Agent": "temporal-agent-harness-example/0.1",
            "Accept": "text/plain,text/html,application/json,*/*;q=0.8",
        },
    )

    try:
        with urlopen(request, timeout=10) as response:
            content_type = response.headers.get("content-type", "")
            raw = response.read(max_chars + 1)
            status = response.status
            final_url = response.url
    except HTTPError as err:
        return {
            "error": f"HTTP {err.code}: {err.reason}",
            "url": url,
        }
    except URLError as err:
        return {
            "error": str(err.reason),
            "url": url,
        }

    encoding = _encoding_from_content_type(content_type)
    text = raw[:max_chars].decode(encoding, errors="replace")

    return {
        "url": url,
        "final_url": final_url,
        "status": status,
        "content_type": content_type,
        "truncated": len(raw) > max_chars,
        "content": text,
    }


def _encoding_from_content_type(content_type: str) -> str:
    for part in content_type.split(";"):
        part = part.strip()
        if part.lower().startswith("charset="):
            return part.split("=", 1)[1].strip()
    return "utf-8"
