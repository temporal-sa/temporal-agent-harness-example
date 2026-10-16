from __future__ import annotations

import asyncio
import json
import re
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urljoin, urlparse

from temporalio.exceptions import ApplicationError

from agent_harness.streaming import StreamContext
from agent_harness.tool_types import ToolType
from agent_harness.tools import ToolContext, ToolResult, tool


CHROME_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/149.0.7827.54 Safari/537.36"
)
DEFAULT_MAX_CHARS = 12000
MAX_MAX_CHARS = 50000
MAX_RAW_BYTES = 2_000_000
MAX_LINKS = 30
MAX_HEADINGS = 30


@tool(
    name="fetch_url",
    description=(
        "Fetch an http or https URL and return readable extracted page text, "
        "metadata, and useful links. Use this after search_web identifies a "
        "specific page or when the user asks you to retrieve a URL."
    ),
    tool_type=ToolType.READ,
)
async def fetch_url(
    ctx: ToolContext,
    url: str,
    max_chars: int = DEFAULT_MAX_CHARS,
) -> ToolResult:
    payload = await ctx.activity(
        _fetch_url_activity,
        args={"url": url, "max_chars": max_chars},
    )
    return ToolResult(payload=payload, error="error" in payload)


async def _fetch_url_activity(
    url: str,
    max_chars: int = DEFAULT_MAX_CHARS,
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
            "content_kind": result.get("content_kind"),
            "blocked_reason": result.get("blocked_reason"),
        },
        kind="fetch_complete",
    )

    status = int(result.get("status") or 0)
    if result.get("retryable") or status in {408, 409, 425, 429} or status >= 500:
        raise ApplicationError(
            str(result.get("error") or f"HTTP {status}: transient fetch failure"),
            type="FetchUrlRequestError",
        )

    return result


def _fetch_url_sync(url: str, max_chars: int) -> dict[str, object]:
    import httpx

    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return {
            "error": "Only http and https URLs are supported.",
            "url": url,
        }

    max_chars = _bounded_max_chars(max_chars)
    try:
        fetched = _http_fetch(url)
    except httpx.TimeoutException:
        return {
            "error": "Request timed out.",
            "url": url,
            "retryable": True,
        }
    except httpx.TooManyRedirects:
        return {
            "error": "Too many redirects.",
            "url": url,
        }
    except httpx.HTTPError as err:
        return {
            "error": str(err),
            "url": url,
            "retryable": True,
        }

    content_type = fetched["content_type"]
    status = int(fetched["status"])
    final_url = str(fetched["final_url"])
    raw = fetched["raw"]
    raw_truncated = bool(fetched["raw_truncated"])
    text = _decode_response(raw, content_type, fetched.get("encoding"))
    content_kind = _content_kind(content_type, text)

    base_payload: dict[str, object] = {
        "url": url,
        "final_url": final_url,
        "status": status,
        "reason_phrase": fetched.get("reason_phrase"),
        "content_type": content_type,
        "content_kind": content_kind,
        "raw_truncated": raw_truncated,
        "user_agent": CHROME_USER_AGENT,
    }

    blocked_reason = _blocked_reason(status, text)
    if content_kind == "unsupported":
        base_payload.update(
            {
                "error": f"Unsupported content type: {content_type or 'unknown'}",
                "blocked": False,
                "blocked_reason": None,
                "truncated": raw_truncated,
            }
        )
        return base_payload

    extracted = _extract_content(
        text=text,
        url=final_url,
        content_kind=content_kind,
    )
    extracted_content = str(extracted["content"])
    content = _truncate_text(extracted_content, max_chars)
    content_truncated = len(extracted_content) > len(content)
    js_required = _looks_javascript_required(content or text)

    payload = {
        **base_payload,
        "title": extracted.get("title"),
        "description": extracted.get("description"),
        "headings": extracted.get("headings", []),
        "links": extracted.get("links", []),
        "extraction_method": extracted.get("extraction_method"),
        "blocked": blocked_reason is not None or js_required,
        "blocked_reason": blocked_reason
        or ("javascript_required" if js_required else None),
        "truncated": raw_truncated or content_truncated,
        "content": content,
    }

    if status >= 400:
        payload["error"] = f"HTTP {status}: {fetched.get('reason_phrase') or 'error'}"
    elif js_required:
        payload["error"] = "Page appears to require JavaScript or browser verification."
    return payload


def _http_fetch(url: str) -> dict[str, Any]:
    import httpx

    timeout = httpx.Timeout(connect=5.0, read=15.0, write=5.0, pool=5.0)
    headers = {
        "User-Agent": CHROME_USER_AGENT,
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,"
            "text/plain;q=0.8,application/json;q=0.7,*/*;q=0.5"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Upgrade-Insecure-Requests": "1",
    }
    with httpx.Client(
        headers=headers,
        timeout=timeout,
        follow_redirects=True,
    ) as client:
        with client.stream("GET", url) as response:
            raw_parts: list[bytes] = []
            raw_size = 0
            raw_truncated = False
            for chunk in response.iter_bytes():
                if not chunk:
                    continue
                remaining = MAX_RAW_BYTES - raw_size
                if remaining <= 0:
                    raw_truncated = True
                    break
                if len(chunk) > remaining:
                    raw_parts.append(chunk[:remaining])
                    raw_size += remaining
                    raw_truncated = True
                    break
                raw_parts.append(chunk)
                raw_size += len(chunk)

            return {
                "status": response.status_code,
                "reason_phrase": response.reason_phrase,
                "final_url": str(response.url),
                "content_type": response.headers.get("content-type", ""),
                "encoding": response.encoding,
                "raw": b"".join(raw_parts),
                "raw_truncated": raw_truncated,
            }


def _decode_response(
    raw: bytes,
    content_type: str,
    encoding: str | None,
) -> str:
    preferred_encoding = encoding or _encoding_from_content_type(content_type)
    try:
        return raw.decode(preferred_encoding, errors="replace")
    except LookupError:
        return raw.decode("utf-8", errors="replace")


def _content_kind(content_type: str, text: str) -> str:
    lower_type = content_type.lower()
    stripped = text.lstrip()[:200].lower()
    if "text/html" in lower_type or "application/xhtml+xml" in lower_type:
        return "html"
    if stripped.startswith("<!doctype html") or stripped.startswith("<html"):
        return "html"
    if "application/json" in lower_type or lower_type.endswith("+json"):
        return "json"
    if stripped.startswith("{") or stripped.startswith("["):
        return "json"
    if (
        lower_type.startswith("text/")
        or "application/xml" in lower_type
        or lower_type.endswith("+xml")
        or not lower_type
    ):
        return "text"
    return "unsupported"


def _extract_content(
    *,
    text: str,
    url: str,
    content_kind: str,
) -> dict[str, object]:
    if content_kind == "html":
        import trafilatura

        metadata = _parse_html_metadata(text, url)
        extracted = trafilatura.extract(
            text,
            url=url,
            include_comments=False,
            include_tables=True,
            include_images=False,
            include_links=False,
            deduplicate=True,
            favor_recall=True,
        )
        if extracted:
            content = _normalize_text(extracted)
            method = "trafilatura"
        else:
            content = metadata["fallback_text"]
            method = "html_parser"
        return {
            "content": content,
            "title": metadata["title"],
            "description": metadata["description"],
            "headings": metadata["headings"],
            "links": metadata["links"],
            "extraction_method": method,
        }

    if content_kind == "json":
        content = _format_json(text)
        return {
            "content": content,
            "title": None,
            "description": None,
            "headings": [],
            "links": [],
            "extraction_method": "json",
        }

    return {
        "content": _normalize_text(text),
        "title": None,
        "description": None,
        "headings": [],
        "links": [],
        "extraction_method": "text",
    }


class _HTMLMetadataParser(HTMLParser):
    def __init__(self, base_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.base_url = base_url
        self.title: str | None = None
        self.description: str | None = None
        self.headings: list[str] = []
        self.links: list[dict[str, str]] = []
        self._text_parts: list[str] = []
        self._skip_depth = 0
        self._capture_title = False
        self._capture_heading: str | None = None
        self._heading_parts: list[str] = []
        self._active_link: dict[str, str] | None = None
        self._active_link_parts: list[str] = []

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        tag = tag.lower()
        attrs_dict = {key.lower(): value or "" for key, value in attrs}
        if tag in {"script", "style", "noscript", "svg", "canvas"}:
            self._skip_depth += 1
            return
        if self._skip_depth:
            return
        if tag == "title":
            self._capture_title = True
        elif tag == "meta":
            name = (attrs_dict.get("name") or attrs_dict.get("property") or "").lower()
            if name in {"description", "og:description", "twitter:description"}:
                content = attrs_dict.get("content", "").strip()
                if content and self.description is None:
                    self.description = _normalize_inline(content)
        elif tag in {"h1", "h2", "h3"}:
            self._capture_heading = tag
            self._heading_parts = []
            self._append_block_break()
        elif tag == "a":
            href = attrs_dict.get("href", "").strip()
            if href and _is_useful_href(href):
                self._active_link = {"url": urljoin(self.base_url, href)}
                self._active_link_parts = []
        elif tag == "br":
            self._text_parts.append("\n")
        elif tag == "li":
            self._text_parts.append("\n- ")
        elif tag in _BLOCK_TAGS:
            self._append_block_break()

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag in {"script", "style", "noscript", "svg", "canvas"}:
            self._skip_depth = max(0, self._skip_depth - 1)
            return
        if self._skip_depth:
            return
        if tag == "title":
            self._capture_title = False
        elif tag in {"h1", "h2", "h3"} and self._capture_heading == tag:
            heading = _normalize_inline(" ".join(self._heading_parts))
            if heading and heading not in self.headings:
                self.headings.append(heading)
            self._capture_heading = None
            self._heading_parts = []
            self._append_block_break()
        elif tag == "a" and self._active_link is not None:
            label = _normalize_inline(" ".join(self._active_link_parts))
            if label and len(self.links) < MAX_LINKS:
                link = {**self._active_link, "text": label[:200]}
                if link["url"] not in {existing["url"] for existing in self.links}:
                    self.links.append(link)
            self._active_link = None
            self._active_link_parts = []
        elif tag in _BLOCK_TAGS:
            self._append_block_break()

    def handle_data(self, data: str) -> None:
        if self._skip_depth:
            return
        if self._capture_title:
            title = _normalize_inline(data)
            if title:
                self.title = (self.title + " " + title) if self.title else title
        if self._capture_heading:
            self._heading_parts.append(data)
        if self._active_link is not None:
            self._active_link_parts.append(data)
        self._text_parts.append(data)

    def fallback_text(self) -> str:
        return _normalize_text("".join(self._text_parts))

    def _append_block_break(self) -> None:
        if not self._text_parts or self._text_parts[-1] != "\n":
            self._text_parts.append("\n")


_BLOCK_TAGS = {
    "address",
    "article",
    "aside",
    "blockquote",
    "dd",
    "div",
    "dl",
    "dt",
    "fieldset",
    "figcaption",
    "figure",
    "footer",
    "form",
    "header",
    "hr",
    "main",
    "nav",
    "ol",
    "p",
    "pre",
    "section",
    "table",
    "tbody",
    "td",
    "tfoot",
    "th",
    "thead",
    "tr",
    "ul",
}


def _parse_html_metadata(text: str, url: str) -> dict[str, object]:
    parser = _HTMLMetadataParser(url)
    try:
        parser.feed(text)
        parser.close()
    except Exception:
        pass
    return {
        "title": _normalize_inline(parser.title or "") or None,
        "description": parser.description,
        "headings": parser.headings[:MAX_HEADINGS],
        "links": parser.links[:MAX_LINKS],
        "fallback_text": parser.fallback_text(),
    }


def _encoding_from_content_type(content_type: str) -> str:
    for part in content_type.split(";"):
        part = part.strip()
        if part.lower().startswith("charset="):
            return part.split("=", 1)[1].strip()
    return "utf-8"


def _format_json(text: str) -> str:
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return _normalize_text(text)
    return json.dumps(parsed, indent=2, ensure_ascii=False)


def _normalize_text(text: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    lines = [_normalize_inline(line) for line in text.split("\n")]
    normalized: list[str] = []
    previous_blank = False
    for line in lines:
        if not line:
            if normalized and not previous_blank:
                normalized.append("")
            previous_blank = True
            continue
        normalized.append(line)
        previous_blank = False
    return "\n".join(normalized).strip()


def _normalize_inline(text: str) -> str:
    return re.sub(r"[ \t\f\v]+", " ", text).strip()


def _truncate_text(text: object, max_chars: int) -> str:
    content = str(text or "")
    if len(content) <= max_chars:
        return content
    return content[:max_chars].rstrip()


def _bounded_max_chars(value: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = DEFAULT_MAX_CHARS
    return max(1, min(parsed, MAX_MAX_CHARS))


def _blocked_reason(status: int, text: str) -> str | None:
    if status in {401, 403}:
        return "forbidden"
    if status == 407:
        return "proxy_authentication_required"
    if status == 408:
        return "request_timeout"
    if status == 409:
        return "conflict"
    if status == 423:
        return "locked"
    if status == 425:
        return "too_early"
    if status == 429:
        return "rate_limited"
    if status == 451:
        return "legal_restriction"
    lower = text[:5000].lower()
    if "access denied" in lower or "request blocked" in lower:
        return "access_denied"
    if "checking your browser" in lower or "verify you are human" in lower:
        return "browser_verification"
    if "captcha" in lower or "cf-challenge" in lower:
        return "bot_challenge"
    return None


def _looks_javascript_required(text: str) -> bool:
    lower = text[:5000].lower()
    patterns = (
        "enable javascript",
        "requires javascript",
        "please turn javascript on",
        "you need to enable javascript",
        "javascript is disabled",
        "checking your browser",
        "verify you are human",
    )
    return any(pattern in lower for pattern in patterns)


def _is_useful_href(href: str) -> bool:
    parsed = urlparse(href)
    if parsed.scheme and parsed.scheme not in {"http", "https"}:
        return False
    return not href.startswith(("#", "javascript:", "mailto:", "tel:"))
