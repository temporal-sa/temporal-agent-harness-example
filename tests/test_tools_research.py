"""Research tools (PRD A6): fetch_url and search_web on the harness. No live internet
unless RUN_INTEGRATION=1."""

from __future__ import annotations

import ast
import inspect
import json
import os
import unittest
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
from temporal_agent_harness.harness.agent import tool_activity
from temporalio.exceptions import ApplicationError

from simple_chat_agent.agent import net_guard
from simple_chat_agent.agent.tools import research
from simple_chat_agent.agent.tools.research import (
    FETCH_URL_CONFIG,
    GOOGLE_SEARCH_GROUNDING_API_KEY,
    RESEARCH_TOOLS,
    SEARCH_WEB_CONFIG,
    SEARXNG_BASE_URL,
    FetchUrlResult,
    SearchWebResult,
    _fetch_url,
    _google_grounding_payload,
    _raise_research_request_error,
    _search_web,
    fetch_url,
    research_tools_available,
    search_web,
)

GUARDED_GET = "simple_chat_agent.agent.tools.research.net_guard.guarded_get"


def _response(
    status: int = 200,
    body: bytes | str = b"",
    content_type: str = "text/html; charset=utf-8",
    url: str = "https://example.com/page",
    truncated: bool = False,
) -> net_guard.GuardedResponse:
    if isinstance(body, str):
        body = body.encode()
    return net_guard.GuardedResponse(
        url=url,
        status_code=status,
        reason_phrase=httpx.codes.get_reason_phrase(status),
        headers={"content-type": content_type},
        content=body,
        truncated=truncated,
        peer_ip="93.184.215.14",
    )


class ToolDefinitionTests(unittest.TestCase):
    def test_exports_and_activity_names(self) -> None:
        self.assertEqual(RESEARCH_TOOLS, (fetch_url, search_web))
        names = [tool_activity(t).__name__ for t in RESEARCH_TOOLS]
        self.assertEqual(names, ["fetch_url", "search_web"])

    def test_network_risk_class_in_source(self) -> None:
        # Both tools are network class: not inherently safe, judged as "network".
        tree = ast.parse(inspect.getsource(research))
        found = {}
        for node in tree.body:
            if isinstance(node, ast.AsyncFunctionDef) and node.name in (
                "fetch_url",
                "search_web",
            ):
                (decorator,) = node.decorator_list
                kwargs = {kw.arg: kw.value for kw in decorator.keywords}
                found[node.name] = (
                    ast.literal_eval(kwargs["inherently_safe"]),
                    kwargs["auto_approval_criteria"].id,
                )
        self.assertEqual(
            found,
            {
                "fetch_url": (False, "NETWORK_CRITERIA"),
                "search_web": (False, "NETWORK_CRITERIA"),
            },
        )
        self.assertEqual(research.NETWORK_CRITERIA, "network")

    def test_retry_policies_are_read_class(self) -> None:
        for config in (FETCH_URL_CONFIG, SEARCH_WEB_CONFIG):
            policy = config["retry_policy"]
            self.assertEqual(policy.maximum_attempts, 5)
            self.assertIn("BlockedDestination", policy.non_retryable_error_types or [])
            self.assertLessEqual(config["start_to_close_timeout"], timedelta(minutes=2))

    def test_availability_follows_backend_config(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(research_tools_available(), (fetch_url,))
        with patch.dict(os.environ, {GOOGLE_SEARCH_GROUNDING_API_KEY: "k"}, clear=True):
            self.assertEqual(research_tools_available(), (fetch_url, search_web))
        with patch.dict(
            os.environ, {SEARXNG_BASE_URL: "http://searxng:8080"}, clear=True
        ):
            self.assertEqual(research_tools_available(), (fetch_url, search_web))
        with patch.dict(os.environ, {SEARXNG_BASE_URL: "  "}, clear=True):
            self.assertEqual(research_tools_available(), (fetch_url,))


class FetchUrlRetryTests(unittest.IsolatedAsyncioTestCase):
    async def test_transient_http_statuses_are_retryable(self) -> None:
        for status in (408, 409, 425, 429, 500, 502, 503, 504):
            with (
                self.subTest(status=status),
                patch(GUARDED_GET, new=AsyncMock(return_value=_response(status))),
                self.assertRaises(ApplicationError) as raised,
            ):
                await _fetch_url("https://example.com")
            self.assertEqual(raised.exception.type, "FetchUrlRequestError")
            self.assertFalse(raised.exception.non_retryable)
            self.assertIn(f"HTTP {status}", str(raised.exception))

    async def test_404_is_returned_as_data(self) -> None:
        with patch(
            GUARDED_GET, new=AsyncMock(return_value=_response(404, "<p>Not here</p>"))
        ):
            result = await _fetch_url("https://example.com/missing")
        self.assertEqual(result.status, 404)
        self.assertEqual(result.error, "HTTP 404: Not Found")

    async def test_transport_failures_are_retryable(self) -> None:
        for exc, message in (
            (httpx.ReadTimeout("slow"), "Request timed out."),
            (httpx.ConnectError("refused"), "refused"),
            (
                net_guard.DestinationUnresolvable(
                    "temporary", url="u", host="h", retryable=True
                ),
                "temporary",
            ),
        ):
            with (
                self.subTest(exc=type(exc).__name__),
                patch(GUARDED_GET, new=AsyncMock(side_effect=exc)),
                self.assertRaises(ApplicationError) as raised,
            ):
                await _fetch_url("https://example.com")
            self.assertEqual(raised.exception.type, "FetchUrlRequestError")
            self.assertFalse(raised.exception.non_retryable)
            self.assertIn(message, str(raised.exception))

    async def test_permanent_failures_come_back_as_data(self) -> None:
        for exc, message in (
            (
                net_guard.TooManyRedirects("x", url="u", redirects=[]),
                "Too many redirects.",
            ),
            (
                net_guard.DestinationUnresolvable(
                    "Could not resolve nope.example", url="u", host="h", retryable=False
                ),
                "Could not resolve nope.example",
            ),
        ):
            with (
                self.subTest(exc=type(exc).__name__),
                patch(GUARDED_GET, new=AsyncMock(side_effect=exc)),
            ):
                result = await _fetch_url("https://example.com")
            self.assertEqual(result.error, message)

    async def test_guard_refusal_is_non_retryable_tool_failure(self) -> None:
        # Real guard, no mocks: literal internal addresses never touch the network.
        for url in (
            "http://169.254.169.254/latest/meta-data/iam/security-credentials/",
            "http://127.0.0.1/",
            "http://[::ffff:127.0.0.1]/",
            "http://10.1.2.3/",
            "http://100.64.0.1/",
            "http://[fe80::1]/",
            "file:///etc/passwd",
            "https://example.com:8443/",
        ):
            with self.subTest(url=url), self.assertRaises(ApplicationError) as raised:
                await _fetch_url(url)
            self.assertEqual(raised.exception.type, "BlockedDestination")
            self.assertTrue(raised.exception.non_retryable)

    async def test_redirect_to_metadata_is_refused(self) -> None:
        def factory(host: str, port: int, ip: str) -> httpx.AsyncBaseTransport:
            return httpx.MockTransport(
                lambda request: httpx.Response(
                    302,
                    headers={"Location": "http://169.254.169.254/latest/meta-data/"},
                )
            )

        real_get = net_guard.guarded_get

        async def guarded(url, **kwargs):
            return await real_get(url, transport_factory=factory, **kwargs)

        with (
            patch.object(net_guard, "_resolve", lambda host, port: ["93.184.215.14"]),
            patch(GUARDED_GET, new=guarded),
            self.assertRaises(ApplicationError) as raised,
        ):
            await _fetch_url("https://attacker.example/")
        self.assertEqual(raised.exception.type, "BlockedDestination")
        self.assertIn("metadata", str(raised.exception))


ARTICLE = """<!doctype html>
<html><head>
  <title>Shark Study Results</title>
  <meta name="description" content="A summary of the 2026 shark migration study.">
</head><body>
  <nav><a href="/">Home</a> <a href="javascript:void(0)">menu</a></nav>
  <article>
    <h1>Shark migration</h1>
    <p>Researchers tracked two hundred great white sharks across the Pacific Ocean
    for three years and found that the animals return to the same feeding grounds.</p>
    <h2>Methods</h2>
    <p>Satellite tags recorded position, depth and water temperature every hour,
    giving the team more than four million data points to analyse.</p>
    <p>Read the <a href="https://example.org/paper">full paper</a>.</p>
  </article>
  <script>var tracking = "do not include";</script>
</body></html>"""


class FetchUrlExtractionTests(unittest.IsolatedAsyncioTestCase):
    async def _fetch(self, response: net_guard.GuardedResponse, max_chars: int = 12000):
        with patch(GUARDED_GET, new=AsyncMock(return_value=response)) as guarded:
            result = await _fetch_url("https://example.com/page", max_chars)
        guarded.assert_awaited_once()
        self.assertIsInstance(result, FetchUrlResult)
        return result

    async def test_html_article(self) -> None:
        result = await self._fetch(_response(200, ARTICLE))
        self.assertEqual(result.title, "Shark Study Results")
        self.assertEqual(
            result.description, "A summary of the 2026 shark migration study."
        )
        self.assertEqual(result.headings, ["Shark migration", "Methods"])
        self.assertEqual(result.content_kind, "html")
        self.assertIn(result.extraction_method, ("trafilatura", "html_parser"))
        self.assertIn("great white sharks", result.content)
        self.assertNotIn("do not include", result.content)
        urls = {link.url: link.title for link in result.links}
        self.assertEqual(urls.get("https://example.org/paper"), "full paper")
        self.assertEqual(urls.get("https://example.com/"), "Home")
        self.assertFalse(any(u.startswith("javascript:") for u in urls))
        self.assertFalse(result.blocked)
        self.assertIsNone(result.error)

    async def test_html_parser_fallback_when_trafilatura_finds_nothing(self) -> None:
        with patch("trafilatura.extract", return_value=None):
            result = await self._fetch(_response(200, ARTICLE))
        self.assertEqual(result.extraction_method, "html_parser")
        self.assertIn("great white sharks", result.content)

    async def test_json_is_pretty_printed(self) -> None:
        result = await self._fetch(
            _response(
                200, '{"name":"shark","count":2}', content_type="application/json"
            )
        )
        self.assertEqual(result.content_kind, "json")
        self.assertEqual(result.extraction_method, "json")
        self.assertEqual(
            result.content, json.dumps({"name": "shark", "count": 2}, indent=2)
        )

    async def test_json_detected_without_content_type(self) -> None:
        result = await self._fetch(_response(200, "[1, 2]", content_type=""))
        self.assertEqual(result.content_kind, "json")

    async def test_bot_challenge_is_flagged(self) -> None:
        page = (
            "<html><body><p>Please complete the CAPTCHA to continue.</p></body></html>"
        )
        result = await self._fetch(_response(403, page))
        self.assertTrue(result.blocked)
        self.assertEqual(result.blocked_reason, "forbidden")
        self.assertEqual(result.error, "HTTP 403: Forbidden")

        result = await self._fetch(_response(200, page))
        self.assertTrue(result.blocked)
        self.assertEqual(result.blocked_reason, "bot_challenge")

    async def test_javascript_required_is_flagged(self) -> None:
        page = "<html><body><noscript></noscript><p>You need to enable JavaScript to run this app.</p></body></html>"
        result = await self._fetch(_response(200, page))
        self.assertTrue(result.blocked)
        self.assertEqual(result.blocked_reason, "javascript_required")
        self.assertIn("JavaScript", result.error or "")

    async def test_unsupported_content_type(self) -> None:
        result = await self._fetch(_response(200, b"\x89PNG", content_type="image/png"))
        self.assertEqual(result.content_kind, "unsupported")
        self.assertEqual(result.error, "Unsupported content type: image/png")
        self.assertEqual(result.content, "")

    async def test_content_is_bounded_by_max_chars(self) -> None:
        text = "word " * 5000
        result = await self._fetch(
            _response(200, text, content_type="text/plain"), max_chars=500
        )
        self.assertLessEqual(len(result.content), 500)
        self.assertTrue(result.truncated)
        # Out-of-range values are clamped, not trusted.
        result = await self._fetch(
            _response(200, "x" * 60000, content_type="text/plain"), max_chars=10**9
        )
        self.assertEqual(len(result.content), research.MAX_MAX_CHARS)

    async def test_raw_truncation_is_reported(self) -> None:
        result = await self._fetch(
            _response(200, "short", content_type="text/plain", truncated=True)
        )
        self.assertTrue(result.truncated)

    async def test_charset_is_honoured(self) -> None:
        body = "café".encode("latin-1")
        result = await self._fetch(
            _response(200, body, content_type="text/plain; charset=latin-1")
        )
        self.assertEqual(result.content, "café")

    async def test_str_is_compact(self) -> None:
        many_links = "".join(f'<a href="/l{i}">link {i}</a>' for i in range(40))
        result = await self._fetch(
            _response(200, ARTICLE.replace("<nav>", f"<nav>{many_links}"))
        )
        text = str(result)
        self.assertTrue(text.startswith("https://example.com/page (HTTP 200, html)"))
        self.assertIn("Title: Shark Study Results", text)
        self.assertIn("Links:", text)
        self.assertLessEqual(text.count("\n- "), research._STR_LINKS + 1)
        self.assertLessEqual(len(result.links), research.MAX_LINKS)
        self.assertLess(len(text), len(result.content) + 2000)


class SearchWebTests(unittest.IsolatedAsyncioTestCase):
    async def test_all_primary_engine_failures_fall_back_to_bing(self) -> None:
        primary = {
            "results": [],
            "suggestions": [],
            "unresponsive_engines": [
                ["brave", "too many requests"],
                ["google", "CAPTCHA"],
            ],
        }
        bing = {
            "results": [
                {
                    "title": "Shark study",
                    "url": "https://example.com/study",
                    "content": "A recent study",
                    "engine": "bing",
                }
            ],
            "suggestions": [],
            "unresponsive_engines": [],
        }
        with (
            patch.dict(os.environ, {SEARXNG_BASE_URL: "http://searxng"}, clear=True),
            patch.object(
                research, "_get_json", new=AsyncMock(side_effect=[primary, bing])
            ) as get,
        ):
            result = await _search_web("recent shark research")

        self.assertIsInstance(result, SearchWebResult)
        self.assertEqual(len(result.sources), 1)
        self.assertEqual(result.sources[0].snippet, "A recent study")
        self.assertEqual(result.fallback_engine, "bing")
        self.assertEqual(get.await_count, 2)
        self.assertEqual(get.await_args_list[0].args[0], "http://searxng/search")
        self.assertNotIn("engines", get.await_args_list[0].args[1])
        self.assertEqual(get.await_args_list[1].args[1]["engines"], "bing")

    async def test_bing_failure_does_not_rapidly_retry_blocked_engines(self) -> None:
        primary = {
            "results": [],
            "unresponsive_engines": [["brave", "too many requests"]],
        }
        bing = {"results": [], "unresponsive_engines": [["bing", "CAPTCHA"]]}
        with (
            patch.dict(os.environ, {SEARXNG_BASE_URL: "http://searxng"}, clear=True),
            patch.object(
                research, "_get_json", new=AsyncMock(side_effect=[primary, bing])
            ) as get,
            self.assertRaises(ApplicationError) as raised,
        ):
            await _search_web("recent shark research")
        self.assertEqual(raised.exception.type, "SearchEnginesUnavailable")
        self.assertTrue(raised.exception.non_retryable)
        self.assertEqual(get.await_count, 2)

    async def test_partial_searxng_results_do_not_fail(self) -> None:
        response = {
            "results": [
                {
                    "title": "Shark study",
                    "url": "https://example.com/study",
                    "content": "x",
                },
                {"title": "No url"},
            ],
            "suggestions": ["sharks 2026"],
            "unresponsive_engines": [["google", "too many requests"]],
        }
        with (
            patch.dict(os.environ, {SEARXNG_BASE_URL: "http://searxng"}, clear=True),
            patch.object(research, "_get_json", new=AsyncMock(return_value=response)),
        ):
            result = await _search_web("recent shark research")
        self.assertEqual(len(result.sources), 1)
        self.assertEqual(result.engine_errors, ["google: too many requests"])
        self.assertIsNone(result.fallback_engine)
        self.assertEqual(result.backend, "searxng")
        self.assertEqual(result.suggestions, ["sharks 2026"])

    async def test_max_results_is_clamped(self) -> None:
        response = {
            "results": [
                {"title": f"r{i}", "url": f"https://e.com/{i}"} for i in range(50)
            ]
        }
        with (
            patch.dict(os.environ, {SEARXNG_BASE_URL: "http://searxng"}, clear=True),
            patch.object(research, "_get_json", new=AsyncMock(return_value=response)),
        ):
            self.assertEqual(len((await _search_web("q", 3)).sources), 3)
            self.assertEqual(
                len((await _search_web("q", 999)).sources), research.MAX_SEARCH_RESULTS
            )

    async def test_searxng_http_errors_are_classified(self) -> None:
        for error, retryable in (
            ({"error": "HTTP 429: Too Many Requests"}, True),
            ({"error": "HTTP 502: Bad Gateway"}, True),
            ({"error": "Request timed out."}, True),
            ({"error": "All connection attempts failed"}, True),
            ({"error": "HTTP 403: Forbidden"}, False),
        ):
            with (
                self.subTest(error=error),
                patch.dict(
                    os.environ, {SEARXNG_BASE_URL: "http://searxng"}, clear=True
                ),
                patch.object(research, "_get_json", new=AsyncMock(return_value=error)),
                self.assertRaises(ApplicationError) as raised,
            ):
                await _search_web("q")
            self.assertEqual(raised.exception.type, "ResearchRequestError")
            self.assertEqual(raised.exception.non_retryable, not retryable)

    async def test_google_grounding_replaces_searxng_when_configured(self) -> None:
        grounded = SearchWebResult(query="q", backend="google_search_grounding")
        with (
            patch.dict(
                os.environ,
                {
                    GOOGLE_SEARCH_GROUNDING_API_KEY: "k",
                    SEARXNG_BASE_URL: "http://searxng",
                },
                clear=True,
            ),
            patch.object(
                research,
                "_search_web_with_google_grounding",
                new=AsyncMock(return_value=grounded),
            ) as google,
            patch.object(research, "_get_json", new=AsyncMock()) as get,
        ):
            result = await _search_web("recent shark research")
        self.assertIs(result, grounded)
        google.assert_awaited_once_with(
            query="recent shark research", max_results=8, api_key="k"
        )
        get.assert_not_awaited()

    async def test_unconfigured_and_empty_query(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertIn("not configured", (await _search_web("q")).error or "")
            self.assertEqual((await _search_web("   ")).error, "query is required.")

    def test_google_grounding_payload_maps_sources_and_supports(self) -> None:
        metadata = SimpleNamespace(
            grounding_chunks=[
                SimpleNamespace(
                    web=SimpleNamespace(
                        title="Shark study", uri="https://example.com/study"
                    )
                ),
                SimpleNamespace(
                    web=SimpleNamespace(
                        title="Duplicate", uri="https://example.com/study"
                    )
                ),
                SimpleNamespace(
                    web=SimpleNamespace(title="Other", uri="https://example.org/b")
                ),
            ],
            grounding_supports=[
                SimpleNamespace(
                    segment=SimpleNamespace(text="The study was published this week."),
                    grounding_chunk_indices=[0],
                    confidence_scores=[0.98],
                )
            ],
            web_search_queries=["recent shark research"],
            search_entry_point=SimpleNamespace(rendered_content="<div>Search</div>"),
        )
        response = SimpleNamespace(
            text="A grounded summary.",
            candidates=[SimpleNamespace(grounding_metadata=metadata)],
        )
        result = _google_grounding_payload(
            response=response,
            query="recent shark research",
            max_results=8,
            model="gemini-2.5-flash",
        )
        self.assertEqual(result.answer, "A grounded summary.")
        self.assertEqual(
            [s.url for s in result.sources],
            ["https://example.com/study", "https://example.org/b"],
        )
        self.assertEqual(result.sources[0].title, "Shark study")
        self.assertEqual(
            result.sources[0].snippet, "The study was published this week."
        )
        self.assertIsNone(result.sources[1].snippet)
        self.assertEqual(result.search_queries, ["recent shark research"])
        self.assertEqual(result.search_suggestions_html, "<div>Search</div>")
        self.assertEqual(result.backend, "google_search_grounding")

        capped = _google_grounding_payload(
            response=response, query="q", max_results=1, model="m"
        )
        self.assertEqual(len(capped.sources), 1)

    def test_rate_limit_is_retryable(self) -> None:
        with self.assertRaises(ApplicationError) as raised:
            _raise_research_request_error({"error": "HTTP 429: Too Many Requests"})
        self.assertFalse(raised.exception.non_retryable)

    def test_authentication_failure_is_not_retryable(self) -> None:
        with self.assertRaises(ApplicationError) as raised:
            _raise_research_request_error({"error": "HTTP 401: Unauthorized"})
        self.assertTrue(raised.exception.non_retryable)

    def test_str_is_compact_and_bounded(self) -> None:
        result = SearchWebResult(
            query="sharks",
            backend="searxng",
            answer="Summary.",
            sources=[
                research.Source(
                    title=f"t{i}", url=f"https://e.com/{i}", snippet="s" * 600
                )
                for i in range(research.MAX_SEARCH_RESULTS)
            ],
        )
        text = str(result)
        self.assertTrue(text.startswith("Search: sharks [searxng]"))
        self.assertIn("1. t0 - https://e.com/0", text)
        self.assertLess(len(text), 20 * 700 + 200)


class GetJsonTests(unittest.IsolatedAsyncioTestCase):
    async def _get(self, handler) -> dict:
        transport = httpx.MockTransport(handler)
        real_client = httpx.AsyncClient

        def client(**kwargs):
            return real_client(transport=transport, **kwargs)

        with patch.object(research.httpx, "AsyncClient", side_effect=client):
            return await research._get_json(
                "http://searxng/search", {"q": "x", "drop": None}
            )

    async def test_success_and_param_cleaning(self) -> None:
        seen = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen["params"] = dict(request.url.params)
            return httpx.Response(200, json={"results": []})

        self.assertEqual(await self._get(handler), {"results": []})
        self.assertEqual(seen["params"], {"q": "x"})

    async def test_failures_become_error_dicts(self) -> None:
        self.assertEqual(
            (await self._get(lambda r: httpx.Response(503, text="down")))["error"],
            "HTTP 503: Service Unavailable",
        )
        self.assertTrue(
            (await self._get(lambda r: httpx.Response(200, text="<html>")))[
                "error"
            ].startswith("Response was not valid JSON:")
        )

        def timeout(request: httpx.Request) -> httpx.Response:
            raise httpx.ReadTimeout("slow", request=request)

        self.assertEqual((await self._get(timeout))["error"], "Request timed out.")


@unittest.skipUnless(os.environ.get("RUN_INTEGRATION") == "1", "set RUN_INTEGRATION=1")
class LiveFetchTests(unittest.IsolatedAsyncioTestCase):
    async def test_fetch_example_com(self) -> None:
        result = await _fetch_url("https://example.com/")
        self.assertEqual(result.status, 200)
        self.assertIn("Example Domain", result.title or "")


if __name__ == "__main__":
    unittest.main()
