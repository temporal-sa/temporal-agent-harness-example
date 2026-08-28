from __future__ import annotations

import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from temporalio.exceptions import ApplicationError

from agent_harness.streaming import StreamContext
from simple_chat_agent.worker.tools.research import (
    GOOGLE_SEARCH_GROUNDING_API_KEY,
    SEARCH_WEB_TOOL,
    SEARXNG_BASE_URL,
    _google_grounding_payload,
    _raise_research_request_error,
    _search_web_activity,
    configured_research_tool_names,
)


class ResearchToolTests(unittest.IsolatedAsyncioTestCase):
    async def test_all_primary_engine_failures_fall_back_to_bing(self) -> None:
        primary_response = {
            "results": [],
            "suggestions": [],
            "unresponsive_engines": [
                ["brave", "too many requests"],
                ["google", "CAPTCHA"],
            ],
        }
        bing_response = {
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
            patch(
                "simple_chat_agent.worker.tools.research._get_json",
                side_effect=[primary_response, bing_response],
            ) as get_json,
        ):
            result = await _search_web_activity(
                query="recent shark research",
                stream=StreamContext(stream_id=None),
            )

        self.assertEqual(len(result["results"]), 1)
        self.assertEqual(result["fallback_engine"], "bing")
        self.assertEqual(get_json.call_count, 2)
        self.assertNotIn("engines", get_json.call_args_list[0].args[1])
        self.assertEqual(get_json.call_args_list[1].args[1]["engines"], "bing")

    async def test_bing_failure_does_not_rapidly_retry_blocked_engines(self) -> None:
        primary_response = {
            "results": [],
            "suggestions": [],
            "unresponsive_engines": [["brave", "too many requests"]],
        }
        bing_response = {
            "results": [],
            "suggestions": [],
            "unresponsive_engines": [["bing", "CAPTCHA"]],
        }

        with (
            patch.dict(os.environ, {SEARXNG_BASE_URL: "http://searxng"}, clear=True),
            patch(
                "simple_chat_agent.worker.tools.research._get_json",
                side_effect=[primary_response, bing_response],
            ) as get_json,
            self.assertRaises(ApplicationError) as raised,
        ):
            await _search_web_activity(
                query="recent shark research",
                stream=StreamContext(stream_id=None),
            )

        self.assertEqual(raised.exception.type, "SearchEnginesUnavailable")
        self.assertTrue(raised.exception.non_retryable)
        self.assertEqual(get_json.call_count, 2)

    async def test_partial_searxng_results_do_not_fail(self) -> None:
        response = {
            "results": [
                {
                    "title": "Shark study",
                    "url": "https://example.com/study",
                    "content": "A recent study",
                    "engine": "brave",
                }
            ],
            "suggestions": [],
            "unresponsive_engines": [["google", "too many requests"]],
        }

        with (
            patch.dict(os.environ, {SEARXNG_BASE_URL: "http://searxng"}, clear=True),
            patch(
                "simple_chat_agent.worker.tools.research._get_json",
                return_value=response,
            ),
        ):
            result = await _search_web_activity(
                query="recent shark research",
                stream=StreamContext(stream_id=None),
            )

        self.assertEqual(len(result["results"]), 1)
        self.assertEqual(result["engine_errors"], [["google", "too many requests"]])
        self.assertIsNone(result["fallback_engine"])
        self.assertEqual(result["backend"], "searxng")

    async def test_google_grounding_replaces_searxng_when_configured(self) -> None:
        grounded_payload = {
            "query": "recent shark research",
            "results": [],
            "backend": "google_search_grounding",
        }
        with (
            patch.dict(
                os.environ,
                {
                    GOOGLE_SEARCH_GROUNDING_API_KEY: "grounding-key",
                    SEARXNG_BASE_URL: "http://searxng",
                },
                clear=True,
            ),
            patch(
                "simple_chat_agent.worker.tools.research._search_web_with_google_grounding",
                return_value=grounded_payload,
            ) as google_search,
            patch(
                "simple_chat_agent.worker.tools.research._get_json",
            ) as get_json,
        ):
            result = await _search_web_activity(
                query="recent shark research",
                stream=StreamContext(stream_id=None),
            )

        self.assertEqual(result, grounded_payload)
        google_search.assert_awaited_once()
        get_json.assert_not_called()

    def test_grounding_key_enables_search_web(self) -> None:
        with patch.dict(
            os.environ,
            {GOOGLE_SEARCH_GROUNDING_API_KEY: "grounding-key"},
            clear=True,
        ):
            self.assertEqual(configured_research_tool_names(), [SEARCH_WEB_TOOL])

    def test_google_grounding_payload_maps_sources_and_supports(self) -> None:
        metadata = SimpleNamespace(
            grounding_chunks=[
                SimpleNamespace(
                    web=SimpleNamespace(
                        title="Shark study",
                        uri="https://example.com/study",
                    )
                ),
                SimpleNamespace(
                    web=SimpleNamespace(
                        title="Duplicate",
                        uri="https://example.com/study",
                    )
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

        self.assertEqual(result["answer"], "A grounded summary.")
        self.assertEqual(len(result["results"]), 1)
        self.assertEqual(result["results"][0]["title"], "Shark study")
        self.assertEqual(
            result["results"][0]["content"],
            "The study was published this week.",
        )
        self.assertEqual(result["search_queries"], ["recent shark research"])
        self.assertEqual(result["search_suggestions_html"], "<div>Search</div>")

    def test_rate_limit_is_retryable(self) -> None:
        with self.assertRaises(ApplicationError) as raised:
            _raise_research_request_error({"error": "HTTP 429: Too Many Requests"})

        self.assertFalse(raised.exception.non_retryable)

    def test_authentication_failure_is_not_retryable(self) -> None:
        with self.assertRaises(ApplicationError) as raised:
            _raise_research_request_error({"error": "HTTP 401: Unauthorized"})

        self.assertTrue(raised.exception.non_retryable)


if __name__ == "__main__":
    unittest.main()
