from __future__ import annotations

import unittest
from io import BytesIO
from unittest.mock import AsyncMock, Mock, patch
from urllib.error import HTTPError

from temporalio.exceptions import ApplicationError

from agent_harness.mcp import call_http_mcp_tool
from agent_harness.streaming import StreamContext
from simple_chat_agent.worker.tools.fetch_url import _fetch_url_activity
from simple_chat_agent.worker.tools.github import _github_api_request


class ExternalToolRetryTests(unittest.IsolatedAsyncioTestCase):
    async def test_fetch_url_retries_transient_http_status(self) -> None:
        with patch(
            "simple_chat_agent.worker.tools.fetch_url.asyncio.to_thread",
            new=AsyncMock(
                return_value={
                    "url": "https://example.com",
                    "final_url": "https://example.com",
                    "status": 503,
                    "error": "HTTP 503: Service Unavailable",
                }
            ),
        ):
            with self.assertRaises(ApplicationError) as raised:
                await _fetch_url_activity(
                    "https://example.com",
                    stream=StreamContext(stream_id=None),
                )

        self.assertEqual(raised.exception.type, "FetchUrlRequestError")
        self.assertFalse(raised.exception.non_retryable)

    async def test_fetch_url_returns_nonretryable_http_status(self) -> None:
        response = {
            "url": "https://example.com/missing",
            "final_url": "https://example.com/missing",
            "status": 404,
            "error": "HTTP 404: Not Found",
        }
        with patch(
            "simple_chat_agent.worker.tools.fetch_url.asyncio.to_thread",
            new=AsyncMock(return_value=response),
        ):
            result = await _fetch_url_activity(
                "https://example.com/missing",
                stream=StreamContext(stream_id=None),
            )

        self.assertEqual(result, response)

    async def test_mcp_transport_failure_is_retryable(self) -> None:
        with patch(
            "agent_harness.mcp._call_http_mcp_tool",
            new=AsyncMock(side_effect=ConnectionError("connection reset")),
        ):
            with self.assertRaises(ApplicationError) as raised:
                await call_http_mcp_tool(
                    server_id="demo",
                    server_url="https://mcp.example.com",
                    auth_ref=None,
                    tool_name="search",
                    arguments={},
                )

        self.assertEqual(raised.exception.type, "McpToolCallError")
        self.assertFalse(raised.exception.non_retryable)

    async def test_mcp_reauthorization_failure_is_not_retryable(self) -> None:
        with patch(
            "agent_harness.mcp._call_http_mcp_tool",
            new=AsyncMock(
                side_effect=RuntimeError(
                    "MCP OAuth token requires reauthorization"
                )
            ),
        ):
            with self.assertRaises(ApplicationError) as raised:
                await call_http_mcp_tool(
                    server_id="demo",
                    server_url="https://mcp.example.com",
                    auth_ref="connection",
                    tool_name="search",
                    arguments={},
                )

        self.assertTrue(raised.exception.non_retryable)

    async def test_github_server_error_is_retryable(self) -> None:
        store = Mock()
        store.get_oauth_connection_by_id.return_value = Mock(
            access_token="token"
        )
        error = HTTPError(
            "https://api.github.com/user",
            503,
            "Service Unavailable",
            {},
            BytesIO(b'{"message":"temporarily unavailable"}'),
        )

        with (
            patch(
                "simple_chat_agent.worker.tools.github.app_store",
                return_value=store,
            ),
            patch(
                "simple_chat_agent.worker.tools.github.urlopen",
                side_effect=error,
            ),
        ):
            with self.assertRaises(ApplicationError) as raised:
                _github_api_request("connection", "/user")

        self.assertEqual(raised.exception.type, "GitHubApiError")
        self.assertFalse(raised.exception.non_retryable)


if __name__ == "__main__":
    unittest.main()
