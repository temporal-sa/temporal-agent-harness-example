from __future__ import annotations

import unittest
from unittest.mock import AsyncMock, Mock, patch

from agent_harness.providers import chatgpt, claude, gemini


class ProviderClientReuseTests(unittest.IsolatedAsyncioTestCase):
    async def asyncTearDown(self) -> None:
        await claude.close_anthropic_clients()
        await chatgpt.close_chatgpt_clients()
        await gemini.close_gemini_clients()

    async def test_anthropic_client_is_reused_and_closed_at_shutdown(self) -> None:
        client = Mock()
        client.close = AsyncMock()
        with (
            patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"}, clear=False),
            patch.object(claude, "AsyncAnthropic", return_value=client) as factory,
        ):
            first = claude._anthropic_client()
            second = claude._anthropic_client()

        self.assertIs(first, second)
        factory.assert_called_once()
        await claude.close_anthropic_clients()
        client.close.assert_awaited_once_with()

    async def test_chatgpt_client_is_reused_and_closed_at_shutdown(self) -> None:
        client = Mock()
        client.close = AsyncMock()
        with patch("openai.AsyncOpenAI", return_value=client) as factory:
            first = chatgpt._chatgpt_client("test-key")
            second = chatgpt._chatgpt_client("test-key")

        self.assertIs(first, second)
        factory.assert_called_once()
        await chatgpt.close_chatgpt_clients()
        client.close.assert_awaited_once_with()

    async def test_gemini_client_is_reused_and_closed_at_shutdown(self) -> None:
        async_client = Mock()
        async_client.aclose = AsyncMock()
        client = Mock(aio=async_client)
        with patch.object(gemini.genai, "Client", return_value=client) as factory:
            first = gemini._gemini_client("test-key")
            second = gemini._gemini_client("test-key")

        self.assertIs(first, second)
        factory.assert_called_once()
        await gemini.close_gemini_clients()
        async_client.aclose.assert_awaited_once_with()


if __name__ == "__main__":
    unittest.main()
