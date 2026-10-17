"""Provider web search (P4 4.5): off by default, the per-session setting, the sources list."""

from __future__ import annotations

import asyncio
import os
import unittest
from types import SimpleNamespace
from typing import Any
from unittest import mock

os.environ.setdefault("ANTHROPIC_API_KEY", "unit-test-not-used")
os.environ.setdefault("OPENAI_API_KEY", "unit-test-not-used")

from pydantic_ai.messages import ModelResponse, NativeToolReturnPart, TextPart  # noqa: E402

from simple_chat_agent.agent.contracts import CodingAgentInit, ModelChoice, SetWebSearch, SettingsAck  # noqa: E402
from simple_chat_agent.agent.model_registry import settings_for  # noqa: E402
from simple_chat_agent.agent.runtime import web_search_capabilities, web_sources  # noqa: E402
from simple_chat_agent.agent.workflow import CodingAgent  # noqa: E402
from simple_chat_agent.api.agent_routes import web_search_allowed  # noqa: E402

from tests.test_agent_routes import A, AgentRoutesTestBase  # noqa: E402


def _ret(content: Any, name: str = "web_search") -> NativeToolReturnPart:
    return NativeToolReturnPart(tool_name=name, content=content, tool_call_id="c1")


class SourcesTests(unittest.TestCase):
    def test_provider_shapes_dedup_and_filter(self) -> None:
        anthropic = _ret([{"type": "web_search_result", "url": "https://a.example/1", "title": "A one"}])
        openai = _ret({"status": "completed", "sources": [{"type": "url", "url": "https://b.example/"}]})
        gemini = _ret([{"uri": "https://c.example/x", "title": "C", "domain": "c.example"}])
        noise = [
            _ret([{"url": "https://a.example/1", "title": "dup"}]),
            _ret([{"url": "javascript:alert(1)", "title": "bad"}]),
            _ret([{"url": "https://d.example/"}], name="code_execution"),
        ]
        messages = [ModelResponse(parts=[anthropic, TextPart("x"), openai]), ModelResponse(parts=[gemini, *noise])]
        got = [(s.title, s.url) for s in web_sources(messages)]
        self.assertEqual(
            got,
            [("A one", "https://a.example/1"), ("https://b.example/", "https://b.example/"), ("C", "https://c.example/x")],
        )

    def test_capped(self) -> None:
        part = _ret([{"url": f"https://e.example/{i}"} for i in range(50)])
        self.assertEqual(len(web_sources([ModelResponse(parts=[part])])), 20)


class SettingTests(unittest.TestCase):
    def test_no_search_tool_when_off(self) -> None:
        choice = ModelChoice(provider="anthropic", model="claude-haiku-5-5")
        self.assertEqual(web_search_capabilities(choice, enabled=False), [])
        self.assertEqual(type(web_search_capabilities(choice, enabled=True)[0]).__name__, "WebSearch")
        openai = ModelChoice(provider="openai", model="gpt-5.4-mini")
        self.assertNotIn("openai_include_web_search_sources", settings_for(openai, session_id="s"))
        self.assertTrue(settings_for(openai, session_id="s", web_search=True)["openai_include_web_search_sources"])

    def test_init_defaults_off(self) -> None:
        data = CodingAgentInit()
        self.assertEqual((data.web_search, data.web_search_allowed), (False, False))

    def _agent(self, allowed: bool, running: bool = False) -> Any:
        data = CodingAgentInit(web_search_allowed=allowed)
        agent = SimpleNamespace(_data=data, _run_task=object() if running else None)
        agent._web_search_on = lambda: agent._data.web_search and agent._data.web_search_allowed
        return agent

    def _set(self, agent: Any, enabled: bool) -> SettingsAck:
        return asyncio.run(CodingAgent.set_web_search(agent, SetWebSearch(enabled=enabled)))

    def test_set_web_search(self) -> None:
        agent = self._agent(allowed=True)
        ack = self._set(agent, True)
        self.assertEqual((ack.ok, ack.setting, ack.web_search), (True, "web_search", True))
        self.assertTrue(ack.detail.startswith("Web search on."))
        self.assertTrue(agent._data.web_search)
        busy = self._agent(allowed=True, running=True)
        busy._data = busy._data.model_copy(update={"web_search": True})
        self.assertEqual(self._set(busy, False).detail, "Web search off from the next message.")
        refused = self._agent(allowed=False)
        ack = self._set(refused, True)
        self.assertEqual((ack.ok, ack.web_search, refused._data.web_search), (False, False, False))

    def test_declared_mid_turn_accept_and_not_model_callable(self) -> None:
        from temporal_agent_harness.harness.agent_protocol import MidTurn
        from temporal_agent_harness.harness.agent_workflow import agent_handlers  # type: ignore[attr-defined]

        h = agent_handlers(CodingAgent)["set_web_search"]
        self.assertEqual((h.mid_turn, h.model_callable), (MidTurn.ACCEPT, False))


class ConfigTests(unittest.TestCase):
    def test_allowed_matrix(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=False):
            for name in ("AGENT_WEB_SEARCH", "AGENT_WEB_SEARCH_CATALOG"):
                os.environ.pop(name, None)
            self.assertTrue(web_search_allowed("local"))
            self.assertFalse(web_search_allowed("catalog"))
            os.environ["AGENT_WEB_SEARCH_CATALOG"] = "1"
            self.assertTrue(web_search_allowed("catalog"))
            os.environ["AGENT_WEB_SEARCH"] = "off"
            self.assertFalse(web_search_allowed("local"))
            self.assertFalse(web_search_allowed("catalog"))


class CatalogRouteTests(AgentRoutesTestBase):
    """The base app runs as the catalog without AGENT_WEB_SEARCH_CATALOG."""

    def test_refused_in_the_catalog(self) -> None:
        cfg = self.client.get("/api/agent/config", headers=A).json()
        self.assertEqual(cfg["web_search"], {"available": False, "default": False})
        r = self.client.post("/api/agent/sessions", json={"web_search": True}, headers=A)
        self.assertEqual((r.status_code, r.json()["error"]), (422, "web_search_not_allowed"))
        sid = self.create()["session_id"]
        init = self.temporal.starts[-1]["args"][1]
        self.assertEqual((init.web_search_allowed, init.web_search), (False, False))
        r = self.client.post(
            "/api/agent/messages",
            json={"session_id": sid, "message": {"type": "set_web_search", "payload": {"enabled": True}}},
            headers=A,
        )
        self.assertEqual((r.status_code, r.json()["error"]), (422, "web_search_not_allowed"))
        r = self.client.post(
            "/api/agent/messages",
            json={"session_id": sid, "message": {"type": "set_web_search", "payload": {"enabled": False}}},
            headers=A,
        )
        self.assertEqual(r.status_code, 200, r.text)


if __name__ == "__main__":
    unittest.main()
