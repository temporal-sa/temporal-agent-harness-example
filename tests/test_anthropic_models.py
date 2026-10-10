from __future__ import annotations

import unittest
from unittest.mock import patch

from simple_chat_agent.api.anthropic_models import (
    AnthropicModelOption,
    _fallback_catalog,
    _load_anthropic_model_catalog,
    select_anthropic_model,
)
from simple_chat_agent.common.model_policy import anthropic_model_is_allowed


def _model(model_id: str) -> AnthropicModelOption:
    return AnthropicModelOption(
        id=model_id,
        display_name=model_id,
        created_at=None,
        max_input_tokens=200_000,
        max_tokens=32_000,
        thinking_modes=("enabled",),
        effort_options=("high",),
    )


class AnthropicModelPolicyTests(unittest.TestCase):
    def test_policy_allows_sonnet_and_haiku_through_4_8_and_only_opus_4_8(
        self,
    ) -> None:
        for model_id in (
            "claude-sonnet-4-5-20250929",
            "claude-sonnet-4-6",
            "claude-sonnet-4-8",
            "claude-haiku-4-5-20251001",
            "claude-opus-4-8",
        ):
            with self.subTest(model_id=model_id):
                self.assertTrue(anthropic_model_is_allowed(model_id))

        for model_id in (
            "claude-fable-5",
            "claude-opus-5",
            "claude-sonnet-5",
            "claude-opus-4-7",
            "gpt-5-mini",
        ):
            with self.subTest(model_id=model_id):
                self.assertFalse(anthropic_model_is_allowed(model_id))

    def test_live_catalog_excludes_disallowed_anthropic_models(self) -> None:
        returned_models = [
            _model("claude-opus-5"),
            _model("claude-opus-4-8"),
            _model("claude-opus-4-7"),
            _model("claude-sonnet-5"),
            _model("claude-sonnet-4-6"),
            _model("claude-sonnet-4-5-20250929"),
            _model("claude-haiku-4-5-20251001"),
            _model("claude-fable-5"),
        ]
        with (
            patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"}, clear=True),
            patch(
                "simple_chat_agent.api.anthropic_models._fetch_anthropic_models",
                return_value=returned_models,
            ),
        ):
            catalog = _load_anthropic_model_catalog()

        self.assertEqual(
            catalog.model_ids(),
            [
                "claude-opus-4-8",
                "claude-sonnet-4-6",
                "claude-sonnet-4-5-20250929",
                "claude-haiku-4-5-20251001",
            ],
        )
        self.assertEqual(catalog.default_model, "claude-opus-4-8")

    def test_fallback_ignores_disallowed_configuration(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "ANTHROPIC_MODEL": "claude-opus-5",
                "ANTHROPIC_MODEL_OPTIONS": (
                    "claude-fable-5,claude-opus-5,claude-sonnet-4-6"
                ),
            },
            clear=True,
        ):
            catalog = _fallback_catalog()

        self.assertEqual(catalog.model_ids(), ["claude-sonnet-4-6"])
        self.assertEqual(catalog.default_model, "claude-sonnet-4-6")

    def test_session_model_must_come_from_filtered_catalog(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            catalog = _fallback_catalog()

        self.assertEqual(
            select_anthropic_model(catalog, "claude-opus-4-8"),
            "claude-opus-4-8",
        )
        with self.assertRaisesRegex(ValueError, "Unsupported model 'claude-opus-5'"):
            select_anthropic_model(catalog, "claude-opus-5")


if __name__ == "__main__":
    unittest.main()
