from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from threading import Lock
from typing import Any

from anthropic import Anthropic


FALLBACK_MODEL_OPTIONS = [
    "claude-sonnet-4-5",
    "claude-sonnet-4-6",
    "claude-opus-4-5",
    "claude-opus-4-7",
    "claude-haiku-4-5",
]
FALLBACK_ADAPTIVE_THINKING_MODEL_PREFIXES = ("claude-opus-4-7",)
THINKING_MODE_ORDER = ("adaptive", "enabled")
EFFORT_ORDER = ("low", "medium", "high", "xhigh", "max")
DEFAULT_MODEL_CACHE_SECONDS = 300

_CATALOG_CACHE: tuple[float, "AnthropicModelCatalog"] | None = None
_CATALOG_LOCK = Lock()


@dataclass(frozen=True)
class AnthropicModelOption:
    id: str
    display_name: str
    created_at: str | None
    max_input_tokens: int | None
    max_tokens: int | None
    thinking_modes: tuple[str, ...]
    effort_options: tuple[str, ...]

    def to_api_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "display_name": self.display_name,
            "created_at": self.created_at,
            "max_input_tokens": self.max_input_tokens,
            "max_tokens": self.max_tokens,
            "thinking": {
                "supported": bool(self.thinking_modes),
                "modes": list(self.thinking_modes),
                "default_mode": default_thinking_mode(self),
            },
            "effort_options": list(self.effort_options),
            "default_effort": default_effort(self),
        }


@dataclass(frozen=True)
class AnthropicModelCatalog:
    source: str
    models: tuple[AnthropicModelOption, ...]
    default_model: str
    error: str | None = None

    def model_ids(self) -> list[str]:
        return [model.id for model in self.models]

    def model_by_id(self, model_id: str) -> AnthropicModelOption | None:
        for model in self.models:
            if model.id == model_id:
                return model
        return None


def get_anthropic_model_catalog() -> AnthropicModelCatalog:
    global _CATALOG_CACHE

    ttl = _model_cache_seconds()
    now = time.monotonic()
    cached = _CATALOG_CACHE
    if cached is not None and cached[0] > now:
        return cached[1]

    with _CATALOG_LOCK:
        cached = _CATALOG_CACHE
        now = time.monotonic()
        if cached is not None and cached[0] > now:
            return cached[1]

        catalog = _load_anthropic_model_catalog()
        _CATALOG_CACHE = (now + ttl, catalog)
        return catalog


def default_thinking_mode(model: AnthropicModelOption | None) -> str:
    if model is None:
        return "enabled"
    if "adaptive" in model.thinking_modes:
        return "adaptive"
    if "enabled" in model.thinking_modes:
        return "enabled"
    return "enabled"


def default_effort(model: AnthropicModelOption | None) -> str:
    if model is None:
        return "max"
    for effort in reversed(EFFORT_ORDER):
        if effort in model.effort_options:
            return effort
    return "max"


def _load_anthropic_model_catalog() -> AnthropicModelCatalog:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return _fallback_catalog("ANTHROPIC_API_KEY is not configured")

    try:
        models = _fetch_anthropic_models(api_key)
    except Exception as err:
        return _fallback_catalog(str(err))

    if not models:
        return _fallback_catalog("Anthropic returned no models")

    models = tuple(sorted(_dedupe_models(models), key=_model_sort_key, reverse=True))
    return AnthropicModelCatalog(
        source="anthropic",
        models=models,
        default_model=models[0].id,
    )


def _fetch_anthropic_models(api_key: str) -> list[AnthropicModelOption]:
    client = Anthropic(api_key=api_key)
    first_page = client.models.list(limit=1000, timeout=10)
    models: list[AnthropicModelOption] = []
    for page in first_page.iter_pages():
        for model in page.data:
            if getattr(model, "type", None) != "model":
                continue
            models.append(
                AnthropicModelOption(
                    id=model.id,
                    display_name=model.display_name or model.id,
                    created_at=_isoformat(model.created_at),
                    max_input_tokens=model.max_input_tokens,
                    max_tokens=model.max_tokens,
                    thinking_modes=_thinking_modes(model.capabilities),
                    effort_options=_effort_options(model.capabilities),
                )
            )
    return models


def _fallback_catalog(error: str | None = None) -> AnthropicModelCatalog:
    configured = [
        model.strip()
        for model in os.environ.get("ANTHROPIC_MODEL_OPTIONS", "").split(",")
        if model.strip()
    ]
    options = configured or FALLBACK_MODEL_OPTIONS
    configured_default = os.environ.get("ANTHROPIC_MODEL", "").strip()
    if configured_default:
        options = [configured_default, *options]

    models = tuple(
        sorted(
            _dedupe_models(
                [
                    AnthropicModelOption(
                        id=model,
                        display_name=model,
                        created_at=None,
                        max_input_tokens=None,
                        max_tokens=None,
                        thinking_modes=_fallback_thinking_modes(model),
                        effort_options=EFFORT_ORDER,
                    )
                    for model in options
                ]
            ),
            key=_model_sort_key,
            reverse=True,
        )
    )
    default_model = configured_default or (models[0].id if models else FALLBACK_MODEL_OPTIONS[0])
    return AnthropicModelCatalog(
        source="fallback",
        models=models,
        default_model=default_model,
        error=error,
    )


def _thinking_modes(capabilities: Any) -> tuple[str, ...]:
    thinking = _get(capabilities, "thinking")
    if not _supported(thinking):
        return ()
    types = _get(thinking, "types")
    return tuple(
        mode
        for mode in THINKING_MODE_ORDER
        if _supported(_get(types, mode))
    )


def _effort_options(capabilities: Any) -> tuple[str, ...]:
    effort = _get(capabilities, "effort")
    if not _supported(effort):
        return ()
    return tuple(
        name
        for name in EFFORT_ORDER
        if _supported(_get(effort, name))
    )


def _fallback_thinking_modes(model: str) -> tuple[str, ...]:
    if any(
        model.startswith(prefix)
        for prefix in FALLBACK_ADAPTIVE_THINKING_MODEL_PREFIXES
    ):
        return ("adaptive", "enabled")
    return ("enabled",)


def _model_sort_key(model: AnthropicModelOption) -> tuple[int, float, str]:
    return (_family_rank(model.id), _created_timestamp(model.created_at), model.id)


def _family_rank(model_id: str) -> int:
    normalized = model_id.lower()
    if "opus" in normalized:
        return 3
    if "sonnet" in normalized:
        return 2
    if "haiku" in normalized:
        return 1
    return 0


def _created_timestamp(created_at: str | None) -> float:
    if not created_at:
        return 0.0
    try:
        normalized = created_at.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized).timestamp()
    except ValueError:
        return 0.0


def _isoformat(value: Any) -> str | None:
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat().replace("+00:00", "Z")


def _supported(value: Any) -> bool:
    return bool(_get(value, "supported", False))


def _get(value: Any, name: str, default: Any = None) -> Any:
    if value is None:
        return default
    if isinstance(value, dict):
        return value.get(name, default)
    return getattr(value, name, default)


def _dedupe_models(models: list[AnthropicModelOption]) -> list[AnthropicModelOption]:
    seen: set[str] = set()
    result: list[AnthropicModelOption] = []
    for model in models:
        if model.id in seen:
            continue
        seen.add(model.id)
        result.append(model)
    return result


def _model_cache_seconds() -> int:
    raw = os.environ.get("ANTHROPIC_MODEL_CACHE_SECONDS", "")
    if not raw:
        return DEFAULT_MODEL_CACHE_SECONDS
    try:
        return max(1, int(raw))
    except ValueError:
        return DEFAULT_MODEL_CACHE_SECONDS
