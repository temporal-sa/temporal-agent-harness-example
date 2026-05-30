"""Good Place censoring as persisting, full-context LLM guards.

The pre-guard censors the latest user message (prior turns are already censored
at rest, since pre-guard mutations persist). The post-guard censors the latest
assistant output. Both are pure dict transforms; the harness persists their
results into the context manager.
"""
from __future__ import annotations

from claude_harness.llm_guards import LlmGuardContext, LlmGuardResult

from simple_chat_agent.good_place import censor_content


def good_place_pre_guard(ctx: LlmGuardContext) -> LlmGuardResult:
    request = ctx.request
    history = request.get("chat_history") or []
    for message in reversed(history):
        if message.get("role") == "user":
            message["content"] = censor_content(message["content"])
            break
    return LlmGuardResult.allow(request=request)


def good_place_post_guard(ctx: LlmGuardContext) -> LlmGuardResult:
    response = ctx.response or {}
    message = response.get("message")
    if isinstance(message, dict) and "content" in message:
        message["content"] = censor_content(message["content"])
    return LlmGuardResult.allow(response=response)
