"""Tests for simple_chat_agent.agent.context (PRD A17)."""

from __future__ import annotations

import copy
import json
import random
import unittest
from datetime import datetime, timezone

import anthropic
import httpx
import openai
from google.genai import errors as genai_errors
from pydantic_ai import Agent
from pydantic_ai.exceptions import ModelHTTPError
from pydantic_ai.messages import (
    ModelMessage,
    ModelRequest,
    ModelResponse,
    RetryPromptPart,
    SystemPromptPart,
    TextPart,
    ThinkingPart,
    ToolCallPart,
    ToolReturnPart,
    UserPromptPart,
)
from pydantic_ai.models.function import AgentInfo, FunctionModel
from temporalio.exceptions import ActivityError, ApplicationError, RetryState

from simple_chat_agent.agent import context
from simple_chat_agent.agent.context import (
    COMPACTION_MARKER_PREFIX,
    ContextBudget,
    ContextFit,
    budget_for,
    context_overflow_error,
    estimate_tokens,
    fit_history,
    fit_with,
    is_context_overflow,
    model_limits,
    tighter,
)
from simple_chat_agent.agent.model_registry import DEFAULT_ALLOWLIST

T0 = datetime(2026, 10, 16, tzinfo=timezone.utc)
SYSTEM = "You are a coding agent."
FIRST = "Please fix the failing refund test."


# ---------------------------------------------------------------------------
# Builders and invariants
# ---------------------------------------------------------------------------


def first_request() -> ModelRequest:
    return ModelRequest(
        parts=[SystemPromptPart(SYSTEM, timestamp=T0), UserPromptPart(FIRST, timestamp=T0)],
        instructions="instructions",
        timestamp=T0,
    )


def response(
    rng: random.Random, n_calls: int, call_counter: list[int], *, text_chars: int = 0, thinking: bool = False
) -> ModelResponse:
    parts: list = []
    if thinking:
        parts.append(ThinkingPart("thinking " * 20, signature="sig", provider_name="anthropic"))
    if text_chars:
        parts.append(TextPart("t" * text_chars))
    for _ in range(n_calls):
        call_counter[0] += 1
        parts.append(
            ToolCallPart("read_file", {"path": f"f{call_counter[0]}.py"}, tool_call_id=f"c{call_counter[0]}")
        )
    if not parts:
        parts.append(TextPart("done"))
    return ModelResponse(parts=parts, provider_name="anthropic", timestamp=T0)


def returns_for(rng: random.Random, resp: ModelResponse, *, max_chars: int) -> ModelRequest:
    parts: list = []
    for p in resp.parts:
        if isinstance(p, ToolCallPart):
            if rng.random() < 0.1:
                parts.append(RetryPromptPart("bad args", tool_name=p.tool_name, tool_call_id=p.tool_call_id, timestamp=T0))
            else:
                size = rng.randint(10, max_chars)
                parts.append(ToolReturnPart(p.tool_name, "x" * size, tool_call_id=p.tool_call_id, timestamp=T0))
    return ModelRequest(parts=parts, instructions="instructions", timestamp=T0)


def random_history(seed: int, *, steps: int, max_chars: int = 20_000) -> list[ModelMessage]:
    rng = random.Random(seed)
    counter = [0]
    history: list[ModelMessage] = [first_request()]
    for step in range(steps):
        resp = response(
            rng,
            rng.choice([0, 0, 1, 1, 2, 3]),
            counter,
            text_chars=rng.choice([0, 50, 2_000, 9_000]),
            thinking=rng.random() < 0.3,
        )
        history.append(resp)
        if any(isinstance(p, ToolCallPart) for p in resp.parts):
            history.append(returns_for(rng, resp, max_chars=max_chars))
        else:
            history.append(
                ModelRequest(
                    parts=[UserPromptPart(f"next task {step} " + "u" * rng.choice([10, 3_000]), timestamp=T0)],
                    instructions="instructions",
                    timestamp=T0,
                )
            )
    return history


def call_ids(m: ModelMessage) -> set[str]:
    return {p.tool_call_id for p in m.parts if isinstance(p, ToolCallPart)}


def return_ids(m: ModelMessage) -> set[str]:
    return {
        p.tool_call_id
        for p in m.parts
        if isinstance(p, ToolReturnPart) or (isinstance(p, RetryPromptPart) and p.tool_name)
    }


class InvariantMixin(unittest.TestCase):
    def assert_invariants(self, messages: list[ModelMessage]) -> None:
        self.assertTrue(messages)
        for i, m in enumerate(messages):
            expected = ModelRequest if i % 2 == 0 else ModelResponse
            self.assertIsInstance(m, expected, f"message {i} breaks alternation")
        self.assertIsInstance(messages[-1], ModelRequest)
        for i, m in enumerate(messages):
            if isinstance(m, ModelResponse):
                self.assertEqual(call_ids(m), return_ids(messages[i + 1]), f"tool pairs broken at {i}")
            elif i == 0:
                self.assertEqual(return_ids(m), set())
            else:
                self.assertTrue(return_ids(m) <= call_ids(messages[i - 1]))
        head = messages[0]
        self.assertIsInstance(head.parts[0], SystemPromptPart)
        self.assertEqual(head.parts[0].content, SYSTEM)
        prompts = [p for p in head.parts if isinstance(p, UserPromptPart)]
        self.assertEqual(prompts[0].content, FIRST)
        for m in messages:
            if isinstance(m, ModelRequest):
                self.assertEqual(m.instructions, "instructions")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class FitHistoryPropertyTests(InvariantMixin):
    def test_randomized_histories_keep_invariants_and_are_idempotent(self) -> None:
        for seed in range(150):
            rng = random.Random(10_000 + seed)
            history = random_history(seed, steps=rng.randint(1, 60))
            budget = ContextBudget(
                max_input_tokens=rng.choice([20_000, 50_000, 120_000, 400_000]),
                reserve_output_tokens=rng.choice([1_000, 4_000]),
                system_and_tools_tokens=rng.choice([0, 500, 3_000]),
                safety_margin=rng.choice([500, 2_000]),
                window=rng.choice([4, 10, 40, 1_000]),
                chars_per_token=rng.choice([4.0, 3.0, 2.5]),
            )
            original = copy.deepcopy(history)
            fitted = fit_with(history, budget)
            with self.subTest(seed=seed):
                self.assertEqual(history, original, "input was mutated")
                self.assert_invariants(fitted)
                self.assertEqual(fit_with(fitted, budget), fitted, "not idempotent")
                self.assertEqual(fit_with(copy.deepcopy(original), budget), fitted, "not deterministic")
                tokens = estimate_tokens(fitted, chars_per_token=budget.chars_per_token)
                if budget.message_tokens > 20_000:
                    self.assertLessEqual(tokens, budget.message_tokens)

    def test_small_history_is_returned_unchanged(self) -> None:
        history = random_history(1, steps=3, max_chars=100)
        fitted = fit_history(
            history, max_input_tokens=200_000, reserve_output_tokens=16_000,
            system_and_tools_tokens=1_000, safety_margin=4_000,
        )
        self.assertEqual(fitted, history)
        for a, b in zip(fitted, history):
            self.assertIs(a, b)


class WindowAndMarkerTests(InvariantMixin):
    def test_window_keeps_first_and_recent_messages_and_adds_marker(self) -> None:
        history = random_history(7, steps=60, max_chars=50)
        fitted = fit_history(
            history, max_input_tokens=1_000_000, reserve_output_tokens=16_000,
            system_and_tools_tokens=0, safety_margin=4_000, window=40,
        )
        self.assert_invariants(fitted)
        self.assertLessEqual(len(fitted), 40)
        self.assertIs(fitted[-1], history[-1])
        markers = [p for p in fitted[0].parts if isinstance(p, UserPromptPart) and str(p.content).startswith(COMPACTION_MARKER_PREFIX)]
        self.assertEqual(len(markers), 1)
        dropped = len(history) - len(fitted)
        self.assertIn(f"{COMPACTION_MARKER_PREFIX}{dropped} earlier messages", markers[0].content)
        self.assertEqual(markers[0].timestamp, T0)

    def test_marker_count_accumulates_across_compactions(self) -> None:
        history = random_history(8, steps=40, max_chars=50)
        budget = ContextBudget(max_input_tokens=1_000_000, window=20)
        once = fit_with(history, budget)
        first_drop = len(history) - len(once)
        rng = random.Random(1)
        grown = list(once)
        counter = [10_000]
        for _ in range(15):
            resp = response(rng, 1, counter)
            grown += [resp, returns_for(rng, resp, max_chars=50)]
        twice = fit_with(grown, budget)
        second_drop = len(grown) - len(twice)
        marker = twice[0].parts[-1]
        self.assertIn(f"{COMPACTION_MARKER_PREFIX}{first_drop + second_drop} ", marker.content)
        self.assertEqual(sum(1 for p in twice[0].parts if str(getattr(p, "content", "")).startswith(COMPACTION_MARKER_PREFIX)), 1)

    def test_latest_user_prompt_is_kept_through_a_long_tool_loop(self) -> None:
        rng = random.Random(3)
        counter = [0]
        history: list[ModelMessage] = [first_request()]
        resp = response(rng, 0, counter)
        history += [resp, ModelRequest(parts=[UserPromptPart("THE CURRENT TASK")], instructions="instructions", timestamp=T0)]
        for _ in range(40):
            resp = response(rng, 1, counter)
            history += [resp, returns_for(rng, resp, max_chars=50)]
        fitted = fit_with(history, ContextBudget(max_input_tokens=1_000_000, window=20))
        self.assert_invariants(fitted)
        prompts = [p.content for m in fitted for p in m.parts if isinstance(p, UserPromptPart)]
        self.assertIn("THE CURRENT TASK", prompts)

    def test_system_prompt_parts_from_dropped_messages_are_hoisted(self) -> None:
        history = random_history(9, steps=30, max_chars=50)
        history[2] = ModelRequest(
            parts=[*history[2].parts, SystemPromptPart("late system prompt", timestamp=T0)],
            instructions="instructions", timestamp=T0,
        )
        fitted = fit_with(history, ContextBudget(max_input_tokens=1_000_000, window=10))
        systems = [p.content for p in fitted[0].parts if isinstance(p, SystemPromptPart)]
        self.assertEqual(systems, [SYSTEM, "late system prompt"])
        self.assert_invariants(fitted)
        self.assertEqual(fit_with(fitted, ContextBudget(max_input_tokens=1_000_000, window=10)), fitted)

    def test_incomplete_tool_exchanges_are_dropped(self) -> None:
        rng = random.Random(4)
        counter = [0]
        history: list[ModelMessage] = [first_request()]
        resp = response(rng, 2, counter)
        partial = returns_for(rng, resp, max_chars=50)
        partial = ModelRequest(parts=list(partial.parts[:1]), instructions="instructions", timestamp=T0)
        history += [resp, partial]
        resp2 = response(rng, 1, counter)
        history += [resp2, returns_for(rng, resp2, max_chars=50)]
        fitted = fit_with(history, ContextBudget(max_input_tokens=1_000_000))
        self.assert_invariants(fitted)
        self.assertEqual(len(fitted), 5)  # the call without its result is gone, its pair kept
        self.assertEqual(len(call_ids(fitted[1])), 1)

    def test_anthropic_thinking_is_stripped_only_after_an_edit(self) -> None:
        history = random_history(5, steps=30, max_chars=50)
        untouched = fit_with(history, ContextBudget(max_input_tokens=1_000_000, window=1_000))
        self.assertTrue(any(isinstance(p, ThinkingPart) for m in untouched for p in m.parts))
        edited = fit_with(history, ContextBudget(max_input_tokens=1_000_000, window=10))
        self.assertFalse(any(isinstance(p, ThinkingPart) for m in edited for p in m.parts))

    def test_other_providers_keep_reasoning(self) -> None:
        history = random_history(5, steps=30, max_chars=50)
        history = [
            ModelResponse(parts=list(m.parts), provider_name="openai", timestamp=T0) if isinstance(m, ModelResponse) else m
            for m in history
        ]
        edited = fit_with(history, ContextBudget(max_input_tokens=1_000_000, window=10))
        self.assertTrue(any(isinstance(p, ThinkingPart) for m in edited for p in m.parts))


class TruncationTests(InvariantMixin):
    def test_largest_tool_result_becomes_a_stub(self) -> None:
        big = "line\n" * 40_000  # 200K chars, ~50K tokens
        resp = ModelResponse(parts=[ToolCallPart("read_file", {"path": "a"}, tool_call_id="c1")], timestamp=T0)
        history = [
            first_request(),
            resp,
            ModelRequest(parts=[ToolReturnPart("read_file", big, tool_call_id="c1", timestamp=T0)], instructions="instructions", timestamp=T0),
        ]
        budget = ContextBudget(max_input_tokens=30_000, reserve_output_tokens=4_000, safety_margin=1_000)
        fitted = fit_with(history, budget)
        self.assert_invariants(fitted)
        content = fitted[2].parts[0].content
        stub = json.loads(content)
        self.assertEqual(stub["truncated"], True)
        self.assertEqual(stub["original_chars"], len(big))
        self.assertTrue(big.startswith(stub["preview"]))
        self.assertLessEqual(len(stub["preview"]), context.PREVIEW_CHARS)
        self.assertLessEqual(estimate_tokens(fitted), budget.message_tokens)
        self.assertEqual(fit_with(fitted, budget), fitted)
        # A tighter budget shrinks the existing stub, keeping original_chars.
        tight = fit_with(fitted, ContextBudget(max_input_tokens=1_500, reserve_output_tokens=100, safety_margin=100))
        self.assertEqual(json.loads(tight[2].parts[0].content)["original_chars"], len(big))

    def test_structured_tool_results_are_measured_and_stubbed(self) -> None:
        payload = {"files": [{"path": f"f{i}.py", "body": "y" * 500} for i in range(200)]}
        resp = ModelResponse(parts=[ToolCallPart("list", {}, tool_call_id="c1")], timestamp=T0)
        history = [first_request(), resp, ModelRequest(parts=[ToolReturnPart("list", payload, tool_call_id="c1")], instructions="instructions", timestamp=T0)]
        fitted = fit_with(history, ContextBudget(max_input_tokens=20_000, reserve_output_tokens=2_000, safety_margin=500))
        self.assertTrue(json.loads(fitted[2].parts[0].content)["truncated"])

    def test_text_is_truncated_after_tool_results(self) -> None:
        history = [
            ModelRequest(parts=[SystemPromptPart(SYSTEM), UserPromptPart(FIRST)], instructions="instructions", timestamp=T0),
            ModelResponse(parts=[TextPart("z" * 120_000)], timestamp=T0),
            ModelRequest(parts=[UserPromptPart("short follow-up")], instructions="instructions", timestamp=T0),
        ]
        budget = ContextBudget(max_input_tokens=20_000, reserve_output_tokens=2_000, safety_margin=500)
        fitted = fit_with(history, budget)
        self.assert_invariants(fitted)
        text = fitted[1].parts[0].content
        self.assertTrue(text.startswith("[Truncated to fit the context window; original 120000 chars]"))
        self.assertLessEqual(estimate_tokens(fitted), budget.message_tokens)
        self.assertEqual(fit_with(fitted, budget), fitted)

    def test_oldest_groups_are_dropped_before_truncating(self) -> None:
        history = random_history(11, steps=30, max_chars=8_000)
        budget = ContextBudget(max_input_tokens=60_000, reserve_output_tokens=4_000, safety_margin=1_000, window=1_000)
        fitted = fit_with(history, budget)
        self.assert_invariants(fitted)
        self.assertLess(len(fitted), len(history))
        stubs = [p for m in fitted for p in m.parts if isinstance(p, ToolReturnPart) and str(p.content).startswith('{"truncated"')]
        self.assertEqual(stubs, [])


class LongSessionTests(InvariantMixin):
    def _simulate(self, registry_key: str, turns: int, *, seed: int) -> float:
        """Drive a synthetic session the way Pydantic AI does: the processed history is what
        persists, and each request appends to it. Returns the worst fill ratio seen."""
        rng = random.Random(seed)
        max_input, _ = model_limits(registry_key)
        sys_tools = 6_000
        budget = budget_for(registry_key, reserve_output_tokens=16_000, system_and_tools_tokens=sys_tools)
        history: list[ModelMessage] = [first_request()]
        counter = [0]
        worst = 0.0
        for turn in range(turns):
            if turn:
                history.append(ModelRequest(parts=[UserPromptPart(f"turn {turn}: " + "q" * rng.randint(20, 4_000))], instructions="instructions", timestamp=T0))
            for _ in range(rng.randint(0, 6)):  # tool loop within the turn
                history = fit_with(history, budget)
                worst = max(worst, (estimate_tokens(history) + sys_tools) / max_input)
                resp = response(rng, rng.randint(1, 3), counter, text_chars=rng.choice([0, 400]))
                history += [resp, returns_for(rng, resp, max_chars=rng.choice([2_000, 30_000, 150_000]))]
            history = fit_with(history, budget)
            worst = max(worst, (estimate_tokens(history) + sys_tools) / max_input)
            self.assert_invariants(history)
            history.append(response(rng, 0, counter, text_chars=rng.randint(100, 6_000)))
        return worst

    def test_200_turn_session_never_exceeds_85_percent(self) -> None:
        for key in ("openai:gpt-5.4-mini", "anthropic:claude-haiku-5-5"):
            with self.subTest(model=key):
                self.assertLessEqual(self._simulate(key, 200, seed=42), 0.85)

    def test_200_turn_session_with_small_window_model(self) -> None:
        # An unknown model gets the conservative 128K window; big tool results then force
        # truncation as well as dropping.
        self.assertLessEqual(self._simulate("openai:unknown-model", 200, seed=7), 0.85)


class BudgetHelperTests(unittest.TestCase):
    def test_tighter(self) -> None:
        b = ContextBudget(max_input_tokens=200_000, safety_margin=4_000, chars_per_token=4.0)
        t = tighter(b)
        self.assertEqual(t.safety_margin, 16_000)
        self.assertEqual(t.chars_per_token, 2.5)
        t2 = tighter(ContextBudget(max_input_tokens=200_000, safety_margin=10_000, chars_per_token=2.0))
        self.assertEqual((t2.safety_margin, t2.chars_per_token), (40_000, 2.0))

    def test_message_tokens_respects_max_fill(self) -> None:
        b = ContextBudget(max_input_tokens=1_000_000, reserve_output_tokens=16_000, system_and_tools_tokens=5_000)
        self.assertEqual(b.message_tokens, 850_000 - 5_000)
        small = ContextBudget(max_input_tokens=20_000, reserve_output_tokens=16_000, safety_margin=4_000)
        self.assertEqual(small.message_tokens, 0)

    def test_model_limits_cover_the_allowlist(self) -> None:
        for provider, models in DEFAULT_ALLOWLIST.items():
            for name in models:
                key = f"{provider}:{name}"
                self.assertIn(key, context.MODEL_LIMITS, key)
                self.assertEqual(model_limits(name), model_limits(key))
        self.assertEqual(model_limits("anthropic:claude-sonnet-5-5"), (1_000_000, 128_000))
        self.assertEqual(model_limits("someone:else"), (128_000, 16_000))

    def test_budget_for_clamps_output_reservation(self) -> None:
        b = budget_for("google:gemini-3.5-flash", reserve_output_tokens=500_000)
        self.assertEqual(b.reserve_output_tokens, 65_536)


# ---------------------------------------------------------------------------
# Overflow recognition with realistic error objects
# ---------------------------------------------------------------------------


def _anthropic_error(status: int, body: dict) -> ModelHTTPError:
    from pydantic_ai.models.anthropic import _map_api_errors

    request = httpx.Request("POST", "https://api.anthropic.com/v1/messages")
    response = httpx.Response(status, request=request, json=body)
    cls = anthropic.BadRequestError if status == 400 else anthropic.APIStatusError
    sdk_error = cls(f"Error code: {status} - {body}", response=response, body=body)
    try:
        with _map_api_errors("claude-sonnet-5-5"):
            raise sdk_error
    except ModelHTTPError as exc:
        return exc
    raise AssertionError("not mapped")


def _openai_error(status: int, body: dict) -> ModelHTTPError:
    from pydantic_ai.models.openai import _map_api_errors

    client = openai.AsyncOpenAI(api_key="test", max_retries=0)
    import httpx2  # openai 3.x is built on httpx2

    request = httpx2.Request("POST", "https://api.openai.com/v1/responses")
    response = httpx2.Response(status, request=request, json=body)
    sdk_error = client._make_status_error(f"Error code: {status}", body=body, response=response)
    try:
        with _map_api_errors("gpt-5.5"):
            raise sdk_error
    except ModelHTTPError as exc:
        return exc
    raise AssertionError("not mapped")


def _gemini_error(status: int, body: dict) -> ModelHTTPError:
    # pydantic_ai.models.google maps errors.APIError inline: ModelHTTPError(e.code, body=e.details)
    sdk_error = genai_errors.ClientError(status, body)
    return ModelHTTPError(status_code=sdk_error.code, model_name="gemini-3.5-flash", body=sdk_error.details)


class OverflowRecognitionTests(unittest.TestCase):
    def test_anthropic(self) -> None:
        self.assertTrue(is_context_overflow(_anthropic_error(400, {
            "type": "error",
            "error": {"type": "invalid_request_error", "message": "prompt is too long: 1015000 tokens > 1000000 maximum"},
        })))
        self.assertTrue(is_context_overflow(_anthropic_error(400, {
            "type": "error",
            "error": {"type": "invalid_request_error", "message": "input length and `max_tokens` exceed context limit: 988000 + 16000 > 1000000, decrease input length or `max_tokens` and try again"},
        })))
        self.assertTrue(is_context_overflow(_anthropic_error(413, {
            "type": "error",
            "error": {"type": "request_too_large", "message": "Request exceeds the maximum allowed number of bytes."},
        })))
        self.assertFalse(is_context_overflow(_anthropic_error(400, {
            "type": "error",
            "error": {"type": "invalid_request_error", "message": "max_tokens: 200000 > 128000, which is the maximum allowed number of output tokens for claude-sonnet-5-5"},
        })))
        self.assertFalse(is_context_overflow(_anthropic_error(429, {
            "type": "error", "error": {"type": "rate_limit_error", "message": "Number of request tokens has exceeded your per-minute rate limit"},
        })))

    def test_openai(self) -> None:
        self.assertTrue(is_context_overflow(_openai_error(400, {"error": {
            "message": "Your input exceeds the context window of this model. Please adjust your input and try again.",
            "type": "invalid_request_error", "param": "input", "code": "context_length_exceeded",
        }})))
        self.assertTrue(is_context_overflow(_openai_error(400, {"error": {
            "message": "This model's maximum context length is 272000 tokens. However, your messages resulted in 280123 tokens.",
            "type": "invalid_request_error", "param": "messages", "code": None,
        }})))
        self.assertFalse(is_context_overflow(_openai_error(400, {"error": {
            "message": "Invalid schema for function 'edit_file'.", "type": "invalid_request_error", "param": "tools[0]", "code": "invalid_function_parameters",
        }})))

    def test_gemini(self) -> None:
        self.assertTrue(is_context_overflow(_gemini_error(400, {"error": {
            "code": 400, "message": "The input token count (1100000) exceeds the maximum number of tokens allowed (1048576).", "status": "INVALID_ARGUMENT",
        }})))
        self.assertFalse(is_context_overflow(_gemini_error(400, {"error": {
            "code": 400, "message": "API key not valid. Please pass a valid API key.", "status": "INVALID_ARGUMENT",
        }})))

    def test_workflow_side_activity_error(self) -> None:
        raw = _anthropic_error(400, {"type": "error", "error": {"type": "invalid_request_error", "message": "prompt is too long: 1 > 0 maximum"}})
        app = context_overflow_error(raw)
        self.assertIsInstance(app, ApplicationError)
        self.assertTrue(app.non_retryable)
        activity_error = ActivityError(
            "Activity task failed", scheduled_event_id=5, started_event_id=6, identity="w",
            activity_type="request", activity_id="1", retry_state=RetryState.NON_RETRYABLE_FAILURE,
        )
        activity_error.__cause__ = app
        self.assertTrue(is_context_overflow(activity_error))
        rejected = ApplicationError("rejected", type="ProviderRejected", non_retryable=True)
        other = ActivityError(
            "Activity task failed", scheduled_event_id=5, started_event_id=6, identity="w",
            activity_type="request", activity_id="1", retry_state=RetryState.NON_RETRYABLE_FAILURE,
        )
        other.__cause__ = rejected
        self.assertFalse(is_context_overflow(other))
        self.assertFalse(is_context_overflow(ValueError("x")))
        self.assertFalse(is_context_overflow(None))


# ---------------------------------------------------------------------------
# Capability wiring against a real Agent run
# ---------------------------------------------------------------------------


class ContextFitCapabilityTests(InvariantMixin, unittest.IsolatedAsyncioTestCase):
    async def test_model_sees_fitted_history(self) -> None:
        seen: list[list[ModelMessage]] = []

        def model(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
            seen.append(messages)
            return ModelResponse(parts=[TextPart("ok")])

        agent = Agent(FunctionModel(model), instructions="instructions")
        history = random_history(21, steps=40, max_chars=50)[:-1]  # ends in a response
        cap = ContextFit(ContextBudget(max_input_tokens=1_000_000, window=20))
        result = await agent.run("new prompt", message_history=history, capabilities=[cap])
        self.assertLessEqual(len(seen[0]), 20)
        self.assertTrue(any(str(getattr(p, "content", "")).startswith(COMPACTION_MARKER_PREFIX) for p in seen[0][0].parts))
        self.assertEqual(seen[0][-1].parts[-1].content, "new prompt")
        # Pydantic AI persists the processed history.
        self.assertEqual(result.all_messages()[: len(seen[0])], seen[0])

    async def test_request_budget_measures_instructions_tools_and_max_tokens(self) -> None:
        budgets: list[ContextBudget] = []

        class Spy(ContextFit):
            def request_budget(self, request_context):  # type: ignore[override]
                b = super().request_budget(request_context)
                budgets.append(b)
                return b

        agent = Agent(FunctionModel(lambda m, i: ModelResponse(parts=[TextPart("ok")])), instructions="i" * 4_000)

        @agent.tool_plain
        def lookup(query: str) -> str:
            """Look something up."""
            return query

        await agent.run("hi", capabilities=[Spy(ContextBudget(max_input_tokens=100_000))], model_settings={"max_tokens": 1234})
        self.assertEqual(budgets[0].reserve_output_tokens, 1234)
        self.assertGreater(budgets[0].system_and_tools_tokens, 1_000)


if __name__ == "__main__":
    unittest.main()
