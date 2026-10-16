# Provider Implementations

Providers adapt one model vendor's API to the provider-neutral loop in
`agent_harness.agent.Agent`.

`Agent` owns durable orchestration: context management, tool execution,
steering, interrupts, guards, continue-as-new, and persistence. A provider owns
vendor request/response conversion, provider-specific options, SDK calls,
streaming translation, stop reasons, refusal details, and context-window error
mapping.

This split lets an application keep the same workflow, tool, context, approval,
attachment, and streaming behavior while swapping model vendors.

## Boundary

Provider modules may contain:

- provider request and response dataclasses;
- an `AgentProvider` implementation;
- deterministic message, request, response, and guard conversion helpers;
- the Temporal activity that instantiates the vendor SDK and calls the API;
- vendor stream-event translation into `AgentStreamWriter`;
- provider-specific options such as thinking/reasoning config;
- a small convenience `Agent` subclass when useful for workflow construction.

Provider modules must not contain product auth, user/session state, API routes,
artifact storage, UI behavior, OAuth flows, demo workspace logic, GitHub logic,
or concrete tool implementations. Those belong to the application.

Current provider modules:

| File | Status |
| --- | --- |
| `claude.py` | Primary production-shaped provider used by `simple_chat_agent`. |
| `chatgpt.py` | Offline conversion/serialization coverage; ready for real use subject to credentialed API smoke tests. |
| `gemini.py` | Offline conversion/serialization coverage; ready for real use subject to credentialed API smoke tests. |

## Lifecycle

Provider work happens in two places.

Workflow-safe provider methods run during Temporal workflow replay and must be
pure and deterministic:

- `estimate_request_tokens`
- `create_request`
- `request_chat_history`
- `replace_request_chat_history`
- `replace_request_stream_attempt`
- `request_to_dict` / `request_from_dict`
- `response_to_dict` / `response_from_dict`
- `response_from_guard_execution`
- `response_with_visible_refusal`
- `response_message`

These methods may copy data, normalize JSON-like structures, and convert between
`AgentMessage` and the vendor schema. They must not read environment variables,
instantiate SDK clients, call token-count APIs, perform network or file I/O,
read clocks, use randomness, or depend on mutable external state.

Provider activity code runs outside workflow replay and may do I/O:

- read provider API keys or base URLs from the activity environment;
- instantiate vendor SDK clients;
- call model, token-count, or streaming APIs;
- heartbeat for liveness and cancellation;
- translate SDK exceptions into retryable or non-retryable Temporal errors;
- emit best-effort sideband stream events.

The generic agent starts the provider activity through
`start_provider_activity(...)` with the provider's `ActivityOptions`.

## Required Shape

Implement `AgentProvider` from `interface.py`.

The provider must expose:

- `name`: stable provider name used in activity summaries and stream labels.
- `activity`: Temporal activity function that calls the vendor API.
- `activity_options`: default timeout, heartbeat, retry, and cancellation
  behavior for provider calls.
- `estimate_request_tokens`: deterministic approximation of fixed request
  overhead from the system prompt and tool schema.
- `create_request`: conversion from generic `AgentMessage` history and generic
  tool schemas to the vendor request dataclass.
- `request_chat_history` / `replace_request_chat_history`: generic-history
  access for guards and context-window retries.
- `replace_request_stream_attempt`: retry metadata update so stream consumers
  can discard duplicate partials.
- request/response serialization methods: lossless JSON-like conversion for
  guard activities and workflow state.
- `response_from_guard_execution`: provider-shaped response when an LLM guard
  blocks or terminates.
- `response_with_visible_refusal`: provider refusal normalization into a clear
  assistant message when the provider returned no visible text.
- `response_message`: conversion back to `AgentMessage`.

Use helpers from `agent_harness.providers._shared` for repeated mechanics such
as JSON-like copying, mapping-list serialization, provider metadata round trips,
guard response decoration, refusal fallback text, JSON previews, and common
HTTP retry classification. Keep vendor message shapes and stream event handling
inside the provider module.

## Message Conversion

The generic harness message model lives in `agent_harness.messages`. Provider
code should translate between that model and the vendor schema only at the
provider boundary.

Rules:

- Preserve text, tool-use, and tool-result blocks.
- Preserve tool-call IDs exactly. The ID returned by the provider must be the
  ID used when returning the tool result.
- Map provider-native tool calls into generic `tool_use` blocks with `id`,
  `name`, and `input`.
- Map generic tool-result blocks back into the provider's required tool-result
  shape.
- Preserve provider-only blocks as `provider` blocks when they are useful for a
  later round trip, but do not make the generic agent loop understand provider
  internals.
- Keep refusal, thinking/reasoning, citations, safety ratings, and usage details
  provider-specific unless the generic loop needs them for control flow.

Provider-specific notes:

- Claude tools use `tool_use` blocks in assistant messages and `tool_result`
  blocks in user messages.
- OpenAI Responses function calls use `function_call` output items and
  `function_call_output` input items keyed by `call_id`.
- Gemini manual function calling uses `function_call` parts and
  `function_response` parts. The harness disables SDK automatic function
  calling so workflow-owned tools remain durable and guarded.
- Gemini thinking config supports `thinking_budget` for Gemini 2.5 models and
  `thinking_level` for Gemini 3 models; choose the provider option that matches
  the model family you run.

## Activities, Timeouts, Retries, And Cancellation

Provider activities should:

- accept the provider request dataclass and return the provider response
  dataclass;
- instantiate the SDK/client inside the activity;
- keep SDK retries disabled or low when Temporal owns retries;
- map known bad requests, auth failures, and context-window errors to
  non-retryable `ApplicationError`s;
- leave transient network, rate-limit, timeout, and server errors retryable
  unless the vendor says otherwise;
- set explicit `ActivityOptions`, including `start_to_close_timeout` and
  `heartbeat_timeout`;
- heartbeat while streaming or doing long work;
- honor `activity.wait_for_cancelled()` and emit `agent_cancelled` sideband
  events when a stream is interrupted.

Context-window failures should use the shared
`ContextWindowExceeded` application error type from `interface.py`. The generic
agent catches that type, compacts more aggressively, increments the stream
attempt, and retries once in a replay-safe way.

If the provider has a token-count API, call it only from the activity. Do not
call it from workflow-safe provider methods.

## Streaming

Streaming is sideband and non-durable. The workflow commits only the completed
provider response returned by the activity.

Provider stream translators should:

- emit `agent_started` before provider deltas;
- emit text deltas, thinking/reasoning deltas, and tool-input events through
  `AgentStreamWriter`;
- keep provider raw event names out of the public stream protocol;
- heartbeat after events and on a timer;
- emit `agent_completed` with final id, model, stop reason, stop details, text,
  and usage when available;
- tolerate unknown provider stream events.

If stream emission fails, that failure should not become durable conversation
state unless the provider API call itself failed.

## Refusals And Guard Responses

Providers differ in how refusals appear:

- Claude can stop with `stop_reason == "refusal"`.
- OpenAI Responses can return refusal content parts, `failed`, or `incomplete`
  statuses with error or incomplete details.
- Gemini can finish with safety-oriented reasons such as `SAFETY`,
  `RECITATION`, `BLOCKLIST`, `PROHIBITED_CONTENT`, or `SPII`.

`response_with_visible_refusal` should add a clear assistant-visible fallback
only when the provider stopped/refused and returned no visible text. Do not
overwrite provider text or guard-provided messages.

LLM guard responses are converted to provider-shaped responses so post-guard and
workflow code can continue to use the same provider conversion path. Use the
shared guard helper, then pass the result through the provider's normal
`response_from_dict`.

## Context And Payload Bounds

The generic agent asks the provider for request overhead through
`estimate_request_tokens(...)`; the context manager uses that to reserve space
for system prompt, tools, output, and a safety margin.

Keep workflow history and query payloads bounded:

- store stable JSON-like provider request/response data only;
- avoid putting SDK objects, clients, credentials, or mutable provider state in
  workflow history;
- preserve only provider blocks needed for future message round trips;
- let app workflows page or truncate transcript queries when rendering UI.

Continue-As-New and context overflow recovery must remain replay-safe. Provider
helpers used by workflows must be deterministic.

## How To Add A Provider

1. Create `agent_harness/providers/<provider>.py`.
2. Define provider-specific request and response dataclasses. Use JSON-like
   fields that the Temporal data converter can serialize.
3. Add provider-specific option dataclasses for vendor features such as
   thinking, reasoning, output format, or safety settings.
4. Implement `AgentProvider` with deterministic conversion methods only.
5. Convert generic tool schemas into the vendor's tool declaration schema.
6. Convert `AgentMessage` history to the vendor message/input schema.
7. Convert vendor responses back into `AgentMessage`, preserving tool-call IDs.
8. Add `request_to_dict` / `request_from_dict` and `response_to_dict` /
   `response_from_dict`. Use `_shared` copy helpers for repeated mechanics.
9. Implement the provider activity. Read environment, instantiate SDK clients,
   call token-count APIs, stream, and handle SDK exceptions only here.
10. Add streaming translation through `AgentStreamWriter`, with heartbeats and
    cancellation handling.
11. Add a convenience `<Provider>Agent` subclass if workflow code benefits from
    a narrow provider-specific constructor.
12. Register the provider activity in the application worker.
13. Add offline tests for serialization, message conversion, tool-call
    round trips, guard response conversion, refusals, context errors, and stream
    edge cases.
14. Add credentialed smoke tests outside normal unit tests before claiming full
    production readiness.

## Offline Readiness Coverage

The unit tests should not require provider credentials. They should cover:

- request and response serialization round trips;
- generic history to provider history conversion;
- provider history back to generic history conversion;
- tool-call and tool-result ID preservation;
- guard response conversion;
- refusal fallback behavior;
- usage, stop reason, and stop-detail preservation;
- provider-specific schema conversion edge cases;
- stream aggregation paths that can be tested with fake events.

Credentialed smoke tests are still needed for SDK compatibility, auth, live
model behavior, vendor-side validation, and real streaming event ordering.

## What Stays Out

Do not move these into `agent_harness/providers`:

- product login or OAuth;
- API routes, frontend state, or SSE/WebSocket transport;
- artifact storage or browsing;
- GitHub or demo workspace behavior;
- user/session persistence;
- app-specific approval state;
- tool implementation behavior;
- vendor credentials as durable workflow state.

Providers are adapters around model APIs. Applications own products.
