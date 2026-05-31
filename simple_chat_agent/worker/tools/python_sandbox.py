from __future__ import annotations

import asyncio
import json
import os
from datetime import timedelta
from typing import Any

from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

from claude_harness.streaming import StreamContext
from claude_harness.tool_types import ToolType
from claude_harness.tools import ToolContext, ToolResult, tool
from simple_chat_agent.worker.sandbox.runtime import (
    DEFAULT_TIMEOUT_SECONDS,
    LAMBDA_MAX_TIMEOUT_SECONDS,
    MAX_TIMEOUT_SECONDS,
    execute_python_sandbox,
)

LAMBDA_ACTIVITY_TIMEOUT_SECONDS = LAMBDA_MAX_TIMEOUT_SECONDS + 60
LAMBDA_INVOKE_READ_TIMEOUT_SECONDS = LAMBDA_MAX_TIMEOUT_SECONDS + 30
LAMBDA_ACTIVITY_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=2),
    maximum_interval=timedelta(seconds=10),
    maximum_attempts=3,
)


@tool(
    name="python_sandbox",
    description=(
        "Execute Python code in an isolated sandbox. Use this for calculations, "
        "scripts, imports, network calls, parsing, and data transformations. "
        "For long-running work, write progress to stdout or stderr with print() "
        "or sys.stderr.write(); that output streams back to the user while the "
        "code is still running. Runtime, output, permissions, and retries are "
        "bounded by the sandbox."
    ),
    tool_type=ToolType.MUTATING,
    pre_guards=["mutating_tool_approval"],
)
async def python_sandbox(
    ctx: ToolContext,
    code: str,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
) -> ToolResult:
    payload = await ctx.activity(
        _run_python_sandbox_activity,
        args={"code": code, "timeout_seconds": timeout_seconds},
        schedule_to_start_timeout=timedelta(seconds=30),
        start_to_close_timeout=timedelta(seconds=LAMBDA_ACTIVITY_TIMEOUT_SECONDS),
        schedule_to_close_timeout=timedelta(
            seconds=LAMBDA_ACTIVITY_TIMEOUT_SECONDS * 3
        ),
        retry_policy=LAMBDA_ACTIVITY_RETRY_POLICY,
    )
    return ToolResult(payload=payload, error="error" in payload)


async def _run_python_sandbox_activity(
    code: str,
    timeout_seconds: int,
    *,
    stream: StreamContext,
) -> dict[str, Any]:
    timeout_seconds = max(1, min(timeout_seconds, MAX_TIMEOUT_SECONDS))
    function_name = _sandbox_lambda_function_name()
    if not function_name:
        return await execute_python_sandbox(code, timeout_seconds, stream=stream)

    await stream.emit(
        {"function_name": function_name, "timeout_seconds": timeout_seconds},
        kind="python_sandbox_lambda_invoke",
    )
    payload = await _invoke_sandbox_lambda(
        function_name=function_name,
        code=code,
        timeout_seconds=timeout_seconds,
        stream=stream,
    )
    if "error" in payload:
        await stream.emit(payload, kind="python_sandbox_lambda_error")
    return payload


async def _invoke_sandbox_lambda(
    *,
    function_name: str,
    code: str,
    timeout_seconds: int,
    stream: StreamContext,
) -> dict[str, Any]:
    request = {
        "code": code,
        "timeout_seconds": timeout_seconds,
        "stream": {
            "stream_id": stream.stream_id,
            "tool_name": stream.tool_name,
            "step": stream.step,
        },
    }
    stream_sink = _lambda_stream_sink_config()
    if stream_sink is not None:
        request["stream_sink"] = stream_sink
    try:
        return await asyncio.to_thread(
            _invoke_sandbox_lambda_sync,
            function_name,
            request,
        )
    except Exception as err:
        if isinstance(err, ApplicationError):
            raise
        raise ApplicationError(
            f"Sandbox Lambda invoke failed: {err}",
            type="SandboxLambdaInvokeFailure",
        ) from err


def _invoke_sandbox_lambda_sync(
    function_name: str,
    request: dict[str, Any],
) -> dict[str, Any]:
    invoke_kwargs: dict[str, Any] = {
        "FunctionName": function_name,
        "InvocationType": "RequestResponse",
        "Payload": json.dumps(request).encode("utf-8"),
    }
    qualifier = os.environ.get("PYTHON_SANDBOX_LAMBDA_QUALIFIER", "").strip()
    if qualifier:
        invoke_kwargs["Qualifier"] = qualifier

    response = _lambda_client().invoke(**invoke_kwargs)
    raw_payload = response["Payload"].read().decode("utf-8")
    payload = _parse_lambda_payload(raw_payload)
    status_code = int(response.get("StatusCode") or 0)
    function_error = response.get("FunctionError")

    if function_error:
        return {
            "error": "Sandbox Lambda returned a function error.",
            "type": "SandboxLambdaFunctionError",
            "lambda_error": function_error,
            "lambda_payload": payload,
        }
    if status_code < 200 or status_code >= 300:
        raise ApplicationError(
            f"Sandbox Lambda invoke returned status {status_code}.",
            type="SandboxLambdaInvokeFailure",
        )
    if not isinstance(payload, dict):
        return {
            "error": "Sandbox Lambda returned a non-object payload.",
            "type": "SandboxLambdaPayloadError",
            "lambda_payload": payload,
        }
    return payload


def _parse_lambda_payload(raw_payload: str) -> Any:
    if not raw_payload:
        return {}
    try:
        return json.loads(raw_payload)
    except json.JSONDecodeError:
        return raw_payload


def _lambda_client() -> Any:
    global _cached_lambda_client
    if _cached_lambda_client is None:
        import boto3
        from botocore.config import Config

        _cached_lambda_client = boto3.client(
            "lambda",
            config=Config(
                connect_timeout=10,
                read_timeout=LAMBDA_INVOKE_READ_TIMEOUT_SECONDS,
                retries={"total_max_attempts": 1},
            ),
        )
    return _cached_lambda_client


def _sandbox_lambda_function_name() -> str:
    return os.environ.get("PYTHON_SANDBOX_LAMBDA_FUNCTION", "").strip()


def _lambda_stream_sink_config() -> dict[str, str] | None:
    # The Lambda should not inherit app environment or secrets. The worker sends
    # the narrow stream endpoint/token per invocation, and the Lambda only uses
    # it in the outer handler. The sandboxed child process still receives a
    # minimal env from python_sandbox_runtime._runner_env().
    url = os.environ.get("PYTHON_SANDBOX_STREAM_SINK_URL", "").strip()
    token = os.environ.get("PYTHON_SANDBOX_STREAM_TOKEN", "").strip()
    if not token:
        token = os.environ.get("SIMPLE_CHAT_STREAM_TOKEN", "").strip()
    if not url or not token:
        return None
    return {"url": url, "token": token}


_cached_lambda_client: Any | None = None
