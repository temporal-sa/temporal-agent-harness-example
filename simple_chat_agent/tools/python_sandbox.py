from __future__ import annotations

import ast
import json
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from typing import Any

from claude_harness.streaming import StreamContext
from claude_harness.tool_types import ToolType
from claude_harness.tools import ToolContext, ToolResult, tool

MAX_CODE_CHARS = 10_000
MAX_TIMEOUT_SECONDS = 10
MAX_OUTPUT_CHARS = 20_000
ALLOWED_IMPORT_ROOTS = frozenset(
    {
        "collections",
        "datetime",
        "decimal",
        "fractions",
        "functools",
        "itertools",
        "json",
        "math",
        "random",
        "re",
        "statistics",
    }
)
BLOCKED_CALL_NAMES = frozenset(
    {
        "__import__",
        "breakpoint",
        "compile",
        "eval",
        "exec",
        "globals",
        "help",
        "input",
        "locals",
        "open",
        "vars",
    }
)


@tool(
    name="python_sandbox",
    description=(
        "Execute short, pure-Python code in a restricted local subprocess. "
        "Use this for calculations, text processing, parsing, and small data "
        "transformations. File, network, subprocess, and arbitrary package "
        "access are not available. Use create_artifact when durable file "
        "output is needed."
    ),
    tool_type=ToolType.MUTATING,
    pre_guards=["mutating_tool_approval"],
)
async def python_sandbox(
    ctx: ToolContext,
    code: str,
    timeout_seconds: int = 5,
) -> ToolResult:
    payload = await ctx.activity(
        _run_python_sandbox_activity,
        args={"code": code, "timeout_seconds": timeout_seconds},
    )
    return ToolResult(payload=payload, error="error" in payload)


@dataclass(frozen=True)
class _ValidationResult:
    code: str
    imports: list[str]


async def _run_python_sandbox_activity(
    code: str,
    timeout_seconds: int,
    *,
    stream: StreamContext,
) -> dict[str, Any]:
    timeout_seconds = max(1, min(timeout_seconds, MAX_TIMEOUT_SECONDS))
    await stream.emit(
        {"timeout_seconds": timeout_seconds, "chars": len(code)},
        kind="python_sandbox_start",
    )

    try:
        validation = _validate_and_prepare_code(code)
    except ValueError as err:
        payload = {"error": str(err), "type": "ValidationError"}
        await stream.emit(payload, kind="python_sandbox_rejected")
        return payload

    with tempfile.TemporaryDirectory(prefix="claude-python-sandbox-") as temp_dir:
        try:
            completed = subprocess.run(
                [sys.executable, "-I", "-S", "-c", _RUNNER],
                input=json.dumps({"code": validation.code}),
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=temp_dir,
                env={"PYTHONIOENCODING": "utf-8"},
                timeout=timeout_seconds,
                check=False,
            )
        except subprocess.TimeoutExpired:
            payload = {
                "error": f"Python execution timed out after {timeout_seconds} seconds.",
                "type": "TimeoutExpired",
            }
            await stream.emit(payload, kind="python_sandbox_timeout")
            return payload

    payload = _parse_runner_output(completed.stdout, completed.stderr)
    payload["exit_code"] = completed.returncode
    payload["imports"] = validation.imports
    await stream.emit(
        {
            "exit_code": completed.returncode,
            "ok": completed.returncode == 0 and "error" not in payload,
            "stdout_chars": len(str(payload.get("stdout", ""))),
            "stderr_chars": len(str(payload.get("stderr", ""))),
        },
        kind="python_sandbox_complete",
    )
    return payload


def _validate_and_prepare_code(code: str) -> _ValidationResult:
    if len(code) > MAX_CODE_CHARS:
        raise ValueError(f"Code is too large. Max chars: {MAX_CODE_CHARS}.")

    try:
        tree = ast.parse(code, mode="exec")
    except SyntaxError as err:
        raise ValueError(f"SyntaxError: {err}") from err

    imports: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                _validate_import(alias.name)
                imports.append(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module is None:
                raise ValueError("Relative imports are not allowed.")
            _validate_import(node.module)
            imports.append(node.module)
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            if node.func.id in BLOCKED_CALL_NAMES:
                raise ValueError(f"Call is not allowed: {node.func.id}")
        elif isinstance(node, ast.Attribute) and node.attr.startswith("__"):
            raise ValueError("Dunder attribute access is not allowed.")
        elif isinstance(node, ast.Name) and node.id.startswith("__"):
            raise ValueError("Dunder names are not allowed.")

    if tree.body and isinstance(tree.body[-1], ast.Expr):
        tree.body[-1] = ast.Assign(
            targets=[ast.Name(id="_result", ctx=ast.Store())],
            value=tree.body[-1].value,
        )
        ast.fix_missing_locations(tree)

    return _ValidationResult(code=ast.unparse(tree), imports=sorted(set(imports)))


def _validate_import(module_name: str) -> None:
    root = module_name.split(".", 1)[0]
    if root not in ALLOWED_IMPORT_ROOTS:
        allowed = ", ".join(sorted(ALLOWED_IMPORT_ROOTS))
        raise ValueError(f"Import is not allowed: {module_name}. Allowed: {allowed}")


def _parse_runner_output(stdout: str, stderr: str) -> dict[str, Any]:
    if stderr:
        return {
            "error": "Sandbox runner wrote to stderr.",
            "stderr": _truncate(stderr),
            "raw_stdout": _truncate(stdout),
        }
    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError:
        return {
            "error": "Sandbox runner returned invalid JSON.",
            "raw_stdout": _truncate(stdout),
        }
    if not isinstance(payload, dict):
        return {"error": "Sandbox runner returned a non-object payload."}
    return payload


def _truncate(value: str) -> str:
    if len(value) <= MAX_OUTPUT_CHARS:
        return value
    return value[:MAX_OUTPUT_CHARS] + "\n...[truncated]"


_RUNNER = r"""
import contextlib
import io
import json
import sys
import traceback

import collections
import datetime
import decimal
import fractions
import functools
import itertools
import math
import random
import re
import statistics

ALLOWED_MODULES = {
    "collections": collections,
    "datetime": datetime,
    "decimal": decimal,
    "fractions": fractions,
    "functools": functools,
    "itertools": itertools,
    "json": json,
    "math": math,
    "random": random,
    "re": re,
    "statistics": statistics,
}


def safe_import(name, globals=None, locals=None, fromlist=(), level=0):
    root = name.split(".", 1)[0]
    if level != 0 or root not in ALLOWED_MODULES:
        raise ImportError(f"Import is not allowed: {name}")
    return __import__(name, globals, locals, fromlist, level)


safe_builtins = {
    "__import__": safe_import,
    "abs": abs,
    "all": all,
    "any": any,
    "bool": bool,
    "dict": dict,
    "enumerate": enumerate,
    "filter": filter,
    "float": float,
    "int": int,
    "isinstance": isinstance,
    "len": len,
    "list": list,
    "map": map,
    "max": max,
    "min": min,
    "pow": pow,
    "print": print,
    "range": range,
    "repr": repr,
    "reversed": reversed,
    "round": round,
    "set": set,
    "slice": slice,
    "sorted": sorted,
    "str": str,
    "sum": sum,
    "tuple": tuple,
    "zip": zip,
}


def jsonable(value):
    try:
        json.dumps(value)
        return value
    except TypeError:
        return repr(value)


payload = json.loads(sys.stdin.read())
code = payload["code"]
stdout = io.StringIO()
stderr = io.StringIO()
namespace = {
    "__builtins__": safe_builtins,
    **ALLOWED_MODULES,
}

try:
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        exec(code, namespace, namespace)
    response = {
        "stdout": stdout.getvalue(),
        "stderr": stderr.getvalue(),
        "result": jsonable(namespace.get("_result", namespace.get("result"))),
    }
except BaseException as err:
    response = {
        "error": str(err),
        "type": type(err).__name__,
        "stdout": stdout.getvalue(),
        "stderr": stderr.getvalue(),
        "traceback": traceback.format_exc(limit=5),
    }

print(json.dumps(response))
"""
