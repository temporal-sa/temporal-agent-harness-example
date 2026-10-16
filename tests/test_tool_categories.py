from __future__ import annotations

from enum import StrEnum
import unittest

from agent_harness import tools as tools_module
from agent_harness.guards import GuardContext, GuardPolicy, GuardResult, guard
from agent_harness.tools import ToolContext, ToolResult, ToolSet, tool


class ExampleToolCategory(StrEnum):
    READ_FILE = "read_file"
    WRITE_FILE = "write_file"


@guard(name="allow_write_file", fulfills=ExampleToolCategory.WRITE_FILE)
async def allow_write_file(ctx: GuardContext) -> GuardResult:
    return GuardResult(passed=True)


@tool(
    name="write_file",
    description="Write a file.",
    tool_type=ExampleToolCategory.WRITE_FILE,
    pre_guards=[allow_write_file],
)
async def write_file(ctx: ToolContext, path: str) -> ToolResult:
    return ToolResult(payload={"path": path}, error=False)


@tool(
    name="write_file_with_named_guard",
    description="Write a file with a named guard reference.",
    tool_type=ExampleToolCategory.WRITE_FILE,
    pre_guards=["allow_write_file"],
)
async def write_file_with_named_guard(ctx: ToolContext, path: str) -> ToolResult:
    return ToolResult(payload={"path": path}, error=False)


@tool(
    name="unguarded_write_file",
    description="Write a file without a guard.",
    tool_type=ExampleToolCategory.WRITE_FILE,
)
async def unguarded_write_file(ctx: ToolContext, path: str) -> ToolResult:
    return ToolResult(payload={"path": path}, error=False)


@tool(
    name="failed_write_file",
    description="Return a structured tool failure.",
    tool_type=ExampleToolCategory.WRITE_FILE,
)
async def failed_write_file(ctx: ToolContext, path: str) -> ToolResult:
    return ToolResult(payload={"error": "disk is full", "path": path}, error=True)


async def dynamic_write_file(ctx: ToolContext, args: dict) -> ToolResult:
    return ToolResult(payload=args, error=False)


class BaseProvider:
    async def inherited_tool(self, ctx: ToolContext, path: str) -> ToolResult:
        return ToolResult(payload={"base": path}, error=False)


class ChildProvider(BaseProvider):
    @tool(
        name="child_inherited_tool",
        description="Registered by the child class.",
        tool_type=ExampleToolCategory.READ_FILE,
    )
    async def inherited_tool(self, ctx: ToolContext, path: str) -> ToolResult:
        return ToolResult(payload={"child": path}, error=False)


class ToolCategoryTests(unittest.IsolatedAsyncioTestCase):
    def test_old_guard_import_path_still_works(self) -> None:
        self.assertIs(tools_module.guard, guard)

    def test_guard_policy_require_pre_normalizes_custom_categories(self) -> None:
        policy = GuardPolicy.require_pre(ExampleToolCategory.WRITE_FILE)

        self.assertEqual(policy.required_pre, frozenset({"write_file"}))
        self.assertEqual(policy.required_post, frozenset())

    async def test_custom_tool_category_can_require_guard(self) -> None:
        tools = ToolSet(
            guard_policy=GuardPolicy(
                required_pre=frozenset({ExampleToolCategory.WRITE_FILE}),
            )
        )
        tools.add_tool(write_file)

        result = await tools.execute_tool("write_file", {"path": "notes.txt"})

        self.assertFalse(result.error)
        self.assertEqual(result.payload, {"path": "notes.txt"})

    async def test_tool_set_constructor_registers_initial_tools_and_guards(self) -> None:
        tools = ToolSet(
            guard_policy=GuardPolicy(
                required_pre=frozenset({ExampleToolCategory.WRITE_FILE}),
            ),
            guards=[allow_write_file],
            tools=[write_file_with_named_guard],
        )

        result = await tools.execute_tool(
            "write_file_with_named_guard",
            {"path": "notes.txt"},
        )

        self.assertFalse(result.error)
        self.assertEqual(result.payload, {"path": "notes.txt"})

    async def test_provider_child_method_can_decorate_undecorated_base_name(self) -> None:
        tools = ToolSet(providers=[ChildProvider()])

        result = await tools.execute_tool(
            "child_inherited_tool",
            {"path": "notes.txt"},
        )

        self.assertFalse(result.error)
        self.assertEqual(result.payload, {"child": "notes.txt"})

    async def test_custom_tool_category_rejects_missing_required_guard(self) -> None:
        tools = ToolSet(
            guard_policy=GuardPolicy(
                required_pre=frozenset({ExampleToolCategory.WRITE_FILE}),
            )
        )
        tools.add_tool(unguarded_write_file)

        with self.assertRaisesRegex(ValueError, "requires at least one pre guard"):
            await tools.execute_tool("unguarded_write_file", {"path": "notes.txt"})

    def test_dynamic_tools_normalize_custom_tool_category(self) -> None:
        tools = ToolSet()
        tools.add_dynamic_tool(
            name="dynamic_write_file",
            description="Write a file dynamically.",
            input_schema={
                "type": "object",
                "properties": {"path": {"type": "string"}},
                "required": ["path"],
            },
            tool_type=ExampleToolCategory.WRITE_FILE,
            fn=dynamic_write_file,
        )

        self.assertEqual(
            tools.get_tool("dynamic_write_file").tool_type,
            "write_file",
        )

    def test_terminal_stream_event_contains_bounded_success_result(self) -> None:
        payload = tools_module._stream_tool_result_payload(
            ToolResult(payload={"path": "notes.txt"}, error=False)
        )

        self.assertEqual(payload["result_preview"], '{"path":"notes.txt"}')
        self.assertFalse(payload["result_truncated"])

    def test_terminal_stream_event_contains_structured_failure(self) -> None:
        payload = tools_module._stream_tool_result_payload(
            ToolResult(
                payload={"error": "disk is full", "path": "notes.txt"},
                error=True,
            )
        )

        self.assertEqual(
            payload["error"],
            {"type": "ToolResultError", "message": "disk is full"},
        )
