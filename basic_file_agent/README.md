# Basic File Agent

The smallest agent in this repo: a Temporal Workflow on
[`temporal-agent-harness`](https://pypi.org/project/temporal-agent-harness/) with
Pydantic AI, and two file tools. It uses only the harness's public API.

| File | What it shows |
| --- | --- |
| `tools.py` | `read_file` and `write_file` as activity tools (`@agent.activity_tool_defn`), confined to the workspace. A path outside it is refused. |
| `workflow.py` | The agent: `@agent.defn`, one `@agent.accepts` handler, an `AgentWorkflowRunner`, and a Pydantic AI `TemporalAgent` whose tools run through the harness. |
| `worker.py` | The worker. `AgentHarnessPlugin(tools=...)` registers the tool activities, and `AgentPlugin` registers the model activities. |
| `start_workflow.py` | Starts an agent, sends one prompt with `AgentClient.send_message`, prints the tools it ran and its reply, then closes it. |

**Approvals.** The approval policy is
`ToolApprovalPolicy.allow_tools(["write_file"], also_inherently_safe=True)`.
`read_file` is marked `inherently_safe`, and `write_file` is allow-listed by name,
so no call waits for a person. To make writes wait for a human, drop
`"write_file"` from that list. Each write then waits until something calls
`AgentClient.approve_tool(...)`.

## Run it

You need a Temporal server and an `ANTHROPIC_API_KEY` in the environment.

```bash
# Terminal 1: the worker
uv run python -m basic_file_agent

# Terminal 2: one prompt
uv run python -m basic_file_agent.start_workflow \
  "Create notes/hello.md with one sentence about Temporal, then read it back."
```

| Variable | Default |
| --- | --- |
| `TEMPORAL_ADDRESS` | `127.0.0.1:7244`, this repo's local stack. Use `localhost:7233` for `temporal server start-dev`. |
| `TEMPORAL_NAMESPACE` | `default` |
| `BASIC_FILE_AGENT_WORKSPACE` | `./basic_file_agent_workspace` |
| `BASIC_FILE_AGENT_MODEL` | `anthropic:claude-haiku-5-5` at low effort. Any Pydantic AI model string works, with that provider's key. |

The task queue is `basic-file-agent`.
