"""Temporal worker, workflows, activities, and tool implementations."""


async def main() -> None:
    """Compatibility wrapper for the worker runtime entrypoint."""
    from simple_chat_agent.worker.main import main as run

    await run()


__all__ = ["main"]
