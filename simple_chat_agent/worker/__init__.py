"""The worker entrypoint (simple_chat_agent.worker.main), the codec server, the shutdown
settings and the sandbox executor."""


async def main() -> None:
    """Compatibility wrapper for the worker runtime entrypoint."""
    from simple_chat_agent.worker.main import main as run

    await run()


__all__ = ["main"]
