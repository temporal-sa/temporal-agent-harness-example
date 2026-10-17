"""PRD §5.6: local sign-in never runs on a built-in or short JWT secret."""

from __future__ import annotations

import asyncio
import os
import unittest
from unittest import mock

from simple_chat_agent.api import auth, main


class SessionSecretTests(unittest.TestCase):
    def test_no_built_in_fallback(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("SIMPLE_CHAT_JWT_SECRET", None)
            self.assertIn("not set", auth.session_secret_problem() or "")
            with self.assertRaises(auth.AuthError):
                auth.create_session_token(auth.AuthenticatedUser(user_id="u", username="u"))

    def test_short_secret_refused_and_long_one_works(self) -> None:
        with mock.patch.dict(os.environ, {"SIMPLE_CHAT_JWT_SECRET": "short"}):
            self.assertIn("at least", auth.session_secret_problem() or "")
        with mock.patch.dict(os.environ, {"SIMPLE_CHAT_JWT_SECRET": "x" * 40}):
            self.assertIsNone(auth.session_secret_problem())
            token = auth.create_session_token(auth.AuthenticatedUser(user_id="u1", username="demo"))
            self.assertEqual(auth.user_from_session_token(token).user_id, "u1")

    def test_api_refuses_to_start_with_local_auth_and_no_secret(self) -> None:
        env = {"SIMPLE_CHAT_LOCAL_AUTH_ENABLED": "1", "SIMPLE_CHAT_AUTH_MODE": "local"}
        with mock.patch.dict(os.environ, env), mock.patch.object(main, "load_dotenv"):
            os.environ.pop("SIMPLE_CHAT_JWT_SECRET", None)

            async def start() -> None:
                async with main.lifespan(main.app):
                    pass

            with self.assertRaisesRegex(RuntimeError, "SIMPLE_CHAT_JWT_SECRET"):
                asyncio.run(start())


if __name__ == "__main__":
    unittest.main()
