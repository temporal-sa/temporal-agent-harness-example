import os

# Local sign-in refuses to run without a real signing secret (PRD §5.6); tests that sign in
# get one here. Tests of the refusal itself remove it inside their own patch.
os.environ.setdefault("SIMPLE_CHAT_JWT_SECRET", "test-only-session-secret-" + "x" * 32)
