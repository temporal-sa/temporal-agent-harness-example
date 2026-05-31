# Image for the simple_chat_agent. Used for the frontend, API, and worker
# containers; they differ only by the command they run (see deploy/ manifests).
#
# IMPORTANT: build with the *repo root* as the context (the image needs
# pyproject.toml, uv.lock, and claude_harness/ from there), e.g.:
#   docker build -f simple_chat_agent/Dockerfile .
#
# Built for linux/amd64 to match the EKS node architecture.
FROM node:22-bookworm-slim AS frontend-build

WORKDIR /app/simple_chat_agent/frontend

# Install frontend dependencies in their own layer.
COPY simple_chat_agent/frontend/package*.json ./
RUN npm ci

COPY simple_chat_agent/frontend ./
RUN npm run build

FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS build

WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

# Install dependencies first (cached layer) using only the lockfile + manifest.
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project

FROM python:3.12-slim

WORKDIR /app
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1

# Bring in the resolved virtualenv, then the application source.
COPY --from=build /app/.venv /app/.venv
COPY . .
COPY --from=frontend-build /app/simple_chat_agent/frontend/static/dist /app/simple_chat_agent/frontend/static/dist

# Frontend: 8080 (static SPA). API: 8000 (FastAPI). Worker: 8001 (codec).
EXPOSE 8080 8000 8001

# Default to the worker; the web container overrides this command.
CMD ["python", "-m", "simple_chat_agent.worker.main"]
