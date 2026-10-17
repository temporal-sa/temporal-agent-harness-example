# Image for the coding agent demo: one image for the API, the worker and the sandbox executor;
# the catalog selects each container's command.
#
#   API       python -m simple_chat_agent.api.main         (port 8000; serves the web app at /)
#   worker    python -m simple_chat_agent.worker.main      (default; codec server on 8001)
#   executor  tini -g -- python -m simple_chat_agent.worker.sandbox.executor   (port 8082)
#
# IMPORTANT: build with the *repo root* as the context (the image needs pyproject.toml and
# uv.lock from there), e.g.:
#   docker build --platform linux/amd64 -f simple_chat_agent/Dockerfile .
#
# No keys are baked in: .dockerignore drops every .env file, and the catalog injects secrets at
# run time. Built for linux/amd64 to match the EKS node architecture.
FROM node:22-bookworm-slim AS web-build

WORKDIR /app/simple_chat_agent/web

# Install the web app's dependencies in their own layer.
COPY simple_chat_agent/web/package.json simple_chat_agent/web/package-lock.json simple_chat_agent/web/.npmrc ./
RUN npm ci

COPY simple_chat_agent/web ./
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

# The credential-free workspace executor runs Bubblewrap from this same image, so keep the
# command-line tools an agent needs for shell work. The executor needs git >= 2.40 (its
# startup self-test checks). pytest goes into the system Python the sandbox's PATH reaches
# (/usr/local/bin): the demo fixture's tests run with the network off, as in
# worker/sandbox/Dockerfile.executor. The executor container runs as the non-root sandbox user.
RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        bash \
        bubblewrap \
        ca-certificates \
        curl \
        file \
        git \
        iptables \
        jq \
        procps \
        ripgrep \
        tini \
        util-linux \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir "pytest==8.4.2" \
    && groupadd --gid 65532 sandbox \
    && useradd --uid 65532 --gid 65532 --create-home --home-dir /home/sandbox sandbox \
    && mkdir /workspace /workspaces \
    && chown 65532:65532 /workspace /workspaces

# Bring in the resolved virtualenv, the application source, then the built web app.
COPY --from=build /app/.venv /app/.venv
COPY . .
COPY --from=web-build /app/simple_chat_agent/web/dist /app/simple_chat_agent/web/dist

# API: 8000 (FastAPI + web app). Worker: 8001 (codec server). Executor: 8082.
EXPOSE 8000 8001 8082

# Default to the worker; the API and executor containers override this command.
CMD ["python", "-m", "simple_chat_agent.worker.main"]
