# Local development for the coding agent and its product view (P3).
#
#   make web           build the product view into simple_chat_agent/web/dist (served at /)
#   make web-dev       Vite dev server with live reload, proxying /api and /console to the API
#   make gen           regenerate web/src/generated from the agents' JSON Schema
#   make crash-worker  SIGKILL the local dev worker (demo crash trigger)
#   make dev-stack     print the commands that start the local stack
#   make dev-worker    run the dev worker (uses .env.local)
#   make dev-api       run the API without provider keys (uses .env.local)
#   make image         build the app image (API + worker + executor; builds the web app inside)
#   make executor-image  build the local-development executor image
#
# .env.local holds keys: it is sourced, never printed. Recipes that read it start with `@`.

SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

ROOT := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
WEB := $(ROOT)/simple_chat_agent/web
PYTHON := $(ROOT)/.venv/bin/python
ENV_FILE := $(ROOT)/.env.local

TEMPORAL_PORT ?= 7244
TEMPORAL_UI_PORT ?= 8244
REDIS_PORT ?= 6390
EXECUTOR_PORT ?= 8083
API_PORT ?= 8000
TASK_QUEUE ?= coding-agent-dev

# Load .env.local into the recipe's shell without echoing it (set -a exports every assignment).
LOAD_ENV := if [ -f "$(ENV_FILE)" ]; then set -a; . "$(ENV_FILE)"; set +a; else echo "note: $(ENV_FILE) not found" >&2; fi

.PHONY: help web web-dev gen crash-worker dev-stack dev-worker dev-api image executor-image

help:
	@sed -n '2,12p' $(ROOT)/Makefile | sed 's/^# \{0,1\}//'

web:
	cd "$(WEB)" && npm ci && npm run build

web-dev:
	cd "$(WEB)" && AGENT_API_URL="http://127.0.0.1:$(API_PORT)" npm run dev

IMAGE ?= advanced-ai-agent:dev
EXECUTOR_IMAGE ?= agent-executor:dev

image:
	docker build --platform linux/amd64 -f "$(ROOT)/simple_chat_agent/Dockerfile" -t "$(IMAGE)" "$(ROOT)"

executor-image:
	docker build -f "$(ROOT)/simple_chat_agent/worker/sandbox/Dockerfile.executor" -t "$(EXECUTOR_IMAGE)" "$(ROOT)"

gen:
	cd "$(WEB)" && node scripts/gen.mjs

crash-worker:
	@"$(ROOT)/scripts/crash-worker.sh" $(if $(filter command line,$(origin TASK_QUEUE)),"$(TASK_QUEUE)",)

dev-worker:
	@$(LOAD_ENV); \
	export TEMPORAL_TEST_ADDRESS="127.0.0.1:$(TEMPORAL_PORT)" TEMPORAL_ADDRESS="127.0.0.1:$(TEMPORAL_PORT)"; \
	export SIMPLE_CHAT_REDIS_URL="$${SIMPLE_CHAT_REDIS_URL:-redis://127.0.0.1:$(REDIS_PORT)/0}"; \
	export WORKSPACE_EXECUTOR_URL="$${WORKSPACE_EXECUTOR_URL:-http://127.0.0.1:$(EXECUTOR_PORT)}"; \
	export AGENT_ENVIRONMENT="$${AGENT_ENVIRONMENT:-local}"; \
	cd "$(ROOT)" && exec "$(PYTHON)" -m simple_chat_agent.agent.dev_worker --task-queue "$(TASK_QUEUE)"

# The API holds no provider keys (P2 decision 2), so they are unset after loading .env.local.
# Local login: SIMPLE_CHAT_LOCAL_AUTH_USERNAME / _PASSWORD (default demo / demo). Without
# SIMPLE_CHAT_JWT_SECRET a fresh one is made per run, so a restart signs everyone out.
dev-api:
	@$(LOAD_ENV); \
	unset ANTHROPIC_API_KEY OPENAI_API_KEY GEMINI_API_KEY GOOGLE_API_KEY TYPESAFE_API_KEY TYPESAFE_AI_API_KEY; \
	export TEMPORAL_ADDRESS="127.0.0.1:$(TEMPORAL_PORT)" TEMPORAL_UI_URL="$${TEMPORAL_UI_URL:-http://127.0.0.1:$(TEMPORAL_UI_PORT)}"; \
	export CODING_AGENT_TASK_QUEUE="$(TASK_QUEUE)"; \
	export SIMPLE_CHAT_REDIS_URL="$${SIMPLE_CHAT_REDIS_URL:-redis://127.0.0.1:$(REDIS_PORT)/0}"; \
	export WORKSPACE_EXECUTOR_URL="$${WORKSPACE_EXECUTOR_URL:-http://127.0.0.1:$(EXECUTOR_PORT)}"; \
	export AGENT_ENVIRONMENT="$${AGENT_ENVIRONMENT:-local}" SIMPLE_CHAT_AUTH_MODE=local SIMPLE_CHAT_LOCAL_AUTH_ENABLED=1; \
	export SIMPLE_CHAT_JWT_SECRET="$${SIMPLE_CHAT_JWT_SECRET:-$$(openssl rand -hex 32)}"; \
	cd "$(ROOT)" && exec "$(PYTHON)" -m uvicorn simple_chat_agent.api.main:app --host 127.0.0.1 --port "$(API_PORT)"

dev-stack:
	@echo "Local stack: run each in its own terminal from $(ROOT). Skip what is already running."
	@echo
	@echo "1. Redis (AOF) on $(REDIS_PORT):"
	@echo "   redis-server --port $(REDIS_PORT) --appendonly yes --dir $(ROOT)/.simple_chat_agent"
	@echo
	@echo "2. Temporal dev server on $(TEMPORAL_PORT) (UI on $(TEMPORAL_UI_PORT)). Never the server on 7233:"
	@echo "   temporal server start-dev --port $(TEMPORAL_PORT) --ui-port $(TEMPORAL_UI_PORT) --db-filename dev.db \\"
	@echo "     --log-level warn --search-attribute UserEmail=Keyword --search-attribute CodingAgentOwner=Keyword"
	@echo
	@echo "3. Sandbox executor on $(EXECUTOR_PORT) (see simple_chat_agent/worker/sandbox/README.md):"
	@echo "   make executor-image"
	@echo "   (set -a; . ./.env.local; set +a; docker run -d --name agent-executor -p 127.0.0.1:$(EXECUTOR_PORT):8082 \\"
	@echo "     -e WORKSPACE_EXECUTOR_TOKEN -e WORKSPACE_EXECUTOR_ROOT=/workspaces \\"
	@echo "     --security-opt seccomp=unconfined --security-opt apparmor=unconfined \\"
	@echo "     -v agent-executor-workspaces:/workspaces agent-executor:dev)"
	@echo "   curl -s 127.0.0.1:$(EXECUTOR_PORT)/healthz"
	@echo
	@echo "4. Dev worker (task queue $(TASK_QUEUE)):   make dev-worker"
	@echo "5. API on $(API_PORT), no provider keys:   make dev-api"
	@echo "6. Product view:   make web, then open http://127.0.0.1:$(API_PORT)/"
	@echo "   or live reload: make web-dev, sign in once at http://127.0.0.1:$(API_PORT)/, then use the Vite URL"
	@echo
	@echo "Crash the worker:  make crash-worker   (then make dev-worker to bring it back)"
