SHELL := /bin/bash
.DEFAULT_GOAL := help

CONFIG := config.env
include $(CONFIG)

PROJECT_ROOT := $(shell cd ../.. && pwd)
DATA_NODES := $(LEADER_HOST) $(FOLLOWER_HOST)

# Colours
RED    := \033[0;31m
GREEN  := \033[0;32m
YELLOW := \033[0;33m
BOLD   := \033[1m
RESET  := \033[0m

.PHONY: help setup-all setup-nodes setup-nvme setup-ktls setup-infra certs \
        deploy build ship restart stop start logs run-test teardown teardown-data

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-18s$(RESET) %s\n", $$1, $$2}'

# ---------------------------------------------------------------------------
# Full setup (first-time)
# ---------------------------------------------------------------------------

setup-all: setup-nodes setup-nvme certs setup-infra deploy ## First-time full setup (excludes kTLS kernel — run separately)
	@printf "$(GREEN)Setup complete.$(RESET)\n"
	@printf "$(YELLOW)NOTE:$(RESET) Run 'make setup-ktls' separately if kTLS kernel is not yet built.\n"
	@printf "Then 'make start' to bring up the cluster.\n"

# ---------------------------------------------------------------------------
# Node setup
# ---------------------------------------------------------------------------

setup-nodes: ## One-time OS prep on data nodes (limits, systemd, promtail) — runs in parallel
	@for HOST in $(DATA_NODES); do \
		printf "$(BOLD)>>> Setting up $$HOST$(RESET)\n"; \
		bash setup-nodes.sh $$HOST $(INFRA_HOST) &\
	done; \
	wait
	@printf "$(GREEN)All nodes configured.$(RESET)\n"

setup-nvme: ## Format and mount NVMe on data nodes (DESTRUCTIVE — prompts per node, sequential)
	@for HOST in $(DATA_NODES); do \
		printf "$(BOLD)>>> NVMe setup on $$HOST$(RESET)\n"; \
		bash setup-nvme.sh $$HOST; \
	done

setup-ktls: ## Build kTLS kernel on data nodes (30-60 min each, runs in parallel)
	@printf "$(YELLOW)This will build the kTLS kernel on both data nodes.$(RESET)\n"
	@printf "$(YELLOW)Each build takes 30-60 minutes. Nodes will need a reboot after.$(RESET)\n"
	@read -p "Continue? [y/N] " confirm && [ "$$confirm" = "y" ] || exit 1
	@for HOST in $(DATA_NODES); do \
		printf "$(BOLD)>>> Starting kTLS kernel build on $$HOST$(RESET)\n"; \
		scp setup-ktls.sh $$HOST:/tmp/setup-ktls.sh; \
		ssh $$HOST "bash /tmp/setup-ktls.sh" & \
	done; \
	wait
	@printf "\n$(GREEN)Kernel builds complete.$(RESET)\n"
	@printf "$(YELLOW)Reboot both nodes, then verify with:$(RESET)\n"
	@printf "  ssh $(LEADER_HOST) 'sudo modprobe tls && lsmod | grep tls'\n"
	@printf "  ssh $(FOLLOWER_HOST) 'sudo modprobe tls && lsmod | grep tls'\n"

# ---------------------------------------------------------------------------
# Infrastructure (cluster node)
# ---------------------------------------------------------------------------

setup-infra: ## Deploy Docker + infra stack (MinIO, Prometheus, Loki, Grafana) to infra node
	@bash setup-infra.sh

# ---------------------------------------------------------------------------
# TLS certificates
# ---------------------------------------------------------------------------

certs: ## Generate and distribute TLS certificates
	@bash gen-certs.sh

# ---------------------------------------------------------------------------
# Build & deploy (recurring)
# ---------------------------------------------------------------------------

deploy: build ship restart ## Build, ship binary, and restart services

build: ## Cross-compile Celeriant for ARM64
	@printf "$(BOLD)>>> Cross-compiling for $(CARGO_TARGET)$(RESET)\n"
	cd $(PROJECT_ROOT) && \
		CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc \
		cargo build --release --target $(CARGO_TARGET) -p celeriant
	@printf "$(GREEN)Build complete: $(PROJECT_ROOT)/$(CELERIANT_BINARY)$(RESET)\n"

ship: ## Ship binary to data nodes (without restarting)
	@BINARY="$(PROJECT_ROOT)/$(CELERIANT_BINARY)"; \
	if [ ! -f "$$BINARY" ]; then \
		printf "$(RED)Binary not found at $$BINARY — run 'make build' first$(RESET)\n"; \
		exit 1; \
	fi; \
	for HOST in $(DATA_NODES); do \
		printf "$(BOLD)>>> Shipping to $$HOST$(RESET)\n"; \
		scp "$$BINARY" $$HOST:/tmp/celeriant; \
		ssh $$HOST 'sudo mv /tmp/celeriant $(REMOTE_BIN) && sudo chmod +x $(REMOTE_BIN)'; \
	done
	@printf "$(GREEN)Binary deployed to all nodes.$(RESET)\n"

# ---------------------------------------------------------------------------
# Service management
# ---------------------------------------------------------------------------

restart: stop start ## Restart Celeriant on all nodes (leader first)

stop: ## Stop Celeriant on all nodes
	@for HOST in $(FOLLOWER_HOST) $(LEADER_HOST); do \
		printf "$(BOLD)>>> Stopping celeriant on $$HOST$(RESET)\n"; \
		ssh $$HOST 'sudo systemctl stop celeriant 2>/dev/null || true'; \
	done

start: ## Start Celeriant (leader first, then follower)
	@printf "$(BOLD)>>> Starting celeriant on $(LEADER_HOST) (leader)$(RESET)\n"
	@ssh $(LEADER_HOST) 'sudo systemctl start celeriant'
	@printf "Waiting for leader to acquire S3 lease...\n"
	@sleep 5
	@printf "$(BOLD)>>> Starting celeriant on $(FOLLOWER_HOST) (follower)$(RESET)\n"
	@ssh $(FOLLOWER_HOST) 'sudo systemctl start celeriant'
	@printf "$(GREEN)Cluster started.$(RESET)\n"

status: ## Show service status on all nodes
	@for HOST in $(DATA_NODES); do \
		printf "$(BOLD)>>> $$HOST$(RESET)\n"; \
		ssh $$HOST 'systemctl is-active celeriant 2>/dev/null || echo "not running"'; \
	done

logs: ## Tail logs from both data nodes (Ctrl+C to stop)
	@printf "$(YELLOW)Tailing logs from $(LEADER_HOST) and $(FOLLOWER_HOST)... Ctrl+C to stop.$(RESET)\n"
	@ssh $(LEADER_HOST) 'journalctl -u celeriant -f --no-pager' &
	@ssh $(FOLLOWER_HOST) 'journalctl -u celeriant -f --no-pager' &
	@wait

# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------

run-test: ## Run rpi_cluster_bench integration test against the cluster
	@printf "$(BOLD)>>> Running rpi_cluster_bench against $(LEADER_HOST):$(CLIENT_PORT)$(RESET)\n"
	cd $(PROJECT_ROOT) && \
		CLUSTER_ADDRESS=$(LEADER_HOST):$(CLIENT_PORT) \
		CLUSTER_SERVER_NAME=$(LEADER_HOST) \
		CLUSTER_THROUGHPUT_CONNECTIONS=$(CLUSTER_THROUGHPUT_CONNECTIONS) \
		CLUSTER_LATENCY_CONNECTIONS=$(CLUSTER_LATENCY_CONNECTIONS) \
		CLUSTER_DURATION=$(CLUSTER_DURATION) \
		cargo run --release -p celeriant_integration_tests -- --test rpi_cluster_bench

# ---------------------------------------------------------------------------
# Teardown
# ---------------------------------------------------------------------------

teardown: stop ## Stop all services (Celeriant + infra)
	@printf "$(BOLD)>>> Stopping infra on $(INFRA_HOST)$(RESET)\n"
	@ssh $(INFRA_HOST) 'cd ~/celeriant-infra && docker compose down'
	@printf "$(GREEN)All services stopped.$(RESET)\n"

teardown-data: teardown ## Stop all services AND wipe all data (DESTRUCTIVE)
	@printf "$(RED)$(BOLD)This will delete ALL Celeriant data on $(LEADER_HOST) and $(FOLLOWER_HOST),$(RESET)\n"
	@printf "$(RED)$(BOLD)and all MinIO/Prometheus/Loki/Grafana data on $(INFRA_HOST).$(RESET)\n"
	@read -p "Type 'yes' to confirm: " confirm && [ "$$confirm" = "yes" ] || exit 1
	@for HOST in $(DATA_NODES); do \
		ssh $$HOST 'sudo rm -rf $(REMOTE_DATA_ROOT)/*'; \
	done
	@ssh $(INFRA_HOST) 'cd ~/celeriant-infra && docker compose down -v'
	@printf "$(GREEN)All data wiped.$(RESET)\n"
