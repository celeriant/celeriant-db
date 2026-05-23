SHELL := /bin/bash
.DEFAULT_GOAL := help

CONFIG := config.env
include $(CONFIG)

PROJECT_ROOT := $(shell cd ../.. && pwd)
DATA_NODES := $(LEADER_HOST) $(FOLLOWER_HOST)

# Resolve INFRA_HOST from INFRA_MODE so config.env is the only edit point
# when flipping between local docker-compose and the remote rpi4 'cluster'.
ifeq ($(INFRA_MODE),local)
INFRA_HOST := $(INFRA_HOST_LOCAL)
else ifeq ($(INFRA_MODE),remote)
INFRA_HOST := $(INFRA_HOST_REMOTE)
else
$(error INFRA_MODE must be 'local' or 'remote' (got '$(INFRA_MODE)'))
endif
export INFRA_HOST

# Colours
RED    := \033[0;31m
GREEN  := \033[0;32m
YELLOW := \033[0;33m
BOLD   := \033[1m
RESET  := \033[0m

.PHONY: help setup-all setup-nodes setup-nvme setup-ktls setup-infra certs \
        deploy build ship restart stop start logs run-test teardown teardown-data \
        update-service \
        stop-cs1 stop-cs2 stop-minio kill-cs1 kill-cs2 kill-minio \
        kill-cs1-no-restart kill-cs2-no-restart \
        start-cs1 start-cs2 start-minio status start-infra

help: ## Show this help
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*## "}; {printf "  $(GREEN)%-18s$(RESET) %s\n", $$1, $$2}'

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
		bash setup-nodes.sh $$HOST $(INFRA_HOST) $(MEMORY_CONSUMPTION_PERCENT) $(SHARD_LOG_PREALLOCATE_BYTES) $(RESERVE_COORDINATOR_SHARD) &\
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

deploy: build ship update-service restart ## Build, ship binary, refresh systemd unit (picks up INFRA_MODE), restart

update-service: ## Refresh the systemd unit + promtail config on data nodes (re-runnable; called by `deploy`)
	@for HOST in $(DATA_NODES); do \
		printf "$(BOLD)>>> Refreshing service config on $$HOST (S3 endpoint: $(INFRA_HOST))$(RESET)\n"; \
		bash update-service.sh $$HOST $(INFRA_HOST) $(MEMORY_CONSUMPTION_PERCENT) $(SHARD_LOG_PREALLOCATE_BYTES) $(RESERVE_COORDINATOR_SHARD); \
	done
	@printf "$(GREEN)Service config refreshed on all data nodes.$(RESET)\n"

build: ## Cross-compile Celeriant + celeriant-wal-inspect for ARM64
	@printf "$(BOLD)>>> Cross-compiling for $(CARGO_TARGET)$(RESET)\n"
	cd $(PROJECT_ROOT) && \
		CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc \
		cargo build --release --target $(CARGO_TARGET) -p celeriant -p celeriant_wal_inspect
	@printf "$(GREEN)Build complete:$(RESET)\n"
	@printf "  $(PROJECT_ROOT)/$(CELERIANT_BINARY)\n"
	@printf "  $(PROJECT_ROOT)/$(WAL_INSPECT_BINARY)\n"

ship: ## Ship binaries (celeriant + celeriant-wal-inspect) to data nodes (without restarting)
	@BINARY="$(PROJECT_ROOT)/$(CELERIANT_BINARY)"; \
	WAL_INSPECT="$(PROJECT_ROOT)/$(WAL_INSPECT_BINARY)"; \
	if [ ! -f "$$BINARY" ]; then \
		printf "$(RED)Binary not found at $$BINARY — run 'make build' first$(RESET)\n"; \
		exit 1; \
	fi; \
	if [ ! -f "$$WAL_INSPECT" ]; then \
		printf "$(RED)wal-inspect binary not found at $$WAL_INSPECT — run 'make build' first$(RESET)\n"; \
		exit 1; \
	fi; \
	for HOST in $(DATA_NODES); do \
		printf "$(BOLD)>>> Shipping to $$HOST$(RESET)\n"; \
		scp "$$BINARY" $$HOST:/tmp/celeriant; \
		scp "$$WAL_INSPECT" $$HOST:/tmp/celeriant-wal-inspect; \
		ssh $$HOST 'sudo mv /tmp/celeriant $(REMOTE_BIN) && sudo chmod +x $(REMOTE_BIN) && \
		            sudo mv /tmp/celeriant-wal-inspect $(REMOTE_WAL_INSPECT_BIN) && sudo chmod +x $(REMOTE_WAL_INSPECT_BIN)'; \
	done
	@printf "$(GREEN)Binaries deployed to all nodes.$(RESET)\n"

# ---------------------------------------------------------------------------
# Service management
# ---------------------------------------------------------------------------

restart: stop start ## Restart Celeriant on all nodes (leader first)

stop: ## Stop Celeriant on all nodes
	@for HOST in $(FOLLOWER_HOST) $(LEADER_HOST); do \
		printf "$(BOLD)>>> Stopping celeriant on $$HOST$(RESET)\n"; \
		ssh $$HOST 'sudo systemctl stop celeriant 2>/dev/null || true'; \
	done

stop-cs1: ## Graceful stop cs1
	@printf "$(BOLD)>>> Stopping celeriant on $(LEADER_HOST) (cs1)$(RESET)\n"
	@ssh $(LEADER_HOST) 'sudo systemctl stop celeriant'

stop-cs2: ## Graceful stop cs2
	@printf "$(BOLD)>>> Stopping celeriant on $(FOLLOWER_HOST) (cs2)$(RESET)\n"
	@ssh $(FOLLOWER_HOST) 'sudo systemctl stop celeriant'

stop-minio: ## Graceful stop MinIO on infra node
	@printf "$(BOLD)>>> Stopping MinIO on $(INFRA_HOST)$(RESET)\n"
	@bash infra-exec.sh stop minio

kill-cs1: ## Hard kill cs1 (SIGKILL). Note: systemd `Restart=on-failure` auto-restarts after RestartSec=3.
	@printf "$(RED)>>> Killing celeriant on $(LEADER_HOST) (cs1)$(RESET)\n"
	@ssh $(LEADER_HOST) 'sudo systemctl kill -s KILL celeriant'

kill-cs2: ## Hard kill cs2 (SIGKILL). Note: systemd `Restart=on-failure` auto-restarts after RestartSec=3.
	@printf "$(RED)>>> Killing celeriant on $(FOLLOWER_HOST) (cs2)$(RESET)\n"
	@ssh $(FOLLOWER_HOST) 'sudo systemctl kill -s KILL celeriant'

kill-cs1-no-restart: ## Hard kill cs1 (SIGKILL) and cancel systemd auto-restart. Pairs with start-cs1.
	@printf "$(RED)>>> Killing celeriant on $(LEADER_HOST) (cs1) [no auto-restart]$(RESET)\n"
	@ssh $(LEADER_HOST) 'sudo systemctl kill -s KILL celeriant; sudo systemctl stop celeriant'

kill-cs2-no-restart: ## Hard kill cs2 (SIGKILL) and cancel systemd auto-restart. Pairs with start-cs2.
	@printf "$(RED)>>> Killing celeriant on $(FOLLOWER_HOST) (cs2) [no auto-restart]$(RESET)\n"
	@ssh $(FOLLOWER_HOST) 'sudo systemctl kill -s KILL celeriant; sudo systemctl stop celeriant'

pause-cs1: ## SIGSTOP cs1 — freezes the process without terminating it. Pairs with resume-cs1.
	@printf "$(RED)>>> Pausing celeriant on $(LEADER_HOST) (cs1) [SIGSTOP]$(RESET)\n"
	@ssh $(LEADER_HOST) 'sudo systemctl kill -s STOP celeriant'

pause-cs2: ## SIGSTOP cs2 — freezes the process without terminating it. Pairs with resume-cs2.
	@printf "$(RED)>>> Pausing celeriant on $(FOLLOWER_HOST) (cs2) [SIGSTOP]$(RESET)\n"
	@ssh $(FOLLOWER_HOST) 'sudo systemctl kill -s STOP celeriant'

resume-cs1: ## SIGCONT cs1 — unfreezes a previously-SIGSTOPped cs1.
	@printf "$(GREEN)>>> Resuming celeriant on $(LEADER_HOST) (cs1) [SIGCONT]$(RESET)\n"
	@ssh $(LEADER_HOST) 'sudo systemctl kill -s CONT celeriant'

resume-cs2: ## SIGCONT cs2 — unfreezes a previously-SIGSTOPped cs2.
	@printf "$(GREEN)>>> Resuming celeriant on $(FOLLOWER_HOST) (cs2) [SIGCONT]$(RESET)\n"
	@ssh $(FOLLOWER_HOST) 'sudo systemctl kill -s CONT celeriant'

skew-clock: ## Shift HOST's system clock by OFFSET (e.g. OFFSET="+2 seconds" or "-2 seconds"). Disables NTP first so systemd-timesyncd doesn't immediately correct the jump. Requires HOST, OFFSET.
	@if [ -z "$(HOST)" ] || [ -z "$(OFFSET)" ]; then \
		printf "$(RED)ERROR: skew-clock requires HOST and OFFSET$(RESET)\n"; exit 2; \
	fi
	@printf "$(RED)>>> Skewing clock on $(HOST) by '$(OFFSET)'$(RESET)\n"
	@ssh $(HOST) 'sudo timedatectl set-ntp false && sudo date -s "$(OFFSET)" >/dev/null'

restore-clock: ## Re-enable NTP on HOST to restore correct time. Requires HOST.
	@if [ -z "$(HOST)" ]; then \
		printf "$(RED)ERROR: restore-clock requires HOST$(RESET)\n"; exit 2; \
	fi
	@printf "$(GREEN)>>> Restoring clock on $(HOST) (re-enabling NTP)$(RESET)\n"
	@ssh $(HOST) 'sudo timedatectl set-ntp true'

fill-disk: ## Fill HOST's data disk to within RESERVE_MB of full via fallocate. Requires HOST. Optional RESERVE_MB (default 50).
	@if [ -z "$(HOST)" ]; then \
		printf "$(RED)ERROR: fill-disk requires HOST$(RESET)\n"; exit 2; \
	fi
	$(eval RESERVE_MB ?= 50)
	@printf "$(RED)>>> Filling data disk on $(HOST) to within $(RESERVE_MB) MiB of full$(RESET)\n"
	@ssh $(HOST) "sudo sh -c 'FREE=\$$(df --output=avail -B1 /var/lib/celeriant | tail -n1); RESERVE=\$$(($(RESERVE_MB)*1024*1024)); FILL=\$$((FREE - RESERVE)); if [ \$$FILL -gt 0 ]; then fallocate -l \$$FILL /var/lib/celeriant/.chaos_filler && echo \"Allocated \$$FILL bytes\"; else echo \"Already at or below reserve threshold\"; fi'"

clean-disk: ## Remove the chaos filler file from HOST. Safe to run repeatedly. Requires HOST.
	@if [ -z "$(HOST)" ]; then \
		printf "$(RED)ERROR: clean-disk requires HOST$(RESET)\n"; exit 2; \
	fi
	@printf "$(GREEN)>>> Removing chaos filler on $(HOST)$(RESET)\n"
	@ssh $(HOST) 'sudo rm -f /var/lib/celeriant/.chaos_filler'

partition-host: ## One-way nftables drop on SRC blocking outbound TCP to DST:PORT. Requires SRC, DST, PORT.
	@if [ -z "$(SRC)" ] || [ -z "$(DST)" ] || [ -z "$(PORT)" ]; then \
		printf "$(RED)ERROR: partition-host requires SRC, DST and PORT$(RESET)\n"; exit 2; \
	fi
	@printf "$(RED)>>> Partitioning $(SRC) -> $(DST):$(PORT) (nftables drop on SRC:output)$(RESET)\n"
	@ssh $(SRC) "sudo nft 'add table inet celeriant_chaos' 2>/dev/null || true; \
	             sudo nft 'add chain inet celeriant_chaos output { type filter hook output priority 0 ; }' 2>/dev/null || true; \
	             sudo nft 'add rule inet celeriant_chaos output ip daddr $(DST) tcp dport $(PORT) drop'"

heal-host: ## Remove the nftables drop rules for DST:PORT from SRC. Idempotent. Requires SRC, DST, PORT.
	@if [ -z "$(SRC)" ] || [ -z "$(DST)" ] || [ -z "$(PORT)" ]; then \
		printf "$(RED)ERROR: heal-host requires SRC, DST and PORT$(RESET)\n"; exit 2; \
	fi
	@printf "$(GREEN)>>> Healing $(SRC) -> $(DST):$(PORT)$(RESET)\n"
	@ssh $(SRC) "sudo nft -a list chain inet celeriant_chaos output 2>/dev/null \
	             | grep -F 'daddr $(DST)' | grep -F 'dport $(PORT)' \
	             | grep -oP 'handle \K\d+' \
	             | while read h; do sudo nft delete rule inet celeriant_chaos output handle \$$h; done; \
	             true"

kill-minio: ## Hard kill MinIO on infra node
	@printf "$(RED)>>> Killing MinIO on $(INFRA_HOST)$(RESET)\n"
	@bash infra-exec.sh kill minio

start: ## Start Celeriant (leader first, then follower)
	@printf "$(BOLD)>>> Starting celeriant on $(LEADER_HOST) (leader)$(RESET)\n"
	@ssh $(LEADER_HOST) 'sudo systemctl start celeriant'
	@printf "Waiting for leader to acquire S3 lease...\n"
	@sleep 5
	@printf "$(BOLD)>>> Starting celeriant on $(FOLLOWER_HOST) (follower)$(RESET)\n"
	@ssh $(FOLLOWER_HOST) 'sudo systemctl start celeriant'
	@printf "$(GREEN)Cluster started.$(RESET)\n"

start-cs1: ## Start cs1
	@printf "$(BOLD)>>> Starting celeriant on $(LEADER_HOST) (cs1)$(RESET)\n"
	@ssh $(LEADER_HOST) 'sudo systemctl start celeriant'

start-cs2: ## Start cs2
	@printf "$(BOLD)>>> Starting celeriant on $(FOLLOWER_HOST) (cs2)$(RESET)\n"
	@ssh $(FOLLOWER_HOST) 'sudo systemctl start celeriant'

start-minio: ## Start MinIO on infra node
	@printf "$(BOLD)>>> Starting MinIO on $(INFRA_HOST)$(RESET)\n"
	@bash infra-exec.sh start minio

ifeq ($(INFRA_MODE),local)
MINIO_HEALTH_CMD = curl -sf http://localhost:9000/minio/health/live >/dev/null 2>&1
else
MINIO_HEALTH_CMD = ssh $(INFRA_HOST) 'curl -sf http://localhost:9000/minio/health/live >/dev/null 2>&1'
endif

start-infra: ## Bring up the full infra stack and wait until MinIO is healthy
	@printf "$(BOLD)>>> Starting infra stack on $(INFRA_HOST)$(RESET)\n"
	@bash infra-exec.sh up -d
	@printf "Waiting for MinIO to become healthy...\n"
	@for i in $$(seq 1 60); do \
		if $(MINIO_HEALTH_CMD); then \
			printf "$(GREEN)MinIO healthy.$(RESET)\n"; exit 0; \
		fi; \
		sleep 1; \
	done; \
	printf "$(RED)MinIO failed to start within 60s$(RESET)\n"; exit 1

status: ## Show service status on all nodes
	@for HOST in $(DATA_NODES); do \
		printf "$(BOLD)>>> $$HOST$(RESET)\n"; \
		ssh $$HOST 'systemctl is-active celeriant 2>/dev/null || echo "not running"'; \
	done
	@printf "$(BOLD)>>> $(INFRA_HOST) (minio)$(RESET)\n"
	@bash infra-exec.sh ps minio --format "{{.State}}" 2>/dev/null || echo "not running"

logs: ## Tail logs from both data nodes (Ctrl+C to stop)
	@printf "$(YELLOW)Tailing logs from $(LEADER_HOST) and $(FOLLOWER_HOST)... Ctrl+C to stop.$(RESET)\n"
	@ssh $(LEADER_HOST) 'journalctl -u celeriant -f --no-pager' &
	@ssh $(FOLLOWER_HOST) 'journalctl -u celeriant -f --no-pager' &
	@wait

vitals: ## Snapshot SoC temp, throttle bitmask, CPU freq for data nodes + infra rpi
	@bash vitals.sh $(DATA_NODES) $(INFRA_HOST)

# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------

run-test: ## Run cluster pool benchmark (builds on macOS/Windows/Linux)
	@printf "$(BOLD)>>> Running celeriant-bench against $(LEADER_HOST):$(CLIENT_PORT) + $(FOLLOWER_HOST):$(CLIENT_PORT)$(RESET)\n"
	cd $(PROJECT_ROOT) && \
		cargo run --release -p celeriant_bench -- \
		--address1 $(LEADER_HOST):$(CLIENT_PORT) \
		--address2 $(FOLLOWER_HOST):$(CLIENT_PORT) \
		--server-name $(LEADER_HOST) \
		--tasks $(CLUSTER_THROUGHPUT_CONNECTIONS) \
		--duration $(CLUSTER_DURATION) \
		--ca-cert deploy/rpi-cluster/certs/client-ca.crt \
		--client-cert deploy/rpi-cluster/certs/client.crt \
		--client-key deploy/rpi-cluster/certs/client.key

# ---------------------------------------------------------------------------
# Teardown
# ---------------------------------------------------------------------------

teardown: stop ## Stop all services (Celeriant + infra)
	@printf "$(BOLD)>>> Stopping infra on $(INFRA_HOST)$(RESET)\n"
	@bash infra-exec.sh down
	@printf "$(GREEN)All services stopped.$(RESET)\n"

# Wipes only the side INFRA_MODE currently points at. To clean both:
#   make teardown-data; edit config.env to flip INFRA_MODE; make teardown-data.
teardown-data: teardown ## Stop all services AND wipe all data (DESTRUCTIVE)
	@printf "$(RED)$(BOLD)Wiping ALL Celeriant data on $(LEADER_HOST), $(FOLLOWER_HOST), and $(INFRA_HOST)$(RESET)\n"
	@for HOST in $(DATA_NODES); do \
		ssh $$HOST 'sudo rm -rf $(REMOTE_DATA_ROOT)/*'; \
	done
	@bash infra-exec.sh down -v
	@printf "$(GREEN)All data wiped.$(RESET)\n"
