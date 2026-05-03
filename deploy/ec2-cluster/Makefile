SHELL := /bin/bash
.DEFAULT_GOAL := help

CLUSTER_ENV := .cluster-env
-include $(CLUSTER_ENV)

# Colours
RED    := \033[0;31m
GREEN  := \033[0;32m
YELLOW := \033[0;33m
BOLD   := \033[1m
RESET  := \033[0m

SSH_OPTS := -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null
ifdef KEY_FILE
  SSH_OPTS += -i $(KEY_FILE)
endif

SSH := ssh $(SSH_OPTS) ec2-user
SCP := scp $(SSH_OPTS)

# Benchmark defaults (override: make run-benchmark BENCH_TASKS=4000)
BENCH_TASKS ?= 8000
BENCH_CONNS ?= $(BENCH_TASKS)
BENCH_DURATION ?= 15

REPO_ROOT := $(shell cd ../.. && pwd)

# Cluster profile — encodes the canonical benchmark shapes from README/docs.
# Override individual flags with CDK_ARGS for one-offs (e.g. CDK_ARGS="-c keyPair=my-key").
PROFILE ?= default

ifeq ($(PROFILE),i4i-32c)
  override CDK_ARGS += -c instanceType=i4i.8xlarge -c clientInstanceType=c7i.4xlarge -c clientCount=3
else ifeq ($(PROFILE),i4g-32c)
  override CDK_ARGS += -c instanceType=i4g.8xlarge -c clientInstanceType=c7g.4xlarge -c clientCount=3
else ifneq ($(PROFILE),default)
  $(error Unknown PROFILE '$(PROFILE)'. Valid: default, i4i-32c, i4g-32c)
endif

.PHONY: help build build-arm infra certs deploy start stop restart status logs \
        dashboard run-benchmark run-sweep sync-env teardown teardown-data

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(RESET) %s\n", $$1, $$2}'

# ---------------------------------------------------------------------------
# Infrastructure
# ---------------------------------------------------------------------------

build: ## Build x86_64 release binaries in Docker (for c6id, i4i, etc.)
	@printf "$(BOLD)>>> Building x86_64 release binaries in amazonlinux:2023 container$(RESET)\n"
	cd $(REPO_ROOT) && docker run --rm -v "$$(pwd)":/build -w /build amazonlinux:2023 bash -c '\
		set -e && \
		yum install -y gcc gcc-c++ openssl-devel perl-FindBin perl-File-Compare make git >/dev/null 2>&1 && \
		curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y >/dev/null 2>&1 && \
		source $$HOME/.cargo/env && \
		cargo build --release -p celeriant -p celeriant_integration_tests -p celeriant_cli'
	@printf "$(GREEN)Build complete (x86_64).$(RESET)\n"

build-arm: ## Build ARM64 release binaries in Docker (for i4g, c7g, etc. — requires QEMU binfmt)
	@printf "$(BOLD)>>> Building ARM64 release binaries in amazonlinux:2023 container (QEMU)$(RESET)\n"
	@docker run --rm --privileged tonistiigi/binfmt --install arm64 >/dev/null 2>&1 || true
	cd $(REPO_ROOT) && docker run --rm --platform linux/arm64 -v "$$(pwd)":/build -w /build amazonlinux:2023 bash -c '\
		set -e && \
		yum install -y gcc gcc-c++ openssl-devel perl-FindBin perl-File-Compare make git >/dev/null 2>&1 && \
		curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y >/dev/null 2>&1 && \
		source $$HOME/.cargo/env && \
		cargo build --release -p celeriant -p celeriant_integration_tests -p celeriant_cli'
	@printf "$(GREEN)Build complete (arm64).$(RESET)\n"

infra: ## Deploy CDK stack (PROFILE=i4i-32c|i4g-32c|default; CDK_ARGS for extra flags, e.g. -c keyPair=my-key)
	npx cdk deploy --require-approval never $(CDK_ARGS)

# ---------------------------------------------------------------------------
# TLS certificates
# ---------------------------------------------------------------------------

certs: ## Generate TLS certs (requires deployed infra — reads IPs from stack outputs)
	@bash scripts/generate-certs.sh \
		$$(aws cloudformation describe-stacks --stack-name CeleriantKtlsTestStack \
			--query "Stacks[0].Outputs[?OutputKey=='LeaderPrivateIp'].OutputValue" --output text) \
		$$(aws cloudformation describe-stacks --stack-name CeleriantKtlsTestStack \
			--query "Stacks[0].Outputs[?OutputKey=='FollowerPrivateIp'].OutputValue" --output text) \
		$$(aws cloudformation describe-stacks --stack-name CeleriantKtlsTestStack \
			--query "Stacks[0].Outputs[?OutputKey=='ClientPrivateIp'].OutputValue" --output text)

# ---------------------------------------------------------------------------
# Build & deploy
# ---------------------------------------------------------------------------

deploy: ## Deploy binaries, certs, env files to nodes (use KEY_ARG for SSH key, e.g. KEY_ARG="--key-file ~/.ssh/my-key.pem")
	bash scripts/deploy.sh $(KEY_ARG)

sync-env: ## Re-read CDK outputs into .cluster-env (run if stack was redeployed)
	@STACK=CeleriantKtlsTestStack; \
	get() { aws cloudformation describe-stacks --stack-name $$STACK \
		--query "Stacks[0].Outputs[?OutputKey=='$$1'].OutputValue" --output text; }; \
	CC=$$(get ClientCount 2>/dev/null || echo 1); \
	CPUBS=$$(get ClientPublicIp); \
	for i in $$(seq 2 $$CC); do CPUBS="$$CPUBS,$$(get Client$${i}PublicIp)"; done; \
	printf "LEADER_PUB=%s\nFOLLOWER_PUB=%s\nCLIENT_PUBS=%s\nCLIENT_COUNT=%s\nLEADER_IP=%s\nFOLLOWER_IP=%s\nBUCKET=%s\nREGION=%s\nINSTANCE_TYPE=%s\nCLIENT_INSTANCE_TYPE=%s\nSTORAGE_TYPE=%s\nARCH=%s\nKEY_FILE=$(KEY_FILE)\n" \
		$$(get LeaderPublicIp) $$(get FollowerPublicIp) "$$CPUBS" "$$CC" \
		$$(get LeaderPrivateIp) $$(get FollowerPrivateIp) \
		$$(get BucketName) $$(get Region) \
		$$(get InstanceType) $$(get ClientInstanceType) $$(get StorageType) $$(get Architecture) \
		> $(CLUSTER_ENV); \
	printf "$(GREEN)Wrote $(CLUSTER_ENV)$(RESET)\n"; \
	cat $(CLUSTER_ENV)

# ---------------------------------------------------------------------------
# Service management (mirrors deploy/rpi-cluster/Makefile)
# ---------------------------------------------------------------------------

start: ## Start cluster (leader first, then follower)
	@if [[ -z "$(LEADER_PUB)" ]]; then printf "$(RED)No .cluster-env — run 'make deploy' or 'make sync-env' first$(RESET)\n"; exit 1; fi
	@printf "$(BOLD)>>> Starting celeriant on leader ($(LEADER_PUB))$(RESET)\n"
	@$(SSH)@$(LEADER_PUB) 'sudo systemctl start celeriant'
	@printf "Waiting for leader to acquire S3 lease...\n"
	@sleep 5
	@printf "$(BOLD)>>> Starting celeriant on follower ($(FOLLOWER_PUB))$(RESET)\n"
	@$(SSH)@$(FOLLOWER_PUB) 'sudo systemctl start celeriant'
	@printf "$(GREEN)Cluster started.$(RESET)\n"

stop: ## Stop cluster
	@if [[ -z "$(LEADER_PUB)" ]]; then printf "$(RED)No .cluster-env$(RESET)\n"; exit 1; fi
	@for HOST in $(FOLLOWER_PUB) $(LEADER_PUB); do \
		printf "$(BOLD)>>> Stopping celeriant on $$HOST$(RESET)\n"; \
		$(SSH)@$$HOST 'sudo systemctl stop celeriant 2>/dev/null || true'; \
	done

restart: stop start ## Restart cluster

status: ## Show service status on all nodes
	@if [[ -z "$(LEADER_PUB)" ]]; then printf "$(RED)No .cluster-env$(RESET)\n"; exit 1; fi
	@for HOST in $(LEADER_PUB) $(FOLLOWER_PUB); do \
		printf "$(BOLD)>>> $$HOST$(RESET)\n"; \
		$(SSH)@$$HOST 'systemctl is-active celeriant 2>/dev/null || echo "not running"'; \
	done

logs: ## Tail logs from both data nodes (Ctrl+C to stop)
	@if [[ -z "$(LEADER_PUB)" ]]; then printf "$(RED)No .cluster-env$(RESET)\n"; exit 1; fi
	@printf "$(YELLOW)Tailing logs from leader and follower... Ctrl+C to stop.$(RESET)\n"
	@$(SSH)@$(LEADER_PUB) 'journalctl -u celeriant -f --no-pager' &
	@$(SSH)@$(FOLLOWER_PUB) 'journalctl -u celeriant -f --no-pager' &
	@wait

# ---------------------------------------------------------------------------
# Benchmarks
# ---------------------------------------------------------------------------

dashboard: ## Import Celeriant dashboard to Grafana Cloud (needs GRAFANA_URL and GRAFANA_TOKEN)
	@if [[ -z "$(GRAFANA_URL)" || -z "$(GRAFANA_TOKEN)" ]]; then \
		printf "$(RED)Usage: make dashboard GRAFANA_URL=https://your-stack.grafana.net GRAFANA_TOKEN=glsa_...$(RESET)\n"; \
		exit 1; \
	fi
	@bash scripts/deploy-dashboard.sh --grafana-url $(GRAFANA_URL) --grafana-token $(GRAFANA_TOKEN)

run-benchmark: ## Run benchmark on client node(s) and collect results
	@if [[ -z "$(CLIENT_PUBS)" ]]; then printf "$(RED)No .cluster-env$(RESET)\n"; exit 1; fi
	@BENCH_TASKS=$(BENCH_TASKS) BENCH_CONNS=$(BENCH_CONNS) BENCH_DURATION=$(BENCH_DURATION) \
		bash scripts/run-benchmark.sh

run-sweep: ## Run benchmark at increasing concurrency levels, output CSV
	@if [[ -z "$(CLIENT_PUBS)" ]]; then printf "$(RED)No .cluster-env$(RESET)\n"; exit 1; fi
	@BENCH_DURATION=$(BENCH_DURATION) bash scripts/run-sweep.sh

# ---------------------------------------------------------------------------
# Teardown
# ---------------------------------------------------------------------------

teardown: stop ## Stop cluster and destroy CDK stack
	@printf "$(BOLD)>>> Destroying CDK stack$(RESET)\n"
	npx cdk destroy --force
	@rm -f $(CLUSTER_ENV)
	@printf "$(GREEN)Stack destroyed.$(RESET)\n"

teardown-data: ## Wipe data on data nodes (keeps services and infra)
	@if [[ -z "$(LEADER_PUB)" ]]; then printf "$(RED)No .cluster-env$(RESET)\n"; exit 1; fi
	@printf "$(RED)$(BOLD)This will delete ALL Celeriant data on leader and follower.$(RESET)\n"
	@read -p "Type 'yes' to confirm: " confirm && [ "$$confirm" = "yes" ] || exit 1
	@for HOST in $(LEADER_PUB) $(FOLLOWER_PUB); do \
		$(SSH)@$$HOST 'sudo rm -rf /var/lib/celeriant/*'; \
	done
	@printf "$(GREEN)Data wiped.$(RESET)\n"
