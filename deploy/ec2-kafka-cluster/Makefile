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

# Benchmark defaults
BENCH_RECORD_SIZE ?= 256

REPO_ROOT := $(shell cd ../.. && pwd)

BROKER_HOSTS = $(subst ,, ,$(BROKER_PUBS))

.PHONY: help build infra certs deploy start stop restart status logs \
        run-benchmark run-kafka-bench run-sweep sync-env teardown teardown-data

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(RESET) %s\n", $$1, $$2}'

# ---------------------------------------------------------------------------
# Infrastructure
# ---------------------------------------------------------------------------

build: ## Build kafka-bench binary in Docker (amazonlinux:2023, x86_64)
	@printf "$(BOLD)>>> Building kafka-bench in amazonlinux:2023 container$(RESET)\n"
	cd $(REPO_ROOT) && docker run --rm -v "$$(pwd)":/build -w /build amazonlinux:2023 bash -c '\
		set -e && \
		yum install -y gcc gcc-c++ openssl-devel perl-FindBin perl-File-Compare perl-IPC-Cmd make git cmake >/dev/null 2>&1 && \
		curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y >/dev/null 2>&1 && \
		source $$HOME/.cargo/env && \
		OPENSSL_NO_VENDOR=1 cargo build --release -p kafka_bench'
	@printf "$(GREEN)Build complete: target/release/kafka-bench$(RESET)\n"

infra: ## Deploy CDK stack (use CDK_ARGS for extra flags, e.g. CDK_ARGS="-c keyPair=my-key")
	npx cdk deploy --require-approval never $(CDK_ARGS)

# ---------------------------------------------------------------------------
# TLS certificates
# ---------------------------------------------------------------------------

certs: ## Generate TLS certs (requires deployed infra — reads IPs from stack outputs)
	@bash scripts/generate-certs.sh \
		$$(aws cloudformation describe-stacks --stack-name KafkaBenchmarkStack \
			--query "Stacks[0].Outputs[?OutputKey=='Broker1PrivateIp'].OutputValue" --output text) \
		$$(aws cloudformation describe-stacks --stack-name KafkaBenchmarkStack \
			--query "Stacks[0].Outputs[?OutputKey=='Broker2PrivateIp'].OutputValue" --output text) \
		$$(aws cloudformation describe-stacks --stack-name KafkaBenchmarkStack \
			--query "Stacks[0].Outputs[?OutputKey=='Broker3PrivateIp'].OutputValue" --output text)

# ---------------------------------------------------------------------------
# Deploy
# ---------------------------------------------------------------------------

deploy: ## Deploy Kafka config, certs, format storage (use KEY_ARG for SSH key, TLS_ARG for --no-tls)
	bash scripts/deploy.sh $(KEY_ARG) $(TLS_ARG)

sync-env: ## Re-read CDK outputs into .cluster-env
	@STACK=KafkaBenchmarkStack; \
	get() { aws cloudformation describe-stacks --stack-name $$STACK \
		--query "Stacks[0].Outputs[?OutputKey=='$$1'].OutputValue" --output text; }; \
	CC=$$(get ClientCount 2>/dev/null || echo 2); \
	CPUBS=""; \
	for i in $$(seq 1 $$CC); do \
		if [[ -n "$$CPUBS" ]]; then CPUBS="$$CPUBS,"; fi; \
		CPUBS="$$CPUBS$$(get Client$${i}PublicIp)"; \
	done; \
	printf "BROKER1_PUB=%s\nBROKER2_PUB=%s\nBROKER3_PUB=%s\nBROKER_PUBS=%s,%s,%s\nBROKER1_IP=%s\nBROKER2_IP=%s\nBROKER3_IP=%s\nBROKER_IPS=%s,%s,%s\nCLIENT_PUBS=%s\nCLIENT_COUNT=%s\nREGION=%s\nINSTANCE_TYPE=%s\nCLIENT_INSTANCE_TYPE=%s\nSTORAGE_TYPE=%s\nARCH=%s\nKAFKA_VERSION=%s\nTLS_ENABLED=%s\nKEY_FILE=$(KEY_FILE)\n" \
		$$(get Broker1PublicIp) $$(get Broker2PublicIp) $$(get Broker3PublicIp) \
		$$(get Broker1PublicIp) $$(get Broker2PublicIp) $$(get Broker3PublicIp) \
		$$(get Broker1PrivateIp) $$(get Broker2PrivateIp) $$(get Broker3PrivateIp) \
		$$(get Broker1PrivateIp) $$(get Broker2PrivateIp) $$(get Broker3PrivateIp) \
		"$$CPUBS" "$$CC" \
		$$(get Region) $$(get InstanceType) $$(get ClientInstanceType) \
		$$(get StorageType) $$(get Architecture) $$(get KafkaVersion) \
		"$${TLS_ENABLED:-true}" \
		> $(CLUSTER_ENV); \
	printf "$(GREEN)Wrote $(CLUSTER_ENV)$(RESET)\n"; \
	cat $(CLUSTER_ENV)

# ---------------------------------------------------------------------------
# Service management
# ---------------------------------------------------------------------------

start: ## Start Kafka on all brokers
	@if [[ -z "$(BROKER1_PUB)" ]]; then printf "$(RED)No .cluster-env — run 'make deploy' or 'make sync-env' first$(RESET)\n"; exit 1; fi
	@for HOST in $(BROKER1_PUB) $(BROKER2_PUB) $(BROKER3_PUB); do \
		printf "$(BOLD)>>> Starting Kafka on $$HOST$(RESET)\n"; \
		$(SSH)@$$HOST 'sudo systemctl start kafka'; \
	done
	@printf "$(GREEN)Cluster started. Waiting 10s for controller election...$(RESET)\n"
	@sleep 10
	@printf "$(GREEN)Ready.$(RESET)\n"

stop: ## Stop Kafka on all brokers
	@if [[ -z "$(BROKER1_PUB)" ]]; then printf "$(RED)No .cluster-env$(RESET)\n"; exit 1; fi
	@for HOST in $(BROKER3_PUB) $(BROKER2_PUB) $(BROKER1_PUB); do \
		printf "$(BOLD)>>> Stopping Kafka on $$HOST$(RESET)\n"; \
		$(SSH)@$$HOST 'sudo systemctl stop kafka 2>/dev/null || true'; \
	done

restart: stop start ## Restart cluster

status: ## Show service status on all nodes
	@if [[ -z "$(BROKER1_PUB)" ]]; then printf "$(RED)No .cluster-env$(RESET)\n"; exit 1; fi
	@for HOST in $(BROKER1_PUB) $(BROKER2_PUB) $(BROKER3_PUB); do \
		printf "$(BOLD)>>> $$HOST$(RESET)\n"; \
		$(SSH)@$$HOST 'systemctl is-active kafka 2>/dev/null || echo "not running"'; \
	done

logs: ## Tail logs from all brokers (Ctrl+C to stop)
	@if [[ -z "$(BROKER1_PUB)" ]]; then printf "$(RED)No .cluster-env$(RESET)\n"; exit 1; fi
	@printf "$(YELLOW)Tailing logs from all brokers... Ctrl+C to stop.$(RESET)\n"
	@$(SSH)@$(BROKER1_PUB) 'journalctl -u kafka -f --no-pager' &
	@$(SSH)@$(BROKER2_PUB) 'journalctl -u kafka -f --no-pager' &
	@$(SSH)@$(BROKER3_PUB) 'journalctl -u kafka -f --no-pager' &
	@wait

# ---------------------------------------------------------------------------
# Benchmarks
# ---------------------------------------------------------------------------

BENCH_TASKS ?= 8000
BENCH_DURATION ?= 15

run-benchmark: ## Run kafka-producer-perf-test (Kafka's built-in, batched — for reference only)
	@if [[ -z "$(CLIENT_PUBS)" ]]; then printf "$(RED)No .cluster-env$(RESET)\n"; exit 1; fi
	@bash scripts/run-benchmark.sh

run-kafka-bench: ## Run kafka-bench (Rust, mirrors Celeriant benchmark) on client node(s)
	@if [[ -z "$(CLIENT_PUBS)" ]]; then printf "$(RED)No .cluster-env$(RESET)\n"; exit 1; fi
	@BENCH_TASKS=$(BENCH_TASKS) BENCH_DURATION=$(BENCH_DURATION) \
		BENCH_RECORD_SIZE=$(BENCH_RECORD_SIZE) \
		bash scripts/run-kafka-bench.sh

run-sweep: ## Run kafka-bench at increasing concurrency levels, output CSV
	@if [[ -z "$(CLIENT_PUBS)" ]]; then printf "$(RED)No .cluster-env$(RESET)\n"; exit 1; fi
	@BENCH_DURATION=$(BENCH_DURATION) BENCH_RECORD_SIZE=$(BENCH_RECORD_SIZE) \
		bash scripts/run-sweep.sh

# ---------------------------------------------------------------------------
# Teardown
# ---------------------------------------------------------------------------

teardown: stop ## Stop cluster and destroy CDK stack
	@printf "$(BOLD)>>> Destroying CDK stack$(RESET)\n"
	npx cdk destroy --force
	@rm -f $(CLUSTER_ENV)
	@printf "$(GREEN)Stack destroyed.$(RESET)\n"

teardown-data: ## Wipe Kafka data on all brokers (keeps services and infra)
	@if [[ -z "$(BROKER1_PUB)" ]]; then printf "$(RED)No .cluster-env$(RESET)\n"; exit 1; fi
	@printf "$(RED)$(BOLD)This will delete ALL Kafka data on all brokers.$(RESET)\n"
	@read -p "Type 'yes' to confirm: " confirm && [ "$$confirm" = "yes" ] || exit 1
	@for HOST in $(BROKER1_PUB) $(BROKER2_PUB) $(BROKER3_PUB); do \
		$(SSH)@$$HOST 'sudo rm -rf /var/lib/kafka/*'; \
	done
	@printf "$(GREEN)Data wiped.$(RESET)\n"
