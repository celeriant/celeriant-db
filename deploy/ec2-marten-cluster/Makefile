SHELL := /bin/bash
.DEFAULT_GOAL := help

CLUSTER_ENV := .cluster-env
-include $(CLUSTER_ENV)

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

# Path to the .NET client repo (sibling directory by default)
DOTNET_ROOT ?= $(shell cd ../../.. && pwd)/celeriant-dotnet-client

BENCH_RECORD_SIZE ?= 256

.PHONY: help build infra deploy start stop restart status repl-status logs \
        run-benchmark run-sweep monitor-pg sync-env teardown teardown-data

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(RESET) %s\n", $$1, $$2}'

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

build: ## Build marten-bench (dotnet publish, self-contained linux-x64)
	@printf "$(BOLD)>>> Building marten-bench$(RESET)\n"
	cd $(DOTNET_ROOT)/Marten.Benchmark && \
		dotnet publish -c Release -r linux-x64 --self-contained \
		-o $(CURDIR)/bin/publish
	@printf "$(GREEN)Build complete: bin/publish/$(RESET)\n"

# ---------------------------------------------------------------------------
# Infrastructure
# ---------------------------------------------------------------------------

infra: ## Deploy CDK stack (use CDK_ARGS for extra flags, e.g. CDK_ARGS="-c keyPair=my-key")
	npx cdk deploy --require-approval never $(CDK_ARGS)

# ---------------------------------------------------------------------------
# Deploy
# ---------------------------------------------------------------------------

deploy: ## Configure PostgreSQL and deploy benchmark binary to clients (use KEY_FILE for SSH key)
	bash scripts/deploy.sh $(if $(KEY_FILE),--key-file $(KEY_FILE))

sync-env: ## Re-read CDK outputs into .cluster-env
	@STACK=MartenBenchmarkStack; \
	get() { aws cloudformation describe-stacks --stack-name $$STACK \
		--query "Stacks[0].Outputs[?OutputKey=='$$1'].OutputValue" --output text; }; \
	CC=$$(get ClientCount 2>/dev/null || echo 2); \
	CPUBS=""; \
	for i in $$(seq 1 $$CC); do \
		if [[ -n "$$CPUBS" ]]; then CPUBS="$$CPUBS,"; fi; \
		CPUBS="$$CPUBS$$(get Client$${i}PublicIp)"; \
	done; \
	printf "PG_PUB=%s\nPG_IP=%s\nPG_INSTANCE_ID=%s\nSTANDBY_PUB=%s\nSTANDBY_IP=%s\nSTANDBY_INSTANCE_ID=%s\nCLIENT_PUBS=%s\nCLIENT_COUNT=%s\nREGION=%s\nINSTANCE_TYPE=%s\nCLIENT_INSTANCE_TYPE=%s\nSTORAGE_TYPE=%s\nARCH=%s\nPG_VERSION=%s\nKEY_FILE=$(KEY_FILE)\n" \
		$$(get PostgreSQLPublicIp) $$(get PostgreSQLPrivateIp) $$(get PostgreSQLInstanceId) \
		$$(get StandbyPublicIp) $$(get StandbyPrivateIp) $$(get StandbyInstanceId) \
		"$$CPUBS" "$$CC" \
		$$(get Region) $$(get InstanceType) $$(get ClientInstanceType) \
		$$(get StorageType) $$(get Architecture) $$(get PgVersion) \
		> $(CLUSTER_ENV); \
	printf "$(GREEN)Wrote $(CLUSTER_ENV)$(RESET)\n"; \
	cat $(CLUSTER_ENV)

# ---------------------------------------------------------------------------
# Service management
# ---------------------------------------------------------------------------

PG_SERVICE = postgresql

start: ## Start PostgreSQL (primary first, then standby)
	@if [[ -z "$(PG_PUB)" ]]; then printf "$(RED)No .cluster-env$(RESET)\n"; exit 1; fi
	@printf "$(BOLD)>>> Starting primary$(RESET)\n"
	@$(SSH)@$(PG_PUB) 'sudo systemctl start $(PG_SERVICE)'
	@sleep 2
	@printf "$(BOLD)>>> Starting standby$(RESET)\n"
	@$(SSH)@$(STANDBY_PUB) 'sudo systemctl start $(PG_SERVICE)' 2>/dev/null || true
	@sleep 2
	@printf "$(GREEN)PostgreSQL cluster started.$(RESET)\n"

stop: ## Stop PostgreSQL (standby first, then primary)
	@if [[ -z "$(PG_PUB)" ]]; then printf "$(RED)No .cluster-env$(RESET)\n"; exit 1; fi
	@printf "$(BOLD)>>> Stopping standby$(RESET)\n"
	@$(SSH)@$(STANDBY_PUB) 'sudo systemctl stop $(PG_SERVICE) 2>/dev/null || true' 2>/dev/null || true
	@printf "$(BOLD)>>> Stopping primary$(RESET)\n"
	@$(SSH)@$(PG_PUB) 'sudo systemctl stop $(PG_SERVICE) 2>/dev/null || true'

restart: stop start ## Restart PostgreSQL

status: ## Show primary/standby service status and connection count
	@if [[ -z "$(PG_PUB)" ]]; then printf "$(RED)No .cluster-env$(RESET)\n"; exit 1; fi
	@printf "$(BOLD)Primary:$(RESET) "
	@$(SSH)@$(PG_PUB) 'systemctl is-active $(PG_SERVICE) 2>/dev/null || echo "not running"'
	@$(SSH)@$(PG_PUB) "sudo -u postgres psql -c \"SELECT count(*) AS connections FROM pg_stat_activity;\"" 2>/dev/null || true
	@printf "$(BOLD)Standby:$(RESET) "
	@$(SSH)@$(STANDBY_PUB) 'systemctl is-active $(PG_SERVICE) 2>/dev/null || echo "not running"' 2>/dev/null || printf "unknown\n"

repl-status: ## Show synchronous replication status
	@if [[ -z "$(PG_PUB)" ]]; then printf "$(RED)No .cluster-env$(RESET)\n"; exit 1; fi
	@$(SSH)@$(PG_PUB) "sudo -u postgres psql -c \"SELECT application_name, state, sync_state, sent_lsn, replay_lsn, write_lag, flush_lag, replay_lag FROM pg_stat_replication;\""

logs: ## Tail PostgreSQL logs (Ctrl+C to stop)
	@if [[ -z "$(PG_PUB)" ]]; then printf "$(RED)No .cluster-env$(RESET)\n"; exit 1; fi
	@$(SSH)@$(PG_PUB) 'sudo journalctl -u $(PG_SERVICE) -f --no-pager'

# ---------------------------------------------------------------------------
# Benchmarks
# ---------------------------------------------------------------------------

BENCH_TASKS ?= 2000
BENCH_DURATION ?= 15

run-benchmark: ## Run marten-bench on client node(s)
	@if [[ -z "$(CLIENT_PUBS)" ]]; then printf "$(RED)No .cluster-env$(RESET)\n"; exit 1; fi
	@BENCH_TASKS=$(BENCH_TASKS) BENCH_DURATION=$(BENCH_DURATION) \
		BENCH_RECORD_SIZE=$(BENCH_RECORD_SIZE) \
		bash scripts/run-benchmark.sh

run-sweep: ## Run marten-bench at increasing concurrency levels, output CSV
	@if [[ -z "$(CLIENT_PUBS)" ]]; then printf "$(RED)No .cluster-env$(RESET)\n"; exit 1; fi
	@BENCH_DURATION=$(BENCH_DURATION) BENCH_RECORD_SIZE=$(BENCH_RECORD_SIZE) \
		bash scripts/run-sweep.sh

monitor-pg: ## Monitor PostgreSQL stats during a benchmark (run in separate terminal)
	@if [[ -z "$(PG_PUB)" ]]; then printf "$(RED)No .cluster-env$(RESET)\n"; exit 1; fi
	@bash scripts/monitor-pg.sh

# ---------------------------------------------------------------------------
# Teardown
# ---------------------------------------------------------------------------

teardown: stop ## Stop PostgreSQL and destroy CDK stack
	@printf "$(BOLD)>>> Destroying CDK stack$(RESET)\n"
	npx cdk destroy --force
	@rm -f $(CLUSTER_ENV)
	@printf "$(GREEN)Stack destroyed.$(RESET)\n"

teardown-data: ## Wipe PostgreSQL data (keeps services and infra)
	@if [[ -z "$(PG_PUB)" ]]; then printf "$(RED)No .cluster-env$(RESET)\n"; exit 1; fi
	@printf "$(RED)$(BOLD)This will delete ALL PostgreSQL data.$(RESET)\n"
	@read -p "Type 'yes' to confirm: " confirm && [ "$$confirm" = "yes" ] || exit 1
	@$(SSH)@$(PG_PUB) 'sudo systemctl stop $(PG_SERVICE); sudo rm -rf /var/lib/pgsql/data/*'
	@printf "$(GREEN)Data wiped.$(RESET)\n"
