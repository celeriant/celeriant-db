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

# Benchmark defaults. BENCH_TASKS=auto sizes total concurrency to the data-node
# vCPU count (~1125/vCPU, the measured knee); override with a number. BENCH_CONNS
# is the per-client connection pool (auto = 1:1 with tasks).
BENCH_TASKS ?= auto
BENCH_CONNS ?= auto
BENCH_DURATION ?= 15
# BENCH_PINNED=0 reverts the load generator to borrowing a pool connection per request,
# which sends most writes to a shard the connection isn't on. Only for A/B measurement.
BENCH_PINNED ?= 1

# Grafana home access: `make infra` opens :3000 to your egress IP, auto-detected
# from the machine you deploy from. Override with HOME_IP=1.2.3.4/32, or set
# HOME_IP= (empty) to skip the rule entirely (SSH-tunnel only).
HOME_IP ?= auto

REPO_ROOT := $(shell cd ../.. && pwd)

# Cluster profile — encodes the canonical benchmark shapes from README/docs.
# Override individual flags with CDK_ARGS for one-offs (e.g. CDK_ARGS="-c keyPair=my-key").
PROFILE ?= default

ifeq ($(PROFILE),i4i-32c)
  override CDK_ARGS += -c instanceType=i4i.8xlarge -c clientInstanceType=c7i.4xlarge -c clientCount=3
else ifeq ($(PROFILE),i4i-64c)
  override CDK_ARGS += -c instanceType=i4i.16xlarge -c clientInstanceType=c7i.4xlarge -c clientCount=4
else ifeq ($(PROFILE),i4g-32c)
  override CDK_ARGS += -c instanceType=i4g.8xlarge -c clientInstanceType=c7g.4xlarge -c clientCount=3
else ifneq ($(PROFILE),default)
  $(error Unknown PROFILE '$(PROFILE)'. Valid: default, i4i-32c, i4i-64c, i4g-32c)
endif

.PHONY: help build build-arm infra certs deploy start stop restart status logs \
        setup-infra run-benchmark run-sweep s3-fallback sync-env teardown teardown-data

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(RESET) %s\n", $$1, $$2}'

# ---------------------------------------------------------------------------
# Infrastructure
# ---------------------------------------------------------------------------
#
# Binaries MUST be built in the amazonlinux:2023 container (the targets below). A
# host `cargo build` links against newer glibc and fails on EC2 with "GLIBC_2.XX
# not found" — `make deploy` rejects host-built binaries for this reason.

build: ## Build x86_64 binaries in the amazonlinux:2023 container — REQUIRED (host cargo build fails on EC2)
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

infra: ## Deploy CDK stack (PROFILE=i4i-32c|i4g-32c|default; HOME_IP auto-opens Grafana; CDK_ARGS for extra flags)
	@HIP="$(HOME_IP)"; \
	if [ "$$HIP" = "auto" ]; then \
		HIP="$$(curl -fsS --max-time 5 https://checkip.amazonaws.com 2>/dev/null)"; \
		[ -n "$$HIP" ] && HIP="$$HIP/32"; \
	fi; \
	if [ -n "$$HIP" ]; then \
		printf "$(BOLD)>>> Grafana :3000 will be opened to $$HIP (override: HOME_IP=..., disable: HOME_IP=)$(RESET)\n"; \
		HOME_ARG="-c homeIp=$$HIP"; \
	else \
		printf "$(YELLOW)>>> HOME_IP unset/undetected — Grafana :3000 not exposed; use an SSH tunnel$(RESET)\n"; \
		HOME_ARG=""; \
	fi; \
	npx cdk deploy --require-approval never $(CDK_ARGS) $$HOME_ARG

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
	CPRIVS=$$(get ClientPrivateIp); \
	for i in $$(seq 2 $$CC); do CPUBS="$$CPUBS,$$(get Client$${i}PublicIp)"; CPRIVS="$$CPRIVS,$$(get Client$${i}PrivateIp)"; done; \
	printf "LEADER_PUB=%s\nFOLLOWER_PUB=%s\nCLIENT_PUBS=%s\nCLIENT_PRIVS=%s\nCLIENT_COUNT=%s\nLEADER_IP=%s\nFOLLOWER_IP=%s\nBUCKET=%s\nREGION=%s\nINSTANCE_TYPE=%s\nCLIENT_INSTANCE_TYPE=%s\nSTORAGE_TYPE=%s\nARCH=%s\nKEY_FILE=$(KEY_FILE)\n" \
		$$(get LeaderPublicIp) $$(get FollowerPublicIp) "$$CPUBS" "$$CPRIVS" "$$CC" \
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

setup-infra: ## Stand up Prometheus/Loki/Grafana on client #1 + promtail on data nodes
	@if [[ -z "$(CLIENT_PUBS)" ]]; then printf "$(RED)No .cluster-env — run 'make deploy' or 'make sync-env' first$(RESET)\n"; exit 1; fi
	bash scripts/setup-infra.sh

run-benchmark: ## Run benchmark on client node(s) (concurrency auto-sized to the cluster) and collect results
	@if [[ -z "$(CLIENT_PUBS)" ]]; then printf "$(RED)No .cluster-env$(RESET)\n"; exit 1; fi
	@BENCH_TASKS=$(BENCH_TASKS) BENCH_CONNS=$(BENCH_CONNS) BENCH_DURATION=$(BENCH_DURATION) \
		BENCH_PINNED=$(BENCH_PINNED) bash scripts/run-benchmark.sh

run-sweep: ## Run benchmark at increasing concurrency levels, output CSV
	@if [[ -z "$(CLIENT_PUBS)" ]]; then printf "$(RED)No .cluster-env$(RESET)\n"; exit 1; fi
	@BENCH_DURATION=$(BENCH_DURATION) BENCH_PINNED=$(BENCH_PINNED) bash scripts/run-sweep.sh

s3-fallback: ## Report event batches offloaded to S3 (degraded mode) + fallback counters
	@if [[ -z "$(BUCKET)" ]]; then printf "$(RED)No .cluster-env$(RESET)\n"; exit 1; fi
	@printf "$(BOLD)>>> S3 fallback batches in s3://$(BUCKET)/cluster/fallback/$(RESET)\n"
	@aws s3 ls "s3://$(BUCKET)/cluster/fallback/" --recursive --summarize 2>/dev/null | tail -20 || echo "  (none)"
	@printf "$(BOLD)>>> Fallback counters (data nodes)$(RESET)\n"
	@for HOST in $(LEADER_PUB) $(FOLLOWER_PUB); do \
		printf "  $$HOST: "; \
		$(SSH)@$$HOST 'curl -s localhost:9090/metrics | grep -E "^celeriant_(replication_s3_fallbacks|catchup_fallback|s3_fallback_lease_unconfirmed)_total" | awk "{s[\$$1]+=\$$2} END {for (k in s) printf \"%s=%d \", k, s[k]; print \"\"}"' 2>/dev/null || echo "unreachable"; \
	done

# ---------------------------------------------------------------------------
# Teardown
# ---------------------------------------------------------------------------

# NOTE: the CDK bucket is removalPolicy=DESTROY + autoDeleteObjects, so teardown
# ERASES all S3 evidence (fallback batches, lease history). Run `make s3-fallback`
# BEFORE teardown if the run degraded and you care why.
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
