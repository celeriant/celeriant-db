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

# Data node count. 2 = leader + follower (replicated). 1 = standalone, which is required
# for any run measuring the write path without replication in it — see the topology guard
# in scripts/deploy.sh. Standalone runs must also pass STANDALONE=true to `make deploy`.
DATA_NODES ?= 2

# Cluster profile — encodes the canonical benchmark shapes from README/docs.
# NOTE: profile names count *vCPUs*, not physical cores. i4i.16xlarge is 64 vCPU / 32
# physical cores; only i4i.metal has 64 physical cores.
# Override individual flags with CDK_ARGS for one-offs (e.g. CDK_ARGS="-c keyPair=my-key").
PROFILE ?= default

ifeq ($(PROFILE),i4i-32c)
  override CDK_ARGS += -c instanceType=i4i.8xlarge -c clientInstanceType=c7i.4xlarge -c clientCount=3
  # Measured on this hardware 2026-08-16, one factor at a time, at 24,000 connections.
  #
  #   fsync 4,000 us       swept 100-17,000 on a REPLICATED pair (the first such sweep — the
  #                        older metal and gp3 sweeps were both standalone single nodes). All
  #                        three 4,000 reps beat all three 1,000 reps against sub-1% spread.
  #                        Also the binary default now, so this pin is explicit, not corrective.
  #
  #   replication 8,000 us metal's 15,000 costs -8.8% here. The 250-8,000 band all runs 445-455k
  #                        against 413k at 15,000, but it trades tail for throughput (p99 106 vs
  #                        93 ms) and runs the box at 92-94% CPU instead of 83%. 8,000 is the
  #                        fastest cell that stayed clean: 250 and 4,000 threw 155 and 478 errors
  #                        while 1,000 and 8,000 took none, which reads as a threshold grazed
  #                        near saturation rather than a property of the window. Confirmed 3/3
  #                        clean at 446,667 (0.12% spread).
  #
  # Do NOT copy the replication value to metal: 15,000 is metal's own measured optimum there.
  FSYNC_DELAY_US ?= 4000
  REPLICATION_DELAY_US ?= 8000
else ifeq ($(PROFILE),i4i-64c)
  override CDK_ARGS += -c instanceType=i4i.16xlarge -c clientInstanceType=c7i.4xlarge -c clientCount=4
else ifeq ($(PROFILE),i4g-32c)
  override CDK_ARGS += -c instanceType=i4g.8xlarge -c clientInstanceType=c7g.4xlarge -c clientCount=3
else ifeq ($(PROFILE),i4i-metal)
  # The publish target: 64 physical cores / 128 vCPU, bare metal, full PMU.
  # 128 + 4x16 = 192 vCPU, exactly the Standard Spot quota in ap-southeast-2 — a follower
  # does not fit, so this profile is standalone-only until the quota increase lands.
  #
  # AZ and client type are chosen by SPOT CAPACITY, not price. 2b advertised the cheapest
  # metal ($1.32/hr against $2.54-2.65) and failed to launch anything: cheap signalled a thin
  # market, not a bargain. get-spot-placement-scores at the time of writing:
  #   i4i.metal      2a=3  2b=1  2c=3
  #   c7i.4xlarge x4 2a=1  2b=1  2c=1     <- the type that killed the first attempt
  #   c6i.4xlarge x4 2a=1  2b=1  2c=3
  # 2c is the only AZ scoring 3 for both halves. Re-check before re-running, scores move:
  #   aws ec2 get-spot-placement-scores --instance-types i4i.metal --target-capacity 1 \
  #     --single-availability-zone --region-names ap-southeast-2
  override CDK_ARGS += -c instanceType=i4i.metal -c clientInstanceType=c6i.4xlarge -c clientCount=4 -c az=ap-southeast-2c
  # Measured amortisation windows for THIS hardware. Both were swept on the box; neither is a
  # guess, and they are NOT interchangeable defaults for other instance types:
  #
  #   fsync 1,000 us       now also the binary default. The old 17,000 us default cost -49.7%
  #                        here, which is what moved it. Nitro instance
  #                        store advertises write-through with fua=0, so the kernel elides the
  #                        NVMe FLUSH entirely — there is no device I/O for a long window to
  #                        amortise, only latency to pay. Anything from 100 us to 6,400 us
  #                        measured identically, so 1,000 us is mid-plateau rather than a peak.
  #
  #   replication 15,000 us the opposite: this window amortises a real network round-trip with
  #                        genuine per-message cost, so batching pays. 15,000 us beat the
  #                        17,000 us default by +9.8%, and beat 250 us by +19% while using a
  #                        QUARTER of the CPU (21% busy against 81%).
  #
  # NUM_SHARDS is deliberately unset: the server already defaults to one shard per vCPU, which
  # is what measured best at saturation on this box.
  #
  # Do NOT copy these onto smaller i4i types. On i4i.16xlarge (4 NVMe, 64 vCPU) the server
  # defaults matched or beat this PAIR of values at every concurrency tested.
  #
  # Careful what that does and does not say: that comparison moved BOTH knobs at once, so it
  # cannot attribute the result to either one. The replication window in particular has only
  # ever been swept on metal. A reasonable objection is that it amortises a network round-trip
  # and same-AZ RTT does not vary by instance type, so the optimum should transfer — but the
  # window also sets BATCH SIZE (throughput x window), and at 60k connections 15 ms buys
  # 15,861 writes per batch on metal against 7,000 on the 16xlarge. If the optimum tracks
  # batch size rather than RTT, the 16xlarge would want ~34 ms, which was one of the worst
  # cells on metal. The CPU signatures agree that the two are in different regimes: metal at
  # 15 ms sits at 21% busy / 78% iowait, the 16xlarge at 17 ms at 89% busy / 3% iowait.
  # Unresolved — sweep REPLICATION_DELAY_US alone on a 16xlarge pair to settle it.
  FSYNC_DELAY_US ?= 1000
  REPLICATION_DELAY_US ?= 15000
else ifeq ($(PROFILE),gp3-16c)
  # EBS arm. gp3 defaults to 3000 IOPS / 125 MB/s; ebsIops is settable here, throughput is NOT
  # (CloudFormation has no Throughput field) — MUST be raised post-deploy or the arm measures
  # the gp3 default rather than gp3:
  #   aws ec2 modify-volume --volume-id <id> --throughput 1000
  #
  # 16 vCPU data node, not 32: every 8xlarge shape scored 1/10 for spot placement in all three
  # AZs at the time of writing, while c6i.4xlarge scored 3 in 2c. This makes the gp3 arm a
  # SMALLER box than the i4i.metal arm, so the two are not directly comparable on throughput —
  # each arm is internally consistent for the delay question, which is what rung 2 asks.
  # It also matches the cheap-EBS-node shape the cost-floor story wants.
  override CDK_ARGS += -c instanceType=c6i.4xlarge -c clientInstanceType=c5.2xlarge -c clientCount=3 -c storageType=ebs -c az=ap-southeast-2c
else ifneq ($(PROFILE),default)
  $(error Unknown PROFILE '$(PROFILE)'. Valid: default, i4i-32c, i4i-64c, i4i-metal, gp3-16c, i4g-32c)
endif

# Campaign knobs. Empty means "server default" — deploy.sh omits the line rather than
# restating a default that could drift from the binary's. These are declared AFTER the
# profile block on purpose: a profile may pin a measured value, and `?=` here then leaves it
# alone. A value given on the command line still beats both.
NUM_SHARDS ?=
FSYNC_DELAY_US ?=
REPLICATION_DELAY_US ?=
RESERVE_COORDINATOR_SHARD ?=
MESH_CHANNEL_SIZE ?=
STANDALONE ?=
TLS_MODE ?= strict

.PHONY: help build build-arm infra certs deploy check-disks start stop restart status logs \
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
	npx cdk deploy --require-approval never -c dataNodeCount=$(DATA_NODES) $(CDK_ARGS) $$HOME_ARG

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
	@NUM_SHARDS="$(NUM_SHARDS)" FSYNC_DELAY_US="$(FSYNC_DELAY_US)" \
	 REPLICATION_DELAY_US="$(REPLICATION_DELAY_US)" \
	 RESERVE_COORDINATOR_SHARD="$(RESERVE_COORDINATOR_SHARD)" \
	 MESH_CHANNEL_SIZE="$(MESH_CHANNEL_SIZE)" STANDALONE="$(STANDALONE)" \
	 TLS_MODE="$(TLS_MODE)" \
	 bash scripts/deploy.sh $(KEY_ARG)

sync-env: ## Re-read CDK outputs into .cluster-env (run if stack was redeployed)
	@STACK=CeleriantKtlsTestStack; \
	get() { aws cloudformation describe-stacks --stack-name $$STACK \
		--query "Stacks[0].Outputs[?OutputKey=='$$1'].OutputValue" --output text; }; \
	CC=$$(get ClientCount 2>/dev/null || echo 1); \
	CPUBS=$$(get ClientPublicIp); \
	CPRIVS=$$(get ClientPrivateIp); \
	for i in $$(seq 2 $$CC); do CPUBS="$$CPUBS,$$(get Client$${i}PublicIp)"; CPRIVS="$$CPRIVS,$$(get Client$${i}PrivateIp)"; done; \
	DNC=$$(get DataNodeCount 2>/dev/null || echo 2); \
	printf "LEADER_PUB=%s\nFOLLOWER_PUB=%s\nCLIENT_PUBS=%s\nCLIENT_PRIVS=%s\nCLIENT_COUNT=%s\nLEADER_IP=%s\nFOLLOWER_IP=%s\nBUCKET=%s\nREGION=%s\nINSTANCE_TYPE=%s\nCLIENT_INSTANCE_TYPE=%s\nSTORAGE_TYPE=%s\nARCH=%s\nDATA_NODE_COUNT=%s\nKEY_FILE=$(KEY_FILE)\n" \
		"$$(get LeaderPublicIp)" "$$(get FollowerPublicIp)" "$$CPUBS" "$$CPRIVS" "$$CC" \
		"$$(get LeaderPrivateIp)" "$$(get FollowerPrivateIp)" \
		"$$(get BucketName)" "$$(get Region)" \
		"$$(get InstanceType)" "$$(get ClientInstanceType)" "$$(get StorageType)" "$$(get Architecture)" "$$DNC" \
		> $(CLUSTER_ENV); \
	printf "$(GREEN)Wrote $(CLUSTER_ENV)$(RESET)\n"; \
	cat $(CLUSTER_ENV)

# ---------------------------------------------------------------------------
# Service management (mirrors deploy/rpi-cluster/Makefile)
# ---------------------------------------------------------------------------

start: ## Start cluster (leader first, then follower if present)
	@if [[ -z "$(LEADER_PUB)" ]]; then printf "$(RED)No .cluster-env — run 'make deploy' or 'make sync-env' first$(RESET)\n"; exit 1; fi
	@printf "$(BOLD)>>> Starting celeriant on leader ($(LEADER_PUB))$(RESET)\n"
	@$(SSH)@$(LEADER_PUB) 'sudo systemctl start celeriant'
	@if [[ -n "$(FOLLOWER_PUB)" ]]; then \
		printf "Waiting for leader to acquire S3 lease...\n"; \
		sleep 5; \
		printf "$(BOLD)>>> Starting celeriant on follower ($(FOLLOWER_PUB))$(RESET)\n"; \
		$(SSH)@$(FOLLOWER_PUB) 'sudo systemctl start celeriant'; \
	else \
		printf "$(YELLOW)Single data node (standalone) — no follower to start$(RESET)\n"; \
	fi
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
	@printf "$(YELLOW)Tailing logs from data node(s)... Ctrl+C to stop.$(RESET)\n"
	@for HOST in $(LEADER_PUB) $(FOLLOWER_PUB); do \
		$(SSH)@$$HOST 'journalctl -u celeriant -f --no-pager' & \
	done; wait

# ---------------------------------------------------------------------------
# Benchmarks
# ---------------------------------------------------------------------------

check-disks: ## Pre-flight the NVMe on each data node (run after 'deploy', before 'start')
	@if [[ -z "$(LEADER_PUB)" ]]; then printf "$(RED)No .cluster-env — run 'make deploy' or 'make sync-env' first$(RESET)\n"; exit 1; fi
	@bash scripts/check-disks.sh

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
