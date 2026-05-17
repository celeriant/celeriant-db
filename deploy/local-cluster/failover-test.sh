#!/usr/bin/env bash
set -euo pipefail

# Failover stress test for the local Docker cluster.
# Same methodology as the RPi test: teardown, run bench, kill follower
# mid-test, restart it, check if leadership flipped.
#
# Usage:
#   bash failover-test.sh [connections]       # default: 8000
#   bash failover-test.sh 1000 2000 4000      # sweep multiple values

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

PROJECT_ROOT="$(cd ../.. && pwd)"
DURATION=60
KILL_AT=30
RESTART_AFTER=5
LEASE_SETTLE=8

NODE1_CLIENT=localhost:10000
NODE2_CLIENT=localhost:10002
NODE1_CONTAINER=local-cluster-celeriant-node-1-1
NODE2_CONTAINER=local-cluster-celeriant-node-2-1

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BOLD='\033[1m'
RESET='\033[0m'

CONNECTIONS_LIST=("${@:-8000}")

log()  { printf "${BOLD}[%s]${RESET} %s\n" "$(date +%H:%M:%S)" "$*"; }
pass() { printf "${GREEN}[%s] PASS${RESET} %s\n" "$(date +%H:%M:%S)" "$*"; }
fail() { printf "${RED}[%s] FAIL${RESET} %s\n" "$(date +%H:%M:%S)" "$*"; }
warn() { printf "${YELLOW}[%s] WARN${RESET} %s\n" "$(date +%H:%M:%S)" "$*"; }

extract_current_role() {
    # Strip ANSI escape codes, find most recent shard 0 transition, extract role
    sed $'s/\033\\[[0-9;]*m//g' | grep 'Node status transition shard_id=0' | tail -1 | sed -n 's/.*new=\([A-Za-z]*\).*/\1/p'
}

get_leader_node() {
    local n1_role n2_role
    n1_role=$(docker logs --tail 500 "$NODE1_CONTAINER" 2>&1 | extract_current_role || echo "")
    n2_role=$(docker logs --tail 500 "$NODE2_CONTAINER" 2>&1 | extract_current_role || echo "")
    if [[ "$n1_role" == "Leader" ]]; then
        echo "node1"
    elif [[ "$n2_role" == "Leader" ]]; then
        echo "node2"
    elif [[ "$n1_role" == "Follower" ]]; then
        echo "node2"
    elif [[ "$n2_role" == "Follower" ]]; then
        echo "node1"
    else
        echo "unknown"
    fi
}

get_initial_leader() {
    # Wait for a leader to appear, return which node it is
    local attempts=0
    while (( attempts < 30 )); do
        local leader
        leader=$(get_leader_node)
        if [[ "$leader" != "unknown" ]]; then
            echo "$leader"
            return 0
        fi
        attempts=$((attempts + 1))
        sleep 1
    done
    echo "unknown"
}

run_one() {
    local conns=$1
    log "========== Testing with $conns connections =========="

    # --- Teardown & restart ---
    log "Wiping data and restarting cluster..."
    docker compose down -v 2>&1 | tail -1
    docker compose up -d --build 2>&1 | tail -1

    log "Waiting for MinIO healthy..."
    local minio_attempts=0
    while ! curl -sf http://localhost:9100/minio/health/live >/dev/null 2>&1; do
        minio_attempts=$((minio_attempts + 1))
        if (( minio_attempts > 60 )); then
            fail "MinIO failed to start after 60s"
            return 1
        fi
        sleep 1
    done
    log "MinIO healthy (${minio_attempts}s)"

    log "Waiting for cluster leader..."
    local initial_leader
    initial_leader=$(get_initial_leader)
    if [[ "$initial_leader" == "unknown" ]]; then
        fail "No leader elected after 30s"
        return 1
    fi
    log "Leader: $initial_leader"

    # Identify which container is leader and which is follower
    local leader_container follower_container leader_client
    if [[ "$initial_leader" == "node1" ]]; then
        leader_container="$NODE1_CONTAINER"
        follower_container="$NODE2_CONTAINER"
        leader_client="$NODE1_CLIENT"
    else
        leader_container="$NODE2_CONTAINER"
        follower_container="$NODE1_CONTAINER"
        leader_client="$NODE2_CLIENT"
    fi

    # Confirm both running
    local n1_state n2_state
    n1_state=$(docker inspect -f '{{.State.Status}}' "$NODE1_CONTAINER" 2>/dev/null || echo "dead")
    n2_state=$(docker inspect -f '{{.State.Status}}' "$NODE2_CONTAINER" 2>/dev/null || echo "dead")
    if [[ "$n1_state" != "running" || "$n2_state" != "running" ]]; then
        fail "Nodes not healthy: node1=$n1_state node2=$n2_state"
        return 1
    fi
    log "Cluster healthy. Running benchmark ($conns conns, ${DURATION}s)..."

    # --- Run bench in background ---
    local bench_log="/tmp/local-failover-bench-${conns}-$(date +%s).log"

    cd "$PROJECT_ROOT"
    cargo run --release -p celeriant_bench -- \
        --address1 "$NODE1_CLIENT" \
        --address2 "$NODE2_CLIENT" \
        --plaintext \
        --tasks "$conns" \
        --duration "$DURATION" \
        > "$bench_log" 2>&1 &
    local bench_pid=$!
    cd "$SCRIPT_DIR"

    # --- Wait, then kill follower mid-test ---
    log "Waiting ${KILL_AT}s before stopping follower ($follower_container)..."
    sleep "$KILL_AT"

    log "Stopping follower..."
    docker stop "$follower_container" >/dev/null 2>&1 || true

    log "Follower stopped. Waiting ${RESTART_AFTER}s..."
    sleep "$RESTART_AFTER"

    log "Restarting follower..."
    docker start "$follower_container" >/dev/null 2>&1

    log "Waiting ${LEASE_SETTLE}s for lease to settle..."
    sleep "$LEASE_SETTLE"

    # --- Check who is leader now ---
    local leader_now
    leader_now=$(get_leader_node)

    # --- Wait for bench to finish ---
    log "Waiting for benchmark to complete..."
    wait "$bench_pid" || true

    # --- Parse results ---
    local requests errors throughput p50 p95 p99
    requests=$(sed -n 's/.*Requests: *\([0-9]*\).*/\1/p' "$bench_log" | tail -1)
    errors=$(sed -n 's/.*Errors: *\([0-9]*\).*/\1/p' "$bench_log" | tail -1)
    throughput=$(sed -n 's/.*Throughput: *\([0-9]*\).*/\1/p' "$bench_log" | tail -1)
    p50=$(sed -n 's/.*P50: *\([0-9]*ms\).*/\1/p' "$bench_log" | tail -1)
    p95=$(sed -n 's/.*P95: *\([0-9]*ms\).*/\1/p' "$bench_log" | tail -1)
    p99=$(sed -n 's/.*P99: *\([0-9]*ms\).*/\1/p' "$bench_log" | tail -1)

    # --- Report ---
    echo ""
    printf "${BOLD}--- Results: %s connections ---${RESET}\n" "$conns"
    printf "  Requests:   %s\n" "${requests:-?}"
    printf "  Errors:     %s\n" "${errors:-?}"
    printf "  Throughput: %s req/s\n" "${throughput:-?}"
    printf "  Latency:    P50=%s  P95=%s  P99=%s\n" "${p50:-?}" "${p95:-?}" "${p99:-?}"

    local retained=false
    if [[ "$leader_now" == "$initial_leader" ]]; then
        pass "$initial_leader RETAINED leadership ($conns conns)"
        retained=true
    elif [[ "$leader_now" == "unknown" ]]; then
        warn "Could not determine leader ($conns conns)"
    else
        fail "$leader_now USURPED from $initial_leader ($conns conns)"
    fi

    # Shard 0 diagnostic from the leader
    local shard0_diag
    shard0_diag=$(docker logs --tail 500 "$leader_container" 2>&1 \
        | grep "shard_id=0" | grep -E "(Heartbeat|lease)" | tail -1 || echo "n/a")
    printf "  Shard 0 diagnostic: %s\n" "$shard0_diag"
    printf "  Full log: %s\n\n" "$bench_log"

    $retained
}

# --- Main ---
echo ""
printf "${BOLD}Local Cluster Failover Stress Test${RESET}\n"
printf "  Node 1:   %s\n" "$NODE1_CLIENT"
printf "  Node 2:   %s\n" "$NODE2_CLIENT"
printf "  Duration: %ss, kill follower at %ss, restart after %ss\n" "$DURATION" "$KILL_AT" "$RESTART_AFTER"
printf "  Connection sweep: %s\n\n" "${CONNECTIONS_LIST[*]}"

results=()
for conns in "${CONNECTIONS_LIST[@]}"; do
    if run_one "$conns"; then
        results+=("$conns:RETAINED")
    else
        results+=("$conns:USURPED")
    fi
done

# --- Summary ---
echo ""
printf "${BOLD}========== Summary ==========${RESET}\n"
for r in "${results[@]}"; do
    conns="${r%%:*}"
    outcome="${r##*:}"
    if [[ "$outcome" == "RETAINED" ]]; then
        pass "$conns connections — leader retained"
    else
        fail "$conns connections — leader usurped"
    fi
done
