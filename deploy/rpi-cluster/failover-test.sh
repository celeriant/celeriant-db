#!/usr/bin/env bash
set -euo pipefail

# Failover stress test: teardown, redeploy, run bench with follower kill mid-test.
# Reports whether the leader retained leadership or got usurped.
#
# Usage (via make, which resolves INFRA_HOST from INFRA_MODE):
#   make failover-test                     # default: 8000
#   make failover-test CONNS="2000 4000"   # sweep multiple values

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"
source config.env

PROJECT_ROOT="$(cd ../.. && pwd)"
DURATION=60
KILL_AT=30       # seconds into test to stop follower
RESTART_AFTER=5  # seconds after stop to restart follower
LEASE_SETTLE=8   # seconds after restart before checking leader status

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

# macOS-compatible: extract current node role from most recent transition
# Returns "leader" or "follower" or ""
extract_current_role() {
    grep 'Node status transition shard_id=0' | tail -1 | sed -n 's/.*new=\([A-Za-z]*\).*/\1/p'
}

# macOS-compatible: extract value after a label from bench output
extract_field() {
    local label=$1 file=$2
    sed -n "s/.*${label}: *\([0-9]*\).*/\1/p" "$file" | tail -1
}

extract_field_with_unit() {
    local label=$1 file=$2
    sed -n "s/.*${label}: *\([0-9]*ms\).*/\1/p" "$file" | tail -1
}

get_leader_node() {
    local cs1_role cs2_role
    cs1_role=$(ssh "$LEADER_HOST" "journalctl -u celeriant --no-pager -n 500 2>/dev/null" \
        | extract_current_role || echo "")
    cs2_role=$(ssh "$FOLLOWER_HOST" "journalctl -u celeriant --no-pager -n 500 2>/dev/null" \
        | extract_current_role || echo "")
    if [[ "$cs1_role" == "Leader" ]]; then
        echo "cs1"
    elif [[ "$cs2_role" == "Leader" ]]; then
        echo "cs2"
    elif [[ "$cs1_role" == "Follower" ]]; then
        echo "cs2"
    elif [[ "$cs2_role" == "Follower" ]]; then
        echo "cs1"
    else
        echo "unknown"
    fi
}

run_one() {
    local conns=$1
    log "========== Testing with $conns connections =========="

    # --- Teardown & redeploy ---
    log "Tearing down data..."
    make -s teardown-data 2>&1 | tail -2

    log "Bringing up infra (MinIO, Prometheus, Loki, Grafana)..."
    # start-infra brings the stack up and waits for MinIO health in both
    # INFRA_MODEs. The inline ssh this replaced only worked in remote.
    if ! make -s start-infra 2>&1 | tail -3; then
        fail "infra failed to start"
        return 1
    fi

    log "Starting leader (cs1)..."
    make -s start-cs1
    log "Waiting for leader lease acquisition..."
    sleep 5

    log "Starting follower (cs2)..."
    make -s start-cs2
    sleep 3

    # Confirm both up
    local cs1_status cs2_status
    cs1_status=$(ssh "$LEADER_HOST" 'systemctl is-active celeriant 2>/dev/null || echo dead')
    cs2_status=$(ssh "$FOLLOWER_HOST" 'systemctl is-active celeriant 2>/dev/null || echo dead')
    if [[ "$cs1_status" != "active" || "$cs2_status" != "active" ]]; then
        fail "Nodes not healthy: cs1=$cs1_status cs2=$cs2_status"
        return 1
    fi
    log "Cluster healthy. Running benchmark ($conns conns, ${DURATION}s)..."

    # --- Run bench in background ---
    local bench_log
    bench_log="/tmp/failover-bench-${conns}-$(date +%s).log"

    cd "$PROJECT_ROOT"
    cargo run --release -p celeriant_bench -- \
        --address1 "$LEADER_HOST:$CLIENT_PORT" \
        --address2 "$FOLLOWER_HOST:$CLIENT_PORT" \
        --server-name "$LEADER_HOST" \
        --tasks "$conns" \
        --duration "$DURATION" \
        --ca-cert deploy/rpi-cluster/certs/client-ca.crt \
        --client-cert deploy/rpi-cluster/certs/client.crt \
        --client-key deploy/rpi-cluster/certs/client.key \
        > "$bench_log" 2>&1 &
    local bench_pid=$!
    cd "$SCRIPT_DIR"

    # --- Wait, then kill follower mid-test ---
    log "Waiting ${KILL_AT}s before stopping follower..."
    sleep "$KILL_AT"

    log "Stopping follower (cs2)..."
    ssh "$FOLLOWER_HOST" 'sudo systemctl stop celeriant 2>/dev/null || true'

    log "Follower stopped. Waiting ${RESTART_AFTER}s..."
    sleep "$RESTART_AFTER"

    log "Restarting follower (cs2)..."
    ssh "$FOLLOWER_HOST" 'sudo systemctl start celeriant'

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
    requests=$(extract_field "Requests" "$bench_log")
    errors=$(extract_field "Errors" "$bench_log")
    throughput=$(extract_field "Throughput" "$bench_log")
    p50=$(extract_field_with_unit "P50" "$bench_log")
    p95=$(extract_field_with_unit "P95" "$bench_log")
    p99=$(extract_field_with_unit "P99" "$bench_log")

    # --- Report ---
    echo ""
    printf "${BOLD}--- Results: %s connections ---${RESET}\n" "$conns"
    printf "  Requests:   %s\n" "${requests:-?}"
    printf "  Errors:     %s\n" "${errors:-?}"
    printf "  Throughput: %s req/s\n" "${throughput:-?}"
    printf "  Latency:    P50=%s  P95=%s  P99=%s\n" "${p50:-?}" "${p95:-?}" "${p99:-?}"

    if [[ "$leader_now" == "cs1" ]]; then
        pass "cs1 RETAINED leadership ($conns conns)"
    elif [[ "$leader_now" == "cs2" ]]; then
        fail "cs2 USURPED leadership ($conns conns)"
    else
        warn "Could not determine leader ($conns conns)"
    fi

    # Grab cs1 shard 0 gap for diagnostics
    local shard0_diag
    shard0_diag=$(ssh "$LEADER_HOST" "journalctl -u celeriant --no-pager -n 500 2>/dev/null" \
        | grep "shard_id=0" | grep -E "(Heartbeat|lease)" | tail -5 | head -1 || echo "n/a")
    printf "  Shard 0 diagnostic: %s\n" "$shard0_diag"

    printf "  Full log: %s\n\n" "$bench_log"

    # Return 0 if leader retained, 1 if usurped
    [[ "$leader_now" == "cs1" ]]
}

# --- Main ---
echo ""
printf "${BOLD}Failover Stress Test${RESET}\n"
printf "  Leader:   %s (cs1)\n" "$LEADER_HOST"
printf "  Follower: %s (cs2)\n" "$FOLLOWER_HOST"
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
