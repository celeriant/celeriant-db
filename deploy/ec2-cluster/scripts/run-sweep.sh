#!/bin/bash
# Run rpi_cluster_pool_bench at increasing concurrency levels, output CSV.
#
# Mirrors deploy/ec2-kafka-cluster/scripts/run-sweep.sh and
# deploy/ec2-marten-cluster/scripts/run-sweep.sh.
#
# Environment overrides:
#   BENCH_DURATION — seconds per level (default: 15)
#   SWEEP_LEVELS   — comma-separated concurrency levels
#                    (default: 9000,12000,...,60000)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CDK_DIR="$(dirname "$SCRIPT_DIR")"
CLUSTER_ENV="$CDK_DIR/.cluster-env"
source "$SCRIPT_DIR/iostat-lib.sh"

if [[ ! -f "$CLUSTER_ENV" ]]; then
  echo "ERROR: $CLUSTER_ENV not found — run 'make deploy' or 'make sync-env' first"
  exit 1
fi

source "$CLUSTER_ENV"

CLIENT_PUBS="${CLIENT_PUBS:-${CLIENT_PUB:-}}"
CLIENT_COUNT="${CLIENT_COUNT:-1}"
CLIENT_PUBS_SPACE="${CLIENT_PUBS//,/ }"

if [[ -z "$CLIENT_PUBS_SPACE" ]]; then
  echo "ERROR: No client IPs in $CLUSTER_ENV"
  exit 1
fi

SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
if [[ -n "${KEY_FILE:-}" ]]; then
  SSH_OPTS="$SSH_OPTS -i $KEY_FILE"
fi

DURATION="${BENCH_DURATION:-15}"
DEFAULT_LEVELS="9000,12000,15000,18000,21000,24000,27000,30000,33000,36000,39000,42000,48000,54000,60000"
LEVELS="${SWEEP_LEVELS:-$DEFAULT_LEVELS}"

TIMESTAMP=$(date +%Y%m%d-%H%M%S)
SAFE_TYPE=$(echo "$INSTANCE_TYPE" | tr '.' '-')
RESULT_DIR="$CDK_DIR/results"
CSV_FILE="$RESULT_DIR/${TIMESTAMP}_celeriant-sweep_${SAFE_TYPE}_${STORAGE_TYPE}.csv"
mkdir -p "$RESULT_DIR"

echo "==> Celeriant Concurrency Sweep"
echo "  Data nodes: $INSTANCE_TYPE ($STORAGE_TYPE)"
echo "  Clients:    $CLIENT_COUNT x ${CLIENT_INSTANCE_TYPE:-$INSTANCE_TYPE}"
echo "  Leader:     $LEADER_IP:10000"
echo "  Follower:   $FOLLOWER_IP:10000"
echo "  Duration:   ${DURATION}s per level"
echo "  Levels:     $LEVELS"
echo "  Output:     $CSV_FILE"
echo ""

# CSV header
echo "concurrency,tasks_per_client,clients,total_requests,total_errors,throughput_rps,avg_ms,p50_ms,p95_ms,p99_ms,p999_ms,min_ms,max_ms" > "$CSV_FILE"

IOSTAT_PREFIX="${CSV_FILE%.csv}_iostat"
echo "==> Starting disk capture on data nodes (spans the whole sweep)"
iostat_start "$IOSTAT_PREFIX" || true

for LEVEL in ${LEVELS//,/ }; do
  TASKS_PER_CLIENT=$(( LEVEL / CLIENT_COUNT ))

  echo ""
  echo "==> Level: $LEVEL total tasks ($TASKS_PER_CLIENT per client)"

  PIDS=()
  IDX=0
  for HOST in $CLIENT_PUBS_SPACE; do
    IDX=$((IDX + 1))
    OUTFILE="/tmp/celeriant_sweep_client_${IDX}.txt"

    ssh $SSH_OPTS ec2-user@${HOST} \
      "CLUSTER_ADDRESS_1=${LEADER_IP}:10000 \
       CLUSTER_ADDRESS_2=${FOLLOWER_IP}:10000 \
       CLUSTER_CA_CERT=/etc/celeriant/certs/client-ca.crt \
       CLUSTER_CLIENT_CERT=/etc/celeriant/certs/client.crt \
       CLUSTER_CLIENT_KEY=/etc/celeriant/certs/client.key \
       CLUSTER_SERVER_NAME=${LEADER_IP} \
       CLUSTER_TASKS=${TASKS_PER_CLIENT} \
       CLUSTER_CONNECTIONS=${TASKS_PER_CLIENT} \
       CLUSTER_DURATION=${DURATION} \
       celeriant-integration-tests --test rpi_cluster_pool_bench" \
      2>&1 > "$OUTFILE" &
    PIDS+=($!)
  done

  FAILED=0
  for i in "${!PIDS[@]}"; do
    if ! wait "${PIDS[$i]}"; then
      FAILED=$((FAILED + 1))
    fi
  done

  # Aggregate results
  TOTAL_REQUESTS=0
  TOTAL_ERRORS=0
  ALL_LATENCY_LINES=""

  for i in $(seq 1 "$CLIENT_COUNT"); do
    OUTFILE="/tmp/celeriant_sweep_client_${i}.txt"
    if [[ -f "$OUTFILE" ]]; then
      SUMMARY=$(grep -E "Tasks:.*Requests:" "$OUTFILE" 2>/dev/null || echo "")
      LATENCY=$(grep -E "Latency —" "$OUTFILE" 2>/dev/null || echo "")

      if [[ -n "$SUMMARY" ]]; then
        REQS=$(echo "$SUMMARY" | grep -oP 'Requests: \K[0-9]+')
        ERRS=$(echo "$SUMMARY" | grep -oP 'Errors: \K[0-9]+')
        TOTAL_REQUESTS=$((TOTAL_REQUESTS + REQS))
        TOTAL_ERRORS=$((TOTAL_ERRORS + ERRS))
      fi

      if [[ -n "$LATENCY" ]]; then
        ALL_LATENCY_LINES="$LATENCY"
      fi
    fi
  done

  THROUGHPUT=$((TOTAL_REQUESTS / DURATION))

  # Parse latency from last client (all clients hit the same cluster, latencies are close)
  AVG="0" P50="0" P95="0" P99="0" P999="0" MIN="0" MAX="0"
  if [[ -n "$ALL_LATENCY_LINES" ]]; then
    AVG=$(echo "$ALL_LATENCY_LINES" | grep -oP 'Avg: \K[0-9.]+' || echo "0")
    P50=$(echo "$ALL_LATENCY_LINES" | grep -oP 'P50: \K[0-9]+' || echo "0")
    P95=$(echo "$ALL_LATENCY_LINES" | grep -oP 'P95: \K[0-9]+' || echo "0")
    P99=$(echo "$ALL_LATENCY_LINES" | grep -oP 'P99: \K[0-9]+' || echo "0")
    P999=$(echo "$ALL_LATENCY_LINES" | grep -oP 'P99.9: \K[0-9]+' || echo "0")
    MIN=$(echo "$ALL_LATENCY_LINES" | grep -oP 'Min: \K[0-9]+' || echo "0")
    MAX=$(echo "$ALL_LATENCY_LINES" | grep -oP 'Max: \K[0-9]+' || echo "0")
  fi

  CSV_LINE="$LEVEL,$TASKS_PER_CLIENT,$CLIENT_COUNT,$TOTAL_REQUESTS,$TOTAL_ERRORS,$THROUGHPUT,$AVG,$P50,$P95,$P99,$P999,$MIN,$MAX"
  echo "$CSV_LINE" >> "$CSV_FILE"

  ERRORS_TAG=""
  if [[ $TOTAL_ERRORS -gt 0 ]]; then
    ERRORS_TAG=" (${TOTAL_ERRORS} errors)"
  fi
  echo "  => ${THROUGHPUT} req/s | avg: ${AVG}ms | P99: ${P99}ms | requests: ${TOTAL_REQUESTS}${ERRORS_TAG}"

  sleep 3
done

echo ""
echo "==> Disk utilisation across the sweep (per device, data nodes):"
iostat_stop "$IOSTAT_PREFIX" || true

echo ""
echo "==> Sweep complete. Results: $CSV_FILE"
echo ""
column -t -s',' "$CSV_FILE"
