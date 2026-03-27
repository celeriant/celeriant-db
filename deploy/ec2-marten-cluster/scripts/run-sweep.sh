#!/bin/bash
# Run marten-bench at increasing concurrency levels, output CSV.
#
# Mirrors deploy/ec2-kafka-cluster/scripts/run-sweep.sh and
# the Celeriant ec2-benchmark methodology.
#
# Environment overrides:
#   BENCH_DURATION    — seconds per level (default: 15)
#   BENCH_RECORD_SIZE — event payload bytes (default: 256)
#   SWEEP_LEVELS      — comma-separated concurrency levels
#                       (default: 500,1000,2000,3000,4500,6000,9000,12000,15000,18000,21000,24000)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CDK_DIR="$(dirname "$SCRIPT_DIR")"
CLUSTER_ENV="$CDK_DIR/.cluster-env"

if [[ ! -f "$CLUSTER_ENV" ]]; then
  echo "ERROR: $CLUSTER_ENV not found — run 'make deploy' or 'make sync-env' first"
  exit 1
fi

source "$CLUSTER_ENV"

DURATION="${BENCH_DURATION:-15}"
RECORD_SIZE="${BENCH_RECORD_SIZE:-256}"
DEFAULT_LEVELS="500,1000,2000,3000,4500,6000,9000,12000,15000,18000,21000,24000"
LEVELS="${SWEEP_LEVELS:-$DEFAULT_LEVELS}"

TIMESTAMP=$(date +%Y%m%d-%H%M%S)
SAFE_TYPE=$(echo "$INSTANCE_TYPE" | tr '.' '-')
RESULT_DIR="$CDK_DIR/results"
CSV_FILE="$RESULT_DIR/${TIMESTAMP}_marten-sweep_${SAFE_TYPE}_${STORAGE_TYPE}.csv"
mkdir -p "$RESULT_DIR"

echo "==> Marten/PostgreSQL Concurrency Sweep"
echo "  PostgreSQL:  $INSTANCE_TYPE ($STORAGE_TYPE)"
echo "  Clients:     $CLIENT_COUNT x ${CLIENT_INSTANCE_TYPE:-$INSTANCE_TYPE}"
echo "  PG host:     $PG_IP:5432"
echo "  Duration:    ${DURATION}s per level"
echo "  Record size: ${RECORD_SIZE} bytes"
echo "  Levels:      $LEVELS"
echo "  Output:      $CSV_FILE"
echo ""

# CSV header
echo "concurrency,tasks_per_client,clients,total_requests,total_errors,throughput_rps,avg_ms,p50_ms,p95_ms,p99_ms,p999_ms,min_ms,max_ms" > "$CSV_FILE"

SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
if [[ -n "${KEY_FILE:-}" ]]; then
  SSH_OPTS="$SSH_OPTS -i $KEY_FILE"
fi

CLIENT_PUBS_SPACE="${CLIENT_PUBS//,/ }"

for LEVEL in ${LEVELS//,/ }; do
  TASKS_PER_CLIENT=$(( LEVEL / CLIENT_COUNT ))

  echo ""
  echo "==> Level: $LEVEL total tasks ($TASKS_PER_CLIENT per client)"

  # Run benchmark on all clients in parallel
  PIDS=()
  IDX=0
  for HOST in $CLIENT_PUBS_SPACE; do
    IDX=$((IDX + 1))
    OUTFILE="/tmp/marten_sweep_client_${IDX}.txt"

    ssh $SSH_OPTS ec2-user@${HOST} \
      "PG_HOST=${PG_IP} \
       PG_PORT=5432 \
       PG_DATABASE=marten_bench \
       PG_USER=bench \
       PG_PASSWORD=bench \
       BENCH_TASKS=${TASKS_PER_CLIENT} \
       BENCH_DURATION=${DURATION} \
       BENCH_RECORD_SIZE=${RECORD_SIZE} \
       BENCH_BUCKET_SECS=${DURATION} \
       /opt/marten-bench/marten-bench" \
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
    OUTFILE="/tmp/marten_sweep_client_${i}.txt"
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

  # Parse latency from last client (best approximation when aggregating)
  # For multi-client, individual latency distributions are close since all hit the same PG node
  AVG="0" P50="0" P95="0" P99="0" P999="0" MIN="0" MAX="0"
  if [[ -n "$ALL_LATENCY_LINES" ]]; then
    AVG=$(echo "$ALL_LATENCY_LINES" | grep -oP 'avg: \K[0-9.]+' || echo "0")
    P50=$(echo "$ALL_LATENCY_LINES" | grep -oP 'P50: \K[0-9]+' || echo "0")
    P95=$(echo "$ALL_LATENCY_LINES" | grep -oP 'P95: \K[0-9]+' || echo "0")
    P99=$(echo "$ALL_LATENCY_LINES" | grep -oP 'P99: \K[0-9]+' || echo "0")
    P999=$(echo "$ALL_LATENCY_LINES" | grep -oP 'P99.9: \K[0-9]+' || echo "0")
    MIN=$(echo "$ALL_LATENCY_LINES" | grep -oP 'min: \K[0-9]+' || echo "0")
    MAX=$(echo "$ALL_LATENCY_LINES" | grep -oP 'max: \K[0-9]+' || echo "0")
  fi

  CSV_LINE="$LEVEL,$TASKS_PER_CLIENT,$CLIENT_COUNT,$TOTAL_REQUESTS,$TOTAL_ERRORS,$THROUGHPUT,$AVG,$P50,$P95,$P99,$P999,$MIN,$MAX"
  echo "$CSV_LINE" >> "$CSV_FILE"

  ERRORS_TAG=""
  if [[ $TOTAL_ERRORS -gt 0 ]]; then
    ERRORS_TAG=" (${TOTAL_ERRORS} errors)"
  fi
  echo "  => ${THROUGHPUT} req/s | avg: ${AVG}ms | P99: ${P99}ms | requests: ${TOTAL_REQUESTS}${ERRORS_TAG}"

  # Brief pause between levels to let PG recover
  sleep 3
done

echo ""
echo "==> Sweep complete. Results: $CSV_FILE"
echo ""
column -t -s',' "$CSV_FILE"
