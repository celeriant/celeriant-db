#!/bin/bash
# Run kafka-bench at increasing concurrency levels and output a CSV.
# Mirrors the Celeriant ec2-benchmark.csv format.
#
# Usage: bash scripts/run-sweep.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CDK_DIR="$(dirname "$SCRIPT_DIR")"
CLUSTER_ENV="$CDK_DIR/.cluster-env"

source "$CLUSTER_ENV"

CLIENT_PUBS_SPACE="${CLIENT_PUBS//,/ }"
SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
if [[ -n "${KEY_FILE:-}" ]]; then
  SSH_OPTS="$SSH_OPTS -i $KEY_FILE"
fi

DURATION="${BENCH_DURATION:-15}"
RECORD_SIZE="${BENCH_RECORD_SIZE:-256}"

if [[ "${TLS_ENABLED:-true}" == "true" ]]; then
  BOOTSTRAP="${BROKER1_IP}:9093,${BROKER2_IP}:9093,${BROKER3_IP}:9093"
  TLS_FLAG="true"
else
  BOOTSTRAP="${BROKER1_IP}:9092,${BROKER2_IP}:9092,${BROKER3_IP}:9092"
  TLS_FLAG="false"
fi

# Concurrency levels matching the Celeriant benchmark CSV
CONCURRENCIES="${BENCH_CONCURRENCIES:-9000 12000 15000 18000 21000 24000 27000 30000 33000 36000 39000 42000 48000 54000 60000}"

TIMESTAMP=$(date +%Y%m%d-%H%M%S)
CSV_FILE="$CDK_DIR/results/${TIMESTAMP}_kafka_sweep.csv"
mkdir -p "$CDK_DIR/results"

echo "total_concurrency,per_client,kafka_x86_32c_reqps,kafka_x86_32c_errors,kafka_x86_32c_avg_ms,kafka_x86_32c_p99_ms" > "$CSV_FILE"

echo "==> Kafka benchmark sweep"
echo "  Brokers:     $INSTANCE_TYPE ($STORAGE_TYPE)"
echo "  Clients:     $CLIENT_COUNT ($CLIENT_PUBS_SPACE)"
echo "  Duration:    ${DURATION}s per run"
echo "  Concurrency: $CONCURRENCIES"
echo "  Output:      $CSV_FILE"
echo ""

for TOTAL in $CONCURRENCIES; do
  PER_CLIENT=$((TOTAL / CLIENT_COUNT))
  echo "--- Concurrency: $TOTAL (${PER_CLIENT}/client) ---"

  # Run kafka-bench on all clients in parallel
  PIDS=()
  IDX=0
  for HOST in $CLIENT_PUBS_SPACE; do
    IDX=$((IDX + 1))
    OUTFILE="/tmp/kafka_sweep_client_${IDX}.txt"

    ssh $SSH_OPTS ec2-user@${HOST} \
      "KAFKA_BOOTSTRAP=${BOOTSTRAP} \
       KAFKA_TLS=${TLS_FLAG} \
       KAFKA_CA_CERT=/etc/kafka/certs/ca.crt \
       KAFKA_TASKS=${PER_CLIENT} \
       KAFKA_DURATION=${DURATION} \
       KAFKA_RECORD_SIZE=${RECORD_SIZE} \
       kafka-bench" \
      2>&1 > "$OUTFILE" &
    PIDS+=($!)
  done

  # Wait for all
  for pid in "${PIDS[@]}"; do
    wait "$pid" || true
  done

  # Parse results from all clients
  TOTAL_REQUESTS=0
  TOTAL_ERRORS=0
  # For avg latency: weighted average across clients
  WEIGHTED_LAT_SUM=0
  MAX_P99=0

  for i in $(seq 1 "$CLIENT_COUNT"); do
    OUTFILE="/tmp/kafka_sweep_client_${i}.txt"
    SUMMARY=$(grep -E "Tasks:.*Requests:" "$OUTFILE" 2>/dev/null || echo "")
    LATLINE=$(grep -E "Latency —" "$OUTFILE" 2>/dev/null || echo "")

    if [[ -n "$SUMMARY" ]]; then
      REQS=$(echo "$SUMMARY" | grep -oP 'Requests: \K[0-9]+')
      ERRS=$(echo "$SUMMARY" | grep -oP 'Errors: \K[0-9]+')
      TOTAL_REQUESTS=$((TOTAL_REQUESTS + REQS))
      TOTAL_ERRORS=$((TOTAL_ERRORS + ERRS))
    fi

    if [[ -n "$LATLINE" ]]; then
      AVG=$(echo "$LATLINE" | grep -oP 'Avg: \K[0-9.]+')
      P99=$(echo "$LATLINE" | grep -oP 'P99: \K[0-9]+')
      if [[ -n "$AVG" && -n "$REQS" ]]; then
        WEIGHTED_LAT_SUM=$(echo "$WEIGHTED_LAT_SUM + $AVG * $REQS" | bc)
      fi
      if [[ -n "$P99" && "$P99" -gt "$MAX_P99" ]]; then
        MAX_P99=$P99
      fi
    fi
  done

  THROUGHPUT=$((TOTAL_REQUESTS / DURATION))
  if [[ $TOTAL_REQUESTS -gt 0 ]]; then
    AVG_LAT=$(echo "scale=1; $WEIGHTED_LAT_SUM / $TOTAL_REQUESTS" | bc)
  else
    AVG_LAT=0
  fi

  echo "  Throughput: $THROUGHPUT req/s | Errors: $TOTAL_ERRORS | Avg: ${AVG_LAT}ms | P99: ${MAX_P99}ms"
  echo "$TOTAL,$PER_CLIENT,$THROUGHPUT,$TOTAL_ERRORS,$AVG_LAT,$MAX_P99" >> "$CSV_FILE"

  # Brief pause between runs to let Kafka settle
  sleep 3
done

echo ""
echo "==> Sweep complete. Results:"
cat "$CSV_FILE"
echo ""
echo "==> Saved to $CSV_FILE"
