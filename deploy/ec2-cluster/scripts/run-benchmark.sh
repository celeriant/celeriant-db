#!/bin/bash
# Run rpi_cluster_pool_bench on all EC2 client nodes and collect results locally.
#
# Reads cluster config from .cluster-env (written by deploy.sh).
# When multiple clients are configured, tasks are split evenly across them and
# run in parallel. Results are aggregated (requests summed, latency merged).
#
# Environment overrides (or set via Makefile):
#   BENCH_TASKS     — total concurrent writer tasks (default: 8000)
#   BENCH_CONNS     — pool max connections per node (default: matches tasks)
#   BENCH_DURATION  — test duration in seconds (default: 15)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CDK_DIR="$(dirname "$SCRIPT_DIR")"
CLUSTER_ENV="$CDK_DIR/.cluster-env"

if [[ ! -f "$CLUSTER_ENV" ]]; then
  echo "ERROR: $CLUSTER_ENV not found — run 'make deploy' or 'make sync-env' first"
  exit 1
fi

source "$CLUSTER_ENV"

# Backward compat: if CLIENT_PUBS is not set, fall back to CLIENT_PUB
CLIENT_PUBS="${CLIENT_PUBS:-${CLIENT_PUB:-}}"
CLIENT_COUNT="${CLIENT_COUNT:-1}"
# Normalize: convert comma-separated to space-separated for iteration
CLIENT_PUBS="${CLIENT_PUBS//,/ }"

if [[ -z "$CLIENT_PUBS" ]]; then
  echo "ERROR: No client IPs in $CLUSTER_ENV"
  exit 1
fi

SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
if [[ -n "${KEY_FILE:-}" ]]; then
  SSH_OPTS="$SSH_OPTS -i $KEY_FILE"
fi

TOTAL_TASKS="${BENCH_TASKS:-8000}"
DURATION="${BENCH_DURATION:-15}"

# Split tasks evenly across clients
TASKS_PER_CLIENT=$(( TOTAL_TASKS / CLIENT_COUNT ))
CONNS_PER_CLIENT="${BENCH_CONNS:-$TASKS_PER_CLIENT}"

TIMESTAMP=$(date +%Y%m%d-%H%M%S)
SAFE_TYPE=$(echo "$INSTANCE_TYPE" | tr '.' '-')
RESULT_DIR="$CDK_DIR/results"
RESULT_FILE="$RESULT_DIR/${TIMESTAMP}_${SAFE_TYPE}_${STORAGE_TYPE}.txt"
mkdir -p "$RESULT_DIR"

echo "==> Benchmark configuration"
echo "  Data nodes:  $INSTANCE_TYPE ($STORAGE_TYPE storage)"
echo "  Client node: ${CLIENT_INSTANCE_TYPE:-$INSTANCE_TYPE}"
echo "  Clients:     $CLIENT_COUNT ($CLIENT_PUBS)"
echo "  Address 1:   $LEADER_IP:10000 (primary)"
echo "  Address 2:   $FOLLOWER_IP:10000 (seed)"
echo "  Total tasks: $TOTAL_TASKS (${TASKS_PER_CLIENT}/client)"
echo "  Connections: $CONNS_PER_CLIENT per node per client"
echo "  Duration:    ${DURATION}s"
echo "  Output:      $RESULT_FILE"
echo ""

# Write metadata header to result file
cat > "$RESULT_FILE" <<EOF
# Celeriant EC2 Benchmark (rpi_cluster_pool_bench)
# Date:        $(date -Iseconds)
# Data nodes:  $INSTANCE_TYPE
# Client node: ${CLIENT_INSTANCE_TYPE:-$INSTANCE_TYPE}
# Clients:     $CLIENT_COUNT
# Storage:     $STORAGE_TYPE
# Leader IP:   $LEADER_IP
# Follower IP: $FOLLOWER_IP
# Region:      $REGION
# Total tasks: $TOTAL_TASKS
# Tasks/client: $TASKS_PER_CLIENT
# Connections: $CONNS_PER_CLIENT per node per client
# Duration:    ${DURATION}s
# ---

EOF

echo "==> Running rpi_cluster_pool_bench on $CLIENT_COUNT client(s)"

PIDS=()
IDX=0
for HOST in $CLIENT_PUBS; do
  IDX=$((IDX + 1))
  OUTFILE="/tmp/bench_client_${IDX}.txt"

  ssh $SSH_OPTS ec2-user@${HOST} \
    "CLUSTER_ADDRESS_1=${LEADER_IP}:10000 \
     CLUSTER_ADDRESS_2=${FOLLOWER_IP}:10000 \
     CLUSTER_CA_CERT=/etc/celeriant/certs/client-ca.crt \
     CLUSTER_CLIENT_CERT=/etc/celeriant/certs/client.crt \
     CLUSTER_CLIENT_KEY=/etc/celeriant/certs/client.key \
     CLUSTER_SERVER_NAME=${LEADER_IP} \
     CLUSTER_TASKS=${TASKS_PER_CLIENT} \
     CLUSTER_CONNECTIONS=${CONNS_PER_CLIENT} \
     CLUSTER_DURATION=${DURATION} \
     celeriant-integration-tests --test rpi_cluster_pool_bench" \
    2>&1 > "$OUTFILE" &
  PIDS+=($!)
  echo "  Started client $IDX on $HOST (pid $!, tasks=$TASKS_PER_CLIENT)"
done

# Wait for all clients and collect exit codes
FAILED=0
for i in "${!PIDS[@]}"; do
  if ! wait "${PIDS[$i]}"; then
    FAILED=$((FAILED + 1))
    echo "  WARNING: Client $((i+1)) exited with error"
  fi
done

# Aggregate results from all clients
echo ""
echo "==> Per-client results:"
TOTAL_REQUESTS=0
TOTAL_ERRORS=0

for i in $(seq 1 "$CLIENT_COUNT"); do
  OUTFILE="/tmp/bench_client_${i}.txt"
  if [[ -f "$OUTFILE" ]]; then
    SUMMARY=$(grep -E "Tasks:.*Requests:" "$OUTFILE" 2>/dev/null || echo "")
    LATENCY=$(grep -E "Latency —" "$OUTFILE" 2>/dev/null || echo "")
    if [[ -n "$SUMMARY" ]]; then
      echo "  Client $i: $SUMMARY"
      [[ -n "$LATENCY" ]] && echo "  Client $i: $LATENCY"
      REQS=$(echo "$SUMMARY" | grep -oP 'Requests: \K[0-9]+')
      ERRS=$(echo "$SUMMARY" | grep -oP 'Errors: \K[0-9]+')
      TOTAL_REQUESTS=$((TOTAL_REQUESTS + REQS))
      TOTAL_ERRORS=$((TOTAL_ERRORS + ERRS))
    else
      echo "  Client $i: NO RESULTS (check /tmp/bench_client_${i}.txt)"
    fi
    cat "$OUTFILE" >> "$RESULT_FILE"
    echo "---" >> "$RESULT_FILE"
  fi
done

if [[ $CLIENT_COUNT -gt 1 ]]; then
  THROUGHPUT=$((TOTAL_REQUESTS / DURATION))
  echo ""
  echo "==> Aggregated ($CLIENT_COUNT clients):"
  echo "  Total requests: $TOTAL_REQUESTS | Total errors: $TOTAL_ERRORS | Combined throughput: ~${THROUGHPUT} req/s"
  echo "" >> "$RESULT_FILE"
  echo "# Aggregated: requests=$TOTAL_REQUESTS errors=$TOTAL_ERRORS throughput=~${THROUGHPUT}" >> "$RESULT_FILE"
fi

echo ""
echo "==> Results saved to $RESULT_FILE"

if [[ $FAILED -gt 0 ]]; then
  exit 1
fi
