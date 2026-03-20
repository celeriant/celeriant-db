#!/bin/bash
# Run rpi_cluster_pool_bench on the EC2 client node and collect results locally.
#
# Reads cluster config from .cluster-env (written by deploy.sh).
# Results saved to results/<timestamp>-<instance-type>.txt
#
# Environment overrides (or set via Makefile):
#   BENCH_TASKS     — concurrent writer tasks (default: 8000)
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

SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
if [[ -n "${KEY_FILE:-}" ]]; then
  SSH_OPTS="$SSH_OPTS -i $KEY_FILE"
fi

TASKS="${BENCH_TASKS:-8000}"
CONNS="${BENCH_CONNS:-$TASKS}"
DURATION="${BENCH_DURATION:-15}"

TIMESTAMP=$(date +%Y%m%d-%H%M%S)
SAFE_TYPE=$(echo "$INSTANCE_TYPE" | tr '.' '-')
RESULT_DIR="$CDK_DIR/results"
RESULT_FILE="$RESULT_DIR/${TIMESTAMP}_${SAFE_TYPE}_${STORAGE_TYPE}.txt"
mkdir -p "$RESULT_DIR"

echo "==> Benchmark configuration"
echo "  Instance:    $INSTANCE_TYPE ($STORAGE_TYPE storage)"
echo "  Address 1:   $LEADER_IP:10000 (primary)"
echo "  Address 2:   $FOLLOWER_IP:10000 (seed)"
echo "  Client:      $CLIENT_PUB"
echo "  Tasks:       $TASKS"
echo "  Connections: $CONNS per node"
echo "  Duration:    ${DURATION}s"
echo "  Output:      $RESULT_FILE"
echo ""

# Write metadata header to result file
cat > "$RESULT_FILE" <<EOF
# Celeriant EC2 Benchmark (rpi_cluster_pool_bench)
# Date:        $(date -Iseconds)
# Instance:    $INSTANCE_TYPE
# Storage:     $STORAGE_TYPE
# Leader IP:   $LEADER_IP
# Follower IP: $FOLLOWER_IP
# Region:      $REGION
# Tasks:       $TASKS
# Connections: $CONNS per node
# Duration:    ${DURATION}s
# ---

EOF

echo "==> Running rpi_cluster_pool_bench on client node ($CLIENT_PUB)"

# Run benchmark on client node via SSH, tee output to terminal and result file
ssh $SSH_OPTS ec2-user@${CLIENT_PUB} \
  "CLUSTER_ADDRESS_1=${LEADER_IP}:10000 \
   CLUSTER_ADDRESS_2=${FOLLOWER_IP}:10000 \
   CLUSTER_CA_CERT=/etc/celeriant/certs/client-ca.crt \
   CLUSTER_CLIENT_CERT=/etc/celeriant/certs/client.crt \
   CLUSTER_CLIENT_KEY=/etc/celeriant/certs/client.key \
   CLUSTER_SERVER_NAME=${LEADER_IP} \
   CLUSTER_TASKS=${TASKS} \
   CLUSTER_CONNECTIONS=${CONNS} \
   CLUSTER_DURATION=${DURATION} \
   celeriant-integration-tests --test rpi_cluster_pool_bench" \
  2>&1 | tee -a "$RESULT_FILE"

echo ""
echo "==> Results saved to $RESULT_FILE"
