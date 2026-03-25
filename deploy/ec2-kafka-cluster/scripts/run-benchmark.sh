#!/bin/bash
# Run kafka-producer-perf-test on all client nodes and collect results.
#
# Reads cluster config from .cluster-env (written by deploy.sh).
# Tasks are split evenly across clients and run in parallel.
#
# Environment overrides (or set via Makefile):
#   BENCH_RECORDS    — total records to send per client (default: 5000000)
#   BENCH_RECORD_SIZE — record size in bytes (default: 256)
#   BENCH_THROUGHPUT — target throughput per client, -1 = unlimited (default: -1)
#   BENCH_DURATION   — not used directly (Kafka perf test is record-count based)
#   BENCH_NUM_THREADS — producer threads per client (default: 8)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CDK_DIR="$(dirname "$SCRIPT_DIR")"
CLUSTER_ENV="$CDK_DIR/.cluster-env"

if [[ ! -f "$CLUSTER_ENV" ]]; then
  echo "ERROR: $CLUSTER_ENV not found — run 'make deploy' first"
  exit 1
fi

source "$CLUSTER_ENV"

CLIENT_PUBS="${CLIENT_PUBS//,/ }"
if [[ -z "$CLIENT_PUBS" ]]; then
  echo "ERROR: No client IPs in $CLUSTER_ENV"
  exit 1
fi

SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
if [[ -n "${KEY_FILE:-}" ]]; then
  SSH_OPTS="$SSH_OPTS -i $KEY_FILE"
fi

RECORDS="${BENCH_RECORDS:-5000000}"
RECORD_SIZE="${BENCH_RECORD_SIZE:-256}"
THROUGHPUT="${BENCH_THROUGHPUT:--1}"
NUM_THREADS="${BENCH_NUM_THREADS:-8}"
TOPIC="${BENCH_TOPIC:-benchmark-test}"
BATCH_SIZE="${BENCH_BATCH_SIZE:-65536}"
LINGER_MS="${BENCH_LINGER_MS:-5}"

# Determine bootstrap server and security settings
if [[ "${TLS_ENABLED:-true}" == "true" ]]; then
  BOOTSTRAP="${BROKER1_IP}:9093,${BROKER2_IP}:9093,${BROKER3_IP}:9093"
  STORE_PASS="kafka-bench-changeit"
  PRODUCER_CONFIG=$(cat <<PCFG
security.protocol=SSL
ssl.truststore.type=PKCS12
ssl.truststore.location=/etc/kafka/certs/truststore.p12
ssl.truststore.password=${STORE_PASS}
ssl.endpoint.identification.algorithm=
acks=all
batch.size=${BATCH_SIZE}
linger.ms=${LINGER_MS}
buffer.memory=134217728
PCFG
)
else
  BOOTSTRAP="${BROKER1_IP}:9092,${BROKER2_IP}:9092,${BROKER3_IP}:9092"
  PRODUCER_CONFIG=$(cat <<PCFG
acks=all
batch.size=${BATCH_SIZE}
linger.ms=${LINGER_MS}
buffer.memory=134217728
PCFG
)
fi

TIMESTAMP=$(date +%Y%m%d-%H%M%S)
SAFE_TYPE=$(echo "$INSTANCE_TYPE" | tr '.' '-')
RESULT_DIR="$CDK_DIR/results"
RESULT_FILE="$RESULT_DIR/${TIMESTAMP}_kafka_${SAFE_TYPE}_${STORAGE_TYPE}.txt"
mkdir -p "$RESULT_DIR"

echo "==> Benchmark configuration"
echo "  Brokers:     $INSTANCE_TYPE ($STORAGE_TYPE storage)"
echo "  Client:      ${CLIENT_INSTANCE_TYPE:-$INSTANCE_TYPE}"
echo "  Clients:     $CLIENT_COUNT ($CLIENT_PUBS)"
echo "  Bootstrap:   $BOOTSTRAP"
echo "  TLS:         ${TLS_ENABLED:-true}"
echo "  Records:     $RECORDS per client"
echo "  Record size: ${RECORD_SIZE} bytes"
echo "  Throughput:  $THROUGHPUT (-1 = unlimited)"
echo "  Threads:     $NUM_THREADS per client"
echo "  Topic:       $TOPIC"
echo "  acks=all, replication.factor=2, min.insync.replicas=2"
echo "  Output:      $RESULT_FILE"
echo ""

# Write metadata header
cat > "$RESULT_FILE" <<EOF
# Kafka EC2 Benchmark (kafka-producer-perf-test)
# Date:         $(date -Iseconds)
# Brokers:      $INSTANCE_TYPE (x3)
# Client:       ${CLIENT_INSTANCE_TYPE:-$INSTANCE_TYPE}
# Clients:      $CLIENT_COUNT
# Storage:      $STORAGE_TYPE
# TLS:          ${TLS_ENABLED:-true}
# Kafka:        ${KAFKA_VERSION:-unknown}
# Records:      $RECORDS per client
# Record size:  ${RECORD_SIZE} bytes
# Throughput:   $THROUGHPUT
# Threads:      $NUM_THREADS per client
# Topic:        $TOPIC
# acks:         all
# replication:  2, min.insync.replicas=2
#
# NOTE: Kafka does NOT fsync before ack (even with acks=all).
# Celeriant fsyncs every write to WAL before ack. This is a
# fundamental durability difference — Kafka relies on replication
# for durability, not local disk persistence.
# ---

EOF

# Create the benchmark topic (idempotent — ignores if exists)
echo "==> Creating topic '$TOPIC'"
FIRST_CLIENT="${CLIENT_PUBS%% *}"
ssh $SSH_OPTS ec2-user@${FIRST_CLIENT} \
  "/opt/kafka/bin/kafka-topics.sh --bootstrap-server ${BOOTSTRAP} \
    --create --topic ${TOPIC} --partitions 16 --replication-factor 2 \
    --if-not-exists \
    $(if [[ "${TLS_ENABLED:-true}" == "true" ]]; then
      echo "--command-config /tmp/kafka-client.properties"
    fi)" 2>/dev/null || true

# Deploy producer config to clients and create topic config
echo "==> Deploying producer config to clients"
for HOST in $CLIENT_PUBS; do
  echo "$PRODUCER_CONFIG" | ssh $SSH_OPTS ec2-user@${HOST} 'cat > /tmp/kafka-producer.properties'
  # Also create a minimal client config for kafka-topics.sh
  if [[ "${TLS_ENABLED:-true}" == "true" ]]; then
    cat <<CCFG | ssh $SSH_OPTS ec2-user@${HOST} 'cat > /tmp/kafka-client.properties'
security.protocol=SSL
ssl.truststore.type=PKCS12
ssl.truststore.location=/etc/kafka/certs/truststore.p12
ssl.truststore.password=${STORE_PASS}
ssl.endpoint.identification.algorithm=
CCFG
  fi
done

# Re-create topic now that client config is deployed
echo "==> Ensuring topic '$TOPIC' exists"
ssh $SSH_OPTS ec2-user@${FIRST_CLIENT} \
  "/opt/kafka/bin/kafka-topics.sh --bootstrap-server ${BOOTSTRAP} \
    --create --topic ${TOPIC} --partitions 16 --replication-factor 2 \
    --if-not-exists \
    $(if [[ "${TLS_ENABLED:-true}" == "true" ]]; then
      echo "--command-config /tmp/kafka-client.properties"
    fi)" 2>&1 | tail -1 || true

echo ""
echo "==> Running kafka-producer-perf-test on $CLIENT_COUNT client(s)"

PIDS=()
IDX=0
for HOST in $CLIENT_PUBS; do
  IDX=$((IDX + 1))
  OUTFILE="/tmp/kafka_bench_client_${IDX}.txt"

  ssh $SSH_OPTS ec2-user@${HOST} \
    "/opt/kafka/bin/kafka-producer-perf-test.sh \
      --topic ${TOPIC} \
      --num-records ${RECORDS} \
      --record-size ${RECORD_SIZE} \
      --throughput ${THROUGHPUT} \
      --producer.config /tmp/kafka-producer.properties \
      --producer-props bootstrap.servers=${BOOTSTRAP} \
      --print-metrics" \
    2>&1 > "$OUTFILE" &
  PIDS+=($!)
  echo "  Started client $IDX on $HOST (pid $!, records=$RECORDS)"
done

# Wait for all clients
FAILED=0
for i in "${!PIDS[@]}"; do
  if ! wait "${PIDS[$i]}"; then
    FAILED=$((FAILED + 1))
    echo "  WARNING: Client $((i+1)) exited with error"
  fi
done

# Aggregate results
echo ""
echo "==> Per-client results:"
TOTAL_RECORDS_SENT=0
TOTAL_MB=0

for i in $(seq 1 "$CLIENT_COUNT"); do
  OUTFILE="/tmp/kafka_bench_client_${i}.txt"
  if [[ -f "$OUTFILE" ]]; then
    # kafka-producer-perf-test summary line format:
    # <records_sent> records sent, <records/sec> records/sec (<MB/sec> MB/sec), <avg_latency> ms avg latency, <max_latency> ms max latency, ...
    SUMMARY=$(grep "records sent," "$OUTFILE" | tail -1 || echo "")
    if [[ -n "$SUMMARY" ]]; then
      echo "  Client $i: $SUMMARY"
      RECS=$(echo "$SUMMARY" | awk '{print $1}')
      TOTAL_RECORDS_SENT=$((TOTAL_RECORDS_SENT + RECS))
    else
      echo "  Client $i: NO RESULTS (check /tmp/kafka_bench_client_${i}.txt)"
    fi
    echo "--- Client $i ---" >> "$RESULT_FILE"
    cat "$OUTFILE" >> "$RESULT_FILE"
    echo "" >> "$RESULT_FILE"
  fi
done

if [[ $CLIENT_COUNT -gt 1 ]]; then
  echo ""
  echo "==> Aggregated ($CLIENT_COUNT clients):"
  echo "  Total records sent: $TOTAL_RECORDS_SENT"
  echo "" >> "$RESULT_FILE"
  echo "# Aggregated: total_records=$TOTAL_RECORDS_SENT" >> "$RESULT_FILE"
fi

echo ""
echo "==> Results saved to $RESULT_FILE"

if [[ $FAILED -gt 0 ]]; then
  exit 1
fi
