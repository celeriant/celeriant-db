#!/bin/bash
# Deploy Kafka KRaft configuration, certs, and keystores to the EC2 cluster.
#
# Reads IPs from CDK stack outputs. Run after `npx cdk deploy`.
#
# Usage:
#   ./deploy.sh [--key-file ~/.ssh/your-key.pem] [--no-tls]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CDK_DIR="$(dirname "$SCRIPT_DIR")"
REPO_ROOT="$(cd "$CDK_DIR/../.." && pwd)"
CERT_DIR="$CDK_DIR/certs"

# Parse args
KEY_FILE=""
TLS_ENABLED="true"
while [[ $# -gt 0 ]]; do
  case $1 in
    --key-file) KEY_FILE="$2"; shift 2 ;;
    --no-tls) TLS_ENABLED="false"; shift ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
if [[ -n "$KEY_FILE" ]]; then
  SSH_OPTS="$SSH_OPTS -i $KEY_FILE"
fi

# Read CDK outputs
STACK_NAME="KafkaBenchmarkStack"
echo "==> Reading stack outputs from $STACK_NAME"

get_output() {
  aws cloudformation describe-stacks --stack-name "$STACK_NAME" \
    --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" --output text
}

BROKER1_PUB=$(get_output Broker1PublicIp)
BROKER2_PUB=$(get_output Broker2PublicIp)
BROKER3_PUB=$(get_output Broker3PublicIp)
BROKER1_IP=$(get_output Broker1PrivateIp)
BROKER2_IP=$(get_output Broker2PrivateIp)
BROKER3_IP=$(get_output Broker3PrivateIp)
REGION=$(get_output Region)
INSTANCE_TYPE=$(get_output InstanceType)
CLIENT_INSTANCE_TYPE=$(get_output ClientInstanceType)
CLIENT_COUNT=$(get_output ClientCount 2>/dev/null || echo "2")
STORAGE_TYPE=$(get_output StorageType)
ARCH=$(get_output Architecture)
KAFKA_VERSION=$(get_output KafkaVersion)

CLIENT_PUBS=""
for i in $(seq 1 "$CLIENT_COUNT"); do
  PUB=$(get_output "Client${i}PublicIp")
  if [[ -n "$CLIENT_PUBS" ]]; then CLIENT_PUBS="$CLIENT_PUBS,"; fi
  CLIENT_PUBS="$CLIENT_PUBS$PUB"
done

BROKER_PUBS_CSV="$BROKER1_PUB,$BROKER2_PUB,$BROKER3_PUB"
BROKER_IPS_CSV="$BROKER1_IP,$BROKER2_IP,$BROKER3_IP"

echo "  Broker 1: $BROKER1_PUB ($BROKER1_IP)"
echo "  Broker 2: $BROKER2_PUB ($BROKER2_IP)"
echo "  Broker 3: $BROKER3_PUB ($BROKER3_IP)"
echo "  Clients:  $CLIENT_PUBS ($CLIENT_COUNT nodes)"
echo "  Region:   $REGION"
echo "  Instance: $INSTANCE_TYPE ($STORAGE_TYPE storage)"
echo "  Client:   $CLIENT_INSTANCE_TYPE"
echo "  Kafka:    $KAFKA_VERSION"
echo "  TLS:      $TLS_ENABLED"

# Check certs exist if TLS is enabled
STORE_PASS="kafka-bench-changeit"
if [[ "$TLS_ENABLED" == "true" && ! -f "$CERT_DIR/truststore.p12" ]]; then
  echo "ERROR: TLS enabled but certs not found in $CERT_DIR"
  echo "Run: make certs"
  exit 1
fi

# Check kafka-bench binary exists
BENCH_BINARY="$REPO_ROOT/target/release/kafka-bench"
if [[ ! -f "$BENCH_BINARY" ]]; then
  echo "WARNING: kafka-bench binary not found at $BENCH_BINARY"
  echo "Run: cargo build --release -p kafka_bench"
  echo "(Skipping benchmark binary deploy — use 'make build' first)"
  BENCH_BINARY=""
fi

SSH="ssh $SSH_OPTS ec2-user"
SCP="scp $SSH_OPTS"

# Generate a cluster UUID for KRaft
CLUSTER_ID=$($SSH@${BROKER1_PUB} '/opt/kafka/bin/kafka-storage.sh random-uuid')
echo ""
echo "==> KRaft cluster ID: $CLUSTER_ID"

# Listener and security config based on TLS setting
if [[ "$TLS_ENABLED" == "true" ]]; then
  CLIENT_LISTENER="SSL"
  CLIENT_PORT="9093"
  LISTENER_SECURITY_MAP="CONTROLLER:SSL,SSL:SSL"
  SSL_CONFIG=$(cat <<'SSLCFG'

# TLS configuration
ssl.keystore.type=PKCS12
ssl.keystore.location=/etc/kafka/certs/broker.keystore.p12
ssl.keystore.password=kafka-bench-changeit
ssl.key.password=kafka-bench-changeit
ssl.truststore.type=PKCS12
ssl.truststore.location=/etc/kafka/certs/truststore.p12
ssl.truststore.password=kafka-bench-changeit
ssl.client.auth=none
ssl.endpoint.identification.algorithm=
SSLCFG
)
else
  CLIENT_LISTENER="PLAINTEXT"
  CLIENT_PORT="9092"
  LISTENER_SECURITY_MAP="CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT"
  SSL_CONFIG=""
fi

# Deploy config to each broker
echo ""
echo "==> Deploying Kafka configuration to brokers"

BROKER_PUBS=("$BROKER1_PUB" "$BROKER2_PUB" "$BROKER3_PUB")
BROKER_IPS=("$BROKER1_IP" "$BROKER2_IP" "$BROKER3_IP")

for i in 0 1 2; do
  NODE_ID=$((i + 1))
  PUB="${BROKER_PUBS[$i]}"
  IP="${BROKER_IPS[$i]}"

  CONTROLLER_QUORUM_VOTERS="1@${BROKER1_IP}:9094,2@${BROKER2_IP}:9094,3@${BROKER3_IP}:9094"

  cat > /tmp/kafka-server-${NODE_ID}.properties <<EOF
# KRaft broker+controller configuration — node ${NODE_ID}
process.roles=broker,controller
node.id=${NODE_ID}
controller.quorum.voters=${CONTROLLER_QUORUM_VOTERS}
controller.listener.names=CONTROLLER

# Listeners
listeners=${CLIENT_LISTENER}://${IP}:${CLIENT_PORT},CONTROLLER://${IP}:9094
advertised.listeners=${CLIENT_LISTENER}://${IP}:${CLIENT_PORT}
listener.security.protocol.map=${LISTENER_SECURITY_MAP}
inter.broker.listener.name=${CLIENT_LISTENER}

# Storage
log.dirs=/var/lib/kafka/kraft-logs
num.partitions=16
default.replication.factor=2
min.insync.replicas=2

# Performance tuning
num.io.threads=16
num.network.threads=8
socket.send.buffer.bytes=1048576
socket.receive.buffer.bytes=1048576
socket.request.max.bytes=104857600
num.recovery.threads.per.data.dir=4

# Log retention (keep enough for benchmarks)
log.retention.hours=1
log.segment.bytes=1073741824
log.retention.check.interval.ms=30000

# Replication
replica.fetch.max.bytes=10485760
${SSL_CONFIG}
EOF

  $SCP /tmp/kafka-server-${NODE_ID}.properties ec2-user@${PUB}:/tmp/server.properties
  $SSH@${PUB} 'sudo mv /tmp/server.properties /etc/kafka/server.properties && sudo chown kafka:kafka /etc/kafka/server.properties'

  # Kafka env file (JVM settings)
  cat > /tmp/kafka-${NODE_ID}.env <<EOF
KAFKA_HEAP_OPTS=-Xmx6g -Xms6g
KAFKA_JVM_PERFORMANCE_OPTS=-server -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:+ExplicitGCInvokesConcurrent -XX:MaxInlineLevel=15 -Djava.awt.headless=true
EOF

  $SCP /tmp/kafka-${NODE_ID}.env ec2-user@${PUB}:/tmp/kafka.env
  $SSH@${PUB} 'sudo mv /tmp/kafka.env /etc/kafka/kafka.env && sudo chown kafka:kafka /etc/kafka/kafka.env'

  echo "  Configured broker ${NODE_ID} ($PUB)"
done

# Deploy TLS certs to brokers
if [[ "$TLS_ENABLED" == "true" ]]; then
  echo ""
  echo "==> Deploying TLS keystores to brokers"
  for i in 0 1 2; do
    NODE_ID=$((i + 1))
    PUB="${BROKER_PUBS[$i]}"

    $SCP "$CERT_DIR/broker${NODE_ID}.keystore.p12" ec2-user@${PUB}:/tmp/broker.keystore.p12
    $SCP "$CERT_DIR/truststore.p12" ec2-user@${PUB}:/tmp/truststore.p12
    $SSH@${PUB} 'sudo mv /tmp/broker.keystore.p12 /tmp/truststore.p12 /etc/kafka/certs/ && sudo chown kafka:kafka /etc/kafka/certs/*.p12 && sudo chmod 600 /etc/kafka/certs/*.p12'
    echo "  Deployed certs to broker ${NODE_ID} ($PUB)"
  done

  echo ""
  echo "==> Deploying TLS keystores and CA cert to clients"
  for HOST in ${CLIENT_PUBS//,/ }; do
    $SCP "$CERT_DIR/truststore.p12" "$CERT_DIR/client.keystore.p12" "$CERT_DIR/ca.crt" ec2-user@${HOST}:/tmp/
    $SSH@${HOST} 'sudo mv /tmp/truststore.p12 /tmp/client.keystore.p12 /tmp/ca.crt /etc/kafka/certs/ && sudo chmod 644 /etc/kafka/certs/*.p12 /etc/kafka/certs/ca.crt'
    echo "  Deployed certs to client $HOST"
  done
fi

# Deploy kafka-bench binary to clients
if [[ -n "$BENCH_BINARY" ]]; then
  echo ""
  echo "==> Deploying kafka-bench binary to clients"
  for HOST in ${CLIENT_PUBS//,/ }; do
    $SCP "$BENCH_BINARY" ec2-user@${HOST}:/tmp/kafka-bench
    $SSH@${HOST} 'sudo mv /tmp/kafka-bench /usr/local/bin/ && sudo chmod +x /usr/local/bin/kafka-bench'
    echo "  Deployed to $HOST"
  done
fi

# Format KRaft storage on each broker
echo ""
echo "==> Formatting KRaft storage directories"
for i in 0 1 2; do
  PUB="${BROKER_PUBS[$i]}"
  $SSH@${PUB} "sudo -u kafka /opt/kafka/bin/kafka-storage.sh format \
    -t ${CLUSTER_ID} \
    -c /etc/kafka/server.properties --ignore-formatted 2>&1 | tail -1"
  echo "  Formatted broker $((i+1)) ($PUB)"
done

# Write .cluster-env for Makefile consumption
ENV_FILE="$CDK_DIR/.cluster-env"
cat > "$ENV_FILE" <<EOF
BROKER1_PUB=$BROKER1_PUB
BROKER2_PUB=$BROKER2_PUB
BROKER3_PUB=$BROKER3_PUB
BROKER_PUBS=$BROKER_PUBS_CSV
BROKER1_IP=$BROKER1_IP
BROKER2_IP=$BROKER2_IP
BROKER3_IP=$BROKER3_IP
BROKER_IPS=$BROKER_IPS_CSV
CLIENT_PUBS=$CLIENT_PUBS
CLIENT_COUNT=$CLIENT_COUNT
REGION=$REGION
INSTANCE_TYPE=$INSTANCE_TYPE
CLIENT_INSTANCE_TYPE=$CLIENT_INSTANCE_TYPE
STORAGE_TYPE=$STORAGE_TYPE
ARCH=$ARCH
KAFKA_VERSION=$KAFKA_VERSION
TLS_ENABLED=$TLS_ENABLED
KEY_FILE=$KEY_FILE
EOF
echo ""
echo "==> Wrote $ENV_FILE (used by Makefile)"

echo ""
echo "==> Done! Use the Makefile to manage the cluster:"
echo "  make start          # Start all brokers"
echo "  make status         # Check service status"
echo "  make logs           # Tail logs from brokers"
echo "  make run-benchmark  # Run kafka-producer-perf-test"
echo "  make stop           # Stop cluster"
