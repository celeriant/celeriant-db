#!/bin/bash
# Deploy Celeriant binaries, certs, and env files to the EC2 cluster.
# Installs systemd services on data nodes (mirrors deploy/rpi-cluster).
#
# Reads IPs from CDK stack outputs. Run after `npx cdk deploy`.
# Supports multiple client nodes (controlled by -c clientCount=N in CDK).
#
# Usage:
#   ./deploy.sh --key-file ~/.ssh/your-key.pem

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CDK_DIR="$(dirname "$SCRIPT_DIR")"
REPO_ROOT="$(cd "$CDK_DIR/../.." && pwd)"
CERT_DIR="$CDK_DIR/certs"

# Parse args
KEY_FILE=""
while [[ $# -gt 0 ]]; do
  case $1 in
    --key-file) KEY_FILE="$2"; shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
if [[ -n "$KEY_FILE" ]]; then
  SSH_OPTS="$SSH_OPTS -i $KEY_FILE"
fi

# Read CDK outputs
STACK_NAME="CeleriantKtlsTestStack"
echo "==> Reading stack outputs from $STACK_NAME"

get_output() {
  aws cloudformation describe-stacks --stack-name "$STACK_NAME" \
    --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" --output text
}

LEADER_PUB=$(get_output LeaderPublicIp)
FOLLOWER_PUB=$(get_output FollowerPublicIp)
LEADER_IP=$(get_output LeaderPrivateIp)
FOLLOWER_IP=$(get_output FollowerPrivateIp)
BUCKET=$(get_output BucketName)
REGION=$(get_output Region)
INSTANCE_TYPE=$(get_output InstanceType)
CLIENT_INSTANCE_TYPE=$(get_output ClientInstanceType)
CLIENT_COUNT=$(get_output ClientCount 2>/dev/null || echo "1")
STORAGE_TYPE=$(get_output StorageType)
ARCH=$(get_output Architecture)

# Collect client IPs as comma-separated (safe for Make/shell sourcing)
CLIENT_PUBS=""
CLIENT_PRIVS=""
for i in $(seq 1 "$CLIENT_COUNT"); do
  if [[ $i -eq 1 ]]; then
    PUB=$(get_output ClientPublicIp)
    PRIV=$(get_output ClientPrivateIp)
  else
    PUB=$(get_output "Client${i}PublicIp")
    PRIV=$(get_output "Client${i}PrivateIp")
  fi
  if [[ -n "$CLIENT_PUBS" ]]; then CLIENT_PUBS="$CLIENT_PUBS,"; CLIENT_PRIVS="$CLIENT_PRIVS,"; fi
  CLIENT_PUBS="$CLIENT_PUBS$PUB"
  CLIENT_PRIVS="$CLIENT_PRIVS$PRIV"
done

echo "  Leader:   $LEADER_PUB ($LEADER_IP)"
echo "  Follower: $FOLLOWER_PUB ($FOLLOWER_IP)"
echo "  Clients:  $CLIENT_PUBS ($CLIENT_COUNT nodes)"
echo "  Bucket:   $BUCKET"
echo "  Region:   $REGION"
echo "  Instance: $INSTANCE_TYPE ($STORAGE_TYPE storage)"
echo "  Client:   $CLIENT_INSTANCE_TYPE"
echo "  Arch:     $ARCH"

# Check prerequisites
BINARY="$REPO_ROOT/target/release/celeriant"
TEST_BINARY="$REPO_ROOT/target/release/celeriant-integration-tests"
CLI_BINARY="$REPO_ROOT/target/release/celeriant_cli"
if [[ ! -f "$BINARY" ]]; then
  echo "ERROR: Server binary not found at $BINARY"
  echo "Build in the amazonlinux:2023 container: make build (x86_64) or make build-arm (ARM)"
  exit 1
fi
if [[ ! -f "$TEST_BINARY" ]]; then
  echo "ERROR: Test binary not found at $TEST_BINARY"
  echo "Build in the amazonlinux:2023 container: make build (x86_64) or make build-arm (ARM)"
  exit 1
fi
if [[ ! -f "$CLI_BINARY" ]]; then
  echo "ERROR: CLI binary not found at $CLI_BINARY"
  echo "Build in the amazonlinux:2023 container: make build (x86_64) or make build-arm (ARM)"
  exit 1
fi

# Guard against host-built binaries. A plain `cargo build` links against your
# machine's glibc, which is newer than Amazon Linux 2023's 2.34 — the server then
# fails on boot with "GLIBC_2.XX not found". `make build` / `make build-arm` build
# inside the amazonlinux:2023 container so the binary links against 2.34.
if command -v objdump >/dev/null 2>&1; then
  MAX_GLIBC=$(objdump -T "$BINARY" 2>/dev/null | grep -oE 'GLIBC_[0-9.]+' | sed 's/GLIBC_//' | sort -V | tail -1)
  if [[ -n "$MAX_GLIBC" && "$(printf '2.34\n%s\n' "$MAX_GLIBC" | sort -V | tail -1)" != "2.34" ]]; then
    echo "ERROR: $BINARY links against GLIBC $MAX_GLIBC, but Amazon Linux 2023 has 2.34."
    echo "       This is a host build and will not run on EC2. Rebuild in the container:"
    echo "         make build       # x86_64 (c6id, i4i, c7i)"
    echo "         make build-arm   # ARM64 (i4g, c7g)"
    exit 1
  fi
fi

# Auto-regenerate certs if missing or if SANs don't match current IPs
NEEDS_CERTS=false
if [[ ! -f "$CERT_DIR/node.crt" ]]; then
  NEEDS_CERTS=true
else
  EXISTING_SANS=$(openssl x509 -in "$CERT_DIR/client-server.crt" -noout -text 2>/dev/null \
    | grep -A1 "Subject Alternative Name" | tail -1 || echo "")
  if ! echo "$EXISTING_SANS" | grep -q "$LEADER_IP" || ! echo "$EXISTING_SANS" | grep -q "$FOLLOWER_IP"; then
    echo "  Cert SANs don't match current IPs — regenerating"
    NEEDS_CERTS=true
  fi
fi
if [[ "$NEEDS_CERTS" == "true" ]]; then
  echo "==> Generating TLS certificates"
  CLIENT1_IP=$(get_output ClientPrivateIp)
  bash "$SCRIPT_DIR/generate-certs.sh" "$LEADER_IP" "$FOLLOWER_IP" "$CLIENT1_IP"
fi

SSH="ssh $SSH_OPTS ec2-user"
SCP="scp $SSH_OPTS"

echo ""
echo "==> Deploying server binary to data nodes"
for HOST in $LEADER_PUB $FOLLOWER_PUB; do
  $SCP "$BINARY" ec2-user@${HOST}:/tmp/celeriant
  $SSH@${HOST} 'sudo mv /tmp/celeriant /usr/local/bin/ && sudo chmod +x /usr/local/bin/celeriant'
  echo "  Deployed to $HOST"
done

echo "==> Deploying server binary, test binary, and CLI to client nodes"
for HOST in ${CLIENT_PUBS//,/ }; do
  $SCP "$BINARY" ec2-user@${HOST}:/tmp/celeriant
  $SCP "$TEST_BINARY" ec2-user@${HOST}:/tmp/celeriant-integration-tests
  $SCP "$CLI_BINARY" ec2-user@${HOST}:/tmp/celeriant_cli
  $SSH@${HOST} 'sudo mv /tmp/celeriant /tmp/celeriant-integration-tests /tmp/celeriant_cli /usr/local/bin/ && sudo chmod +x /usr/local/bin/celeriant /usr/local/bin/celeriant-integration-tests /usr/local/bin/celeriant_cli'
  echo "  Deployed to $HOST"
done

echo ""
echo "==> Deploying certs to data nodes"
for HOST in $LEADER_PUB $FOLLOWER_PUB; do
  $SCP "$CERT_DIR/client-ca.crt" "$CERT_DIR/intracluster-ca.crt" \
       "$CERT_DIR/node.crt" "$CERT_DIR/node.key" \
       "$CERT_DIR/client-server.crt" "$CERT_DIR/client-server.key" \
       ec2-user@${HOST}:/tmp/
  $SSH@${HOST} 'sudo mv /tmp/client-ca.crt /tmp/intracluster-ca.crt /tmp/node.crt /tmp/node.key /tmp/client-server.crt /tmp/client-server.key /etc/celeriant/certs/ && sudo chmod 600 /etc/celeriant/certs/*.key'
  echo "  Deployed to $HOST"
done

echo "==> Deploying certs to client nodes"
for HOST in ${CLIENT_PUBS//,/ }; do
  $SCP "$CERT_DIR/client-ca.crt" "$CERT_DIR/client.crt" "$CERT_DIR/client.key" \
       ec2-user@${HOST}:/tmp/
  $SSH@${HOST} 'sudo mv /tmp/client-ca.crt /tmp/client.crt /tmp/client.key /etc/celeriant/certs/ && sudo chmod 600 /etc/celeriant/certs/client.key'
  echo "  Deployed to $HOST"
done

echo ""
echo "==> Generating and deploying env files"

generate_env() {
  local NODE_IP=$1
  cat <<EOF
CELERIANT_DATA_ROOT=/var/lib/celeriant
CELERIANT_LISTEN_ADDRESS=0.0.0.0
CELERIANT_CLIENT_PORT=10000
CELERIANT_REPLICATION_PORT=10001
CELERIANT_LOG_LEVEL=info
CELERIANT_METRICS_ENABLED=true
CELERIANT_METRICS_PORT=9090
CELERIANT_ADVERTISED_CLIENT_ADDRESS=${NODE_IP}:10000
CELERIANT_ADVERTISED_REPLICATION_ADDRESS=${NODE_IP}:10001
CELERIANT_S3_ENABLED=true
CELERIANT_S3_REGION=${REGION}
CELERIANT_S3_BUCKET=${BUCKET}
CELERIANT_TLS_MODE=strict
CELERIANT_TLS_CA_CERT=/etc/celeriant/certs/client-ca.crt
CELERIANT_TLS_INTRACLUSTER_CA_CERT=/etc/celeriant/certs/intracluster-ca.crt
CELERIANT_TLS_NODE_CERT=/etc/celeriant/certs/node.crt
CELERIANT_TLS_NODE_KEY=/etc/celeriant/certs/node.key
CELERIANT_TLS_CLIENT_CERT=/etc/celeriant/certs/client-server.crt
CELERIANT_TLS_CLIENT_KEY=/etc/celeriant/certs/client-server.key
CELERIANT_TLS_CLIENT_AUTH=require
CELERIANT_MEMORY_CONSUMPTION_PERCENT=60
CELERIANT_SHARD_LOG_PREALLOCATE_BYTES=134217728
EOF
}

# Deploy env files to /etc/celeriant/ (read by systemd EnvironmentFile)
generate_env "$LEADER_IP" > /tmp/celeriant-leader.env
generate_env "$FOLLOWER_IP" > /tmp/celeriant-follower.env

$SCP /tmp/celeriant-leader.env ec2-user@${LEADER_PUB}:/tmp/celeriant.env
$SSH@${LEADER_PUB} 'sudo mv /tmp/celeriant.env /etc/celeriant/celeriant.env'

$SCP /tmp/celeriant-follower.env ec2-user@${FOLLOWER_PUB}:/tmp/celeriant.env
$SSH@${FOLLOWER_PUB} 'sudo mv /tmp/celeriant.env /etc/celeriant/celeriant.env'

echo "==> Tuning kernel network parameters on all nodes"
for HOST in $LEADER_PUB $FOLLOWER_PUB ${CLIENT_PUBS//,/ }; do
  $SSH@${HOST} 'sudo sysctl -w net.ipv4.tcp_tw_reuse=1 net.ipv4.tcp_max_syn_backlog=65535 net.core.netdev_max_backlog=65535 >/dev/null'
done
echo "  Done"

echo "==> Enabling systemd service on data nodes"
for HOST in $LEADER_PUB $FOLLOWER_PUB; do
  $SSH@${HOST} 'sudo systemctl enable celeriant'
  echo "  Enabled on $HOST"
done

# Write .cluster-env for Makefile consumption
ENV_FILE="$CDK_DIR/.cluster-env"
cat > "$ENV_FILE" <<EOF
LEADER_PUB=$LEADER_PUB
FOLLOWER_PUB=$FOLLOWER_PUB
CLIENT_PUBS=$CLIENT_PUBS
CLIENT_PRIVS=$CLIENT_PRIVS
CLIENT_COUNT=$CLIENT_COUNT
LEADER_IP=$LEADER_IP
FOLLOWER_IP=$FOLLOWER_IP
BUCKET=$BUCKET
REGION=$REGION
INSTANCE_TYPE=$INSTANCE_TYPE
CLIENT_INSTANCE_TYPE=$CLIENT_INSTANCE_TYPE
STORAGE_TYPE=$STORAGE_TYPE
ARCH=$ARCH
KEY_FILE=$KEY_FILE
EOF
echo ""
echo "==> Wrote $ENV_FILE (used by Makefile)"

echo ""
echo "==> Done! Use the Makefile to manage the cluster:"
echo "  make start        # Start leader then follower"
echo "  make status       # Check service status"
echo "  make logs         # Tail logs from both nodes"
echo "  make run-benchmark # Run benchmark and collect results"
echo "  make stop         # Stop cluster"
