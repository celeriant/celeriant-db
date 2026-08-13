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

# Campaign knobs and the node env generator live in node-env-lib.sh, shared with the cell
# sweep driver so the two cannot drift apart.
source "$SCRIPT_DIR/node-env-lib.sh"
init_campaign_knobs

# Read CDK outputs
STACK_NAME="CeleriantKtlsTestStack"
echo "==> Reading stack outputs from $STACK_NAME"

get_output() {
  aws cloudformation describe-stacks --stack-name "$STACK_NAME" \
    --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" --output text
}

LEADER_PUB=$(get_output LeaderPublicIp)
LEADER_IP=$(get_output LeaderPrivateIp)
# Absent when the stack was synthesised with -c dataNodeCount=1. An empty follower is the
# standalone signal throughout this script; DATA_NODE_PUBS drives every per-node loop so a
# single-node stack never tries to reach a node that does not exist.
FOLLOWER_PUB=$(get_output FollowerPublicIp)
FOLLOWER_IP=$(get_output FollowerPrivateIp)
DATA_NODE_PUBS="$LEADER_PUB"
DATA_NODE_COUNT=1
if [[ -n "$FOLLOWER_PUB" ]]; then
  DATA_NODE_PUBS="$LEADER_PUB $FOLLOWER_PUB"
  DATA_NODE_COUNT=2
fi

# Topology must agree with the standalone knob, or the run produces a plausible-looking
# number that means nothing:
#
#   standalone + follower  — both nodes get the same env and run as two UNRELATED databases.
#                            run-benchmark.sh seeds the client pool with both addresses, so
#                            the reported throughput is the sum of two separate DBs.
#   replicated + no follower — nothing to replicate to; the replication delay under test
#                            never engages, and the cell silently measures standalone.
#
# Both are wrong-number generators rather than crashes, so fail here instead.
if [[ "$STANDALONE" == "true" && -n "$FOLLOWER_PUB" ]]; then
  echo "ERROR: STANDALONE=true but the stack has a follower ($FOLLOWER_PUB)." >&2
  echo "       Redeploy the stack with -c dataNodeCount=1 (make infra DATA_NODES=1)." >&2
  exit 1
fi
if [[ "$STANDALONE" != "true" && -z "$FOLLOWER_PUB" ]]; then
  echo "ERROR: single-data-node stack but STANDALONE is not 'true' (got '${STANDALONE:-unset}')." >&2
  echo "       Set STANDALONE=true, or redeploy with -c dataNodeCount=2 for a replicated run." >&2
  exit 1
fi
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
if [[ -n "$FOLLOWER_PUB" ]]; then
  echo "  Follower: $FOLLOWER_PUB ($FOLLOWER_IP)"
else
  echo "  Follower: (none — single data node)"
fi
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
  # `|| true`: grep exits 1 when the binary references no GLIBC symbols, and under
  # `set -o pipefail` that kills the whole script silently, mid-deploy, with no message.
  MAX_GLIBC=$(objdump -T "$BINARY" 2>/dev/null | grep -oE 'GLIBC_[0-9.]+' | sed 's/GLIBC_//' | sort -V | tail -1 || true)
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
  if ! echo "$EXISTING_SANS" | grep -q "$LEADER_IP"; then
    echo "  Cert SANs don't match current IPs — regenerating"
    NEEDS_CERTS=true
  elif [[ -n "$FOLLOWER_IP" ]] && ! echo "$EXISTING_SANS" | grep -q "$FOLLOWER_IP"; then
    echo "  Cert SANs don't match current IPs — regenerating"
    NEEDS_CERTS=true
  fi
fi
if [[ "$NEEDS_CERTS" == "true" ]]; then
  echo "==> Generating TLS certificates"
  CLIENT1_IP=$(get_output ClientPrivateIp)
  # generate-certs.sh takes exactly three IPs and interpolates each into the SAN list, so an
  # empty follower would emit a malformed "IP:" and fail openssl. With one data node the
  # leader IP stands in for the follower slot; a duplicated SAN entry is harmless.
  bash "$SCRIPT_DIR/generate-certs.sh" "$LEADER_IP" "${FOLLOWER_IP:-$LEADER_IP}" "$CLIENT1_IP"
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

# Deploy env files to /etc/celeriant/ (read by systemd EnvironmentFile)
generate_env "$LEADER_IP" > /tmp/celeriant-leader.env
$SCP /tmp/celeriant-leader.env ec2-user@${LEADER_PUB}:/tmp/celeriant.env
$SSH@${LEADER_PUB} 'sudo mv /tmp/celeriant.env /etc/celeriant/celeriant.env'

if [[ -n "$FOLLOWER_PUB" ]]; then
  generate_env "$FOLLOWER_IP" > /tmp/celeriant-follower.env
  $SCP /tmp/celeriant-follower.env ec2-user@${FOLLOWER_PUB}:/tmp/celeriant.env
  $SSH@${FOLLOWER_PUB} 'sudo mv /tmp/celeriant.env /etc/celeriant/celeriant.env'
fi

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
DATA_NODE_COUNT=$DATA_NODE_COUNT
TLS_MODE=$TLS_MODE
S3_ENABLED=$S3_ENABLED
NUM_SHARDS=$NUM_SHARDS
FSYNC_DELAY_US=$FSYNC_DELAY_US
REPLICATION_DELAY_US=$REPLICATION_DELAY_US
RESERVE_COORDINATOR_SHARD=$RESERVE_COORDINATOR_SHARD
MESH_CHANNEL_SIZE=$MESH_CHANNEL_SIZE
STANDALONE=$STANDALONE
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
