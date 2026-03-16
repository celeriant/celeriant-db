#!/bin/bash
# Deploy Celeriant binaries and certs to the EC2 kTLS test cluster.
#
# Reads IPs from CDK stack outputs. Run after `npx cdk deploy`.
#
# Usage:
#   ./deploy.sh [--key-file ~/.ssh/your-key.pem]
#
# Prerequisites:
#   - CDK stack deployed (npx cdk deploy)
#   - Certs generated (./generate-certs.sh)
#   - Binaries built (cargo build --release -p celeriant -p celeriant_integration_tests)

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
CLIENT_PUB=$(get_output ClientPublicIp)
LEADER_IP=$(get_output LeaderPrivateIp)
FOLLOWER_IP=$(get_output FollowerPrivateIp)
BUCKET=$(get_output BucketName)
REGION=$(get_output Region)

echo "  Leader:   $LEADER_PUB ($LEADER_IP)"
echo "  Follower: $FOLLOWER_PUB ($FOLLOWER_IP)"
echo "  Client:   $CLIENT_PUB"
echo "  Bucket:   $BUCKET"
echo "  Region:   $REGION"

# Check prerequisites
BINARY="$REPO_ROOT/target/release/celeriant"
TEST_BINARY="$REPO_ROOT/target/release/celeriant-integration-tests"
CLI_BINARY="$REPO_ROOT/target/release/celeriant_cli"
if [[ ! -f "$BINARY" ]]; then
  echo "ERROR: Server binary not found at $BINARY"
  echo "Run: cargo build --release -p celeriant -p celeriant_integration_tests -p celeriant_cli"
  exit 1
fi
if [[ ! -f "$TEST_BINARY" ]]; then
  echo "ERROR: Test binary not found at $TEST_BINARY"
  echo "Run: cargo build --release -p celeriant -p celeriant_integration_tests -p celeriant_cli"
  exit 1
fi
if [[ ! -f "$CLI_BINARY" ]]; then
  echo "ERROR: CLI binary not found at $CLI_BINARY"
  echo "Run: cargo build --release -p celeriant -p celeriant_integration_tests -p celeriant_cli"
  exit 1
fi
if [[ ! -f "$CERT_DIR/node.crt" ]]; then
  echo "ERROR: Certs not found in $CERT_DIR"
  echo "Run: ./generate-certs.sh $LEADER_IP $FOLLOWER_IP <client-ip>"
  exit 1
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

echo "==> Deploying test binary and CLI to client node"
$SCP "$TEST_BINARY" ec2-user@${CLIENT_PUB}:/tmp/celeriant-integration-tests
$SCP "$CLI_BINARY" ec2-user@${CLIENT_PUB}:/tmp/celeriant_cli
$SSH@${CLIENT_PUB} 'sudo mv /tmp/celeriant-integration-tests /tmp/celeriant_cli /usr/local/bin/ && sudo chmod +x /usr/local/bin/celeriant-integration-tests /usr/local/bin/celeriant_cli'

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

echo "==> Deploying certs to client node"
$SCP "$CERT_DIR/client-ca.crt" "$CERT_DIR/client.crt" "$CERT_DIR/client.key" \
     ec2-user@${CLIENT_PUB}:/tmp/
$SSH@${CLIENT_PUB} 'sudo mv /tmp/client-ca.crt /tmp/client.crt /tmp/client.key /etc/celeriant/certs/ && sudo chmod 600 /etc/celeriant/certs/client.key'

echo ""
echo "==> Generating env files"

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

generate_env "$LEADER_IP" > /tmp/celeriant-leader.env
generate_env "$FOLLOWER_IP" > /tmp/celeriant-follower.env

$SCP /tmp/celeriant-leader.env ec2-user@${LEADER_PUB}:/tmp/celeriant.env
$SCP /tmp/celeriant-follower.env ec2-user@${FOLLOWER_PUB}:/tmp/celeriant.env

echo ""
echo "==> Done! To start the cluster:"
echo ""
echo "  # Terminal 1 — leader"
echo "  ssh $SSH_OPTS ec2-user@${LEADER_PUB}"
echo "  set -a && source /tmp/celeriant.env && set +a && celeriant"
echo ""
echo "  # Terminal 2 — follower (wait ~5s for leader to grab S3 lease)"
echo "  ssh $SSH_OPTS ec2-user@${FOLLOWER_PUB}"
echo "  set -a && source /tmp/celeriant.env && set +a && celeriant"
echo ""
echo "  # Terminal 3 — benchmark client"
echo "  ssh $SSH_OPTS ec2-user@${CLIENT_PUB}"
echo "  CELERIANT_TLS_CA_CERT=/etc/celeriant/certs/client-ca.crt \\"
echo "  CELERIANT_TLS_CLIENT_CERT=/etc/celeriant/certs/client.crt \\"
echo "  CELERIANT_TLS_CLIENT_KEY=/etc/celeriant/certs/client.key \\"
echo "    celeriant-integration-tests batch --address ${LEADER_IP}:10000"
