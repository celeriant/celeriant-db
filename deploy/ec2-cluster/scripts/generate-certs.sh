#!/bin/bash
# Generate dual-CA TLS certificates for the EC2 kTLS test cluster.
#
# Mirrors the RPi setup (docs/pending/rpi-ktls-testbed.md):
#   - Intracluster CA → signs node certs (replication port 10001)
#   - Client CA → signs client-server cert (client port 10000) + client cert (benchmark)
#
# Usage:
#   ./generate-certs.sh <leader-private-ip> <follower-private-ip> <client-private-ip>
#
# Outputs to ./certs/

set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "Usage: $0 <leader-ip> <follower-ip> <client-ip>"
  exit 1
fi

LEADER_IP=$1
FOLLOWER_IP=$2
CLIENT_IP=$3

CERT_DIR="$(dirname "$0")/../certs"
rm -rf "$CERT_DIR"
mkdir -p "$CERT_DIR"
cd "$CERT_DIR"

echo "==> Generating intracluster CA"
openssl ecparam -genkey -name prime256v1 -out intracluster-ca.key
openssl req -new -x509 -key intracluster-ca.key -out intracluster-ca.crt \
  -days 30 -subj "/CN=celeriant-intracluster-ca"

echo "==> Generating client CA"
openssl ecparam -genkey -name prime256v1 -out client-ca.key
openssl req -new -x509 -key client-ca.key -out client-ca.crt \
  -days 30 -subj "/CN=celeriant-client-ca"

echo "==> Generating node cert (signed by intracluster CA)"
cat > node.cnf <<EOF
[req]
distinguished_name = req_dn
req_extensions = v3_req
prompt = no

[req_dn]
CN = celeriant-node

[v3_req]
subjectAltName = IP:${LEADER_IP},IP:${FOLLOWER_IP},IP:127.0.0.1,DNS:localhost
extendedKeyUsage = serverAuth, clientAuth
EOF

openssl ecparam -genkey -name prime256v1 -out node.key
openssl req -new -key node.key -out node.csr -config node.cnf
openssl x509 -req -in node.csr -CA intracluster-ca.crt -CAkey intracluster-ca.key \
  -CAcreateserial -out node.crt -days 30 -extensions v3_req -extfile node.cnf

echo "==> Generating client-server cert (signed by client CA, presented on port 10000)"
cat > client-server.cnf <<EOF
[req]
distinguished_name = req_dn
req_extensions = v3_req
prompt = no

[req_dn]
CN = celeriant-client-server

[v3_req]
subjectAltName = IP:${LEADER_IP},IP:${FOLLOWER_IP},IP:127.0.0.1,DNS:localhost
extendedKeyUsage = serverAuth
EOF

openssl ecparam -genkey -name prime256v1 -out client-server.key
openssl req -new -key client-server.key -out client-server.csr -config client-server.cnf
openssl x509 -req -in client-server.csr -CA client-ca.crt -CAkey client-ca.key \
  -CAcreateserial -out client-server.crt -days 30 -extensions v3_req -extfile client-server.cnf

echo "==> Generating client cert (signed by client CA, used by benchmark)"
cat > client.cnf <<EOF
[req]
distinguished_name = req_dn
req_extensions = v3_req
prompt = no

[req_dn]
CN = celeriant-bench-client

[v3_req]
subjectAltName = IP:${CLIENT_IP},DNS:localhost
extendedKeyUsage = clientAuth
EOF

openssl ecparam -genkey -name prime256v1 -out client.key
openssl req -new -key client.key -out client.csr -config client.cnf
openssl x509 -req -in client.csr -CA client-ca.crt -CAkey client-ca.key \
  -CAcreateserial -out client.crt -days 30 -extensions v3_req -extfile client.cnf

# Clean up CSRs and serial files
rm -f *.csr *.srl *.cnf

echo ""
echo "Certs generated in $CERT_DIR:"
ls -la *.crt *.key
echo ""
echo "Trust model:"
echo "  Client port (10000) → presents client-server.crt (client CA signed)"
echo "  Replication port (10001) → presents node.crt (intracluster CA signed)"
echo "  Benchmark client → uses client.crt (client CA signed), verifies server via client-ca.crt"
