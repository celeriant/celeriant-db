#!/bin/bash
# Generate TLS certificates and keystores for the Kafka KRaft benchmark cluster.
#
# Produces:
#   - CA cert + key
#   - Per-broker keystores (PKCS12) with certs signed by the CA
#   - Client truststore (PKCS12) containing the CA cert
#   - Client keystore (PKCS12) for mutual TLS (optional, for authenticated benchmarks)
#
# Usage:
#   ./generate-certs.sh <broker1-ip> <broker2-ip> <broker3-ip>
#
# Outputs to ./certs/

set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "Usage: $0 <broker1-ip> <broker2-ip> <broker3-ip>"
  exit 1
fi

BROKER1_IP=$1
BROKER2_IP=$2
BROKER3_IP=$3

CERT_DIR="$(dirname "$0")/../certs"
rm -rf "$CERT_DIR"
mkdir -p "$CERT_DIR"
cd "$CERT_DIR"

STORE_PASS="kafka-bench-changeit"
KEY_PASS="$STORE_PASS"
VALIDITY=30

echo "==> Generating CA"
openssl ecparam -genkey -name prime256v1 -out ca.key
openssl req -new -x509 -key ca.key -out ca.crt \
  -days "$VALIDITY" -subj "/CN=kafka-bench-ca"

# Create truststore containing the CA cert (shared by all nodes + clients)
keytool -importcert -alias ca -file ca.crt \
  -keystore truststore.p12 -storetype PKCS12 \
  -storepass "$STORE_PASS" -noprompt

echo "==> Generating broker certificates"
BROKER_IPS=("$BROKER1_IP" "$BROKER2_IP" "$BROKER3_IP")

for i in 1 2 3; do
  IP="${BROKER_IPS[$((i-1))]}"
  NAME="broker${i}"

  cat > "${NAME}.cnf" <<EOF
[req]
distinguished_name = req_dn
req_extensions = v3_req
prompt = no

[req_dn]
CN = kafka-${NAME}

[v3_req]
subjectAltName = IP:${IP},IP:127.0.0.1,DNS:localhost
extendedKeyUsage = serverAuth, clientAuth
EOF

  openssl ecparam -genkey -name prime256v1 -out "${NAME}.key"
  openssl req -new -key "${NAME}.key" -out "${NAME}.csr" -config "${NAME}.cnf"
  openssl x509 -req -in "${NAME}.csr" -CA ca.crt -CAkey ca.key \
    -CAcreateserial -out "${NAME}.crt" -days "$VALIDITY" \
    -extensions v3_req -extfile "${NAME}.cnf"

  # Create PKCS12 keystore for this broker
  openssl pkcs12 -export -in "${NAME}.crt" -inkey "${NAME}.key" \
    -chain -CAfile ca.crt -name "${NAME}" \
    -out "${NAME}.keystore.p12" -passout "pass:${STORE_PASS}"

  echo "  Generated ${NAME} keystore (SAN: IP:${IP})"
done

echo "==> Generating client certificate"
cat > client.cnf <<EOF
[req]
distinguished_name = req_dn
req_extensions = v3_req
prompt = no

[req_dn]
CN = kafka-bench-client

[v3_req]
extendedKeyUsage = clientAuth
EOF

openssl ecparam -genkey -name prime256v1 -out client.key
openssl req -new -key client.key -out client.csr -config client.cnf
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key \
  -CAcreateserial -out client.crt -days "$VALIDITY" \
  -extensions v3_req -extfile client.cnf

openssl pkcs12 -export -in client.crt -inkey client.key \
  -chain -CAfile ca.crt -name "client" \
  -out client.keystore.p12 -passout "pass:${STORE_PASS}"

# Clean up CSRs, serial files, configs
rm -f *.csr *.srl *.cnf

echo ""
echo "Certs generated in $CERT_DIR:"
ls -la *.crt *.key *.p12
echo ""
echo "Store password: ${STORE_PASS}"
echo ""
echo "Files:"
echo "  ca.crt / ca.key            — CA certificate and key"
echo "  truststore.p12             — Truststore (CA cert, shared by all)"
echo "  broker{1,2,3}.keystore.p12 — Per-broker keystores"
echo "  client.keystore.p12        — Client keystore (for mTLS benchmarks)"
