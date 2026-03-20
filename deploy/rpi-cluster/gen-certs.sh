#!/usr/bin/env bash
# Generate TLS certificates and distribute to data nodes.
# Two separate CAs enforce trust domain isolation:
#   - Client CA: signs client certs + client-facing server cert (port 10000)
#   - Intracluster CA: signs node certs (port 10001)
set -euo pipefail

source config.env

CERT_DIR="${CERT_DIR}"
DATA_NODES=("$LEADER_HOST" "$FOLLOWER_HOST")

GREEN='\033[0;32m'
BOLD='\033[1m'
RESET='\033[0m'

if [ -d "$CERT_DIR" ] && [ -f "$CERT_DIR/client-ca.crt" ]; then
    printf "Certs already exist in %s/. To regenerate, delete the directory first.\n" "$CERT_DIR"
    exit 0
fi

mkdir -p "$CERT_DIR"
cd "$CERT_DIR"

printf "${BOLD}>>> Generating certificates${RESET}\n"

# --- Client CA ---
openssl ecparam -genkey -name prime256v1 -out client-ca.key 2>/dev/null
openssl req -new -x509 -key client-ca.key -out client-ca.crt -days 365 \
    -subj "/CN=celeriant-client-ca" 2>/dev/null

# --- Intracluster CA ---
openssl ecparam -genkey -name prime256v1 -out intracluster-ca.key 2>/dev/null
openssl req -new -x509 -key intracluster-ca.key -out intracluster-ca.crt -days 365 \
    -subj "/CN=celeriant-intracluster-ca" 2>/dev/null

# SANs covering both data node hostnames
ALL_SANS="DNS:${LEADER_HOST},DNS:${FOLLOWER_HOST},DNS:localhost,IP:127.0.0.1"

# --- Node cert (intracluster CA) ---
cat > node.cnf <<EOF
[req]
distinguished_name = req_dn
req_extensions = v3_req
prompt = no
[req_dn]
CN = celeriant-node
[v3_req]
subjectAltName = ${ALL_SANS}
EOF
openssl ecparam -genkey -name prime256v1 -out node.key 2>/dev/null
openssl req -new -key node.key -out node.csr -config node.cnf 2>/dev/null
openssl x509 -req -in node.csr -CA intracluster-ca.crt -CAkey intracluster-ca.key \
    -CAcreateserial -out node.crt -days 365 -extensions v3_req -extfile node.cnf 2>/dev/null

# --- Client-facing server cert (client CA) ---
cat > client-server.cnf <<EOF
[req]
distinguished_name = req_dn
req_extensions = v3_req
prompt = no
[req_dn]
CN = celeriant-client-server
[v3_req]
subjectAltName = ${ALL_SANS}
EOF
openssl ecparam -genkey -name prime256v1 -out client-server.key 2>/dev/null
openssl req -new -key client-server.key -out client-server.csr -config client-server.cnf 2>/dev/null
openssl x509 -req -in client-server.csr -CA client-ca.crt -CAkey client-ca.key \
    -CAcreateserial -out client-server.crt -days 365 -extensions v3_req -extfile client-server.cnf 2>/dev/null

# --- Client cert (client CA, for benchmark client) ---
cat > client.cnf <<EOF
[req]
distinguished_name = req_dn
req_extensions = v3_req
prompt = no
[req_dn]
CN = celeriant-client
[v3_req]
subjectAltName = ${ALL_SANS}
EOF
openssl ecparam -genkey -name prime256v1 -out client.key 2>/dev/null
openssl req -new -key client.key -out client.csr -config client.cnf 2>/dev/null
openssl x509 -req -in client.csr -CA client-ca.crt -CAkey client-ca.key \
    -CAcreateserial -out client.crt -days 365 -extensions v3_req -extfile client.cnf 2>/dev/null

# Clean up CSRs and configs
rm -f *.csr *.cnf *.srl

cd ..
printf "${GREEN}Certificates generated in %s/${RESET}\n" "$CERT_DIR"

# --- Distribute to nodes ---
printf "\n${BOLD}>>> Distributing certificates to data nodes${RESET}\n"

for HOST in "${DATA_NODES[@]}"; do
    printf ">>> %s\n" "$HOST"
    ssh "$HOST" "sudo mkdir -p ${REMOTE_CERT_DIR}"
    scp "$CERT_DIR"/{client-ca.crt,intracluster-ca.crt,node.crt,node.key,client-server.crt,client-server.key} "$HOST":/tmp/
    ssh "$HOST" bash -s <<REMOTE_CERTS
sudo mv /tmp/client-ca.crt /tmp/intracluster-ca.crt /tmp/node.crt /tmp/node.key \
    /tmp/client-server.crt /tmp/client-server.key ${REMOTE_CERT_DIR}/
sudo chown root:root ${REMOTE_CERT_DIR}/*
sudo chmod 644 ${REMOTE_CERT_DIR}/*.crt
sudo chmod 600 ${REMOTE_CERT_DIR}/node.key ${REMOTE_CERT_DIR}/client-server.key
REMOTE_CERTS
done

printf "${GREEN}Certificates deployed.${RESET}\n"
