#!/usr/bin/env bash
# Validate the PKI before a run: present, unexpired, and matching what the data
# nodes actually trust. Run via `make check-certs`.
#
# The failure this exists to prevent: local certs that look fine but were signed
# by a different CA than the one deployed, which surfaces only as a TLS
# handshake error deep inside a bench.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"
source config.env

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[0;33m'; BOLD='\033[1m'; RESET='\033[0m'
EXPIRY_WARN_SECS=$((30 * 24 * 3600))
DATA_NODES=("$LEADER_HOST" "$FOLLOWER_HOST")
fail=0

printf "${BOLD}>>> Checking certificates in %s/${RESET}\n" "$CERT_DIR"

# --- Present? ---
required=(client-ca.crt client.crt client.key intracluster-ca.crt node.crt client-server.crt)
for f in "${required[@]}"; do
    if [ ! -f "$CERT_DIR/$f" ]; then
        printf "${RED}MISSING${RESET} %s/%s — run 'make certs'\n" "$CERT_DIR" "$f"
        fail=1
    fi
done
if [ "$fail" -ne 0 ]; then
    printf "\n${RED}${BOLD}PKI incomplete. Run 'make certs' to generate and distribute.${RESET}\n"
    exit 1
fi

# --- Unexpired? gen-certs.sh issues 365-day certs, so this bites eventually. ---
for f in client-ca.crt client.crt intracluster-ca.crt node.crt client-server.crt; do
    expires=$(openssl x509 -in "$CERT_DIR/$f" -noout -enddate 2>/dev/null | cut -d= -f2)
    if ! openssl x509 -in "$CERT_DIR/$f" -noout -checkend 0 >/dev/null 2>&1; then
        printf "${RED}EXPIRED${RESET} %-22s %s — run 'make certs'\n" "$f" "$expires"
        fail=1
    elif ! openssl x509 -in "$CERT_DIR/$f" -noout -checkend "$EXPIRY_WARN_SECS" >/dev/null 2>&1; then
        printf "${YELLOW}EXPIRING${RESET} %-21s %s\n" "$f" "$expires"
    else
        printf "${GREEN}ok${RESET}      %-22s expires %s\n" "$f" "$expires"
    fi
done

# --- Does the local CA match what the nodes trust? ---
fingerprint() { openssl x509 -noout -fingerprint -sha256 -in "$1" 2>/dev/null | cut -d= -f2; }

local_client_ca=$(fingerprint "$CERT_DIR/client-ca.crt")
local_intra_ca=$(fingerprint "$CERT_DIR/intracluster-ca.crt")

for HOST in "${DATA_NODES[@]}"; do
    remote=$(ssh -o ConnectTimeout=5 -o BatchMode=yes "$HOST" \
        "sudo openssl x509 -noout -fingerprint -sha256 -in ${REMOTE_CERT_DIR}/client-ca.crt 2>/dev/null | cut -d= -f2; \
         sudo openssl x509 -noout -fingerprint -sha256 -in ${REMOTE_CERT_DIR}/intracluster-ca.crt 2>/dev/null | cut -d= -f2" 2>/dev/null)

    if [ -z "$remote" ]; then
        printf "${YELLOW}SKIP${RESET}    %s unreachable — cannot compare deployed CA\n" "$HOST"
        continue
    fi

    remote_client_ca=$(sed -n 1p <<<"$remote")
    remote_intra_ca=$(sed -n 2p <<<"$remote")

    if [ "$local_client_ca" != "$remote_client_ca" ]; then
        printf "${RED}MISMATCH${RESET} %s client-ca differs from local — the node will reject this client cert.\n" "$HOST"
        printf "         local  %s\n         node   %s\n" "$local_client_ca" "$remote_client_ca"
        printf "         Run 'make certs' to regenerate and redistribute.\n"
        fail=1
    elif [ "$local_intra_ca" != "$remote_intra_ca" ]; then
        printf "${RED}MISMATCH${RESET} %s intracluster-ca differs from local — replication will fail.\n" "$HOST"
        printf "         Run 'make certs' to regenerate and redistribute.\n"
        fail=1
    else
        printf "${GREEN}ok${RESET}      %s trusts this PKI\n" "$HOST"
    fi
done

if [ "$fail" -ne 0 ]; then
    printf "\n${RED}${BOLD}PKI check failed.${RESET}\n"
    exit 1
fi
printf "\n${GREEN}${BOLD}PKI ok.${RESET}\n"
