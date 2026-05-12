#!/usr/bin/env bash
# Idempotent systemd unit + promtail config refresh for a Celeriant data node.
# Pulled out of setup-nodes.sh so `make deploy` can re-sync after a config.env
# change (e.g. flipping INFRA_MODE) without redoing OS prep.
#
# Usage: update-service.sh <hostname> <infra_hostname> <memory_pct> <log_prealloc> <reserve_shard>
set -euo pipefail

HOST="$1"
INFRA_HOST="$2"
MEMORY_CONSUMPTION_PERCENT="$3"
SHARD_LOG_PREALLOCATE_BYTES="$4"
RESERVE_COORDINATOR_SHARD="$5"

ADVERTISED_HOST="$HOST"

printf "\n=== Updating systemd unit on %s (S3 endpoint: %s) ===\n" "$HOST" "$INFRA_HOST"

cat > /tmp/celeriant-"$HOST".service <<EOF
[Unit]
Description=Celeriant Database
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=/usr/local/bin/celeriant
Restart=on-failure
RestartSec=3
LimitNOFILE=1048576
LimitMEMLOCK=infinity

Environment=CELERIANT_DATA_ROOT=/var/lib/celeriant
Environment=CELERIANT_LISTEN_ADDRESS=0.0.0.0
Environment=CELERIANT_CLIENT_PORT=10000
Environment=CELERIANT_REPLICATION_PORT=10001
Environment=CELERIANT_LOG_LEVEL=info
Environment=CELERIANT_METRICS_ENABLED=true
Environment=CELERIANT_METRICS_PORT=9090

Environment=CELERIANT_ADVERTISED_CLIENT_ADDRESS=${ADVERTISED_HOST}:10000
Environment=CELERIANT_ADVERTISED_REPLICATION_ADDRESS=${ADVERTISED_HOST}:10001

Environment=CELERIANT_S3_ENABLED=true
Environment=CELERIANT_S3_REGION=us-east-1
Environment=CELERIANT_S3_BUCKET=celeriant-cluster
Environment=CELERIANT_S3_ACCESS_KEY_ID=minioadmin
Environment=CELERIANT_S3_SECRET_ACCESS_KEY=minioadmin
Environment=CELERIANT_S3_ENDPOINT_OVERRIDE=http://${INFRA_HOST}:9000
Environment=CELERIANT_S3_ALLOW_HTTP=true

Environment=CELERIANT_TLS_MODE=strict
Environment=CELERIANT_TLS_CA_CERT=/etc/celeriant/certs/client-ca.crt
Environment=CELERIANT_TLS_INTRACLUSTER_CA_CERT=/etc/celeriant/certs/intracluster-ca.crt
Environment=CELERIANT_TLS_NODE_CERT=/etc/celeriant/certs/node.crt
Environment=CELERIANT_TLS_NODE_KEY=/etc/celeriant/certs/node.key
Environment=CELERIANT_TLS_CLIENT_CERT=/etc/celeriant/certs/client-server.crt
Environment=CELERIANT_TLS_CLIENT_KEY=/etc/celeriant/certs/client-server.key
Environment=CELERIANT_TLS_CLIENT_AUTH=require

Environment=CELERIANT_MEMORY_CONSUMPTION_PERCENT=${MEMORY_CONSUMPTION_PERCENT}
Environment=CELERIANT_SHARD_LOG_PREALLOCATE_BYTES=${SHARD_LOG_PREALLOCATE_BYTES}
Environment=CELERIANT_RESERVE_COORDINATOR_SHARD=${RESERVE_COORDINATOR_SHARD}

# NIC tuning for RPi + MinIO. The default concurrent upload count
# saturates the Pi's 1GbE NIC + MinIO write path during S3 fallback
# storms, causing partition_leader_minio to drop from ~23k req/s to
# ~79 req/s (see status-log.md). Throttling uploads to 1-at-a-time
# plus a 1ms inter-upload delay sits just below the saturation
# threshold. Re-tune when moving to hardware with faster networking
# or a dedicated S3 endpoint.
Environment=CELERIANT_S3_MAX_CONCURRENT_FALLBACK_UPLOADS=8
Environment=CELERIANT_S3_REPLICATION_DELAY_US=1000000

[Install]
WantedBy=multi-user.target
EOF

scp /tmp/celeriant-"$HOST".service "$HOST":/tmp/celeriant.service
ssh "$HOST" 'sudo mv /tmp/celeriant.service /etc/systemd/system/ && sudo systemctl daemon-reload && sudo systemctl enable celeriant'
rm -f /tmp/celeriant-"$HOST".service

printf "\n=== Updating promtail config on %s (loki: %s) ===\n" "$HOST" "$INFRA_HOST"

cat > /tmp/promtail-"$HOST".yml <<EOF
server:
  http_listen_port: 9080
  grpc_listen_port: 0

positions:
  filename: /tmp/positions.yaml

clients:
  - url: http://${INFRA_HOST}:3100/loki/api/v1/push

scrape_configs:
  - job_name: journal
    journal:
      json: false
      max_age: 12h
      labels:
        job: celeriant
        node: ${HOST}
    relabel_configs:
      - source_labels: ['__journal__systemd_unit']
        target_label: unit
      - source_labels: ['__journal__systemd_unit']
        regex: celeriant.service
        action: keep
EOF

scp /tmp/promtail-"$HOST".yml "$HOST":/tmp/promtail.yml
ssh "$HOST" 'sudo mv /tmp/promtail.yml /etc/promtail/config.yml && sudo systemctl restart promtail 2>/dev/null || true'
rm -f /tmp/promtail-"$HOST".yml

printf "=== %s service refresh complete ===\n" "$HOST"
