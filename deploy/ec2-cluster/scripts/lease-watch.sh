#!/bin/bash
# Sample the data nodes' lease/renewal counters from a CLIENT node, for the duration of a run.
#
# Why not just scrape from the laptop at the end: the failure under investigation wedges the
# data node so hard that sshd cannot complete a banner exchange, so there is no "afterwards".
# The client node stays reachable, is on the same VPC, and can reach :9090 on the private IPs.
# Sampling continuously from there is the only way the evidence survives the wedge.
#
# Usage:
#   lease-watch.sh start     # launch the sampler on client #1 (detached, survives logout)
#   lease-watch.sh stop      # stop it
#   lease-watch.sh collect   # copy the samples back to results/
#
# Output: CSV `unix_ms,node,metric_with_labels,value` — one row per counter per sample.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CDK_DIR="$(dirname "$SCRIPT_DIR")"
CLUSTER_ENV="$CDK_DIR/.cluster-env"

[[ -f "$CLUSTER_ENV" ]] || { echo "ERROR: $CLUSTER_ENV not found — run 'make deploy' or 'make sync-env'"; exit 1; }
source "$CLUSTER_ENV"

SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10"
[[ -n "${KEY_FILE:-}" ]] && SSH_OPTS="$SSH_OPTS -i $KEY_FILE"

CLIENT="${CLIENT_PUBS%%,*}"
[[ -n "$CLIENT" ]] || { echo "ERROR: no client IP in $CLUSTER_ENV"; exit 1; }

REMOTE_OUT=/tmp/lease-watch.csv
REMOTE_PID=/tmp/lease-watch.pid
INTERVAL="${LEASE_WATCH_INTERVAL:-2}"

# The three counters that decide the root cause, plus the surrounding context needed to
# tell a fix from a coincidence. Anchored with ^ so a label value cannot match by accident.
PATTERN='^celeriant_(s3_fallback_lease_unconfirmed_total|s3_lease_renewal_requested_total|s3_lease_renewal_handled_total|s3_lease_on_demand_renewal_total|s3_lease_superseded_total|replication_s3_fallbacks_total|replication_spin_(retry|timeout|fenced|terminal)_total|intrashard_dequeued_total|node_role)'

case "${1:-}" in
  start)
    # -m 3 so a wedged node times out fast instead of stalling the sample loop; a missing
    # sample is itself evidence, a stalled sampler is not.
    remote_script=$(cat <<REMOTE
set -u
: > $REMOTE_OUT
while true; do
  now=\$(date +%s%3N)
  for pair in "leader ${LEADER_IP}" "follower ${FOLLOWER_IP}"; do
    set -- \$pair
    node=\$1; ip=\$2
    [ -n "\$ip" ] || continue
    if body=\$(curl -s -m 3 "http://\$ip:9090/metrics" 2>/dev/null); then
      echo "\$body" | grep -E '$PATTERN' | grep -v '^#' \
        | while read -r metric value; do echo "\$now,\$node,\$metric,\$value"; done
    else
      echo "\$now,\$node,SCRAPE_FAILED,1"
    fi
  done >> $REMOTE_OUT
  sleep $INTERVAL
done
REMOTE
    )
    # shellcheck disable=SC2086
    ssh $SSH_OPTS ec2-user@"$CLIENT" "cat > /tmp/lease-watch-inner.sh" <<< "$remote_script"
    # shellcheck disable=SC2086
    ssh $SSH_OPTS ec2-user@"$CLIENT" \
      "nohup setsid bash /tmp/lease-watch-inner.sh >/dev/null 2>&1 & echo \$! > $REMOTE_PID; sleep 1; cat $REMOTE_PID"
    echo "Sampler started on $CLIENT every ${INTERVAL}s → $REMOTE_OUT"
    ;;

  stop)
    # shellcheck disable=SC2086
    ssh $SSH_OPTS ec2-user@"$CLIENT" \
      "[ -f $REMOTE_PID ] && kill -- -\$(cat $REMOTE_PID) 2>/dev/null; pkill -f lease-watch-inner.sh; rm -f $REMOTE_PID; true"
    echo "Sampler stopped."
    ;;

  collect)
    STAMP="$(date +%Y%m%d-%H%M%S)"
    DEST="$CDK_DIR/results/lease-watch-$STAMP.csv"
    mkdir -p "$CDK_DIR/results"
    # shellcheck disable=SC2086
    scp $SSH_OPTS ec2-user@"$CLIENT":$REMOTE_OUT "$DEST"
    echo "Collected → $DEST ($(wc -l < "$DEST") rows)"
    echo
    echo "Final value of each counter:"
    awk -F, '$3 != "SCRAPE_FAILED" { last[$2","$3] = $4 } END { for (k in last) printf "  %s = %s\n", k, last[k] }' \
      "$DEST" | sort
    echo
    echo "Scrape failures (node unreachable — the wedge itself):"
    awk -F, '$3 == "SCRAPE_FAILED" { n[$2]++ } END { for (k in n) printf "  %s: %d samples\n", k, n[k] }' "$DEST"
    ;;

  *)
    echo "usage: $0 {start|stop|collect}"; exit 1;;
esac
