#!/bin/bash
# Per-device disk capture on the data nodes for the duration of a benchmark.
# Sourced by run-benchmark.sh and run-sweep.sh.
#
# Each data node streams `iostat -x` back over a live ssh connection for the
# whole run; killing that ssh at the end stops the capture (no fragile remote
# backgrounding). Answers "are we disk-bound, and is the second NVMe idle?" — a
# single drive pinned near 100% %util means RAID0 (-c raid0=true) would help; a
# low %util means the ceiling is elsewhere (replication/network/fsync).
#
# Expects from the caller (at call time, not source time): SSH_OPTS, LEADER_PUB,
# FOLLOWER_PUB — so this is safe to source before .cluster-env is loaded.

IOSTAT_PIDS=()

# Stream a 1s-interval capture from each data node into <prefix>_<host>.txt.
# sysstat is baked into the AMI; install on demand for older clusters.
iostat_start() {
  local prefix="$1"
  IOSTAT_PIDS=()
  for HOST in $LEADER_PUB $FOLLOWER_PUB; do
    ssh $SSH_OPTS ec2-user@"$HOST" 'command -v iostat >/dev/null 2>&1 || sudo dnf install -y sysstat >/dev/null 2>&1 || true' >/dev/null 2>&1 || true
    ssh $SSH_OPTS ec2-user@"$HOST" 'stdbuf -oL iostat -xtdy 1' > "${prefix}_${HOST}.txt" 2>/dev/null &
    IOSTAT_PIDS+=("$!")
  done
}

# Stop the streaming sessions and print/save a per-device %util summary (avg/max
# over the captured samples) for each nvme device.
iostat_stop() {
  local prefix="$1"
  for pid in "${IOSTAT_PIDS[@]:-}"; do kill "$pid" 2>/dev/null || true; done
  for HOST in $LEADER_PUB $FOLLOWER_PUB; do
    ssh $SSH_OPTS ec2-user@"$HOST" "pkill -f 'iostat -xtdy' 2>/dev/null || true" >/dev/null 2>&1 || true
  done
  local summary="${prefix}_summary.txt"
  : > "$summary"
  for HOST in $LEADER_PUB $FOLLOWER_PUB; do
    local raw="${prefix}_${HOST}.txt"
    [[ -s "$raw" ]] || continue
    # %util is the last column of `iostat -x`; report avg/max per nvme device.
    awk -v host="$HOST" '
      /^nvme/ { n[$1]++; s[$1]+=$NF; if ($NF>m[$1]) m[$1]=$NF }
      END { for (d in n) printf "  %-16s %-9s util avg %5.1f%%  max %5.1f%%  (%d samples)\n",
              host, d, s[d]/n[d], m[d], n[d] }
    ' "$raw" | sort | tee -a "$summary"
  done
}
