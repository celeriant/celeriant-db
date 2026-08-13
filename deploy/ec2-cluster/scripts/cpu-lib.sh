#!/bin/bash
# Per-CORE CPU capture on the data nodes for the duration of a benchmark.
# Sourced by run-benchmark.sh and the cell sweep driver.
#
# Why per core and not per thread. The prior campaign read per-thread CPU time from
# /proc/<pid>/task/*/stat and concluded that halved per-shard skew (99% -> 52%) explained a
# throughput win. Two reactors sharing one hardware thread cannot each exceed ~50% by
# arithmetic, so 52% was the saturation ceiling, not evidence of anything. Once reactors
# outnumber hardware threads, per-thread CPU stops being a saturation signal. This samples
# /proc/stat instead, which is per hardware thread from the kernel's own accounting.
#
# Why iowait is excluded from busy. A reactor parked in the fsync/replication amortisation
# window is accounted as iowait, not idle. Counting iowait as busy reports a saturated box
# that is actually waiting on a timer: at 4 shards under light load the cores read 0% idle
# but only 47% busy. So busy = total - idle - iowait, and iowait is reported separately
# because the gap between them is the amortisation window's cost.
#
# Expects from the caller (at call time, not source time): SSH_OPTS, LEADER_PUB,
# FOLLOWER_PUB — safe to source before .cluster-env is loaded.

CPU_PIDS=()

# Stream /proc/stat snapshots at 1s intervals from each data node.
cpu_start() {
  local prefix="$1"
  CPU_PIDS=()
  for HOST in $LEADER_PUB $FOLLOWER_PUB; do
    ssh $SSH_OPTS ec2-user@"$HOST" \
      'while :; do grep "^cpu" /proc/stat; echo "--- $(date +%s)"; sleep 1; done' \
      > "${prefix}_${HOST}.txt" 2>/dev/null &
    CPU_PIDS+=("$!")
  done
}

# Stop capture and summarise. Busy is computed from the delta between the FIRST and LAST
# snapshot, i.e. averaged across the whole benchmark window, rather than per-second samples
# that would be dominated by scheduling jitter.
cpu_stop() {
  local prefix="$1"
  for pid in "${CPU_PIDS[@]:-}"; do kill "$pid" 2>/dev/null || true; done
  for HOST in $LEADER_PUB $FOLLOWER_PUB; do
    ssh $SSH_OPTS ec2-user@"$HOST" "pkill -f 'grep \"^cpu\" /proc/stat' 2>/dev/null || true" >/dev/null 2>&1 || true
  done

  local summary="${prefix}_summary.txt"
  : > "$summary"
  for HOST in $LEADER_PUB $FOLLOWER_PUB; do
    local raw="${prefix}_${HOST}.txt"
    [[ -s "$raw" ]] || continue
    cpu_summarise "$raw" "$HOST" | tee -a "$summary"
  done
}

# Parse a /proc/stat capture into per-core busy/iowait. Split out so it can be unit-tested
# against a fixture without any ssh.
#
# /proc/stat core line: cpuN user nice system idle iowait irq softirq steal guest guest_nice
# Guest time is already included in user/nice, so it is not added again.
cpu_summarise() {
  local raw="$1" host="$2"
  awk -v host="$host" '
    /^cpu[0-9]+ / {
      c = $1
      total = $2 + $3 + $4 + $5 + $6 + $7 + $8 + $9
      idle  = $5
      iowt  = $6
      if (!(c in first_total)) {
        first_total[c] = total; first_idle[c] = idle; first_iowt[c] = iowt
      }
      last_total[c] = total; last_idle[c] = idle; last_iowt[c] = iowt
    }
    END {
      n = 0; sum = 0; mx = -1; mn = 1e9; hot = 0; iosum = 0
      for (c in first_total) {
        dt = last_total[c] - first_total[c]
        if (dt <= 0) continue
        di = last_idle[c] - first_idle[c]
        dw = last_iowt[c] - first_iowt[c]
        busy = 100.0 * (dt - di - dw) / dt
        iow  = 100.0 * dw / dt
        n++; sum += busy; iosum += iow
        if (busy > mx) { mx = busy; mxc = c }
        if (busy < mn) { mn = busy; mnc = c }
        if (busy >= 90.0) hot++
      }
      if (n == 0) { printf "  %-16s (no usable samples)\n", host; exit }
      printf "  %-16s cores %d  busy mean %5.1f%%  max %5.1f%% (%s)  min %5.1f%% (%s)  >=90%%: %d/%d  iowait mean %5.1f%%\n",
             host, n, sum/n, mx, mxc, mn, mnc, hot, n, iosum/n
    }
  ' "$raw"
}
