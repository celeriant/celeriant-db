#!/bin/bash
# Drives celeriant_bench simultaneously from N independent client machines (default:
# cs1 + cs2) against a target Celeriant cluster, then merges results into joint
# throughput + percentiles via merge-results.sh.
#
# Usage:
#   run.sh <address1> <address2> <tasks-per-machine> <duration-secs> [--plaintext]
#
# Example (standalone plaintext smoke test):
#   run.sh 192.168.88.252:10000 192.168.88.252:10000 50 5 --plaintext
#
# Example (real mTLS leader/follower run, 12000 tasks/machine x 2 machines = 24000 total):
#   Override the client set with BENCH_CLIENTS="ip1 ip2 ..." if needed.
#   run.sh $LEADER:10000 $FOLLOWER:10000 12000 60
set -euo pipefail

ADDR1="${1:?address1 required}"
ADDR2="${2:?address2 required}"
TASKS="${3:?tasks-per-machine required}"
DURATION="${4:?duration-secs required}"
# Everything after duration is forwarded verbatim to celeriant-bench on each client, so
# --plaintext / --workload / --schema / --occ need no plumbing here.
shift 4
EXTRA_ARGS=("$@")

# cs1, cs2. cs3 (192.168.88.78) is deliberately excluded: it also runs frigate/
# homeassistant and sits at load ~4 on 4 cores, so it under-generates and skews
# the merged percentiles. Override with BENCH_CLIENTS="ip1 ip2 ..." if needed.
read -r -a CLIENTS <<< "${BENCH_CLIENTS:-192.168.88.214 192.168.88.213}"
REMOTE_BIN="~/celeriant-bench-bin"
RESULTS_DIR="$(mktemp -d)"
trap 'rm -rf "$RESULTS_DIR"' EXIT

# Clear the previous run's artefacts BEFORE launching. The completion poll below waits for
# ~/bench_result.summary to exist; a stale file left by an earlier run makes that poll return
# immediately and merge the PREVIOUS run's numbers as if they were new. That fails silently —
# the output looks like a plausible result — which is fatal for the N=5 repeated-run rigor.
echo "Clearing previous results on ${#CLIENTS[@]} clients: ${CLIENTS[*]}"
for ip in "${CLIENTS[@]}"; do
  ssh "$ip" 'rm -f ~/bench_result.latencies ~/bench_result.summary ~/bench_run.log'
done

echo "Launching on ${#CLIENTS[@]} clients: ${CLIENTS[*]}"
# Each client gets a disjoint aggregate range. Task ids restart at 0 on every machine, so
# without this both clients drive the SAME aggregates. That is harmless for plain appends
# but makes every OCC write collide with the other machine's, which would price
# expected_version as a conflict storm rather than as the comparison it actually is.
#
# Only passed for --workload runs. The baseline path ignores the offset anyway, and older
# celeriant-bench binaries reject the unknown flag outright — so adding it unconditionally
# would break every plain run against a client that has not been redeployed yet.
for i in "${!CLIENTS[@]}"; do
  ip="${CLIENTS[$i]}"
  OFFSET_ARG=""
  case " ${EXTRA_ARGS[*]} " in
    *" --workload "*) OFFSET_ARG="--aggregate-offset $(( i * TASKS ))" ;;
  esac
  ssh "$ip" "ulimit -n 60000 2>/dev/null || true; BENCH_RAW_DUMP_PREFIX=~/bench_result $REMOTE_BIN --address1 $ADDR1 --address2 $ADDR2 --tasks $TASKS --duration $DURATION $OFFSET_ARG ${EXTRA_ARGS[*]} > ~/bench_run.log 2>&1 < /dev/null &" &
done
wait

# Poll until every client's summary file exists (each client runs ~DURATION secs +
# connection setup; a fixed sleep is fragile if setup takes longer under heavier load).
echo "Waiting for all clients to finish (duration=${DURATION}s + connection setup)..."
DEADLINE=$((SECONDS + DURATION + 120))
for ip in "${CLIENTS[@]}"; do
  until ssh "$ip" 'test -f ~/bench_result.summary' 2>/dev/null; do
    if [ "$SECONDS" -gt "$DEADLINE" ]; then
      echo "TIMEOUT waiting on $ip — pulling logs for diagnosis"
      ssh "$ip" 'cat ~/bench_run.log' || true
      break
    fi
    sleep 2
  done
done

for ip in "${CLIENTS[@]}"; do
  scp -q "$ip:~/bench_result.latencies" "$RESULTS_DIR/${ip}.latencies" || echo "WARN: no latencies file from $ip"
  scp -q "$ip:~/bench_result.summary" "$RESULTS_DIR/${ip}.summary" || echo "WARN: no summary file from $ip"
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
"$SCRIPT_DIR/merge-results.sh" "$RESULTS_DIR"
