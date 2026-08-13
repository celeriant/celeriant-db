#!/bin/bash
# Drive a sweep of campaign cells against a deployed cluster and emit a CSV.
#
# A "cell" is one combination of campaign knobs (session/goal.md). This pushes each cell's
# env to the data nodes, restarts, warms up, benchmarks, and records the result — without
# re-copying binaries, so a cell costs a restart rather than a full deploy.
#
# Three pieces of methodology are baked in because the prior campaign was burned by all
# three:
#
#   ABBA ordering. Cells run forward, then backward, then forward... (boustrophedon), so
#   every cell's repetitions are symmetric about the midpoint of the run. Any monotone
#   drift — thermal, spot-neighbour, disk fill — cancels to first order instead of being
#   attributed to whichever cell happened to run last. Spike 1 used two monotone passes and
#   is explicitly flagged in goal.md as needing this before anything is published.
#
#   Worse-of-N. Reported throughput is the MINIMUM across repetitions and reported latency
#   the MAXIMUM, never the mean. On i4i, p99 carries a warm-up transient: reps 1-2 read
#   79/92ms and reps 3-5 an identical 51ms. A naive before/after straddling that transient
#   manufactured a 44.6% effect that did not exist.
#
#   Per-core CPU, iowait excluded from busy. See cpu-lib.sh.
#
# Usage:
#   CELLS_FILE=cells/fsync.txt REPS=2 bash scripts/run-cell-sweep.sh
#
# Cell file format — one cell per line, '#' comments and blank lines ignored:
#   <name> <KNOB>=<value> [<KNOB>=<value> ...]
# e.g.
#   fsync_250   FSYNC_DELAY_US=250
#   shards_128  NUM_SHARDS=128 RESERVE_COORDINATOR_SHARD=false

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CDK_DIR="$(dirname "$SCRIPT_DIR")"
CLUSTER_ENV="$CDK_DIR/.cluster-env"

[[ -f "$CLUSTER_ENV" ]] || { echo "ERROR: $CLUSTER_ENV not found — run 'make deploy' first" >&2; exit 1; }
source "$CLUSTER_ENV"
source "$SCRIPT_DIR/node-env-lib.sh"
source "$SCRIPT_DIR/cpu-lib.sh"

CELLS_FILE="${CELLS_FILE:-}"
[[ -f "$CELLS_FILE" ]] || { echo "ERROR: set CELLS_FILE to a cell definition file" >&2; exit 1; }

REPS="${REPS:-2}"
DURATION="${BENCH_DURATION:-30}"
WARMUP="${WARMUP_SECS:-20}"
# Wipe the data root before every cell. Required whenever a cell varies an immutable field
# (num_shards, reserve_coordinator_shard, routing_rule, timestamp_*, compression.level) —
# the server refuses to start against a data root recorded with different values:
#   "num_shards: saved=4, configured=8 ... Immutable configuration does not match"
# It is also the honest default even for mutable cells, since otherwise each cell inherits
# the previous cell's accumulated segments and later cells run against a fuller disk.
WIPE="${WIPE_BETWEEN_CELLS:-true}"
READY_TIMEOUT="${READY_TIMEOUT:-300}"

SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
[[ -n "${KEY_FILE:-}" ]] && SSH_OPTS="$SSH_OPTS -i $KEY_FILE"
SSH="ssh $SSH_OPTS"

CLIENT_PUBS_SP="${CLIENT_PUBS//,/ }"
CLIENT_COUNT="${CLIENT_COUNT:-1}"
DATA_NODE_PUBS="$LEADER_PUB"
[[ -n "${FOLLOWER_PUB:-}" ]] && DATA_NODE_PUBS="$LEADER_PUB $FOLLOWER_PUB"
SEED_IP="${FOLLOWER_IP:-$LEADER_IP}"

# --- Parse cells ---------------------------------------------------------
CELL_NAMES=(); CELL_KNOBS=()
while read -r line; do
  line="${line%%#*}"
  [[ -z "${line// }" ]] && continue
  read -r name rest <<<"$line"
  CELL_NAMES+=("$name")
  CELL_KNOBS+=("$rest")
done < "$CELLS_FILE"
NCELLS=${#CELL_NAMES[@]}
(( NCELLS > 0 )) || { echo "ERROR: no cells parsed from $CELLS_FILE" >&2; exit 1; }

TIMESTAMP=$(date +%Y%m%d-%H%M%S)
RESULT_DIR="$CDK_DIR/results"; mkdir -p "$RESULT_DIR"
TAG=$(basename "$CELLS_FILE" .txt)
CSV="$RESULT_DIR/${TIMESTAMP}_sweep_${TAG}.csv"
LOG="$RESULT_DIR/${TIMESTAMP}_sweep_${TAG}.log"

echo "cell,pass,rep,knobs,tasks,throughput_rps,p50_ms,p95_ms,p99_ms,errors,cpu_busy_mean,cpu_busy_max,iowait_mean" > "$CSV"

echo "==> Cell sweep: $NCELLS cells x $REPS reps (ABBA/boustrophedon order)"
echo "    cells:    $CELLS_FILE"
echo "    cluster:  $INSTANCE_TYPE ($STORAGE_TYPE), ${DATA_NODE_COUNT:-?} data node(s), $CLIENT_COUNT client(s)"
echo "    duration: ${DURATION}s bench after ${WARMUP}s warm-up, wipe-between-cells=$WIPE"
echo "    output:   $CSV"
echo ""

# --- Per-cell execution --------------------------------------------------

apply_cell() {   # $1 = knob assignments, space separated
  # Clear every knob first: a cell that omits a knob must inherit the SERVER default, not
  # the previous cell's value. Leaking state across cells is how a sweep silently measures
  # the wrong thing.
  unset NUM_SHARDS FSYNC_DELAY_US REPLICATION_DELAY_US RESERVE_COORDINATOR_SHARD \
        MESH_CHANNEL_SIZE S3_ENABLED CPU_LIST TASKS
  CELL_TASKS=""
  STANDALONE="${SWEEP_STANDALONE:-}"
  TLS_MODE="${SWEEP_TLS_MODE:-strict}"
  for kv in $1; do export "${kv?}"; done
  # TASKS is a harness knob, not a server env var — pull it out before init_campaign_knobs.
  CELL_TASKS="${TASKS:-}"
  unset TASKS
  init_campaign_knobs
}

# Pin the server to a CPU set for this cell, or clear any previous pin.
#
# taskset via a systemd drop-in, NOT an env var: CELERIANT_DIAG_CPU_LIST was deliberately not
# replayed into this binary and no longer exists, so anything driving it would be silently
# ignored while the run looked fine. External pinning is the only mechanism available.
#
# Reading the results needs the glommio ordering quirk in mind: glommio pops CpuSets LIFO
# while shard ids are handed out FIFO, so shard i lands on cpu_list[n-1-i] — shard 0, which
# carries the coordinator when reserve_coordinator_shard is on, lands on the LAST cpu in the
# list, not the first.
apply_cpu_pin() {
  local list="${CPU_LIST:-}"
  for HOST in $DATA_NODE_PUBS; do
    if [[ -n "$list" ]]; then
      $SSH ec2-user@"$HOST" "sudo mkdir -p /etc/systemd/system/celeriant.service.d && \
        printf '[Service]\nExecStart=\nExecStart=/usr/bin/taskset -c %s /usr/local/bin/celeriant\n' '$list' \
        | sudo tee /etc/systemd/system/celeriant.service.d/pin.conf >/dev/null && \
        sudo systemctl daemon-reload"
    else
      $SSH ec2-user@"$HOST" "sudo rm -f /etc/systemd/system/celeriant.service.d/pin.conf 2>/dev/null; \
        sudo systemctl daemon-reload"
    fi
  done
}


# Total replication events applied across BOTH data nodes.
#
# Which node leads is decided by the S3 lease, NOT by the CDK resource names: a restart
# routinely leaves the instance CDK calls "Leader" running as a Follower and vice versa
# (observed: LEADER_PUB -> `new=Follower { leader_lease_epoch: 2 }` while FOLLOWER_PUB ->
# `new=Leader`). Probing a fixed node therefore reports zero applied events whenever the roles
# happen to be inverted, which looks identical to broken replication. Summing both nodes is
# role-agnostic: only the follower ever applies, so the sum IS the follower's count.
replication_applied_total() {
  local total=0 n
  for HOST in $DATA_NODE_PUBS; do
    n=$($SSH ec2-user@"$HOST" 'curl -s localhost:9090/metrics 2>/dev/null | grep "^celeriant_replication_applied_events_total" | awk "{s+=\$2} END{print s+0}"' 2>/dev/null || echo 0)
    total=$(( total + ${n:-0} ))
  done
  echo "$total"
}

wait_ready() {   # $1 = data node public IP
  local host=$1 waited=0
  while (( waited < READY_TIMEOUT )); do
    if $SSH ec2-user@"$host" "ss -ltn 2>/dev/null | grep -q ':10000 '" 2>/dev/null; then
      return 0
    fi
    sleep 2; waited=$((waited + 2))
  done
  echo "ERROR: $host did not open :10000 within ${READY_TIMEOUT}s" >&2
  $SSH ec2-user@"$host" 'sudo journalctl -u celeriant -n 30 --no-pager' >&2 2>/dev/null || true
  return 1
}

# Restart every data node onto the current cell's env.
#
# Ordering matters on a replicated pair and is NOT a loop of stop-then-start per host. Doing
# it that way takes the leader down while the follower is still up, the follower acquires the
# S3 lease, and when the leader returns BOTH believe they are leader: heartbeats come back
# `Rejected(NotAFollower)` and the cluster wedges at 0 req/s with ~70% iowait. Observed
# directly — it produced six consecutive zero-throughput cells.
#
# So: stop everything first, push env to everything, then start the LEADER ALONE and wait for
# it to be serving before the follower is allowed to join and discover it.
restart_cell() {
  apply_cpu_pin

  for HOST in $DATA_NODE_PUBS; do
    $SSH ec2-user@"$HOST" 'sudo systemctl stop celeriant 2>/dev/null || true'
  done

  for HOST in $DATA_NODE_PUBS; do
    local ip="$LEADER_IP"
    [[ "$HOST" == "${FOLLOWER_PUB:-}" ]] && ip="$FOLLOWER_IP"
    generate_env "$ip" > /tmp/cell.env
    scp $SSH_OPTS /tmp/cell.env ec2-user@"$HOST":/tmp/celeriant.env >/dev/null
    if [[ "$WIPE" == "true" ]]; then
      $SSH ec2-user@"$HOST" 'sudo rm -rf /var/lib/celeriant/* 2>/dev/null || true'
    fi
    $SSH ec2-user@"$HOST" 'sudo mv /tmp/celeriant.env /etc/celeriant/celeriant.env'
  done

  # A wipe discards the nodes' identity while S3 still advertises the old membership, so the
  # stale cluster state has to go with it or the pair cannot re-form.
  if [[ "$WIPE" == "true" && -n "${FOLLOWER_PUB:-}" && -n "${BUCKET:-}" ]]; then
    aws s3 rm "s3://$BUCKET/cluster/" --recursive >/dev/null 2>&1 || true
  fi

  $SSH ec2-user@"$LEADER_PUB" 'sudo systemctl start celeriant'
  wait_ready "$LEADER_PUB" || return 1

  if [[ -n "${FOLLOWER_PUB:-}" ]]; then
    $SSH ec2-user@"$FOLLOWER_PUB" 'sudo systemctl start celeriant'
    wait_ready "$FOLLOWER_PUB" || return 1
    # Settle: the port opens before the pair has finished discovering each other. The warm-up
    # probe below is the real check, but starting traffic into a half-formed cluster is what
    # pushes it into S3 fallback in the first place, so give discovery a head start.
    sleep "${REPLICATION_SETTLE_SECS:-25}"
  fi
}

# Run the benchmark on all clients; echo "throughput p50 p99 errors".
run_bench() {   # $1 = duration, $2 = "warmup" to discard output
  local dur=$1 mode=${2:-measure}
  local tls_env
  if [[ "${TLS_MODE:-strict}" == "disabled" ]]; then
    tls_env="CLUSTER_PLAINTEXT=1"
  else
    tls_env="CLUSTER_CA_CERT=/etc/celeriant/certs/client-ca.crt \
      CLUSTER_CLIENT_CERT=/etc/celeriant/certs/client.crt \
      CLUSTER_CLIENT_KEY=/etc/celeriant/certs/client.key \
      CLUSTER_SERVER_NAME=${LEADER_IP}"
  fi

  # A cell may carry TASKS=<n> to set its own concurrency. Without this a concurrency ladder
  # has to be N separate sweeps, and points from different sweeps cannot be ABBA-ordered
  # against each other — drift between runs would be indistinguishable from a real difference.
  local tasks_total="${CELL_TASKS:-${BENCH_TASKS:-36000}}"
  local tasks_per=$(( tasks_total / CLIENT_COUNT ))

  # Clear previous output BEFORE launching. The aggregate globs /tmp/cell_client_*.txt, so a
  # file left by an earlier run with MORE clients is silently summed into this run's
  # throughput. That is exactly what happened going from a 4-client metal stack to a 3-client
  # gp3 stack: cell_client_4.txt survived and added a stale 418,805 req/s to every cell.
  # Caught by a Little's law check (8,000 tasks / 33 ms p50 cannot be 795,633 req/s), not by
  # anything looking wrong in the output.
  rm -f /tmp/cell_client_*.txt

  local pids=() idx=0
  for HOST in $CLIENT_PUBS_SP; do
    idx=$((idx + 1))
    $SSH ec2-user@"$HOST" \
      "CLUSTER_ADDRESS_1=${LEADER_IP}:10000 CLUSTER_ADDRESS_2=${SEED_IP}:10000 $tls_env \
       CLUSTER_TASKS=$tasks_per CLUSTER_CONNECTIONS=$tasks_per CLUSTER_DURATION=$dur \
       CLUSTER_PINNED_CONNS=${BENCH_PINNED:-1} \
       celeriant-integration-tests --test rpi_cluster_pool_bench" \
      > "/tmp/cell_client_${idx}.txt" 2>&1 &
    pids+=($!)
  done
  for p in "${pids[@]}"; do wait "$p" || true; done
  [[ "$mode" == "warmup" ]] && return 0

  # Sanity guard: the number of result files must equal the client count. A mismatch means
  # a client died or a stale file crept in, and the summed throughput would be wrong.
  # A missing result file means a load generator did not report — usually because spot
  # reclaimed it. This is FATAL to the cell, not a warning: the surviving clients still return
  # zero errors and a tight bracket, so the run looks perfectly healthy while offering a
  # fraction of the intended concurrency. Observed live — losing 2 of 3 clients halved
  # throughput and presented as "server degradation", with the server sitting at 7% CPU and
  # 92% iowait because it was starved rather than slow.
  #
  # Every other guard in this driver watches the SERVER. Nothing watched whether the load
  # generators still existed, which is why that one took a wipe-and-recheck to catch.
  local got; got=$(ls /tmp/cell_client_*.txt 2>/dev/null | wc -l)
  if [[ "$got" -ne "$CLIENT_COUNT" ]]; then
    echo "        ERROR: only $got of $CLIENT_COUNT clients reported — load generators lost." >&2
    echo "               Offered concurrency was not achieved; this is not a measurement." >&2
    echo "$name,$pass,$rep,${knobs//,/;},${CELL_TASKS:-},,,,,CLIENTS_LOST,,," >> "$CSV"
    return 1
  fi

  # Aggregate: throughput SUMS across clients; latency takes the WORST client, since
  # percentiles from separate clients cannot be averaged into a meaningful figure.
  awk '
    /Throughput:/ { for(i=1;i<=NF;i++) if($i=="Throughput:") tp += $(i+1)
                    for(i=1;i<=NF;i++) if($i=="Errors:")     er += $(i+1) }
    /Latency/ { for(i=1;i<=NF;i++) {
                  if($i=="P50:") { v=$(i+1); sub(/ms.*/,"",v); if(v+0>p50) p50=v+0 }
                  if($i=="P95:") { v=$(i+1); sub(/ms.*/,"",v); if(v+0>p95) p95=v+0 }
                  if($i=="P99:") { v=$(i+1); sub(/ms.*/,"",v); if(v+0>p99) p99=v+0 } } }
    END { printf "%.0f %d %d %d %d\n", tp, p50, p95, p99, er }
  ' /tmp/cell_client_*.txt
}

# --- Schedule: boustrophedon ---------------------------------------------
# pass 1 forward, pass 2 backward, ... For two cells this is literally A B B A.
declare -a ORDER_IDX ORDER_PASS
for ((pass = 1; pass <= REPS; pass++)); do
  if (( pass % 2 == 1 )); then
    for ((i = 0; i < NCELLS; i++)); do ORDER_IDX+=("$i"); ORDER_PASS+=("$pass"); done
  else
    for ((i = NCELLS - 1; i >= 0; i--)); do ORDER_IDX+=("$i"); ORDER_PASS+=("$pass"); done
  fi
done

echo "==> Order: $(for k in "${!ORDER_IDX[@]}"; do printf '%s ' "${CELL_NAMES[${ORDER_IDX[$k]}]}"; done)"
echo ""

declare -A REP_COUNT
TOTAL=${#ORDER_IDX[@]}
for k in "${!ORDER_IDX[@]}"; do
  i=${ORDER_IDX[$k]}; pass=${ORDER_PASS[$k]}
  name=${CELL_NAMES[$i]}; knobs=${CELL_KNOBS[$i]}
  REP_COUNT[$name]=$(( ${REP_COUNT[$name]:-0} + 1 ))
  rep=${REP_COUNT[$name]}

  printf "[%2d/%2d] %-16s pass %d rep %d  {%s}\n" "$((k+1))" "$TOTAL" "$name" "$pass" "$rep" "$knobs"
  apply_cell "$knobs"
  if ! restart_cell; then
    echo "        FAILED to start — recording as error row and continuing" | tee -a "$LOG"
    echo "$name,$pass,$rep,${knobs//,/;},${CELL_TASKS:-},,,,,START_FAILED,,," >> "$CSV"
    continue
  fi

  # On a replicated pair, ":10000 listening" is NOT ready. The leader still has to discover the
  # follower and establish replication; writes that land first queue up and spill to the S3
  # fallback path, which wedges throughput at ~0 and fills cluster/fallback/ with batches. The
  # warm-up doubles as the probe: replication must be APPLYING on the follower before any
  # measurement is taken.
  if (( WARMUP > 0 )); then
    printf "        warm-up %ss...\n" "$WARMUP"
    rep_before=0; rep_after=0
    if [[ -n "${FOLLOWER_PUB:-}" ]]; then
      rep_before=$(replication_applied_total)
    fi
    run_bench "$WARMUP" warmup
    if [[ -n "${FOLLOWER_PUB:-}" ]]; then
      rep_after=$($SSH ec2-user@"$FOLLOWER_PUB" 'curl -s localhost:9090/metrics 2>/dev/null | grep "^celeriant_replication_applied_events_total" | awk "{s+=\$2} END{print s+0}"' 2>/dev/null || echo 0)
      if (( rep_after <= rep_before )); then
        echo "        ERROR: follower applied 0 events during warm-up — replication not established." >&2
        echo "               The cell would measure standalone or a wedged cluster, not replication." >&2
        echo "$name,$pass,$rep,${knobs//,/;},${CELL_TASKS:-},,,,,REPLICATION_NOT_ESTABLISHED,,," >> "$CSV"
        continue
      fi
      printf "        replication confirmed: follower applied %s events during warm-up\n" "$((rep_after - rep_before))"
    fi
  fi

  CPU_PREFIX="/tmp/cpu_${name}_${pass}_${rep}"
  cpu_start "$CPU_PREFIX"
  read -r tp p50 p95 p99 errs < <(run_bench "$DURATION")
  cpu_stop "$CPU_PREFIX" > /dev/null 2>&1 || true
  cpustats=$(cpu_summarise "${CPU_PREFIX}_${LEADER_PUB}.txt" "$LEADER_PUB" 2>/dev/null || echo "")
  busy_mean=$(echo "$cpustats" | grep -oP 'busy mean\s+\K[0-9.]+' || echo "")
  busy_max=$(echo "$cpustats"  | grep -oP 'max\s+\K[0-9.]+'       || echo "")
  iow_mean=$(echo "$cpustats"  | grep -oP 'iowait mean\s+\K[0-9.]+' || echo "")

  printf "        %s req/s  p50 %sms  p95 %sms  p99 %sms  errors %s  busy %s%%  iowait %s%%\n" \
         "$tp" "$p50" "$p95" "$p99" "$errs" "${busy_mean:-?}" "${iow_mean:-?}"

  # Errors mean the cell was shedding load, so its throughput is not a throughput figure and
  # its latency percentiles are computed over the requests that happened to survive. Observed
  # on a small ARM pair at 16k connections: 70,694 errors alongside a p99 of 2,501 ms. Loud,
  # because a number like that is publishable-looking and completely invalid.
  if [[ "${errs:-0}" -gt 0 ]]; then
    echo "        WARNING: $errs errors — cell was shedding load; not a valid measurement" >&2
  fi

  # Zero throughput is a wedged cluster, not a measurement. Recording it as data lets a
  # broken run masquerade as a legitimate "this setting is terrible" result — which is
  # exactly how the split-brain restart bug presented before it was diagnosed.
  if [[ "${tp:-0}" -eq 0 ]]; then
    echo "        ERROR: 0 req/s — cluster is not serving. Check for split brain:" >&2
    echo "               journalctl -u celeriant | grep NotAFollower" >&2
    echo "$name,$pass,$rep,${knobs//,/;},${CELL_TASKS:-},,,,,ZERO_THROUGHPUT,,," >> "$CSV"
    continue
  fi
  # Commas inside a knob value (CPU_LIST=0-31,64-95) split the field even when quoted,
  # because awk -F, does not honour quoting — the parser then reads "64" as the throughput
  # and every later column shifts. Store the knobs with semicolons so the row stays 11 fields.
  echo "$name,$pass,$rep,${knobs//,/;},${CELL_TASKS:-${BENCH_TASKS:-}},$tp,$p50,$p95,$p99,$errs,$busy_mean,$busy_max,$iow_mean" >> "$CSV"
done

# --- Worse-of-N summary --------------------------------------------------
echo ""
echo "==> Worse-of-N summary (throughput = MIN across reps, latency = MAX)"
awk -F, 'NR>1 && $6 != "" {
    n[$1]++
    if (!(($1) in mn) || $6+0 < mn[$1]) mn[$1] = $6+0
    if ($6+0 > mx[$1]) mx[$1] = $6+0
    if ($9+0 > p99[$1]) p99[$1] = $9+0
    sum[$1] += $6+0
  }
  END {
    printf "  %-16s %10s %10s %10s %9s %8s\n", "cell", "worst", "best", "mean", "spread", "worstP99"
    for (c in n) {
      sp = (mx[c] > 0) ? 100.0*(mx[c]-mn[c])/mx[c] : 0
      printf "  %-16s %10d %10d %10.0f %8.1f%% %7dms%s\n", c, mn[c], mx[c], sum[c]/n[c], sp, p99[c],
             (sp > 5.0 ? "   <-- bracket >5%, treat as noise-dominated" : "")
    }
  }' "$CSV" | sort

echo ""
echo "==> CSV: $CSV"
echo "    Compare cells on the WORST column. A spread wider than the difference between two"
echo "    cells means the cells are indistinguishable at this rep count — raise REPS."
