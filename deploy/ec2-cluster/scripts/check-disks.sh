#!/bin/bash
# Pre-flight: is every NVMe in this cluster actually pulling its weight?
#
# On instance store you get whichever physical drives the host happens to have, with no SLA on
# their condition. A single slow member of a RAID0 stripe throttles the whole array, and nothing
# in the deploy path notices — the benchmark just reports that host's number as if it were the
# instance type's number.
#
# This is not hypothetical. On 2026-08-16 an i4i.8xlarge leader ran one stripe member at 79%
# util against 14.6% on the other under identical load. That cluster was also the only one of
# three that day to show ~10% run-to-run spread and a bistable CPU/batch regime, and it produced
# a 24k figure 12% below what two later clusters measured.
#
# METHOD, and why it is not the obvious one. The direct test — fio against each raw member — is
# NOT safe here: the members are assembled into an active /dev/md0, so writing to one corrupts
# the array. Reading raw is safe but near-useless on a fresh instance store, where never-written
# LBAs are served from the controller without touching NAND.
#
# So the load goes through the filesystem (XFS on md0), where RAID0 spreads it across members by
# construction, and we watch what each member does with its equal share:
#
#   equal w/s, equal await   -> healthy stripe
#   equal w/s, one high await-> that drive is slow. This is the case worth catching.
#   unequal w/s              -> not a drive fault; the array is not striping evenly
#
# %util is deliberately NOT the signal. It assumes a single-queue device ("fraction of time at
# least one request was in flight") and saturates on multi-queue NVMe — the 2026-08-16 capture
# reported 503%, which is not a value a utilisation can take. await is a real per-I/O latency.
#
# Run AFTER `make deploy` and BEFORE `make start`: celeriant's own I/O would confound the
# measurement, and the scratch file needs the data root to itself.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CDK_DIR="$(dirname "$SCRIPT_DIR")"
CLUSTER_ENV="$CDK_DIR/.cluster-env"

[[ -f "$CLUSTER_ENV" ]] || { echo "ERROR: $CLUSTER_ENV not found — run 'make deploy' or 'make sync-env' first" >&2; exit 1; }
source "$CLUSTER_ENV"

SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
[[ -n "${KEY_FILE:-}" ]] && SSH_OPTS="$SSH_OPTS -i $KEY_FILE"

DURATION="${CHECK_DISKS_SECS:-20}"
# Slowest-to-fastest member await ratio that trips a warning.
#
# Calibrated, not guessed, and the baseline is NOT the same on every box:
#
#   i4i.8xlarge  (2 drives)  2.30-3.00x across four healthy nodes, await 0.115-0.346 ms
#   i4i.16xlarge (4 drives)  1.04-1.05x across two healthy nodes, await 0.082-0.088 ms
#
# Both measured 2026-08-16, w/s equal across members to within a few parts in 100,000 in every
# case — so the stripe splits load perfectly and only latency differs. The 8xlarge pushes
# ~211k w/s per drive against the 16xlarge's ~119k, which points at the asymmetry being a
# SATURATION effect that shows up when two drives carry what four would share, rather than a
# defect. Do not read 3x on an 8xlarge as a bad drive.
#
# 4.0 sits above the worst healthy case seen (3.00x) with room for jitter. It is deliberately
# loose: a tighter threshold false-positives on every 8xlarge. Re-calibrate per instance type —
# on the 16xlarge, where healthy is 1.05x, 4.0 is far too permissive to catch a real outlier.
THRESHOLD="${CHECK_DISKS_AWAIT_RATIO:-4.0}"
STRICT="${CHECK_DISKS_STRICT:-0}"

DATA_NODE_PUBS="$LEADER_PUB"
[[ -n "${FOLLOWER_PUB:-}" ]] && DATA_NODE_PUBS="$LEADER_PUB $FOLLOWER_PUB"

RESULT_DIR="$CDK_DIR/results"; mkdir -p "$RESULT_DIR"
STAMP=$(date +%Y%m%d-%H%M%S)
REPORT="$RESULT_DIR/${STAMP}_disk-check.txt"

# Tee the whole run to the report via exec, NOT by piping the loop — a pipeline would put the
# loop in a subshell and WARNED would never escape it, silently disabling the strict exit.
exec > >(tee "$REPORT") 2>&1

echo "==> Disk pre-flight: ${DURATION}s direct random write per data node, per-member await"
echo "    warn when slowest member await exceeds ${THRESHOLD}x the fastest member"
echo ""

WARNED=0

for HOST in $DATA_NODE_PUBS; do
  echo "--- $HOST"

  if ssh $SSH_OPTS ec2-user@"$HOST" 'systemctl is-active --quiet celeriant' 2>/dev/null; then
    echo "    SKIP: celeriant is running — its I/O would confound this. Run before 'make start'."
    continue
  fi

  # fio and sysstat are not in the base AMI on every generation.
  ssh $SSH_OPTS ec2-user@"$HOST" \
    'command -v fio >/dev/null 2>&1 || sudo dnf install -y fio >/dev/null 2>&1 || true
     command -v iostat >/dev/null 2>&1 || sudo dnf install -y sysstat >/dev/null 2>&1 || true' >/dev/null 2>&1 || true

  if ! ssh $SSH_OPTS ec2-user@"$HOST" 'command -v fio >/dev/null 2>&1'; then
    echo "    SKIP: fio unavailable and could not be installed"
    continue
  fi

  # The scratch file lives on the data root so it lands on the array under test. Removed on
  # every exit path, including failure, so a tripped check never leaves the mount dirty.
  OUT=$(ssh $SSH_OPTS ec2-user@"$HOST" "
    set -e
    MNT=/var/lib/celeriant
    SCRATCH=\$MNT/.disk-check.\$\$
    cleanup() { sudo rm -f \"\$SCRATCH\"; }
    trap cleanup EXIT

    stdbuf -oL iostat -xdy 1 > /tmp/disk-check-iostat.txt 2>/dev/null &
    IOPID=\$!
    sudo fio --name=precheck --filename=\"\$SCRATCH\" --rw=randwrite --bs=4k --direct=1 \
        --ioengine=libaio --iodepth=32 --numjobs=4 --group_reporting \
        --size=2G --runtime=${DURATION} --time_based --output-format=terse >/dev/null 2>&1
    kill \$IOPID 2>/dev/null || true

    # Average w/s and await per member across the captured samples. Column layout differs by
    # sysstat version, so locate w/s and w_await by header name rather than by position.
    awk '
      /^Device/ { for (i=1;i<=NF;i++) { if (\$i==\"w/s\") wcol=i; if (\$i==\"w_await\") acol=i } next }
      /^nvme/ && wcol && acol { n[\$1]++; w[\$1]+=\$wcol; a[\$1]+=\$acol }
      END { for (d in n) printf \"%s %.1f %.3f\\n\", d, w[d]/n[d], a[d]/n[d] }
    ' /tmp/disk-check-iostat.txt | sort
  " 2>/dev/null || true)

  if [[ -z "$OUT" ]]; then
    echo "    SKIP: no per-device samples captured"
    continue
  fi

  # Only members carrying real write load are comparable. The filter is RELATIVE to the busiest
  # device, not a fixed floor: the root EBS volume idles at ~13 w/s against members doing
  # ~211,000, and a fixed '> 10' let it into the comparison as if it were a stripe member.
  ACTIVE=$(echo "$OUT" | awk 'NR==FNR { if ($2 > mx) mx=$2; next } $2 >= mx*0.1' <(echo "$OUT") -)
  COUNT=$(echo "$ACTIVE" | grep -c . || true)

  printf "    %-10s %10s %12s\n" "device" "w/s" "w_await ms"
  echo "$ACTIVE" | while read -r d w a; do printf "    %-10s %10.1f %12.3f\n" "$d" "$w" "$a"; done

  if (( COUNT < 2 )); then
    echo "    (single active device — nothing to compare)"
    echo ""
    continue
  fi

  # Slowest against FASTEST, not against the median. With a two-member stripe the median is the
  # midpoint of the pair, so max/median is bounded by 2.0 and cannot express a lopsided pair at
  # all — the first version of this check measured a 2.9x spread and reported 1.49, under its
  # own threshold. max/min is the ratio that means what it says at any member count.
  VERDICT=$(echo "$ACTIVE" | awk -v thr="$THRESHOLD" '
    NR==1 { lo=$3; hi=$3; slow=$1 }
    { if ($3 < lo) lo=$3; if ($3 > hi) { hi=$3; slow=$1 } }
    END {
      if (lo <= 0) { print "OK 0 0 none"; exit }
      ratio = hi/lo
      printf "%s %.2f %.3f %s\n", (ratio > thr ? "WARN" : "OK"), ratio, lo, slow
    }')

  read -r STATUS RATIO FASTEST SLOWEST <<<"$VERDICT"
  if [[ "$STATUS" == "WARN" ]]; then
    echo "    WARN: $SLOWEST await is ${RATIO}x the fastest member (${FASTEST} ms) — suspect drive on this host"
    echo "          Replace the instance before trusting any number measured on it."
    WARNED=1
  else
    echo "    OK: slowest member is ${RATIO}x the fastest (${FASTEST} ms)"
  fi
  echo ""
done

echo "==> Report: $REPORT"

if (( WARNED )) && [[ "$STRICT" == "1" ]]; then
  exit 1
fi
exit 0
