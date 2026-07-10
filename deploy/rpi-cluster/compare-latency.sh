#!/bin/sh
# Compare client-observed write latency between two chaos runs (F-P3-1).
#
# Usage: compare-latency.sh <baseline.json> <candidate.json> [band_pct]
#   e.g. compare-latency.sh runs/1751700000/baseline.json runs/1751790000/baseline.json
#
# Reads .bench.p50_ms / .bench.p99_ms from each scenario report and prints
# the candidate's delta vs the baseline against a +/-band_pct band (default 5).
# Exits 1 if either percentile is out of band, 2 on usage/read errors.
# Warns (but still compares) when the two runs used different --tasks.
set -eu

[ $# -ge 2 ] || { echo "usage: $0 <baseline.json> <candidate.json> [band_pct]" >&2; exit 2; }
base=$1
cand=$2
band=${3:-5}

for f in "$base" "$cand"; do
    [ -r "$f" ] || { echo "error: cannot read $f" >&2; exit 2; }
done

get() { jq -e "$2" "$1" 2>/dev/null || { echo "error: $2 missing in $1" >&2; exit 2; }; }

base_tasks=$(get "$base" .params.tasks)
cand_tasks=$(get "$cand" .params.tasks)
if [ "$base_tasks" != "$cand_tasks" ]; then
    echo "WARNING: task counts differ (baseline $base_tasks vs candidate $cand_tasks) — deltas are not comparable" >&2
fi

echo "baseline:  $base (tasks=$base_tasks)"
echo "candidate: $cand (tasks=$cand_tasks)"
echo "band:      +/-${band}%"

fail=0
for pct in p50_ms p99_ms; do
    b=$(get "$base" ".bench.$pct")
    c=$(get "$cand" ".bench.$pct")
    verdict=$(echo "$b $c $band" | awk '{
        b=$1; c=$2; band=$3
        if (b == 0) { d = (c == 0) ? 0 : 999 } else { d = (c - b) * 100.0 / b }
        printf "%+.2f%% %s", d, (d > band || d < -band) ? "OUT-OF-BAND" : "in band"
    }')
    echo "$pct: $b -> $c ($verdict)"
    case "$verdict" in *OUT-OF-BAND*) fail=1 ;; esac
done

exit $fail
