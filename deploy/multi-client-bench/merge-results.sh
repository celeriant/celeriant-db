#!/bin/bash
# Merges raw latency dumps + summaries from N independent celeriant_bench clients
# (BENCH_RAW_DUMP_PREFIX output) into joint throughput + percentile numbers.
# Usage: merge-results.sh <results-dir>
# Expects <results-dir>/*.latencies and matching *.summary files.
set -euo pipefail
DIR="${1:?usage: merge-results.sh <results-dir>}"

MERGED="$(mktemp)"
trap 'rm -f "$MERGED"' EXIT
cat "$DIR"/*.latencies | sort -n > "$MERGED"

TOTAL_OK=0
MAX_ELAPSED=0
NUM_MACHINES=0
for f in "$DIR"/*.summary; do
  ok=$(grep '^ok=' "$f" | cut -d= -f2)
  elapsed=$(grep '^elapsed_secs=' "$f" | cut -d= -f2)
  TOTAL_OK=$((TOTAL_OK + ok))
  NUM_MACHINES=$((NUM_MACHINES + 1))
  MAX_ELAPSED=$(awk -v a="$MAX_ELAPSED" -v b="$elapsed" 'BEGIN{print (a>b)?a:b}')
done

N=$(wc -l < "$MERGED")
THROUGHPUT=$(awk -v ok="$TOTAL_OK" -v el="$MAX_ELAPSED" 'BEGIN{printf "%.0f", ok/el}')

pct() {
  local p="$1"
  local idx=$(awk -v n="$N" -v p="$p" 'BEGIN{printf "%d", n*p/100}')
  [ "$idx" -ge "$N" ] && idx=$((N - 1))
  sed -n "$((idx + 1))p" "$MERGED"
}

AVG=$(awk '{sum+=$1} END{printf "%.1f", sum/NR}' "$MERGED")
MIN=$(head -1 "$MERGED")
MAX=$(tail -1 "$MERGED")

echo "=== Joint result across $NUM_MACHINES machines ==="
echo "Total requests: $N | Total ok: $TOTAL_OK | Max wall time: ${MAX_ELAPSED}s"
echo "Joint throughput: ${THROUGHPUT} req/s"
echo "Joint latency (ms) — Avg: $AVG | P50: $(pct 50) | P95: $(pct 95) | P99: $(pct 99) | P99.9: $(pct 99.9) | Min: $MIN | Max: $MAX"
