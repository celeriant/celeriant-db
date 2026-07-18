#!/usr/bin/env bash
# Bounded first-pass AFL campaigns. AFL++ -V exits cleanly at the time limit (no orphaned child).
# Clean targets get 30 min (a crash = new bug); already-crashing targets get 5 min (catalog distinct paths).
set -u
cd "$(dirname "$0")"
# out/ is gitignored, so a fresh checkout won't have it; the per-target log
# redirect below fails silently (killing the campaign) if the dir is missing.
mkdir -p out
export AFL_I_DONT_CARE_ABOUT_MISSING_CRASHES=1
export AFL_SKIP_CPUFREQ=1
export AFL_NO_UI=1
export AFL_BENCH_UNTIL_CRASH=0

declare -A DUR=(
  [bincode_decode]=1800
  [wire_header]=1800
  [versioned_block]=1800
  [serialised_datablock]=300
  [sbbf]=300
  [metablock_bytes]=300
)

pids=()
for t in "${!DUR[@]}"; do
  rm -rf "out/$t"
  ( cargo afl fuzz -V "${DUR[$t]}" -i "corpus/$t" -o "out/$t" "target/release/fuzz_$t" \
      >"out/${t}.log" 2>&1 ; echo "$t done rc=$?" >>campaign_status.txt ) &
  pids+=($!)
done
: >campaign_status.txt
echo "launched ${#pids[@]} campaigns: ${pids[*]}"
wait
echo "ALL CAMPAIGNS COMPLETE" >>campaign_status.txt
# Summarize crashes found per target
for t in "${!DUR[@]}"; do
  cd_dir="out/$t/default/crashes"
  n=$(ls -1 "$cd_dir" 2>/dev/null | grep -v README | wc -l)
  echo "$t: $n crash file(s)" >>campaign_status.txt
done
