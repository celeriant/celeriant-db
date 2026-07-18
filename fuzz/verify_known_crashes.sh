#!/usr/bin/env bash
# Replay every reproducer in known_crashes/ against its built fuzz target.
#
# Each input here is a KNOWN-OPEN decode-path crash. This asserts they still
# reproduce, so a silent behaviour change is loud: if one stops crashing, either
# the underlying bug was fixed (move that input out of known_crashes/) or
# something is masking it. Seconds to run, unlike a fresh fuzzing campaign that
# has almost no chance of reconstructing these CRC-gated inputs from scratch.
#
# Exit 0 iff every reproducer still crashes. Non-zero if any exited clean.
# Needs the instrumented binaries: `cargo afl build --release` first.
set -u
cd "$(dirname "$0")"

targets=$(ls target/release/fuzz_* 2>/dev/null | xargs -rn1 basename | sed 's/^fuzz_//')
if [ -z "$targets" ]; then
  echo "no target binaries in target/release/ — run: cargo afl build --release" >&2
  exit 2
fi

fixed=0 crashing=0
for f in known_crashes/*.bin; do
  [ -e "$f" ] || { echo "no reproducers in known_crashes/"; exit 2; }
  base=$(basename "$f" .bin)
  # Longest target name that prefixes the filename wins (metablock_bytes_empty
  # -> metablock_bytes, multi_block_segment_scan_afl_found_* -> multi_block_segment_scan).
  best=""
  for t in $targets; do
    case "$base" in "$t"*) [ ${#t} -gt ${#best} ] && best="$t";; esac
  done
  if [ -z "$best" ]; then printf "%-58s NO MATCHING TARGET\n" "$base"; continue; fi

  timeout 10 "target/release/fuzz_$best" < "$f" >/dev/null 2>&1
  rc=$?
  case $rc in
    134|139) printf "%-58s still crashes (sig %d)\n" "$base" "$rc"; crashing=$((crashing+1));;
    124)     printf "%-58s TIMEOUT (>10s)\n" "$base"; crashing=$((crashing+1));;
    0)       printf "%-58s NO LONGER CRASHES — fixed? investigate\n" "$base"; fixed=$((fixed+1));;
    *)       printf "%-58s exit %d\n" "$base" "$rc"; crashing=$((crashing+1));;
  esac
done

echo "---"
echo "$crashing still reproduce, $fixed no longer crash"
[ "$fixed" -eq 0 ]
