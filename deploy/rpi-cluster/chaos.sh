#!/usr/bin/env bash
#
# Run the chaos harness with its own stdout and stderr captured into the run
# directory it creates. The bench prints one line per failing task to stderr
# and nothing else persists it; a run directory without those lines cannot
# explain its own error count.
#
# Usage: deploy/rpi-cluster/chaos.sh --scenario baseline --tasks 15000
#        deploy/rpi-cluster/chaos.sh --full --tasks 15000
#
# Everything after the script name is passed through to celeriant_chaos.

set -u -o pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
log="$(mktemp "${TMPDIR:-/tmp}/celeriant-chaos-XXXXXX.log")"

# Copy the log into every run directory the harness announced. Runs on EXIT so
# an interrupted or failed run keeps its log too.
stash_log() {
    local dir copied=0
    while IFS= read -r dir; do
        [ -d "$dir" ] || continue
        cp "$log" "$dir/harness.log" && copied=1
        echo "harness log: $dir/harness.log"
    done < <(sed -n 's/^Run directory: //p' "$log" | sort -u)
    [ "$copied" = 1 ] || echo "harness log (no run directory found): $log"
    [ "$copied" = 1 ] && rm -f "$log"
}
trap stash_log EXIT

cd "$repo_root"
cargo run --release -p celeriant_chaos -- "$@" 2>&1 | tee "$log"
exit "${PIPESTATUS[0]}"
