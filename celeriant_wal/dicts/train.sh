#!/usr/bin/env bash
# Retrain the built-in json-web-events-v1 zstd dictionary from the corpus.
#
# Usage: ./train.sh
#
# Requires: zstd CLI >= 1.4.0
# Output:   json_web_events_v1.zstd_dict (overwrites existing file)
#
# Each line in the .jsonl file is one training sample. zstd --train needs
# separate files, so we split into a temp directory first.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CORPUS="${SCRIPT_DIR}/json_web_events_v1.training.jsonl"
OUTPUT="${SCRIPT_DIR}/json_web_events_v1.zstd_dict"
TMPDIR="$(mktemp -d)"

cleanup() { rm -rf "$TMPDIR"; }
trap cleanup EXIT

i=0
while IFS= read -r line; do
    printf '%s' "$line" > "${TMPDIR}/sample_${i}.json"
    i=$((i + 1))
done < "$CORPUS"

zstd --train "${TMPDIR}"/*.json -o "$OUTPUT" --maxdict=65536

echo "Wrote ${OUTPUT} ($(wc -c < "$OUTPUT") bytes)"
