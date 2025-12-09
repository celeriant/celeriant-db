#!/bin/bash
# Demo script for Celeriant CLI
# Usage: ./demo.sh [server_address]

SERVER="${1:-127.0.0.1:10000}"
CLI="cargo run -p celeriant_cli --"

echo "=== Celeriant CLI Demo ==="
echo "Server: $SERVER"
echo ""

echo "1. Listing organisations..."
$CLI --server "$SERVER" list-orgs
echo ""

echo "2. Creating a test aggregate with an event..."
$CLI --server "$SERVER" write \
    --org 1 --type 1 --id 99999 \
    --client-id 1 --event-type 1 \
    --data '{"demo": "Hello from Celeriant CLI!", "timestamp": "'$(date -Iseconds)'"}' \
    --allow-create
echo ""

echo "3. Checking aggregate exists..."
$CLI --server "$SERVER" exists --org 1 --type 1 --id 99999
echo ""

echo "4. Writing more events..."
for i in {1..5}; do
    $CLI --server "$SERVER" write \
        --org 1 --type 1 --id 99999 \
        --client-id 1 --event-type $i \
        --data "{\"event_number\": $i, \"message\": \"Event $i\"}"
done
echo ""

echo "5. Reading all events..."
$CLI --server "$SERVER" read --org 1 --type 1 --id 99999 --from 1
echo ""

echo "6. Reading as JSON..."
$CLI --server "$SERVER" read --org 1 --type 1 --id 99999 --from 1 --format json | head -50
echo "..."
echo ""

echo "7. Cleanup - deleting test aggregate..."
$CLI --server "$SERVER" delete --org 1 --type 1 --id 99999
echo ""

echo "=== Demo Complete ==="