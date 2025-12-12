#!/bin/bash

echo "=== File Descriptor Limit Test ==="
echo ""

# Show current limits
echo "Current limits:"
echo "  Soft limit (ulimit -Sn): $(ulimit -Sn)"
echo "  Hard limit (ulimit -Hn): $(ulimit -Hn)"
echo "  System-wide limit: $(cat /proc/sys/fs/file-max)"
echo "  Currently open (system): $(cat /proc/sys/fs/file-nr | awk '{print $1}')"
echo ""

# Create a temp directory for our test files
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

echo "Opening file descriptors until failure..."
echo ""

# Array to hold file descriptors
count=0
success=true

# Open FDs until we hit the limit
while $success; do
    # Try to open a new file descriptor
    if exec {fd}> "$TMPDIR/fd_$count" 2>/dev/null; then
        ((count++))
        # Progress indicator every 1000 FDs
        if ((count % 1000 == 0)); then
            echo "  Opened $count file descriptors..."
        fi
    else
        success=false
    fi
done

echo ""
echo "=== Results ==="
echo "  Successfully opened: $count file descriptors"
echo "  Failed at FD #$((count + 1))"
echo ""

# Show what's using FDs in current process
echo "FDs used by this script (excluding test FDs):"
echo "  stdin(0), stdout(1), stderr(2), plus ~$(($(ls /proc/$$/fd | wc -l) - count)) others"
echo ""

echo "Done! Temp files cleaned up automatically."