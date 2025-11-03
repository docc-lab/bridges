#!/bin/bash

# Script to run bridges on all tagged-*-* directories in data/
# Excludes *-lossy and *-reconstructed directories (these are outputs)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${SCRIPT_DIR}/data"

if [ ! -d "$DATA_DIR" ]; then
    echo "Error: Data directory $DATA_DIR does not exist"
    exit 1
fi

echo "Scanning for tagged directories in $DATA_DIR..."

# Find all directories matching tagged-*-* pattern
# Exclude those ending in -lossy or -reconstructed
directories=$(find "$DATA_DIR" -maxdepth 1 -type d -name "tagged-*-*" \
    ! -name "*-lossy" ! -name "*-reconstructed" | sort)

if [ -z "$directories" ]; then
    echo "No tagged directories found in $DATA_DIR"
    exit 0
fi

echo "Found directories:"
echo "$directories" | sed 's|^.*/||' | sed 's/^/  - /'
echo ""

# Count total directories
total=$(echo "$directories" | wc -l)
current=0

# Process each directory
while IFS= read -r dir; do
    current=$((current + 1))
    dir_name=$(basename "$dir")
    
    echo "========================================="
    echo "[$current/$total] Processing: $dir_name"
    echo "========================================="
    
    cd "$SCRIPT_DIR"
    
    # Run bridges on this directory
    if go run jaeger_trace_loader.go -input folder -folder "$dir"; then
        echo "✅ Successfully processed $dir_name"
    else
        echo "❌ Error processing $dir_name"
        exit 1
    fi
    
    echo ""
    
done <<< "$directories"

echo "========================================="
echo "All directories processed successfully!"
echo "========================================="

