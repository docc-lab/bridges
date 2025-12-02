#!/bin/bash
# Generate synthetic traces for all combinations of depth and width

set -e  # Exit on error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GENERATE_SCRIPT="${SCRIPT_DIR}/generate_trace.py"

# Check if generate_trace.py exists
if [ ! -f "$GENERATE_SCRIPT" ]; then
    echo "Error: generate_trace.py not found at $GENERATE_SCRIPT" >&2
    exit 1
fi

# Base output directory
BASE_DIR="synthetic-traces"

# Create base directory if it doesn't exist
mkdir -p "$BASE_DIR"

echo "Generating synthetic traces..."
echo "Depth range: 2 to 10"
echo "Width range: 1 to 20"
echo "Traces per configuration: 20"
echo ""

total_configs=0
for depth in {2..10}; do
    for width in {1..20}; do
        output_dir="${BASE_DIR}/t-${depth}-${width}"
        total_configs=$((total_configs + 1))
        
        echo "[$total_configs] Generating traces: depth=$depth, width=$width"
        echo "  Output directory: $output_dir"
        
        python3 "$GENERATE_SCRIPT" \
            --depth "$depth" \
            --width "$width" \
            --output-dir "$output_dir" \
            --num-traces 20
        
        # Verify we got 20 files
        file_count=0
        for _ in "$output_dir"/*.json; do
            [ -e "$_" ] && file_count=$((file_count + 1))
        done
        if [ "$file_count" -ne 20 ]; then
            echo "  WARNING: Expected 20 files, found $file_count" >&2
        else
            echo "  ✓ Generated 20 traces"
        fi
        echo ""
    done
done

echo "=========================================="
echo "Generation complete!"
echo "Total configurations: $total_configs"
echo "Total traces generated: $((total_configs * 20))"
echo "Output directory: $BASE_DIR"

