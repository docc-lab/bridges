#!/usr/bin/env python3
"""
Simple plots: call depth per trace and number of spans per trace.
Uses data from any reconstructed metadata file (hash/bloom doesn't matter).
"""

import json
from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np
from collections import defaultdict

def load_metadata(filepath):
    """Load metadata from a JSON file."""
    with open(filepath, 'r') as f:
        return json.load(f)

def calculate_call_depth(spans):
    """Calculate maximum call depth from spans."""
    if not spans:
        return 0
    
    # Build parent-child relationships
    children = defaultdict(list)
    span_ids = set()
    root_spans = []
    
    for span in spans:
        span_id = span.get('spanID') or span.get('spanId', '')
        if not span_id:
            continue
        span_ids.add(span_id)
        
        # Check if this is a root span
        is_root = True
        references = span.get('references') or []
        for ref in references:
            if ref and ref.get('refType') == 'CHILD_OF':
                parent_id = ref.get('spanID') or ref.get('spanId', '')
                if parent_id and parent_id in span_ids:
                    children[parent_id].append(span_id)
                    is_root = False
        
        if is_root:
            root_spans.append(span_id)
    
    if not root_spans:
        return 0
    
    # BFS to find maximum depth
    max_depth = 0
    queue = [(root_id, 0) for root_id in root_spans]
    visited = set()
    
    while queue:
        span_id, depth = queue.pop(0)
        if span_id in visited:
            continue
        visited.add(span_id)
        max_depth = max(max_depth, depth)
        
        for child_id in children[span_id]:
            queue.append((child_id, depth + 1))
    
    return max_depth

def main():
    """Main function."""
    data_dir = Path(__file__).parent / 'data'
    
    # Find any reconstructed metadata file (hash or bloom, doesn't matter)
    reconstructed_dirs = list(data_dir.glob('*-reconstructed'))
    if not reconstructed_dirs:
        print("Error: No reconstructed directories found")
        return
    
    # Use the first one
    metadata_path = reconstructed_dirs[0] / 'metadata.json'
    print(f"Loading data from {metadata_path}")
    
    metadata = load_metadata(metadata_path)
    traces = metadata.get('traces', [])
    
    call_depths = []
    span_counts = []
    
    for trace in traces:
        # Get original span count
        original_spans = trace.get('original_spans', 0)
        
        # Filter out traces with only 1 span or extremely large outliers
        if original_spans <= 1 or original_spans > 10000:
            continue
        
        # Get call depth from original or reconstructed trace
        call_depth = 0
        rr = trace.get('reconnection_result', {})
        if rr:
            # Try taggedTrace (original before data loss) first
            tagged_trace = rr.get('taggedTrace')
            if tagged_trace and 'spans' in tagged_trace:
                call_depth = calculate_call_depth(tagged_trace['spans'])
            # Fall back to originalTrace (lossy) or reconnectedTrace
            elif not call_depth:
                original_trace = rr.get('originalTrace')
                if original_trace and 'spans' in original_trace:
                    call_depth = calculate_call_depth(original_trace['spans'])
            if not call_depth:
                reconnected_trace = rr.get('reconnectedTrace')
                if reconnected_trace and 'spans' in reconnected_trace:
                    call_depth = calculate_call_depth(reconnected_trace['spans'])
        
        # Store metrics
        call_depths.append(call_depth)
        span_counts.append(original_spans)
    
    print(f"\nFound {len(span_counts)} traces (filtered out traces with 1 span)")
    print(f"  Spans per trace: min={min(span_counts)}, max={max(span_counts)}, mean={np.mean(span_counts):.1f}, median={np.median(span_counts):.1f}")
    print(f"  Call depth per trace: min={min(call_depths)}, max={max(call_depths)}, mean={np.mean(call_depths):.1f}, median={np.median(call_depths):.1f}")
    
    # Plot 1: Call depth per trace
    fig, ax = plt.subplots(figsize=(8, 6))
    max_depth = max(call_depths) if call_depths else 0
    bins = np.arange(0, max_depth + 2) - 0.5
    ax.hist(call_depths, bins=bins, edgecolor='black', alpha=0.7)
    ax.set_xlabel('Call Depth per Trace', fontsize=12)
    ax.set_ylabel('Frequency', fontsize=12)
    ax.set_title('Distribution of Call Depth per Trace', fontsize=14)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig('call_depth_per_trace.png', dpi=300, bbox_inches='tight')
    print(f"\nPlot saved to call_depth_per_trace.png")
    plt.close()
    
    # Plot 2: Number of spans per trace
    fig, ax = plt.subplots(figsize=(8, 6))
    bins = np.linspace(0, max(span_counts), 30)
    ax.hist(span_counts, bins=bins, edgecolor='black', alpha=0.7)
    ax.set_xlabel('Number of Spans per Trace', fontsize=12)
    ax.set_ylabel('Frequency', fontsize=12)
    ax.set_title('Distribution of Spans per Trace (Original)', fontsize=14)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig('spans_per_trace.png', dpi=300, bbox_inches='tight')
    print(f"Plot saved to spans_per_trace.png")
    plt.close()
    
    print("\nDone!")

if __name__ == '__main__':
    main()

