#!/usr/bin/env python3
"""
Calculate average ancestry data size for synthetic traces in hybrid mode.

This script mirrors the logic of trace_tagger.go to accurately calculate ancestry data sizes.
"""

import os
import sys
import argparse
import math
import base64
from pathlib import Path
from typing import Dict, List, Set, Optional

# Optional plotting dependencies
try:
    import numpy as np
    import matplotlib.pyplot as plt
    import seaborn as sns
    PLOTTING_AVAILABLE = True
except ImportError:
    PLOTTING_AVAILABLE = False


class Span:
    """Represents a span in the trace tree."""
    def __init__(self, span_id: str, depth: int, parent_id: Optional[str] = None):
        self.span_id = span_id
        self.depth = depth
        self.parent_id = parent_id
        self.children: List[str] = []
        self.is_root = parent_id is None
        self.is_leaf = False
        self.is_high_priority = False
        self.ancestry_path: List[str] = []
        self.hash_array: List[str] = []
        self.start_time = depth * 1000  # Simple ordering for 2nd child selection


def build_synthetic_tree(depth: int, width: int) -> Dict[str, Span]:
    """
    Build a complete w-ary tree structure.
    Returns a dictionary mapping span_id -> Span.
    """
    spans: Dict[str, Span] = {}
    span_counter = 0
    
    def create_node(node_depth: int, parent_id: Optional[str] = None) -> str:
        nonlocal span_counter
        span_id = f"span_{span_counter:08x}"
        span_counter += 1
        
        span = Span(span_id, node_depth, parent_id)
        spans[span_id] = span
        
        if parent_id:
            spans[parent_id].children.append(span_id)
        
        # Create children
        if node_depth < depth:
            for i in range(width):
                child_id = create_node(node_depth + 1, span_id)
                # No need to store, already added to children
        
        return span_id
    
    # Create root
    create_node(0, None)
    
    # Mark leaves
    for span in spans.values():
        if len(span.children) == 0:
            span.is_leaf = True
    
    return spans


def calculate_bloom_filter_size(num_elements: int, false_positive_rate: float = 0.01) -> int:
    """
    Calculate the size of a serialized bloom filter (base64 encoded).
    """
    if num_elements == 0:
        return 0
    
    # Calculate number of bits needed
    m = -num_elements * math.log(false_positive_rate) / (math.log(2) ** 2)
    m_bits = int(math.ceil(m))
    m_bytes = (m_bits + 7) // 8  # Round up to bytes
    
    # Gob encoding overhead: type info + length + data
    gob_overhead = 25
    
    # Base64 encoding increases size by ~33%
    serialized_size = m_bytes + gob_overhead
    base64_size = int(math.ceil(serialized_size * 4 / 3))
    
    return base64_size


def process_trace_like_tagger(spans: Dict[str, Span], checkpoint_distance: int, ancestry_mode: str = "hybrid"):
    """
    Process spans following the exact logic of trace_tagger.go.
    """
    # Build maps (like trace_tagger does)
    span_map = spans
    children: Dict[str, List[str]] = {}
    parent_map: Dict[str, str] = {}
    root_spans: List[Span] = []
    
    for span_id, span in span_map.items():
        children[span_id] = span.children
        if span.parent_id:
            parent_map[span_id] = span.parent_id
        else:
            root_spans.append(span)
    
    # Calculate depth for each span (already set during tree building, but verify)
    depth_map: Dict[str, int] = {}
    for span_id, span in span_map.items():
        depth_map[span_id] = span.depth
    
    # Identify leaves
    leaf_set: Set[str] = set()
    for span_id, span in span_map.items():
        if len(span.children) == 0:
            leaf_set.add(span_id)
    
    # First pass: mark all spans with their priority (like trace_tagger)
    priority_map: Dict[str, bool] = {}
    for span_id, span in span_map.items():
        depth = depth_map[span_id]
        is_root = span.is_root
        is_leaf = span_id in leaf_set
        
        # Determine priority (matching trace_tagger logic)
        high_priority = False
        if is_root or is_leaf:
            high_priority = True
        elif checkpoint_distance > 0 and depth % checkpoint_distance == 0:
            high_priority = True
        
        priority_map[span_id] = high_priority
        span.is_high_priority = high_priority
    
    # Helper: find last high priority ancestor (matching trace_tagger)
    def find_last_high_priority_ancestor(span_id: str, skip_self: bool) -> Optional[str]:
        span = span_map.get(span_id)
        if not span:
            return None
        
        if not skip_self and priority_map.get(span_id, False):
            return span_id
        
        parent_id = span.parent_id
        if not parent_id:
            return span_id  # Root
        
        if parent_id not in span_map:
            return span_id  # Fallback
        
        return find_last_high_priority_ancestor(parent_id, False)
    
    # Helper: build path from ancestor to span
    def build_path_from_ancestor(span_id: str, ancestor_id: str) -> List[str]:
        if span_id == ancestor_id:
            return [span_id]
        
        span = span_map.get(span_id)
        if not span:
            return []
        
        parent_id = span.parent_id
        if not parent_id:
            return [span_id]
        
        if parent_id not in span_map:
            return [span_id]
        
        parent_path = build_path_from_ancestor(parent_id, ancestor_id)
        return parent_path + [span_id]
    
    # Build reset path (matching trace_tagger's buildResetPath)
    def build_reset_path(span_id: str) -> List[str]:
        span = span_map.get(span_id)
        if not span:
            return []
        
        # Find the last high priority ancestor, skipping current span
        ancestor_id = find_last_high_priority_ancestor(span_id, True)
        if not ancestor_id:
            return []
        
        parent_id = span.parent_id
        if not parent_id:
            return []  # Root has no ancestry
        
        # Build path from ancestor to parent (NOT including current span)
        return build_path_from_ancestor(parent_id, ancestor_id)
    
    # For hybrid mode: detect fan-outs and track hash assignments (matching trace_tagger)
    hash_assignments_by_checkpoint: Dict[str, Dict[str, List[str]]] = {}
    
    if ancestry_mode == "hybrid":
        # Helper: find first high-priority descendant
        def find_first_high_priority_descendant(span_id: str) -> Optional[str]:
            if priority_map.get(span_id, False):
                return span_id
            
            child_ids = children.get(span_id, [])
            for child_id in child_ids:
                result = find_first_high_priority_descendant(child_id)
                if result:
                    return result
            return None
        
        # Helper: find checkpoint for a span
        def find_checkpoint(span_id: str) -> str:
            if priority_map.get(span_id, False):
                ancestor = find_last_high_priority_ancestor(span_id, True)
                if not ancestor:
                    return span_id
                return ancestor
            
            parent_id = parent_map.get(span_id)
            if not parent_id:
                return span_id  # Root
            return find_checkpoint(parent_id)
        
        # Detect fan-outs and assign hash to 2nd child (matching trace_tagger)
        for parent_id, child_ids in children.items():
            if len(child_ids) > 1:
                # Fan-out detected: sort children by start time
                parent_span = span_map[parent_id]
                children_with_time = []
                for child_id in child_ids:
                    child_span = span_map[child_id]
                    children_with_time.append((child_span.start_time, child_id))
                
                # Sort by start time
                children_with_time.sort()
                
                # Select 2nd child (index 1)
                if len(children_with_time) >= 2:
                    selected_child_id = children_with_time[1][1]
                    # Find first high-priority descendant
                    high_priority_span_id = find_first_high_priority_descendant(selected_child_id)
                    if high_priority_span_id:
                        # Find which checkpoint this assignment belongs to
                        checkpoint_id = find_checkpoint(high_priority_span_id)
                        if not checkpoint_id:
                            checkpoint_id = high_priority_span_id
                        
                        # Initialize map for this checkpoint if needed
                        if checkpoint_id not in hash_assignments_by_checkpoint:
                            hash_assignments_by_checkpoint[checkpoint_id] = {}
                        
                        # Get parent depth
                        parent_depth = depth_map[parent_id]
                        
                        # Add parent spanID with depth to hash array
                        hash_entry = f"{parent_id}:{parent_depth}"
                        if high_priority_span_id not in hash_assignments_by_checkpoint[checkpoint_id]:
                            hash_assignments_by_checkpoint[checkpoint_id][high_priority_span_id] = []
                        hash_assignments_by_checkpoint[checkpoint_id][high_priority_span_id].append(hash_entry)
    
    # Process each span to set ancestry tags (only for high priority)
    total_bloom_size = 0
    total_hash_size = 0
    total_mode_size = 0
    
    for span_id, span in span_map.items():
        if not priority_map.get(span_id, False):
            continue  # Only process high-priority spans
        
        # Build ancestry data with reset path
        path = build_reset_path(span_id)
        span.ancestry_path = path
        
        # Calculate bloom filter size for ancestry
        if ancestry_mode in ["bloom", "hybrid"]:
            path_length = len(path)
            if path_length > 0:
                bloom_size = calculate_bloom_filter_size(path_length, 0.01)
                total_bloom_size += bloom_size
        
        # Ancestry mode tag
        total_mode_size += 6  # "hybrid" = 6 bytes
        
        # For hybrid mode: handle hash arrays
        if ancestry_mode == "hybrid":
            # Find last checkpoint
            last_checkpoint = find_last_high_priority_ancestor(span_id, True)
            if not last_checkpoint:
                last_checkpoint = span_id
            
            # Get hash assignments for this span from the current checkpoint interval
            hash_parents = []
            if last_checkpoint in hash_assignments_by_checkpoint:
                if span_id in hash_assignments_by_checkpoint[last_checkpoint]:
                    hash_parents = hash_assignments_by_checkpoint[last_checkpoint][span_id]
            
            span.hash_array = hash_parents
            
            # Calculate hash array size
            if len(hash_parents) > 0:
                # Hash array: comma-separated "spanID:depth" pairs
                hash_array_str = ",".join(hash_parents)
                total_hash_size += len(hash_array_str.encode('utf-8'))
    
    total_ancestry_size = total_bloom_size + total_hash_size + total_mode_size
    total_nodes = len(span_map)
    
    return total_ancestry_size, total_bloom_size, total_hash_size, total_mode_size, total_nodes


def calculate_ancestry_size_hybrid(depth: int, width: int, checkpoint_distance: int = 1):
    """
    Calculate ancestry data size by building a synthetic tree and processing it like trace_tagger.
    
    For very large trees, uses optimized calculation without building the full tree structure.
    """
    # Calculate total nodes
    if width == 1:
        total_nodes = depth + 1
    else:
        total_nodes = (width ** (depth + 1) - 1) // (width - 1)
    
    # For very large trees (>1M nodes), use optimized calculation
    if total_nodes > 1_000_000:
        return calculate_ancestry_size_optimized(depth, width, checkpoint_distance)
    
    # For smaller trees, build the actual structure
    spans = build_synthetic_tree(depth, width)
    
    # Process like trace_tagger
    total_size, bloom_size, hash_size, mode_size, total_nodes = process_trace_like_tagger(
        spans, checkpoint_distance, ancestry_mode="hybrid"
    )
    
    return total_size, bloom_size, hash_size, mode_size, total_nodes


def calculate_ancestry_size_optimized(depth: int, width: int, checkpoint_distance: int = 1):
    """
    Optimized calculation that doesn't build the full tree structure.
    Calculates sizes analytically based on tree structure.
    """
    # Calculate total nodes
    if width == 1:
        total_nodes = depth + 1
    else:
        total_nodes = (width ** (depth + 1) - 1) // (width - 1)
    
    # Determine which depths are checkpoints
    checkpoint_depths = []
    for d in range(depth + 1):
        is_checkpoint = (d == 0 or d == depth or (checkpoint_distance > 0 and d % checkpoint_distance == 0))
        if is_checkpoint:
            checkpoint_depths.append(d)
    
    # Calculate bloom filter sizes
    total_bloom_size = 0
    for cp_depth in checkpoint_depths:
        if cp_depth == 0:  # Root has no ancestry
            continue
        
        # Find last checkpoint before this one
        last_cp_depth = 0
        for d in range(cp_depth - 1, -1, -1):
            if d in checkpoint_depths:
                last_cp_depth = d
                break
        
        # Ancestry path length: from last checkpoint to parent (inclusive)
        parent_depth = cp_depth - 1
        if last_cp_depth == parent_depth:
            path_length = 1
        else:
            path_length = parent_depth - last_cp_depth + 1
        
        # Count how many checkpoints are at this depth
        if cp_depth == 0:
            num_at_depth = 1
        else:
            num_at_depth = width ** cp_depth
        
        # Calculate bloom filter size for each checkpoint
        if path_length > 0:
            bloom_size_per = calculate_bloom_filter_size(path_length, 0.01)
            total_bloom_size += num_at_depth * bloom_size_per
    
    # Calculate hash array sizes
    total_hash_size = 0
    if width > 1:
        # Hash entry format: "spanID:depth" (e.g., "span_00000001:3")
        # spanID is like "span_00000001" = 12 chars, ":" = 1 char, depth = 1-2 digits
        # Total: ~14-15 bytes per entry. Use 15 as safe estimate.
        hash_entry_size = 15  # "span_XXXXXXXX:depth" format
        
        # For each checkpoint, count hash entries it receives
        for cp_depth in checkpoint_depths:
            if cp_depth == 0:  # Root doesn't receive hash entries
                continue
            
            # Find last checkpoint before this one
            last_cp_depth = 0
            for d in range(cp_depth - 1, -1, -1):
                if d in checkpoint_depths:
                    last_cp_depth = d
                    break
            
            # Count fan-outs in the interval from last checkpoint to this checkpoint
            # Each checkpoint receives entries from fan-outs at depths in [last_cp_depth, cp_depth-1]
            # But only from fan-outs in its path (one per depth level in the interval)
            num_fan_outs_in_path = cp_depth - last_cp_depth
            
            # Count checkpoints at this depth
            num_at_depth = width ** cp_depth
            
            # Each checkpoint receives hash entries from fan-outs in its path
            total_hash_size += num_at_depth * num_fan_outs_in_path * hash_entry_size
    
    # Calculate mode tag sizes
    total_checkpoint_spans = sum(width ** d if d > 0 else 1 for d in checkpoint_depths)
    total_mode_size = total_checkpoint_spans * 6  # "hybrid" = 6 bytes
    
    total_ancestry_size = total_bloom_size + total_hash_size + total_mode_size
    
    return total_ancestry_size, total_bloom_size, total_hash_size, total_mode_size, total_nodes


def process_synthetic_traces(base_dir=None, depth_range=(2, 10), width_range=(1, 20), checkpoint_distance=1):
    """
    Calculate average ancestry sizes for synthetic traces.
    """
    results = []
    
    if base_dir:
        # Discover configurations from directory structure
        base_path = Path(base_dir)
        
        if not base_path.exists():
            print(f"Error: Directory {base_dir} does not exist", file=sys.stderr)
            return []
        
        # Find all t-depth-width directories
        for dir_path in sorted(base_path.iterdir()):
            if not dir_path.is_dir():
                continue
            
            dir_name = dir_path.name
            if not dir_name.startswith("t-"):
                continue
            
            # Parse depth and width from directory name
            try:
                parts = dir_name.split("-")
                if len(parts) != 3:
                    continue
                depth = int(parts[1])
                width = int(parts[2])
            except ValueError:
                continue
            
            # Count number of trace files
            trace_files = list(dir_path.glob("*.json"))
            num_traces = len(trace_files)
            
            # Calculate ancestry size
            total_size, bloom_size, hash_size, mode_size, total_nodes = calculate_ancestry_size_hybrid(
                depth, width, checkpoint_distance=checkpoint_distance
            )
            
            # Average size per span
            avg_size = total_size / total_nodes if total_nodes > 0 else 0
            
            results.append({
                "depth": depth,
                "width": width,
                "num_traces": num_traces,
                "total_nodes": total_nodes,
                "avg_ancestry_size_per_span": avg_size,
                "total_ancestry_size": total_size,
                "bloom_size": bloom_size,
                "hash_size": hash_size,
                "mode_size": mode_size
            })
    else:
        # Calculate for all combinations
        for depth in range(depth_range[0], depth_range[1] + 1):
            for width in range(width_range[0], width_range[1] + 1):
                # Calculate ancestry size
                total_size, bloom_size, hash_size, mode_size, total_nodes = calculate_ancestry_size_hybrid(
                    depth, width, checkpoint_distance=checkpoint_distance
                )
                
                # Average size per span
                avg_size = total_size / total_nodes if total_nodes > 0 else 0
                
                results.append({
                    "depth": depth,
                    "width": width,
                    "num_traces": 0,
                    "total_nodes": total_nodes,
                    "avg_ancestry_size_per_span": avg_size,
                    "total_ancestry_size": total_size,
                    "bloom_size": bloom_size,
                    "hash_size": hash_size,
                    "mode_size": mode_size
                })
    
    return results


def create_heatmap(results, output_file=None, checkpoint_distance=1):
    """Create a heatmap showing average ancestry data size per span."""
    if not PLOTTING_AVAILABLE:
        print("Error: matplotlib and seaborn are required for plotting.", file=sys.stderr)
        print("Install them with: pip install matplotlib seaborn", file=sys.stderr)
        return
    
    if not results:
        print("No results to plot", file=sys.stderr)
        return
    
    # Extract unique depths and widths
    depths = sorted(set(r["depth"] for r in results))
    widths = sorted(set(r["width"] for r in results))
    
    # Create a 2D array for the heatmap
    heatmap_data = np.full((len(depths), len(widths)), np.nan)
    
    # Fill in the data
    for r in results:
        depth_idx = depths.index(r["depth"])
        width_idx = widths.index(r["width"])
        heatmap_data[depth_idx, width_idx] = r["avg_ancestry_size_per_span"]
    
    # Create the heatmap
    plt.figure(figsize=(12, 8))
    ax = sns.heatmap(
        heatmap_data,
        xticklabels=widths,
        yticklabels=depths,
        annot=True,
        fmt='.1f',
        cmap='YlOrRd',
        cbar_kws={'label': 'Average Ancestry Data Size per Span (bytes)'}
    )
    
    # Reverse y-axis so lower depths are at bottom, higher at top
    ax.invert_yaxis()
    
    plt.xlabel('Width (fan-out degree)', fontsize=12)
    plt.ylabel('Depth (call depth)', fontsize=12)
    plt.title(f'Average Ancestry Data Size per Span\n(Hybrid mode, checkpoint distance = {checkpoint_distance})', fontsize=14, fontweight='bold')
    plt.tight_layout()
    
    if output_file:
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Heatmap saved to {output_file}", file=sys.stderr)
    else:
        plt.show()
    
    plt.close()


def main():
    parser = argparse.ArgumentParser(
        description="Calculate average ancestry data size for synthetic traces"
    )
    parser.add_argument(
        "--base-dir",
        type=str,
        default=None,
        help="Base directory containing synthetic trace folders. If not provided, calculates for all combinations."
    )
    parser.add_argument(
        "--depth-min",
        type=int,
        default=2,
        help="Minimum depth (default: 2)"
    )
    parser.add_argument(
        "--depth-max",
        type=int,
        default=10,
        help="Maximum depth (default: 10)"
    )
    parser.add_argument(
        "--width-min",
        type=int,
        default=1,
        help="Minimum width (default: 1)"
    )
    parser.add_argument(
        "--width-max",
        type=int,
        default=20,
        help="Maximum width (default: 20)"
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output CSV file. If not specified, prints to stdout."
    )
    parser.add_argument(
        "--heatmap",
        type=str,
        default="depth-width-heatmap.pdf",
        help="Generate and save a heatmap to the specified file (default: depth-width-heatmap.pdf). Set to empty string to disable."
    )
    parser.add_argument(
        "--checkpoint-distance",
        type=int,
        default=1,
        help="Checkpoint distance (PriorityDepth). Spans at depths that are multiples of this value are checkpoints, plus root and leaves. (default: 1)"
    )
    
    args = parser.parse_args()
    
    if args.checkpoint_distance < 1:
        print("Error: checkpoint-distance must be >= 1", file=sys.stderr)
        sys.exit(1)
    
    if args.base_dir:
        results = process_synthetic_traces(base_dir=args.base_dir, checkpoint_distance=args.checkpoint_distance)
    else:
        results = process_synthetic_traces(
            base_dir=None,
            depth_range=(args.depth_min, args.depth_max),
            width_range=(args.width_min, args.width_max),
            checkpoint_distance=args.checkpoint_distance
        )
    
    if not results:
        if args.base_dir:
            print("No trace directories found", file=sys.stderr)
        else:
            print("No results generated", file=sys.stderr)
        return
    
    # Sort by depth, then width
    results.sort(key=lambda x: (x["depth"], x["width"]))
    
    # Generate heatmap if requested
    if args.heatmap:
        create_heatmap(results, args.heatmap, checkpoint_distance=args.checkpoint_distance)
    
    # Output results
    import csv
    
    fieldnames = ["depth", "width", "num_traces", "total_nodes", "avg_ancestry_size_per_span", 
                  "total_ancestry_size", "bloom_size", "hash_size", "mode_size"]
    
    if args.output:
        with open(args.output, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for r in results:
                writer.writerow(r)
        print(f"Results written to {args.output}", file=sys.stderr)
    else:
        writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
        writer.writeheader()
        for r in results:
            writer.writerow(r)


if __name__ == "__main__":
    main()
