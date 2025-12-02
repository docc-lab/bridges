#!/usr/bin/env python3
"""
Synthetic Jaeger/OpenTelemetry trace generator.

Generates traces with a complete w-ary tree structure where:
- Root is at depth 0
- All leaves are at depth d (call_depth)
- Every node has w children (width)
- Total leaves = w^d

Output format matches Jaeger API format with {"data": [{"traceID": ..., "spans": ..., "processes": ...}]}
"""

import json
import os
import random
import sys
import argparse
from typing import List, Dict, Any


def generate_32bit_span_id() -> str:
    """Generate a unique 32-bit span ID as hex string."""
    # Generate random 32-bit integer (0 to 2^32-1)
    span_id = random.randint(1, 2**32 - 1)
    # Convert to hex string (8 hex digits for 32 bits)
    return format(span_id, '08x')


def generate_trace_id() -> str:
    """Generate a 64-bit trace ID as hex string (Jaeger format)."""
    # Generate random 64-bit integer
    trace_id = random.randint(1, 2**64 - 1)
    # Convert to hex string (16 hex digits for 64 bits)
    return format(trace_id, '016x')


class SpanNode:
    """Represents a node in the trace tree."""
    def __init__(self, depth: int, index: int, parent: 'SpanNode' = None):
        self.depth = depth
        self.index = index
        self.parent = parent
        self.span_id = generate_32bit_span_id()
        self.children: List['SpanNode'] = []
        self.start_time_us = 0
        self.end_time_us = 0
        self.process_id = None


def build_tree(depth: int, width: int) -> SpanNode:
    """
    Build a complete w-ary tree of depth d.
    Returns the root node.
    """
    if depth < 0:
        raise ValueError("Depth must be >= 0")
    if width < 1:
        raise ValueError("Width must be >= 1")
    
    # Generate unique span IDs for all nodes first
    # We'll build the tree level by level
    nodes_by_level: List[List[SpanNode]] = [[] for _ in range(depth + 1)]
    
    # Create root node
    root = SpanNode(depth=0, index=0)
    nodes_by_level[0].append(root)
    
    # Build tree level by level
    for level in range(depth):
        for parent in nodes_by_level[level]:
            for child_idx in range(width):
                child = SpanNode(depth=level + 1, index=child_idx, parent=parent)
                parent.children.append(child)
                nodes_by_level[level + 1].append(child)
    
    return root


def assign_timestamps(root: SpanNode, base_time_us: int = 1798570615451777):
    """
    Assign start and end timestamps to all spans in a depth-first manner.
    Ensures parent spans start before children and end after all children.
    Uses microseconds (Jaeger format) instead of nanoseconds.
    """
    def dfs(node: SpanNode, current_time: int) -> int:
        node.start_time_us = current_time
        
        # Process all children
        child_duration = 1000  # 1ms per child operation (in microseconds)
        for child in node.children:
            current_time = dfs(child, current_time)
            current_time += child_duration
        
        # End time is after all children complete
        if node.children:
            node.end_time_us = current_time
        else:
            # Leaf node: short duration
            node.end_time_us = current_time + 500  # 0.5ms
        
        return node.end_time_us
    
    dfs(root, base_time_us)


def node_to_span(node: SpanNode, trace_id: str) -> Dict[str, Any]:
    """Convert a SpanNode to a Jaeger format span JSON object."""
    span = {
        "traceID": trace_id,
        "spanID": node.span_id,
        "operationName": f"Operation_depth_{node.depth}_idx_{node.index}",
        "startTime": node.start_time_us,
        "duration": node.end_time_us - node.start_time_us,
        "processID": node.process_id,
        "flags": 1
    }
    
    # Add references array instead of parentSpanId
    if node.parent is not None:
        span["references"] = [
            {
                "refType": "CHILD_OF",
                "traceID": trace_id,
                "spanID": node.parent.span_id
            }
        ]
    else:
        span["references"] = []
    
    return span


def collect_all_spans(root: SpanNode) -> List[SpanNode]:
    """Collect all spans from the tree in breadth-first order."""
    spans = []
    queue = [root]
    
    while queue:
        node = queue.pop(0)
        spans.append(node)
        queue.extend(node.children)
    
    return spans


def generate_trace(call_depth: int, width: int, trace_id: str = None) -> Dict[str, Any]:
    """
    Generate a synthetic Jaeger format trace.
    
    Args:
        call_depth: Depth of the tree (d). Root is at depth 0, leaves at depth d.
        width: Fan-out degree (w). Each node has w children.
        trace_id: Optional trace ID. If None, generates a random one.
    
    Returns:
        Dictionary representing the trace in Jaeger format wrapped in {"data": [...]}.
    """
    if trace_id is None:
        trace_id = generate_trace_id()
    
    # Build the tree
    root = build_tree(call_depth, width)
    
    # Assign timestamps
    assign_timestamps(root)
    
    # Collect all spans
    all_spans = collect_all_spans(root)
    
    # Assign process IDs (use sequential IDs like p1, p2, etc.)
    processes = {}
    for idx, node in enumerate(all_spans):
        process_id = f"p{idx + 1}"
        node.process_id = process_id
        processes[process_id] = {"serviceName": f"Service{idx + 1}"}
    
    # Convert to Jaeger format
    spans_json = [node_to_span(node, trace_id) for node in all_spans]
    
    # Create trace object in Jaeger format
    trace = {
        "traceID": trace_id,
        "spans": spans_json,
        "processes": processes
    }
    
    # Wrap in data array (Jaeger API format)
    return {"data": [trace]}


def main():
    parser = argparse.ArgumentParser(
        description="Generate synthetic Jaeger format traces with specified depth and width"
    )
    parser.add_argument(
        "--depth", "-d",
        type=int,
        required=True,
        help="Call depth (d). Root is at depth 0, leaves at depth d."
    )
    parser.add_argument(
        "--width", "-w",
        type=int,
        required=True,
        help="Fan-out width (w). Each node has w children."
    )
    parser.add_argument(
        "--output-dir", "-o",
        type=str,
        required=True,
        help="Output directory path. Will be created if it doesn't exist."
    )
    parser.add_argument(
        "--num-traces", "-n",
        type=int,
        default=1,
        help="Number of traces to generate (default: 1). Each trace gets a unique trace ID."
    )
    
    args = parser.parse_args()
    
    if args.depth < 0:
        print("Error: depth must be >= 0", file=sys.stderr)
        sys.exit(1)
    
    if args.width < 1:
        print("Error: width must be >= 1", file=sys.stderr)
        sys.exit(1)
    
    if args.num_traces < 1:
        print("Error: num-traces must be >= 1", file=sys.stderr)
        sys.exit(1)
    
    # Create output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Calculate expected number of nodes
    total_nodes = sum(args.width ** i for i in range(args.depth + 1))
    num_leaves = args.width ** args.depth
    
    print(f"Generating {args.num_traces} trace(s) with depth={args.depth}, width={args.width}", file=sys.stderr)
    print(f"Total nodes per trace: {total_nodes}, Leaves per trace: {num_leaves}", file=sys.stderr)
    print(f"Output directory: {args.output_dir}\n", file=sys.stderr)
    
    # Generate multiple traces
    for i in range(args.num_traces):
        trace_data = generate_trace(args.depth, args.width)
        trace_id = trace_data["data"][0]["traceID"]
        
        # Save to file named {traceid}.json
        output_path = os.path.join(args.output_dir, f"{trace_id}.json")
        trace_json = json.dumps(trace_data, indent=2)
        
        with open(output_path, 'w') as f:
            f.write(trace_json)
        
        print(f"Generated trace {i+1}/{args.num_traces}: {trace_id}.json", file=sys.stderr)
    
    print(f"\nAll traces written to {args.output_dir}", file=sys.stderr)


if __name__ == "__main__":
    main()

