#!/usr/bin/env python3
"""
Plot metrics from trace metadata files.
Loads metadata.json from all *-reconstructed directories in data/ and creates plots.
The reconstructed metadata contains all needed information: ancestry data size (from original trace),
reconnection results (for FPR), and checkpoint distance.
"""

import json
import os
import sys
from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np
from collections import defaultdict

def load_metadata(filepath):
    """Load metadata from a JSON file."""
    with open(filepath, 'r') as f:
        return json.load(f)

def extract_checkpoint_distance(metadata):
    """Extract checkpoint distance from metadata."""
    return metadata.get('checkpoint_distance', 0)

def calculate_ancestry_metrics(metadata):
    """Calculate ancestry data size metrics from metadata."""
    traces = metadata.get('traces', [])
    if not traces:
        return [], [], 0
    
    checkpoint_distance = extract_checkpoint_distance(metadata)
    ancestry_sizes = []
    ancestry_sizes_per_span = []
    
    for trace in traces:
        # Filter out traces with only 1 span
        original_spans = trace.get('original_spans', 0)
        reconstructed_spans = trace.get('reconstructed_spans', 0)
        if original_spans <= 1 and reconstructed_spans <= 1:
            continue
        
        ancestry_bytes = trace.get('ancestry_data_size_bytes', 0)
        ancestry_bytes_per_span = trace.get('ancestry_data_size_bytes_per_span', 0.0)
        
        ancestry_sizes.append(ancestry_bytes)
        ancestry_sizes_per_span.append(ancestry_bytes_per_span)
    
    return ancestry_sizes, ancestry_sizes_per_span, checkpoint_distance

def calculate_false_positive_rate(metadata):
    """
    Calculate or extract false positive rate from metadata.
    For now, we'll try to extract from reconnection results or data loss info.
    If not available, we'll use a placeholder that can be calculated later.
    """
    traces = metadata.get('traces', [])
    fpr_values = []
    
    for trace in traces:
        # Filter out traces with only 1 span
        original_spans = trace.get('original_spans', 0)
        reconstructed_spans = trace.get('reconstructed_spans', 0)
        if original_spans <= 1 and reconstructed_spans <= 1:
            continue
        
        # Try to get false positive rate from reconnection results
        # This might need to be calculated from bloom filter properties
        reconnection_result = trace.get('reconnection_result')
        if reconnection_result:
            # If there's a confidence metric, we could derive FPR from it
            # For now, we'll use confidence as a proxy (1 - confidence might approximate FPR)
            bridge_edges = reconnection_result.get('bridge_edges', [])
            if bridge_edges:
                # Use average confidence as a proxy metric
                confidences = [edge.get('confidence', 0.0) for edge in bridge_edges]
                avg_confidence = np.mean(confidences) if confidences else 0.0
                # Approximate FPR as (1 - confidence), but this is a placeholder
                fpr = 1.0 - avg_confidence
                fpr_values.append(fpr)
            else:
                fpr_values.append(0.0)
        else:
            # No reconnection data, default to 0
            fpr_values.append(0.0)
    
    return fpr_values

def extract_checkpoint_from_dir_name(dir_name):
    """Extract checkpoint distance from directory name (e.g., 'tagged-hash-3-reconstructed' -> 3)."""
    # Remove suffixes
    base = dir_name.replace('-lossy', '').replace('-reconstructed', '')
    # Split by hyphen and try to parse the last part as checkpoint distance
    parts = base.split('-')
    if len(parts) == 0:
        return 0
    try:
        checkpoint = int(parts[-1])
        return checkpoint
    except ValueError:
        return 0

def extract_type_from_dir_name(dir_name):
    """Extract ancestry type from directory name (e.g., 'tagged-hash-3-reconstructed' -> 'hash')."""
    # Remove suffixes
    base = dir_name.replace('-lossy', '').replace('-reconstructed', '')
    # Split by hyphen: tagged-<type>-<depth>
    parts = base.split('-')
    if len(parts) >= 3 and parts[0] == 'tagged':
        return parts[1]  # Return 'hash' or 'bloom'
    return 'unknown'

def aggregate_data_by_checkpoint(metadata_dir):
    """Load all metadata files from *-reconstructed directories and aggregate by checkpoint distance.
    
    Note: We only need reconstructed metadata as it contains:
    - Ancestry data size (calculated from original trace before data loss)
    - Reconnection results (for FPR calculation)
    - Checkpoint distance
    """
    metadata_dir_path = Path(metadata_dir)
    
    aggregated = defaultdict(lambda: {
        'ancestry_sizes': [],
        'ancestry_sizes_per_span': [],
        'fpr_values': [],
        'source': [],
        'types': []  # Track hash vs bloom
    })
    
    # Find all directories matching the pattern
    reconstructed_dirs = list(metadata_dir_path.glob('*-reconstructed'))
    
    # Process reconstructed directories
    for reconstructed_dir in reconstructed_dirs:
        metadata_path = reconstructed_dir / 'metadata.json'
        if metadata_path.exists():
            checkpoint = extract_checkpoint_from_dir_name(reconstructed_dir.name)
            ancestry_type = extract_type_from_dir_name(reconstructed_dir.name)
            print(f"Loading reconstructed metadata from {metadata_path} (checkpoint: {checkpoint}, type: {ancestry_type})")
            try:
                reconstructed_metadata = load_metadata(metadata_path)
                ancestry_sizes, ancestry_sizes_per_span, checkpoint_from_metadata = calculate_ancestry_metrics(reconstructed_metadata)
                # Use checkpoint from metadata if available, otherwise use extracted from dir name
                if checkpoint_from_metadata > 0:
                    checkpoint = checkpoint_from_metadata
                fpr_values = calculate_false_positive_rate(reconstructed_metadata)
                
                if checkpoint > 0:  # Only include if checkpoint distance was set
                    aggregated[checkpoint]['ancestry_sizes'].extend(ancestry_sizes)
                    aggregated[checkpoint]['ancestry_sizes_per_span'].extend(ancestry_sizes_per_span)
                    aggregated[checkpoint]['fpr_values'].extend(fpr_values)
                    aggregated[checkpoint]['source'].extend(['reconstructed'] * len(ancestry_sizes))
                    aggregated[checkpoint]['types'].extend([ancestry_type] * len(ancestry_sizes))
                else:
                    print(f"  Warning: Checkpoint distance not set for {metadata_path}, skipping")
            except Exception as e:
                print(f"Error loading {metadata_path}: {e}")
    
    return aggregated

def plot_checkpoint_vs_ancestry_with_fpr(aggregated_data, output_file='checkpoint_ancestry_fpr.png'):
    """
    Plot checkpoint distance (x-axis) vs average ancestry data size (y-axis) 
    with distribution visualization, differentiated by hash/bloom type.
    Shows trend lines connecting averages across checkpoint distances.
    """
    # Organize data by checkpoint and type
    hash_by_checkpoint = defaultdict(list)
    bloom_by_checkpoint = defaultdict(list)
    
    for checkpoint, data in sorted(aggregated_data.items()):
        for ancestry_size, ancestry_type in zip(data['ancestry_sizes'], data['types']):
            if ancestry_type == 'hash':
                hash_by_checkpoint[checkpoint].append(ancestry_size)
            elif ancestry_type == 'bloom':
                bloom_by_checkpoint[checkpoint].append(ancestry_size)
    
    if not hash_by_checkpoint and not bloom_by_checkpoint:
        print("No data to plot. Make sure metadata files contain checkpoint_distance and ancestry data.")
        return
    
    # Create the plot
    fig, ax = plt.subplots(figsize=(10, 6))
    
    # Plot hash data
    if hash_by_checkpoint:
        hash_checkpoints = sorted(hash_by_checkpoint.keys())
        hash_means = [np.mean(hash_by_checkpoint[cp]) for cp in hash_checkpoints]
        hash_stds = [np.std(hash_by_checkpoint[cp]) for cp in hash_checkpoints]
        
        # Plot individual data points with distribution
        for cp in hash_checkpoints:
            y_data = hash_by_checkpoint[cp]
            x_data = [cp] * len(y_data)
            # Add small jitter for visibility
            x_jittered = [cp + np.random.uniform(-0.1, 0.1) for _ in x_data]
            ax.scatter(x_jittered, y_data, alpha=0.3, s=20, color='blue', marker='o')
        
        # Plot mean line with error bars
        ax.errorbar(hash_checkpoints, hash_means, yerr=hash_stds, 
                   marker='o', linestyle='-', linewidth=2, markersize=8,
                   color='blue', label='Hash', capsize=5, capthick=2)
    
    # Plot bloom data
    if bloom_by_checkpoint:
        bloom_checkpoints = sorted(bloom_by_checkpoint.keys())
        bloom_means = [np.mean(bloom_by_checkpoint[cp]) for cp in bloom_checkpoints]
        bloom_stds = [np.std(bloom_by_checkpoint[cp]) for cp in bloom_checkpoints]
        
        # Plot individual data points with distribution
        for cp in bloom_checkpoints:
            y_data = bloom_by_checkpoint[cp]
            x_data = [cp] * len(y_data)
            # Add small jitter for visibility
            x_jittered = [cp + np.random.uniform(-0.1, 0.1) for _ in x_data]
            ax.scatter(x_jittered, y_data, alpha=0.3, s=20, color='red', marker='^')
        
        # Plot mean line with error bars
        ax.errorbar(bloom_checkpoints, bloom_means, yerr=bloom_stds,
                   marker='^', linestyle='-', linewidth=2, markersize=8,
                   color='red', label='Bloom', capsize=5, capthick=2)
    
    # Add legend
    ax.legend(loc='best', fontsize=11)
    
    # Set labels and title
    ax.set_xlabel('Checkpoint Distance', fontsize=12)
    ax.set_ylabel('Ancestry Data Size per Trace (bytes)', fontsize=12)
    ax.set_title('Checkpoint Distance vs Average Ancestry Data Size per Trace\n(with distribution)', fontsize=14)
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Plot saved to {output_file}")
    plt.close()

def plot_checkpoint_vs_ancestry_per_span_with_fpr(aggregated_data, output_file='checkpoint_ancestry_per_span_fpr.png'):
    """
    Plot checkpoint distance (x-axis) vs average ancestry data size per span (y-axis) 
    with distribution visualization, differentiated by hash/bloom type.
    Shows trend lines connecting averages across checkpoint distances.
    """
    # Organize data by checkpoint and type
    hash_by_checkpoint = defaultdict(list)
    bloom_by_checkpoint = defaultdict(list)
    
    for checkpoint, data in sorted(aggregated_data.items()):
        for ancestry_size_per_span, ancestry_type in zip(data['ancestry_sizes_per_span'], data['types']):
            if ancestry_type == 'hash':
                hash_by_checkpoint[checkpoint].append(ancestry_size_per_span)
            elif ancestry_type == 'bloom':
                bloom_by_checkpoint[checkpoint].append(ancestry_size_per_span)
    
    if not hash_by_checkpoint and not bloom_by_checkpoint:
        print("No data to plot. Make sure metadata files contain checkpoint_distance and ancestry data.")
        return
    
    # Create the plot
    fig, ax = plt.subplots(figsize=(10, 6))
    
    # Plot hash data
    if hash_by_checkpoint:
        hash_checkpoints = sorted(hash_by_checkpoint.keys())
        hash_means = [np.mean(hash_by_checkpoint[cp]) for cp in hash_checkpoints]
        hash_stds = [np.std(hash_by_checkpoint[cp]) for cp in hash_checkpoints]
        
        # Plot individual data points with distribution
        for cp in hash_checkpoints:
            y_data = hash_by_checkpoint[cp]
            x_data = [cp] * len(y_data)
            # Add small jitter for visibility
            x_jittered = [cp + np.random.uniform(-0.1, 0.1) for _ in x_data]
            ax.scatter(x_jittered, y_data, alpha=0.3, s=20, color='blue', marker='o')
        
        # Plot mean line with error bars
        ax.errorbar(hash_checkpoints, hash_means, yerr=hash_stds,
                   marker='o', linestyle='-', linewidth=2, markersize=8,
                   color='blue', label='Hash', capsize=5, capthick=2)
    
    # Plot bloom data
    if bloom_by_checkpoint:
        bloom_checkpoints = sorted(bloom_by_checkpoint.keys())
        bloom_means = [np.mean(bloom_by_checkpoint[cp]) for cp in bloom_checkpoints]
        bloom_stds = [np.std(bloom_by_checkpoint[cp]) for cp in bloom_checkpoints]
        
        # Plot individual data points with distribution
        for cp in bloom_checkpoints:
            y_data = bloom_by_checkpoint[cp]
            x_data = [cp] * len(y_data)
            # Add small jitter for visibility
            x_jittered = [cp + np.random.uniform(-0.1, 0.1) for _ in x_data]
            ax.scatter(x_jittered, y_data, alpha=0.3, s=20, color='red', marker='^')
        
        # Plot mean line with error bars
        ax.errorbar(bloom_checkpoints, bloom_means, yerr=bloom_stds,
                   marker='^', linestyle='-', linewidth=2, markersize=8,
                   color='red', label='Bloom', capsize=5, capthick=2)
    
    # Add legend
    ax.legend(loc='best', fontsize=11)
    
    # Set labels and title
    ax.set_xlabel('Checkpoint Distance', fontsize=12)
    ax.set_ylabel('Ancestry Data Size per Span (bytes/span)', fontsize=12)
    ax.set_title('Checkpoint Distance vs Ancestry Data Size per Span\n(with distribution)', fontsize=14)
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Plot saved to {output_file}")
    plt.close()

def main():
    """Main function."""
    # Default data directory
    data_dir = Path(__file__).parent / 'data'
    
    # Allow command line override
    if len(sys.argv) > 1:
        data_dir = Path(sys.argv[1])
    
    if not data_dir.exists():
        print(f"Error: Data directory {data_dir} does not exist")
        sys.exit(1)
    
    print(f"Loading metadata from {data_dir}")
    
    # Aggregate data by checkpoint distance
    aggregated_data = aggregate_data_by_checkpoint(data_dir)
    
    if not aggregated_data:
        print("No data found with checkpoint distances. Make sure metadata files exist and contain checkpoint_distance.")
        sys.exit(1)
    
    print(f"\nFound data for checkpoint distances: {sorted(aggregated_data.keys())}")
    for checkpoint, data in sorted(aggregated_data.items()):
        print(f"  Checkpoint {checkpoint}: {len(data['ancestry_sizes'])} traces")
    
    # Create plots
    print("\nGenerating plots...")
    plot_checkpoint_vs_ancestry_with_fpr(aggregated_data)
    plot_checkpoint_vs_ancestry_per_span_with_fpr(aggregated_data)
    
    print("\nDone!")

if __name__ == '__main__':
    main()

