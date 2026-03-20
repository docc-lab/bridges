#!/usr/bin/env python3
"""
Flexible plotter for bridges/*_bagsize.json outputs.

Example:
  python3 plot_bagsize_field.py \
    --field avg_baggage_call \
    --bridges pb cgpb \
    --cpds 3 4 5 6 \
    --prefix uber_ \
    --out baggage_avg.pdf
"""

import argparse
import json
import os
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import matplotlib.pyplot as plt


BRIDGE_META = {
    # mode -> (label, color, marker)
    "pb": ("Path", "#2f9e44", "s"),
    "cgpb": ("Call Graph Preserving", "#e03131", "^"),
    "structural": ("Structural", "#1c7ed6", "o"),
    "sb": ("Structural", "#1c7ed6", "o"),  # allow alias
}


FIELD_YLABEL = {
    "amortized_by_total": "Overhead (in bytes)",
    "amortized_by_checkpoint": "Overhead per checkpoint span (in bytes)",
    "max_checkpoint_payload": "Overhead (worst case) (in bytes)",
    "avg_baggage_call": "Baggage bytes (average) (in bytes)",
    "max_baggage_call": "Baggage bytes (worst case) (in bytes)",
    "num_baggage_calls": "Number of baggage calls",
    "num_checkpoint_spans": "Number of checkpoint spans",
    "num_spans": "Number of spans",
}


def _load_field_values(json_path: Path, field: str) -> List[float]:
    with open(json_path, "r") as f:
        data = json.load(f)
    if field not in data:
        raise KeyError(f"Missing field {field} in {json_path}")
    arr = data[field]
    if not isinstance(arr, list):
        raise TypeError(f"Expected field {field} to be a list in {json_path}")
    return [float(x) for x in arr]


def _file_for(mode: str, prefix: str, cpd: int, out_dir: Path, template: str) -> Path:
    # template expects {prefix}, {mode}, {cpd}
    fname = template.format(prefix=prefix, mode=mode, cpd=cpd)
    return out_dir / fname


def main() -> None:
    parser = argparse.ArgumentParser(description="Plot a single field from bagsize outputs.")
    parser.add_argument("--field", required=True, help="JSON key to plot (e.g., avg_baggage_call).")
    parser.add_argument(
        "--bridges",
        nargs="+",
        default=["pb", "cgpb"],
        help="Bridge modes to plot (e.g., pb cgpb structural).",
    )
    parser.add_argument(
        "--cpds",
        nargs="+",
        type=int,
        default=[2, 3, 4, 5, 6],
        help="Checkpoint distances to plot.",
    )
    parser.add_argument(
        "--data-dir",
        default="bridges/output",
        help="Directory containing *_bagsize.json files.",
    )
    parser.add_argument(
        "--prefix",
        default="uber_",
        help="Prefix for filenames (e.g., uber_ for uber_pb_cpd{cpd}_bagsize.json).",
    )
    parser.add_argument(
        "--file-template",
        default="{prefix}{mode}_cpd{cpd}_bagsize.json",
        help="Filename template relative to --data-dir.",
    )
    parser.add_argument("--out", required=True, help="Output image/PDF path.")
    parser.add_argument("--legend-loc", default="upper left", help="Matplotlib legend location.")
    parser.add_argument("--jitter", type=float, default=0.08, help="Horizontal jitter for scatter points.")
    parser.add_argument("--scatter-alpha", type=float, default=0.22, help="Scatter alpha.")
    parser.add_argument(
        "--cloud-max-samples",
        type=int,
        default=100,
        help="Max number of samples to draw for the scatter cloud per cpd (per bridge). If omitted, plots all samples.",
    )
    parser.add_argument(
        "--x-offset-step",
        type=float,
        default=0.08,
        help="Per-bridge x-offset step for the solid dots and error bars (so series don't overlap).",
    )
    parser.add_argument(
        "--ymin",
        type=float,
        default=0.0,
        help="Minimum y-axis value (default: 0). Use something like -1 to let matplotlib autoscale below 0.",
    )
    parser.add_argument("--seed", type=int, default=42, help="Seed for jitter randomness.")
    args = parser.parse_args()

    out_dir = Path(args.data_dir)
    if not out_dir.exists():
        raise SystemExit(f"Data directory does not exist: {out_dir}")

    cpds: List[int] = list(args.cpds)
    field: str = args.field
    ylabel = FIELD_YLABEL.get(field, field)

    rng = np.random.default_rng(args.seed)

    fig, ax = plt.subplots(figsize=(11, 6))

    bridge_modes = list(args.bridges)
    n_series = len(bridge_modes)

    for series_idx, mode in enumerate(bridge_modes):
        if mode not in BRIDGE_META:
            raise SystemExit(f"Unknown bridge mode '{mode}'. Known: {sorted(BRIDGE_META.keys())}")

        label, color, marker = BRIDGE_META[mode]

        medians: List[float] = []
        iqr_lows: List[float] = []   # Q1
        iqr_highs: List[float] = []  # Q3

        # Offset each series slightly so solid dots + error bars don't land on the same x
        # (errorbar/line markers included).
        series_centering = (n_series - 1) / 2.0
        x_offset = (series_idx - series_centering) * float(args.x_offset_step)

        for cpd in cpds:
            json_path = _file_for(mode=mode, prefix=args.prefix, cpd=cpd, out_dir=out_dir, template=args.file_template)
            if not json_path.exists():
                raise SystemExit(f"Missing file for mode={mode} cpd={cpd}: {json_path}")

            values = _load_field_values(json_path, field=field)
            # Scatter: one point per trace sample
            if args.cloud_max_samples is not None and len(values) > args.cloud_max_samples:
                idxs = rng.choice(len(values), size=args.cloud_max_samples, replace=False)
                cloud_values = [values[i] for i in idxs]
            else:
                cloud_values = values

            x_jittered = (cpd + x_offset) + rng.uniform(-args.jitter, args.jitter, size=len(cloud_values))
            ax.scatter(x_jittered, cloud_values, s=14, alpha=args.scatter_alpha, color=color, marker=marker)

            median = float(np.median(values))
            q1 = float(np.quantile(values, 0.25))
            q3 = float(np.quantile(values, 0.75))
            medians.append(median)
            iqr_lows.append(q1)
            iqr_highs.append(q3)

        # Mean line with error bars
        x_positions = [cpd + x_offset for cpd in cpds]
        # Asymmetric IQR error bars around the median.
        lower_err = [m - q1 for m, q1 in zip(medians, iqr_lows)]
        upper_err = [q3 - m for m, q3 in zip(medians, iqr_highs)]
        ax.errorbar(
            x_positions,
            medians,
            yerr=[lower_err, upper_err],
            color=color,
            marker=marker,
            linewidth=2.2,
            capsize=5,
            label=label,
        )

    ax.set_xlabel("Checkpoint distance")
    ax.set_ylabel(ylabel)
    ax.set_ylim(bottom=args.ymin)
    ax.set_xticks(cpds)
    ax.grid(True, alpha=0.28)
    ax.legend(loc=args.legend_loc, fontsize=11)
    plt.tight_layout()
    plt.savefig(args.out, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Saved plot to {args.out}")


if __name__ == "__main__":
    main()

