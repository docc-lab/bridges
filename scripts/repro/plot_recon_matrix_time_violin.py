#!/usr/bin/env python3
"""Paper-sized per-trace reconstruction-time violins for PB0/CGP0/SB3.

The x-axis groups drop rates and each group contains CPD 3..8 in increasing
order.  KDEs are calculated in log10(ms) space, as in
plot_recon_time_violin.py.  All model figures share one y-axis range.
"""

import argparse
import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import numpy as np


MODELS = ("pb0", "cgp0", "sb3")
MODEL_LABELS = {"pb0": "P-Bridge", "cgp0": "CG-Bridge", "sb3": "S-Bridge"}
DROPS = (("0.05", "d005"), ("0.25", "d025"), ("0.5", "d05"),
         ("0.75", "d075"), ("0.95", "d095"), ("1", "d10"))
CPDS = tuple(range(3, 9))
COLORS = plt.cm.viridis(np.linspace(0.0, 0.9, len(CPDS)))
LABEL_FS, TICK_FS, LEG_FS = 9, 8, 7
SUBSAMPLE = 25_000
NGRID = 256
WIDTH = 0.135


def parse_args():
    parser = argparse.ArgumentParser(
        description="PB0/CGP0/SB3 reconstruction-time violins from per-trace CSVs"
    )
    parser.add_argument("timing_dir", type=Path, help="directory containing MODEL_cpdN_DROP.csv")
    parser.add_argument("outdir", type=Path, help="new figure output directory")
    parser.add_argument("--rebuild", action="store_true", help="ignore cached KDE curves")
    parser.add_argument(
        "--include-empty",
        action="store_true",
        help="include traces with no reconstruction obligation (paper default excludes them)",
    )
    parser.add_argument(
        "--expected-traces",
        type=int,
        default=100_000,
        help="required data-row count in every timing CSV (0 disables the check)",
    )
    parser.add_argument("--xlabel", default="Drop rate")
    parser.add_argument("--ylabel", default="Time (ms)")
    return parser.parse_args()


def read_distribution(path, include_empty, expected_traces, rng):
    values = []
    row_count = 0
    with path.open(newline="") as handle:
        rows = csv.reader(handle)
        header = next(rows, None)
        if header != ["tid", "survivors", "spans", "dropped", "feasible", "recon_ns"]:
            raise ValueError(f"unexpected timing header in {path}: {header}")
        for row in rows:
            if len(row) != 6:
                raise ValueError(f"malformed timing row in {path}: {row}")
            row_count += 1
            if include_empty or row[4] == "1":
                ns = int(row[5])
                if ns > 0:
                    values.append(ns)
    if expected_traces and row_count != expected_traces:
        raise ValueError(
            f"expected {expected_traces} timing rows in {path}, found {row_count}"
        )
    if not values:
        raise ValueError(f"no selected timing samples in {path}")
    array = np.asarray(values, dtype=np.float64) / 1e6
    if len(array) > SUBSAMPLE:
        array = rng.choice(array, SUBSAMPLE, replace=False)
    return np.log10(array)


def kde(values):
    low, high = float(values.min()), float(values.max())
    pad = 0.05 * (high - low + 1e-9)
    grid = np.linspace(low - pad, high + pad, NGRID)
    std = values.std(ddof=1) if len(values) > 1 else 1.0
    q75, q25 = np.percentile(values, [75, 25])
    iqr = q75 - q25
    sigma = min(std, iqr / 1.349) if iqr > 0 else std
    if sigma <= 0:
        sigma = 1e-3
    bandwidth = max(0.9 * sigma * len(values) ** (-0.2), 1e-9)
    scaled = (grid[:, None] - values[None, :]) / bandwidth
    density = np.exp(-0.5 * scaled * scaled).sum(1)
    density /= len(values) * bandwidth * np.sqrt(2 * np.pi)
    return grid, density, float(np.median(values))


def load_curves(model, args):
    subset = "all" if args.include_empty else "feasible"
    cache_path = args.timing_dir / f"{model}_violin_cache_{subset}.npz"
    if cache_path.exists() and not args.rebuild:
        saved = np.load(cache_path)
        return {key: saved[key] for key in saved.files}

    rng = np.random.default_rng(0)
    curves = {}
    for _, drop_code in DROPS:
        for cpd in CPDS:
            path = args.timing_dir / f"{model}_cpd{cpd}_{drop_code}.csv"
            values = read_distribution(
                path, args.include_empty, args.expected_traces, rng
            )
            grid, density, median = kde(values)
            key = f"{drop_code}_c{cpd}"
            curves[f"{key}|grid"] = grid
            curves[f"{key}|density"] = density
            curves[f"{key}|median"] = np.asarray([median])
    np.savez(cache_path, **curves)
    return curves


def plot_model(model, curves, args, ylow, yhigh, data_high):
    fig, ax = plt.subplots(figsize=(2.2, 1.2))
    half_width = WIDTH * 0.95 / 2
    for drop_index, (_, drop_code) in enumerate(DROPS):
        for cpd_index, cpd in enumerate(CPDS):
            key = f"{drop_code}_c{cpd}"
            grid = curves[f"{key}|grid"]
            density = curves[f"{key}|density"]
            median = float(curves[f"{key}|median"][0])
            position = drop_index + (cpd_index - (len(CPDS) - 1) / 2) * WIDTH
            width = density / density.max() * half_width
            ax.fill_betweenx(
                grid,
                position - width,
                position + width,
                facecolor=COLORS[cpd_index],
                edgecolor="black",
                linewidth=0.2,
                alpha=0.85,
            )
            median_width = np.interp(median, grid, density) / density.max() * half_width
            ax.hlines(median, position - median_width, position + median_width,
                      color="black", linewidth=0.5)

    ax.set_xticks(range(len(DROPS)))
    ax.set_xticklabels([label for label, _ in DROPS])
    ax.set_xlim(-0.45, len(DROPS) - 1 + 0.45)
    if args.xlabel:
        ax.set_xlabel(args.xlabel, fontsize=LABEL_FS)
    if args.ylabel:
        ax.set_ylabel(args.ylabel, fontsize=LABEL_FS)
    ax.set_ylim(ylow, yhigh)
    ticks = list(range(int(np.ceil(ylow)), int(np.floor(data_high)) + 1))
    if len(ticks) > 6:
        ticks = ticks[::2]
    ax.set_yticks(ticks)
    ax.set_yticklabels([f"$10^{{{tick}}}$" for tick in ticks])
    ax.tick_params(axis="both", labelsize=TICK_FS, width=0.5, length=2.5, pad=1.5)
    for spine in ax.spines.values():
        spine.set_linewidth(0.5)
    ax.set_axisbelow(True)
    ax.yaxis.grid(True, color="0.88", linewidth=0.3)
    handles = [
        Patch(facecolor=COLORS[index], edgecolor="black", linewidth=0.4, label=str(cpd))
        for index, cpd in enumerate(CPDS)
    ]
    legend = ax.legend(
        handles=handles,
        fontsize=LEG_FS,
        ncol=6,
        loc="upper left",
        borderaxespad=0.15,
        columnspacing=0.4,
        handletextpad=0.2,
        borderpad=0.2,
        handlelength=0.6,
        handleheight=0.6,
    )
    legend.get_frame().set_linewidth(0.5)
    fig.tight_layout(pad=0.2)
    fig.patch.set_linewidth(0)
    stem = args.outdir / f"{model}_reconstruction_time_violin_prime_up_first100k"
    for extension in ("pdf", "png"):
        path = stem.with_suffix(f".{extension}")
        fig.savefig(path, dpi=300, facecolor="white", edgecolor="none")
        print(f"wrote {path} ({MODEL_LABELS[model]})")
    plt.close(fig)


def main():
    args = parse_args()
    args.outdir.mkdir(parents=True, exist_ok=True)
    curves = {model: load_curves(model, args) for model in MODELS}
    grids = [
        value
        for model_curves in curves.values()
        for key, value in model_curves.items()
        if key.endswith("|grid")
    ]
    data_low = min(float(grid.min()) for grid in grids)
    data_high = max(float(grid.max()) for grid in grids)
    ylow = data_low - 0.05
    yhigh = data_high + 1.15
    for model in MODELS:
        plot_model(model, curves[model], args, ylow, yhigh, data_high)


if __name__ == "__main__":
    main()
