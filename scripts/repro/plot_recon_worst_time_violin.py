#!/usr/bin/env python3
"""Compare each bridge's empirically slowest drop-rate timing group.

For each bridge, the selected drop rate maximizes the geometric mean of the
six exact per-CPD median reconstruction times.  This gives every CPD equal
weight and avoids allowing long-tail outliers or CPD-specific feasible-trace
counts to determine which group is called "worst".  Violin KDEs use the same
log10(ms) method and deterministic subsampling as the full-matrix plotter.
"""

import argparse
import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import numpy as np

from plot_recon_matrix_time_violin import (
    COLORS,
    CPDS,
    DROPS,
    MODEL_LABELS,
    MODELS,
    load_curves,
)


LABEL_FS, TICK_FS, LEG_FS = 9, 8, 7
WIDTH = 0.135


def parse_args():
    parser = argparse.ArgumentParser(
        description="Worst-drop timing violins for P-/CG-/S-Bridge"
    )
    parser.add_argument("timing_dir", type=Path)
    parser.add_argument("outdir", type=Path)
    parser.add_argument("--rebuild", action="store_true")
    parser.add_argument(
        "--include-empty",
        action="store_true",
        help="include traces with no reconstruction obligation",
    )
    parser.add_argument("--expected-traces", type=int, default=100_000)
    return parser.parse_args()


def exact_median_ms(path, include_empty, expected_traces):
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
        raise ValueError(f"expected {expected_traces} rows in {path}, found {row_count}")
    if not values:
        raise ValueError(f"no selected timing samples in {path}")
    return float(np.median(np.asarray(values, dtype=np.float64))) / 1e6


def select_worst_groups(args):
    rankings = {}
    selected = {}
    for model in MODELS:
        model_rows = []
        for drop_label, drop_code in DROPS:
            medians = np.asarray(
                [
                    exact_median_ms(
                        args.timing_dir / f"{model}_cpd{cpd}_{drop_code}.csv",
                        args.include_empty,
                        args.expected_traces,
                    )
                    for cpd in CPDS
                ]
            )
            score = float(np.exp(np.mean(np.log(medians))))
            model_rows.append((score, drop_label, drop_code, medians))
        model_rows.sort(reverse=True, key=lambda row: row[0])
        rankings[model] = model_rows
        selected[model] = model_rows[0]
    return selected, rankings


def plot(selected, curves, args):
    selected_grids = [
        curves[model][f"{selected[model][2]}_c{cpd}|grid"]
        for model in MODELS
        for cpd in CPDS
    ]
    data_low = min(float(grid.min()) for grid in selected_grids)
    data_high = max(float(grid.max()) for grid in selected_grids)
    ylow = data_low - 0.05
    yhigh = data_high + 1.15

    fig, ax = plt.subplots(figsize=(2.6, 1.4))
    half_width = WIDTH * 0.95 / 2
    for model_index, model in enumerate(MODELS):
        _, _, drop_code, _ = selected[model]
        for cpd_index, cpd in enumerate(CPDS):
            key = f"{drop_code}_c{cpd}"
            grid = curves[model][f"{key}|grid"]
            density = curves[model][f"{key}|density"]
            median = float(curves[model][f"{key}|median"][0])
            position = model_index + (cpd_index - (len(CPDS) - 1) / 2) * WIDTH
            violin_width = density / density.max() * half_width
            ax.fill_betweenx(
                grid,
                position - violin_width,
                position + violin_width,
                facecolor=COLORS[cpd_index],
                edgecolor="black",
                linewidth=0.2,
                alpha=0.85,
            )
            median_width = np.interp(median, grid, density) / density.max() * half_width
            ax.hlines(
                median,
                position - median_width,
                position + median_width,
                color="black",
                linewidth=0.5,
            )

    ax.set_xticks(range(len(MODELS)))
    ax.set_xticklabels(
        [
            f"{MODEL_LABELS[model]}\n$d={selected[model][1]}$"
            for model in MODELS
        ]
    )
    ax.set_xlim(-0.48, len(MODELS) - 1 + 0.48)
    ax.set_ylabel("Time (ms)", fontsize=LABEL_FS)
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
        Patch(
            facecolor=COLORS[index],
            edgecolor="black",
            linewidth=0.4,
            label=str(cpd),
        )
        for index, cpd in enumerate(CPDS)
    ]
    legend = ax.legend(
        handles=handles,
        title="CPD",
        fontsize=LEG_FS,
        title_fontsize=LEG_FS,
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

    stem = args.outdir / "bridge_reconstruction_time_violin_worst_drop_groups_prime_up_first100k"
    for extension in ("pdf", "png"):
        path = stem.with_suffix(f".{extension}")
        fig.savefig(path, dpi=300, facecolor="white", edgecolor="none")
        print(f"wrote {path}")
    plt.close(fig)


def main():
    args = parse_args()
    args.outdir.mkdir(parents=True, exist_ok=True)
    selected, rankings = select_worst_groups(args)
    for model in MODELS:
        score, drop_label, _, medians = selected[model]
        print(
            f"{MODEL_LABELS[model]}: selected d={drop_label}; "
            f"CPD medians ms={','.join(f'{value:.6g}' for value in medians)}; "
            f"geometric mean={score:.6g} ms"
        )
        print(
            "  all group scores (ms): "
            + ", ".join(f"d={row[1]}:{row[0]:.6g}" for row in rankings[model])
        )
    curves = {model: load_curves(model, args) for model in MODELS}
    plot(selected, curves, args)


if __name__ == "__main__":
    main()
