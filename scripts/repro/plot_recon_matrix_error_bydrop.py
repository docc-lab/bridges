#!/usr/bin/env python3
"""Paper-sized reconstruction-error bars for a trace_recon JSON matrix.

The x-axis groups drop rates and each group contains CPD 3..8 in increasing
order.  One PDF and one 300-DPI PNG are emitted per bridge model.  Styling is
intentionally shared with plot_recon_error_bydrop.py.
"""

import argparse
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


MODELS = ("pb0", "cgp0", "sb3")
MODEL_LABELS = {"pb0": "P-Bridge", "cgp0": "CG-Bridge", "sb3": "S-Bridge"}
RATES = (0.05, 0.25, 0.5, 0.75, 0.95, 1.0)
CPDS = tuple(range(3, 9))
LABEL_FS, TICK_FS, LEG_FS = 9, 8, 7
OKABE = ("#0072B2", "#E69F00", "#009E73", "#D55E00", "#CC79A7", "#56B4E9")
HATCHES = ("////", "\\\\\\\\", "xxxx", "....", "++++", "oooo")
matplotlib.rcParams["hatch.linewidth"] = 0.3


def parse_args():
    parser = argparse.ArgumentParser(
        description="PB0/CGP0/SB3 reconstruction-error bars from a trace_recon matrix"
    )
    parser.add_argument("matrix", type=Path, help="directory containing MODEL_cpdN.json")
    parser.add_argument("outdir", type=Path, help="new figure output directory")
    parser.add_argument(
        "--metric",
        choices=("feasible", "all"),
        default="feasible",
        help="denominator: nonempty/feasible traces (paper default), or all traces",
    )
    parser.add_argument("--xlabel", default="Drop rate")
    parser.add_argument("--ylabel", default="Error (%)")
    return parser.parse_args()


def load_matrix(root):
    data = {}
    for model in MODELS:
        for cpd in CPDS:
            path = root / f"{model}_cpd{cpd}.json"
            doc = json.loads(path.read_text())
            if doc.get("mode") != model or doc.get("checkpoint_distance") != cpd:
                raise ValueError(f"configuration mismatch in {path}")
            if not doc.get("prime_m"):
                raise ValueError(f"expected prime-up results in {path}")
            by_rate = {float(item["drop_rate"]): item for item in doc["rate_summaries"]}
            missing = set(RATES) - set(by_rate)
            if missing:
                raise ValueError(f"missing drop rates {sorted(missing)} in {path}")
            data[model, cpd] = by_rate
    return data


def error_percent(item, metric):
    topo = item["topology_summary"]
    if metric == "feasible":
        numerator, denominator = topo["clean"], topo["feasible"]
    else:
        numerator, denominator = topo["clean_all"], topo["traces"]
    return 100.0 * (1.0 - numerator / denominator) if denominator else 0.0


def plot_model(model, data, metric, outdir, xlabel, ylabel, ymax):
    x = np.arange(len(RATES))
    width = 0.8 / len(CPDS)
    fig, ax = plt.subplots(figsize=(2.2, 1.2))
    for index, cpd in enumerate(CPDS):
        values = [error_percent(data[model, cpd][rate], metric) for rate in RATES]
        offset = (index - (len(CPDS) - 1) / 2) * width
        ax.bar(
            x + offset,
            values,
            width,
            label=str(cpd),
            color=OKABE[index],
            hatch=HATCHES[index],
            edgecolor="black",
            linewidth=0.4,
        )
    if xlabel:
        ax.set_xlabel(xlabel, fontsize=LABEL_FS)
    if ylabel:
        ax.set_ylabel(ylabel, fontsize=LABEL_FS)
    ax.set_xticks(x)
    ax.set_xticklabels([f"{rate:g}" for rate in RATES])
    ax.tick_params(axis="both", labelsize=TICK_FS, width=0.5, length=2.5, pad=1.5)
    ax.set_axisbelow(True)
    ax.yaxis.grid(True, color="0.88", linewidth=0.3)
    ax.set_ylim(0, ymax)
    step = 2 if ymax <= 14 else 5
    ax.set_yticks(np.arange(0, int(np.ceil(ymax)) + 1, step))
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_linewidth(0.5)
    legend = ax.legend(
        ncol=6,
        fontsize=LEG_FS,
        loc="upper left",
        frameon=True,
        columnspacing=0.4,
        handletextpad=0.2,
        borderpad=0.2,
        handlelength=0.6,
        handleheight=0.6,
    )
    legend.get_frame().set_linewidth(0.5)
    fig.tight_layout(pad=0.2)
    fig.patch.set_linewidth(0)
    stem = outdir / f"{model}_reconstruction_error_by_drop_rate_prime_up_first100k"
    for extension in ("pdf", "png"):
        path = stem.with_suffix(f".{extension}")
        fig.savefig(path, dpi=300, facecolor="white", edgecolor="none")
        print(f"wrote {path} ({MODEL_LABELS[model]})")
    plt.close(fig)


def main():
    args = parse_args()
    args.outdir.mkdir(parents=True, exist_ok=True)
    data = load_matrix(args.matrix)
    errors = [
        error_percent(data[model, cpd][rate], args.metric)
        for model in MODELS
        for cpd in CPDS
        for rate in RATES
    ]
    ymax = max(errors) * 1.08 if errors else 1.0
    for model in MODELS:
        plot_model(model, data, args.metric, args.outdir, args.xlabel, args.ylabel, ymax)


if __name__ == "__main__":
    main()
