#!/usr/bin/env python3
"""Paper-sized reconstruction-accuracy bars for a trace_recon JSON matrix.

The x-axis groups drop rates; each group contains CPD 3..8 in increasing
order. One PDF and one 300-DPI PNG are emitted per bridge model.
"""

import argparse
import json
import math
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
HATCHES = ("////", "\\\\", "xxxx", "....", "++++", "oooo")
matplotlib.rcParams["hatch.linewidth"] = 0.3


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("matrix", type=Path, help="directory containing MODEL_cpdN.json")
    parser.add_argument("outdir", type=Path, help="new figure output directory")
    parser.add_argument(
        "--metric",
        choices=("feasible", "all"),
        default="feasible",
        help="denominator: nonempty/feasible traces (paper default), or all traces",
    )
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


def accuracy(item, metric):
    topo = item["topology_summary"]
    if metric == "feasible":
        numerator, denominator = topo["clean"], topo["feasible"]
    else:
        numerator, denominator = topo["clean_all"], topo["traces"]
    value = 100.0 * numerator / denominator if denominator else 100.0
    # Normal-approximation 95% binomial interval. At this corpus size the bars
    # are small, but retaining them makes the sampling uncertainty explicit.
    proportion = numerator / denominator if denominator else 1.0
    error = 100.0 * 1.96 * math.sqrt(proportion * (1.0 - proportion) / denominator) if denominator else 0.0
    return value, error


def plot_model(model, data, metric, outdir, ymin):
    x = np.arange(len(RATES))
    width = 0.8 / len(CPDS)
    fig, ax = plt.subplots(figsize=(2.45, 1.55))
    for index, cpd in enumerate(CPDS):
        points = [accuracy(data[model, cpd][rate], metric) for rate in RATES]
        values = [point[0] for point in points]
        errors = [point[1] for point in points]
        offset = (index - (len(CPDS) - 1) / 2) * width
        ax.bar(
            x + offset,
            values,
            width,
            yerr=errors,
            label=str(cpd),
            color=OKABE[index],
            hatch=HATCHES[index],
            edgecolor="black",
            linewidth=0.4,
            capsize=1.0,
            error_kw={"linewidth": 0.35, "capthick": 0.35},
        )
    ax.set_xlabel("Drop rate", fontsize=LABEL_FS)
    ax.set_ylabel("Correct traces (%)", fontsize=LABEL_FS)
    ax.set_xticks(x)
    ax.set_xticklabels([f"{rate:g}" for rate in RATES])
    ax.tick_params(axis="both", labelsize=TICK_FS, width=0.5, length=2.5, pad=1.5)
    ax.set_axisbelow(True)
    ax.yaxis.grid(True, color="0.88", linewidth=0.3)
    ax.set_ylim(ymin, 100.15)
    ax.set_yticks(np.arange(ymin, 101, 2))
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_linewidth(0.5)
    legend = ax.legend(
        title="CPD",
        title_fontsize=LEG_FS,
        ncol=6,
        fontsize=LEG_FS,
        loc="lower left",
        frameon=True,
        columnspacing=0.35,
        handletextpad=0.15,
        borderpad=0.2,
        handlelength=0.55,
        handleheight=0.55,
    )
    legend.get_frame().set_linewidth(0.5)
    fig.tight_layout(pad=0.25)
    stem = outdir / f"{model}_reconstruction_accuracy_by_drop_rate_prime_up_first100k"
    for extension in ("pdf", "png"):
        path = stem.with_suffix(f".{extension}")
        fig.savefig(path, dpi=300, facecolor="white", edgecolor="none")
        print(f"wrote {path} ({MODEL_LABELS[model]})")
    plt.close(fig)


def main():
    args = parse_args()
    args.outdir.mkdir(parents=True, exist_ok=True)
    data = load_matrix(args.matrix)
    values = [
        accuracy(data[model, cpd][rate], args.metric)[0]
        for model in MODELS
        for cpd in CPDS
        for rate in RATES
    ]
    ymin = max(0, int(math.floor(min(values))) - 1)
    if ymin % 2:
        ymin -= 1
    for model in MODELS:
        plot_model(model, data, args.metric, args.outdir, ymin)


if __name__ == "__main__":
    main()
