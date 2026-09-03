#!/usr/bin/env python3
"""Render paper figures for the CGP0 fanout-correlation experiment."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import numpy as np


CPDS = np.arange(3, 9)
BLUE = "#0072B2"
ORANGE = "#E69F00"
GREEN = "#009E73"
RED = "#D55E00"
PURPLE = "#CC79A7"
SKY = "#56B4E9"
LABEL_FS, TICK_FS, LEG_FS = 9, 8, 7
NGRID = 256
SUBSAMPLE = 25_000


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "experiment",
        type=Path,
        help="directory containing fanout_correlation_cells.csv and timing/",
    )
    parser.add_argument(
        "--outdir",
        type=Path,
        help="figure directory (default: EXPERIMENT/figures)",
    )
    parser.add_argument(
        "--skip-timing",
        action="store_true",
        help="render evidence figures even if timing CSVs are not ready",
    )
    return parser.parse_args()


def load_cells(path: Path) -> list[dict[str, float]]:
    with path.open(newline="") as handle:
        rows = [{key: float(value) for key, value in row.items()} for row in csv.DictReader(handle)]
    if [int(row["cpd"]) for row in rows] != CPDS.tolist():
        raise ValueError(f"expected CPD 3..8 in {path}")
    return rows


def pearson(xs: np.ndarray, ys: np.ndarray) -> float:
    return float(np.corrcoef(xs, ys)[0, 1])


def binomial_halfwidth(successes: np.ndarray, totals: np.ndarray) -> np.ndarray:
    proportions = successes / totals
    return 1.96 * np.sqrt(proportions * (1.0 - proportions) / totals)


def style_axis(ax: plt.Axes, grid: bool = True) -> None:
    ax.tick_params(axis="both", labelsize=TICK_FS, width=0.5, length=2.5, pad=1.5)
    if grid:
        ax.set_axisbelow(True)
        ax.yaxis.grid(True, color="0.88", linewidth=0.4)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_linewidth(0.5)


def save(fig: plt.Figure, stem: Path, rect: tuple[float, float, float, float] | None = None) -> None:
    fig.tight_layout(pad=0.35, rect=rect)
    for extension in ("pdf", "png"):
        path = stem.with_suffix(f".{extension}")
        fig.savefig(path, dpi=300, facecolor="white", edgecolor="none")
        print(f"wrote {path}")
    plt.close(fig)


def plot_cpd_trends(rows: list[dict[str, float]], outdir: Path) -> None:
    clean = np.asarray([row["clean_rate_pct"] for row in rows])
    clean_n = np.asarray([row["traces"] for row in rows])
    clean_success = np.asarray([row["clean_traces"] for row in rows])
    clean_ci = 100.0 * binomial_halfwidth(clean_success, clean_n)

    fig, (top, middle, bottom) = plt.subplots(
        3,
        1,
        figsize=(4.15, 3.25),
        sharex=True,
        gridspec_kw={"height_ratios": [1, 0.9, 1.1]},
    )
    top.errorbar(
        CPDS,
        clean,
        yerr=clean_ci,
        color=BLUE,
        marker="o",
        markersize=3.5,
        linewidth=1.0,
        capsize=2,
    )
    top.set_ylabel("Clean traces (%)", fontsize=LABEL_FS)
    top.set_ylim(min(clean - clean_ci) - 0.7, max(clean + clean_ci) + 0.7)
    style_axis(top)

    middle.plot(
        CPDS,
        [row["truth_fanouts_per_window"] for row in rows],
        color=RED,
        marker="o",
        markersize=3.2,
        linewidth=0.9,
    )
    middle.set_ylabel("True fanouts\nper window", fontsize=LABEL_FS)
    style_axis(middle)

    series = (
        ("Applicable fanout groups", "applicable_fanout_groups_per_route", ORANGE, "s"),
        ("Required HA fanouts", "required_ha_per_route", GREEN, "^"),
        ("Multi-Bloom fanout groups", "multi_bloom_fanout_groups_per_route", PURPLE, "D"),
    )
    lines = []
    for label, key, color, marker in series:
        line, = bottom.plot(
            CPDS,
            [row[key] for row in rows],
            label=label,
            color=color,
            marker=marker,
            markersize=3.2,
            linewidth=0.9,
        )
        lines.append((line, label, rows[-1][key]))
    for line, label, value in lines:
        bottom.annotate(
            label,
            (8, value),
            xytext=(5, 0),
            textcoords="offset points",
            color=line.get_color(),
            fontsize=LEG_FS,
            va="center",
        )
    bottom.set_xlabel("Checkpoint distance", fontsize=LABEL_FS)
    bottom.set_ylabel("Mean per route", fontsize=LABEL_FS)
    bottom.set_xticks(CPDS)
    bottom.set_xlim(2.75, 9.35)
    style_axis(bottom)
    save(
        fig,
        outdir / "cgp0_fanout_evidence_cpd_trends_prime_up_drop1_first100k",
    )


def plot_correlation(rows: list[dict[str, float]], outdir: Path) -> None:
    groups = np.asarray([row["applicable_fanout_groups_per_route"] for row in rows])
    clean = np.asarray([row["clean_rate_pct"] for row in rows])
    clean_n = np.asarray([row["traces"] for row in rows])
    clean_success = np.asarray([row["clean_traces"] for row in rows])
    clean_ci = 100.0 * binomial_halfwidth(clean_success, clean_n)
    fit = np.polyfit(groups, clean, 1)
    xline = np.linspace(groups.min() - 0.002, groups.max() + 0.002, 100)

    fig, ax = plt.subplots(figsize=(2.55, 1.9))
    ax.plot(xline, np.polyval(fit, xline), color="0.35", linewidth=0.8, zorder=1)
    ax.errorbar(
        groups,
        clean,
        yerr=clean_ci,
        fmt="o",
        color=BLUE,
        markeredgecolor="black",
        markeredgewidth=0.35,
        markersize=4,
        capsize=1.8,
        linewidth=0.6,
        zorder=2,
    )
    offsets = ((3, 2), (3, -9), (3, 2), (3, -9), (-14, 3), (3, -9))
    for cpd, x, y, offset in zip(CPDS, groups, clean, offsets):
        ax.annotate(str(cpd), (x, y), xytext=offset, textcoords="offset points", fontsize=LEG_FS)
    ax.text(
        0.04,
        0.95,
        f"Pearson $r={pearson(groups, clean):.3f}$",
        transform=ax.transAxes,
        ha="left",
        va="top",
        fontsize=LEG_FS,
    )
    ax.set_xlabel("Applicable fanout groups per route", fontsize=LABEL_FS)
    ax.set_ylabel("Clean traces (%)", fontsize=LABEL_FS)
    style_axis(ax)
    save(
        fig,
        outdir / "cgp0_fanout_groups_accuracy_correlation_prime_up_drop1_first100k",
    )


def split_error(rows: list[dict[str, float]], prefix: str) -> tuple[np.ndarray, ...]:
    zero_n = np.asarray([row[f"{prefix}_zero_routes"] for row in rows])
    zero_e = np.asarray([row[f"{prefix}_zero_errors"] for row in rows])
    pos_n = np.asarray([row[f"{prefix}_positive_routes"] for row in rows])
    pos_e = np.asarray([row[f"{prefix}_positive_errors"] for row in rows])
    return (
        100.0 * zero_e / zero_n,
        100.0 * binomial_halfwidth(zero_e, zero_n),
        100.0 * pos_e / pos_n,
        100.0 * binomial_halfwidth(pos_e, pos_n),
    )


def plot_conditional_errors(rows: list[dict[str, float]], outdir: Path) -> None:
    panels = (
        ("Required HA", "required_ha"),
        ("Applicable fanout group", "fanout_groups"),
    )
    fig, axes = plt.subplots(1, 2, figsize=(4.8, 2.1), sharey=True)
    x = np.arange(len(CPDS))
    width = 0.36
    all_values = []
    for ax, (title, prefix) in zip(axes, panels):
        zero, zero_ci, positive, positive_ci = split_error(rows, prefix)
        all_values.extend((zero + zero_ci).tolist())
        all_values.extend((positive + positive_ci).tolist())
        ax.bar(
            x - width / 2,
            zero,
            width,
            yerr=zero_ci,
            color=SKY,
            edgecolor="black",
            linewidth=0.4,
            hatch="////",
            capsize=1.5,
            error_kw={"linewidth": 0.45, "capthick": 0.45},
        )
        ax.bar(
            x + width / 2,
            positive,
            width,
            yerr=positive_ci,
            color=ORANGE,
            edgecolor="black",
            linewidth=0.4,
            hatch="....",
            capsize=1.5,
            error_kw={"linewidth": 0.45, "capthick": 0.45},
        )
        ax.set_title(title, fontsize=LABEL_FS, pad=2)
        ax.set_xlabel("Checkpoint distance", fontsize=LABEL_FS)
        ax.set_xticks(x)
        ax.set_xticklabels(CPDS)
        style_axis(ax)
    axes[0].set_ylabel("Route topology error (%)", fontsize=LABEL_FS)
    axes[0].set_ylim(0, max(all_values) * 1.12)
    legend = fig.legend(
        handles=(
            Patch(facecolor=SKY, edgecolor="black", linewidth=0.4, hatch="////", label="Evidence absent"),
            Patch(facecolor=ORANGE, edgecolor="black", linewidth=0.4, hatch="....", label="$\\geq$1 evidence item"),
        ),
        ncol=2,
        fontsize=LEG_FS,
        loc="upper center",
        bbox_to_anchor=(0.5, 1.0),
        frameon=True,
        borderpad=0.2,
    )
    legend.get_frame().set_linewidth(0.5)
    save(
        fig,
        outdir / "cgp0_route_error_by_fanout_evidence_prime_up_drop1_first100k",
        rect=(0, 0, 1, 0.88),
    )


def read_timing(path: Path, rng: np.random.Generator) -> np.ndarray:
    values = []
    rows = 0
    with path.open(newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)
        expected = ["tid", "survivors", "spans", "dropped", "feasible", "recon_ns"]
        if header != expected:
            raise ValueError(f"unexpected timing header in {path}: {header}")
        for row in reader:
            rows += 1
            if row[4] == "1" and int(row[5]) > 0:
                values.append(int(row[5]) / 1e6)
    if rows != 100_000:
        raise ValueError(f"expected 100000 rows in {path}, found {rows}")
    array = np.asarray(values, dtype=np.float64)
    if len(array) > SUBSAMPLE:
        array = rng.choice(array, SUBSAMPLE, replace=False)
    return np.log10(array)


def kde(values: np.ndarray) -> tuple[np.ndarray, np.ndarray, float]:
    low, high = float(values.min()), float(values.max())
    pad = 0.05 * (high - low + 1e-9)
    grid = np.linspace(low - pad, high + pad, NGRID)
    std = values.std(ddof=1) if len(values) > 1 else 1.0
    q75, q25 = np.percentile(values, [75, 25])
    iqr = q75 - q25
    sigma = min(std, iqr / 1.349) if iqr > 0 else std
    bandwidth = max(0.9 * sigma * len(values) ** (-0.2), 1e-9)
    density = np.exp(-0.5 * ((grid[:, None] - values[None, :]) / bandwidth) ** 2).sum(1)
    density /= len(values) * bandwidth * np.sqrt(2.0 * np.pi)
    return grid, density, float(np.median(values))


def plot_timing(experiment: Path, outdir: Path) -> None:
    timing = experiment / "timing"
    rng = np.random.default_rng(0)
    curves = []
    for cpd in CPDS:
        path = timing / f"cgp0_prime_up_drop1_cpd{cpd}.csv"
        curves.append(kde(read_timing(path, rng)))

    fig, ax = plt.subplots(figsize=(2.8, 1.9))
    colors = plt.cm.viridis(np.linspace(0.05, 0.9, len(CPDS)))
    half_width = 0.34
    for position, color, (grid, density, median) in zip(CPDS, colors, curves):
        width = density / density.max() * half_width
        ax.fill_betweenx(
            grid,
            position - width,
            position + width,
            facecolor=color,
            edgecolor="black",
            linewidth=0.3,
            alpha=0.88,
        )
        median_width = np.interp(median, grid, density) / density.max() * half_width
        ax.hlines(median, position - median_width, position + median_width, color="black", linewidth=0.7)
    low = min(float(curve[0].min()) for curve in curves)
    high = max(float(curve[0].max()) for curve in curves)
    ticks = np.arange(np.ceil(low), np.floor(high) + 1)
    ax.set_yticks(ticks)
    ax.set_yticklabels([f"$10^{{{int(tick)}}}$" for tick in ticks])
    ax.set_xticks(CPDS)
    ax.set_xlabel("Checkpoint distance", fontsize=LABEL_FS)
    ax.set_ylabel("Reconstruction time (ms)", fontsize=LABEL_FS)
    style_axis(ax)
    save(
        fig,
        outdir / "cgp0_reconstruction_time_violin_prime_up_drop1_first100k",
    )


def main() -> None:
    args = parse_args()
    outdir = args.outdir or args.experiment / "figures"
    outdir.mkdir(parents=True, exist_ok=True)
    rows = load_cells(args.experiment / "fanout_correlation_cells.csv")
    plot_cpd_trends(rows, outdir)
    plot_correlation(rows, outdir)
    plot_conditional_errors(rows, outdir)
    if not args.skip_timing:
        plot_timing(args.experiment, outdir)


if __name__ == "__main__":
    main()
