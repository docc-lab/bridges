#!/usr/bin/env python3
"""cgp2 reconstruction error (= 100 - exclEmpty = FP rate) grouped by drop rate,
one bar per cpd. Full-corpus sweep; day1+day2 pooled by a feasible-weighted
average: err = 100 * (1 - (clean_d1+clean_d2)/(feas_d1+feas_d2)). No titles,
large fonts. Timing-rerun dumps; only complete drop rates plotted."""
import os, re, argparse
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

SC = "/tmp/claude-36637/-users-tomislav/9c554978-de8c-42cf-bd6a-bbbe3b07797f/scratchpad"
RATES = ["0.05", "0.25", "0.5", "0.75", "0.95", "1.0"]
CPDS = [3, 4, 5, 6, 7, 8]
SUFFIX = {"0.05": "_d005", "0.25": "_d025", "0.5": "_d05", "0.75": "_d075", "0.95": "_d095", "1.0": "_d10"}
FINAL = re.compile(r"feasible=(\d+) empty=\d+ \| correct=[\d.]+% \(clean=(\d+)\)")
LABEL_FS, TICK_FS, LEG_FS = 9, 8, 7  # typical paper text sizes for a 2.2x1.2in column figure
# Colorblind-safe (Okabe-Ito) + distinct hatch per cpd = redundant encoding, so
# bars are distinguishable by pattern even in grayscale / for CVD readers.
OKABE = ["#0072B2", "#E69F00", "#009E73", "#D55E00", "#CC79A7", "#56B4E9"]
HATCHES = ["/", "\\", "x", ".", "+", "o"]  # single-pass = bigger cells (same patterns)
matplotlib.rcParams["hatch.linewidth"] = 0.4  # thin hatches for the small canvas

ap = argparse.ArgumentParser(description="cgp2 reconstruction-error (FP-rate) bars (2.2x1.2in paper figure)")
ap.add_argument("--xlabel", default=None, help="x-axis label text (omit for no label)")
ap.add_argument("--ylabel", default=None, help="y-axis label text (omit for no label)")
args = ap.parse_args()

def _drop_ok(mode, drop):
    for day in ("day1", "day2"):
        for cpd in CPDS:
            f = f"{SC}/tim_{day}_{mode}_c{cpd}{SUFFIX[drop]}.txt"
            if not os.path.exists(f) or not FINAL.search(open(f).read()):
                return False
    return True

def _cf(day, mode, drop, cpd):
    f = f"{SC}/tim_{day}_{mode}_c{cpd}{SUFFIX[drop]}.txt"
    t = open(f).read()
    # Match the final CGP2 summary line (not the PROGRESS lines, which also have feasible=).
    m = FINAL.search(t)
    return int(m.group(2)), int(m.group(1))  # clean, feas

def err(mode, drop, cpd):  # pooled FP rate across both days, feasible-weighted
    c1, f1 = _cf("day1", mode, drop, cpd)
    c2, f2 = _cf("day2", mode, drop, cpd)
    return 100.0 * (1.0 - (c1 + c2) / (f1 + f2))

def _ymax():
    m = 0.0
    for mode in ("up", "down", "none"):
        for r in RATES:
            if not _drop_ok(mode, r):
                continue
            for c in CPDS:
                m = max(m, err(mode, r, c))
    return m * 1.08 if m else 1.0

def plot_bydrop(mode, outfile, ymax):
    rates = [r for r in RATES if _drop_ok(mode, r)]
    if not rates:
        print("skip", mode, "(no complete drops)"); return
    data = {c: [err(mode, r, c) for r in rates] for c in CPDS}
    n = len(CPDS); x = np.arange(len(rates)); bw = 0.8 / n
    fig, ax = plt.subplots(figsize=(2.2, 1.2))
    for i, c in enumerate(CPDS):
        off = (i - (n - 1) / 2) * bw
        ax.bar(x + off, data[c], bw, label=str(c), color=OKABE[i],
               hatch=HATCHES[i], edgecolor="black", linewidth=0.4)
    if args.xlabel:
        ax.set_xlabel(args.xlabel, fontsize=LABEL_FS)
    if args.ylabel:
        ax.set_ylabel(args.ylabel, fontsize=LABEL_FS)
    ax.set_xticks(x); ax.set_xticklabels(rates)
    ax.tick_params(axis="both", labelsize=TICK_FS, width=0.5, length=2.5, pad=1.5)
    ax.set_axisbelow(True)
    ax.yaxis.grid(True, color="0.88", linewidth=0.3)
    ax.set_ylim(0, ymax)
    ax.set_yticks(np.arange(0, int(np.ceil(ymax)) + 1, 2))
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_linewidth(0.5)
    leg = ax.legend(ncol=6, fontsize=LEG_FS, loc="upper left", frameon=True,
                    columnspacing=0.4, handletextpad=0.2, borderpad=0.2,
                    handlelength=0.6, handleheight=0.6)  # one row, small square swatches
    leg.get_frame().set_linewidth(0.5)  # thin frame box (default is heavy at this scale)
    fig.tight_layout(pad=0.2)
    fig.patch.set_linewidth(0)  # kill the figure-frame rectangle (gray border in PDF)
    fig.savefig(outfile, dpi=300, facecolor="white", edgecolor="none")  # exact 2.2x1.2 canvas
    print("wrote", outfile, f"({mode}, drops: {', '.join(rates)})")

for ext in ("pdf", "png"):
    ymax = _ymax()
    plot_bydrop("up", f"/users/tomislav/cgp2_err_primeup_bydrop.{ext}", ymax)
    plot_bydrop("down", f"/users/tomislav/cgp2_err_primedown_bydrop.{ext}", ymax)
    plot_bydrop("none", f"/users/tomislav/cgp2_err_noprime_bydrop.{ext}", ymax)
