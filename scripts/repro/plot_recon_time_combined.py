#!/usr/bin/env python3
"""One condensed reconstruction-time figure across all three bridge types.

3 bridge types x 6 cpds = 18 violins on a single shared log10(ms) y-axis,
clustered by bridge type. Each type is drawn in its own sequential colormap
(grayscale-distinguishable light->dark across cpd, CVD-distinct hue between
types; clusters are also spatially separated). Each type is shown at *its own
most-expensive drop rate* (the drop with the highest median recon time), which
is reported in the legend. day1+day2 pooled; feasible subset.

Timing sources (override via env):
  PB_TD  (default $RM/pb2_timing)   -- cols tid,survivors,spans,dropped,feasible,recon_ns
  CGP_TD (default $RM/cgp2_timing)  -- same
  SB_TD  (default sbridge_timing)   -- cols topo_ns,struct_ns  (recon = topo+struct)
"""
import csv, os, sys
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Patch

CPDS = [3, 4, 5, 6, 7, 8]
DROPS = ["d005", "d025", "d05", "d075", "d095", "d10"]
DROPLBL = {"d005": "0.05", "d025": "0.25", "d05": "0.5", "d075": "0.75", "d095": "0.95", "d10": "1.0"}
RM = "/mydata/recon_multidrop"
SB_DEFAULT = "/mydata/uber/bignode_state/sbridge_timing"
# (label, kind, timing dir, colormap)
TYPES = [
    ("PB",       "recon", os.environ.get("PB_TD",  f"{RM}/pb2_timing"),  "Blues"),
    ("CGP",      "recon", os.environ.get("CGP_TD", f"{RM}/cgp2_timing"), "Oranges"),
    ("S-Bridge", "sb",    os.environ.get("SB_TD",  SB_DEFAULT),          "Purples"),
]
LABEL_FS, TICK_FS, CLUS_FS = 13, 11, 12
SUB, NGRID = 25000, 256
rng = np.random.default_rng(0)

_CACHE = {}
def load(kind, tdir, cpd, dc):
    key = (kind, tdir, cpd, dc)
    if key in _CACHE:
        return _CACHE[key]
    vals = []
    for day in ("day1", "day2"):
        if kind == "recon":
            f = f"{tdir}/timing_{day}_up_c{cpd}_{dc}.csv"  # timing is prime-independent; use up
            try:
                with open(f) as fh:
                    r = csv.reader(fh); next(r, None)
                    for row in r:
                        if len(row) >= 6 and row[4] == "1":
                            vals.append(int(row[5]))
            except FileNotFoundError:
                pass
        else:  # sbridge total = topo + struct
            f = f"{tdir}/sbtim_{day}_c{cpd}_{dc}.csv"
            try:
                with open(f) as fh:
                    r = csv.reader(fh); next(r, None)
                    for row in r:
                        if len(row) >= 2:
                            t = int(row[0]) + int(row[1])
                            if t > 0:
                                vals.append(t)
            except FileNotFoundError:
                pass
    arr = np.array(vals, dtype=np.float64) / 1e6  # -> ms
    _CACHE[key] = arr
    return arr

def kde(x, n=NGRID):
    lo, hi = float(x.min()), float(x.max()); pad = 0.05 * (hi - lo + 1e-9)
    grid = np.linspace(lo - pad, hi + pad, n)
    std = x.std(ddof=1) if len(x) > 1 else 1.0
    q75, q25 = np.percentile(x, [75, 25]); iqr = q75 - q25
    sigma = min(std, iqr / 1.349) if iqr > 0 else std
    if sigma <= 0: sigma = 1e-3
    bw = 0.9 * sigma * len(x) ** (-0.2)
    u = (grid[:, None] - x[None, :]) / bw
    dens = np.exp(-0.5 * u * u).sum(1) / (len(x) * bw * np.sqrt(2 * np.pi))
    return grid, dens

def worst_drop(kind, tdir):
    best, bestmed = None, -1.0
    for dc in DROPS:
        meds = [np.median(load(kind, tdir, cpd, dc)) for cpd in CPDS if len(load(kind, tdir, cpd, dc))]
        if meds and np.median(meds) > bestmed:
            bestmed, best = np.median(meds), dc
    return best

W, GAP = 0.8, 1.5      # violin slot width; gap between clusters
HW = W * 0.42          # violin half-width
fig, ax = plt.subplots(figsize=(5.0, 2.2))
xt, xtl = [], []
gmin, gmax = 1e9, -1e9
x = 0.0
for lbl, kind, tdir, cmapname in TYPES:
    dc = worst_drop(kind, tdir)
    cmap = plt.get_cmap(cmapname)
    cols = cmap(np.linspace(0.4, 0.95, len(CPDS)))  # light(cpd3) -> dark(cpd8)
    start = x
    for ci, cpd in enumerate(CPDS):
        a = load(kind, tdir, cpd, dc)
        if len(a) == 0:
            x += W; continue
        if len(a) > SUB:
            a = rng.choice(a, SUB, replace=False)
        g, d = kde(np.log10(a))
        w = d / d.max() * HW
        ax.fill_betweenx(g, x - w, x + w, facecolor=cols[ci], edgecolor="black", linewidth=0.2, alpha=0.9)
        med = float(np.median(np.log10(a)))
        wm = np.interp(med, g, d) / d.max() * HW
        ax.hlines(med, x - wm, x + wm, color="black", linewidth=0.5)
        gmin, gmax = min(gmin, g.min()), max(gmax, g.max())
        xt.append(x); xtl.append(str(cpd))
        x += W
    ax.text((start + x - W) / 2, 1.01, f"{lbl}\n(drop {DROPLBL[dc]})",
            transform=ax.get_xaxis_transform(), ha="center", va="bottom",
            fontsize=CLUS_FS, linespacing=0.95)  # type over its worst drop
    x += GAP

ax.set_xticks(xt); ax.set_xticklabels(xtl)
ax.set_xlim(-0.6, x - GAP + 0.6 - W + W)
yt = list(range(int(np.floor(gmin)), int(np.ceil(gmax)) + 1))
ax.set_yticks(yt); ax.set_yticklabels([f"$10^{{{k}}}$" for k in yt])
ax.set_ylim(gmin - 0.05, gmax + 0.05)
ax.set_ylabel("recon. time (ms)", fontsize=LABEL_FS)
ax.set_xlabel("checkpoint distance", fontsize=LABEL_FS)
ax.tick_params(axis="both", labelsize=TICK_FS, width=0.5, length=2.5, pad=1.5)
for sp in ax.spines.values():
    sp.set_linewidth(0.5)
ax.set_axisbelow(True); ax.yaxis.grid(True, color="0.88", linewidth=0.3)
fig.tight_layout(pad=0.3)
fig.patch.set_linewidth(0)
out = sys.argv[1] if len(sys.argv) > 1 else "/users/tomislav/recon_time_combined"
base = out.rsplit(".", 1)[0] if out.endswith((".pdf", ".png")) else out
for ext in ("pdf", "png"):
    fig.savefig(f"{base}.{ext}", dpi=300, facecolor="white", edgecolor="none")
    print("wrote", f"{base}.{ext}")
