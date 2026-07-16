#!/usr/bin/env python3
"""Single-figure summary of cgp2 reconstruction time across ALL cpd x drop:
grouped violins (x = drop rate, one violin per cpd), log-time y-axis. Violins are
computed in log10(ms) space so shapes are meaningful over the ~0.1ms-100s range.
exclEmpty subset, day1+day2 pooled, mode-independent (up-mode CSVs)."""
import csv, os, sys, argparse
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Patch

TD = os.environ.get("VIOLIN_TD", "/mydata/uber/bignode_state/cgp2_timing")
DROPS = [("0.05", "d005"), ("0.25", "d025"), ("0.5", "d05"), ("0.75", "d075"), ("0.95", "d095"), ("1.0", "d10")]
CPDS = [3, 4, 5, 6, 7, 8]
# cpd is ORDINAL (3..8), so a perceptually-uniform sequential map (viridis) is
# both colorblind-safe AND encodes the ordering (dark=low cpd -> bright=high cpd).
CMAP = plt.cm.viridis(np.linspace(0.0, 0.9, 6))
LABEL_FS, TICK_FS, LEG_FS = 9, 8, 7  # typical paper text sizes for a 2.2x1.2in column figure
SUB = 25000  # subsample per distribution for KDE
rng = np.random.default_rng(0)

_DEF_OUT = "/tmp/claude-36637/-users-tomislav/9c554978-de8c-42cf-bd6a-bbbe3b07797f/scratchpad/violin_proof.png"
ap = argparse.ArgumentParser(description="cgp2 reconstruction-time violins (2.2x1.2in paper figure)")
ap.add_argument("out", nargs="?", default=_DEF_OUT, help="output path (.pdf/.png)")
ap.add_argument("--rebuild", action="store_true", help="re-parse CSVs + recompute KDEs (else load cache)")
ap.add_argument("--xlabel", default=None, help="x-axis label text (omit for no label)")
ap.add_argument("--ylabel", default=None, help="y-axis label text (omit for no label)")
args = ap.parse_args()

positions, colors = [], []
W = 0.135
NGRID = 256
CACHE = f"{TD}/violin_cache.npz"

def _parse(dc, c):
    vals = []
    for day in ("day1", "day2"):
        try:
            with open(f"{TD}/timing_{day}_up_c{c}_{dc}.csv") as fh:
                r = csv.reader(fh); next(r, None)
                for row in r:
                    if len(row) >= 6 and row[4] == "1":
                        vals.append(int(row[5]))
        except FileNotFoundError:
            pass
    if not vals:
        return None
    a = np.array(vals, dtype=np.float64) / 1e6  # ms
    if len(a) > SUB:
        a = rng.choice(a, SUB, replace=False)
    return np.log10(a)

def _kde(x, n=NGRID):  # gaussian KDE, Silverman bandwidth (matches violinplot's look, no scipy)
    lo, hi = float(x.min()), float(x.max())
    pad = 0.05 * (hi - lo + 1e-9)
    grid = np.linspace(lo - pad, hi + pad, n)
    std = x.std(ddof=1) if len(x) > 1 else 1.0
    q75, q25 = np.percentile(x, [75, 25]); iqr = q75 - q25
    sigma = min(std, iqr / 1.349) if iqr > 0 else std
    if sigma <= 0: sigma = 1e-3
    bw = 0.9 * sigma * len(x) ** (-0.2)
    u = (grid[:, None] - x[None, :]) / bw
    dens = np.exp(-0.5 * u * u).sum(1) / (len(x) * bw * np.sqrt(2 * np.pi))
    return grid, dens, float(np.median(x))

# Both parsing the 72 raw CSVs (~0.5-0.8M rows each) AND the per-violin KDE take
# minutes; cache the KDE curves (grid, density, median) per drop x cpd so that
# styling-only reruns (fonts, sizes, colors, spacing) render in ~1s. Pass
# --rebuild to force a re-parse+recompute when the underlying timing CSVs change.
if os.path.exists(CACHE) and not args.rebuild:
    z = np.load(CACHE)
    cache = {k: z[k] for k in z.files}
else:
    cache = {}
    for _, dc in DROPS:
        for c in CPDS:
            a = _parse(dc, c)
            if a is None:
                continue
            g, d, m = _kde(a)
            k = f"{dc}_c{c}"
            cache[f"{k}|grid"], cache[f"{k}|dens"], cache[f"{k}|med"] = g, d, np.array([m])
    np.savez(CACHE, **cache)

grids, denss, meds = [], [], []
for di, (_, dc) in enumerate(DROPS):
    for ci, c in enumerate(CPDS):
        k = f"{dc}_c{c}"
        if f"{k}|grid" not in cache:
            continue
        grids.append(cache[f"{k}|grid"]); denss.append(cache[f"{k}|dens"]); meds.append(float(cache[f"{k}|med"][0]))
        positions.append(di + (ci - (len(CPDS) - 1) / 2) * W)
        colors.append(CMAP[ci])

fig, ax = plt.subplots(figsize=(2.2, 1.2))  # same proportions as the bar-chart figures
HW = W * 0.95 / 2  # max half-width of a violin (widest slice normalized to this)
for g, d, m, pos, col in zip(grids, denss, meds, positions, colors):
    w = d / d.max() * HW
    ax.fill_betweenx(g, pos - w, pos + w, facecolor=col, edgecolor="black", linewidth=0.2, alpha=0.85)
    wm = np.interp(m, g, d) / d.max() * HW  # median tick spans the violin width at that y
    ax.hlines(m, pos - wm, pos + wm, color="black", linewidth=0.5)

ax.set_xticks(range(len(DROPS)))
ax.set_xticklabels([d for d, _ in DROPS])
ax.set_xlim(-0.45, len(DROPS) - 1 + 0.45)  # trim whitespace beyond the outer groups
if args.xlabel:
    ax.set_xlabel(args.xlabel, fontsize=LABEL_FS)
if args.ylabel:
    ax.set_ylabel(args.ylabel, fontsize=LABEL_FS)
yt = [0, 1, 2, 3, 4, 5]  # log10 ms
ax.set_yticks(yt)
ax.set_yticklabels([f"$10^{{{k}}}$" for k in yt])
ymin = min(float(g.min()) for g in grids)  # fit the full violin bottoms (KDE tails dip below 10^0)
gmax = max(float(g.max()) for g in grids)
ax.set_ylim(ymin - 0.05, gmax + 1.15)  # top headroom so the top-left legend clears the tails
ax.tick_params(axis="both", labelsize=TICK_FS, width=0.5, length=2.5, pad=1.5)
for sp in ax.spines.values():
    sp.set_linewidth(0.5)
ax.set_axisbelow(True); ax.yaxis.grid(True, color="0.88", linewidth=0.3)
handles = [Patch(facecolor=CMAP[i], edgecolor="black", linewidth=0.4, label=str(c)) for i, c in enumerate(CPDS)]
leg = ax.legend(handles=handles, fontsize=LEG_FS, ncol=6, loc="upper left",
                borderaxespad=0.15, columnspacing=0.4, handletextpad=0.2, borderpad=0.2,
                handlelength=0.6, handleheight=0.6)  # one row, top-left, snug to the top edge
leg.get_frame().set_linewidth(0.5)  # thin frame box (default is heavy at this scale)
fig.tight_layout(pad=0.2)
out = args.out
fig.patch.set_linewidth(0)  # kill the figure-frame rectangle (shows as a gray border in PDF)
fig.savefig(out, dpi=300, facecolor="white", edgecolor="none")  # no bbox_inches='tight' -> canvas stays exactly 2.2x1.2 in
print("wrote", out)
