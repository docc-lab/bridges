#!/usr/bin/env python3
"""CG-Bridge topology-error grouped bar chart (figs/ family style) for a cgprb
sweep directory (cpdN_rX.err files). Bars = % of traces failing topology
(100 - topo_correct), grouped by drop rate, one bar per checkpoint distance
(cpd 3-8; cpd2 excluded as it's trivially 0). Behind each, in a lighter shade,
the error restricted to dropfanout traces (100 - topo_among_dropfanout).
Saves both PNG and PDF.

  plot_cgp_topo_error.py <sweep-dir> [out-prefix]
"""
import re, os, sys
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.patches import Patch

sweep_dir = sys.argv[1] if len(sys.argv) > 1 else "/users/tomislav/cgprb_sample_sweep_day2_10k"
base = os.path.basename(os.path.normpath(sweep_dir))
out_prefix = sys.argv[2] if len(sys.argv) > 2 else f"/users/tomislav/bridges/figs/cgp_topo_error_{base}"

rates = ["0.05", "0.25", "0.5", "0.75", "0.95"]
labels = ["5%", "25%", "50%", "75%", "95%"]
cpds = [3, 4, 5, 6, 7, 8]
col = {3: "#3b7dd8", 4: "#4caf6e", 5: "#9c4fd8", 6: "#d8743b", 7: "#c0392b", 8: "#7b4f2a"}
TOPO_RE = re.compile(r"topo_correct=([0-9.]+)%")
DF_RE = re.compile(r"topo_correct_among_them=([0-9.]+)%")


def lighten(h, f=0.55):
    h = h.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f"#{int(r+(255-r)*f):02x}{int(g+(255-g)*f):02x}{int(b+(255-b)*f):02x}"


def errors(cpd, r):
    p = os.path.join(sweep_dir, f"cpd{cpd}_r{r}.err")
    tc = df = None
    if os.path.exists(p):
        for line in open(p):
            if line.startswith("TOPO"):
                m = TOPO_RE.search(line)
                if m:
                    tc = float(m.group(1))
                m = DF_RE.search(line)
                if m:
                    df = float(m.group(1))
    return (None if tc is None else 100.0 - tc,
            None if df is None else 100.0 - df)


err = {c: [errors(c, r)[0] for r in rates] for c in cpds}
dferr = {c: [errors(c, r)[1] for r in rates] for c in cpds}
missing = [f"cpd{c}_r{r}" for c in cpds for r in rates if errors(c, r)[0] is None]

x = np.arange(len(rates))
w = 0.8 / len(cpds)
fig, ax = plt.subplots(figsize=(11, 5), dpi=150)
for j, c in enumerate(cpds):
    off = (j - (len(cpds) - 1) / 2) * w
    ax.bar(x + off, [e if e is not None else 0 for e in dferr[c]], w,
           color=lighten(col[c]), zorder=1)
    ax.bar(x + off, [e if e is not None else 0 for e in err[c]], w,
           color=col[c], label=f"cpd {c}", zorder=2)
ax.set_xticks(x)
ax.set_xticklabels(labels)
ax.set_xlabel("drop rate")
ax.set_ylabel("traces failing topology (%)")
leg1 = ax.legend(ncol=6, frameon=False, fontsize=9, loc="upper left")
ax.add_artist(leg1)
shade = [Patch(facecolor="#555555", label="all traces (100 − topo_correct)"),
         Patch(facecolor=lighten("#555555"),
               label="dropfanout traces (100 − topo_among_dropfanout)")]
ax.legend(handles=shade, frameon=False, fontsize=9, loc="upper right")
ax.grid(axis="y", alpha=.3)
ax.spines[["top", "right"]].set_visible(False)
fig.tight_layout()
for ext in ("png", "pdf"):
    fig.savefig(f"{out_prefix}.{ext}")
    print("wrote", f"{out_prefix}.{ext}")
if missing:
    print("MISSING:", ", ".join(missing))
