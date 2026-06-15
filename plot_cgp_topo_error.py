#!/usr/bin/env python3
"""
CG-Bridge topology-error grouped bar chart, matching the figs/ family style.
Bars = % of traces that FAIL topology reconstruction (100 - topo_correct),
grouped by drop rate, one bar per checkpoint distance (cpd 3-8; cpd2 excluded).

Behind each solid bar, in a LIGHTER shade of the same color, is the error rate
restricted to DROPFANOUT traces (100 - topo_correct_among_them) — the traces that
actually lost a fan-out, i.e. the hard cases. It is always >= the overall error
(no-fanout traces are trivially correct), so the light bar shows above the solid.
Reads the TOPO line each cgprb sweep cell prints to its .err.
"""
import re
import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.patches import Patch

rates = ["0.05", "0.25", "0.5", "0.75", "0.95", "1.0"]
labels = ["5%", "25%", "50%", "75%", "95%", "100%"]
cpds = [3, 4, 5, 6, 7, 8]

# cpd -> dir holding cpd<c>_r<rate>.err (cpd3/4 fill-in, cpd5-8 sweep)
DIRS = {3: "/users/tomislav/cgp_sweep34", 4: "/users/tomislav/cgp_sweep34",
        5: "/users/tomislav/cgp_sweep58", 6: "/users/tomislav/cgp_sweep58",
        7: "/users/tomislav/cgp_sweep58", 8: "/users/tomislav/cgp_sweep58"}
# match the existing figs palette for cpd3-8.
col = {3: "#3b7dd8", 4: "#4caf6e", 5: "#9c4fd8",
       6: "#d8743b", 7: "#c0392b", 8: "#7b4f2a"}
TOPO_RE = re.compile(r"topo_correct=([0-9.]+)%")
DF_RE = re.compile(r"topo_correct_among_them=([0-9.]+)%")


def lighten(hexc, f=0.55):
    hexc = hexc.lstrip("#")
    r, g, b = int(hexc[0:2], 16), int(hexc[2:4], 16), int(hexc[4:6], 16)
    r = int(r + (255 - r) * f)
    g = int(g + (255 - g) * f)
    b = int(b + (255 - b) * f)
    return f"#{r:02x}{g:02x}{b:02x}"


def errors(cpd, r):
    """Return (overall_error, dropfanout_error) in %, or (None, None)."""
    path = os.path.join(DIRS[cpd], f"cpd{cpd}_r{r}.err")
    tc = df = None
    if os.path.exists(path):
        for line in open(path):
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
    # dropfanout error BEHIND, lighter shade (taller, shows above the solid bar)
    ax.bar(x + off, [e if e is not None else 0 for e in dferr[c]], w,
           color=lighten(col[c]), zorder=1)
    # overall topo error IN FRONT, full color
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
out = "/users/tomislav/bridges/figs/cgp_topo_error.png"
fig.savefig(out)
print("wrote", out)
if missing:
    print("MISSING:", ", ".join(missing))
