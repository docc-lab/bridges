#!/usr/bin/env python3
"""
CG-Bridge topology-error grouped bar chart, matching the figs/ family style
(wrong_by_droprate.png / traces_affected_by_droprate.png). Bars = % of traces
that FAIL topology reconstruction (100 - topo_correct), grouped by drop rate,
one bar per checkpoint distance (cpd 2-8). Reads the TOPO line each cgprb sweep
cell prints to its .err.
"""
import re
import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

rates = ["0.05", "0.25", "0.5", "0.75", "0.95", "1.0"]
labels = ["5%", "25%", "50%", "75%", "95%", "100%"]
cpds = [2, 3, 4, 5, 6, 7, 8]

# cpd -> dir holding cpd<c>_r<rate>.err (cpd2 mini-sweep, cpd3/4 fill-in, cpd5-8 sweep)
DIRS = {2: "/users/tomislav/cgp_minisweep",
        3: "/users/tomislav/cgp_sweep34", 4: "/users/tomislav/cgp_sweep34",
        5: "/users/tomislav/cgp_sweep58", 6: "/users/tomislav/cgp_sweep58",
        7: "/users/tomislav/cgp_sweep58", 8: "/users/tomislav/cgp_sweep58"}
# match the existing figs palette for cpd3-8; cpd2 gets a distinct magenta.
col = {2: "#e377c2", 3: "#3b7dd8", 4: "#4caf6e", 5: "#9c4fd8",
       6: "#d8743b", 7: "#c0392b", 8: "#7b4f2a"}
TOPO_RE = re.compile(r"topo_correct=([0-9.]+)%")


def topo_error(cpd, r):
    path = os.path.join(DIRS[cpd], f"cpd{cpd}_r{r}.err")
    tc = None
    if os.path.exists(path):
        for line in open(path):
            if line.startswith("TOPO"):
                m = TOPO_RE.search(line)
                if m:
                    tc = float(m.group(1))
    return None if tc is None else 100.0 - tc


err = {c: [topo_error(c, r) for r in rates] for c in cpds}
missing = [f"cpd{c}_r{r}" for c in cpds for r in rates if topo_error(c, r) is None]

x = np.arange(len(rates))
w = 0.8 / len(cpds)
fig, ax = plt.subplots(figsize=(11, 5), dpi=150)
for j, c in enumerate(cpds):
    off = (j - (len(cpds) - 1) / 2) * w
    ax.bar(x + off, [e if e is not None else 0 for e in err[c]], w, label=f"cpd {c}", color=col[c])
ax.set_xticks(x)
ax.set_xticklabels(labels)
ax.set_xlabel("drop rate")
ax.set_ylabel("traces failing topology (%)")
ax.set_title("Topology error rate by drop rate and checkpoint distance\nUber corpus (full), CP-SAT solver")
ax.legend(ncol=7, frameon=False, fontsize=9)
ax.grid(axis="y", alpha=.3)
ax.spines[["top", "right"]].set_visible(False)
fig.tight_layout()
out = "/users/tomislav/bridges/figs/cgp_topo_error.png"
fig.savefig(out)
print("wrote", out)
if missing:
    print("MISSING:", ", ".join(missing))
