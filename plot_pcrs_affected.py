#!/usr/bin/env python3
"""
PCRS affected-trace rate by drop rate and checkpoint distance.

Bars = % of traces with >=1 reconstruction error (the `tr=` field of the cpsat
PCRS PROGRESS line), grouped by drop rate, one bar per checkpoint distance
(cpd 3-8). Full Uber corpus, CP-SAT solver.

Data source: the cpsat PCRS sweeps' final PROGRESS line in each cell's .err:
  cpsat_sweep_cpd3to7/cpd{c}_r{r}_aware.err  (cpd 3-7)
  cpsat_sweep_cpd8/cpd8_r{r}_aware.err       (cpd 8)
(Previously this figure was produced by an ad-hoc heredoc; this is the saved
generator.)
"""
import re
import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

rates = ["0.05", "0.25", "0.5", "0.75", "0.95"]
labels = ["5%", "25%", "50%", "75%", "95%"]
cpds = [3, 4, 5, 6, 7, 8]
# figs/ family palette for cpd3-8.
col = {3: "#3b7dd8", 4: "#4caf6e", 5: "#9c4fd8",
       6: "#d8743b", 7: "#c0392b", 8: "#7b4f2a"}
TR_RE = re.compile(r"\btr=([0-9.]+)%")


def affected(cpd, r):
    d = "/users/tomislav/cpsat_sweep_cpd8" if cpd == 8 else "/users/tomislav/cpsat_sweep_cpd3to7"
    path = os.path.join(d, f"cpd{cpd}_r{r}_aware.err")
    val = None
    if os.path.exists(path):
        for line in open(path):
            if line.startswith("PROGRESS"):
                m = TR_RE.search(line)
                if m:
                    val = float(m.group(1))  # last PROGRESS line = final value
    return val


data = {c: [affected(c, r) for r in rates] for c in cpds}
missing = [f"cpd{c}_r{r}" for c in cpds for r in rates if affected(c, r) is None]

x = np.arange(len(rates))
w = 0.8 / len(cpds)
fig, ax = plt.subplots(figsize=(11, 5), dpi=150)
for j, c in enumerate(cpds):
    off = (j - (len(cpds) - 1) / 2) * w
    ax.bar(x + off, [v if v is not None else 0 for v in data[c]], w,
           label=f"cpd {c}", color=col[c])

# ~2x axis fonts.
ax.set_xticks(x)
ax.set_xticklabels(labels, fontsize=20)
ax.tick_params(axis="y", labelsize=20)
ax.set_xlabel("drop rate", fontsize=22)
ax.set_ylabel("affected traces (%)", fontsize=22)

# Compress the y scale ~20% (data fills ~80% of the axis) so the bars are
# shorter and the upper area is free for the legend. Still linear.
dmax = max(v for c in cpds for v in data[c] if v is not None)
ax.set_ylim(0, dmax / 0.8)

# Bigger, boxed legend tucked into the empty top-left.
ax.legend(ncol=2, frameon=True, fontsize=18, loc="upper left",
          borderpad=0.6, labelspacing=0.4, columnspacing=1.2, handlelength=1.4)
ax.grid(axis="y", alpha=.3)
ax.spines[["top", "right"]].set_visible(False)
fig.tight_layout()
out = "/users/tomislav/bridges/figs/traces_affected_by_droprate.png"
fig.savefig(out)
print("wrote", out)
if missing:
    print("MISSING:", ", ".join(missing))
