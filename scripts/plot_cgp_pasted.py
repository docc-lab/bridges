#!/usr/bin/env python3
"""CG-Bridge topology-error grouped bar chart (figs/ family style) from the
pasted sweep grids. Bars = % traces failing topology (100 - topo_correct),
grouped by drop rate, one bar per cpd (3-8). Behind each, lighter shade =
dropfanout error (100 - topo_among_dropfanout). Saves PNG + PDF per day."""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.patches import Patch

rates = ["0.05", "0.25", "0.5", "0.75", "0.95"]
labels = ["5%", "25%", "50%", "75%", "95%"]
cpds = [3, 4, 5, 6, 7, 8]
col = {3: "#3b7dd8", 4: "#4caf6e", 5: "#9c4fd8", 6: "#d8743b", 7: "#c0392b", 8: "#7b4f2a"}

# (topo_correct, topo_dropfanout) per cpd, over rates 0.05/0.25/0.5/0.75/0.95
DATA = {
 "day1": {  # sample=100000
   3: [(99.8580,99.7217),(98.7500,98.3860),(97.5740,97.1919),(97.5710,97.3118),(99.0510,98.9749)],
   4: [(99.2660,98.6110),(95.8860,94.8370),(92.4920,91.2311),(92.1330,91.0025),(96.4070,95.9157)],
   5: [(99.7150,99.4225),(98.4440,98.0042),(96.9820,96.4722),(96.8120,96.3418),(98.7310,98.5433)],
   6: [(99.0590,98.1897),(95.4240,94.2349),(91.6850,90.4145),(90.9270,89.7924),(95.9360,95.4504)],
   7: [(99.3900,98.9944),(96.0730,95.1624),(92.7400,91.4153),(92.2170,90.6499),(96.3820,95.4416)],
   8: [(99.8050,99.6670),(98.6260,98.3568),(97.0190,96.5726),(96.5620,96.0400),(98.4940,98.2320)],
 },
 "day2": {  # sample=180000
   3: [(99.8250,99.6297),(98.5361,97.9926),(97.3083,96.7225),(97.2050,96.7587),(98.8822,98.7361)],
   4: [(99.2506,98.4691),(95.6756,94.1501),(92.3561,90.5366),(91.9039,90.2176),(96.3222,95.5688)],
   5: [(99.7139,99.4164),(98.2617,97.6369),(96.7500,95.9973),(96.5544,95.8795),(98.5794,98.3023)],
   6: [(99.0656,98.1037),(95.0294,93.3679),(90.8122,88.8033),(90.2011,88.3277),(95.4461,94.5409)],
   7: [(99.3856,98.8926),(96.1039,94.9056),(92.7044,91.0618),(92.1306,90.3692),(96.4017,95.4789)],
   8: [(99.8089,99.6537),(98.6628,98.2747),(96.9950,96.3529),(96.4039,95.6747),(98.4361,98.0878)],
 },
}


# Sample-size-weighted merge of the two days into one unified grid.
# topo_correct is over ALL traces, so (tc1*50k + tc2*90k)/140k is exact; the
# dropfanout fractions are near-identical across days, so the same weighting is
# right for topo_dropfanout to within noise.
W = {"day1": 100000, "day2": 180000}
_tot = W["day1"] + W["day2"]
DATA["merged"] = {
    c: [((DATA["day1"][c][i][0] * W["day1"] + DATA["day2"][c][i][0] * W["day2"]) / _tot,
         (DATA["day1"][c][i][1] * W["day1"] + DATA["day2"][c][i][1] * W["day2"]) / _tot)
        for i in range(len(rates))]
    for c in cpds
}


def lighten(h, f=0.55):
    h = h.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f"#{int(r+(255-r)*f):02x}{int(g+(255-g)*f):02x}{int(b+(255-b)*f):02x}"


for day, grid in DATA.items():
    x = np.arange(len(rates))
    w = 0.8 / len(cpds)
    fig, ax = plt.subplots(figsize=(11, 5), dpi=150)
    for j, c in enumerate(cpds):
        off = (j - (len(cpds) - 1) / 2) * w
        err = [100 - tc for tc, _ in grid[c]]
        dferr = [100 - df for _, df in grid[c]]
        ax.bar(x + off, dferr, w, color=lighten(col[c]), zorder=1)
        ax.bar(x + off, err, w, color=col[c], label=f"cpd {c}", zorder=2)
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
        out = f"/users/tomislav/bridges/figs/cgp_topo_error_{day}.{ext}"
        fig.savefig(out)
        print("wrote", out)
    plt.close(fig)
