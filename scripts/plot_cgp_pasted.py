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
 "day1": {  # sample=50000
   3: [(99.864,99.7331),(98.648,98.2532),(97.558,97.1704),(97.55,97.2865),(99.026,98.9478)],
   4: [(99.248,98.5735),(95.916,94.8747),(92.556,91.2685),(92.118,90.96),(96.442,95.9517)],
   5: [(99.726,99.4373),(98.432,97.9879),(96.996,96.4768),(96.866,96.3959),(98.808,98.6351)],
   6: [(99.08,98.2155),(95.424,94.2239),(91.662,90.3769),(90.846,89.6862),(95.952,95.4634)],
   7: [(99.392,98.9703),(96.012,95.0599),(92.732,91.3769),(92.142,90.5316),(96.39,95.4441)],
   8: [(99.804,99.6634),(98.592,98.3143),(96.976,96.5105),(96.578,96.0536),(98.484,98.2139)],
 },
 "day2": {  # sample=90000
   3: [(99.8044,99.5866),(98.4978,97.9405),(97.2756,96.6859),(97.1911,96.7434),(98.8933,98.7494)],
   4: [(99.2767,98.5009),(95.6789,94.1339),(92.2956,90.4661),(91.9478,90.2672),(96.3611,95.6259)],
   5: [(99.6989,99.3893),(98.2989,97.6928),(96.7644,96.0208),(96.5367,95.8518),(98.5733,98.2958)],
   6: [(99.0322,98.0264),(95.0156,93.3354),(90.8444,88.8356),(90.2156,88.3357),(95.5056,94.5992)],
   7: [(99.37,98.8585),(96.1011,94.8905),(92.81,91.1852),(92.1044,90.3285),(96.3667,95.4295)],
   8: [(99.8144,99.6624),(98.6778,98.2945),(97.0089,96.3695),(96.4189,95.69),(98.4122,98.0562)],
 },
}


# Sample-size-weighted merge of the two days into one unified grid.
# topo_correct is over ALL traces, so (tc1*50k + tc2*90k)/140k is exact; the
# dropfanout fractions are near-identical across days, so the same weighting is
# right for topo_dropfanout to within noise.
W = {"day1": 50000, "day2": 90000}
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
