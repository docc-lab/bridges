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
 "day1": {  # full corpus (521,305 traces)
   3: [(99.8389,99.6793),(98.7654,98.4000),(97.5932,97.2102),(97.5454,97.2822),(99.0702,98.9950)],
   4: [(99.2367,98.5516),(95.8698,94.8213),(92.5067,91.2508),(92.1094,90.9606),(96.4481,95.9546)],
   5: [(99.7019,99.4015),(98.4410,97.9950),(96.9607,96.4333),(96.7861,96.3050),(98.6935,98.5001)],
   6: [(99.0397,98.1513),(95.4175,94.2192),(91.6116,90.3108),(90.9001,89.7475),(95.8690,95.3701)],
   7: [(99.3610,98.9306),(96.0526,95.1298),(92.7446,91.4128),(92.2228,90.6503),(96.3937,95.4549)],
   8: [(99.7873,99.6362),(98.6758,98.4098),(97.1339,96.6926),(96.6315,96.1121),(98.5022,98.2390)],
 },
 "day2": {  # full corpus (843,274 traces)
   3: [(99.8030,99.5826),(98.5318,97.9859),(97.3274,96.7448),(97.2715,96.8334),(98.9279,98.7870)],
   4: [(99.2446,98.4484),(95.7610,94.2568),(92.4190,90.6066),(91.9088,90.2170),(96.3682,95.6229)],
   5: [(99.7077,99.3991),(98.2682,97.6487),(96.7093,95.9493),(96.5445,95.8627),(98.6000,98.3274)],
   6: [(99.0533,98.0889),(94.9896,93.3150),(90.8363,88.8352),(90.1786,88.2861),(95.4996,94.6055)],
   7: [(99.4047,98.9313),(96.1612,94.9907),(92.7779,91.1490),(92.2166,90.4704),(96.4075,95.4776)],
   8: [(99.8062,99.6415),(98.6722,98.2824),(97.0567,96.4221),(96.4884,95.7696),(98.4651,98.1197)],
 },
}


# Trace-count-weighted merge of the two days into one unified grid (full corpus).
# topo_correct is over ALL traces, so (tc1*n1 + tc2*n2)/(n1+n2) is exact; the
# dropfanout fractions are near-identical across days, so the same weighting is
# right for topo_dropfanout to within noise.
W = {"day1": 521305, "day2": 843274}
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
