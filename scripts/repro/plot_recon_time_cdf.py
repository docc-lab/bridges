#!/usr/bin/env python3
"""CDF of per-trace cgp2 reconstruction time (exclEmpty subset), one curve per
cpd, for a single drop rate. Log time axis (the distribution spans ~0.1 ms to
~100 s). day1+day2 pooled, mode-independent (up-mode CSVs). Colorblind-safe
(Okabe-Ito) with distinct line styles for grayscale/CVD redundancy.

  python3 plot_recon_time_cdf.py <dropcode> <out.pdf>   # dropcode e.g. d05
"""
import csv, sys
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

TD = "/mydata/uber/bignode_state/cgp2_timing"
CPDS = [3, 4, 5, 6, 7, 8]
OKABE = ["#0072B2", "#E69F00", "#009E73", "#D55E00", "#CC79A7", "#000000"]
STYLES = ["-", "--", "-.", ":", "-", "--"]
LABEL_FS, TICK_FS, LEG_FS = 40, 34, 32

dc = sys.argv[1] if len(sys.argv) > 1 else "d05"
out = sys.argv[2] if len(sys.argv) > 2 else f"/users/tomislav/recon_time_cdf_{dc}.pdf"
DROPLBL = {"d005": "0.05", "d025": "0.25", "d05": "0.5", "d075": "0.75", "d095": "0.95", "d10": "1.0"}

fig, ax = plt.subplots(figsize=(14, 9))
for i, c in enumerate(CPDS):
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
        continue
    v = np.sort(np.array(vals, dtype=np.float64)) / 1e6  # ns -> ms
    y = np.arange(1, len(v) + 1) / len(v)
    step = max(1, len(v) // 3000)  # subsample the monotone curve for a light file
    ax.plot(v[::step], y[::step], STYLES[i], color=OKABE[i], linewidth=3.5, label=str(c))

ax.set_xscale("log")
ax.set_xlabel("reconstruction time (ms)", fontsize=LABEL_FS)
ax.set_ylabel("CDF", fontsize=LABEL_FS)
ax.tick_params(axis="both", labelsize=TICK_FS)
ax.set_ylim(0, 1.0)
ax.grid(True, which="both", color="0.88", linewidth=0.8)
ax.legend(title="cpd", fontsize=LEG_FS, title_fontsize=LEG_FS, loc="lower right",
          frameon=True, ncol=2, columnspacing=1.0, handlelength=1.8, labelspacing=0.3)
fig.tight_layout()
fig.savefig(out, bbox_inches="tight")
print("wrote", out, f"(drop {DROPLBL.get(dc, dc)})")
