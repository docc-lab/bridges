#!/usr/bin/env python3
# Plot a cgprb sample-sweep: topo_correct vs drop rate, one line per cpd.
# Reads the per-cell .err files (cpdN_rX.err) produced by run_cgprb_sample_sweep.sh.
#
#   python3 scripts/plot_sample_sweep.py <sweep-dir>   (default ~/cgprb_sample_sweep_day2)
import sys, glob, re, os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

d = sys.argv[1] if len(sys.argv) > 1 else os.path.expanduser("~/cgprb_sample_sweep_day2")

# cpd -> {drop: (topo_correct, topo_dropfanout)}
data = {}
for f in glob.glob(os.path.join(d, "cpd*_r*.err")):
    m = re.search(r"cpd(\d+)_r([0-9.]+)\.err$", os.path.basename(f))
    if not m:
        continue
    cpd, drop = int(m.group(1)), float(m.group(2))
    txt = open(f).read()
    tc = re.search(r"topo_correct=([0-9.]+)%", txt)
    td = re.search(r"topo_correct_among_them=([0-9.]+)%", txt)
    if not tc:
        continue
    data.setdefault(cpd, {})[drop] = (float(tc.group(1)),
                                      float(td.group(1)) if td else None)

if not data:
    sys.exit(f"no completed cells found in {d}")

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5))
for cpd in sorted(data):
    pts = sorted(data[cpd].items())
    xs = [p[0] for p in pts]
    ax1.plot(xs, [p[1][0] for p in pts], marker="o", label=f"cpd={cpd}")
    ax2.plot(xs, [p[1][1] for p in pts if p[1][1] is not None],
             marker="o", label=f"cpd={cpd}")
for ax, title in ((ax1, "all traces"), (ax2, "dropped-fanout traces")):
    ax.set_xlabel("drop rate")
    ax.set_ylabel("topo_correct (%)")
    ax.set_title(title)
    ax.grid(True, alpha=0.3)
    ax.legend(fontsize=8)
fig.suptitle(f"cgprb topo_correct — {os.path.basename(os.path.normpath(d))}")
out = os.path.join(d, "topo_sweep.png")
fig.savefig(out, dpi=130, bbox_inches="tight")
print("wrote", out)
