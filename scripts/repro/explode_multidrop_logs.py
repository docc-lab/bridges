#!/usr/bin/env python3
"""Adapter: explode multidrop_sweep.sh accuracy logs into the per-drop
`tim_<day>_<prime>_c<cpd>_<dc>.txt` files that plot_recon_error_bydrop.py reads.

Each multi-drop log `<recon>_<day>_<prime>_c<cpd>.log` holds one summary line per
drop rate, tagged like `CGP2[d05] traces=... feasible=... | correct=..% (clean=..)`.
We write each summary line to its own file (the bar plotter greps one FINAL line
per file), grouped per reconstructor under `<dir>/<recon>_bars/`.

Usage:
  python explode_multidrop_logs.py <multidrop_out_dir> [recon ...]   # default: cgp2 pb2

Then render, e.g.:
  ERR_SC=<dir>/cgp2_bars ERR_OUTPRE=/path/cgp2 ERR_OUTSUF=_v2 \
      python plot_recon_error_bydrop.py
"""
import re, os, sys, glob

D = sys.argv[1] if len(sys.argv) > 1 else "."
RECONS = sys.argv[2:] or ["cgp2", "pb2"]
for recon in RECONS:
    outdir = os.path.join(D, f"{recon}_bars")
    os.makedirs(outdir, exist_ok=True)
    n = 0
    for log in glob.glob(os.path.join(D, f"{recon}_*.log")):
        b = os.path.basename(log)[:-4]
        m = re.match(rf"{recon}_(day\d)_(\w+)_c(\d+)$", b)
        if not m:
            continue
        day, prime, cpd = m.groups()
        for line in open(log):
            mm = re.match(r"(?:CGP2|PB2)\[(d\w+)\] traces=", line)  # summary line only
            if mm:
                with open(os.path.join(outdir, f"tim_{day}_{prime}_c{cpd}_{mm.group(1)}.txt"), "w") as fh:
                    fh.write(line)
                n += 1
    print(f"{recon}: wrote {n} per-drop files to {outdir}")
