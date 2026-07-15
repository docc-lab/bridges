#!/usr/bin/env python3
"""Scan the cgp2 timing-rerun per-config .txt files and emit a consolidated
results markdown to /mydata. Re-runnable: includes only fully-complete drops
(all 36 configs = 2 days x 3 modes x 6 cpd present with a TIMING line)."""
import re, os, datetime

SC = "/tmp/claude-36637/-users-tomislav/9c554978-de8c-42cf-bd6a-bbbe3b07797f/scratchpad"
OUT = "/mydata/uber/bignode_state/cgp2_timing_results.md"
DROPS = [("0.05","d005"),("0.25","d025"),("0.5","d05"),("0.75","d075"),("0.95","d095"),("1.0","d10")]
MODES = ["up","down","none"]
CPDS = [3,4,5,6,7,8]
DAYTOT = {"day1":521305,"day2":843274}

def parse(day, mode, cpd, dc):
    f = f"{SC}/tim_{day}_{mode}_c{cpd}_{dc}.txt"
    if not os.path.exists(f): return None
    t = open(f).read()
    m = re.search(r"feasible=(\d+) empty=\d+ \| correct=[\d.]+% \(clean=(\d+)\) \| correctExclEmpty=([\d.]+)%", t)
    tm = re.search(r"TIMING\[exclEmpty\] traces=(\d+).*?mean=([\d.]+)ms p50=([\d.]+) p90=([\d.]+) p99=([\d.]+) max=([\d.]+)", t)
    if not m or not tm: return None
    return dict(feas=int(m.group(1)), clean=int(m.group(2)), excl=float(m.group(3)),
                mean=float(tm.group(2)), p50=float(tm.group(3)), p90=float(tm.group(4)),
                p99=float(tm.group(5)), mx=float(tm.group(6)))

def drop_complete(dc):
    return all(parse(d,m,c,dc) for d in ("day1","day2") for m in MODES for c in CPDS)

L = []
L.append("# cgp2 reconstruction — timing + accuracy rerun (consolidated)\n")
L.append(f"_Generated {datetime.date(2026,7,5)}. Full unfiltered Uber corpus (day1=521,305, day2=843,274 traces)._\n")
L.append("cgp2 (from-scratch call-graph-preserving reconstructor over cgprb payloads), `--prefix-len 8`, "
         "deterministic. Three bloom-sizing modes: **up** = prime round-up (`--prime-m`), **down** = prime "
         "round-down byte-capped (`--prime-m --prime-m-bytecap`), **none** = no prime rounding.\n")
L.append("Metrics: **exclEmpty** = fraction of feasible (non-empty) reconstructions with zero wrong reconnections "
         "(FP rate = 100 − exclEmpty). **recon time** = per-trace wall-time over the exclEmpty subset (ms); "
         "mode-independent (bloom size doesn't change solve work), so shown once (prime-up) per day.\n")
L.append("Per-trace timing CSVs: `/mydata/uber/bignode_state/cgp2_timing/timing_<day>_<mode>_c<cpd>_<dropcode>.csv` "
         "(cols: tid, survivors, spans, dropped, feasible, recon_ns).\n")

done = []
for drop, dc in DROPS:
    if not drop_complete(dc): continue
    done.append(drop)
    L.append(f"\n## drop {drop}\n")
    # accuracy: pooled both days, per mode
    L.append("### exclEmpty accuracy (%), pooled day1+day2 (feasible-weighted)\n")
    L.append("| cpd | prime-up | prime-down | no-prime |")
    L.append("|----:|---------:|-----------:|---------:|")
    for c in CPDS:
        cells=[]
        for m in MODES:
            tc=sum(parse(d,m,c,dc)["clean"] for d in ("day1","day2"))
            tf=sum(parse(d,m,c,dc)["feas"]  for d in ("day1","day2"))
            cells.append(f"{100*tc/tf:.2f}")
        L.append(f"| {c} | {cells[0]} | {cells[1]} | {cells[2]} |")
    # timing: per day, mode-independent (up)
    L.append("\n### recon time (ms, exclEmpty subset) — mode-independent, per day\n")
    L.append("| cpd | day | mean | p50 | p90 | p99 | max |")
    L.append("|----:|----:|-----:|----:|----:|----:|----:|")
    for c in CPDS:
        for d in ("day1","day2"):
            r=parse(d,"up",c,dc)
            L.append(f"| {c} | {d[-1]} | {r['mean']:.2f} | {r['p50']:.2f} | {r['p90']:.2f} | {r['p99']:.2f} | {r['mx']:.1f} |")

L.append(f"\n---\n_Drops included: {', '.join(done) if done else 'none complete yet'}. "
         f"Regenerate with gen_timing_results.py as more complete._\n")
open(OUT,"w").write("\n".join(L))
print(f"wrote {OUT}: {len(done)} complete drops [{', '.join(done)}]")
