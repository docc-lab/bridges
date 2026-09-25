#!/usr/bin/env python3
import json, glob, os, csv, collections, sys
RATES=["d005","d025","d05","d075","d095","d10"]; LBL={"d005":"0.05","d025":"0.25","d05":"0.50","d075":"0.75","d095":"0.95","d10":"1.00"}
for day,root,ntr in (("DAY 1","/home/ubuntu/sweep_day1_final",521305),("DAY 2","/home/ubuntu/sweep_day2_final",843274)):
    cells=json.load(open(f"{root}/aggregate_cells.json"))
    get=lambda m,a,c: cells[f"{m}|{a}|{c}"]
    print(f"\n==================== {day} ({ntr:,} traces) ====================")
    print("A) CORRECTLY RECONSTRUCTED TRACES out of all traces (clean_all = clean + trivially-empty-and-clean). Denominator-free, like-for-like.")
    print(f"{'bridge':<6} {'drop':>5} | {'forward':>8} {'reverse':>8} {'Δ traces':>9} {'Δ %pts':>7} | {'oblig fwd':>9} {'oblig rev':>9} | {'cond.err fwd':>12} {'cond.err rev':>12}")
    for m in ("pb0","cgp0","sb3"):
        for c in RATES:
            f,r=get(m,"forward",c),get(m,"reverse",c)
            d=r["clean_all"]-f["clean_all"]
            ef=100-100*f["clean"]/f["feasible"]; er=100-100*r["clean"]/r["feasible"]
            print(f"{m.upper():<6} {LBL[c]:>5} | {f['clean_all']:>8,} {r['clean_all']:>8,} {d:>+9,} {100*d/ntr:>+7.3f} | {f['feasible']:>9,} {r['feasible']:>9,} | {ef:>11.2f}% {er:>11.2f}%")
    # B) timing
    print("\nB) RECONSTRUCTION TIME (recon_ns, feasible traces only, pooled across shards; exact nearest-rank quantiles)  ms")
    print(f"{'bridge':<6} {'drop':>5} | {'median fwd':>10} {'median rev':>10} {'ratio':>6} | {'p99 fwd':>9} {'p99 rev':>9} {'ratio':>6} | {'mean fwd':>9} {'mean rev':>9} {'ratio':>6}")
    def load(m,a,c):
        vals=[]
        for f in glob.glob(f"{root}/recon/{m}_{a}_shard*_{c}.csv"):
            with open(f) as fh:
                rd=csv.reader(fh); next(rd,None)
                for row in rd:
                    if row[4]=='1': vals.append(int(row[5]))
        vals.sort(); n=len(vals)
        q=lambda p: vals[min(n-1,max(0,int(p*n+0.999999)-1))]/1e6 if n else float('nan')
        return (q(0.5), q(0.99), (sum(vals)/n/1e6) if n else float('nan'), n)
    for m in ("pb0","cgp0","sb3"):
        for c in RATES:
            (mf,pf,af,nf),(mr,pr,ar,nr)=load(m,"forward",c),load(m,"reverse",c)
            print(f"{m.upper():<6} {LBL[c]:>5} | {mf:>10.3f} {mr:>10.3f} {mr/mf:>6.2f} | {pf:>9.1f} {pr:>9.1f} {pr/pf:>6.2f} | {af:>9.3f} {ar:>9.3f} {ar/af:>6.2f}")
        sys.stdout.flush()
