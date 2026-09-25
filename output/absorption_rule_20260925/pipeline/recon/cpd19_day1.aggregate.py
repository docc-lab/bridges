#!/usr/bin/env python3
"""Aggregate the CPD 1:9 sweep. Follows BRIDGES_RUN_HANDOFF.md sections 3-4."""
import json, sys, collections
from pathlib import Path

ROOT = Path("/home/ubuntu/sweep_day1_final")
MODES, ARMS = ["pb0", "cgp0", "sb3"], ["forward", "reverse"]
RATES = ["d005", "d025", "d05", "d075", "d095", "d10"]
RATELABEL = {"d005":"0.05","d025":"0.25","d05":"0.50","d075":"0.75","d095":"0.95","d10":"1.00"}

SUMCOLS = ["traces","feasible","empty","empty_clean","clean","clean_all",
           "real_nodes","edge_exact","edge_anonymous_valid","edge_wrong","constraint_wrong"]

def summary_for(mode, rs, key):
    """Per-mode summary selection. A missing summary is an ERROR, never zero."""
    g = rs.get("sb3_summary" if mode == "sb3" else "greedy_summary") or {}
    if not g:
        raise SystemExit(f"{key}: no summary for mode {mode}; refusing to score as zero")
    return g

def main():
    cells = collections.defaultdict(lambda: collections.Counter())
    conflicts = collections.defaultdict(lambda: collections.Counter())
    problems, missing, contention = [], [], []
    njobs = 0

    for mode in MODES:
        for arm in ARMS:
            for shard in sorted(p.name for p in Path("/dev/shm/day1_shards").iterdir() if p.is_dir()):
                key = f"{mode}_{arm}_{shard}"
                jf = ROOT / "recon" / f"{key}.json"
                if not jf.exists():
                    missing.append(key); continue
                njobs += 1
                d = json.loads(jf.read_text())
                rates_seen = set()
                for rs in d["rate_summaries"]:
                    code = rs["drop_code"]; rates_seen.add(code)
                    ts = rs["topology_summary"]
                    c = cells[(mode, arm, code)]
                    for k in SUMCOLS:
                        c[k] += ts.get(k, 0)
                    g = summary_for(mode, rs, key)
                    cc = conflicts[(mode, arm, code)]
                    for k in ("parent_conflicts","ha_conflicts","amq_conflicts","unrouted_units","borrow_retractions","certain_root_fallbacks"):
                        cc[k] += g.get(k, 0)
                    # per-cell internal consistency (brief section 4.4)
                    if ts["feasible"] + ts["empty"] != ts["traces"]:
                        problems.append(f"{key}@{code}: feasible+empty != traces")
                    if ts["clean"] + ts["empty_clean"] != ts["clean_all"]:
                        problems.append(f"{key}@{code}: clean+empty_clean != clean_all")
                    if ts["edge_exact"]+ts["edge_anonymous_valid"]+ts["edge_wrong"] != ts["real_nodes"]:
                        problems.append(f"{key}@{code}: edge partition != real_nodes")
                if rates_seen != set(RATES):
                    problems.append(f"{key}: rates {sorted(rates_seen)} != expected 6")
                # CPU contention gate
                rf = ROOT / "resources" / f"{key}.resources.json"
                if rf.exists():
                    try:
                        r = json.loads(rf.read_text())
                        ratio = (r["user_seconds"] + r["system_seconds"]) / max(r["wall_seconds"], 1e-9)
                        if ratio < 0.99:
                            contention.append((key, round(ratio, 4)))
                        if r["exit_status"] != 0:
                            problems.append(f"{key}: exit_status {r['exit_status']}")
                    except Exception as e:
                        problems.append(f"{key}: bad resources json ({e})")

    print(f"jobs aggregated : {njobs} / {len(MODES)*len(ARMS)*30}")
    print(f"missing jobs    : {len(missing)}" + (f"  e.g. {missing[:3]}" if missing else ""))
    print(f"cells           : {len(cells)} (expect {len(MODES)*len(ARMS)*6} mode/arm/rate groups)")
    print(f"consistency     : {len(problems)} problems" + (f" -> {problems[:3]}" if problems else " (all identities hold)"))
    print(f"CPU contention  : {len(contention)} jobs below 0.99" + (f" {contention[:5]}" if contention else ""))
    tot = sum(c["parent_conflicts"]+c["ha_conflicts"]+c["amq_conflicts"]+c["unrouted_units"] for c in conflicts.values())
    print(f"HARD-EVIDENCE VIOLATIONS (parent+ha+amq+unrouted): {tot}")
    print(f"telemetry: borrow_retractions={sum(c['borrow_retractions'] for c in conflicts.values())} "
          f"certain_root_fallbacks={sum(c['certain_root_fallbacks'] for c in conflicts.values())}")
    print()
    print("Error % = 100 - 100*clean/feasible   (pooled over shards, summed counters)")
    for arm in ARMS:
        print(f"\n--- {arm} ---")
        print(f"{'drop':>6} | " + " | ".join(f"{m.upper():>7}" for m in MODES))
        for code in RATES:
            row = []
            for m in MODES:
                c = cells.get((m, arm, code))
                row.append("    n/a" if not c or not c["feasible"]
                           else f"{100 - 100.0*c['clean']/c['feasible']:7.2f}")
            print(f"{RATELABEL[code]:>6} | " + " | ".join(row))
    out = {f"{m}|{a}|{c}": dict(v) for (m,a,c), v in cells.items()}
    (ROOT / "aggregate_cells.json").write_text(json.dumps(out, indent=1))
    print(f"\nwrote {ROOT/'aggregate_cells.json'}")
    if problems or missing or tot:
        print("\n** NOT CLEAN — see above **")

main()
