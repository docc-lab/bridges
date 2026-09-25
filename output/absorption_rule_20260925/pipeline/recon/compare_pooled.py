#!/usr/bin/env python3
"""Shard-agnostic comparison: pooled per-(mode,arm,rate) counters, OLD sweep dir vs NEW sweep dir.
Because drops are per-trace-seeded and replay is per-trace, pooled counters must agree except
where the fixed engine changed a trace's reconstruction."""
import json, glob, os, sys, collections
OLD, NEW, LABEL = sys.argv[1], sys.argv[2], sys.argv[3]
RATES=["d005","d025","d05","d075","d095","d10"]; LBL={"d005":"0.05","d025":"0.25","d05":"0.50","d075":"0.75","d095":"0.95","d10":"1.00"}
KEYS=("traces","feasible","clean","clean_all","real_nodes","edge_exact","edge_anonymous_valid","edge_wrong","constraint_wrong")
def pool(d):
    out=collections.defaultdict(collections.Counter); viol=collections.Counter(); tel=collections.Counter(); n=0
    for f in glob.glob(f'{d}/recon/*.json'):
        key=os.path.basename(f)[:-5]; mode,arm=key.split('_')[0],key.split('_')[1]; n+=1
        j=json.load(open(f)); skey='sb3_summary' if mode=='sb3' else 'greedy_summary'
        for rs in j['rate_summaries']:
            c=out[(mode,arm,rs['drop_code'])]; ts=rs['topology_summary']
            for k in KEYS: c[k]+=ts.get(k,0)
            g=rs.get(skey) or {}
            viol[(mode,arm)]+=g.get('parent_conflicts',0)+g.get('ha_conflicts',0)+g.get('amq_conflicts',0)+g.get('unrouted_units',0)
            for k in ('borrow_retractions','unrouted_units','certain_root_fallbacks','hard_overrides'): tel[k]+=g.get(k,0)
    return out,viol,tel,n
po,vo,to,no=pool(OLD); pn,vn,tn,nn=pool(NEW)
print(f"=== {LABEL}: OLD {no} jobs ({OLD}) vs NEW {nn} jobs ({NEW}) ===")
print(f"NEW hard-evidence violations by (mode,arm): {dict(vn)}   OLD: {dict(vo)}")
print(f"NEW telemetry: {dict(tn)}")
diffcells=0
print(f"{'mode':<5} {'arm':<8} {'drop':>5} | {'traces':>8} {'feasible':>8} | {'clean old':>9} {'clean new':>9} {'Δ':>5} | {'wrong old':>9} {'wrong new':>9} {'Δ':>5} | {'err% old':>8} {'err% new':>8}")
for mode in ("pb0","cgp0","sb3"):
    for arm in ("forward","reverse"):
        for code in RATES:
            a,b=po.get((mode,arm,code)),pn.get((mode,arm,code))
            if not a or not b: continue
            assert a['traces']==b['traces'], f"trace count differs {mode} {arm} {code}: {a['traces']} vs {b['traces']} (different corpus/cleaning!)"
            assert a['real_nodes']==b['real_nodes'], f"real_nodes differ {mode} {arm} {code}"
            eo=100-100*a['clean']/a['feasible']; en=100-100*b['clean']/b['feasible']
            dc=b['clean']-a['clean']; dw=b['edge_wrong']-a['edge_wrong']
            if dc or dw or a['feasible']!=b['feasible']: diffcells+=1
            flag=' *' if (dc or dw) else ''
            print(f"{mode:<5} {arm:<8} {LBL[code]:>5} | {b['traces']:>8} {b['feasible']:>8} | {a['clean']:>9} {b['clean']:>9} {dc:>+5} | {a['edge_wrong']:>9} {b['edge_wrong']:>9} {dw:>+5} | {eo:8.2f} {en:8.2f}{flag}")
print(f"\ncells (mode,arm,rate) where clean or edge_wrong moved: {diffcells} of {len(pn)}")
