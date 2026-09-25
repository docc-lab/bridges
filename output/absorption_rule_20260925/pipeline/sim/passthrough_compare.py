#!/usr/bin/env python3
"""Passthrough ON vs OFF, reverse arm, both checkpoint policies. Like-for-like scopes."""
import json
name={'pcrb':'PB0','cgprb':'CGP0','sb3':'SB3'}
def v(path):
    d=json.load(open(path)); rh=d['reverse_histograms']
    return dict(fwd=d['baggage_call_bytes']['sum_bytes'], own=d['bridge_payload_bytes']['sum_bytes'],
        exp=rh['combined_checkpoint_payload_bytes']['sum'], ret=rh['reverse_encoded_baggage_bytes']['sum'],
        emit=rh['checkpoint_spans_per_trace']['sum'], truss=rh['accepted_trusses_per_receiving_checkpoint']['sum'],
        hs=rh['reverse_distance_span_hops']['sum'], hn=rh['reverse_distance_span_hops']['count'],
        hmax=rh['reverse_distance_span_hops']['max'], raw=rh['raw_truss_bytes']['sum'])
SRC={'cpd19':('/home/ubuntu/sim_day2/sim/%s_reverse.hist.json','/home/ubuntu/passthru/cpd19/sim/%s_reverse_pass.hist.json'),
     'cpd5' :('/home/ubuntu/cpd5/sim/sim/%s_reverse.hist.json','/home/ubuntu/passthru/cpd5/sim/%s_reverse_pass.hist.json')}
for tag,(off_t,on_t) in SRC.items():
    print(f"\n================ {tag}, day 2, reverse arm — passthrough OFF vs ON ================")
    print(f"{'bridge':<6} {'pass':<5} | {'emit spans':>14} {'export GB':>10} {'ret wire GB':>11} {'TOTAL GB':>9} | {'trusses':>14} {'mean hops':>9} {'max':>4}")
    for m in ('pcrb','cgprb','sb3'):
        try: a,b=v(off_t%m),v(on_t%m)
        except Exception as e: print(f"{name[m]:<6} (pending: {e})"); continue
        for lbl,x in (('OFF',a),('ON',b)):
            tot=(x['exp']+x['fwd']+x['ret'])/1e9
            print(f"{name[m]:<6} {lbl:<5} | {x['emit']:>14,} {x['exp']/1e9:>10.2f} {x['ret']/1e9:>11.2f} {tot:>9.2f} | "
                  f"{x['truss']:>14,} {x['hs']/x['hn'] if x['hn'] else 0:>9.2f} {x['hmax']:>4}")
        ta=(a['exp']+a['fwd']+a['ret'])/1e9; tb=(b['exp']+b['fwd']+b['ret'])/1e9
        print(f"       ->  emit {100*(b['emit']-a['emit'])/a['emit']:+.1f}%  export {100*(b['exp']-a['exp'])/a['exp']:+.1f}%  "
              f"retwire {100*(b['ret']-a['ret'])/a['ret']:+.1f}%  total {100*(tb-ta)/ta:+.1f}%  hops {a['hs']/a['hn']:.2f}->{b['hs']/b['hn']:.2f}")
