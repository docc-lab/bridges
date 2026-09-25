"""Where the absorption rule moves checkpoint load, and why the totals hide it.

  /mydata/uber/policy_probe_20260924/sim   50k day-1 traces, PB, CPD 1:9 seed 42,
                                           leaf rejection q=1, each policy run
                                           twice differing only in
                                           --reverse-pass-checkpoints

Three things the aggregate totals do not show:

1. Absorbing at a scheduled checkpoint is FREE in count terms -- that span was
   already emitting -- so the default pays in weight instead. Reported as
   trusses per new emitter.

2. Total checkpoint bytes are CONSERVED: the same trusses absorbed elsewhere.
   emitters x bytes-per-emission is the identity, and passing raises the first
   while lowering the second.

3. The aggregate is flat because a large internal transfer cancels. Load moves
   off the leaf-free interior and onto the few instances hosting root spans,
   which absorb mandatorily under BOTH rules and so cannot decline it.
"""
import csv, json
import numpy as np

SIM = '/mydata/uber/policy_probe_20260924/sim'
POLICIES = ['depth_cubic', 'upstream_pressure', 'inverse_depth']


def totals(key):
    return json.load(open(f'{SIM}/{key}.pressure.json'))['totals']


def instances(key):
    d = json.load(open(f'{SIM}/{key}.pressure.json'))
    g = lambda f: np.array([i[f] for i in d['instances']], float)
    return (g('num_spans'), g('num_checkpoint_spans'),
            g('combined_checkpoint_payload_bytes'), g('num_leaf_spans'), g('num_root_spans'))


def truss_counts(key):
    acc = man = 0
    with open(f'{SIM}/{key}.csv') as fh:
        for r in csv.DictReader(l for l in fh if not l.startswith('#')):
            acc += int(r['num_accepted_trusses'])
            man += int(r['num_mandatory_absorptions'])
    return acc, man


def main():
    print('1. absorbing at a scheduled checkpoint creates no new emitter\n')
    print(f'{"policy":18}{"arm":6}{"accepted":>13}{"mandatory":>13}{"mand%":>8}'
          f'{"new emitters":>14}{"trusses/emitter":>17}')
    for pol in POLICIES:
        for arm in ('stop', 'pass'):
            t = totals(f'{pol}_{arm}')
            acc, man = truss_counts(f'{pol}_{arm}')
            prom = t['num_promoted_checkpoint_spans']
            print(f'{pol:18}{arm:6}{acc:13,}{man:13,}{100*man/acc:7.1f}%{prom:14,}{acc/prom:17.2f}')
        print()

    print('2. total checkpoint bytes are conserved; the factorization changes\n')
    print(f'{"policy":18}{"arm":6}{"emitters":>13}{"ckpt bytes":>15}{"B/emission":>12}'
          f'{"reverse wire":>15}{"B/return edge":>15}')
    for pol in POLICIES:
        base = None
        for arm in ('stop', 'pass'):
            t = totals(f'{pol}_{arm}')
            e = t['num_checkpoint_spans']
            print(f'{pol:18}{arm:6}{e:13,}{t["combined_checkpoint_payload_bytes"]:15,}'
                  f'{t["combined_checkpoint_payload_bytes"]/e:12.1f}'
                  f'{t["reverse_encoded_baggage_bytes"]:15,}'
                  f'{t["reverse_encoded_baggage_bytes"]/t["num_reverse_return_edges"]:15.2f}')
            if base is None:
                base = t
            else:
                ce = e / base['num_checkpoint_spans']
                cw = ((t['combined_checkpoint_payload_bytes'] / e) /
                      (base['combined_checkpoint_payload_bytes'] / base['num_checkpoint_spans']))
                print(f'{"":24}{"pass/stop":>13}  emitters {ce:.4f} x weight {cw:.4f} '
                      f'= {ce*cw:.4f}   reverse wire '
                      f'{t["reverse_encoded_baggage_bytes"]/base["reverse_encoded_baggage_bytes"]:.2f}x')
        print()

    print('3. the flat aggregate hides a transfer onto the root-hosting instances\n')
    for pol in POLICIES:
        sp, cs, bs, lf, rt = instances(f'{pol}_stop')
        _, cp, bp, _, _ = instances(f'{pol}_pass')
        isroot = rt > 0
        leaffree = (lf / np.maximum(sp, 1) < .01) & ~isroot
        leafdom = lf / np.maximum(sp, 1) >= .8
        other = ~(isroot | leaffree | leafdom)
        print(f'  {pol}')
        print(f'    {"group":22}{"n":>7}{"% emitters":>12}{"% ckpt bytes":>14}'
              f'{"B/emit stop":>13}{"B/emit pass":>13}{"delta":>10}')
        for tag, sel in (('root-hosting', isroot), ('leaf-free, non-root', leaffree),
                         ('leaf-dominated', leafdom), ('everything else', other)):
            a, b = cs[sel], cp[sel]
            ws, wp = bs[sel].sum() / a.sum(), bp[sel].sum() / b.sum()
            print(f'    {tag:22}{int(sel.sum()):7}{100*a.sum()/cs.sum():11.1f}%'
                  f'{100*bs[sel].sum()/bs.sum():13.1f}%{ws:13.1f}{wp:13.1f}{wp-ws:+10.1f}')
        moved = bp[isroot].sum() - bs[isroot].sum()
        net = bp.sum() - bs.sum()
        print(f'    bytes moved onto root-hosting instances: {moved:+,.0f} '
              f'({100*moved/bs.sum():.1f}% of the corpus total) '
              f'against a NET change of {net:+,.0f}\n')


if __name__ == '__main__':
    main()
