"""Shipped config against the candidate, at both checkpoint densities.

  A = depth_cubic       + stop  (shipped)
  B = upstream_pressure + pass  (candidate)

Same corpus, seed and flags; 50,000 day-1 traces, PB0, leaf rejection q=1.
upstream_pressure is the only policy whose acceptance probability RISES as a
truss climbs, and the only one where passing reduces the worst checkpoint.
"""
import csv, json
import numpy as np

A = ('depth_cubic_stop', 'cubic + stop  (shipped)')
B = ('upstream_pressure_pass', 'upstream + pass (candidate)')
RUNS = [('1:9  E[D]=5', 'relbin_probe_20260924'),
        ('1:21 E[D]=11', 'cpd21_probe_20260924')]
FIELDS = ['max_combined_checkpoint_payload', 'max_reverse_encoded_baggage',
          'max_accepted_trusses', 'num_accepted_trusses', 'num_mandatory_absorptions']
FLOOR = 1000   # spans, below which a per-instance rate is too few samples


def load(d, key):
    t = json.load(open(f'/mydata/uber/{d}/sim/{key}.pressure.json'))
    out = {f: [] for f in FIELDS}
    with open(f'/mydata/uber/{d}/sim/{key}.csv') as fh:
        for r in csv.DictReader(l for l in fh if not l.startswith('#')):
            for f in FIELDS:
                out[f].append(int(r[f]))
    return t, {f: np.array(v, float) for f, v in out.items()}


def inst_rate(t):
    sp = np.array([i['num_spans'] for i in t['instances']], float)
    ck = np.array([i['num_checkpoint_spans'] for i in t['instances']], float)
    m = sp >= FLOOR
    return ck[m] / sp[m]


def main():
    for lab, d in RUNS:
        ta, ca = load(d, A[0])
        tb, cb = load(d, B[0])
        Ta, Tb = ta['totals'], tb['totals']
        n = Ta['num_spans']
        ra, rb = inst_rate(ta), inst_rate(tb)
        q = lambda v, x: np.quantile(v, x)
        rows = [
            ('checkpoint spans', Ta['num_checkpoint_spans'], Tb['num_checkpoint_spans'], '{:,.0f}'),
            ('  as % of spans', 100*Ta['num_checkpoint_spans']/n, 100*Tb['num_checkpoint_spans']/n, '{:.1f}%'),
            ('checkpoint bytes', Ta['combined_checkpoint_payload_bytes'], Tb['combined_checkpoint_payload_bytes'], '{:,.0f}'),
            ('  bytes per emission', Ta['combined_checkpoint_payload_bytes']/Ta['num_checkpoint_spans'],
                                     Tb['combined_checkpoint_payload_bytes']/Tb['num_checkpoint_spans'], '{:.1f}'),
            ('reverse wire bytes', Ta['reverse_encoded_baggage_bytes'], Tb['reverse_encoded_baggage_bytes'], '{:,.0f}'),
            ('  bytes per return edge', Ta['reverse_encoded_baggage_bytes']/Ta['num_reverse_return_edges'],
                                        Tb['reverse_encoded_baggage_bytes']/Tb['num_reverse_return_edges'], '{:.2f}'),
            ('forward baggage bytes', Ta['forward_baggage_bytes'], Tb['forward_baggage_bytes'], '{:,.0f}'),
            ('mandatory absorption', 100*ca['num_mandatory_absorptions'].sum()/ca['num_accepted_trusses'].sum(),
                                     100*cb['num_mandatory_absorptions'].sum()/cb['num_accepted_trusses'].sum(), '{:.1f}%'),
            ('worst ckpt payload p50', q(ca['max_combined_checkpoint_payload'], .5), q(cb['max_combined_checkpoint_payload'], .5), '{:,.0f}'),
            ('                   p99', q(ca['max_combined_checkpoint_payload'], .99), q(cb['max_combined_checkpoint_payload'], .99), '{:,.0f}'),
            ('                 p99.9', q(ca['max_combined_checkpoint_payload'], .999), q(cb['max_combined_checkpoint_payload'], .999), '{:,.0f}'),
            ('worst pile-up      p99', q(ca['max_accepted_trusses'], .99), q(cb['max_accepted_trusses'], .99), '{:,.0f}'),
            ('worst return edge  p99', q(ca['max_reverse_encoded_baggage'], .99), q(cb['max_reverse_encoded_baggage'], .99), '{:,.0f}'),
            ('instance ckpt rate p50', q(ra, .5), q(rb, .5), '{:.3f}'),
            ('                   p99', q(ra, .99), q(rb, .99), '{:.3f}'),
            ('                   max', ra.max(), rb.max(), '{:.3f}'),
        ]
        print('=' * 104)
        print(f'{lab}   50,000 day-1 traces, PB0, seed 42, q=1')
        print(f'{"":28}{A[1]:>26}{B[1]:>28}{"B/A":>12}')
        for name, x, y, fmt in rows:
            print(f'{name:28}{fmt.format(x):>26}{fmt.format(y):>28}{y/x if x else float("nan"):12.2f}')
        print()


if __name__ == '__main__':
    main()
