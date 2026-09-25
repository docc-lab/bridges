"""Pooled day-1 + day-2 truss sizes -> the LaTeX bagsize table numbers.

  /mydata/uber/sim_day1_cpd19/cpd19_reverse_passthrough/sim
  /mydata/uber/sim_passthrough_day2/cpd19_reverse_passthrough/sim

1,364,579 traces (521,305 + 843,274), CPD 1:9 seed 42, depth_cubic,
--reverse-pass-checkpoints, engine 8cc104f.

The .hist.json files carry exact per-value bins, so pooling is an exact merge
(sum counts per value) and every percentile below is exact, not interpolated.
The two schemas differ: reverse_histograms bins key on 'value', the top-level
baggage_call_bytes histogram keys on 'bytes'.
"""
import json
from collections import defaultdict

DAYS = [('day1', '/mydata/uber/sim_day1_cpd19/cpd19_reverse_passthrough/sim'),
        ('day2', '/mydata/uber/sim_passthrough_day2/cpd19_reverse_passthrough/sim')]
MODES = [('pcrb', 'PB'), ('cgprb', 'CGPB'), ('sb3', 'SB')]
FIELDS = [('checkpoint payload', lambda d: d['reverse_histograms']['combined_checkpoint_payload_bytes']),
          ('forward baggage',    lambda d: d['baggage_call_bytes']),
          ('reverse baggage',    lambda d: d['reverse_histograms']['reverse_encoded_baggage_bytes'])]


def one(h):
    key = 'value' if 'value' in h['bins'][0] else 'bytes'
    return ({b[key]: b['count'] for b in h['bins']},
            h['sum'] if 'sum' in h else h['sum_bytes'],
            h['count'],
            h['max'] if 'max' in h else h['max_bytes'])


def merge(parts):
    bins = defaultdict(int)
    total = n = mx = 0
    for b, s, c, m in parts:
        for v, k in b.items():
            bins[v] += k
        total += s; n += c; mx = max(mx, m)
    assert sum(bins.values()) == n, 'bin counts disagree with the declared total'
    return bins, total, n, mx


def pct(bins, n, p):
    target, c = p * n, 0
    for v in sorted(bins):
        c += bins[v]
        if c >= target:
            return v
    return max(bins)


def compact(v):
    """Twelve columns of full-precision integers do not fit \\columnwidth at
    \\scriptsize, so large values get a K/M suffix."""
    if v >= 1e6:
        return f'{v/1e6:.2f}M'
    if v >= 1e5:
        return f'{v/1000:.0f}K'
    if v >= 2000:
        return f'{v/1000:.1f}K'
    return f'{v:.0f}'


def main():
    traces = sum(json.load(open(f'{p}/pcrb_reverse_pass.pressure.json'))['num_traces']
                 for _, p in DAYS)
    print(f'pooled traces: {traces:,}\n')
    print(f'{"bridge":6}{"field":21}{"n":>16}{"median":>9}{"mean":>9}{"p99":>9}{"p99.9":>10}{"max":>12}')
    rows = {}
    for m, lab in MODES:
        rows[lab] = {}
        for name, get in FIELDS:
            bins, total, n, mx = merge(
                [one(get(json.load(open(f'{p}/{m}_reverse_pass.hist.json')))) for _, p in DAYS])
            r = (pct(bins, n, .5), total / n, pct(bins, n, .99), pct(bins, n, .999), mx)
            rows[lab][name] = r
            print(f'{lab:6}{name:21}{n:16,}{r[0]:9,}{r[1]:9.1f}{r[2]:9,}{r[3]:10,}{r[4]:12,}')
        print()
    print('LaTeX rows (mn / p99 / p99.9 / max per group):')
    for lab, macro in (('PB', r'\PBridge'), ('CGPB', r'\CGBridge'), ('SB', r'\SBridge')):
        cells = []
        for name, _ in FIELDS:
            med, mean, p99, p999, mx = rows[lab][name]
            cells += [f'{mean:.0f}', compact(p99), compact(p999), compact(mx)]
        print(f'      {macro:9} & ' + ' & '.join(cells) + r' \\')


if __name__ == '__main__':
    main()
