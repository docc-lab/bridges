"""Checkpoint density sweep: does passthrough stop losing when checkpoints are rare?

  /mydata/uber/relbin_probe_20260924   CPD 1:9   E[D]=5
  /mydata/uber/cpd21_probe_20260924    CPD 1:21  E[D]=11

50,000 day-1 traces each, PB0 (pcrb), seed 42, leaf rejection q=1. Six policies,
each run twice, identical but for --reverse-pass-checkpoints.

1:21 needs the two-byte context and the payload distance byte (commit 4a6f428);
before that the one-byte context capped ranges at 1:16.
"""
import csv, json
import numpy as np

POL = ['inverse_depth', 'depth_linear', 'depth_quadratic',
       'depth_cubic', 'depth_quartic', 'upstream_pressure']
RUNS = [('1:9  E[D]=5', 'relbin_probe_20260924'),
        ('1:21 E[D]=11', 'cpd21_probe_20260924')]
FIELDS = ['max_combined_checkpoint_payload', 'max_reverse_encoded_baggage',
          'max_accepted_trusses', 'num_accepted_trusses', 'num_mandatory_absorptions']


def totals(d, key):
    return json.load(open(f'/mydata/uber/{d}/sim/{key}.pressure.json'))['totals']


def cols(d, key):
    out = {f: [] for f in FIELDS}
    with open(f'/mydata/uber/{d}/sim/{key}.csv') as fh:
        for r in csv.DictReader(l for l in fh if not l.startswith('#')):
            for f in FIELDS:
                out[f].append(int(r[f]))
    return {f: np.array(v, float) for f, v in out.items()}


def main():
    for lab, d in RUNS:
        print('=' * 118)
        print(f'{lab}    50,000 day-1 traces, PB0, seed 42, leaf-reject q=1')
        print(f'{"policy":18}{"arm":6}{"ckpt spans":>12}{"%spans":>8}{"B/emit":>8}'
              f'{"revwire B/edge":>15}{"mand%":>7} | p99 {"payload":>10}{"pile-up":>9}'
              f'{"rev bag":>10} | {"pass/stop p99 payload":>22}')
        for pol in POL:
            r = {}
            for arm in ('stop', 'pass'):
                t, c = totals(d, f'{pol}_{arm}'), cols(d, f'{pol}_{arm}')
                e, n = t['num_checkpoint_spans'], t['num_spans']
                q = lambda f: np.quantile(c[f], .99)
                r[arm] = q('max_combined_checkpoint_payload')
                ratio = f'{r["pass"]/r["stop"]:22.2f}' if arm == 'pass' else ''
                print(f'{pol:18}{arm:6}{e:12,}{100*e/n:7.1f}%'
                      f'{t["combined_checkpoint_payload_bytes"]/e:8.1f}'
                      f'{t["reverse_encoded_baggage_bytes"]/t["num_reverse_return_edges"]:15.2f}'
                      f'{100*c["num_mandatory_absorptions"].sum()/c["num_accepted_trusses"].sum():6.1f}% |'
                      f'{q("max_combined_checkpoint_payload"):15,.0f}'
                      f'{q("max_accepted_trusses"):9,.0f}{q("max_reverse_encoded_baggage"):10,.0f}'
                      f' |{ratio}')
            print()


if __name__ == '__main__':
    main()
