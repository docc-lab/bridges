"""Pooled day-1 + day-2 reconstruction times.

  /mydata/uber/recon_final_cpd19_depth_cubic/day1  (30 shards)
  /mydata/uber/recon_final_cpd19_depth_cubic/day2  (50 shards)

CPD 1:9, depth_cubic, engine b53c0b0. NOTE this is the stop-at-checkpoint arm:
these runs predate --reverse-pass-checkpoints, so they do NOT match the byte
table, which is the passthrough arm at engine 8cc104f.

Reads every per-trace row rather than the sampled subset the violin figure
draws. Keeps feasible traces with recon_ns > 0, matching recon_error_time.py.
recon_ns excludes I/O by construction (shards staged in tmpfs).

Each trace contributes up to one reconstruction per drop rate, so the pooled n
counts reconstructions, not traces, and the pooled mean is n-weighted across
drop rates.
"""
import numpy as np
from pathlib import Path

DAYS = [(Path('/mydata/uber/recon_final_cpd19_depth_cubic/day1'), 30),
        (Path('/mydata/uber/recon_final_cpd19_depth_cubic/day2'), 50)]
MODES = [('pb0', 'PB'), ('cgp0', 'CGPB'), ('sb3', 'SB')]
CODES = [('d005', '0.05'), ('d025', '0.25'), ('d05', '0.5'),
         ('d075', '0.75'), ('d095', '0.95'), ('d10', '1.0')]


def times_ms(mode, arm, codes):
    out = []
    for code in codes:
        for root, nsh in DAYS:
            for s in range(nsh):
                p = root / 'recon' / f'{mode}_{arm}_shard{s:02d}_{code}.csv'
                with p.open() as fh:
                    head = fh.readline().rstrip('\n').split(',')
                    fi, ni = head.index('feasible'), head.index('recon_ns')
                    for line in fh:
                        f = line.split(',')
                        if f[fi] == '1':
                            v = int(f[ni])
                            if v > 0:
                                out.append(v)
    return np.array(out, dtype=np.float64) / 1e6


def main():
    for arm in ('reverse', 'forward'):
        print(f'=== CPD 1:9, {arm} arm, day1+day2, per drop rate ===')
        print(f'{"bridge":6}{"drop":6}{"n":>12}{"mean ms":>10}{"p50 ms":>10}{"p99 ms":>10}')
        for m, lab in MODES:
            for code, dl in CODES:
                t = times_ms(m, arm, [code])
                print(f'{lab:6}{dl:6}{t.size:12,}{t.mean():10.3f}'
                      f'{np.quantile(t,.5):10.3f}{np.quantile(t,.99):10.2f}')
            print()
        print(f'=== CPD 1:9, {arm} arm, ALL six drop rates pooled ===')
        print(f'{"bridge":6}{"n":>13}{"mean ms":>10}{"p50":>9}{"p90":>9}{"p99":>9}{"p99.9":>11}{"max":>10}')
        for m, lab in MODES:
            t = times_ms(m, arm, [c for c, _ in CODES])
            q = lambda x: np.quantile(t, x)
            print(f'{lab:6}{t.size:13,}{t.mean():10.3f}{q(.5):9.3f}{q(.9):9.3f}'
                  f'{q(.99):9.2f}{q(.999):11.2f}{t.max():10.1f}')
            del t
        print()


if __name__ == '__main__':
    main()
