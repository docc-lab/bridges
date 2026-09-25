"""Which structural coordinate predicts where the absorption rule hurts.

  /mydata/uber/relpos_probe_20260924/sim   50k day-1 traces, PB, CPD 1:9 seed 42,
                                           leaf rejection q=1, run with the
                                           relative-position moments

Four candidate coordinates for an instance, all policy-independent:

  absolute depth              mean depth of its spans
  depth / trace depth         normalized against the deepest span ANYWHERE in
                              the trace -- the intuitive choice, and it does not
                              work; the deepest span may be down an unrelated
                              branch
  depth / (depth + height)    position along the span's OWN call path, so the
                              root is 0 and every leaf is exactly 1 whatever the
                              trace's shape or size
  leaf share                  fraction of its spans that are leaves

Correlated against the per-instance checkpoint rate under the default, and
against the CHANGE in that rate when scheduled checkpoints are passed. The
change is what a deployment would care about, and only the call-path form
predicts it.

Also reports how homogeneous an instance is in the coordinate: it only serves as
an instance-level coordinate if instances do not straddle wide ranges of it.
"""
import json
import numpy as np

SIM = '/mydata/uber/relpos_probe_20260924/sim'
POLICY = 'depth_cubic'
SCALE = 1000.0     # relative positions are stored as scaled integer moments
FLOOR = 1000       # spans, below which a rate is too few samples to be stable


def load(key):
    d = json.load(open(f'{SIM}/{key}.pressure.json'))
    g = lambda f: np.array([i[f] for i in d['instances']], float)
    return {f: g(f) for f in ('num_spans', 'num_checkpoint_spans', 'num_leaf_spans',
                              'span_depth_sum', 'rel_path_sum', 'rel_path_sq_sum',
                              'rel_trace_sum')}


def main():
    a, b = load(f'{POLICY}_stop'), load(f'{POLICY}_pass')
    m = a['num_spans'] >= FLOOR
    sp = a['num_spans'][m]
    rate_stop = a['num_checkpoint_spans'][m] / sp
    rate_pass = b['num_checkpoint_spans'][m] / sp
    delta = rate_pass - rate_stop

    mean_path = a['rel_path_sum'][m] / sp / SCALE
    sd_path = np.sqrt(np.maximum(a['rel_path_sq_sum'][m] / sp / SCALE / SCALE - mean_path**2, 0))
    coords = [('absolute depth', a['span_depth_sum'][m] / sp),
              ('depth / trace depth', a['rel_trace_sum'][m] / sp / SCALE),
              ('depth / (depth+height)', mean_path),
              ('leaf share', a['num_leaf_spans'][m] / sp)]

    print(f'{len(sp):,} instances with >= {FLOOR:,} spans, policy {POLICY}\n')
    print(f'{"coordinate":26}{"corr(rate_stop)":>18}{"corr(delta)":>14}')
    for name, x in coords:
        print(f'{name:26}{np.corrcoef(rate_stop, x)[0,1]:+18.3f}{np.corrcoef(delta, x)[0,1]:+14.3f}')

    print(f'\nwithin-instance spread of call-path position: '
          f'p50 sd {np.median(sd_path):.3f}, p90 {np.quantile(sd_path,.9):.3f}')
    print('an instance occupies a well-defined position, so its mean is usable\n')

    edges = np.linspace(0, 1, 11)
    for name, x in (('call-path position', mean_path), ('leaf share', coords[3][1])):
        h, _ = np.histogram(x, edges)
        print(f'instances per decile, {name}:')
        print('  ' + '  '.join(f'{v:5d}' for v in h))
    print('\nleaf share is bimodal, which is why it is the weaker coordinate:')
    print('a span with a short subtree below it lands mid-range on the call-path')
    print('axis but is lumped with the pure interior by leaf share.')


if __name__ == '__main__':
    main()
