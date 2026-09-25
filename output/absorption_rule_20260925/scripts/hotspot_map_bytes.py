"""Where in the call topology do checkpoint BYTES land?

  /mydata/uber/instdepth_probe_20260924/sim/<policy>_{stop,pass}.pressure.json

50,000 day-1 traces, PB0, random CPD 1:9 seed 42, leaf rejection q=1, depth_cubic.
Run with the span-depth moments, so every instance carries the mean and spread of
the tree positions it served.

The byte companion to hotspot_map.py, on identical axes so the two can be read
against each other. That figure colours by how MANY of an instance's spans emit;
this one colours by how HEAVY each emission is.

    weight = combined_checkpoint_payload_bytes / num_checkpoint_spans

The two are factors of a conserved product -- total bytes are fixed at ~906 MB
because every returned truss is absorbed exactly once whatever the rule -- so the
change panels are mirror images: passing reddens the rate map and blues this one.
Stop absorbs at the first scheduled checkpoint above the origin, a span that was
already going to emit, so its cost lands in weight rather than count; 5.93 trusses
pile onto each new emitter for cubic. Passing moves that cost from weight to
count.

  x  mean depth of the instance's spans -- how far down the tree it sits
  y  leaf share of its spans            -- how terminal its work is

The y bands are deliberately unequal, because leaf share is bimodal: 2,950 of the
5,234 instances are below 1% leaf and 2,008 are above 80%, with only 276 in
between. Instances are almost entirely either absorbers or originators, and a
uniform grid would spend most of its area on an empty middle. Band heights are
drawn equal and the population of each is printed on it, so the reader is not
misled into reading area as mass.

Those two axes are not decoration. Under q=1 a leaf originates a truss and never
absorbs one, so leaf share is the single predictor of how much passing costs an
instance (corr -0.365, against +0.019 for depth and -0.014 for root share). The
map is built so that the negative result is visible too: the heat does not sort
left-to-right.

  (a) stop at ckpt   (b) pass ckpt   (c) the change

Cells are checkpoint-weighted means over the instances that fall in them -- an
instance's weight is divided by its own emissions, so the cell mean is weighted by
emissions, not by spans. A cell holding fewer than MIN_INST instances is left
blank rather than drawn from noise. The colour scale is logarithmic because weight
spans 16 B (a bare forward checkpoint with no truss attached) to several hundred.

NOTE: instances are the supplied modeled queue/endpoint-instance slots from the
per-event sidecar, not observed physical servers. 50k day-1 probe.
"""
import json
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

OUT = Path(__file__).resolve().parent
SIM = Path('/mydata/uber/instdepth_probe_20260924/sim')
POLICY = 'depth_cubic'
FLOOR = 1000        # spans, below which a rate is too few samples to be stable
MIN_INST = 5        # instances per cell, below which the cell is left blank
DBINS = np.linspace(0, 24, 17)
# matched to the bimodal population, not to the unit interval
LBINS = np.array([0, .01, .2, .5, .8, 1.001])
LLAB = ['0', '<1%', '20%', '50%', '80%', '100%']


def load(arm):
    d = json.loads((SIM / f'{POLICY}_{arm}.pressure.json').read_text())
    f = lambda n: np.array([i[n] for i in d['instances']], float)
    sp, ck, by = f('num_spans'), f('num_checkpoint_spans'), f('combined_checkpoint_payload_bytes')
    lf, ds = f('num_leaf_spans'), f('span_depth_sum')
    m = (sp >= FLOOR) & (ck > 0)
    return ck[m], by[m] / ck[m], lf[m] / sp[m], ds[m] / sp[m]


def grid(depth, leaf, val, weight):
    """Traffic-weighted mean of val per (depth, leaf) cell; NaN where too sparse."""
    num, _, _ = np.histogram2d(depth, leaf, [DBINS, LBINS], weights=val * weight)
    den, _, _ = np.histogram2d(depth, leaf, [DBINS, LBINS], weights=weight)
    cnt, _, _ = np.histogram2d(depth, leaf, [DBINS, LBINS])
    out = np.divide(num, den, out=np.full_like(num, np.nan), where=den > 0)
    out[cnt < MIN_INST] = np.nan
    return out


def main():
    plt.rcParams.update({
        'font.size': 7, 'font.family': 'sans-serif',
        'axes.spines.top': False, 'axes.spines.right': False,
        'axes.edgecolor': '#c8c7c2', 'axes.labelcolor': '#0b0b0b',
        'xtick.color': '#0b0b0b', 'ytick.color': '#0b0b0b', 'axes.linewidth': .6,
    })
    ck, w_stop, leaf, depth = load('stop')
    ck_p, w_pass, _, _ = load('pass')

    g_stop = grid(depth, leaf, w_stop, ck)
    g_pass = grid(depth, leaf, w_pass, ck_p)
    g_diff = g_pass - g_stop

    fig, (a1, a2, a3) = plt.subplots(1, 3, figsize=(6.9, 1.95), sharey=True)
    # bands are drawn equal-height; y is an index, not a linear leaf-share axis
    ext = [DBINS[0], DBINS[-1], 0, len(LBINS) - 1]
    kw = dict(origin='lower', aspect='auto', extent=ext, interpolation='nearest')

    norm = matplotlib.colors.LogNorm(vmin=16, vmax=400)
    im1 = a1.imshow(g_stop.T, cmap='magma_r', norm=norm, **kw)
    im2 = a2.imshow(g_pass.T, cmap='magma_r', norm=norm, **kw)
    # One cell -- the root-hosting instances at depth 0 -- moves an order of
    # magnitude more than any other and would flatten the rest to white. The scale
    # is set from the bulk and that cell is allowed to saturate, labelled with its
    # true value so nothing is hidden.
    lim = float(np.nanpercentile(np.abs(g_diff), 92))
    im3 = a3.imshow(g_diff.T, cmap='RdBu_r', vmin=-lim, vmax=lim, **kw)
    fi, fj = np.unravel_index(np.nanargmax(np.abs(g_diff)), g_diff.shape)
    a3.annotate(f'{g_diff[fi, fj]:+.0f} B', (DBINS[fi] + .6, fj + .55), fontsize=5,
                color='white', va='bottom', ha='left')

    # how many instances fall in each band, so equal height is not read as equal mass
    pop = [int(((leaf >= lo) & (leaf < hi)).sum()) for lo, hi in zip(LBINS, LBINS[1:])]
    for i, n in enumerate(pop):
        a1.annotate(f'n={n:,}', (.4, i + .5), fontsize=5, color='#3a3a38',
                    va='center', ha='left')

    for ax, t in ((a1, '(a) stop at ckpt'), (a2, '(b) pass ckpt'), (a3, '(c) change')):
        ax.set_title(t, fontsize=6.5, pad=2)
        ax.set_xlabel('mean depth of the instance', fontsize=6.5, labelpad=1)
        ax.set_yticks(range(len(LBINS)))
        ax.set_yticklabels(LLAB, fontsize=5.5)
        ax.tick_params(labelsize=6, length=2, width=.6, pad=1.5)
        ax.set_facecolor('#f4f3f0')
    a1.set_ylabel('leaf share of its spans', fontsize=6.5, labelpad=1)

    for im, ax, lab in ((im2, a2, 'bytes per emission'), (im3, a3, '$\\Delta$ bytes')):
        cb = fig.colorbar(im, ax=ax, pad=.02, fraction=.055)
        cb.set_label(lab, fontsize=5.5, labelpad=1)
        cb.ax.tick_params(labelsize=5, length=1.5, width=.5, pad=1)

    fig.subplots_adjust(left=.062, right=.985, top=.87, bottom=.24, wspace=.16)
    for e in ('pdf', 'png'):
        fig.savefig(OUT / f'hotspot_map_bytes.{e}', dpi=300)

    print(f'{len(ck):,} instances with >= {FLOOR:,} spans and >=1 emission, policy {POLICY}\n')
    print(f'{"leaf share":>14}{"n":>7}{"mean depth":>12}{"wt stop":>11}{"wt pass":>11}{"delta":>9}')
    for lo, hi in zip(LBINS, LBINS[1:]):
        s = (leaf >= lo) & (leaf < hi)
        if not s.any():
            continue
        a, b = ck[s], ck_p[s]
        print(f'{f"{lo:.2f}-{hi:.2f}":>14}{int(s.sum()):7}{(depth[s]*a).sum()/a.sum():12.2f}'
              f'{(w_stop[s]*a).sum()/a.sum():11.1f}{(w_pass[s]*b).sum()/b.sum():11.1f}'
              f'{(w_pass[s]*b).sum()/b.sum()-(w_stop[s]*a).sum()/a.sum():+9.1f}')
    print('\nmost extreme cells (depth band, leaf band): delta bytes per emission')
    idx = np.dstack(np.unravel_index(np.argsort(-np.abs(np.nan_to_num(g_diff)), axis=None),
                                     g_diff.shape))[0][:5]
    for i, j in idx:
        print(f'  depth {DBINS[i]:5.1f}-{DBINS[i+1]:5.1f}  leaf {LLAB[j]:>5}-{LLAB[j+1]:<5}'
              f'  stop {g_stop[i,j]:7.1f}  pass {g_pass[i,j]:7.1f}  delta {g_diff[i,j]:+8.1f}')
    print(f'\ncorr(wt_stop, depth) {np.corrcoef(w_stop, depth)[0,1]:+.3f}   '
          f'corr(wt_stop, leaf) {np.corrcoef(w_stop, leaf)[0,1]:+.3f}')
    print(f'corr(delta,   depth) {np.corrcoef(w_pass-w_stop, depth)[0,1]:+.3f}   '
          f'corr(delta,   leaf) {np.corrcoef(w_pass-w_stop, leaf)[0,1]:+.3f}')


if __name__ == '__main__':
    main()
