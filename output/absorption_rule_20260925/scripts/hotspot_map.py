"""Where in the call topology does checkpoint load land?

  /mydata/uber/instdepth_probe_20260924/sim/<policy>_{stop,pass}.pressure.json

50,000 day-1 traces, PB0, random CPD 1:9 seed 42, leaf rejection q=1, depth_cubic.
Run with the span-depth moments, so every instance carries the mean and spread of
the tree positions it served.

A literal system topology coloured by heat is the picture one wants and is not a
figure. This is the same read in coordinates instead of a drawing: each of the
5,234 instances is placed by the two structural properties that define where it
sits, and the cell is coloured by how hard it is checkpointing.

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

Cells are traffic-weighted means over the instances that fall in them, and a cell
holding fewer than MIN_INST instances is left blank rather than drawn from noise.
The bottom edge (leaf share 0) is where the absorbers live; the left edge is the
root-hosting instances, already saturated under either rule because the trace root
absorbs mandatorily in both.

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
    sp, ck, lf, ds = f('num_spans'), f('num_checkpoint_spans'), f('num_leaf_spans'), f('span_depth_sum')
    m = sp >= FLOOR
    return sp[m], ck[m] / sp[m], lf[m] / sp[m], ds[m] / sp[m]


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
    sp, r_stop, leaf, depth = load('stop')
    _, r_pass, _, _ = load('pass')

    g_stop = grid(depth, leaf, r_stop, sp)
    g_pass = grid(depth, leaf, r_pass, sp)
    g_diff = g_pass - g_stop

    fig, (a1, a2, a3) = plt.subplots(1, 3, figsize=(6.9, 1.95), sharey=True)
    # bands are drawn equal-height; y is an index, not a linear leaf-share axis
    ext = [DBINS[0], DBINS[-1], 0, len(LBINS) - 1]
    kw = dict(origin='lower', aspect='auto', extent=ext, interpolation='nearest')

    im1 = a1.imshow(g_stop.T, cmap='magma_r', vmin=0, vmax=1, **kw)
    im2 = a2.imshow(g_pass.T, cmap='magma_r', vmin=0, vmax=1, **kw)
    lim = np.nanmax(np.abs(g_diff))
    im3 = a3.imshow(g_diff.T, cmap='RdBu_r', vmin=-lim, vmax=lim, **kw)

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

    for im, ax, lab in ((im2, a2, 'checkpoint rate'), (im3, a3, '$\\Delta$ rate')):
        cb = fig.colorbar(im, ax=ax, pad=.02, fraction=.055)
        cb.set_label(lab, fontsize=5.5, labelpad=1)
        cb.ax.tick_params(labelsize=5, length=1.5, width=.5, pad=1)

    fig.subplots_adjust(left=.062, right=.985, top=.87, bottom=.24, wspace=.16)
    for e in ('pdf', 'png'):
        fig.savefig(OUT / f'hotspot_map.{e}', dpi=300)

    print(f'{len(sp):,} instances with >= {FLOOR:,} spans, policy {POLICY}\n')
    print(f'{"leaf share":>14}{"n":>7}{"mean depth":>12}{"rate stop":>11}{"rate pass":>11}{"delta":>9}')
    for lo, hi in zip(LBINS, LBINS[1:]):
        s = (leaf >= lo) & (leaf < hi)
        if not s.any():
            continue
        w = sp[s]
        print(f'{f"{lo:.2f}-{hi:.2f}":>14}{int(s.sum()):7}{(depth[s]*w).sum()/w.sum():12.2f}'
              f'{(r_stop[s]*w).sum()/w.sum():11.3f}{(r_pass[s]*w).sum()/w.sum():11.3f}'
              f'{((r_pass[s]-r_stop[s])*w).sum()/w.sum():+9.3f}')
    print(f'\ncorr(rate_stop, depth) {np.corrcoef(r_stop, depth)[0,1]:+.3f}   '
          f'corr(rate_stop, leaf) {np.corrcoef(r_stop, leaf)[0,1]:+.3f}')
    print(f'corr(delta,     depth) {np.corrcoef(r_pass-r_stop, depth)[0,1]:+.3f}   '
          f'corr(delta,     leaf) {np.corrcoef(r_pass-r_stop, leaf)[0,1]:+.3f}')


if __name__ == '__main__':
    main()
