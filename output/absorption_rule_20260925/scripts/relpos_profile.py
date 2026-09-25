"""Checkpoint load against a span's relative position on its own call path.

  /mydata/uber/relpos_probe_20260924/sim/<policy>_{stop,pass}.pressure.json

50,000 day-1 traces, PB0, random CPD 1:9 seed 42, leaf rejection q=1. Each policy
run twice, identical but for --reverse-pass-checkpoints.

The coordinate is

    pos = depth / (depth + height),   height = longest path to a leaf below it

so the trace root is exactly 0 and every leaf is exactly 1 whatever the shape or
size of the trace. It replaces the two weak axes of the earlier map. Against the
per-instance change in checkpoint rate it correlates -0.650, versus -0.365 for
leaf share, +0.019 for absolute depth and +0.007 for depth/trace-depth -- that
last being the reason to normalize along the path rather than against the deepest
span anywhere in the trace, which washes the signal out.

SPANS are binned here, not instances. An instance carries only a mean position,
and pooling a rate over an instance blends the positions it serves: the ten
instances whose mean position is under 0.05 handle 83.5% root spans at rate 1.0
and 16.5% near-root spans at rate 0.165, so their pooled rate reads 0.862 and the
root appears not to checkpoint always. It does -- all 50,000 roots checkpoint in
every arm. Binning the spans themselves makes bin 0 exactly the roots and the
last bin exactly the leaves, which the tests assert.

  (a) how many of its spans emit
  (b) how heavy each emission is

Together they are the conserved trade: total checkpoint bytes differ by +0.5%
between the arms, so what rises in (a) must fall in (b). The exception is the
left edge, where the trace root absorbs mandatorily under BOTH rules and so
cannot decline what passing sends it -- the only place both panels rise.

NOTE: instances are the supplied modeled queue/endpoint-instance slots from the
per-event sidecar, not observed physical servers. 50k day-1 probe.
"""
import json
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import numpy as np

OUT = Path(__file__).resolve().parent
SIM = Path('/mydata/uber/relbin_probe_20260924/sim')
SCALE = 1000.0
FLOOR = 1000
BINS = np.linspace(0, 1, 21)
POLICIES = [('depth_cubic', 'cubic', '#0072b2'), ('upstream_pressure', 'upstream', '#009e73')]
ARMS = [('stop', '-'), ('pass', '--')]


def load(key):
    d = json.loads((SIM / f'{key}.pressure.json').read_text())
    rows = sorted(d['rel_path_bins'], key=lambda r: r['bin'])
    mid = np.array([(r['from'] + r['to']) / 2 for r in rows])
    sp = np.array([r['num_spans'] for r in rows], float)
    ck = np.array([r['num_checkpoint_spans'] for r in rows], float)
    by = np.array([r['combined_checkpoint_payload_bytes'] for r in rows], float)
    rate = np.divide(ck, sp, out=np.full_like(sp, np.nan), where=sp > 0)
    wt = np.divide(by, ck, out=np.full_like(by, np.nan), where=ck > 0)
    return mid, sp, rate, wt


def main():
    plt.rcParams.update({
        'font.size': 7, 'font.family': 'sans-serif',
        'axes.spines.top': False, 'axes.spines.right': False,
        'axes.edgecolor': '#c8c7c2', 'axes.labelcolor': '#0b0b0b',
        'xtick.color': '#0b0b0b', 'ytick.color': '#0b0b0b', 'axes.linewidth': .6,
    })
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(3.33, 1.7))

    tab = {}
    for pol, lab, col in POLICIES:
        for arm, ls in ARMS:
            mid, sp, r, w = load(f'{pol}_{arm}')
            tab[(pol, arm)] = (mid, sp, r, w)
            a1.plot(mid, r, color=col, ls=ls, lw=1.1)
            a2.plot(mid, w, color=col, ls=ls, lw=1.1)

    a1.set_ylabel('checkpoint rate', fontsize=6.5, labelpad=1)
    a1.set_ylim(0, 1)
    a2.set_ylabel('bytes per emission', fontsize=6.5, labelpad=1)
    a2.set_yscale('log')
    for ax, t in ((a1, '(a) how many emit'), (a2, '(b) how heavy')):
        ax.set_xlim(0, 1)
        ax.set_xticks([0, .5, 1])
        ax.set_xticklabels(['root\n0', '0.5', 'leaf\n1'], fontsize=6)
        ax.set_xlabel('position on its call path', fontsize=6.5, labelpad=1)
        ax.set_title(t, fontsize=6.5, pad=2)
        ax.grid(color='#f0efec', lw=.5)
        ax.tick_params(labelsize=6, length=2, width=.6, pad=1.5)

    keys = [Line2D([], [], color=c, lw=1.1, label=l) for _, l, c in POLICIES] + \
           [Line2D([], [], color='#52514e', lw=1.1, ls=s,
                   label={'stop': 'stop', 'pass': 'pass'}[a]) for a, s in ARMS]
    fig.legend(handles=keys, loc='lower center', ncol=4, frameon=False, fontsize=5.5,
               handlelength=1.3, columnspacing=.8, handletextpad=.35,
               bbox_to_anchor=(.55, -.035))
    fig.subplots_adjust(left=.13, right=.96, top=.88, bottom=.36, wspace=.36)
    for e in ('pdf', 'png'):
        fig.savefig(OUT / f'relpos_profile.{e}', dpi=300)

    for pol, lab, _ in POLICIES:
        print(f'\n{lab}   (spans binned by call-path position)')
        print(f'{"position":>12}{"spans":>13}{"rate stop":>11}{"rate pass":>11}{"delta":>9}'
              f'{"wt stop":>10}{"wt pass":>10}')
        mid, sp, r0, w0 = tab[(pol, 'stop')]
        _, _, r1, w1 = tab[(pol, 'pass')]
        for i in range(len(mid)):
            if not sp[i]:
                continue
            print(f'{mid[i]-.025:6.2f}-{mid[i]+.025:<5.2f}{sp[i]:13,.0f}{r0[i]:11.4f}'
                  f'{r1[i]:11.4f}{r1[i]-r0[i]:+9.4f}{w0[i]:10.1f}{w1[i]:10.1f}')


if __name__ == '__main__':
    main()
