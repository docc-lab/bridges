"""Acceptance-policy trade-off, with and without mandatory absorption.

  /mydata/uber/policy_probe_20260924/sim/<policy>_{stop,pass}.csv

50,000 day-1 traces, PB0, random CPD 1:9 seed 42, leaf rejection q=1. Each
policy was run twice, identical but for --reverse-pass-checkpoints, so the
shift between the two curves is attributable to that flag alone.

  stop  a returning truss is absorbed at the first scheduled checkpoint above
        its origin (the shipped default)
  pass  only the trace root forces absorption; the policy decides everything

Axes follow the earlier trade-off figure: checkpoints per trace against the two
maxima that the policy family was selected to control. Lower-left is better on
both panels.

NOTE: 50k-trace probe, day 1. The full-corpus day-2 passthrough run is separate.
"""
import csv
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

OUT = Path(__file__).resolve().parent
SIM = Path('/mydata/uber/policy_probe_20260924/sim')

# The depth-weighted family is a curve through increasing exponent; the other
# two policies are isolated points and are drawn as such.
FAMILY = [('1/n', 'inverse_depth'), ('linear', 'depth_linear'), ('quad', 'depth_quadratic'),
          ('cubic', 'depth_cubic'), ('quartic', 'depth_quartic')]
SINGLES = [('upstream', 'upstream_pressure', '#9a9994', '^'), ('p=0.2', 'prob02', '#eb6834', 's')]
ARMS = [('stop at ckpt', 'stop', '#0072b2', '-', 'o', True),
        ('pass ckpt', 'pass', '#009e73', '--', 'o', False)]


def stats(key):
    n = cps = 0
    pay = rev = 0
    with (SIM / f'{key}.csv').open() as fh:
        for r in csv.DictReader(l for l in fh if not l.startswith('#')):
            n += 1
            cps += int(r['num_ckpt_spans'])
            pay = max(pay, int(r['max_combined_checkpoint_payload']))
            rev = max(rev, int(r['max_reverse_encoded_baggage']))
    return cps / n, rev, pay


def main():
    plt.rcParams.update({
        'font.size': 10, 'font.family': 'sans-serif',
        'axes.spines.top': False, 'axes.spines.right': False,
        'axes.edgecolor': '#c8c7c2', 'axes.labelcolor': '#0b0b0b',
        'xtick.color': '#0b0b0b', 'ytick.color': '#0b0b0b', 'axes.linewidth': .6,
    })
    fig, (a1, a2) = plt.subplots(2, 1, figsize=(3.33, 4.6), sharex=True,
                                 gridspec_kw={'hspace': .14})

    table = {}
    for ax, idx, ylab, annotate in ((a1, 1, 'largest return edge (B)', False),
                                    (a2, 2, 'largest checkpoint (B)', True)):
        for arm_lab, arm, col, ls, mk, filled in ARMS:
            xs, ys = [], []
            for lbl, pol in FAMILY:
                s = stats(f'{pol}_{arm}')
                table[(pol, arm)] = s
                xs.append(s[0]); ys.append(s[idx])
            ax.plot(xs, ys, ls, marker=mk, color=col, lw=1.2, ms=4,
                    mfc=col if filled else 'white', mew=1.1)
            if annotate:
                for (lbl, _), x, y in zip(FAMILY, xs, ys):
                    ax.annotate(lbl, (x, y), xytext=(3, 4), textcoords='offset points',
                                fontsize=6.5, color=col)
            for slab, pol, scol, smk in SINGLES:
                s = stats(f'{pol}_{arm}')
                table[(pol, arm)] = s
                ax.plot([s[0]], [s[idx]], smk, color=scol, ms=5.5,
                        mfc=scol if filled else 'white', mew=1.1)
                if annotate:
                    ax.annotate(slab, (s[0], s[idx]), xytext=(4, -9),
                                textcoords='offset points', fontsize=6.5, color=scol)
        ax.set_ylabel(ylab, fontsize=9, labelpad=2)
        ax.grid(color='#f0efec', lw=.6)
        ax.tick_params(labelsize=9, length=2, width=.6)

    a1.set_yscale('log')
    a2.set_ylim(0, None)
    a2.set_xlabel('checkpoints per trace', fontsize=10)
    # headroom on the right so the outermost annotation is not clipped
    lo, hi = a2.get_xlim()
    a2.set_xlim(lo, hi + 0.10 * (hi - lo))
    a1.text(.02, .06, 'lower-left is better', transform=a1.transAxes, fontsize=8, color='#52514e')

    keys = [Line2D([], [], color=c, ls=l, marker=m, ms=4, lw=1.2,
                   mfc=c if f else 'white', mew=1.1, label=lab)
            for lab, _, c, l, m, f in ARMS] + \
           [Line2D([], [], color=c, ls='', marker=m, ms=5.5, label=lab)
            for lab, _, c, m in SINGLES]
    fig.legend(handles=keys, loc='upper center', bbox_to_anchor=(.57, 1.0), ncol=2,
               frameon=False, fontsize=8, handlelength=1.6, columnspacing=.9,
               handletextpad=.4)
    fig.subplots_adjust(left=.25, right=.975, top=.90, bottom=.095)
    for ext in ('pdf', 'png'):
        fig.savefig(OUT / f'policy_tradeoff_passthrough.{ext}', dpi=300)

    print(f'{"policy":18}{"arm":6}{"ckpt/trace":>12}{"return max":>12}{"payload max":>13}')
    for lbl, pol in FAMILY + [(s[0], s[1]) for s in SINGLES]:
        for arm in ('stop', 'pass'):
            c, rv, py = table[(pol, arm)]
            print(f'{pol:18}{arm:6}{c:12.2f}{rv:12,}{py:13,}')


if __name__ == '__main__':
    main()
