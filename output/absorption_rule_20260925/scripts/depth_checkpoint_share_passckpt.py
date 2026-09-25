"""Checkpointing share by depth: does letting a truss pass a scheduled checkpoint
move load up the tree?

  /mydata/uber/passckpt_probe_20260924/sim/{forward,rev_stop,rev_pass}.pressure.json

50,000 day-1 traces, PB0, random CPD 1:9 seed 42, depth_cubic, leaf rejection
q=1. The three arms differ in exactly one thing each, so the deltas are
attributable:

  forward    no returns at all (baseline)
  rev STOP   returns absorbed at the first scheduled checkpoint above the origin
  rev PASS   --reverse-pass-checkpoints: only the trace root forces absorption

Deliberately NOT merged into depth_checkpoint_share.py: that figure is the full
day-2 corpus (843,274 traces) comparing checkpoint POLICIES, this one is a 50k
day-1 probe comparing absorption RULES. Different corpus, different question.

Colour = configuration, line style = arm (forward solid, reverse dashed).
"""
import json
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

OUT = Path(__file__).resolve().parent
SIM = Path('/mydata/uber/passckpt_probe_20260924/sim')
MAXD = 20

# Draw order is z-order: the rule we ship (rev STOP) goes on top.
ARMS = [
    ('+ reverse, pass ckpt', 'rev_pass', '#009e73', '--'),
    ('+ reverse, stop at ckpt', 'rev_stop', '#d55e00', '--'),
    ('forward', 'forward', '#0072b2', '-'),
]


def depths(key):
    d = json.loads((SIM / f'{key}.pressure.json').read_text())
    return {r['depth']: r for r in d['depths']}, d['num_traces']


def main():
    ds = list(range(0, MAXD + 1))
    series, traffic = [], None
    for lab, key, col, ls in ARMS:
        dep, ntr = depths(key)
        y = [100 * dep[d]['num_checkpoint_spans'] / dep[d]['num_spans']
             if d in dep and dep[d]['num_spans'] else 0 for d in ds]
        series.append((lab, y, col, ls))
        if key == 'forward':  # span counts are identical across arms
            tot = sum(r['num_spans'] for r in dep.values())
            traffic = ([100 * dep[d]['num_spans'] / tot if d in dep else 0 for d in ds], ntr)

    plt.rcParams.update({
        'font.size': 10, 'font.family': 'sans-serif',
        'axes.spines.top': False, 'axes.spines.right': False,
        'axes.edgecolor': '#c8c7c2', 'axes.labelcolor': '#0b0b0b',
        'xtick.color': '#0b0b0b', 'ytick.color': '#0b0b0b', 'axes.linewidth': .6,
    })
    fig, (a1, a2) = plt.subplots(2, 1, figsize=(3.33, 3.4), sharex=True,
                                 gridspec_kw={'height_ratios': [3.2, 1], 'hspace': .12})
    for i, (lab, y, col, ls) in enumerate(series):
        a1.plot(ds, y, color=col, lw=1.3, ls=ls, label=lab, zorder=2 + i)
    a1.set_ylim(0, 128)
    a1.set_yticks([0, 20, 50, 75, 100])
    a1.set_yticklabels(['0', '1/E[D]', '50', '75', '100'], fontsize=9)
    a1.set_ylabel('spans checkpointing (%)', fontsize=9.5)
    a1.grid(axis='y', color='#f0efec', lw=.6)
    a1.tick_params(labelsize=9, length=2, width=.6)
    keys = [Line2D([], [], color=c, lw=1.4, ls=s, label=l) for l, _, c, s in reversed(ARMS)]
    a1.legend(handles=keys, loc='upper center', frameon=False, fontsize=8,
              handlelength=1.8, labelspacing=.22, handletextpad=.5, borderaxespad=.15)

    y, ntr = traffic
    a2.plot(ds, y, color='#9a9994', lw=1.3)
    a2.fill_between(ds, y, color='#9a9994', alpha=.25)
    a2.set_ylabel('traffic (%)', fontsize=10)
    a2.set_xlabel('span depth in trace', fontsize=10)
    a2.set_xticks(range(0, MAXD + 1, 5))
    a2.grid(axis='y', color='#f0efec', lw=.6)
    a2.tick_params(labelsize=9, length=2, width=.6)

    fig.subplots_adjust(left=.20, right=.98, top=.985, bottom=.115)
    for ext in ('pdf', 'png'):
        fig.savefig(OUT / f'depth_checkpoint_share_passckpt.{ext}', dpi=300)

    print(f'{"depth":>6}{"traffic %":>11}' + ''.join(f'{l:>24}' for l, _, _, _ in reversed(ARMS)))
    for i, d in enumerate(ds):
        print(f'{d:>6}{y[i]:11.2f}' + ''.join(f'{s[1][i]:24.1f}' for s in reversed(series)))
    for lab, key, _, _ in ARMS:
        dep, _ = depths(key)
        num = sum(d * r['num_checkpoint_spans'] for d, r in dep.items())
        den = sum(r['num_checkpoint_spans'] for r in dep.values())
        print(f'{lab:24} emitters {den:12,}   mean depth of an emitter {num / den:.2f}')
    print(f'\n{ntr:,} traces')


if __name__ == '__main__':
    main()
