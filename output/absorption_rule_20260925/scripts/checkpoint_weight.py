"""Checkpoint WEIGHT per instance and per service: how heavy is one emission?
checkpoints spread the load, or concentrate it?

  /mydata/uber/policy_probe_20260924/sim/<policy>_{stop,pass}.pressure.json

50,000 day-1 traces, PB0, random CPD 1:9 seed 42, leaf rejection q=1. Each
policy run twice, identical but for --reverse-pass-checkpoints.

The companion to checkpoint_rate.py. That figure plots how MANY of a unit's spans
emit; this one plots how HEAVY each emission is.

    weight(u) = combined_checkpoint_payload_bytes(u) / num_checkpoint_spans(u)

The two are the factors of a conserved product: total bytes = (spans that emit) x
(bytes per emission), and the total is fixed at ~906 MB because every returned
truss is absorbed exactly once whatever the rule. So the two figures necessarily
move in opposite directions, and reading either alone gives the wrong answer.

Passing RAISES the rate and LOWERS the weight. Under stop a truss is absorbed at
the first scheduled checkpoint above its origin -- a span that was already going
to emit -- so absorption is free in rate terms and is paid for in weight instead:
5.93 trusses pile onto each new emitter for cubic, 12.54 for upstream_pressure.
Passing lets the truss reach an ordinary span, which must be promoted, so the
cost moves from weight to rate.

Which rule is better therefore depends entirely on what is expensive. Per-emission
overhead -- an export call, an attribute write, a span-processor hop -- favours
stop. Per-emission size limits favour passing. Neither is what actually sinks
passing, which is a 3x rise in reverse WIRE bytes: transport, not emission, and
absent from both figures.

Mechanism: mandatory absorption at a scheduled checkpoint is a fan-in limiter. It
bounds a truss to the window its origin leaf started in, so a subtree's returns
split across many absorbers. Remove it and a truss climbs until the policy
accepts; ancestors higher up subtend exponentially larger subtrees, so whoever
accepts high absorbs that whole subtree's returns and saturates. The shallow
-biased policies move most (upstream p99 0.540 -> 0.950) because they were the
ones the cap was overriding most often.

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
SIM = Path('/mydata/uber/policy_probe_20260924/sim')
# Both floors are span counts, not row counts; services are larger, so the same
# "enough spans for the fraction to be stable" argument sets a higher bar.
FLOOR = {'instances': 1000, 'services': 5000}

POLICIES = [('depth_cubic', 'cubic'), ('depth_linear', 'linear'),
            ('depth_quartic', 'quartic'), ('upstream_pressure', 'upstream'),
            ('inverse_depth', '1/n')]
ARMS = [('stop', 'stop at ckpt', '#0072b2'), ('pass', 'pass ckpt', '#e69f00')]
SHOW = 'depth_cubic'


def weights(key, unit):
    d = json.loads((SIM / f'{key}.pressure.json').read_text())
    sp = np.array([i['num_spans'] for i in d[unit]], float)
    ck = np.array([i['num_checkpoint_spans'] for i in d[unit]], float)
    by = np.array([i['combined_checkpoint_payload_bytes'] for i in d[unit]], float)
    m = (sp >= FLOOR[unit]) & (ck > 0)
    return by[m] / ck[m]


def main():
    plt.rcParams.update({
        'font.size': 7, 'font.family': 'sans-serif',
        'axes.spines.top': False, 'axes.spines.right': False,
        'axes.edgecolor': '#c8c7c2', 'axes.labelcolor': '#0b0b0b',
        'xtick.color': '#0b0b0b', 'ytick.color': '#0b0b0b', 'axes.linewidth': .6,
    })
    fig, (a1, a2, a3) = plt.subplots(1, 3, figsize=(6.9, 1.85))

    data = {(p, a, u): weights(f'{p}_{a}', u)
            for p, _ in POLICIES for a, _, _ in ARMS for u in FLOOR}

    bins = np.logspace(np.log10(20), np.log10(2000), 46)
    for ax, unit, tag in ((a1, 'instances', 'instance'), (a2, 'services', 'service')):
        for arm, lab, col in ARMS:
            v = data[(SHOW, arm, unit)]
            ax.hist(v, bins=bins, histtype='stepfilled', color=col, alpha=.45, lw=0, zorder=2)
            ax.hist(v, bins=bins, histtype='step', color=col, lw=1.0, zorder=3)
        n = len(data[(SHOW, 'stop', unit)])
        ax.set_yscale('log')
        ax.set_xscale('log')
        ax.set_xlim(20, 2000)
        ax.set_xlabel(f'bytes per emission, one {tag}', fontsize=6.5, labelpad=1)
        ax.set_ylabel(f'{tag}s', fontsize=6.5, labelpad=1)
        ax.set_title(f'({"ab"[unit == "services"]}) per {tag}, cubic  n={n:,}',
                     fontsize=6.5, pad=2)
    a2.axvspan(.8, 1.0, color='#d55e00', alpha=.07, zorder=1)
    a2.annotate('empty', (.79, 22), fontsize=5.5, color='#a33', ha='right')
    a1.legend(handles=[Line2D([], [], color=c, lw=1.2, label=l) for _, l, c in ARMS],
              frameon=False, fontsize=5.5, handlelength=1.1, handletextpad=.4,
              borderaxespad=.1, loc='upper right')

    x = np.arange(len(POLICIES))
    w = .36
    for j, (arm, lab, col) in enumerate(ARMS):
        a3.bar(x + (j - .5) * w, [np.quantile(data[(p, arm, 'instances')], .99) for p, _ in POLICIES],
               w, color=col, zorder=3, edgecolor='white', linewidth=.4, bottom=1)
        a3.scatter(x + (j - .5) * w,
                   [np.quantile(data[(p, arm, 'services')], .99) for p, _ in POLICIES],
                   marker='_', s=34, color='#0b0b0b', linewidths=1.0, zorder=5)
    a3.set_yscale('log')
    a3.set_xticks(x)
    a3.set_xticklabels([l for _, l in POLICIES], fontsize=6, rotation=35, ha='right')
    a3.set_ylabel('99th pct bytes per emission', fontsize=6.5, labelpad=1)
    a3.set_title('(c) heaviest emissions, every policy', fontsize=6.5, pad=2)
    a3.legend(handles=[Line2D([], [], color='#0b0b0b', ls='', marker='_', ms=5,
                              markeredgewidth=1.0, label='per service')],
              frameon=False, fontsize=5.5, handlelength=.9, handletextpad=.3,
              borderaxespad=.1, loc='upper left')

    for ax in (a1, a2, a3):
        ax.grid(axis='y', color='#f0efec', lw=.5, zorder=0)
        ax.tick_params(labelsize=6, length=2, width=.6, pad=1.5)

    fig.subplots_adjust(left=.058, right=.995, top=.87, bottom=.25, wspace=.29)
    for ext in ('pdf', 'png'):
        fig.savefig(OUT / f'checkpoint_weight.{ext}', dpi=300)

    for unit in ('instances', 'services'):
        print(f'\nper {unit[:-1].upper()}, >= {FLOOR[unit]:,} spans '
              f'(n={len(data[(SHOW,"stop",unit)]):,})')
        print(f'{"policy":12}{"arm":6}{"p10":>8}{"p50":>8}{"p90":>8}{"p99":>8}{"max":>9}')
        for pol, lab in POLICIES:
            for arm, _, _ in ARMS:
                r = data[(pol, arm, unit)]
                print(f'{lab:12}{arm:6}{np.quantile(r,.1):8.1f}{np.quantile(r,.5):8.1f}'
                      f'{np.quantile(r,.9):8.1f}{np.quantile(r,.99):8.1f}{r.max():9.1f}')
        print(f'{"":18}' + '-' * 49)
        for unit2 in (unit,):
            hi = {lab: (np.quantile(data[(p,'stop',unit2)], .99), np.quantile(data[(p,'pass',unit2)], .99))
                  for p, lab in POLICIES}
            print(f'{"p99 range":18}stop {min(v[0] for v in hi.values()):.0f}-{max(v[0] for v in hi.values()):.0f}'
                  f'   pass {min(v[1] for v in hi.values()):.0f}-{max(v[1] for v in hi.values()):.0f}')


if __name__ == '__main__':
    main()
