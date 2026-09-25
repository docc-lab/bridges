"""Checkpoint rate per instance and per service: does passing scheduled
checkpoints spread the load, or concentrate it?

  /mydata/uber/policy_probe_20260924/sim/<policy>_{stop,pass}.pressure.json

50,000 day-1 traces, PB0, random CPD 1:9 seed 42, leaf rejection q=1. Each
policy run twice, identical but for --reverse-pass-checkpoints.

The quantity is the CHECKPOINT RATE: of the spans a unit handled, what fraction
did it have to emit a checkpoint for.

    rate(u) = num_checkpoint_spans(u) / num_spans(u)

A rate is the right unit for a hotspot claim and a share of the global total is
not. It is bounded in [0,1], normalized by the unit's own traffic so the load
balancer's skew cancels, and needs no ranking step -- so it cannot be inflated by
selecting the top units on the same noisy quantity being measured. A unit at 0.95
is emitting for nearly every span it serves, regardless of how small its slice of
the global total is.

Panels (a) and (b) share an x axis deliberately: that is the result. Passing
widens both distributions -- the direction is the same at both granularities, so
it is not an artifact of the modeled instance assignment -- but services never
approach saturation (worst 0.665) while individual instances reach 1.000.
Averaging a service over its instances dilutes the hotspot about twofold, which
is what a per-service measurement would have reported and missed.

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


def rates(key, unit):
    d = json.loads((SIM / f'{key}.pressure.json').read_text())
    sp = np.array([i['num_spans'] for i in d[unit]], float)
    ck = np.array([i['num_checkpoint_spans'] for i in d[unit]], float)
    m = sp >= FLOOR[unit]
    return ck[m] / sp[m]


def main():
    plt.rcParams.update({
        'font.size': 7, 'font.family': 'sans-serif',
        'axes.spines.top': False, 'axes.spines.right': False,
        'axes.edgecolor': '#c8c7c2', 'axes.labelcolor': '#0b0b0b',
        'xtick.color': '#0b0b0b', 'ytick.color': '#0b0b0b', 'axes.linewidth': .6,
    })
    fig, (a1, a2, a3) = plt.subplots(1, 3, figsize=(6.9, 1.85))

    data = {(p, a, u): rates(f'{p}_{a}', u)
            for p, _ in POLICIES for a, _, _ in ARMS for u in FLOOR}

    bins = np.linspace(0, 1, 46)
    for ax, unit, tag in ((a1, 'instances', 'instance'), (a2, 'services', 'service')):
        for arm, lab, col in ARMS:
            v = data[(SHOW, arm, unit)]
            ax.hist(v, bins=bins, histtype='stepfilled', color=col, alpha=.45, lw=0, zorder=2)
            ax.hist(v, bins=bins, histtype='step', color=col, lw=1.0, zorder=3)
        n = len(data[(SHOW, 'stop', unit)])
        ax.set_yscale('log')
        ax.set_xlim(0, 1)
        ax.set_xlabel(f'checkpoint rate of {"an" if tag == "instance" else "a"} {tag}',
                      fontsize=6.5, labelpad=1)
        ax.set_ylabel(f'{tag}s', fontsize=6.5, labelpad=1)
        ax.set_title(f'({"ab"[unit == "services"]}) per {tag}, cubic  n={n:,}',
                     fontsize=6.5, pad=2)
    # the saturated corner only instances reach
    a1.axvspan(.8, 1.0, color='#d55e00', alpha=.07, zorder=1)
    a1.annotate('saturating', (.79, 22), fontsize=5.5, color='#a33', ha='right')
    a2.axvspan(.8, 1.0, color='#d55e00', alpha=.07, zorder=1)
    a2.annotate('empty', (.79, 22), fontsize=5.5, color='#a33', ha='right')
    a1.legend(handles=[Line2D([], [], color=c, lw=1.2, label=l) for _, l, c in ARMS],
              frameon=False, fontsize=5.5, handlelength=1.1, handletextpad=.4,
              borderaxespad=.1, loc='upper right')

    x = np.arange(len(POLICIES))
    w = .36
    for j, (arm, lab, col) in enumerate(ARMS):
        a3.bar(x + (j - .5) * w, [np.quantile(data[(p, arm, 'instances')], .99) for p, _ in POLICIES],
               w, color=col, zorder=3, edgecolor='white', linewidth=.4)
        a3.scatter(x + (j - .5) * w,
                   [np.quantile(data[(p, arm, 'services')], .99) for p, _ in POLICIES],
                   marker='_', s=34, color='#0b0b0b', linewidths=1.0, zorder=5)
    a3.set_ylim(0, 1)
    a3.set_xticks(x)
    a3.set_xticklabels([l for _, l in POLICIES], fontsize=6, rotation=35, ha='right')
    a3.set_ylabel('99th pct checkpoint rate', fontsize=6.5, labelpad=1)
    a3.set_title('(c) the worst unit, every policy', fontsize=6.5, pad=2)
    a3.legend(handles=[Line2D([], [], color='#0b0b0b', ls='', marker='_', ms=5,
                              markeredgewidth=1.0, label='per service')],
              frameon=False, fontsize=5.5, handlelength=.9, handletextpad=.3,
              borderaxespad=.1, loc='upper left')

    for ax in (a1, a2, a3):
        ax.grid(axis='y', color='#f0efec', lw=.5, zorder=0)
        ax.tick_params(labelsize=6, length=2, width=.6, pad=1.5)

    fig.subplots_adjust(left=.058, right=.995, top=.87, bottom=.25, wspace=.29)
    for ext in ('pdf', 'png'):
        fig.savefig(OUT / f'checkpoint_rate.{ext}', dpi=300)

    for unit in ('instances', 'services'):
        print(f'\nper {unit[:-1].upper()}, >= {FLOOR[unit]:,} spans '
              f'(n={len(data[(SHOW,"stop",unit)]):,})')
        print(f'{"policy":12}{"arm":6}{"p10":>7}{"p50":>7}{"p90":>7}{"p99":>7}{"max":>7}'
              f'{"sd":>7}{"IQR":>7}')
        for pol, lab in POLICIES:
            for arm, _, _ in ARMS:
                r = data[(pol, arm, unit)]
                print(f'{lab:12}{arm:6}{np.quantile(r,.1):7.3f}{np.quantile(r,.5):7.3f}'
                      f'{np.quantile(r,.9):7.3f}{np.quantile(r,.99):7.3f}{r.max():7.3f}'
                      f'{r.std():7.3f}{np.quantile(r,.75)-np.quantile(r,.25):7.3f}')
        print(f'{"":18}' + '-' * 49)
        for unit2 in (unit,):
            hi = {lab: (np.quantile(data[(p,'stop',unit2)], .99), np.quantile(data[(p,'pass',unit2)], .99))
                  for p, lab in POLICIES}
            print(f'{"p99 range":18}stop {min(v[0] for v in hi.values()):.3f}-{max(v[0] for v in hi.values()):.3f}'
                  f'   pass {min(v[1] for v in hi.values()):.3f}-{max(v[1] for v in hi.values()):.3f}')


if __name__ == '__main__':
    main()
