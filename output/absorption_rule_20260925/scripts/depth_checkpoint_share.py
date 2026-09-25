"""Share of spans that checkpoint, by span depth — both checkpoint policies.

  random CPD 1:9  /mydata/uber/sim_day2_cpd19_depth_cubic/sim/pcrb_{forward,reverse}
  fixed  CPD 5    /mydata/uber/recon_sim_cpd5_fixed/day2_sim/sim/pcrb_{forward,reverse}

DAY 2 ONLY, deliberately: fixed CPD 5 was never simulated on day 1, so a
pooled-day version would silently compare a two-day average against a one-day
one. Day 2 is 843,274 traces / 800,890,614 spans.

Colour = checkpoint policy, line style = arm (forward solid, reverse dashed).

Bridge type is not a variable here: the checkpoint schedule is
bridge-independent -- PB0, CGP0 and SB3 emit byte-identical counts at every
depth, and only the BYTES differ, which is what the bagsize figures show.

The 1/E[D] reference sits on the y axis as a tick label rather than a line
crossing the curves.

Caveat worth carrying into the caption: fixed 5 vs random 1:9 moves three
things at once -- mean spacing vs E[D]=5, uniform capacity-4 Blooms vs
per-window capacity 0-8, and which pruning mechanisms are active. This figure
shows the checkpoint-placement consequence, not "the effect of randomization".
"""
import json
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

OUT = Path(__file__).resolve().parent
MAXD = 20
R19 = Path('/mydata/uber/sim_day2_cpd19_depth_cubic/sim')
R5 = Path('/mydata/uber/recon_sim_cpd5_fixed/day2_sim/sim')

# Standard Okabe-Ito, matching the rest of the figure set.
C19, C5 = '#0072b2', '#d55e00'
# Order is draw order: fixed 5 goes down first so random 1:9 -- the operating
# point -- sits on top of it rather than being buried under the sawtooth.
ARMS = [
    ('fixed 5, forward',      R5 / 'pcrb_forward.pressure.json',  C5,  '-'),
    ('fixed 5, + reverse',    R5 / 'pcrb_reverse.pressure.json',  C5,  '--'),
    ('random 1:9, forward',   R19 / 'pcrb_forward.pressure.json', C19, '-'),
    ('random 1:9, + reverse', R19 / 'pcrb_reverse.pressure.json', C19, '--'),
]


def depths(path):
    d = json.loads(Path(path).read_text())
    return {r['depth']: r for r in d['depths']}, d['num_traces']


def main():
    ds = list(range(0, MAXD + 1))
    series = []
    traffic = None
    for lab, path, col, ls in ARMS:
        dep, ntr = depths(path)
        y = [100 * dep[d]['num_checkpoint_spans'] / dep[d]['num_spans']
             if d in dep and dep[d]['num_spans'] else 0 for d in ds]
        series.append((lab, y, col, ls))
        if traffic is None:  # span counts are identical across policies and arms
            tot = sum(r['num_spans'] for r in dep.values())
            traffic = ([100 * dep[d]['num_spans'] / tot if d in dep else 0 for d in ds], ntr)

    plt.rcParams.update({
        'font.size': 10, 'font.family': 'sans-serif',
        'axes.spines.top': False, 'axes.spines.right': False,
        'axes.edgecolor': '#c8c7c2', 'axes.labelcolor': '#0b0b0b',
        'xtick.color': '#0b0b0b', 'ytick.color': '#0b0b0b', 'axes.linewidth': .6,
    })
    fig, (a1, a2) = plt.subplots(2, 1, figsize=(3.33, 2.15), sharex=True,
                                 gridspec_kw={'height_ratios': [3.0, 1], 'hspace': .10})
    for i, (lab, y, col, ls) in enumerate(series):
        a1.plot(ds, y, color=col, lw=1.0, ls=ls, label=lab, zorder=2 + i)
    # Fixed 5 spikes to 100% at every multiple of 5, so there is no empty
    # corner: give the legend its own band above the data instead.
    a1.set_ylim(0, 126)
    a1.set_yticks([0, 20, 50, 75, 100])
    a1.set_yticklabels(['0', '1/E[D]', '50', '75', '100'], fontsize=7)
    a1.set_ylabel('spans checkpointing (%)', fontsize=7.5, labelpad=2)
    a1.grid(axis='y', color='#f0efec', lw=.6)
    a1.tick_params(labelsize=7, length=2, width=.6)
    keys = [Line2D([], [], color=C19, lw=1.4, label='random 1:9'),
            Line2D([], [], color=C5, lw=1.4, label='fixed 5'),
            Line2D([], [], color='#52514e', lw=1.4, ls='-', label='forward'),
            Line2D([], [], color='#52514e', lw=1.4, ls='--', label='+ reverse cubic')]
    a1.legend(handles=keys, loc='upper center', ncol=2, frameon=False, fontsize=6.5,
              handlelength=1.4, labelspacing=.15, columnspacing=.8,
              handletextpad=.4, borderaxespad=.08)

    y, ntr = traffic
    a2.plot(ds, y, color='#9a9994', lw=1.0)
    a2.fill_between(ds, y, color='#9a9994', alpha=.25)
    a2.set_ylabel('traffic (%)', fontsize=7.5, labelpad=2)
    a2.set_xlabel('span depth in trace', fontsize=8, labelpad=1)
    a2.set_xticks(range(0, MAXD + 1, 5))
    a2.grid(axis='y', color='#f0efec', lw=.6)
    a2.tick_params(labelsize=7, length=2, width=.6)

    fig.subplots_adjust(left=.165, right=.985, top=.99, bottom=.175)
    for ext in ('pdf', 'png'):
        fig.savefig(OUT / f'depth_checkpoint_share.{ext}', dpi=300)

    print(f'{"depth":>6}' + ''.join(f'{lab:>22}' for lab, _, _, _ in series) + f'{"traffic %":>11}')
    for i, d in enumerate(ds):
        print(f'{d:>6}' + ''.join(f'{s[1][i]:22.1f}' for s in series) + f'{y[i]:11.2f}')
    print(f'\nday 2: {ntr:,} traces, {sum(y):.1f}% of traffic within depth 0-{MAXD}')


if __name__ == '__main__':
    main()
