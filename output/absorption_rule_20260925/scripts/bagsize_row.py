"""Per-trace bagsize distributions as one full-page-width row, per policy.

  random CPD 1:9  /mydata/uber/sim_day2_cpd19_depth_cubic/sim/*_reverse.csv
  fixed  CPD 5    /mydata/uber/recon_sim_cpd5_fixed/day2_sim/sim/*_reverse.csv

Day 2 only (843,274 traces): fixed CPD 5 was never simulated on day 1.

All three panels come from the SAME run -- the reverse arm -- so the checkpoint
payload and forward baggage shown are the ones that coexist with the reverse
return edges, not values borrowed from the forward-only arm.

Colour encodes the bridge; line style encodes trace mean versus trace worst.
Means use the overall denominator, every unit including empty return edges.
"""
import csv
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.lines import Line2D

OUT = Path(__file__).resolve().parent
BR = [('PB', 'pcrb', '#0072b2'), ('CGPB', 'cgprb', '#e69f00'), ('SB', 'sb3', '#009e73')]
POLICIES = {
    'cpd19': ('random CPD 1:9', Path('/mydata/uber/sim_day2_cpd19_depth_cubic/sim')),
    'cpd5':  ('fixed CPD 5',    Path('/mydata/uber/recon_sim_cpd5_fixed/day2_sim/sim')),
}
# Per-panel x limits chosen from the measured support, not a shared guess:
# checkpoint payload reaches 1,604,740 B and reverse 1,039,800 B (both were
# CLIPPED by the old shared 6e5 limit), while forward baggage stops at 15,212 B
# and was wasting 1.5 decades. Ticks label every other decade; at ~1.1 in per
# panel every decade would collide whatever the font size.
PAN = [
    ('Checkpoint', 'B / ckpt', 'combined_checkpoint_payload_sum',
     'num_ckpt_spans', 'max_combined_checkpoint_payload', (10, 3e6), (1, 3, 5)),
    ('Fwd baggage', 'B / call', 'bag_sum', 'n_bag', 'bag_max', (10, 3e4), (1, 2, 3, 4)),
    ('Rev baggage', 'B / return', 'reverse_encoded_baggage_sum',
     'num_reverse_return_edges', 'max_reverse_encoded_baggage', (0.5, 3e6), (0, 2, 4, 6)),
]

plt.rcParams.update({
    'font.size': 11, 'font.family': 'sans-serif',
    'axes.spines.top': False, 'axes.spines.right': False,
    'axes.edgecolor': '#c8c7c2', 'axes.labelcolor': '#0b0b0b',
    'xtick.color': '#0b0b0b', 'ytick.color': '#0b0b0b', 'axes.linewidth': .7,
})


def load(path):
    with path.open() as fh:
        return list(csv.DictReader(l for l in fh if not l.startswith('#')))


def render(policy):
    label, root = POLICIES[policy]
    data = {sim: load(root / f'{sim}_reverse.csv') for _, sim, _ in BR}

    # Authored at the final printed size: the acmart sigconf column is ~3.3 in,
    # so include at width=\\columnwidth with NO scaling. Authoring at full-page
    # width and letting LaTeX shrink it 2x is what halved every font and rule.
    # Height set so each panel keeps roughly the 2.5:1 aspect of the original
    # full-width row; only the physical scale changed, not the proportions.
    fig, axes = plt.subplots(1, 3, figsize=(3.3, 1.05), sharey=True)
    totals = {}
    for ax, (title, xlab, sumk, cntk, maxk, xlim, decades) in zip(axes, PAN):
        for lbl, sim, col in BR:
            rows = [r for r in data[sim] if int(r[cntk]) > 0]
            totals[(lbl, title)] = sum(int(r[sumk]) for r in data[sim])
            mean = np.array([int(r[sumk]) / int(r[cntk]) for r in rows])
            worst = np.array([int(r[maxk]) for r in rows])
            for arr, ls in ((mean, '-'), (worst, '--')):
                srt = np.sort(arr)
                y = np.arange(1, len(srt) + 1) / len(srt)
                ax.step(np.where(srt <= 0, 0.6, srt), y, where='post',
                        color=col, ls=ls, lw=1.0)
        ax.set_xscale('log'); ax.set_ylim(0, 1.03); ax.set_xlim(*xlim)
        ax.set_xticks([10.0 ** d for d in decades])
        ax.set_xticklabels([f'$10^{{{d}}}$' for d in decades])
        ax.set_title(title, fontsize=7.5, pad=2)
        ax.set_xlabel(xlab, fontsize=7, labelpad=1)
        ax.grid(color='#f0efec', lw=.6)
        ax.tick_params(labelsize=6.5, length=2, width=.6, pad=1.5)
    axes[0].set_ylabel('frac. traces', fontsize=7, labelpad=1)
    axes[0].set_yticks([0, .5, 1])
    # One shared key above the row: two in-panel legends cost more area than
    # 1.1 in panels can spare.
    keys = [Line2D([], [], color=c, lw=1.2, label=l) for l, _, c in BR] + \
           [Line2D([], [], color='#52514e', lw=1.2, ls=st, label=l)
            for st, l in (('-', 'mean'), ('--', 'worst'))]
    fig.legend(handles=keys, loc='upper center', bbox_to_anchor=(.55, 1.005), ncol=5,
               frameon=False, fontsize=6.5, handlelength=1.3, columnspacing=.8,
               handletextpad=.35, borderaxespad=0)
    fig.subplots_adjust(left=.125, right=.985, top=.73, bottom=.30, wspace=.16)
    for ext in ('pdf', 'png'):
        fig.savefig(OUT / f'bagsize_row_{policy}.{ext}', dpi=300)
    plt.close(fig)
    return label, totals


def emission_totals():
    """Absolute byte totals, with export and in-band scopes kept SEPARATE.

    The run README quotes a "forward total" of ckpt_sum on the forward arm
    against a "reverse total" of combined + reverse_encoded on the reverse arm.
    Those are different scopes: the forward column is export-only while the
    reverse column adds return-path wire traffic, and the forward arm's own
    in-band cost (bag_sum) is left out of its side. Pairing them yields a ~3.3x
    figure that is not like-for-like. Reported here as four explicit columns so
    the comparison a reader makes is the one they intend.
    """
    print('\n=== absolute totals by SCOPE (day 2), GB ===')
    print(f'{"bridge":6}{"arm":9}{"export":>9}{"fwd baggage":>13}{"return wire":>13}{"all in-band":>13}')
    agg = {}
    for lbl, sim, _ in BR:
        for policy in ('cpd19', 'cpd5'):
            for arm in ('forward', 'reverse'):
                t = {'combined_checkpoint_payload_sum': 0, 'bag_sum': 0, 'reverse_encoded_baggage_sum': 0}
                with (POLICIES[policy][1] / f'{sim}_{arm}.csv').open() as fh:
                    for r in csv.DictReader(l for l in fh if not l.startswith('#')):
                        for k in t:
                            t[k] += int(r[k])
                agg[(lbl, policy, arm)] = t
        for policy in ('cpd19', 'cpd5'):
            for arm in ('forward', 'reverse'):
                t = agg[(lbl, policy, arm)]
                ex, fb, rw = (t['combined_checkpoint_payload_sum'] / 1e9,
                              t['bag_sum'] / 1e9, t['reverse_encoded_baggage_sum'] / 1e9)
                tag = f'{"1:9" if policy == "cpd19" else "f5"}/{arm[:3]}'
                print(f'{lbl:6}{tag:9}{ex:9.2f}{fb:13.2f}{rw:13.2f}{ex + fb + rw:13.2f}')

    print('\n=== the reverse trade, stated three ways (day 2, random 1:9) ===')
    for lbl, _, _ in BR:
        f, r = agg[(lbl, 'cpd19', 'forward')], agg[(lbl, 'cpd19', 'reverse')]
        ex_f, ex_r = f['combined_checkpoint_payload_sum'], r['combined_checkpoint_payload_sum']
        all_f = ex_f + f['bag_sum'] + f['reverse_encoded_baggage_sum']
        all_r = ex_r + r['bag_sum'] + r['reverse_encoded_baggage_sum']
        bad = (ex_r + r['reverse_encoded_baggage_sum']) / ex_f
        print(f'  {lbl:5} export only {ex_r / ex_f:5.2f}x | export+all in-band {all_r / all_f:5.2f}x '
              f'| README pairing {bad:5.2f}x (NOT like-for-like)')


def main():
    got = {}
    for policy in POLICIES:
        label, totals = render(policy)
        got[policy] = totals
        print(f'\n=== {label} (day 2, reverse arm): absolute totals ===')
        print(f'{"bridge":8}' + ''.join(f'{t:>24}' for t, *_ in PAN))
        for lbl, _, _ in BR:
            print(f'{lbl:8}' + ''.join(f'{totals[(lbl, t)] / 1e9:20.2f} GB' for t, *_ in PAN))

    print('\n=== policy effect on absolute bytes (day 2, reverse arm) ===')
    print(f'{"bridge":8}{"quantity":22}{"1:9 GB":>10}{"fixed5 GB":>12}{"change":>10}')
    for lbl, _, _ in BR:
        for t, *_ in PAN:
            a = got['cpd19'][(lbl, t)] / 1e9
            b = got['cpd5'][(lbl, t)] / 1e9
            print(f'{lbl:8}{t:22}{a:10.2f}{b:12.2f}{100 * (b - a) / a:9.1f}%')
    emission_totals()
    print(f'\nfigures -> {OUT}')


if __name__ == '__main__':
    main()
