"""Reconstruction error and time figures, both checkpoint policies, both days.

  random CPD 1:9   /mydata/uber/recon_final_cpd19_depth_cubic/day{1,2}
  fixed  CPD 5     /mydata/uber/recon_sim_cpd5_fixed/day{1,2}_recon

Both were produced by the same engine (b53c0b0) and the same binary sha; the
only intended difference is the checkpoint policy. Days are pooled by SUMMING
counters, never by averaging percentages -- error = 1 - clean/feasible is a
ratio of sums, so the implied weight is `feasible`, not `traces`.

Emits, for each policy and arm:
    recon_error_<policy>_<arm>.pdf   grouped bars, error % by drop rate
    recon_time_<policy>_<arm>.pdf    violins, per-trace time, log y
and one cross-policy figure:
    recon_error_policy_crossover.pdf
"""
import json
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.lines import Line2D

OUT = Path(__file__).resolve().parent
BR = [('PB', 'pb0', '#0072b2', '//'), ('CGPB', 'cgp0', '#e69f00', '\\\\'), ('SB', 'sb3', '#009e73', 'xx')]
CODES = ['d005', 'd025', 'd05', 'd075', 'd095', 'd10']
LBL = ['0.05', '0.25', '0.5', '0.75', '0.95', '1.0']
SUMMABLE = ('traces', 'feasible', 'empty', 'empty_clean', 'clean', 'clean_all',
            'real_nodes', 'edge_exact', 'edge_anonymous_valid', 'edge_wrong', 'constraint_wrong')

RUNS = {
    'cpd19': {'label': 'random CPD 1:9',
              'days': {'day1': (Path('/mydata/uber/recon_final_cpd19_depth_cubic/day1'), 30),
                       'day2': (Path('/mydata/uber/recon_final_cpd19_depth_cubic/day2'), 50)}},
    'cpd5':  {'label': 'fixed CPD 5',
              'days': {'day1': (Path('/mydata/uber/recon_sim_cpd5_fixed/day1_recon'), 30),
                       'day2': (Path('/mydata/uber/recon_sim_cpd5_fixed/day2_recon'), 50)}},
}
CAP = 25000
rng = np.random.default_rng(42)

plt.rcParams.update({
    'font.size': 11, 'font.family': 'sans-serif',
    'axes.edgecolor': '#000000', 'axes.labelcolor': '#000000',
    'xtick.color': '#000000', 'ytick.color': '#000000',
    'axes.linewidth': .8, 'hatch.linewidth': .6,
})


def pooled_cells(policy):
    """Sum the two days' counters per cell."""
    parts = []
    for day, (root, _) in RUNS[policy]['days'].items():
        parts.append(json.loads((root / 'aggregate_cells.json').read_text()))
    if set(parts[0]) != set(parts[1]):
        raise SystemExit(f'{policy}: day1/day2 cell keys differ; refusing to pool')
    return {k: {f: sum(p[k][f] for p in parts) for f in SUMMABLE} for k in parts[0]}


def err(c):
    return 100 - 100 * c['clean'] / c['feasible'] if c['feasible'] else 0.0


def times_ms(policy, mode, arm, code):
    out = []
    for day, (root, nsh) in RUNS[policy]['days'].items():
        for s in range(nsh):
            p = root / 'recon' / f'{mode}_{arm}_shard{s:02d}_{code}.csv'
            with p.open() as fh:
                head = fh.readline().rstrip('\n').split(',')
                fi, ni = head.index('feasible'), head.index('recon_ns')
                for line in fh:
                    f = line.split(',')
                    if f[fi] == '1':
                        out.append(int(f[ni]))
    a = np.array(out, dtype=np.float64) / 1e6
    return a[a > 0]


def plot_error(policy, arm, cells, ymax):
    vals = {m: [err(cells[f'{m}|{arm}|{c}']) for c in CODES] for _, m, _, _ in BR}
    fig, ax = plt.subplots(figsize=(3.33, 2.0))
    w, centers = 0.27, np.arange(len(CODES))
    for i, (lbl, mode, col, hat) in enumerate(BR):
        ax.bar(centers + (i - 1) * w, vals[mode], width=w * .9,
               color=col, hatch=hat, edgecolor='black', lw=.6, label=lbl)
    ax.set_xticks(centers); ax.set_xticklabels(LBL, fontsize=10)
    ax.set_xlabel('drop rate', fontsize=10.5, labelpad=2)
    ax.set_ylabel('error (%)', fontsize=10.5)
    ax.set_ylim(0, ymax); ax.set_yticks(np.arange(0, ymax, 2))
    ax.spines['top'].set_visible(False); ax.spines['right'].set_visible(False)
    ax.grid(axis='y', color='#e8e8e8', lw=.8); ax.set_axisbelow(True)
    ax.tick_params(labelsize=10, length=3, width=.8, pad=2)
    ax.legend(loc='upper left', ncol=3, fontsize=9, handlelength=1.0, handleheight=1.0,
              columnspacing=.7, handletextpad=.35, borderpad=.2, frameon=False)
    fig.subplots_adjust(left=.135, right=.985, top=.975, bottom=.21)
    for ext in ('pdf', 'png'):
        fig.savefig(OUT / f'recon_error_{policy}_{arm}.{ext}', dpi=300)
    plt.close(fig)
    return vals


def plot_time(policy, arm):
    fig, ax = plt.subplots(figsize=(3.33, 2.0))
    w, centers, span = 0.27, np.arange(len(CODES)), []
    stats = {}
    for i, (lbl, mode, col, _) in enumerate(BR):
        samples = []
        for code in CODES:
            a = times_ms(policy, mode, arm, code)
            stats[(mode, code)] = (len(a), float(np.median(a)), float(np.percentile(a, 99)))
            lo, hi = np.percentile(a, [0.1, 99.9])
            a = a[(a >= lo) & (a <= hi)]
            s = a if len(a) <= CAP else rng.choice(a, CAP, replace=False)
            samples.append(np.log10(s)); span.append((np.log10(s).min(), np.log10(s).max()))
        v = ax.violinplot(samples, positions=centers + (i - 1) * w, widths=w * .92,
                          showextrema=False, showmedians=True)
        for b in v['bodies']:
            b.set_facecolor(col); b.set_edgecolor('black'); b.set_linewidth(.5); b.set_alpha(1)
        v['cmedians'].set_color('black'); v['cmedians'].set_linewidth(.8)
        ax.plot([], [], color=col, lw=5, label=lbl)
    ax.set_xticks(centers); ax.set_xticklabels(LBL, fontsize=10)
    ax.set_xlabel('drop rate', fontsize=10.5, labelpad=2)
    ax.set_ylabel('recon. time (ms)', fontsize=10.5)
    lo = min(a for a, _ in span); hi = max(b for _, b in span); pad = .06 * (hi - lo)
    ticks = [t for t in (-2, -1, 0, 1, 2, 3, 4, 5) if lo - pad <= t <= hi + pad + .55]
    ax.set_yticks(ticks); ax.set_yticklabels([f'$10^{{{t}}}$' for t in ticks], fontsize=10)
    ax.set_ylim(lo - pad, hi + pad + .55); ax.set_xlim(-.55, len(CODES) - .45)
    ax.grid(axis='y', color='#e8e8e8', lw=.8); ax.set_axisbelow(True)
    ax.tick_params(length=3, width=.8, pad=2)
    ax.legend(loc='upper left', ncol=3, fontsize=9, handlelength=.9, columnspacing=.7,
              handletextpad=.35, borderpad=.2, frameon=False)
    fig.subplots_adjust(left=.175, right=.985, top=.975, bottom=.21)
    for ext in ('pdf', 'png'):
        fig.savefig(OUT / f'recon_time_{policy}_{arm}.{ext}', dpi=300)
    plt.close(fig)
    return stats


def plot_crossover(vals):
    """The headline of the fixed-5 run: the policies swap rank as loss rises.
    Colour = bridge, line style = policy."""
    fig, ax = plt.subplots(figsize=(3.33, 2.2))
    x = np.arange(len(CODES))
    for lbl, mode, col, _ in BR:
        ax.plot(x, vals['cpd19']['forward'][mode], color=col, lw=1.4, ls='-', marker='o', ms=3)
        ax.plot(x, vals['cpd5']['forward'][mode], color=col, lw=1.4, ls='--', marker='s', ms=3)
    ax.set_xticks(x); ax.set_xticklabels(LBL, fontsize=10)
    ax.set_xlabel('drop rate', fontsize=10.5, labelpad=2)
    ax.set_ylabel('error (%)', fontsize=10.5)
    ax.spines['top'].set_visible(False); ax.spines['right'].set_visible(False)
    ax.grid(axis='y', color='#e8e8e8', lw=.8); ax.set_axisbelow(True)
    ax.tick_params(labelsize=10, length=3, width=.8, pad=2)
    keys = [Line2D([], [], color=c, lw=1.6, label=l) for l, _, c, _ in BR] + \
           [Line2D([], [], color='#52514e', lw=1.4, ls='-', marker='o', ms=3, label='random 1:9'),
            Line2D([], [], color='#52514e', lw=1.4, ls='--', marker='s', ms=3, label='fixed 5')]
    ax.legend(handles=keys, loc='upper left', ncol=2, fontsize=8, frameon=False,
              handlelength=1.6, columnspacing=.8, labelspacing=.25, handletextpad=.4)
    fig.subplots_adjust(left=.135, right=.985, top=.975, bottom=.19)
    for ext in ('pdf', 'png'):
        fig.savefig(OUT / f'recon_error_policy_crossover.{ext}', dpi=300)
    plt.close(fig)


def main():
    cells = {p: pooled_cells(p) for p in RUNS}
    ymax = float(np.ceil(max(err(c) for cc in cells.values() for c in cc.values()))) + 2.0

    vals = {}
    for policy in RUNS:
        vals[policy] = {}
        for arm in ('forward', 'reverse'):
            vals[policy][arm] = plot_error(policy, arm, cells[policy], ymax)
            print(f'\n=== {RUNS[policy]["label"]} / {arm}: error % (pooled days) ===')
            print(f'{"drop":>6} ' + ''.join(f'{l:>9}' for l, _, _, _ in BR))
            for j, lab in enumerate(LBL):
                print(f'{lab:>6} ' + ''.join(f'{vals[policy][arm][m][j]:9.2f}' for _, m, _, _ in BR))

    plot_crossover(vals)
    print('\n=== crossover, forward arm: 1:9 -> fixed 5 (pooled days) ===')
    print(f'{"drop":>6} ' + ''.join(f'{l:>18}' for l, _, _, _ in BR))
    for j, lab in enumerate(LBL):
        cellsr = ''.join(f'{vals["cpd19"]["forward"][m][j]:8.2f} ->{vals["cpd5"]["forward"][m][j]:7.2f}'
                         for _, m, _, _ in BR)
        print(f'{lab:>6} ' + cellsr)

    for policy in RUNS:
        for arm in ('forward', 'reverse'):
            st = plot_time(policy, arm)
            print(f'\n=== {RUNS[policy]["label"]} / {arm}: recon time ===')
            print(f'{"mode":6}{"drop":>6}{"n":>12}{"p50 ms":>10}{"p99 ms":>11}')
            for _, m, _, _ in BR:
                for code, lab in zip(CODES, LBL):
                    n, p50, p99 = st[(m, code)]
                    print(f'{m:6}{lab:>6}{n:12,}{p50:10.3f}{p99:11.2f}')
    print(f'\nfigures -> {OUT}')


if __name__ == '__main__':
    main()
