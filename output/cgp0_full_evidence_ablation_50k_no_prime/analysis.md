# CGP0 full-evidence check: 50k Day 1, no prime rounding

> **Superseded for accuracy comparisons.** These numbers used the historical
> permissive scorer. Canonical comparisons use the maximal-evidence defaults
> and the `evidence-bounded-v1` contract in
> `docs/reconstruction_evaluation.md`.

Uniform sample of 50,000 traces from the full unfiltered Day 1 corpus
(`sample-seed=29`), CPD 4, drop rate 0.5, per-trace seed 42, prefix length 8,
and Bloom FPR target 0.0001 without prime rounding.

| Variant | Clean | Clean excl. empty | Exact edges | Hard conflicts | Wall time |
|---|---:|---:|---:|---:|---:|
| Full evidence (new default) | 43,132 | 87.85% | 96.190% | 0 | 1m08s |
| Historical legacy CGP0 | 43,076 | 87.74% | 64.583% | not audited in the historical binary | 1m09s |

The new default recovered 56 additional clean traces (+0.1141 percentage
points), explicitly reconstructed the surviving/exact-parent topology, and
ended with zero ParentID or HA conflicts. It performed 8,250,919 candidate
evaluations and made 500 hard-evidence overrides of the ordinary first greedy
choice.
