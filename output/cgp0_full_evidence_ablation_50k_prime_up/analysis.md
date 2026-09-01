# CGP0 full-evidence greedy ablation: 50k Day 1, prime-up

> **Superseded for accuracy comparisons.** These numbers used the historical
> permissive scorer. They remain useful only as reconstruction-mechanism
> diagnostics. Canonical comparisons use the maximal-evidence defaults and the
> `evidence-bounded-v1` contract in `docs/reconstruction_evaluation.md`.

Uniform sample of 50,000 traces from the full unfiltered Day 1 corpus
(`sample-seed=29`). Common settings: CPD 4, drop rate 0.5, per-trace drop seed
42, prefix length 8, Bloom FPR target 0.0001, and prime round-up.

| Variant | Clean | Clean excl. empty | Exact edges | Hard conflicts | Candidate evaluations | Wall time |
|---|---:|---:|---:|---:|---:|---:|
| Full evidence (new default) | 46,408 | 94.5250% | 96.216% | 0 | 8,250,538 | 2m15s |
| Legacy lean | 46,453 | 94.6167% | 64.597% | 13,331,678 (12,564,923 parent; 766,755 HA) | 0 | 2m07s* |
| No grouped evidence | 46,335 | 94.3763% | 96.216% | 0 | 8,250,560 | 2m09s |
| No hard HA | 46,293 | 94.2908% | 96.215% | 1,110 HA | 8,250,426 | 2m11s |
| No route fallback | 46,409 | 94.5270% | 96.216% | 110 HA | 8,250,534 | 2m11s |

There were 49,096 feasible and 904 empty traces. The five main timing cells ran
concurrently with six workers each, so the wall times are comparative rather
than isolated microbenchmarks. The legacy hard-evidence audit was rerun after
the other cells with 12 workers; its audited topology/accuracy counts were
identical. `*` is the original equal-contention six-worker timing.

Grouped evidence recovered 73 clean traces (+0.1487 points) over its ablation.
Hard HA recovered 115 (+0.2342 points) and prevented 1,110 exact ancestry
violations. Route fallback changed the coarse clean count by only one trace,
but prevented 110 exact HA violations; this demonstrates why trace-clean alone
cannot decide whether a mechanism is safe to remove.

Legacy lean appears 45 traces better under the coarse trace-isomorphism score,
but it omits most explicit topology: it materializes only 64.6% of the scored
edge space and violates over 13.3 million facts directly present in surviving
records. Its anonymous gaps are less falsifiable, not more faithful. It remains
an ablation and cannot be the correctness default.

The full default ended with zero ParentID and HA conflicts.
