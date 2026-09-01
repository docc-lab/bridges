# Full Day 1 prime-up reconstruction analysis

> **Superseded for accuracy comparisons.** These numbers predate the
> evidence-bounded scorer and include an ordinal-disabled ablation beside the
> headline models. See `docs/reconstruction_evaluation.md` and the replacement
> canonical run.

Corpus: `/mydata/uber/bignode_state/day1_unfilt_corpus` with
`/mydata/uber/day1.store` (521,305 traces; no sampling or cleanliness filter).

Common settings: checkpoint distance 4, drop rate 0.5, per-trace drop seed 42,
checkpoint prefix 8 bytes, Bloom target FPR 0.0001, and prime round-up
(`--prime-m`, without byte capping). SB3 used 64-bit delayed-end owner
fingerprints and Lehmer-coded EE/DEE groups.

| Reconstruction model | Clean traces | Clean, excluding empty | Clean, crediting empty | Compatible | Hard conflicts |
|---|---:|---:|---:|---:|---:|
| CGP0 greedy baseline | 484,433 | 94.5541% | 94.6479% | n/a | n/a |
| SB3 greedy, ordinals excluded from topology decisions | 484,217 | 94.5120% | 94.6064% | 493,273 (94.6227%) | 0 (parent 0, HA 0) |
| SB3 greedy, ordinals used for topology pruning | 489,623 | 95.5671% | 95.6434% | 499,868 (95.8878%) | 0 (parent 0, HA 0) |

All modes saw 512,334 feasible and 8,971 empty traces. Ordinal topology
guidance recovered 5,406 clean traces over the SB3 ablation (+1.0552 percentage
points), eliminating 19.23% of the ablation's remaining errors. It recovered
5,190 clean traces over CGP0 (+1.0130 points), eliminating 18.60% of CGP0's
remaining errors.

SB3 topology compatibility improved by 6,595 traces (+1.2651 points), removing
23.53% of the ablation's incompatible traces. Ordinals changed the ordinary
greedy topology choice 33,595 times.

| Mode | Exact reconstructed edges | Wrong edges per trace | Wall time | Peak RSS |
|---|---:|---:|---:|---:|
| CGP0 | 64.618% (241,530,499 / 373,779,232) | 0.20 | 11m01s | 538 MiB |
| SB3, no ordinal topology guidance | 96.227% (437,931,824 / 455,101,742) | 0.47 | 16m51s | 768 MiB |
| SB3, ordinal topology guidance | 96.229% (437,938,210 / 455,101,744) | 0.42 | 24m02s | 936 MiB |

The edge denominators are not directly comparable between CGP0 and SB3: SB3
materializes exact surviving ParentID relationships and named synthetic nodes,
so its scored topology contains substantially more explicit edges.

## Why the SB3 ablation is not identical to CGP0

`--sb3-ignore-ordinals` disables sparse ordinals only as topology-selection
evidence. It retains SB3's exact dropped-ParentID materialization, same-parent
route units, fanout-group Bloom intersections, global transactional HA
constraints, and fallback to another admissible route when the ordinary greedy
choice violates hard evidence. CGP0 stops at the first deepest Bloom-admissible
anchor and leaves ordinary dropped gap nodes anonymous even when surviving
records expose their exact ParentID.

The no-ordinal SB3 ablation is not charged for ignored sparse-ordinal
incompatibility. With no prime rounding, its extra hard/grouped evidence
produced a net gain of 870 clean traces over historical CGP0. Under prime-up it
finished 216 clean traces (0.0422 points) behind. This small reversal reflects
the different reconstruction granularity and greedy route strategy: legacy
CGP0 leaves ordinary gaps anonymous, while the full engine materializes
available exact identities and is therefore accountable for more explicit
topology. It is not an ordinal-scoring penalty.
