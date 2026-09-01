# Full Day 1 greedy reconstruction analysis

> **Superseded for accuracy comparisons.** These numbers predate the
> evidence-bounded scorer. See `docs/reconstruction_evaluation.md` and the
> replacement canonical run.

Corpus: `/mydata/uber/bignode_state/day1_unfilt_corpus` with
`/mydata/uber/day1.store` (521,305 traces; no sampling or cleanliness filter).

Common reconstruction settings: checkpoint distance 4, drop rate 0.5,
per-trace drop seed 42, checkpoint prefix 8 bytes, Bloom target FPR 0.0001.
There was no prime rounding in these runs. SB3 used 64-bit delayed-end owner
fingerprints and Lehmer-coded EE/DEE groups.

| Topology mode | Clean traces | Clean, excluding empty | Clean, crediting empty | Compatible | Hard conflicts |
|---|---:|---:|---:|---:|---:|
| CGP0 greedy baseline | 449,871 | 87.8081% | 88.0180% | n/a | n/a |
| SB3 greedy, ordinals excluded from topology decisions | 450,741 | 87.9780% | 88.1848% | 459,910 (88.2228%) | 0 (parent 0, HA 0) |
| SB3 greedy, ordinals used for topology pruning | 461,219 | 90.0231% | 90.1948% | 472,959 (90.7260%) | 0 (parent 0, HA 0) |

All modes saw 512,334 feasible and 8,971 empty traces. Relative to the SB3
topology ablation, ordinal topology evidence recovered 10,478 additional clean
traces (+2.0452 percentage points) and removed 17.01% of its remaining
non-empty trace-level errors. Relative to CGP0, ordinal-guided SB3 recovered
11,348 clean traces (+2.2150 points), an 18.17% error reduction.

The SB3 topology-compatibility result improved by 13,049 traces (+2.5031
points), eliminating 21.25% of the ablation's incompatible traces. Ordinals
changed the ordinary greedy topology choice 118,549 times.

| Mode | Exact reconstructed edges | Wrong edges per trace | Wall time | Peak RSS |
|---|---:|---:|---:|---:|
| CGP0 | 64.605% (241,482,021 / 373,784,250) | 0.78 | 10m36s | 514 MiB |
| SB3, no ordinal topology guidance | 96.202% (437,815,675 / 455,101,671) | 1.76 | 13m37s | 1,048 MiB |
| SB3, ordinal topology guidance | 96.207% (437,838,761 / 455,101,667) | 1.52 | 18m35s | 1,377 MiB |

The edge denominators are not directly comparable between CGP0 and SB3: SB3
materializes exact surviving ParentID relationships and named synthetic nodes,
so its scored topology contains substantially more explicit edges.

The no-ordinal SB3 ablation is not charged for ignored sparse-ordinal
incompatibility. Its 870-trace gain over the historical CGP0 implementation is
therefore a topology-engine difference: exact-parent route units, grouped Bloom
corroboration, global hard HA constraints, and candidate fallback.

## HA invariant discovered by the full corpus

The first full SB3 pass exposed six traces (12 total HA conflicts) absent from
the 50k sample. Two exact HA obligations for different fanout identities at the
same absolute depth could independently remain unresolved after their routes
merged onto one upstream terminal. That pair is jointly impossible even though
each obligation alone is still unresolved.

The HA tracker now rejects such a merge during candidate evaluation and rolls
the trial route back. This is candidate-space pruning, not post-hoc topology
repair. A focused replay of all six offending trace IDs passed in both SB3 modes
with zero parent and HA conflicts; both corrected full-corpus reruns also ended
with zero hard conflicts.

Corrected SB3 binary SHA-256:
`c9f757b2d7eeba97e6a0ca0b6ff149d6df89ce15bc0b9b1aa6a988c7ea0537e2`.
