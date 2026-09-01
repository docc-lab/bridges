# Canonical maximal-evidence reconstruction: full Day 1, prime-up

This is the replacement accuracy comparison for CGP0 and SB3. Both models use
their maximal-evidence defaults and the `evidence-bounded-v1` scorer documented
in `docs/reconstruction_evaluation.md`. No legacy, ordinal-disabled, or
evidence-disabled ablation appears as a competing model in this table.

## Matched configuration

- Corpus: `/mydata/uber/bignode_state/day1_unfilt_corpus`
- Trace store: `/mydata/uber/day1.store`
- Traces: all 521,305 (no sample or cleanliness filter)
- Checkpoint distance: 4
- Drop rate: 0.5; seed 42; per-trace drop seeding
- Bloom target FPR: 0.0001; prime round-up enabled; no byte cap
- Prefix length: 8 bytes
- SB3 lateral encoding: 64-bit DEE-owner fingerprints and Lehmer-coded EE/DEE

Both JSON manifests report `score_policy=evidence-bounded-v1` and
`evidence_profile=maximal`.

## Results

| Model | Clean nontrivial | Nontrivial rate | Clean all traces | All-trace rate | Wrong topology segments | Structural-constraint failures | ParentID/HA conflicts |
|---|---:|---:|---:|---:|---:|---:|---:|
| CGP0 full-evidence greedy | 484,156 / 512,334 | 94.500072% | 493,127 / 521,305 | 94.594719% | 108,881 | 0 | 0 |
| SB3 full-evidence sparse-ordinal greedy | 489,620 / 512,334 | 95.566564% | 498,591 / 521,305 | 95.642858% | 87,616 | 21,439 | 0 |

SB3 recovered 5,464 additional clean nontrivial traces: +1.066492 percentage
points, eliminating 19.391014% of CGP0's remaining trace failures. The
all-trace improvement is +1.048139 points.

Every empty result was actually scored. All 8,971 empty cases were score-clean
and also had no dropped spans, so `clean_all` credits them based on evidence;
it does not assume every empty reconstruction is correct.

## Auditable segment accounting

Both models were judged over the same 455,101,746 nameable-source parent
segments.

| Model | Exact identity | Valid anonymous identity | Wrong | Segment-correct |
|---|---:|---:|---:|---:|
| CGP0 full-evidence greedy | 437,931,823 | 17,061,042 | 108,881 | 99.976075% |
| SB3 full-evidence sparse-ordinal greedy | 437,938,201 | 17,075,929 | 87,616 | 99.980748% |

For each row, `exact + valid anonymous + wrong = 455,101,746`. A valid
anonymous node is accepted only where no surviving SpanID, ParentID, or HA
record names the corresponding truth identity, and its chain must still end at
the correct next nameable ancestor. A wrong survivor or named synthetic in that
slot is an error.

All 368,249,023 surviving-span parent segments were exact for both models. The
SB3 structural-constraint count is kept outside the edge partition: 21,439
traces could not satisfy their emitted sparse ordinals. A trace must have zero
wrong segments and zero structural-constraint failures to be clean.

## Reproducibility and runtime

Before the full run, two independent 10,000-trace runs of each model produced
byte-identical JSON results, including topology and evidence telemetry. Equal-
depth carrier selection, checkpoint-window processing, and conflicted ordinal
alignment are explicitly ordered rather than depending on Go map iteration.

The two full jobs ran concurrently for the first 8.5 minutes, so these wall
times are operational records rather than isolated benchmarks:

| Model | Wall time | Peak RSS |
|---|---:|---:|
| CGP0 full-evidence greedy | 8m29s | 671 MiB |
| SB3 full-evidence sparse-ordinal greedy | 16m48s | 1,171 MiB |

