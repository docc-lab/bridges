# Canonical maximal-evidence PB0: full Day 1, prime-up

PB0 now uses the shared full-evidence non-SAT greedy substrate in path-only
mode. This run uses the actual P-Bridge payload (checkpoint prefix plus window
Bloom), not the CGP hash-array extension. HA and sparse-ordinal evidence are
therefore unavailable and are explicitly ignored by the reconstructor.

## Configuration

- Corpus: `/mydata/uber/bignode_state/day1_unfilt_corpus`
- Trace store: `/mydata/uber/day1.store`
- Traces: all 521,305 (no sample or cleanliness filter)
- Checkpoint distance: 4
- Drop rate: 0.5; seed 42; per-trace drop seeding
- Bloom target FPR: 0.0001; prime round-up enabled; no byte cap
- Prefix length: 8 bytes
- Score: `path-evidence-v1`
- Evidence profile: `maximal`

## Result

| Metric | Result |
|---|---:|
| Clean nontrivial traces | 492,915 / 512,334 (96.209699%) |
| Clean all traces | 501,886 / 521,305 (96.274925%) |
| Exact observable fragment paths | 130,891,901 / 130,960,521 (99.947603%) |
| Wrong observable fragment paths | 68,620 |
| Missing/extra bridge constraints | 0 |
| ParentID conflicts | 0 |
| HA conflicts/usage | 0 |

All 8,971 traces with no reconstruction obligation were independently
score-clean and had no dropped spans. They are credited in `clean_all`; an
algorithm that emits no bridge for an observable fragment remains nontrivial
and incorrect.

Path accuracy is not directly comparable to CGP0/SB3 topology accuracy: PB0 is
graded on reconnection and anonymous gap length, while the topology models must
also recover nameable synthetic identities, fanout topology, and (for SB3)
sparse structure.

## Evidence used

The default PB0 engine uses every fact in the P-Bridge model:

- literal surviving `ParentID` values to form exact-parent route units;
- all reachable in-window carrier Blooms;
- Bloom intersection across fragments naming the same dropped parent;
- checkpoint-prefix bounds and absolute depths; and
- deterministic deepest admissible-anchor selection with exact gap length.

The former first-deepest-match implementation is retained only as the
`--pb0-legacy` ablation and was not used as a competing headline result.

## Scoring and reproducibility

`path-evidence-v1` derives obligations from the surviving input rather than
from emitted bridges. Every surviving fragment root must have exactly one
bridge to its true nearest surviving ancestor with the depth-implied number of
anonymous synthetic levels. Missing/duplicate bridges, wrong anchors, wrong
gap lengths, and unexpected bridges are errors. Dropped subtrees with no
surviving descendant remain unobservable and are forgiven.

Two independent 10,000-trace validation runs produced byte-identical JSON
outputs before the full run. The full run completed in 4m22s with 826 MiB peak
RSS using 20 workers.

