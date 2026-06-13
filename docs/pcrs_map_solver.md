# PCRS: solver-based reconstruction as MAP inference (`recon/pcrs.go`)

PCRS (`--mode pcrs`) reconstructs trace topology from PCRB wire payloads
(truncated checkpoint-root prefix + inherited window bloom; see
`bridge/pcrb.go`) by treating reconstruction as **maximum a posteriori
(MAP) estimation** under an explicit generative model. Everything the
engine does is a *count*, a *theorem*, or an *order*: there are no tuned
thresholds and no heuristic scores. This document states the model,
derives the objective, describes the engine as it ships, and discloses
its bounded approximations and their measured behavior.

## The generative model

- Spans drop independently with rate `r`; checkpoint spans (depth ≡ 0
  mod cpd) always survive as carriers of `_br` payloads.
- A carrier's payload contains (a) an **exact** truncated span-ID prefix
  of its nearest checkpoint ancestor and (b) a **window bloom** over the
  spans strictly between that checkpoint and itself, geometry derived
  from a target false-positive rate ε.
- Blooms have **no false negatives**; non-members test positive
  independently with probability ≈ ε.

Reconstruction = maximize `P(configuration | observations)` =
`P(observations | configuration) · P(configuration)`.

## Terminology

- **Fragment**: a connected component of survivors (root's parent
  dropped). **Orphan**: a fragment with no carrier — placed by
  inference. **Open end**: a surviving non-carrier with zero surviving
  children (provably lost all children). **Window / chain**: each
  carrier's payload defines a window between its anchor checkpoint and
  itself, modeled as a chain of one slot per depth.

## Invariants are prior structure

`P(configuration) = 0` for: non-tree output; two spans on one chain
level; writes onto walls (the slot above a fragment root on its own
chain holds that root's dropped parent); spacing violations (the slot
above every fragment/orphan root stays synthetic); an uncovered open
end; an unplaced orphan. The last two are enforceable because the truth
itself satisfies them with certainty (no-false-negative recoverability:
every orphan's true seat is in its candidate domain; every open end's
true window gates it), so restricting search to invariant-satisfying
configurations never excludes the truth.

## The likelihood: explained bloom positives

A placement that says "span s is in window W" explains s's positive
bits with probability 1; every positive left unexplained costs a factor
ε. Hence `log P(obs | config) = const − (#unexplained) · log(1/ε)`, and
maximizing likelihood is exactly **maximizing the count of explained
bloom positives**. Option gains are literal counts (an orphan path of
length L explains L+1 positives; an end write explains 1; displacing a
soft write un-explains 1). The lexicographic bands (reseat 2·10⁹ ≻
demand skip 10⁹ ≻ coverage 10⁶ ≻ likelihood units) encode the prior ≻
likelihood order; correctness requires only the separation inequality
(each band exceeds the maximum attainable sum of lower bands), so the
constants are representatives of an order, not tuning.

## Architecture

### Propagation: arc consistency, ties never broken

- **Rule C** (obligation before discretion): an open end gated by a
  window (bloom-positive, in depth range, chain-consistent) is written
  into every chain that gates it; surplus writes are soft.
- **Rule A**: a level with exactly one eligible candidate takes it.
- **Rule B**: orphan unification commits only when single-window,
  single-chain, and sole-claimant on every level (eager variants were
  measured to open false-positive channels).

### Candidate walk and doors

Per window, candidates are gathered by descending from the anchor
through surviving edges while bloom tests stay positive (a true member
can never sit below a true negative on a survivor path), re-entering
across drop gaps via fragment roots registered to this window's anchor
by their carriers' prefixes. Registration-keyed doors are sufficient:
carrier-less gapped fragments are orphans (whose placement domain,
`pathOn`, is already evidence-keyed over **all** windows), and
carrier-bearing fragments register to the window their material belongs
to. An evidence-keyed "door-complete" variant was built and reverted:
the case it covers is structurally vacant (the CANDCHECK probe found
zero missing candidates, ever) at real combinatorial cost. CANDCHECK
remains armed as the tripwire.

### Threading as search, sticky

A chain level with ≥ 2 eligible candidates is a solver decision, not a
wall: each candidate contributes its unique-descent continuation as an
option, scored by explained bits ("thread items": free skip, no
coverage band — no invariant demands threading, so demand items always
dominate them in shared clusters). Exact posterior ties are **forced**
(`--tie-policy aware`, default: prefer the candidate not already
explained by another chain — the same likelihood applied across
windows; span-ID order as final fallback; `stop` and `id` retained for
ablation). Thread decisions are **sticky**: re-litigated only when a
demand's seating displaces them. Spontaneous re-adjudication was built
and reverted — it defended a channel never observed while causing
measured churn. The solver runs even in traces with zero invariant
demands (its earlier absence in quiet traces was the entire residual
shallow-attachment rate at low drop).

### Demand model

- A span that is both an unplaced orphan root and an uncovered open end
  is ONE assignment per level (identity-aware writes), not a collision.
- **Implied-end suppression**: an end on its orphan's *common spine*
  (spans present in every option path) never becomes a separate demand
  item — its coverage is implied by the placement invariant, and a
  separate item double-counted one obligation. Branch ends keep full
  independent demands.
- **Scoped end-coverage closure**: a sitting C/D write that is an end's
  sole appearance joins any cluster whose options touch its level, so
  displacement and re-seating are decided in one joint assignment.
- **Lexicographic displacement pricing**: displacing an end's sole
  coverage write costs at the coverage band (10⁶) — likelihood gains
  can never out-bid an invariant.

### Search: plain deterministic branch-and-bound, disclosed cap

Items cluster by shared chains (union-find); clusters are independent
sub-problems (per-cluster optimality composes). Each cluster is solved
by branch-and-bound with the simple admissible bound (sum of remaining
items' best gains) and a **disclosed certification budget of 10M
nodes**: a handful of clusters per corpus run (tight assignment cores
up to ~800 items) exceed any practical budget while certifying among
near-tied all-seated arrangements; they ship their best-found COMPLETE
seating (the first descent finishes within ~|items| nodes), are counted
(`capped=N`) and anatomized (CLUSTERDUMP). Capped best-found seatings
have never measurably changed any metric, and all invariants are
re-verified by the census regardless.

The deliberately plain search preserves **re-solve idempotence**: on an
unchanged ledger a re-solve reproduces its previous assignment, so the
propagate/solve rounds reach a fixpoint. Three search-space reduction
layers — option deduplication, symmetry breaking, a mutual-exclusion
(per-level value cap + pigeonhole) bound — were built, measured, and
**removed**: their kept-sets and canonical orders depended on the
mutating ledger, so each round's re-solve returned a *different*
equally-scored optimum, and the resulting churn (capped re-solves ×
unbounded rounds) cost far more than the techniques saved. Recorded as
future work below.

## Verification

Three pillars, all reproducible from the shipped tooling:

1. **Analytic sanity anchor**: at `--drop-rate 1.0` reconstruction
   degenerates to pure prefix anchoring with a closed-form prediction
   of perfection. Measured at full corpus: 167.9M bridges across cpd 3
   and 6, zero errors, zero residual diagnostics.
2. **Scorer non-leniency** (`--score-audit`,
   `cmd/trace_recon/audit.go`): the dangerous failure direction for the
   scorer is leniency (over-crediting reports better-than-actual
   results). Fault injection corrupts a copy of each trace's
   reconstruction and requires `ScorePCR` to move by exactly the
   penalty predicted by an independent ancestry implementation. Five
   single-fault classes (lateral / same-depth / shallow / descendant
   anchor corruption; silent fragment deletion) plus random multi-fault
   cocktails of 1–8 interacting faults graded by full score-vector
   agreement (deletions change the reduced set and other bridges'
   correct grades, so cocktail expectations are derived globally).
   Measured at full corpus: **2.3M injected faults, 100.0000% detected
   with exact penalties, zero misclassifications.**
3. **Adversarial definitional tests** (`recon/score_traps_test.go`):
   descendant anchors, same-depth siblings, shallow ancestors, and both
   directions of reduced-set loss billing (losses must be billed;
   surviving nearer ancestors must not be skippable).

Cross-checks: the scorer's shallow-bridge counters match the
independently-logged BENIGNSKIP event stream exactly in every measured
cell; worker-count and seed determinism verified byte-identical.

## Scoring (`recon/pcr.go: ScorePCR`)

- **exact**: anchored to the nearest *surviving* true ancestor — the
  information-theoretic ceiling given who survived.
- **wrong** (reported including the shallow sub-class, per project
  ruling): anchor is not the nearest surviving ancestor. The shallow
  sub-class (true ancestor, too high) is broken out diagnostically; it
  measures 0.00% in all twelve corpus cells (literal zero in nine; a
  parts-per-million residue of bloom-FP "end thefts" in the rest, each
  event logged with span IDs).
- **Reduced-set semantics**: discarded fragments are billed to `lost%`
  and treated as absent by accuracy metrics — accuracy and coverage
  cannot launder each other (`tr%` = traces containing ≥1 wrong
  bridge).
- Field algebra (validated against reference runs): with
  `R = num_reconnected = C + W`: exact `= C/R`, shallow `= (A−C)/R`,
  wrong `= W/R`. Canonical table: `scripts/results_table.py`.

## Results (Uber corpus, 451,466 traces/cell, K=4, fp 1e-4)

Per-bridge wrong% / traces-affected%, by non-checkpoint loss rate:

```
            5%        25%        50%        75%        95%       100%
cpd3   0.0070/0.07  0.0197/0.58  0.0285/1.16  0.0239/1.20  0.0068/0.45  0/0
cpd6   0.1633/0.84  0.2560/3.32  0.3429/5.91  0.3228/6.12  0.1090/2.58  0/0
```

Per-bridge error peaks at 50% loss; traces-affected peaks at 75% loss
(bridges-per-trace keeps growing after the per-bridge rate turns down).
Open-end coverage 100.0000% and span loss 0.000% in all cells; 6.2M
orphans placed at the heaviest cell.

## Known residue and future work

1. **ε-floor end thefts**: an open end absorbed into a wrong chain by a
   bloom false positive; its true descendants then anchor shallow
   (parts-per-million, individually logged, repeat victims across drop
   rates confirm deterministic bloom collisions). Open question: the
   stolen end should also hold its true seat via rule-C multi-write —
   which guard blocks it is undiagnosed (CANDCHECK forensics queued).
2. **Work-unit cap**: the 10M cap counts recursions, not work; clusters
   with very fat option lists take minutes to reach it. A budget in
   option-checks (~5·10⁸) would bound wall time uniformly. Designed,
   not yet applied.
3. **Full certification of capped cores**: Régin-style dynamic matching
   feasibility filtering is the principled route (static matching
   bounds buy nothing — the cores admit perfect matchings; the
   explosion is near-tie certification). Any reintroduced quotient
   technique (dedup/symmetry) must be keyed to a round-frozen ledger
   snapshot, or seed re-solves with the previous assignment, to
   preserve idempotence.
4. **Standalone structural verifier** (`cmd/recon_verify`, certifying-
   algorithm pattern): re-derive tree-ness, span conservation, and
   per-bridge evidence (prefix bytes, bloom bits) from raw payloads,
   independent of the engine. Design agreed; artifact-vs-in-memory
   interface open.

## Diagnostics (`TRACE_RECON_DEBUG=1`)

| line | meaning |
|---|---|
| `PROGRESS traces=…` | cumulative metrics every 10k traces (always on) |
| `SOLVE t=…` | per-round demand/seating census |
| `ROUNDCAP t=…` | round loop exited by exhaustion, not convergence |
| `SEARCHSKIP …` + `SKIPOPT …` | item left unseated by a cluster optimum, with per-option autopsy |
| `ITEMZERO t=…` | demand orphan with empty domain (theorem-violation alarm) |
| `UNPLACED t=… class` | census-time unplaced classification |
| `DRYEND …` | census-time unmatched-end forensics |
| `CLUSTER solve/nodes/capped` | cluster sizes, node counts, budget hits |
| `CLUSTERDUMP t=…` | anatomy of any cluster crossing the cap |
| `THREADTIE t=…` | forced posterior ties (disclosure counter) |
| `BENIGNSKIP C=…` | scorer-side: surviving ancestor bypassed by a shallow bridge |
| `CANDCHECK t=…` / `ENDMATCH t=…` | targeted probes via `TRACE_RECON_DEBUG_ENDS=hexids` |

The accounting discipline: *silence is a bug* — every unplaced orphan,
unmatched end, skipped item, capped cluster, and forced tie is named,
classified, and counted; a residual outside every diagnostic class is
itself the finding.
