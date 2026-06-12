# PCRS: pure-solver reconstruction as MAP inference (`recon/pcrs.go`)

PCRS (`--mode pcrs`) reconstructs trace topology from PCRB wire payloads
(truncated checkpoint-root prefix + inherited window bloom; see
`bridge/pcrb.go`) by treating reconstruction as **maximum a posteriori
(MAP) estimation** under an explicit generative model. Everything the
engine does is one of three things: a *count*, a *theorem*, or an
*order*. There are no tuned thresholds, no greedy tie-breaks, and no
heuristic scores — this document states the model, derives the objective
from it, and discloses the engineering guards and their measured
behavior.

## The generative model

- Spans drop independently with rate `r`; checkpoint spans (depth ≡ 0
  mod cpd) always survive as carriers of `_br` payloads.
- A carrier's payload contains (a) an **exact** truncated span-ID prefix
  of its nearest checkpoint ancestor and (b) a **window bloom** over the
  spans strictly between that checkpoint and itself (the carrier's
  *window*), with bloom geometry derived from a target false-positive
  rate ε.
- Blooms have **no false negatives**: a true window member always tests
  positive. A non-member tests positive independently with probability
  ≈ ε.

Reconstruction = find the placement of all surviving material that
maximizes `P(configuration | observations)`. By Bayes this is
`P(observations | configuration) · P(configuration)`, and each factor
maps onto a solver component below.

## Terminology

- **Fragment**: a connected component of survivors. Its root's parent
  was dropped.
- **Orphan**: a fragment with no carrier — under PCR-family anchoring it
  has no exact prefix to attach by, and must be *placed* by inference.
- **Open end**: a survivor, not itself a carrier, all of whose children
  dropped. It provably lost descendants.
- **Window / chain**: each carrier's payload defines a window between
  its anchor checkpoint and itself; the reconstructor models it as a
  *chain* — one slot (level) per depth, since a window is a single
  root-path in truth.

## Invariants are prior structure, not preferences

`P(configuration)` is zero for any assignment violating:

1. **Tree structure**, one span per chain level.
2. **Walls**: the slot directly above a fragment root on its own chain
   holds a dropped span (that is what *made* it a fragment root); no
   surviving span may be written there.
3. **Spacing**: the slot above every fragment/orphan root stays
   synthetic — orphan roots cannot be direct children of placed spans.
4. **Total coverage**: every open end receives at least one window
   match. *Theorem*: an open end inside a surviving window range is a
   true member of some window whose bloom must contain it (no false
   negatives), so the truth satisfies this.
5. **Total placement**: every orphan is placed. *Recoverability
   theorem*: every orphan's spans appear in the bloom of its true
   window's carrier, so its true seat is always in its candidate domain.

Because the truth itself satisfies 4 and 5 with certainty, restricting
the hypothesis space to configurations that satisfy them can never
exclude the true configuration. The engine treats them as rules: a
violated invariant in the final state indicates a wrong commitment
upstream (or a model violation), and is surfaced by diagnostics — never
silently accepted.

## The likelihood: explained bloom positives

Fix a candidate configuration. Every positive bloom test it *explains*
(a placed true member) has likelihood 1; every positive it leaves
*unexplained* must be a false positive, contributing a factor ε. Hence

```
log P(obs | config) = const − (# unexplained positives) · log(1/ε)
```

and maximizing the likelihood is **exactly maximizing the count of
explained bloom positives**. This is the entire objective:

- an orphan-placement option's gain is `len(path) + 1` — the number of
  positives that placement explains;
- an end-coverage write's gain is 1 — the one positive it explains;
- displacing a soft (rule-A/C/D) write costs 1 — the one positive it
  un-explains.

Unit-for-unit counting in a single currency (`log(1/ε)` per bit). No
coefficient was fit to data, and the objective has a one-line
definition: *the most probable reconstruction under the stated
drop-and-bloom model*.

## Architecture: propagate, then search

### Propagation = arc consistency (deductive, zero discretion)

Rules run to fixpoint and commit **only facts that hold in every
nonzero-prior configuration**:

- **Rule C** (before A — obligation precedes discretion): an open end
  gated by a window (bloom-positive, depth in range, chain-consistent)
  is written into *every* chain that gates it. Multi-writes are sound
  because coverage is a must, and surplus writes are soft (displaceable
  by search).
- **Rule A**: unique-claimant collapse — a level with exactly one
  possible occupant takes it.
- **Rule B** (demoted to true arc consistency): an orphan unifies into
  a chain only when it has a single gated window, a single chain, and
  every needed level is claimed by no other orphan. Anything weaker is
  a *choice* and belongs to search. (Eager rule-B variants were
  measured to open three false-positive channels: wrong-window commits,
  commit-order lotteries, contamination cascades.)

Propagation never breaks ties. If a fact isn't forced, it isn't
committed.

### Search = exact per-cluster branch-and-bound (owns every choice)

Demand items (unplaced orphans, unmatched ends) plus **closure
conscripts** — everything feasibility depends on — are clustered by
shared chains (union-find) and each cluster is solved exactly:

- **Blocker closure**: a placed orphan occupying levels a demand needs
  becomes a reassignable item (whole-fragment move), recursively.
- **End-coverage closure**: a sitting end-write contending for a
  demanded level joins as an item (its full gate-set as options),
  recursively — same-depth contention is assigned jointly, never by
  displacement roulette.
- Items in different clusters share no chains, hence no constraints:
  per-cluster optimality composes to global optimality.

Within a cluster, branch-and-bound with an admissible optimistic bound
enumerates assignments. Conflict semantics are span-identity-aware:
writing the same span to the same level twice (an orphan/end "alter
ego" of one span) is one assignment, not a collision.

### The objective constants encode an order, not thresholds

The full objective is **lexicographic**:

```
keep reseated placements ≻ satisfy demands ≻ cover broadly ≻ MAP likelihood
   (reseat-skip 2·10⁹)      (skip 10⁹)       (bonus 10⁶)     (gain/cost 1)
```

Correctness requires only the *separation inequality*: each band
exceeds the maximum attainable sum of all lower bands (bounded by
cluster size × window depth × max gain ≪ 10⁶). Any constants satisfying
it produce bit-identical optima — replacing 10⁹ with 10¹² changes
nothing. The numbers are representatives of an order. The sentinel for
"no assignment yet" is −2⁶², i.e. −∞: the optimum is always returned
and applied, however many skip penalties it carries (a finite sentinel
was observed to silently freeze clusters whose optimum held ≥ 2 skips).

### Guards (present, never binding, verified per run)

Two completeness escapes exist for pathological inputs: cluster-size
cap 256 items and node budget 5·10⁶ per cluster. On the full evaluation
grid both counters are **zero** (measured maxima: 22 items, ~2.1·10⁴
nodes), so all reported numbers are exact cluster optima. Both are
asserted observable via diagnostics (below) — they are disclosures, not
knobs.

## Scoring (`recon/pcr.go: ScorePCR`)

Every metric is a count against the information-theoretic ceiling:

- **exact**: bridge anchored to the *nearest surviving* true ancestor —
  the best any method can do, since the intervening spans no longer
  exist.
- **benign**: anchored to a true ancestor, but shallower. Lineage
  correct, no false ancestry introduced; only depth resolution lost.
  Measured composition at cpd6 r0.5: 97.9% of bypassed
  nearest-ancestors are interior spans correctly anchored by their own
  fragments.
- **wrong**: anchored to a non-ancestor — a false edge. `tr%` = share
  of traces containing at least one truly-wrong bridge.
- **Reduced-set semantics**: discarded fragments are reported as
  `lost%` and treated as absent by the accuracy metrics — accuracy and
  coverage cannot launder each other.
- **Invariant audits**: `oe match` (end coverage), `placed`
  (orphans), `forced` (search-decided assignments, flagged because the
  evidence under-determined them). These are audits, not quality
  scores.

Field algebra (verified against reference runs): `num_anchor_ancestor`
includes the exact cases and `num_misattached` includes the benign
ones; with `R = num_reconnected = C + W`:
`exact = C/R`, `benign = (A−C)/R`, `wrong = (W−(A−C))/R`. The canonical
table generator is `scripts/results_table.py`.

## Honest caveats

1. **MAP is conditional on the model** (independent drops, independent
   bloom FPs — the standard idealization).
2. **Ties** between equal-posterior optima break deterministically by
   enumeration order; tie frequency is measurable if needed.
3. **Same-span alter-ego writes double-count gain** (both items score
   their shared bit). Affects only tie regions; feasibility and
   invariants are unaffected.
4. **False-positive absorption**: with ≈10⁵ open ends tested against
   many windows at ε = 10⁻⁴, a handful of ends are absorbed into a
   *wrong* chain via an FP gate (measured: 3 of 121,643 ends at cpd6
   r0.5, i.e. 0.0025% — the magnitude the model predicts). These are
   detectable in principle (two windows claiming overlapping material)
   and surface as benign-shallow attachments of their true descendants,
   never as silent loss.

## Diagnostics (`TRACE_RECON_DEBUG=1`)

| line | meaning |
|---|---|
| `SOLVE t=… orph= ends= items= any=` | per-round demand/seating census |
| `SEARCHSKIP t=… kind depth opts cluster id parent` | item left unseated by a cluster optimum |
| `SKIPOPT hard:…` / `contention:…` | per-option autopsy: illegal on the base ledger (and why) vs lost to a named in-cluster competitor |
| `ITEMZERO t=… n=` | demand orphans with empty domains (theorem violation alarm) |
| `UNPLACED t=… class depth opts bblocked` | census-time unplaced classification (`ZEROOPT`/`BBLOCK`/`OTHER`) |
| `DRYEND …` | census-time unmatched-end forensics |
| `CLUSTER solve/skip/nodes` | cluster sizes, guard hits, budget use, `found=` |
| `BENIGNSKIP C= depth open= anchor=` | scorer-side: each surviving ancestor bypassed by a benign bridge, open-end-or-interior |
| `ENDMATCH t=… id level prov window-anchor …` | targeted (`TRACE_RECON_DEBUG_ENDS=hexid,…`): where a specific end's coverage landed |

The accounting discipline these enforce: *silence is a bug*. Every
unplaced orphan, unmatched end, skipped item, and guard hit is named
and classified; a residual that doesn't appear in a diagnostic class is
itself the finding.
