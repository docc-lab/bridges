# CGP-on-PCRS as a CP-SAT model

Status: design spec (pre-implementation). Goal: reconstruct call-graph topology
(`--cgrp`) by expressing the **entire** per-cluster MAP problem — placement,
naming, routing, coverage, threading — as one CP-SAT model, so the solver owns
all optimization AND all propagation. We delete the hand-rolled arc-consistency
(rules A/B/C/D, the round loop) and the Go branch-and-bound (`solveCluster`),
because both are things a real CP engine does correctly by construction.

Guiding principle: **Go builds the problem; CP-SAT solves and propagates it.**
"Custom code" is fine for *constructing* the model; it is not fine for
*re-implementing search or constraint propagation*.

## The split

Stays in Go (construction — not MAP-solving):
- windowing, Pass-1 prefix anchoring (`coveringPCRBPayload`, checkpoint hit);
- candidate enumeration: the bloom + chain-consistency tests that decide which
  options *exist* (`pathOn`, `gate`, `chainConsistent`);
- the **name universe** (every nameable dropped position + candidate identity);
- bloom → objective-weight precompute;
- **clustering** into independent subproblems (kept: monoliths grind — see the
  monster-cluster work-cap history; CP-SAT still needs bounded models).

Goes into CP-SAT (per cluster):
- placement, naming, threading occupancy, routing, coverage, exclusivity, and
  the explained-bloom-positives objective.

## Granularity: window is the unit; cluster is its minimal closure

Checkpointing's purpose is window-locality — the bloom resets at each checkpoint,
so reconstruction happens inside a window against its anchor. The **window is the
natural solve unit**. We do NOT solve the whole trace jointly (that is the grind
the work-cap exists to avoid).

A **cluster** is not "going beyond the window for full-trace optimality" — it is
the *minimal* coupling, computed as union-find (per trace) over the solve **items**
(orphans, open ends, thread items, reseats), keyed by **shared chain OR shared
named synthetic**: two items merge iff an option of each lands on the same fragment
chain, or both are coalesced onto the same named synthetic (a fan-out binds its
children into one cluster). With no cross-window link, every cluster *is* a single
window. A cluster grows past a window when (a) a carrier-less **orphan is gated in
more than one window** — its `pathOn` options touch chains in several windows and
union them — or (b) a **checkpoint root names a synthetic in its parent window**,
linking the two. In those cases joint solve is the *only* correct resolution;
assigning greedily would be a guess. Pre-coalescing makes much of this structure
**hard**, which *shrinks* the search inside each cluster.

So one CP-SAT model per cluster gives per-window solving in the common case and
minimal cross-window coupling only where an ambiguous orphan forces it — never the
whole trace. **Naming shrinks clusters back toward pure windows:** an exact
parent-id match to a named synthetic pins an orphan to one window, dropping it from
the others and splitting the cluster the orphan was bridging.

## Naming model (the `--cgrp` core)

Three concepts kept distinct (conflating them as a hard `occ`+`'B'` wall is what
broke orphan placement — orphans got walled out of slots they needed):

- **Synthetic** — an inferred dropped position on a chain.
- **Named synthetic** — a synthetic *with an identity*. Same structural role
  (orphans may sit directly above or below it); extra constraints only: unique
  identity, never merges with a differently-named synthetic, descendants route
  through it. Only **un**named synthetics merge.
- **Materialization** — a real span occupying a position. An orphan whose
  parent-id equals a named synthetic materializes as its child.

Names come from (all are EXACT — a parent-id is a certain identity — so all HARD):
1. **HA entries** — dropped branch points witnessed by survivors. Known up front.
2. **Every fragment root** — its shallowest node has a dropped parent (that's what
   makes it a root), so `root.Depth-1` is a named synthetic, identity =
   `root.ParentID`. This holds **even when the root is a checkpoint**: the
   checkpoint is the anchor, but the span above it dropped too, so its `ParentID`
   names a synthetic in the *parent window* — which is how naming climbs across
   checkpoint boundaries (and another cross-window cluster link). For non-checkpoint
   roots this is the slot PCRS already *reserves*; now it carries an identity.
3. **Every orphan** — same: a carrier-less orphan names the synthetic directly
   above it by its `ParentID`.
4. **Transitively** — as placement climbs, each materialized span's dropped parent
   names the slot above it, engaging more blooms and more exact child-edges.

Because (4) depends on placement, naming↔placement is a **fixpoint**; we reify the
universe (below) and let CP-SAT propagation be the fixpoint rather than looping.

**Pre-coalescing fan-outs (hard requirement).** Group all fragment roots + orphans
by `ParentID`. Every distinct dropped `ParentID` becomes ONE shared named synthetic;
all children carrying it are **coalesced** onto that node and **must** route
through it — enforced, not scored, because parent-id equality is *certain*. A
coalesced parent with ≥2 children is a recovered **fan-out** (a sibling group): the
exact topology signal, committed hard before any bloom-scored option is considered.
This is the cgprb `directOf` / cpd2 invariant lifted into the model — named nodes
are unique and never merge; only un-named synthetics merge.

**Soft common-spine coalescing (scored, window-local).** Distinct from the hard
parent-id case above. Whenever an orphan is reachable via **more than one path
within the same window**, the model must *offer* merging those paths through the
orphan as a candidate — the orphan sits on their common spine. This is **not
forced**: the solver takes it only when the combined bloom gain (fragments
connecting from below PLUS the single shared upstream connection) beats keeping the
paths separate. Restricted to within a window — a cross-window orphan picks one
window via its `place[o,p]` domain (the cluster), it does not coalesce across a
checkpoint boundary. So: parent-id coalescing is certainty (hard); common-spine
coalescing is an optimization over ambiguous multi-path reach (soft, scored).

## Variables (per cluster)

- `place[o,p] ∈ {0,1}` — orphan **fragment** `o` materialized at position
  `p=(chain fs, depth d)`, for the feasible `(o,p)` enumerated by `pathOn`/`gate`.
  `o` is an entire connected orphan component, placed **rigidly** (internal
  surviving edges fixed; only the attachment point varies); committing `place[o,p]`
  materializes the whole subtree. Gain = Σ of all member nodes' explained positives.
- `name[pos,id] ∈ {0,1}` — position `pos` carries identity `id`.
- `thread[fs,d,c] ∈ {0,1}` — interior survivor candidate `c` occupies level `d`
  of chain `fs`.
- `cover[e] ∈ {0,1}` (derived) — open-end `e` received ≥1 fragment.

## Constraints (mapped to today's mechanisms)

| current PCRS mechanism | CP-SAT encoding | clean? |
|---|---|---|
| ledger `occ`/`open()` | `AtMostOne(thread[fs,d,·] ∪ named/placed at d)` per (fs,d) | clean |
| **exclusivity** (named unique; "nothing else at depth(HA)") | `AtMostOne(name[pos,·])`; a different id at an occupied pos is just excluded | clean |
| Rule C (open-end multi-write across chains) | `cover[e] ⟺ OR(assignments placing e)` ; an end is an ancestor of all below it → multi-chain occupancy allowed | clean |
| Rule A (unique-claimant collapse) | emergent from AtMostOne + propagation (delete the code) | clean |
| Rule B (orphan unification) | `place` channeling + AtMostOne; unification = shared `name[pos,id]` | mostly |
| naming / materialization | `place[o,p] ⟹ name[above(p), parent(o)]`; HA names fixed =1 | clean |
| routing-through | `place[o,p] ⟹ name[posₖ,idₖ]` for each named ancestor on the route | clean |
| placement guarantee | `Σ_p place[o,p] = 1`, or `≤1` with dominant unplaced penalty | clean |
| reseat immovability | hard-fix `name`/`place` for committed roots | clean |
| sticky threading | **dropped** — pure round-loop artifact: a one-shot solve has no rounds to be sticky against, and its `aware` tie-break is just the explained-positives objective | clean |
| implied-end suppression / common-spine | objective shaping + excluded options at construction | **thorny** |
| displacement pricing (1e6 lexicographic) | objective weight tiers (below) | clean |
| work-cap / disclosed cap | CP-SAT deterministic time limit; accept FEASIBLE | clean |

## Objective (lexicographic via weight tiers)

Maximize, in strict precedence (each tier dominates the next, as today's bands do):
1. **− unplaced penalty** (placement guarantee) — weight `T1`.
2. **− coverage violation** (open ends unmatched) — weight `T2`, `T1 ≫ T2`.
3. **+ explained bloom positives** (the MAP likelihood) — unit weights, `T2 ≫ 1`.

So coverage/placement are *soft-but-dominant*: edge cases degrade gracefully
instead of returning UNSAT, matching the existing forcing/disclosed-cap behavior.

Coverage tier 2 applies to **every** open end — correct paths require all of them
to receive a fragment, not just branch ends. **Count the bloom gain once:** an end
on an orphan's common spine is already covered by that orphan's placement gain, so
do not add a second coverage credit for it; only this no-double-count rule is what
"implied-end suppression" reduces to. Common spine stays a construction-time
computation purely to decide which ends already have their coverage credited.

## Determinism (resolved)

The sweep/audit methodology requires machine-independent results. CP-SAT's
non-determinism comes from its multi-worker portfolio search (workers race; the
winner varies run to run). We don't need it: parallelism already lives at the
**trace** level (one goroutine per trace, many traces in flight), so the inner
per-cluster solve runs **single-worker** and deterministic, with the cores kept
busy by other traces. No throughput lost — same shape as today, CP-SAT swapped in
for the Go B&B.

Pin: `num_search_workers = 1`, fixed `random_seed`, and **`max_deterministic_time`
(a deterministic operation count), NOT `max_time_in_seconds`** — a wall-clock cap
would make a slower machine stop at a different point and ship a different
best-found on monster clusters, breaking machine-independence. This is continuous
with the current work-cap, which already counts operations rather than wall-clock
for exactly this reason; we replace the Go op-counter with CP-SAT's deterministic-
time budget.

## Shim protocol expansion (the bulk of the work)

Today the shim speaks a flat `C / I / O` items×options×gains assignment. The new
model needs: boolean vars, `AtMostOne`, reified `OR`/implication (channeling),
hard-fixed bools, and a weighted linear objective. Define a small declarative
protocol (vars, constraint lines, objective terms) emitted by Go and built into
`CpModelBuilder` in the shim. This replaces, not extends, the `C/I/O` form.

## Migration / validation (no blind swap)

1. Build the CP-SAT model engine behind `--cgrp`; current PCRS stays as-is.
2. **Shadow A/B** on the existing sweep cells (cpd×rate): require parity-or-better
   on exact / wrong / oe / lost AND the fault-injection audit.
3. Only after parity does the CP-SAT path become default; current PCRS remains the
   cross-check, never a silently-diverging fallback.

## Resolved

- **Sticky threading**: dropped (round-loop artifact; subsumed by one-shot solve +
  the explained-positives objective).
- **Granularity**: per-cluster, where the cluster is the union-find closure over
  items keyed by shared chains — a single window unless a cross-window orphan
  bridges; naming shrinks clusters back toward pure windows.
- **Determinism**: single-worker CP-SAT, `max_deterministic_time` + fixed
  `random_seed`; trace-level parallelism unchanged.
- **Coverage / implied-end**: coverage is a dominant-tier penalty on **every** open
  end. Implied-end suppression reduces to a single objective rule — count bloom gain
  **once** (no second credit for a spine-implied end). Common spine survives only as
  a construction-time computation (which ends are pre-credited) and as the trigger
  for the soft, window-local coalescing option.
- **Multi-node orphans**: placed rigidly — one `place` var per fragment commits the
  whole subtree.

## Open questions

- Deterministic-time budget sizing on the 800+-item monster clusters: measure the
  best-found quality vs. budget before fixing the cap. (A measurement, not a design
  question.)
