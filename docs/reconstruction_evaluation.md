# Canonical reconstruction evaluation

This document fixes the comparison and scoring contract for PB0, CGP0, and SB3.
Headline accuracy numbers must not mix evidence ablations, different samples,
different drop realizations, or different scorers.

## Canonical implementations

- **PB0:** the default full-evidence greedy path engine over the P-Bridge
  checkpoint-prefix/window-Bloom payload. It uses literal ParentID route units,
  same-parent Bloom pooling, all reachable in-window Blooms, checkpoint bounds,
  and deterministic deepest-anchor selection. It does not consume the HA
  topology extension or sparse ordinals.
- **CGP0:** the default full-evidence greedy engine. It uses literal ParentID
  materialization, grouped Bloom evidence at exact-parent and HA fanout points,
  hard HA ancestry, and admissible-route fallback.
- **SB3:** the default full-evidence greedy engine with sparse ordinals enabled
  as candidate-pruning evidence, plus all mechanisms used by CGP0.

`--pb0-legacy`, `--cgp0-legacy`, `--sb3-ignore-ordinals`, and the
`--greedy-no-*` switches are mechanism ablations. They may be reported in a
separate ablation section, but they are not alternative headline models and
must not be used to claim bridge-to-bridge accuracy.

## Matched-run requirements

A bridge comparison uses the same:

- corpus and trace selection (including sample size and sample seed);
- checkpoint distance, Bloom target and rounding mode;
- drop rate, base seed, and per-trace drop-seed policy;
- worker-independent deterministic reconstruction inputs; and
- scorer policy.

The output fields `topology_summary.score_policy` and
`topology_summary.evidence_profile` identify the scorer and whether the model
used its maximal evidence. Canonical CGP0/SB3 results use
`evidence-bounded-v1`; canonical PB0 uses `path-evidence-v1`. Every headline
model uses `maximal`. The JSON manifest also records the corpus,
trace store, sample, sample seed, Bloom prime-rounding mode, and all drop/config
parameters needed to reject mismatched comparisons.

## Evidence-bounded topology score

An exact span identity is **nameable** if any surviving record exposes it as:

1. a surviving `SpanID`;
2. a survivor's literal `ParentID`; or
3. an HA `ParentID`.

For every nameable non-root truth node, the scorer checks the entire parent
segment up to the next nameable truth ancestor:

- a nameable truth identity must appear exactly in its truth position;
- one anonymous reconstructed node is allowed for each intervening truth node
  whose identity appears in no surviving record;
- the anonymous chain must have the right length and terminate at the exact
  next nameable ancestor; and
- a survivor or a different named synthetic in an anonymous slot is wrong,
  even if it happens to have the right depth.

Missing nameable nodes or edges, missing/extra anonymous levels, cycles, and
wrong terminal ancestors are errors. A dropped truth node is forgiven only if
no surviving record names it and no nameable descendant makes its anonymous
position observable.

SB3 sparse-ordinal incompatibility is a separate structural-constraint error,
not an edge. A trace is clean only when both `edge_wrong` and
`constraint_wrong` are zero. This keeps the edge partition auditable:

```text
edge_exact + edge_anonymous_valid + edge_wrong = real_nodes
```

## Trace denominators

The harness reports both:

- `clean / feasible` for traces whose surviving records expose at least one
  reconstruction obligation; and
- `clean_all / traces`, where an empty reconstruction is credited only when the
  canonical scorer actually finds no observable error (`empty_clean`).

Do not automatically credit every empty result. The scorer is run on empty
results as well, so an empty result with a missing observable edge remains
incorrect.

## P-Bridge path score

Every surviving span whose literal parent is absent is an observable fragment
root. `path-evidence-v1` requires exactly one bridge for each such root. The
bridge must terminate at the true nearest surviving ancestor and contain the
depth-implied number of anonymous synthetic levels. Missing or duplicate
bridges, wrong anchors, wrong gap lengths, and bridges for non-fragment roots
are errors. A dropped subtree with no surviving descendant is forgiven because
no surviving P-Bridge record reveals it.
