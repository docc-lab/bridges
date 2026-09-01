# Full-evidence greedy reconstruction and ablations

CGP0 and SB3 use one non-SAT topology design. The default engine enumerates
Bloom-admissible routes, removes candidates contradicted by exact evidence,
then uses deepest-anchor/named-fanout-first greedy order only to choose among
the remaining routes. SB3 adds sparse ordinal constraints to that same
candidate loop; CGP0 does not carry or consume ordinals.

The default engine uses all currently supported non-ordinal mechanisms:

1. Surviving `ParentID` values materialize exact dropped parents, and fragments
   naming the same parent share one route unit.
2. Blooms from exact-parent siblings and HA-witnessed fanout groups are
   intersected for claims about their common upstream ancestry.
3. An HA carrier must descend through the named fanout at its exact absolute
   depth. These obligations are tracked transactionally as routes are tried.
4. If the ordinary deepest/named-first route contradicts hard evidence, it is
   rolled back and the next admissible route is tried. Greedy order remains the
   tie-breaker; it is not allowed to override evidence.

No CP-SAT or other solver is involved.

## Command-line ablations

| Flag | Modes | Effect |
|---|---|---|
| `--cgp0-legacy` | CGP0 | Restores the former lean first-match algorithm, anonymous ordinary gaps, opportunistic HA joins, and no global hard-evidence tracker. |
| `--greedy-no-grouped-evidence` | CGP0, SB3 | Lets one deterministic member nominate a shared route instead of intersecting the proven group's Blooms. Exact parents and hard HA remain active. |
| `--greedy-no-hard-ha` | CGP0, SB3 | Keeps HA identities as optional Bloom-confirmed fanout names but does not require carriers to route through them. Final hard-conflict telemetry remains active. |
| `--greedy-no-route-fallback` | CGP0, SB3 | Evaluates only the first deepest/named-first route. A hard contradiction leaves the unit unresolved instead of trying the next candidate. |
| `--sb3-ignore-ordinals` | SB3 | Excludes sparse ordinals from topology candidate pruning while retaining every non-ordinal mechanism above. Ignored ordinal incompatibility is not charged to topology accuracy. |

Flags may be combined for factorial accuracy/runtime experiments. CGP0 JSON
outputs record the selected flags and a `greedy_summary` containing candidate
evaluations, hard overrides, and final parent/HA conflicts. The stderr summary
reports the same counters.

## Interpretation

`--cgp0-legacy` is the historical speed baseline; it is not the production
default. The fine-grained flags isolate specific mechanisms inside the shared
engine. In all default runs, `hard_conflicts` must be zero. A nonzero value is
an invariant failure, while a nonzero value under `--greedy-no-hard-ha` is an
expected measurement of what that ablation sacrificed.
