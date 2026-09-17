# Downstream AMQ enforcement in PB0, CGP0, and SB3

The default shared greedy engine preserves each resolved carrier's AMQ as
an ancestry constraint throughout reconstruction. Every nameable ancestor
strictly inside its checkpoint window must pass that filter. The checkpoint
root, the carrier itself, and anonymous gap identifiers are excluded from
AMQ queries. Each filter uses the geometry decoded for its assigned CPD.

Previously, candidate generation checked surviving ancestor chains only up
to their first missing parent. A later join could inherit an earlier guessed
upstream path containing an ID rejected by the joining carrier's filter.
Parent/HA conflict counters did not detect that violation.

`recon/greedy_amq.go` tracks where each filter's currently known path ends.
When a candidate supplies that node's parent, the filter follows the actual
proposed ancestry, including previously installed routes. Any negative rejects
the trial. A path ending at another unresolved parent keeps the filter pending.
AMQ state rolls back with topology when AMQ, HA, or ordinal checks reject a trial.

For CGP0/SB3, a fanout witness already establishes ancestry even before its
carrier is connected. Its filter therefore also constrains the witnessed
fanout's upstream path from the outset. This prevents an earlier optional
choice from ignoring evidence supplied by a mandatory future join. Borrowed
window evidence likewise remains active above the fragment it admitted.

When unfinished paths join, their pending fanout witnesses and filters must
also agree. A witnessed fanout is already a mandatory ancestor even while its
connecting edges are missing. Every filter at the shared unresolved terminal
must accept that fanout if its depth lies strictly inside the filter's window.
This joint check runs after advancing both trackers and before accepting a
candidate, regardless of which constraint arrived at the terminal. A negative
rolls back the candidate and both trackers, allowing the next greedy choice.
Without this check, a join could pass each tracker in isolation yet leave a
fanout requirement that the attached fragment's filter made impossible.

The existing deterministic greedy order and route fallback remain in use.
An optional join can be rejected in favor of another admissible attachment;
AMQ consistency does not guarantee recovery of the true topology. Unresolved
checkpoint identities retain the existing unresolved-window behavior.
Legacy and solver reconstructors are unchanged.

The final parent map is audited separately from the incremental tracker.
`amq_conflicts` counts carriers with contradictory named ancestry, including
invalid literal input evidence, and is included in `hard_conflicts` alongside
parent and HA conflicts. `amq_prunes` counts route trials rejected by these
AMQ checks. Both fields are included in the PB0/CGP0 and SB3 JSON summaries.
Older results without these checks cannot establish global AMQ consistency
from their zero parent/HA conflict counts.

Regression tests use actual emitted payloads with a real Bloom false positive,
cover fixed CPD and the 2:8 policy, and independently walk the returned parent
map against carrier filters. Additional tests cover deferred constraints,
transaction rollback, and contradictory input. The randomized-checkpoint
reconstruction matrix also checks every carrier's reconstructed ancestry.
Pending-fanout regressions exercise both arrival orders, greedy fallback,
rollback, and exclusion of checkpoint roots and carriers from AMQ membership.
