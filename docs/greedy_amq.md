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

## Borrowed evidence is defeasible

An orphan fragment carries no window evidence of its own. `cgpResolveEvidence`
admits it by *borrowing* a deeper carrier whose filter tests positive for both
the orphan root and its named parent. That two-hit corroboration is a
probabilistic inference, not a fact: with the default 1e-4 query FPR it fails
at roughly 1e-8 per candidate carrier, and a full day of Uber traces offers
enough candidates for it to fail. When it does, the borrowed filter describes
an unrelated branch, and its negatives say nothing about the orphan's
ancestry.

The engine therefore treats a borrow as a hypothesis at two points.

**At borrow time**, every candidate carrier must also accept each ancestor the
trace already knows exactly for the orphan's parent: the HA witnesses carried
inside sibling fragments whose roots literally name that same parent, at
depths inside the candidate's window. A filter that rejects such an ancestor
cannot belong to a descendant of the orphan, because a Bloom has no false
negatives. This is an exact rejection. It moves the borrow to the next
admissible carrier, which in the observed failure was the orphan's true
descendant.

**At routing time**, borrowed AMQ constraints are tagged. If a route unit
exhausts every candidate, the engine sets aside its members' borrowed
filters, rebuilds their candidate anchors from the members whose evidence is
their own, and searches once more. Exact evidence -- literal parents, HA
witnesses, and the carriers' own filters -- still applies in full. If a route
is then found, the borrow is **retracted**: the filter never re-enters the
tracker and is excluded from the final audit, the fragment stops supplying
candidates or a window, and `borrow_retractions` is incremented. If the
retry also fails, everything is restored. The same retry protects the
certain-ancestor fallback below.

Retraction is deliberately conservative. It fires only when a borrowed filter
is the sole reason no exact-evidence-compatible route exists; a borrow that is
merely wrong but not contradicted continues to shape routing and is measured
as ordinary Bloom false-positive error. `--greedy-no-borrow-retraction`
restores the historical behavior for ablation.

## The certain-ancestor fallback is a guarantee, and its failure is counted

After every unit has had its turn and pending fanout obligations have
propagated, a unit still without a parent edge is attached to the deepest
ancestor it knows for certain: a required HA fanout that survived, or the
window root every resolved member names through its own checkpoint prefix.
Both are exact; attaching to either is admissible by construction. Earlier
the fallback knew only the window root and could not pass through a
surviving required fanout, so an exact HA fact defeated the last resort.

Routes are never applied partially. A candidate that cannot reserve a private
node for every gap level is refused rather than installed as a dangling
chain, and private nodes are reserved on demand for anchors discovered after
setup, with their depths registered for the hard checks.

If a unit knows a certain ancestor and still cannot reach it, the evidence it
holds is self-contradictory. That is counted in `unrouted_units` and included
in `hard_conflicts`. A run reporting zero hard conflicts therefore also
asserts that no such unit was left dangling.

The final parent map is audited separately from the incremental tracker.
`amq_conflicts` counts carriers with contradictory named ancestry, including
invalid literal input evidence, and is included in `hard_conflicts` alongside
parent and HA conflicts and `unrouted_units`. `amq_prunes` counts route trials
rejected by these AMQ checks; `borrow_retractions` counts orphan borrows
withdrawn because exact evidence contradicted them. All are included in the
PB0/CGP0 and SB3 JSON summaries. Retracted borrows are excluded from the
final AMQ audit.
Older results without these checks cannot establish global AMQ consistency
from their zero parent/HA conflict counts.

Regression tests use actual emitted payloads with a real Bloom false positive,
cover fixed CPD and the 2:8 policy, and independently walk the returned parent
map against carrier filters. Additional tests cover deferred constraints,
transaction rollback, and contradictory input. The randomized-checkpoint
reconstruction matrix also checks every carrier's reconstructed ancestry.
Pending-fanout regressions exercise both arrival orders, greedy fallback,
rollback, and exclusion of checkpoint roots and carriers from AMQ membership.
