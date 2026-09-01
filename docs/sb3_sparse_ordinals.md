# SB3: sparse branch ordinals over a reconstructed topology

## Motivation

The current S-Bridge encodes a start ordinal at every level of every emitted
breadcrumb. Those ordinal paths form an ordered trie, so the structure layer
also acts as a dense topology encoding. That makes the attractive conceptual
stack

```text
P-Bridge -> CG-Bridge -> S-Bridge
```

misleading: current S-Bridge does not merely order a topology recovered by the
lower layers; its ordinals independently recover that topology.

SB3 separates the concerns. It first recovers an unordered topology using the
P/CG probabilistic substrate, then adds only the evidence needed to order actual
fanouts.

## Sparse-ordinal rule

For a parent whose children start in the order `c1, c2, ..., ck`:

- `c1` is the implicit continuation child and emits no ordinal record.
- `c2 ... ck` retain their ordinary start ordinals `2 ... k` and each append
  one record to the propagated sparse chain.
- A unary edge contributes no record.

The number of ordinal-bearing edges in a rooted tree is

```text
B = sum_v max(children(v) - 1, 0) = leaves - 1.
```

Consequently, chains and mostly-unary trees are cheap, while a flat `k`-way
fanout still needs `k-1` records. SB3 should therefore be described as having
cost proportional to branching complexity, not as being uniformly sparse. In
the wide-fanout limit it approaches dense ordinal encoding. That is expected:
exactly associating and ordering `k` distinct branches requires linear evidence
somewhere. An approximate SB3 variant may later cap, hash, or omit records at
very wide fanouts.

## Layering

The intended reconstruction pipeline is:

```text
CGP0 greedy substrate recover an initially unordered tree, including synthetic nodes
SB3 vertical layer   align sparse ordinal chains to fanout-child edges
SB3 lateral layer    apply the existing EE/DEE event-order evidence
```

The emitted topology substrate remains CGPRB's checkpoint prefix, window Bloom,
and hash array, but SB3 has its own greedy reconstructor. It does not call a PB
or CGP reconstructor and it never invokes SAT/CP-SAT. The engine groups surviving
fragments by their literal dropped-parent ID, intersects the sibling fragments'
Bloom/HA evidence, enumerates every admissible surviving anchor and intermediate
named fanout, and starts from the ordinary deepest/HA-preferring choices.
With ordinal guidance disabled, that deepest-first choice is the regression
oracle: SB3 must not substitute a second global topology objective.

Sparse ordinals are compiled into admissibility constraints before a route is
selected. Bloom and pooled fanout evidence enumerate candidate surviving-anchor
paths. An incremental checkpoint-window automaton assigns exactly one implicit
child edge at each observed fanout and an injective ordinal `2..k` to every
other child edge. A candidate that would give two child groups the same ordinal,
give one child group inconsistent witnesses, or introduce a second implicit
child is rejected before it is committed. The automaton is transactional during
candidate trials, so rejection rolls back both topology and ordinal assignments.
It does not build a complete guessed topology and score ordinal conflicts
afterward.

Repeated witnesses for the same ordinal are legal only when they remain inside
the same immediate child subtree. Under a fixed fanout and consumed chain prefix,
equal next ordinals therefore form a must-link child group, while different next
ordinals form different child groups. The qualifier matters: two carriers in
the same ordinal-bearing subtree naturally inherit the same record.

Literal ParentIDs and HA ancestry witnesses are hard facts. Exact named parents
are materialized directly. Every HA witness is installed in a transactional
path tracker before greedy selection begins. Reaching its named fanout satisfies
the constraint; crossing the fanout's absolute depth through another node
rejects and rolls back the tentative route; stopping at a node whose parent is
not reconstructed yet leaves the constraint pending on that exact node.

Whenever a later candidate supplies a pending node's parent, the tracker
advances the obligation in the same transaction. If a descendant hands an
obligation to an upstream route unit after that unit's first greedy turn, the
unit is revisited with the witnessed fanout installed as a required path node.
Thus missing ancestry in a partial topology is unresolved rather than falsely
contradictory, but no unresolved HA obligation may survive final construction.
This distinction preserves the CGP0 greedy base without allowing an emitted HA
conflict.

If every ordinal-admissible path in a candidate domain conflicts with a hard
witness, SB3 keeps the first hard-compatible route and reports the ordinal
incompatibility rather than discarding the exact fact. Only that route unit's
ordinal carriers are quarantined from further pruning; the rest of the
checkpoint window continues contributing ordinal constraints. Intermediate
optional HA joins remain named-first, with a private anonymous fallback.

For scalability, candidate IDs are indexed by their Bloom probe masks (an exact
accelerator, not an extra approximation). Connected carriers update only the
fanouts along their at-most-CPD path; already assigned fanout edges are checked
without copying the rest of a wide fanout. The final full alignment runs once as
verification and ordered-tree materialization. Candidate evaluations, ordinal
prunes, hard-priority overrides, and final conflicts remain observable so each
evidence source can be measured.

This is algorithmic non-interference rather than an information-theoretic claim:
ordering evidence inevitably reveals that a fanout exists, but topology choices
remain explicitly enumerable and the evaluation can measure how often ordinal
evidence validates or changes them.

## Window-local sparse chains

Like the existing bridges, SB3 resets at checkpoints. An emitting checkpoint
describes its position in its parent window and then becomes the root of a new
window. A leaf describes its path from the current window root.

For each carrier, its sparse chain is the sequence of non-first child ordinals
encountered between the window root and that carrier. For example:

```text
root
|- A (implicit first)
|  |- A1 (implicit first)  chain at A1: []
|  `- A2 (ordinal 2)       chain at A2: [2]
`- B (ordinal 2)           chain at B:  [2]
```

The two `[2]` chains are not ambiguous after the base topology has associated
each carrier with its recovered path: one decorates the `A -> A2` edge and the
other decorates `root -> B`.

## Depth-free ordinal alignment

Ordinal records carry no depth. The decoder already knows the recovered path
from a window root to every carrier. It aligns the sparse chains top-down:

1. Partition the window's carriers by the immediate child subtree of the
   current recovered node.
2. The implicit-first subtree has at least one carrier reachable through only
   implicit-first edges, and hence at least one empty remaining chain.
3. In every non-first child subtree, every remaining chain begins with the same
   local ordinal `j >= 2`.
4. Assign that ordinal to the child edge, consume it from all carriers in the
   subtree, and recurse. Recurse into the implicit-first subtree without
   consuming a record.

This relies on the bridge's carrier invariant: checkpoints and leaves emit and
are not dropped, so every child subtree has a carrier before or at the next
window boundary. Missing carriers, conflicting prefixes, duplicate ordinals, or
an ordinal outside `2..k` make the recovered topology ordinal-incompatible. A
greedy reconstructor should reject or try its next candidate rather than invent
an alignment.

Absolute depth is still part of the underlying P/CG topology substrate and may
remain in DEE owner records. "Depth-free" applies specifically to entries in
the sparse ordinal chain.

## EE and DEE remain lateral

The existing event-order scheme remains semantically unchanged:

- An EE group records earlier siblings that ended before a child started. Such
  a group is necessarily empty for child 1, so it can ride directly beside the
  sparse record for child 2 or later without losing information.
- EE values continue to use the original `1..k` child ordinal namespace.
- DEE records retain their owner fingerprint, owner depth, child-count/rank
  information, queue model, optional per-instance queues, optional single-pop
  behavior, and optional Lehmer coding.
- Once every recovered fanout has been labeled, the existing merged-event and
  EE/DEE structure reconstruction can run without changing its ordering model.

Keeping ordinals as `2..k` instead of renumbering them to `1..k-1` is important:
it preserves the current EE/DEE namespace and keeps the lateral implementation
orthogonal to the new vertical representation.

### Topology-to-structure handoff

The topology engine represents the sparse-unlabeled continuation edge as
ordinal `0` while it is testing candidate routes. That zero is not a usable
event label and must not leak into the structure phase. After a topology is
accepted, SB3 completes every sibling set as follows:

1. assign ordinal `1` to the unique sparse-unlabeled first-child edge (and to a
   unary child);
2. retain the explicit sparse labels `2..k` on the other edges;
3. require the result to be a bijection over `1..k` before consuming any
   lateral evidence;
4. validate each child's EE block in that completed namespace, attribute DEEs
   by owner depth/fingerprint plus child/end compatibility, and materialize the
   parent's complete end order.

DEE attribution is allowed to identify an otherwise anonymous synthetic parent
when it is the unique topology/content-compatible candidate. The learned owner
fingerprint is then fixed for later evidence. Multiple compatible anonymous
parents are an ambiguity and no guess is made; a DEE with no compatible parent
is likewise rejected. A parent is structurally complete only when EE plus DEE
name exactly `k-1` distinct child ends, leaving exactly one implicit last end.

`ReconstructSB3WithDEE` implements this entire handoff. DEEs are grouped by the
trace ID embedded in each record before the call, because a record may be
physically transported by a later trace. `DecodeSB3SpanPayloadFull` exposes
those trailing records to collectors. `ReconstructSB3` remains a convenience
call with no DEE input and therefore reports an incomplete structure whenever
a parent actually requires delayed evidence. `--topo-only` marks the lateral
phase intentionally omitted rather than complete.

## Evaluation questions

The implementation should report enough information to answer:

- branch density `B / (N - 1)` per trace;
- sparse-chain and total payload distributions;
- ordinal alignment success/conflict rates on greedy PB/CG topologies;
- how often ordinal compatibility changes a greedy topological choice;
- topology, event-order, and critical-path accuracy;
- the wide-fanout tail where SB3 approaches dense ordinal encoding.
