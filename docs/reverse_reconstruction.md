# Reverse checkpoint evidence and reconstruction

The reverse simulator keeps forward checkpoint scheduling unchanged and moves
an eligible leaf's intact truss to an ancestor's exported span. Every span that
emits an accepted return is a checkpoint span for collection and counting. This
does not make it a forward checkpoint: its descendants retain the previously
assigned checkpoint root, distance, and Bloom geometry.

## Status

Reverse evidence is consumed by the shared PB0/CGP0/SB3 greedy topology engine
under the **intended-checkpoint model**:

- A checkpoint is any span that emits a truss. A receiver that emits only
  returned trusses is **demoted**: its own record is an ordinary span, eligible
  for collection loss like any other, and its snapshot is not checkpoint
  evidence.
- The span identified by each returned truss is **promoted**: the rejected leaf
  is the checkpoint, reconstructed from its truss with exact identity, depth,
  window root, geometry, and filter. Trusses are checkpoint payloads and are
  retained regardless of whether the record that carried them survives.
- The reconstructor therefore sees the original set of intended checkpoints.
  At total loss the survivor set is exactly that set, so PB0 joins every
  checkpoint to its named previous checkpoint root with no filter probes.

The reconstruction harness (`cmd/trace_recon`) accepts the simulator's reverse
options (`--reverse-policy`, `--reverse-probability`, `--reverse-ttl-range`,
`--reverse-seed`, `--leaf-reject`), routes trusses through
`bridge.ReverseHandler` during replay, demotes receivers that carried only
returned trusses, applies collection loss, decodes every carried bundle, binds
each truss to its checkpoint, and scores the emitted topology against pre-loss
truth with the existing strict scorers.

| Fact | Representation | Effect |
| --- | --- | --- |
| Promoted checkpoint whose record survived | Its `Span` receives the truss (`BloomBits`, prefix, geometry, HA, ordinals) with `LeafCarrier` set | Identical to a leaf that checkpointed locally |
| Promoted checkpoint whose record was lost | A new `Span` with exact `SpanID`, `Depth`, truss evidence, `LeafCarrier`, and `ParentUnknown`; `ParentID` is zero and meaningless | Fragment root with no named parent; never a trace root |
| Demoted receiver | Ordinary span record without `_br`; its carried bundle is decoded separately | Not protected, not a window root, not a carrier |
| Original checkpoint that also carried trusses | Unchanged checkpoint record; bundle decoded separately | Still a window root |
| Carrier of a truss | `EvidenceOwner` on the checkpoint `Span` | Provenance only |

`recon.MergeReverseEvidence` performs the binding. Engine behavior for a
promoted checkpoint with an unknown parent: it forms its own route unit whose
head is the checkpoint itself (`anonymousParent`). The unit's depth is the
checkpoint's depth, so the node at depth-1 is inferred from admissible evidence
like any other gap level: a surviving ordinary span at depth-1 confirmed by the
filter and every downstream filter, a witnessed or Bloom-confirmed named
fanout, an anonymous node, or the checkpoint root itself. When no ordinary
interior span survives, the only admissible anchor is the named root and no
filter is probed. Persistent downstream AMQ, HA, window, and ordinal constraints
apply unchanged. Two promoted checkpoints never coalesce by parent identity,
because neither names one.

Scoring: `ScorePBPathStrict` creates an obligation for every promoted
checkpoint with an unknown parent and requires the emitted bridge to reach its
true nearest surviving ancestor with the depth-implied synthetic count.
`ScoreCGP2Evidence` treats it as a nameable node whose parent chain must match
truth through nameable nodes and anonymous stand-ins. Trace eligibility uses
the post-routing protected set: intended checkpoints are protected, demoted
receivers are not.

Tests: `recon/reverse_recon_test.go` replays synthetic traces through the real
`ReverseHandler` for all three bridges, randomized and fixed windows, four
policies (inverse-depth, promote-all, absorb-at-checkpoints, half rejection),
and three loss scenarios (promoted checkpoints' records lost; those and their
parents lost; every unprotected record lost). It asserts that at total loss no
non-checkpoint span survives, that demoted carriers hold trusses and no own
payload, and that promoted checkpoints never become window roots.
`cmd/trace_recon/reverse_test.go` drives the full harness from a trace store.
Both require zero hard conflicts and clean strict scores with a negligible
Bloom FPR.

## Three identities that must stay separate

| Item | Meaning |
| --- | --- |
| Origin span | The span whose incoming path, HA entries, sparse ordinals, and depth the intact truss describes. |
| Export owner | The receiving span whose checkpoint record contains the returned truss. |
| Original checkpoint root | The root of the forward window named inside that truss. |

Receiving a truss changes only the export owner. A grouped checkpoint payload
can contain several origins and windows, plus the export owner's own truss.
Each constituent retains its own filter and geometry. The receiver's ordinary
`ParentID` remains the receiver's parent, not an origin's parent. Its return path
establishes an ancestor relationship in a correctly routed trace, not a direct
parent edge.

A promoted receiver's own snapshot can repeat an HA entry already present in
one of its descendants' returned trusses. Both refer to the same named fanout.
Reconstruction must retain their path constraints without treating that repeated
entry as a newly witnessed fanout or assuming one exporter per HA identity.

## Collector extraction implemented now

`DecodeReverseEvidence(ownerID, encodedValue, cfg)` decodes the binary
`bridges.checkpoint` value exposed as `ReverseEndResult.CheckpointContext`.
`DecodeReverseSegmentEvidence` handles an already
decoded segment. They return `ReverseEvidence` records with:

- Export-owner ID and immutable origin ID/depth.
- A private copy of the exact payload bytes, its bridge kind, and the decoded
  original-window prefix and Bloom geometry.
- HA entries and sparse ordinals when present; SB3 trailing DEEs remain
  separately available for collection-pipeline routing by trace identity.

The payload's encoded depth must agree with the segment's origin depth. The
declared kind must agree with the payload type. Bad payloads fail decoding; a
bundle decode returns no partial evidence. The helper never reads original
topology or the simulator-only `OriginCheckpointDepth`, never generates a
parent identity, and never marks the origin as an ordinary surviving span.

The returned bundle uses `bridges.reverse.binary.v1`, the simulator's native
binary format. Integration tests consume actual emitted `CheckpointContext`
values for all three bridge types, checking origin identity, forward window,
payload bytes, HA, and ordinals. They validate evidence extraction; they do not
establish correctness of the outstanding reconstruction-engine changes below.

## Why a naive adapter was wrong (design record)

`recon.Span` represents an ordinary surviving record with exact `ParentID`;
zero means an actual root. The current fragment parser, exact-parent
coalescing, candidate construction, and SB3 alignment all rely on that meaning.
The reverse segment contains origin ID, depth, and truss bytes, **not** the
origin's ordinary `ParentID`. A rejected leaf still exports ordinary metadata,
but that independent record becomes eligible for collection loss.

If the origin record survives, its own collected parent metadata can be paired
with the returned truss using the origin ID. If it does not survive, making a
`Span` with parent zero invents a root; copying the export owner's parent
invents an edge; recovering the original parent from the pre-loss trace leaks
evaluation truth into reconstruction. None is a valid adapter.

There is a separate role issue even when all origin records survive:
`newCheckpointIndex` currently treats a randomized non-leaf Bloom carrier as
a baggage reset. A reverse-created checkpoint's own partial-window payload
does not reset baggage and must not enter this reset category. Reusing
`LeafCarrier` to represent such a receiver would also be wrong: it is an
internal span and remains a possible ancestor, whereas that flag excludes
actual leaves from ancestor candidates.

## Engine input and processing changes (implemented as described above)

Keep ordinary collected records separate from decoded origin evidence. The
engine needs an explicit parent-known state, an explicit original-forward-
checkpoint role, and a separate checkpoint-export role. With those facts:

1. Create nodes from surviving records and exact origin identities in retained
   evidence. An evidence-only origin has known depth but an unknown parent;
   it is not an ordinary survivor and does not name a parent for coalescing.
2. Add exact edges only from surviving metadata or other explicitly encoded
   relationships. Bind every returned filter, HA witness, and ordinal chain
   to its origin node. Deduplicate evidence by origin and payload identity
   without losing its export-owner provenance.
3. Build forward windows using original checkpoint roots and intact prefixes.
   A promoted receiver supplies additional evidence in its existing window;
   it does not split that window. Do not use the export owner as a substitute
   prefix root or merge sibling filters just because they share an envelope.
4. Extend candidate generation to evidence-only origins. They can lack an
   exact parent name, so the present named-parent prechecks and same-parent
   coalescing cannot be applied to them. Their parent at depth `n-1` must be
   inferred from admissible evidence; all downstream Bloom/HA/ordinal
   constraints still apply to their reconstructed path.
5. Run SB3 sparse-ordinal alignment from each **origin** to its original
   checkpoint root. Route embedded DEEs by their own trace identifiers through
   the collection pipeline before the existing EE/DEE structure phase.

Carrier provenance can additionally supply the exact fact that a returned
origin descends from its emitter, assuming the routing contract guarantees
ancestor-only returns. That is an extra ancestry constraint, not a direct
edge, a replacement window identity, or ordinary metadata survival.

## Collection loss, protection, and scoring

The reconstruction harness currently protects a span iff its `collSpan.br` is
non-nil (`computeDropped` and `computeDroppedMulti`). Reverse routing must
finish before this protection decision. Protection must reflect the final
exported checkpoint record:

- Original checkpoints remain protected.
- An unrejected forced-leaf checkpoint remains protected.
- Every receiver that exports accepted returns is protected, once per span,
  even when it carries many origins.
- A rejected leaf has no local checkpoint payload and its ordinary metadata
  is eligible for loss. Moving its truss does not protect that leaf record.

An exported bundle survives or is lost with its owner. Under the existing
protected-checkpoint policy all accepted trusses are retained, even if their
origin records are lost. A future policy that drops checkpoint records must
drop all contents of one owner atomically, not sample the contained trusses
independently.

The current strict scorers derive required named nodes from surviving spans,
literal parents, and HA evidence. Reverse reconstruction adds exact origin
identities even when ordinary metadata is missing. Scoring must therefore
distinguish ordinary metadata survival from retained evidence, require these
evidence-named origins and constraints, and still compare emitted topology
against pre-loss truth. It must not relabel a lost origin as a surviving record
or grade only the receiver. Trace eligibility at a loss rate must use the new
post-routing protected set rather than the original leaf-protected set.

## Validation performed and still required

Use a chain and unequal-depth fanouts with original checkpoints above the
accepting receivers. Drop origin metadata while retaining its returned
payload; test partial sibling acceptance and multiple origins on one receiver.
Check that unknown parents stay unknown until reconstruction, forwarded
window roots never change, promoted receivers remain admissible ancestors,
and every rejected origin is represented exactly once. For SB3, include
ordinal evidence and a DEE belonging to a different trace than its export
owner. Evaluate topology and structure only after this ingestion and scoring
contract is implemented.

The unit and harness tests above cover chain and unequal-depth fanouts,
lost-origin and paired-origin cases, partial sibling acceptance, several
origins per owner, promoted receivers as admissible ancestors, unknown parents
that stay unknown until inferred, and duplicate HA from a promoted snapshot plus
a descendant truss. Full-corpus reverse reconstruction accuracy and timing
sweeps have not yet been run; results below the tests' negligible-FPR regime
are the next measurement.
