# P-Bridge reconstruction (`recon/`, `cmd/trace_recon`)

Reconstructs trace topology from surviving spans and their P-Bridge
payloads, then scores the result against pre-drop ground truth. Requires
payloads produced in `--emit-depth` mode (absolute depth in `_br`, `_d` on
interior non-checkpoint spans); see `docs/depth_emission.md`.

## Inputs

The reconstructor sees only what a production reconstruction module would:

- **Surviving spans**: `{spanID, parentID}` plus one decoded bridge
  attribute each —
  - `_br` = `type(1) || varint(depth) || bloomBits` on checkpoint spans
    (depth ≡ 0 mod cpd, roots included) and leaves;
  - `_d` = `varint(depth)` on interior non-checkpoint spans.
- **Global config**: cpd and the bloom false-positive rate, from which the
  bloom geometry (m, k) is derived exactly as the SDK derives it.

Ground truth (the pre-drop span set) is used **only** for scoring, never
for reconstruction.

## Definitions

- **Orphan**: a surviving span whose `parentID` is not in the surviving
  set (and ≠ 0). Orphans and surviving-fragment roots are the same thing:
  every fragment contributes exactly one bridge, regardless of its
  internal shape (fan-outs inside a fragment ride along verbatim on real
  edges).
- **Bloom window**: a span's bloom contains its ancestor chain *back to
  and including the last checkpoint*, plus itself — blooms reset at each
  checkpoint span (the checkpoint re-adds itself post-reset).
- **Checkpoint band of `o`**: depths `(o.depth, nextCkpt]`, where
  `nextCkpt` is the first checkpoint level below `o`. A carrier whose
  bloom legitimately contains `o` must sit in this band — anything deeper
  has a (surviving) checkpoint between itself and `o`, whose reset
  evicted `o` from its bloom.

## Algorithm

The algorithm box (paper form; the prose steps below expand each piece,
and the comments in `recon/recon.go` cite these line numbers as
`[alg. N]`):

```
 1: procedure reconnect_orphans(S)                ▷ S: surviving spans
 2:   orphans ← {o ∈ S : parent(o) ∉ S}           ▷ each orphan roots one fragment
 3:   sort orphans by depth, deepest first
 4:   for o ∈ orphans do
 5:     bf ← covering_bloom(o)
 6:     anchor ← find_anchor(o, bf)
 7:     g ← depth(o) − depth(anchor) − 1          ▷ exact hole size from _d/_br depths
 8:     insert g synthetic spans between anchor and o, connect with bridge edges
 9:     children(anchor) ← children(anchor) ∪ {o} ▷ bridge is walkable for later orphans
10:   end for
11: end procedure

12: function covering_bloom(o)
13:   if o carries _br then return bloom(o)
14:   band ← cpd · (⌊depth(o)/cpd⌋ + 1)           ▷ first checkpoint level below o
15:   for carrier c below o, nearest level first, walking surviving edges
        and bridge edges, pruning at depth(c) > band do
16:     if path to c crossed no bridge edge then return bloom(c)
17:     else if id(o) ∈ bloom(c) then return bloom(c)   ▷ bridges are guesses: verify
18:   end for
19:   for carrier c ∈ S with depth(o) < depth(c) ≤ band, shallowest first do
20:     if id(o) ∈ bloom(c) then return bloom(c)   ▷ safety net: membership scan
21:   end for
22: end function

23: function find_anchor(o, bf)
24:   lo ← cpd · ⌊(depth(o) − 1)/cpd⌋              ▷ last protected checkpoint above o
25:   for d ← depth(o) − 2 down to lo do           ▷ k−1 holds only the dropped parent's
26:     H ← {c ∈ S : depth(c) = d ∧ id(c) ∈ bf       siblings — never the true anchor
            ∧ chain_consistent(c, bf, lo)}
27:     if H ≠ ∅ then return any c ∈ H             ▷ |H| > 1: flag ambiguous
28:   end for
29: end function

30: function chain_consistent(c, bf, lo)
31:   while depth(c) > lo do
32:     p ← parent_id(c)                           ▷ recorded on c even if p was dropped
33:     if p ∉ bf then return false
34:     if p ∉ S then return true                  ▷ disconnection: nothing more nameable
35:     c ← p
36:   end while
37:   return true                                  ▷ floor reached: parent legitimately absent
38: end function
```

Orphans are processed **deepest-first** (`--order bottom-up`, the
default). Each orphan `o` at depth `d`:

1. **Covering bloom.** Use `o`'s own `_br` if it carries one. Otherwise
   walk down from `o`, level by level, through **edges** — real surviving
   parent links *and bridges already built by deeper orphans* — to the
   nearest `_br` carrier within `o`'s checkpoint band. When the path to a
   carrier crosses a reconstructed bridge, the borrow is **verified**:
   `o`'s ID must test positive in the carrier's bloom (the structure it
   was reached through is itself a reconstruction guess; containment is
   the evidence it's right). Deepest-first ordering guarantees that by
   the time `o` is processed, everything that descends from it has
   already been attached below it — so a carrier exists and is reachable
   whenever one survives (under the v1 drop policy leaves always survive
   and always carry `_br`, so that is: always).

   A band-bounded **membership scan** (shallowest carrier whose bloom
   contains `o`'s ID) remains as a last-resort safety net for the case
   where a deeper bridge was misattached and the walk dead-ends. Bridges
   whose bloom was borrowed (walk across a bridge, or scan) are flagged
   (`ViaCarrier`, reported as `num_borrowed_bloom`).

2. **Candidate window.** Candidates are surviving spans at depths
   `[c, d−2]`, where `c = cpd · floor((d−1)/cpd)` is the last protected
   checkpoint level strictly above `o`. The true nearest surviving
   ancestor provably lies in this window (checkpoints never drop, and
   `o`'s depth-(d−1) ancestor is by definition its dropped parent, so it
   cannot be shallower than `c` or deeper than `d−2`). Depth `d−1` is
   deliberately excluded: it can never hold the true anchor, but it does
   hold the dropped parent's surviving siblings — which pass the chain
   check structurally (their ancestry above `d−2` *is* the orphan's
   ancestry), need only a single membership false positive to test in,
   and then outrank the true anchor on depth. Scanning `d−1` is pure
   misattachment surface. The window floor never collides with the
   ceiling for a real orphan: a checkpoint-depth parent cannot drop, so
   `(d−1) mod cpd ≠ 0` and `c ≤ d−2`.

3. **Anchor selection.** Test each candidate's span ID against the
   covering bloom, deepest depth first. Candidates passing the ID test
   must also pass **ancestry-chain consistency** (`--consistency chain`,
   the default): walking up from the candidate, every ancestor ID we can
   name must also test positive in the bloom —
   - through surviving ancestors the walk continues (each survivor's
     record names its parent, *even when that parent was dropped*);
   - at a disconnection point the dropped parent's ID is tested and the
     walk stops (nothing further is nameable);
   - at the window floor the walk stops untested — that span is the
     checkpoint whose reset created the bloom, so its parent is
     legitimately absent.

   A true anchor always passes (its chain is the orphan's own ancestry;
   blooms have no false negatives); an FP candidate must win an
   independent ~FP coincidence per step. The deepest surviving candidate
   wins; same-depth ties are broken by smallest span ID and flagged
   `Ambiguous`.

4. **Synthetic chain.** Hole size is exact, not inferred:
   `gap = d − anchor.depth − 1` synthetic spans chain the anchor to the
   orphan (this is what per-span absolute depth pays for). P-Bridge does
   **not** merge synthetic parents across sibling orphans (paper §3.4):
   a *dropped* fan-out span is rebuilt once per surviving child fragment.
   The resulting synthetic-duplication factor (`num_synthetic /
   num_dropped`) is the measurable fidelity price of P-Bridge vs.
   CG-Bridge. A *surviving* fan-out needs no handling at all — its edges
   are real.

## Empirical results (3000 Uber traces, drop-rate 0.5, seed 42)

Misattachment — a reconnected orphan attached under the wrong anchor —
is the dominant error: reconnection itself is 100% and `gap correct`
tracks `anchor correct` exactly in every measured config. The errors are
**lone deep bloom FPs** ("deepest positive wins" hands the anchor to a
false positive below the true anchor); measured same-depth ambiguity is
zero.

Misattachment rate by bloom FP target (`--bloom-fp`), with and without
the chain check, independent order:

| cpd | fp | bloom | none | chain |
|----:|------:|------:|--------:|--------:|
| 3 | 1e-2 | 4 B | 60.9% | 32.8% |
| 3 | 1e-4 | 8 B | 17.3% | 5.7% |
| 3 | 1e-6 | 11 B | 0.98% | 0.24% |
| 6 | 1e-2 | 8 B | 44.7% | 14.9% |
| 6 | 1e-4 | 15 B | 6.2% | 1.0% |
| 6 | 1e-6 | 22 B | 0.09% | 0.013% |

- **The FPR target is the dominant lever**: 1e-4 → 1e-6 costs +3 B
  (cpd 3) / +7 B (cpd 6) per checkpoint payload and buys 18–70×.
  Effective FP diverges from the configured rate unevenly at small m
  (1e-5 barely improves on 1e-4; 1e-6 cliffs), so tune by measurement,
  not by formula.
- **The chain check is free accuracy** (2–7×, no wire bytes) but is
  structurally limited: it cannot reject candidates that *share*
  ancestry with the true anchor — siblings are fully immune, and at
  shallow windows every depth-1 candidate's parent is the (single) root,
  making the test vacuous there. The residual error is therefore
  "near-relative" misattachment — topologically close to correct.
- **Order**: independent vs bottom-up is a dead heat without the chain
  check (the deep-FP noise floor swamps the difference). With the chain
  check on, bottom-up wins where borrows are common (cpd 6 @ 1e-4:
  0.82% vs 1.02%) because structural discovery picks true descendants
  where the scan can pick FP claimants. Hence the default.

**Recommended config: `--order bottom-up --consistency chain`** (the
defaults), with the bloom FPR target as the per-deployment
overhead/accuracy dial. Best measured: 0.24% (cpd 3, 11 B) / 0.013%
(cpd 6, 22 B) misattachment at drop-rate 0.5.

## Scoring (vs. pre-drop truth)

Per orphan with a reconstructed bridge: **anchor correct** (== true
nearest surviving ancestor, computed by walking the true parent chain
past dropped spans); **gap correct** (synthetic count == true dropped
spans in between; only evaluated when the anchor is correct);
**misattached** (anchor wrong).

Per trace (JSON arrays in load order): `num_spans, num_dropped,
num_orphans, num_reconnected, num_anchor_correct, num_gap_correct,
num_misattached, num_unanchored, num_synthetic, num_borrowed_bloom,
num_ambiguous, num_ambiguous_misattached`. The header records
`checkpoint_distance, drop_rate, bloom_fp, order, consistency, seed`.

## Drop model (`cmd/trace_recon`)

Only non-checkpoint spans (`_d` carriers) are drop candidates — `_br`
carriers ride the high-priority collector queue and are never dropped.
`--drop-rate 1.0` (default) drops all of them; `--drop-rate p --seed s`
drops each independently with probability p. The harness is streaming
and per-trace: collect → drop → reconstruct → score as each trace
closes. Inputs: a JSON trace dir, or `--corpus` (with `--trace-count N`
processing the first N corpus traces; sound for PB because its handler
state is strictly per-trace — revisit for S-Bridge, whose DEE crosses
traces).

## Open hypothesis: the cpd-4 anomaly (checkpoint-grid phase alignment)

Full-corpus sweeps at fp 1e-6 show accuracy is **non-monotonic in cpd**
(span misattach at drop 0.05: cpd 2 = 14.7%, cpd 3 = 0.75%, cpd 4 =
1.95%, cpd 5 = 0.86%, cpd 6 = 0.075%) even though every cpd gets the
same ~29 bits/element by construction. Within each cpd, the rate is flat
across drop rates — per-bridge error is a function of filter geometry ×
trace structure only. Three forces plausibly superpose:

1. **Window growth hurts**: more candidate levels below the true anchor
   = more FP trials per orphan.
2. **Chain length helps**: a candidate's chain-check strength is the
   number of ancestors it can name before the window floor — up to
   `(d−1) mod cpd` for the deepest (most dangerous) candidate level.
   Subset ablations measured the chain check worth only 2–3× at cpd 3
   but 6–7× at cpd 6, consistent with this.
3. **Delivered FP decays with absolute m**: small filters deliver well
   above their configured rate (measured ~1e-3 at m=58); by m=173 the
   filter may be near-honest.

**Hypothesis (untested)**: orphan depths have *phases* relative to the
checkpoint grid. At phases where the dangerous candidate level sits on
the window floor, chains are zero-step and the check is vacuous — and
those floor candidates are uncles sharing the orphan's own floor
ancestor, which chain consistency can *never* reject. The Uber depth
distribution (p50 max-depth 20, width concentrated near the leaves) may
dump its width-weighted orphan mass onto cpd 4's weak phases while
slicing luckier offsets for cpd 3 and 5 — i.e. the dataset's depths are
adversarial to cpd 4's grid specifically.

**Future test** (instrumentation sketch): aggregate `(orphans,
misattached)` by window phase `(d−1) mod cpd` and by absolute depth
bucket in `ScorePB` (env-gated histogram), run cpd 3/4/5 at one drop
rate. Predictions if true: cpd 4's misattachments concentrate in one or
two phases; those phases coincide with the dataset's widest depth bands;
cpd 3/5 have their own weak phases carrying less orphan mass. Companion
probes that complete the decomposition: per-geometry effective-FP
measurement (force 3, no trace structure involved) and a full-corpus
cpd 6 run with `--consistency none` (force 2 in isolation).

## Future work: bursty drop model

The current drop model is uniform Bernoulli per non-checkpoint span.
Verified consequence: large traces lose almost exactly the configured
fraction of their droppable spans (median 0.2498 at rate 0.25, p1–p99 =
0.21–0.29 over the 63k traces with >1000 spans), scattered with no
spatial structure. This is the *kindest* spatial distribution for
P-Bridge: holes stay small and survivors stay dense around them.

Real loss is bursty: overloaded agents drop runs of consecutive spans
from the same service instance, and the paper's Alibaba analysis shows
holes cluster (traces with holes contain many; repeated holes share
overloaded resources). A v2 drop policy should model correlated drops —
per-service and/or per-time-window bursts, parameterizable from the
arrival-loss analysis (`docs/arrival_loss.md`, per-service dangling
rates and the span-emission-rate U-shape). Expected effects at equal
average loss: larger contiguous holes, longer synthetic chains, more
C-type orphans (borrowing), and a harder test of the membership
fallback — i.e. the same average rate should yield strictly worse
reconstruction, and the gap measures how much the Bernoulli results
flatter us.

## Limitations and open issues

- **Checkpoint loss is out of scope (v1).** The window bounds, the
  carrier-coverage guarantee, and unanchored-impossibility all assume
  checkpoints survive. The checkpoint-loss extension (root inference)
  should take the triage shape: anchor evidenced fragments first, then
  place evidence-less fragments by elimination or leave them
  floating-and-flagged.
- **The residual error floor is near-relative confusion** (siblings /
  descendants of the true anchor), which no amount of ancestor-bloom
  evidence can resolve — that is CG-Bridge's territory (its hash-array
  entries name fan-out spans exactly). A misattachment-distance metric
  (tree distance from chosen to true anchor) would quantify how close
  the residual errors land.
- **Timestamps are deliberately unused**: production spans are not
  CRISP-normalized (the corpus is), so time-containment filtering would
  overstate real-world accuracy.
- Synthetic spans are counted, not materialized — no Jaeger-loadable
  reconstructed output yet.
- The runtime plugin (blueprint-docc-mod) presumably still carries both
  bloom bugs fixed in this repo (murmur block-loop cross-mixing; plain
  double-hashing probe schedule) and needs the same patches for deployed
  behavior to match what this harness models.
