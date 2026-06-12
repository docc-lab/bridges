package recon

import (
	"testing"

	"bridges/bloom"
	"bridges/bridge"
)

func pcrbConfig(cpd int, fp float64) Config {
	return NewPCRBConfig(cpd, 4, fp)
}

// windowBloom builds the bits of a window bloom containing the given IDs.
func windowBloom(t *testing.T, cfg Config, ids ...uint64) []byte {
	t.Helper()
	bf, err := bloom.New(cfg.BloomM, cfg.BloomK)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		hex := bridge.HexOf(id)
		bf.Add(hex[:])
	}
	return bf.ToBytes()
}

// Truth (cpd=4): 1(ckpt,d0) - 2(d1) - 3(d2) - 4(d3) - 5(leaf,d4... non-ckpt? d4%4==0 -> ckpt).
// Use depths 0..3 plus orphan at d3 to keep one window: 1(d0) - 2(d1) - 3(d2) - o=9(d3, leaf).
// Drop 3 only. Survivors: 1, 2, 9. Orphan 9 carries prefix(1) + bloom{2,3,9}.
// Threading must walk through surviving 2 (d1), then past dropped 3 (d2),
// yielding anchor 2 with 1 synthetic — the true nearest surviving ancestor.
func TestPCRBThreadsThroughSurvivingIntermediate(t *testing.T) {
	cfg := pcrbConfig(4, 1e-4)
	bloomBits := windowBloom(t, cfg, 2, 3) // inherited: strictly-above window ancestors, no self
	survivors := []Span{
		{SpanID: 1, ParentID: 0, Depth: 0, CkptPrefix: prefixOf(0, 4), BloomBits: windowBloom(t, cfg)},
		{SpanID: 2, ParentID: 1, Depth: 1},
		{SpanID: 9, ParentID: 3, Depth: 3, CkptPrefix: prefixOf(1, 4), BloomBits: bloomBits},
	}
	res := ReconstructPCRB(survivors, cfg)
	if res.Reconnected != 1 {
		t.Fatalf("res=%+v", res)
	}
	b := res.Bridges[0]
	if b.AnchorID != 2 || b.Synthetic != 1 {
		t.Fatalf("bridge=%+v want anchor=2 synthetic=1", b)
	}
}

// Same topology but 2 is NOT in the orphan's bloom (it is a sibling branch,
// not an ancestor): truth 1 - 2(d1) and 1 - 3'(d1, dropped) - 3(d2, dropped) - 9(d3).
// The orphan's bloom holds {3', 3, 9}. Threading must NOT pick 2; anchor
// stays at checkpoint 1 with 2 synthetics.
func TestPCRBDoesNotThreadThroughNonAncestor(t *testing.T) {
	cfg := pcrbConfig(4, 1e-4)
	bloomBits := windowBloom(t, cfg, 7, 3) // 7 = dropped d1 ancestor; no self
	survivors := []Span{
		{SpanID: 1, ParentID: 0, Depth: 0, CkptPrefix: prefixOf(0, 4), BloomBits: windowBloom(t, cfg)},
		{SpanID: 2, ParentID: 1, Depth: 1},
		{SpanID: 9, ParentID: 3, Depth: 3, CkptPrefix: prefixOf(1, 4), BloomBits: bloomBits},
	}
	res := ReconstructPCRB(survivors, cfg)
	b := res.Bridges[0]
	if b.AnchorID != 1 || b.Synthetic != 2 {
		t.Fatalf("bridge=%+v want anchor=1 (flat) synthetic=2", b)
	}
}

// Threading across a dropped level into a co-window fragment:
// truth 1(d0,ckpt) - 2(d1, dropped) - 3(d2) - 4(d3, dropped) - 9(d4=ckpt orphan).
// Survivors: 1, 3 (fragment root, interior: no payload, borrows from 9? no -
// 3's fragment is {3}, no carrier; give 3 a leaf child 6 at d3 carrying
// payload so 3 anchors at 1), 6, 9.
// Orphan 9's bloom (inherited, window above): {2, 3, 4}. Level d1: no
// positive among candidates (2 dropped; frag root 3 is at d2). Level d2:
// fragment root 3 positive -> pos=3. Level d3: 6 (3's real child) is NOT in
// the bloom -> no positive. Final anchor 3, synthetic 1 (the d3 slot).
func TestPCRBThreadsIntoCoWindowFragment(t *testing.T) {
	cfg := pcrbConfig(4, 1e-4)
	survivors := []Span{
		{SpanID: 1, ParentID: 0, Depth: 0, CkptPrefix: prefixOf(0, 4), BloomBits: windowBloom(t, cfg)},
		{SpanID: 3, ParentID: 2, Depth: 2},
		{SpanID: 6, ParentID: 3, Depth: 3, CkptPrefix: prefixOf(1, 4), BloomBits: windowBloom(t, cfg, 2, 3)},
		{SpanID: 9, ParentID: 4, Depth: 4, CkptPrefix: prefixOf(1, 4), BloomBits: windowBloom(t, cfg, 2, 3, 4)},
	}
	res := ReconstructPCRB(survivors, cfg)
	if res.Reconnected != 2 {
		t.Fatalf("res=%+v", res)
	}
	var b9 *Bridge
	for i := range res.Bridges {
		if res.Bridges[i].OrphanID == 9 {
			b9 = &res.Bridges[i]
		}
	}
	if b9 == nil || b9.AnchorID != 3 || b9.Synthetic != 1 {
		t.Fatalf("bridge for 9 = %+v want anchor=3 synthetic=1", b9)
	}
}

// Leaf-carrier exclusion: a leaf's ID in the fragment's bloom (FP or
// otherwise) must never be chosen as an attachment point — leaves have no
// descendants. Topology (cpd=4): 1(ckpt d0) - 2(d1,dropped) - 3(d2,dropped) - 9(d3 orphan-root);
// plus 1 - 8(d1, leaf carrier). Bloom of 9 deliberately contains 8.
func TestPCRBLeafCarrierExcluded(t *testing.T) {
	cfg := pcrbConfig(4, 1e-4)
	survivors := []Span{
		{SpanID: 1, ParentID: 0, Depth: 0, CkptPrefix: prefixOf(0, 4), BloomBits: windowBloom(t, cfg)},
		{SpanID: 8, ParentID: 1, Depth: 1, CkptPrefix: prefixOf(1, 4), BloomBits: windowBloom(t, cfg), LeafCarrier: true},
		{SpanID: 9, ParentID: 3, Depth: 3, CkptPrefix: prefixOf(1, 4), BloomBits: windowBloom(t, cfg, 8, 2, 3)},
	}
	res := ReconstructPCRB(survivors, cfg)
	b := res.Bridges[0]
	if b.OrphanID != 9 || b.AnchorID != 1 || b.Synthetic != 2 {
		t.Fatalf("bridge=%+v want flat anchor=1 synthetic=2 (leaf 8 must not be threaded)", b)
	}
}

// Must-receive fixpoint: forward threading stops on ambiguity, but a
// uniquely-claimed open end pulls the fragment through.
// Topology (cpd=4): W=1(d0,ckpt); 2(d1, interior, ALL children dropped ->
// open end); 3(d1, interior, surviving leaf-carrier child 4); fragment root
// 9(d3) whose bloom contains {2, 3} (3 = simulated FP) -> level d1 is
// ambiguous, forward walk stops flat. Open end 2 has exactly one claimant
// (9) -> committed at 2.
func TestPCRBMustReceiveResolvesAmbiguity(t *testing.T) {
	cfg := pcrbConfig(4, 1e-4)
	survivors := []Span{
		{SpanID: 1, ParentID: 0, Depth: 0, CkptPrefix: prefixOf(0, 4), BloomBits: windowBloom(t, cfg)},
		{SpanID: 2, ParentID: 1, Depth: 1}, // open end
		{SpanID: 3, ParentID: 1, Depth: 1},
		{SpanID: 4, ParentID: 3, Depth: 2, CkptPrefix: prefixOf(1, 4), BloomBits: windowBloom(t, cfg, 3), LeafCarrier: true},
		{SpanID: 9, ParentID: 5, Depth: 3, CkptPrefix: prefixOf(1, 4), BloomBits: windowBloom(t, cfg, 2, 3)},
	}
	res := ReconstructPCRB(survivors, cfg)
	var b9 *Bridge
	for i := range res.Bridges {
		if res.Bridges[i].OrphanID == 9 {
			b9 = &res.Bridges[i]
		}
	}
	if b9 == nil || b9.AnchorID != 2 || b9.Synthetic != 1 {
		t.Fatalf("bridge for 9 = %+v want anchor=2 (open-end pull) synthetic=1", b9)
	}
	if res.OpenEnds != 1 || res.OpenEndsMatched != 1 {
		t.Fatalf("open ends %d/%d want 1/1", res.OpenEndsMatched, res.OpenEnds)
	}
}

// Orphan placement (v7): spacing eliminators and the happy path.
// Truth (cpd=6): W=1(d0,ckpt) - 2(d1,dropped) - 3(d2 ORPHAN root) - 4(d3,
// dropped) - 9(d4 carrier-fragment root, leaf) ... f=9's bloom {2,3,4}.
// Orphan {3} fits f's chain at d2: slot above (d1) empty, parent 2 and self
// 3 test positive, bottom d2 <= 9.depth-2. f re-anchors below 3.
func TestPCRBOrphanPlacement(t *testing.T) {
	cfg := pcrbConfig(6, 1e-4)
	survivors := []Span{
		{SpanID: 1, ParentID: 0, Depth: 0, CkptPrefix: prefixOf(0, 4), BloomBits: windowBloom(t, cfg)},
		{SpanID: 3, ParentID: 2, Depth: 2}, // orphan: no carrier in its fragment
		{SpanID: 9, ParentID: 4, Depth: 4, CkptPrefix: prefixOf(1, 4), BloomBits: windowBloom(t, cfg, 2, 3, 4), LeafCarrier: true},
	}
	res := ReconstructPCRB(survivors, cfg)
	if res.OrphansPlaced != 1 || len(res.Unanchored) != 0 {
		t.Fatalf("placed=%d unanchored=%v", res.OrphansPlaced, res.Unanchored)
	}
	var b3, b9 *Bridge
	for i := range res.Bridges {
		switch res.Bridges[i].OrphanID {
		case 3:
			b3 = &res.Bridges[i]
		case 9:
			b9 = &res.Bridges[i]
		}
	}
	if b3 == nil || b3.AnchorID != 1 || b3.Synthetic != 1 {
		t.Fatalf("orphan bridge=%+v want anchor=1 synthetic=1", b3)
	}
	if b9 == nil || b9.AnchorID != 3 {
		t.Fatalf("fragment bridge=%+v want re-anchored at 3", b9)
	}
}

// Spacing: an orphan root directly below the chain anchor (depth top+1) is
// impossible — its dropped parent would have to BE the surviving anchor.
func TestPCRBOrphanSpacingAboveRoot(t *testing.T) {
	cfg := pcrbConfig(6, 1e-4)
	survivors := []Span{
		{SpanID: 1, ParentID: 0, Depth: 0, CkptPrefix: prefixOf(0, 4), BloomBits: windowBloom(t, cfg)},
		{SpanID: 3, ParentID: 2, Depth: 1}, // orphan root at d1 = anchor+1: must NOT place
		{SpanID: 9, ParentID: 4, Depth: 4, CkptPrefix: prefixOf(1, 4), BloomBits: windowBloom(t, cfg, 2, 3, 4), LeafCarrier: true},
	}
	res := ReconstructPCRB(survivors, cfg)
	if res.OrphansPlaced != 0 || len(res.Unanchored) != 1 {
		t.Fatalf("placed=%d unanchored=%v — d1 orphan must be rejected (parent slot = anchor)", res.OrphansPlaced, res.Unanchored)
	}
}

// Spacing: an orphan span can never occupy root.depth-1 of the chain's
// fragment (that slot is the fragment's dropped parent).
func TestPCRBOrphanSpacingAboveFragment(t *testing.T) {
	cfg := pcrbConfig(6, 1e-4)
	survivors := []Span{
		{SpanID: 1, ParentID: 0, Depth: 0, CkptPrefix: prefixOf(0, 4), BloomBits: windowBloom(t, cfg)},
		{SpanID: 3, ParentID: 2, Depth: 3}, // orphan root at d3 = 9.depth-1: must NOT place
		{SpanID: 9, ParentID: 4, Depth: 4, CkptPrefix: prefixOf(1, 4), BloomBits: windowBloom(t, cfg, 2, 3, 4), LeafCarrier: true},
	}
	res := ReconstructPCRB(survivors, cfg)
	if res.OrphansPlaced != 0 || len(res.Unanchored) != 1 {
		t.Fatalf("placed=%d unanchored=%v — d3 orphan must be rejected (fragment's parent slot)", res.OrphansPlaced, res.Unanchored)
	}
}
