package recon

import (
	"testing"

	"bridges/bridge"
)

func prefixOf(id uint64, k int) []byte {
	b := bridge.BigEndian8(id)
	return b[:k]
}

// Topology (cpd=3, prefix 4):
//
//	1 (ckpt, d0) - 2 (d1) - 3 (d2) - 4 (ckpt, d3) - 5 (d4) - 6 (d5) - 7 (ckpt-leaf, d6)
//
// Drop 5 and 6. Orphan: 7 (checkpoint, carries own _br naming 4).
func TestPCRReconstructCheckpointOrphan(t *testing.T) {
	cfg := Config{CPD: 3, PrefixLen: 4}
	survivors := []Span{
		{SpanID: 1, ParentID: 0, Depth: 0, CkptPrefix: prefixOf(0, 4)},
		{SpanID: 2, ParentID: 1, Depth: 1},
		{SpanID: 3, ParentID: 2, Depth: 2},
		{SpanID: 4, ParentID: 3, Depth: 3, CkptPrefix: prefixOf(1, 4)},
		{SpanID: 7, ParentID: 6, Depth: 6, CkptPrefix: prefixOf(4, 4)},
	}
	res := ReconstructPCR(survivors, cfg)
	if res.Orphans != 1 || res.Reconnected != 1 {
		t.Fatalf("orphans=%d reconnected=%d", res.Orphans, res.Reconnected)
	}
	b := res.Bridges[0]
	if b.OrphanID != 7 || b.AnchorID != 4 || b.Synthetic != 2 || b.Ambiguous || b.ViaCarrier != 0 {
		t.Fatalf("bridge=%+v", b)
	}
}

// Same topology, but drop 5 only. Orphan: 6 (interior, _d only) — must walk
// its real fragment down to leaf 7, whose payload names checkpoint 4.
func TestPCRReconstructBorrowedCarrier(t *testing.T) {
	cfg := Config{CPD: 3, PrefixLen: 4}
	survivors := []Span{
		{SpanID: 1, ParentID: 0, Depth: 0, CkptPrefix: prefixOf(0, 4)},
		{SpanID: 2, ParentID: 1, Depth: 1},
		{SpanID: 3, ParentID: 2, Depth: 2},
		{SpanID: 4, ParentID: 3, Depth: 3, CkptPrefix: prefixOf(1, 4)},
		{SpanID: 6, ParentID: 5, Depth: 5},
		{SpanID: 7, ParentID: 6, Depth: 6, CkptPrefix: prefixOf(4, 4)},
	}
	res := ReconstructPCR(survivors, cfg)
	if res.Orphans != 1 || res.Reconnected != 1 {
		t.Fatalf("orphans=%d reconnected=%d", res.Orphans, res.Reconnected)
	}
	b := res.Bridges[0]
	if b.OrphanID != 6 || b.AnchorID != 4 || b.Synthetic != 1 || b.ViaCarrier != 7 {
		t.Fatalf("bridge=%+v", b)
	}
}

// Prefix collision: two checkpoints at the named depth share the truncated
// prefix. The reconstruction must flag Ambiguous and pick the smaller ID.
func TestPCRPrefixCollision(t *testing.T) {
	cfg := Config{CPD: 2, PrefixLen: 2}
	// Checkpoint IDs 0x0102030405060708 and 0x0102FFFFFFFFFFFF share 2
	// leading bytes, so the orphan's named depth-2 lookup matches both.
	a := uint64(0x0102030405060708)
	b := uint64(0x0102FFFFFFFFFFFF)
	survivors := []Span{
		{SpanID: 1, ParentID: 0, Depth: 0, CkptPrefix: prefixOf(0, 2)},
		{SpanID: 2, ParentID: 1, Depth: 1},
		{SpanID: a, ParentID: 2, Depth: 2, CkptPrefix: prefixOf(1, 2)},
		{SpanID: 3, ParentID: 1, Depth: 1},
		{SpanID: b, ParentID: 3, Depth: 2, CkptPrefix: prefixOf(1, 2)},
		{SpanID: 20, ParentID: 19, Depth: 3, CkptPrefix: prefixOf(a, 2)}, // orphan; true parent chain ran through a
	}
	res := ReconstructPCR(survivors, cfg)
	if res.Reconnected != 1 {
		t.Fatalf("reconnected=%d", res.Reconnected)
	}
	br := res.Bridges[0]
	if !br.Ambiguous {
		t.Fatalf("expected prefix-collision ambiguity, got %+v", br)
	}
	if br.AnchorID != a { // smaller of the colliding pair
		t.Fatalf("anchor=%x want %x", br.AnchorID, a)
	}
}

// AnchorAncestor scoring: PCR anchors at the checkpoint even when a nearer
// ancestor survived — counted as misattached under the nearest-survivor
// metric but as ancestor-correct under AnchorAncestor.
func TestScoreAnchorAncestor(t *testing.T) {
	truth := []TruthSpan{
		{SpanID: 1, ParentID: 0, Depth: 0},
		{SpanID: 2, ParentID: 1, Depth: 1}, // survives: true nearest ancestor of 4
		{SpanID: 3, ParentID: 2, Depth: 2}, // dropped
		{SpanID: 4, ParentID: 3, Depth: 3}, // orphan
	}
	dropped := map[uint64]struct{}{3: {}}
	res := Result{
		Orphans:     1,
		Reconnected: 1,
		Bridges:     []Bridge{{OrphanID: 4, AnchorID: 1, Synthetic: 2}}, // anchored at checkpoint 1
	}
	sc := ScorePB(res, truth, dropped)
	if sc.AnchorCorrect != 0 || sc.Misattached != 1 {
		t.Errorf("nearest-survivor metric: correct=%d mis=%d", sc.AnchorCorrect, sc.Misattached)
	}
	if sc.AnchorAncestor != 1 {
		t.Errorf("AnchorAncestor=%d want 1 (anchor 1 is a true ancestor of orphan 4)", sc.AnchorAncestor)
	}
}

// Fragment-loss accounting: a carrier-less fragment (interior orphan whose
// fragment contains no _br carrier) is unanchored, and ScorePCR charges the
// whole fragment as thrown away.
func TestScorePCRFragmentLoss(t *testing.T) {
	cfg := Config{CPD: 4, PrefixLen: 4}
	// Truth: 1(d0,ckpt) - 2(d1) - 3(d2) - 4(d3) - 5(d4,ckpt); plus 3 - 6(d3 leaf).
	// Drop 2 and 4. Survivors: 1, 3, 5, 6.
	//   - 5 is a checkpoint orphan, self-carries, anchors at 1.
	//   - 3 is an interior orphan; its fragment {3, 6} has no carrier in
	//     band... give 6 no payload (interior in truth) to force that.
	truth := []TruthSpan{
		{SpanID: 1, ParentID: 0, Depth: 0},
		{SpanID: 2, ParentID: 1, Depth: 1},
		{SpanID: 3, ParentID: 2, Depth: 2},
		{SpanID: 4, ParentID: 3, Depth: 3},
		{SpanID: 5, ParentID: 4, Depth: 4},
		{SpanID: 6, ParentID: 3, Depth: 3},
	}
	dropped := map[uint64]struct{}{2: {}, 4: {}}
	survivors := []Span{
		{SpanID: 1, ParentID: 0, Depth: 0, CkptPrefix: prefixOf(0, 4)},
		{SpanID: 3, ParentID: 2, Depth: 2},
		{SpanID: 5, ParentID: 4, Depth: 4, CkptPrefix: prefixOf(1, 4)},
		{SpanID: 6, ParentID: 3, Depth: 3},
	}
	res := ReconstructPCR(survivors, cfg)
	if res.Reconnected != 1 || len(res.Unanchored) != 1 || res.Unanchored[0] != 3 {
		t.Fatalf("res=%+v", res)
	}
	sc := ScorePCR(res, truth, dropped)
	if sc.FragmentsLost != 1 {
		t.Errorf("FragmentsLost=%d want 1", sc.FragmentsLost)
	}
	if sc.SpansLost != 2 { // orphan 3 and its surviving child 6
		t.Errorf("SpansLost=%d want 2", sc.SpansLost)
	}
}
