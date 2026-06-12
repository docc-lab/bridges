package recon

import "testing"

// Diagnostic: run the hand-verified PCRB topologies through the v8 solver.
func TestPCRSThreadsThroughSurvivingIntermediate(t *testing.T) {
	cfg := pcrbConfig(4, 1e-4)
	survivors := []Span{
		{SpanID: 1, ParentID: 0, Depth: 0, CkptPrefix: prefixOf(0, 4), BloomBits: windowBloom(t, cfg)},
		{SpanID: 2, ParentID: 1, Depth: 1},
		{SpanID: 9, ParentID: 3, Depth: 3, CkptPrefix: prefixOf(1, 4), BloomBits: windowBloom(t, cfg, 2, 3)},
	}
	res := ReconstructPCRS(survivors, cfg)
	if res.Reconnected != 1 {
		t.Fatalf("res=%+v", res)
	}
	b := res.Bridges[0]
	t.Logf("bridge: orphan=%d anchor=%d syn=%d", b.OrphanID, b.AnchorID, b.Synthetic)
	if b.AnchorID != 2 || b.Synthetic != 1 {
		t.Fatalf("bridge=%+v want anchor=2 synthetic=1", b)
	}
}

func TestPCRSOrphanPlacement(t *testing.T) {
	cfg := pcrbConfig(6, 1e-4)
	survivors := []Span{
		{SpanID: 1, ParentID: 0, Depth: 0, CkptPrefix: prefixOf(0, 4), BloomBits: windowBloom(t, cfg)},
		{SpanID: 3, ParentID: 2, Depth: 2},
		{SpanID: 9, ParentID: 4, Depth: 4, CkptPrefix: prefixOf(1, 4), BloomBits: windowBloom(t, cfg, 2, 3, 4), LeafCarrier: true},
	}
	res := ReconstructPCRS(survivors, cfg)
	t.Logf("placed=%d unanchored=%v bridges=%+v", res.OrphansPlaced, res.Unanchored, res.Bridges)
	if res.OrphansPlaced != 1 || len(res.Unanchored) != 0 {
		t.Fatalf("placed=%d unanchored=%v", res.OrphansPlaced, res.Unanchored)
	}
}
