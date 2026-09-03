package main

import (
	"testing"

	"bridges/recon"
)

func TestFanoutEvidenceCountsLeafCarriersAsWindows(t *testing.T) {
	truth := []recon.TruthSpan{
		{SpanID: 1, ParentID: 0, Depth: 0},
		{SpanID: 2, ParentID: 1, Depth: 1},
		{SpanID: 3, ParentID: 1, Depth: 1},
	}
	survivors := []recon.Span{
		{SpanID: 1, Depth: 0, BloomBits: []byte{0}},
		{SpanID: 2, ParentID: 1, Depth: 1, BloomBits: []byte{0}, LeafCarrier: true},
		{SpanID: 3, ParentID: 1, Depth: 1, BloomBits: []byte{0}, LeafCarrier: true,
			HA: []recon.HAEntry{{ParentID: 1, Depth: 1}}},
	}
	result := recon.Result{ReconParent: map[uint64]uint64{2: 1, 3: 1}}
	var got fanoutEvidenceAcc
	got.add(recon.GreedyFanoutStats{HAEnabled: true}, result, survivors, truth, 3, true)

	if got.windows != 2 || got.correctWindows != 2 {
		t.Fatalf("leaf carrier windows = %d correct = %d, want 2 and 2", got.windows, got.correctWindows)
	}
	if got.haEntriesOnWindowPaths != 1 || got.haEntriesOffWindowPaths != 0 {
		t.Fatalf("HA on/off path = %d/%d, want 1/0", got.haEntriesOnWindowPaths, got.haEntriesOffWindowPaths)
	}
	if got.truthFanoutOccurrences != 2 || got.knownFanoutOccurrences != 2 {
		t.Fatalf("truth/known fanout occurrences = %d/%d, want 2/2", got.truthFanoutOccurrences, got.knownFanoutOccurrences)
	}
}

func TestFanoutEvidenceIncludesWindowBoundaryFanout(t *testing.T) {
	truth := []recon.TruthSpan{
		{SpanID: 1, ParentID: 0, Depth: 0},
		{SpanID: 2, ParentID: 1, Depth: 1},
		{SpanID: 3, ParentID: 1, Depth: 1},
		{SpanID: 4, ParentID: 3, Depth: 2},
	}
	survivors := []recon.Span{
		{SpanID: 1, Depth: 0, BloomBits: []byte{0}},
		{SpanID: 2, ParentID: 1, Depth: 1, BloomBits: []byte{0}, LeafCarrier: true},
		{SpanID: 4, ParentID: 3, Depth: 2, BloomBits: []byte{0},
			HA: []recon.HAEntry{{ParentID: 1, Depth: 1}}},
	}
	result := recon.Result{ReconParent: map[uint64]uint64{2: 1, 4: 3, 3: 1}}
	var got fanoutEvidenceAcc
	got.add(recon.GreedyFanoutStats{HAEnabled: true}, result, survivors, truth, 2, true)

	if got.haEntriesOnWindowPaths != 1 || got.haEntriesOffWindowPaths != 0 {
		t.Fatalf("boundary HA on/off path = %d/%d, want 1/0", got.haEntriesOnWindowPaths, got.haEntriesOffWindowPaths)
	}
}

func TestFanoutEvidenceSeparatesAnchorFromRouteTopology(t *testing.T) {
	truth := []recon.TruthSpan{
		{SpanID: 1, ParentID: 0, Depth: 0},
		{SpanID: 2, ParentID: 1, Depth: 1},
		{SpanID: 3, ParentID: 2, Depth: 2},
	}
	survivors := []recon.Span{
		{SpanID: 1, Depth: 0, BloomBits: []byte{0}},
		{SpanID: 3, ParentID: 2, Depth: 2, BloomBits: []byte{0}, LeafCarrier: true},
	}
	route := recon.GreedyRouteEvidence{
		Routed: true, AnchorID: 1, OrphanIDs: []uint64{3},
		ApplicableFanoutGroups: 1,
	}
	result := recon.Result{
		ReconParent: map[uint64]uint64{3: 2, 2: 99, 99: 1},
		ReconAnon:   map[uint64]bool{99: true},
		GreedyChain: recon.GreedyChainStats{Routes: []recon.GreedyRouteEvidence{route}},
	}
	var got fanoutEvidenceAcc
	got.add(recon.GreedyFanoutStats{HAEnabled: true}, result, survivors, truth, 3, false)

	if got.routedUnits != 1 || got.correctAnchorRoutes != 1 {
		t.Fatalf("routed/anchor-correct = %d/%d, want 1/1", got.routedUnits, got.correctAnchorRoutes)
	}
	if got.correctTopologyRoutes != 0 {
		t.Fatalf("topology-correct routes = %d, want 0", got.correctTopologyRoutes)
	}
}
