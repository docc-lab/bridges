package recon

import "testing"

func TestScoreCGP2EvidenceRequiresLiteralSurvivorParent(t *testing.T) {
	truth := []TruthSpan{
		{SpanID: 1, ParentID: 0, Depth: 0},
		{SpanID: 2, ParentID: 1, Depth: 1},
		{SpanID: 3, ParentID: 2, Depth: 2},
	}
	survivors := []Span{
		{SpanID: 1, ParentID: 0, Depth: 0},
		{SpanID: 3, ParentID: 2, Depth: 2},
	}
	res := Result{
		ReconParent: map[uint64]uint64{3: 9, 9: 1},
		ReconAnon:   map[uint64]bool{9: true},
	}

	got := ScoreCGP2Evidence(res, survivors, truth)
	if got.EdgeWrong != 2 {
		t.Fatalf("EdgeWrong=%d, want 2 (literal parent hidden and named parent missing): %+v", got.EdgeWrong, got)
	}
}

func TestScoreCGP2EvidenceForgivesOnlyGenuinelyUnnameableSlots(t *testing.T) {
	// Span 2 is absent from every surviving record.  Span 3 is nameable from
	// survivor 4's ParentID, so only the single slot for span 2 may be anonymous.
	truth := []TruthSpan{
		{SpanID: 1, ParentID: 0, Depth: 0},
		{SpanID: 2, ParentID: 1, Depth: 1},
		{SpanID: 3, ParentID: 2, Depth: 2},
		{SpanID: 4, ParentID: 3, Depth: 3},
	}
	survivors := []Span{
		{SpanID: 1, ParentID: 0, Depth: 0},
		{SpanID: 4, ParentID: 3, Depth: 3},
	}
	res := Result{
		ReconParent: map[uint64]uint64{4: 3, 3: 9, 9: 1},
		ReconAnon:   map[uint64]bool{9: true},
	}

	got := ScoreCGP2Evidence(res, survivors, truth)
	if got.EdgeWrong != 0 || got.EdgeExact != 1 || got.EdgeAnonOK != 1 {
		t.Fatalf("unexpected evidence score: %+v", got)
	}
}

func TestScoreCGP2EvidenceRejectsWrongSurvivorBehindAnonymousSlot(t *testing.T) {
	// The old per-edge scorer stopped at anonymous node 9 and therefore never
	// noticed that its upstream edge terminates at unrelated survivor 5.
	truth := []TruthSpan{
		{SpanID: 1, ParentID: 0, Depth: 0},
		{SpanID: 2, ParentID: 1, Depth: 1},
		{SpanID: 3, ParentID: 2, Depth: 2},
		{SpanID: 4, ParentID: 3, Depth: 3},
		{SpanID: 5, ParentID: 1, Depth: 1},
	}
	survivors := []Span{
		{SpanID: 1, ParentID: 0, Depth: 0},
		{SpanID: 4, ParentID: 3, Depth: 3},
		{SpanID: 5, ParentID: 1, Depth: 1},
	}
	res := Result{
		ReconParent: map[uint64]uint64{4: 3, 3: 9, 9: 5, 5: 1},
		ReconAnon:   map[uint64]bool{9: true},
	}

	legacy := ScoreCGP2Iso(res, truth, map[uint64]struct{}{2: {}, 3: {}})
	if legacy.EdgeWrong != 0 {
		t.Fatalf("test no longer demonstrates the legacy blind spot: %+v", legacy)
	}
	got := ScoreCGP2Evidence(res, survivors, truth)
	if got.EdgeWrong != 1 {
		t.Fatalf("EdgeWrong=%d, want 1 for wrong terminal survivor: %+v", got.EdgeWrong, got)
	}
}

func TestScoreCGP2EvidenceTreatsHAIdentityAsNameable(t *testing.T) {
	truth := []TruthSpan{
		{SpanID: 1, ParentID: 0, Depth: 0},
		{SpanID: 2, ParentID: 1, Depth: 1},
		{SpanID: 3, ParentID: 2, Depth: 2},
		{SpanID: 4, ParentID: 3, Depth: 3},
	}
	survivors := []Span{
		{SpanID: 1, ParentID: 0, Depth: 0},
		{SpanID: 4, ParentID: 3, Depth: 3, HA: []HAEntry{{ParentID: 2, Depth: 2}}},
	}
	res := Result{
		ReconParent: map[uint64]uint64{4: 3, 3: 9, 9: 1},
		ReconAnon:   map[uint64]bool{9: true},
	}

	got := ScoreCGP2Evidence(res, survivors, truth)
	if got.EdgeWrong != 2 {
		t.Fatalf("EdgeWrong=%d, want 2 (HA node skipped and absent as source): %+v", got.EdgeWrong, got)
	}
}

func TestScoreCGP2EvidenceSharesParentIDNameabilityAcrossFragments(t *testing.T) {
	// Survivor 5 names span 2.  The scorer must therefore reject an anonymous
	// stand-in for span 2 on survivor 4's branch as well.
	truth := []TruthSpan{
		{SpanID: 1, ParentID: 0, Depth: 0},
		{SpanID: 2, ParentID: 1, Depth: 1},
		{SpanID: 3, ParentID: 2, Depth: 2},
		{SpanID: 4, ParentID: 3, Depth: 3},
		{SpanID: 5, ParentID: 2, Depth: 2},
	}
	survivors := []Span{
		{SpanID: 1, ParentID: 0, Depth: 0},
		{SpanID: 4, ParentID: 3, Depth: 3},
		{SpanID: 5, ParentID: 2, Depth: 2},
	}
	res := Result{
		ReconParent: map[uint64]uint64{4: 3, 3: 9, 9: 1, 5: 2, 2: 1},
		ReconAnon:   map[uint64]bool{9: true},
	}

	got := ScoreCGP2Evidence(res, survivors, truth)
	if got.EdgeWrong != 1 {
		t.Fatalf("EdgeWrong=%d, want 1 for globally nameable parent hidden by anonymous node: %+v", got.EdgeWrong, got)
	}
}

func TestScoreCGP2EvidenceCountsMissingNameableSource(t *testing.T) {
	truth := []TruthSpan{
		{SpanID: 1, ParentID: 0, Depth: 0},
		{SpanID: 2, ParentID: 1, Depth: 1},
		{SpanID: 3, ParentID: 2, Depth: 2},
	}
	survivors := []Span{
		{SpanID: 1, ParentID: 0, Depth: 0},
		{SpanID: 3, ParentID: 2, Depth: 2},
	}
	res := Result{
		ReconParent: map[uint64]uint64{3: 2},
		ReconAnon:   map[uint64]bool{},
	}

	got := ScoreCGP2Evidence(res, survivors, truth)
	if got.EdgeWrong != 1 || got.EdgeExact != 1 {
		t.Fatalf("unexpected score for missing named synthetic source: %+v", got)
	}
}
