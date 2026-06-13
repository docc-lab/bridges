package recon

import "testing"

// Adversarial scorer tests: each case hands ScorePCR a reconstruction
// containing a known error and asserts the error is COUNTED. The danger
// direction for a scorer is leniency (over-crediting reports
// better-than-actual results), so these tests check detection, not just
// acceptance of good output.
//
// Topology used throughout (depths in parens):
//
//	1(0) ── 2(1) ── 3(2) ── 4(3) ── 5(4)
//	          └──── 6(2)
//
// Span 3 is dropped in every case, making 4 a fragment root whose
// NEAREST SURVIVING ancestor is 2.
var trapTruth = []TruthSpan{
	{SpanID: 1, ParentID: 0, Depth: 0},
	{SpanID: 2, ParentID: 1, Depth: 1},
	{SpanID: 3, ParentID: 2, Depth: 2},
	{SpanID: 4, ParentID: 3, Depth: 3},
	{SpanID: 5, ParentID: 4, Depth: 4},
	{SpanID: 6, ParentID: 2, Depth: 2},
}

func trapDropped(ids ...uint64) map[uint64]struct{} {
	m := make(map[uint64]struct{})
	for _, id := range ids {
		m[id] = struct{}{}
	}
	return m
}

func scoreOne(t *testing.T, anchor uint64) Score {
	t.Helper()
	res := Result{
		Reconnected: 1,
		Bridges:     []Bridge{{OrphanID: 4, AnchorID: anchor}},
	}
	return ScorePCR(res, trapTruth, trapDropped(3))
}

// Exact control: anchoring 4 under 2 (its nearest surviving ancestor)
// must grade exact and nothing else.
func TestScoreTrapExactControl(t *testing.T) {
	sc := scoreOne(t, 2)
	if sc.AnchorCorrect != 1 || sc.AnchorAncestor != 1 || sc.Misattached != 0 {
		t.Fatalf("exact control miscounted: C=%d A=%d W=%d", sc.AnchorCorrect, sc.AnchorAncestor, sc.Misattached)
	}
}

// Descendant anchor: 5 is 4's own child. A lenient ancestry test
// (e.g. connectivity instead of direction) would over-credit this.
func TestScoreTrapDescendantAnchor(t *testing.T) {
	sc := scoreOne(t, 5)
	if sc.Misattached != 1 || sc.AnchorCorrect != 0 || sc.AnchorAncestor != 0 {
		t.Fatalf("descendant anchor not penalized: C=%d A=%d W=%d", sc.AnchorCorrect, sc.AnchorAncestor, sc.Misattached)
	}
}

// Same-depth sibling: 6 sits at the dropped parent's depth but is not an
// ancestor of 4 — the classic near-miss a depth-based check would pass.
func TestScoreTrapSameDepthSibling(t *testing.T) {
	sc := scoreOne(t, 6)
	if sc.Misattached != 1 || sc.AnchorCorrect != 0 || sc.AnchorAncestor != 0 {
		t.Fatalf("same-depth sibling not penalized: C=%d A=%d W=%d", sc.AnchorCorrect, sc.AnchorAncestor, sc.Misattached)
	}
}

// Shallow true ancestor: 1 is a genuine ancestor of 4 but ABOVE the
// nearest surviving one (2). Must count as misattached (the project
// ruling: any non-exact attachment is wrong), with the ancestor flag set
// (it is the sub-class diagnostic) and exact NOT credited.
func TestScoreTrapShallowAncestor(t *testing.T) {
	sc := scoreOne(t, 1)
	if sc.AnchorCorrect != 0 {
		t.Fatalf("shallow anchor credited as exact")
	}
	if sc.AnchorAncestor != 1 {
		t.Fatalf("shallow anchor not recognized as true ancestor (sub-class lost)")
	}
	if sc.Misattached != 1 {
		t.Fatalf("shallow anchor not counted as misattached")
	}
}

// Reduced-set: spans of a DISCARDED fragment are absent for nearest-
// ancestor purposes — and must be billed to SpansLost, never laundered.
// Drop 3 and 7; fragment {4,5} is unanchored (discarded); fragment {8}
// is anchored at 2. With 4 and 5 lost, 8's nearest SURVIVING ancestor in
// the reduced set is 2, so the bridge is exact — but only because the
// loss is simultaneously counted (2 spans).
func TestScoreTrapReducedSetLostFragment(t *testing.T) {
	truth := []TruthSpan{
		{SpanID: 1, ParentID: 0, Depth: 0},
		{SpanID: 2, ParentID: 1, Depth: 1},
		{SpanID: 3, ParentID: 2, Depth: 2},
		{SpanID: 4, ParentID: 3, Depth: 3},
		{SpanID: 5, ParentID: 4, Depth: 4},
		{SpanID: 7, ParentID: 5, Depth: 5},
		{SpanID: 8, ParentID: 7, Depth: 6},
	}
	res := Result{
		Reconnected: 1,
		Bridges:     []Bridge{{OrphanID: 8, AnchorID: 2}},
		Unanchored:  []uint64{4},
	}
	sc := ScorePCR(res, truth, trapDropped(3, 7))
	if sc.SpansLost != 2 || sc.FragmentsLost != 1 {
		t.Fatalf("loss not billed: spansLost=%d fragmentsLost=%d (want 2, 1)", sc.SpansLost, sc.FragmentsLost)
	}
	if sc.AnchorCorrect != 1 || sc.Misattached != 0 {
		t.Fatalf("reduced-set exact misgraded: C=%d W=%d", sc.AnchorCorrect, sc.Misattached)
	}
	// The flip side: WITHOUT the discard, anchoring 8 at 2 must be
	// penalized (4 and 5 survive and are nearer).
	res2 := Result{Reconnected: 1, Bridges: []Bridge{{OrphanID: 8, AnchorID: 2}}}
	sc2 := ScorePCR(res2, truth, trapDropped(3, 7))
	if sc2.Misattached != 1 || sc2.AnchorCorrect != 0 {
		t.Fatalf("shallow-past-surviving not penalized: C=%d W=%d", sc2.AnchorCorrect, sc2.Misattached)
	}
}
