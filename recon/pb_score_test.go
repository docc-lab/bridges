package recon

import "testing"

func pbStrictFixture() ([]Span, []TruthSpan, map[uint64]struct{}) {
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
	dropped := map[uint64]struct{}{2: {}, 3: {}}
	return survivors, truth, dropped
}

func TestScorePBPathStrictAcceptsExactAnchorAndGap(t *testing.T) {
	survivors, truth, dropped := pbStrictFixture()
	res := Result{Bridges: []Bridge{{OrphanID: 4, AnchorID: 1, Synthetic: 2}}}
	got := ScorePBPathStrict(res, survivors, truth, dropped)
	if !got.Clean() || got.RealNodes != 1 || got.EdgeExact != 1 {
		t.Fatalf("unexpected strict path score: %+v", got)
	}
}

func TestScorePBPathStrictRejectsWrongGapWithCorrectAnchor(t *testing.T) {
	survivors, truth, dropped := pbStrictFixture()
	res := Result{Bridges: []Bridge{{OrphanID: 4, AnchorID: 1, Synthetic: 1}}}
	got := ScorePBPathStrict(res, survivors, truth, dropped)
	if got.EdgeWrong != 1 || got.Clean() {
		t.Fatalf("wrong synthetic count was accepted: %+v", got)
	}
}

func TestScorePBPathStrictCountsMissingBridge(t *testing.T) {
	survivors, truth, dropped := pbStrictFixture()
	got := ScorePBPathStrict(Result{}, survivors, truth, dropped)
	if got.RealNodes != 1 || got.EdgeWrong != 1 || got.Clean() {
		t.Fatalf("missing observable fragment bridge was accepted: %+v", got)
	}
}

func TestScorePBPathStrictRejectsUnexpectedBridge(t *testing.T) {
	survivors, truth, dropped := pbStrictFixture()
	res := Result{Bridges: []Bridge{
		{OrphanID: 4, AnchorID: 1, Synthetic: 2},
		{OrphanID: 1, AnchorID: 9, Synthetic: 0},
	}}
	got := ScorePBPathStrict(res, survivors, truth, dropped)
	if got.ConstraintWrong != 1 || got.Clean() {
		t.Fatalf("unexpected bridge was accepted: %+v", got)
	}
}
