package recon

import (
	"testing"

	"bridges/bridge"
)

// buildTree runs the real PB handler (EmitDepth + Capture) over a complete
// binary tree of the given depth: span IDs 1..2^(d+1)-1, span i's parent is
// i/2, depth(i) = floor(log2(i)). Starts fire in ID order (parents first),
// ends in reverse. Returns per-span truth and captured payloads.
func buildTree(t *testing.T, depth, cpd int) (truth []TruthSpan, br map[uint64][]byte, dDepth map[uint64]int) {
	t.Helper()
	h := bridge.NewPathBridgeHandler(cpd, bridge.DefaultBloomFPRate)
	h.EmitDepth = true
	h.Capture = true

	const tid = uint64(0xfeed)
	n := uint64(1)<<(depth+1) - 1
	br = make(map[uint64][]byte)
	dDepth = make(map[uint64]int)

	spanDepth := func(i uint64) int {
		d := -1
		for ; i > 0; i >>= 1 {
			d++
		}
		return d
	}

	for i := uint64(1); i <= n; i++ {
		pid := i / 2
		truth = append(truth, TruthSpan{SpanID: i, ParentID: pid, Depth: spanDepth(i)})
		r := h.OnStart(&bridge.Event{TraceID: tid, SpanID: i, ParentID: pid, ServiceID: 1}, 0)
		if r.Payload != nil {
			br[i] = r.Payload
		}
	}
	for i := n; i >= 1; i-- {
		r := h.OnEnd(&bridge.Event{TraceID: tid, SpanID: i, ParentID: i / 2, ServiceID: 1})
		if r.Payload != nil {
			br[i] = r.Payload
		}
		if r.DepthBytes > 0 {
			dDepth[i] = r.Depth
		}
	}
	h.EvictTrace(tid)
	return truth, br, dDepth
}

func survivorsFrom(t *testing.T, truth []TruthSpan, br map[uint64][]byte, cfg Config, dropped map[uint64]struct{}) []Span {
	t.Helper()
	var out []Span
	for _, ts := range truth {
		if _, gone := dropped[ts.SpanID]; gone {
			continue
		}
		sp := Span{SpanID: ts.SpanID, ParentID: ts.ParentID, Depth: ts.Depth}
		if p, ok := br[ts.SpanID]; ok {
			d, bits, err := DecodePBPayload(p, cfg)
			if err != nil {
				t.Fatalf("decode span %d: %v", ts.SpanID, err)
			}
			if d != ts.Depth {
				t.Fatalf("span %d: payload depth %d != truth %d", ts.SpanID, d, ts.Depth)
			}
			sp.BloomBits = bits
		}
		out = append(out, sp)
	}
	return out
}

// TestPBDropAll drops every non-checkpoint span of a depth-5 binary tree at
// cpd 3 and checks full, exact reconstruction.
//
// Tree: 63 spans. Checkpoints (carry _br): d0 root, d3 (8), d5 leaves (32).
// Dropped (_d carriers): d1 (2), d2 (4), d4 (16) = 22 spans.
// Orphans: all d3 spans (parent at d2 dropped) and all d5 spans (parent at
// d4 dropped) = 40. Expected anchors: d3 -> root (gap 2), d5 -> its d3
// ancestor (gap 1). Synthetic total = 8*2 + 32*1 = 48.
func TestPBDropAll(t *testing.T) {
	cfg := NewConfig(3, bridge.DefaultBloomFPRate)
	cfg.ChainCheck = true
	truth, br, dDepth := buildTree(t, 5, 3)

	if len(br)+len(dDepth) != len(truth) {
		t.Fatalf("every span must carry _br or _d: br=%d _d=%d truth=%d", len(br), len(dDepth), len(truth))
	}

	dropped := make(map[uint64]struct{})
	for id := range dDepth {
		dropped[id] = struct{}{}
	}
	if len(dropped) != 22 {
		t.Fatalf("expected 22 droppable spans, got %d", len(dropped))
	}

	res := ReconstructPB(survivorsFrom(t, truth, br, cfg, dropped), cfg)
	sc := ScorePB(res, truth, dropped)

	want := Score{
		Spans: 63, Dropped: 22, Orphans: 40, Reconnected: 40,
		AnchorCorrect: 40, GapCorrect: 40, Misattached: 0, Unanchored: 0,
		Synthetic: 48,
	}
	if sc != want {
		t.Fatalf("score mismatch:\n got  %+v\n want %+v", sc, want)
	}

	// Spot-check one bridge of each kind.
	byOrphan := map[uint64]Bridge{}
	for _, b := range res.Bridges {
		byOrphan[b.OrphanID] = b
	}
	if b := byOrphan[8]; b.AnchorID != 1 || b.Synthetic != 2 { // depth-3 span 8 -> root
		t.Errorf("orphan 8: %+v, want anchor=1 synthetic=2", b)
	}
	// Leaf 32: chain 32 -> 16(d4, dropped) -> 8(d3, checkpoint survivor).
	// Anchor is 8 (= 32>>2) with one dropped span (16) in the hole.
	if b := byOrphan[32]; b.AnchorID != 8 || b.Synthetic != 1 {
		t.Errorf("orphan 32: %+v, want anchor=8 synthetic=1", b)
	}
}

// TestPBPartialDrop drops only the d2 level: orphans are the d3 spans, whose
// nearest surviving ancestors are the d1 spans (gap 1) — closer than the
// checkpoint window's lower bound, exercising "nearest survivor" rather than
// "jump to checkpoint".
func TestPBPartialDrop(t *testing.T) {
	cfg := NewConfig(3, bridge.DefaultBloomFPRate)
	cfg.ChainCheck = true
	truth, br, _ := buildTree(t, 5, 3)

	dropped := make(map[uint64]struct{})
	for _, ts := range truth {
		if ts.Depth == 2 {
			dropped[ts.SpanID] = struct{}{}
		}
	}

	res := ReconstructPB(survivorsFrom(t, truth, br, cfg, dropped), cfg)
	sc := ScorePB(res, truth, dropped)

	want := Score{
		Spans: 63, Dropped: 4, Orphans: 8, Reconnected: 8,
		AnchorCorrect: 8, GapCorrect: 8, Misattached: 0, Unanchored: 0,
		Synthetic: 8,
	}
	if sc != want {
		t.Fatalf("score mismatch:\n got  %+v\n want %+v", sc, want)
	}
	// d3 span 8 (ancestors 4,2,1): parent 4 dropped, anchor must be span 2 at d1.
	for _, b := range res.Bridges {
		if b.OrphanID == 8 && (b.AnchorID != 2 || b.Synthetic != 1) {
			t.Errorf("orphan 8: %+v, want anchor=2 synthetic=1", b)
		}
	}
}

// TestPBChainBorrowedBloom encodes the disconnected-middle scenario: the
// chain A(1,d0) -> B(2,d1) -> C(3,d2) -> D(4,d3) -> E(5,d4) at cpd 4, with B
// and D dropped. A and E are checkpoints (E's pre-reset bloom = {A..E}); C
// carries only _d and has NO surviving descendants — its only child D is
// gone, and E hangs off the dropped D. The in-fragment descendant search
// finds nothing for C; the membership fallback must discover that E's bloom
// contains C, borrow it, and anchor C to A. E anchors to C via its own
// bloom. Final graph: A -> synB -> C -> synD -> E.
// buildChain runs the PB handler over a linear chain of n spans (IDs 1..n,
// span i at depth i-1, parent i-1) at the given cpd. Returns truth and the
// captured _br payloads.
func buildChain(t *testing.T, n uint64, cpd int) ([]TruthSpan, map[uint64][]byte) {
	t.Helper()
	h := bridge.NewPathBridgeHandler(cpd, bridge.DefaultBloomFPRate)
	h.EmitDepth = true
	h.Capture = true

	const tid = uint64(0xc0ffee)
	var truth []TruthSpan
	br := make(map[uint64][]byte)
	for i := uint64(1); i <= n; i++ {
		pid := i - 1 // 0 for the root
		truth = append(truth, TruthSpan{SpanID: i, ParentID: pid, Depth: int(i - 1)})
		r := h.OnStart(&bridge.Event{TraceID: tid, SpanID: i, ParentID: pid, ServiceID: 1}, 0)
		if r.Payload != nil {
			br[i] = r.Payload
		}
	}
	for i := n; i >= 1; i-- {
		r := h.OnEnd(&bridge.Event{TraceID: tid, SpanID: i, ParentID: i - 1, ServiceID: 1})
		if r.Payload != nil {
			br[i] = r.Payload
		}
	}
	h.EvictTrace(tid)
	return truth, br
}

func TestPBChainBorrowedBloom(t *testing.T) {
	const cpd = 4
	cfg := NewConfig(cpd, bridge.DefaultBloomFPRate)
	cfg.ChainCheck = true
	truth, br := buildChain(t, 5, cpd)

	// A (depth 0) and E (depth 4) must be the only _br carriers.
	if _, ok := br[1]; !ok {
		t.Fatal("A should carry _br")
	}
	if _, ok := br[5]; !ok {
		t.Fatal("E should carry _br")
	}
	if len(br) != 2 {
		t.Fatalf("expected 2 carriers, got %d: %v", len(br), br)
	}

	dropped := map[uint64]struct{}{2: {}, 4: {}} // B and D

	res := ReconstructPB(survivorsFrom(t, truth, br, cfg, dropped), cfg)
	sc := ScorePB(res, truth, dropped)

	want := Score{
		Spans: 5, Dropped: 2, Orphans: 2, Reconnected: 2,
		AnchorCorrect: 2, GapCorrect: 2, Misattached: 0, Unanchored: 0,
		Synthetic: 2, Borrowed: 1,
	}
	if sc != want {
		t.Fatalf("score mismatch:\n got  %+v\n want %+v", sc, want)
	}

	byOrphan := map[uint64]Bridge{}
	for _, b := range res.Bridges {
		byOrphan[b.OrphanID] = b
	}
	// C (3): anchored to A (1) through E's borrowed bloom, synthesizing B.
	if b := byOrphan[3]; b.AnchorID != 1 || b.Synthetic != 1 || b.ViaCarrier != 5 {
		t.Errorf("orphan C: %+v, want anchor=1 synthetic=1 viaCarrier=5", b)
	}
	// E (5): anchored to C (3) via its own bloom, synthesizing D.
	if b := byOrphan[5]; b.AnchorID != 3 || b.Synthetic != 1 || b.ViaCarrier != 0 {
		t.Errorf("orphan E: %+v, want anchor=3 synthetic=1 viaCarrier=0", b)
	}
}

// TestPBNoDrop: zero drops means zero orphans and an all-zero score apart
// from span count.
func TestPBNoDrop(t *testing.T) {
	cfg := NewConfig(3, bridge.DefaultBloomFPRate)
	cfg.ChainCheck = true
	truth, br, _ := buildTree(t, 4, 3)

	res := ReconstructPB(survivorsFrom(t, truth, br, cfg, nil), cfg)
	sc := ScorePB(res, truth, nil)
	if sc.Orphans != 0 || sc.Synthetic != 0 || sc.Spans != 31 {
		t.Fatalf("unexpected score on intact trace: %+v", sc)
	}
}

// TestPBOrderParity: on the existing scenarios, bottom-up and independent
// processing must produce identical scores (they may differ only in how
// fallback blooms are discovered, never in the bridges chosen — given no
// adversarial FP pattern in these fixtures).
func TestPBOrderParity(t *testing.T) {
	cfgI := NewConfig(3, bridge.DefaultBloomFPRate)
	cfgB := cfgI
	cfgB.BottomUp = true

	truth, br, dDepth := buildTree(t, 5, 3)
	dropAll := make(map[uint64]struct{})
	for id := range dDepth {
		dropAll[id] = struct{}{}
	}
	d2only := make(map[uint64]struct{})
	for _, ts := range truth {
		if ts.Depth == 2 {
			d2only[ts.SpanID] = struct{}{}
		}
	}

	for name, dropped := range map[string]map[uint64]struct{}{"drop-all": dropAll, "d2-only": d2only} {
		scI := ScorePB(ReconstructPB(survivorsFrom(t, truth, br, cfgI, dropped), cfgI), truth, dropped)
		scB := ScorePB(ReconstructPB(survivorsFrom(t, truth, br, cfgB, dropped), cfgB), truth, dropped)
		if scI != scB {
			t.Errorf("%s: order changed the score:\n independent %+v\n bottom-up   %+v", name, scI, scB)
		}
	}

	// The chain scenario: bottom-up must find C's bloom through E's
	// reconstructed bridge (structural walk), independent through the scan —
	// same bridges either way.
	cfg4I := NewConfig(4, bridge.DefaultBloomFPRate)
	cfg4B := cfg4I
	cfg4B.BottomUp = true
	chainTruth, chainBR := buildChain(t, 5, 4)
	dropped := map[uint64]struct{}{2: {}, 4: {}}
	scI := ScorePB(ReconstructPB(survivorsFrom(t, chainTruth, chainBR, cfg4I, dropped), cfg4I), chainTruth, dropped)
	scB := ScorePB(ReconstructPB(survivorsFrom(t, chainTruth, chainBR, cfg4B, dropped), cfg4B), chainTruth, dropped)
	if scI != scB {
		t.Errorf("chain: order changed the score:\n independent %+v\n bottom-up   %+v", scI, scB)
	}
	if scI.Borrowed != 1 || scI.AnchorCorrect != 2 {
		t.Errorf("chain independent score unexpected: %+v", scI)
	}
}

func TestDecodePBPayloadErrors(t *testing.T) {
	cfg := NewConfig(3, bridge.DefaultBloomFPRate)
	if _, _, err := DecodePBPayload(nil, cfg); err == nil {
		t.Error("nil payload should fail")
	}
	if _, _, err := DecodePBPayload([]byte{9, 0, 0}, cfg); err == nil {
		t.Error("wrong type byte should fail")
	}
	if _, _, err := DecodePBPayload([]byte{1, 0, 0xde}, cfg); err == nil {
		t.Error("bloom length mismatch should fail")
	}
}
