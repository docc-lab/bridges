package recon

import (
	"sort"
	"testing"

	"bridges/bridge"
)

// tspan is one span of a synthetic ground-truth trace.
type tspan struct {
	id     uint64
	parent uint64
	start  int64
	end    int64
}

// runSBridge drives a synthetic trace through the real SBridgeHandler exactly
// as the simulator would (events in global sorted order, parentSeqNum = the
// child's 1-based start rank under its parent), capturing the actual serialized
// _br payload of every emitting span. Returns the reconstructor inputs.
func runSBridge(t *testing.T, traceID uint64, spans []tspan, cpd int) []SBInput {
	t.Helper()
	depth := map[uint64]int{}
	for _, s := range spans {
		if s.parent == 0 {
			depth[s.id] = 0
		}
	}
	// depths via repeated relaxation (parents precede children in id order? not
	// guaranteed, so loop to fixpoint).
	for changed := true; changed; {
		changed = false
		for _, s := range spans {
			if s.parent != 0 {
				if pd, ok := depth[s.parent]; ok {
					if _, done := depth[s.id]; !done {
						depth[s.id] = pd + 1
						changed = true
					}
				}
			}
		}
	}

	type ev struct {
		ts    int64
		end   bool // false = start
		depth int
		id    uint64
		par   uint64
	}
	var evs []ev
	for _, s := range spans {
		evs = append(evs,
			ev{s.start, false, depth[s.id], s.id, s.parent},
			ev{s.end, true, depth[s.id], s.id, s.parent})
	}
	sort.Slice(evs, func(i, j int) bool {
		a, b := evs[i], evs[j]
		if a.ts != b.ts {
			return a.ts < b.ts
		}
		if a.end != b.end {
			return !a.end // starts before ends
		}
		ar, br := a.depth, b.depth
		if a.end { // ends: deeper first
			ar, br = -ar, -br
		}
		if ar != br {
			return ar < br
		}
		return a.id < b.id
	})

	h := bridge.NewSBridgeHandler(cpd, nil)
	payloads := map[uint64][]byte{}
	h.EmitSink = func(_ /*tid*/, sid uint64, payload []byte) {
		payloads[sid] = append([]byte(nil), payload...)
	}
	nextSeq := map[uint64]int{}
	for _, e := range evs {
		event := &bridge.Event{TraceID: traceID, SpanID: e.id, ParentID: e.par, ServiceID: 0}
		if e.end {
			h.OnEnd(event)
			continue
		}
		seq := 0
		if e.par != 0 {
			seq = nextSeq[e.par] + 1
			nextSeq[e.par] = seq
		}
		h.OnStart(event, seq)
	}

	in := make([]SBInput, 0, len(payloads))
	for sid, p := range payloads {
		in = append(in, SBInput{SpanID: sid, Payload: p})
	}
	return in
}

// truthFromSpans builds ground truth: each parent's children indexed by their
// 1-based start-order rank (the same ordinal the handler assigns).
func truthFromSpans(spans []tspan) SBTruth {
	var root uint64
	bySpan := map[uint64]tspan{}
	kidsByParent := map[uint64][]tspan{}
	for _, s := range spans {
		bySpan[s.id] = s
		if s.parent == 0 {
			root = s.id
		} else {
			kidsByParent[s.parent] = append(kidsByParent[s.parent], s)
		}
	}
	childByOrd := map[uint64]map[int]uint64{}
	for parent, kids := range kidsByParent {
		sort.Slice(kids, func(i, j int) bool { return kids[i].start < kids[j].start })
		m := map[int]uint64{}
		for i, k := range kids {
			m[i+1] = k.id
		}
		childByOrd[parent] = m
	}
	return SBTruth{RootID: root, ChildByOrd: childByOrd}
}

// TestSBridgeReconstructNoDropSingleWindow proves the chains fully determine the
// tree (shape + fingerprints) with no drops and cpd larger than the depth (one
// window rooted at the depth-0 checkpoint; leaves emit, interior spans inferred).
func TestSBridgeReconstructNoDropSingleWindow(t *testing.T) {
	const traceID = 0xabcdef0123456789
	// ids chosen with distinct top-4/top-2 bytes so fp checks are meaningful.
	spans := []tspan{
		{id: 0x1111_0000_0000_0001, parent: 0, start: 0, end: 100},               // root (depth 0 checkpoint)
		{id: 0x2222_0000_0000_0002, parent: 0x1111_0000_0000_0001, start: 10, end: 40},  // A
		{id: 0x3333_0000_0000_0003, parent: 0x1111_0000_0000_0001, start: 50, end: 90},  // B
		{id: 0x4444_0000_0000_0004, parent: 0x2222_0000_0000_0002, start: 12, end: 20},  // A1 leaf
		{id: 0x5555_0000_0000_0005, parent: 0x2222_0000_0000_0002, start: 22, end: 38},  // A2 leaf
		{id: 0x6666_0000_0000_0006, parent: 0x3333_0000_0000_0003, start: 60, end: 80},  // B1 leaf
	}
	const cpd = 100

	inputs := runSBridge(t, traceID, spans, cpd)
	// Only checkpoints (root) + leaves (A1,A2,B1) emit.
	if len(inputs) != 4 {
		t.Fatalf("expected 4 emitting spans, got %d", len(inputs))
	}

	res := ReconstructSBridge(inputs, nil, Config{CPD: cpd})
	v := ScoreSBridge(res, truthFromSpans(spans))
	if !v.Correct {
		t.Fatalf("reconstruction not correct: unsolvable=%v reason=%q", v.Unsolvable, v.Reason)
	}
}

// TestSBridgeOrphanMatch feeds the interior survivor as an orphan and checks it
// uniquely matches its synthetic placeholder (own-fp + parent-fp + depth +
// ordinal), with zero ambiguity.
func TestSBridgeOrphanMatch(t *testing.T) {
	const traceID = 0x00000000cafef00d
	const (
		root = 0x1111_0000_0000_0001 // d0 ckpt
		A    = 0x2222_0000_0000_0002 // d1 interior (no emit) -> orphan
		A1   = 0x3333_0000_0000_0003 // d2 leaf
		A2   = 0x4444_0000_0000_0004 // d2 leaf
	)
	spans := []tspan{
		{id: root, parent: 0, start: 0, end: 100},
		{id: A, parent: root, start: 10, end: 90},
		{id: A1, parent: A, start: 20, end: 40},
		{id: A2, parent: A, start: 50, end: 80},
	}
	const cpd = 4

	inputs := runSBridge(t, traceID, spans, cpd) // emits root, A1, A2
	orphans := []SBOrphan{{SpanID: A, ParentID: root, Depth: 1, Ordinal: 1}}

	res := ReconstructSBridge(inputs, orphans, Config{CPD: cpd})
	if res.OrphanPlaced != 1 || res.OrphanAmbiguous != 0 || res.OrphanNoPlace != 0 {
		t.Fatalf("orphan stats placed=%d ambiguous=%d noplace=%d, want 1/0/0",
			res.OrphanPlaced, res.OrphanAmbiguous, res.OrphanNoPlace)
	}
	if !hasReal(res.Root, A) {
		t.Fatalf("orphan A should be identified with its placeholder node")
	}
	if v := ScoreSBridgeUnderDrop(res, truthFromSpans(spans)); !v.Correct {
		t.Fatalf("not correct: %q", v.Reason)
	}
}

// hasReal reports whether any reconstructed node carries the given span id.
func hasReal(n *SBNode, id uint64) bool {
	if n == nil {
		return false
	}
	if n.RealID == id {
		return true
	}
	for _, c := range n.Children {
		if hasReal(c, id) {
			return true
		}
	}
	return false
}

// TestSBridgeReconstructDropLeaf drops a non-checkpoint leaf and checks the rest
// still embeds correctly: no wrong edges (ScoreSBridgeUnderDrop Correct), the
// dropped leaf is absent, and its interior parent is still inferred from the
// surviving sibling's chain.
func TestSBridgeReconstructDropLeaf(t *testing.T) {
	const traceID = 0x00000000feedface
	const (
		root = 0x1111_0000_0000_0001 // d0 ckpt
		A    = 0x2222_0000_0000_0002 // d1 interior (no emit)
		A1   = 0x3333_0000_0000_0003 // d2 leaf
		A2   = 0x4444_0000_0000_0004 // d2 leaf  <- dropped
	)
	spans := []tspan{
		{id: root, parent: 0, start: 0, end: 100},
		{id: A, parent: root, start: 10, end: 90},
		{id: A1, parent: A, start: 20, end: 40},
		{id: A2, parent: A, start: 50, end: 80},
	}
	const cpd = 4 // only depth 0 is a checkpoint

	all := runSBridge(t, traceID, spans, cpd) // emits: root, A1, A2
	inputs := make([]SBInput, 0, len(all))
	for _, in := range all {
		if in.SpanID == A2 { // drop the non-checkpoint leaf
			continue
		}
		inputs = append(inputs, in)
	}

	res := ReconstructSBridge(inputs, nil, Config{CPD: cpd})
	v := ScoreSBridgeUnderDrop(res, truthFromSpans(spans))
	if !v.Correct {
		t.Fatalf("drop-leaf recon not correct: unsolvable=%v reason=%q", v.Unsolvable, v.Reason)
	}
	if hasReal(res.Root, A2) {
		t.Fatalf("dropped leaf A2 must be absent from the reconstruction")
	}
	if res.Root.RealID != root || !hasReal(res.Root, A1) {
		t.Fatalf("root + surviving leaf A1 should be reconstructed (A inferred between them)")
	}
}

// TestSBridgeReconstructNoDropMultiWindow uses cpd=2 over a depth-4 tree, so
// there are windows anchored at depths 0, 2 and 4 (checkpoints root, C, F, H).
// Exercises ckpt4 stitching, interior inferred nodes (A,D,G), checkpoint anchors
// that are also leaves (F,H), and a non-checkpoint leaf (L).
func TestSBridgeReconstructNoDropMultiWindow(t *testing.T) {
	const traceID = 0x0123456789abcdef
	const (
		root = 0x1111_1111_0000_0001 // d0 ckpt
		A    = 0x2222_2222_0000_0002 // d1
		C    = 0x3333_3333_0000_0003 // d2 ckpt (window anchor)
		D    = 0x4444_4444_0000_0004 // d3
		F    = 0x5555_5555_0000_0005 // d4 ckpt + leaf
		G    = 0x6666_6666_0000_0006 // d3
		H    = 0x7777_7777_0000_0007 // d4 ckpt + leaf
		L    = 0x8888_8888_0000_0008 // d3 leaf (non-ckpt)
	)
	spans := []tspan{
		{id: root, parent: 0, start: 0, end: 100},
		{id: A, parent: root, start: 5, end: 98},
		{id: C, parent: A, start: 8, end: 95},
		{id: D, parent: C, start: 12, end: 55},
		{id: F, parent: D, start: 15, end: 50},
		{id: G, parent: C, start: 60, end: 90},
		{id: H, parent: G, start: 62, end: 88},
		{id: L, parent: C, start: 92, end: 94},
	}
	const cpd = 2

	inputs := runSBridge(t, traceID, spans, cpd)
	// Emitting = checkpoints (root, C, F, H) + non-checkpoint leaves (L).
	// A, D, G are interior non-checkpoint spans with children -> no emit.
	if len(inputs) != 5 {
		t.Fatalf("expected 5 emitting spans, got %d", len(inputs))
	}

	res := ReconstructSBridge(inputs, nil, Config{CPD: cpd})
	v := ScoreSBridge(res, truthFromSpans(spans))
	if !v.Correct {
		t.Fatalf("reconstruction not correct: unsolvable=%v reason=%q", v.Unsolvable, v.Reason)
	}
}
