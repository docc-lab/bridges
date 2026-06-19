package recon

import (
	"reflect"
	"sort"
	"testing"

	"bridges/bridge"
)

// driveSBridgeCapture replays a synthetic trace through a fresh SBridgeHandler
// (same sorted-event / parentSeqNum driving as runSBridge) capturing both the
// emitted _br payloads and every DEE quad (via DEESink, the inline side-store).
func driveSBridgeCapture(t *testing.T, traceID uint64, spans []tspan, cpd int) (payloads, dees [][]byte) {
	t.Helper()
	depth := map[uint64]int{}
	for _, s := range spans {
		if s.parent == 0 {
			depth[s.id] = 0
		}
	}
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
		end   bool
		depth int
		id    uint64
		par   uint64
	}
	var evs []ev
	for _, s := range spans {
		evs = append(evs, ev{s.start, false, depth[s.id], s.id, s.parent}, ev{s.end, true, depth[s.id], s.id, s.parent})
	}
	sort.Slice(evs, func(i, j int) bool {
		a, b := evs[i], evs[j]
		if a.ts != b.ts {
			return a.ts < b.ts
		}
		if a.end != b.end {
			return !a.end
		}
		ar, br := a.depth, b.depth
		if a.end {
			ar, br = -ar, -br
		}
		if ar != br {
			return ar < br
		}
		return a.id < b.id
	})
	h := bridge.NewSBridgeHandler(cpd, nil)
	h.EmitOC = true
	h.EmitSink = func(_, _ uint64, p []byte) { payloads = append(payloads, append([]byte(nil), p...)) }
	h.DEESink = func(_ uint64, q []byte) { dees = append(dees, append([]byte(nil), q...)) }
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
	return payloads, dees
}

// TestAttributeDEE: two lost parents with COLLIDING 2-byte fps at the same
// depth. Fingerprint alone can't tell them apart, but the DEE contents can.
func TestAttributeDEE(t *testing.T) {
	const ownerFP = uint32(0xabcd1234) // top2 = 0xabcd
	p := DEECandidate{ID: 1, FP: 0xabcd, Depth: 3,
		ChildOrds: map[int]bool{1: true, 2: true, 3: true}, EE: map[int]bool{1: true}}
	q := DEECandidate{ID: 2, FP: 0xabcd, Depth: 3, // same fp, same depth (collision)
		ChildOrds: map[int]bool{1: true, 2: true}, EE: map[int]bool{1: true}}
	cands := []DEECandidate{p, q}

	// seq 2 is valid + unwitnessed for BOTH -> truncated-fp collision survives -> ambiguous
	if _, st := AttributeDEE(ownerFP, 3, []int{2}, cands, 16); st != DEEAmbiguous {
		t.Errorf("seqs=[2]: want ambiguous, got %v", st)
	}
	// seq 3 isn't a child of Q -> Q pruned -> unique P
	if idx, st := AttributeDEE(ownerFP, 3, []int{3}, cands, 16); st != DEEPlaced || cands[idx].ID != 1 {
		t.Errorf("seqs=[3]: want placed->P, got idx=%d st=%v", idx, st)
	}
	// seq 1 is already witnessed (EE) in both -> can't end twice -> no candidate
	if _, st := AttributeDEE(ownerFP, 3, []int{1}, cands, 16); st != DEENoPlace {
		t.Errorf("seqs=[1]: want noplace, got %v", st)
	}
	// wrong depth -> no candidate
	if _, st := AttributeDEE(ownerFP, 4, []int{2}, cands, 16); st != DEENoPlace {
		t.Errorf("wrong depth: want noplace, got %v", st)
	}
}

// TestGatherEndOrder proves the EE blocks + DEE faithfully encode a parent's
// children's end order. Root has three leaves that end out of start order
// (L1@20, L3@50, L2@90 -> end order ordinals [1,3,2]); we reassemble that purely
// from what the children carried + the DEE side-store.
func TestGatherEndOrder(t *testing.T) {
	const traceID = 0x00000000abcdef01
	const (
		root = 0x1111_0000_0000_0001
		l1   = 0x2222_0000_0000_0002 // ord1, ends first  (20)
		l2   = 0x3333_0000_0000_0003 // ord2, ends last   (90)
		l3   = 0x4444_0000_0000_0004 // ord3, ends middle (50)
	)
	spans := []tspan{
		{id: root, parent: 0, start: 0, end: 100},
		{id: l1, parent: root, start: 10, end: 20},
		{id: l2, parent: root, start: 30, end: 90},
		{id: l3, parent: root, start: 40, end: 50},
	}
	const cpd = 100 // single window: root is the only checkpoint, l1..l3 are leaves

	payloads, dees := driveSBridgeCapture(t, traceID, spans, cpd)

	// gather root's children (depth-1 chain levels) + their EE blocks
	var children []SBChild
	for _, p := range payloads {
		br, err := bridge.DecodeSBridgeBR(p, cpd, 16)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if len(br.Chain) == 0 {
			continue // root's own payload (no chain)
		}
		last := br.Chain[len(br.Chain)-1]
		if last.Depth == 1 { // a child of the root
			children = append(children, SBChild{Ord: last.Ord, EE: last.EE})
		}
	}
	// root's DEE from the side-store (owner-fp = top4(root), depth 0)
	var dee []int
	for _, q := range dees {
		qs, err := bridge.DecodeDEEQuads(q)
		if err != nil {
			t.Fatalf("decode dee: %v", err)
		}
		for _, dq := range qs {
			if dq.OwnerFP == uint32(root>>32) && dq.Depth == 0 {
				dee = append(dee, dq.Seqs...)
			}
		}
	}

	got := GatherEndOrder(children, dee)
	want := []int{1, 3, 2} // L1, L3, L2 by end time
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("end order = %v, want %v (children=%+v dee=%v)", got, want, children, dee)
	}
	if so := GatherStartOrder(children); !reflect.DeepEqual(so, []int{1, 2, 3}) {
		t.Fatalf("start order = %v, want [1 2 3]", so)
	}
}

// TestStructureScoring checks the order-vs-order critical-path scorer: the
// bottleneck is read off the recovered EndOrder, never off timestamps.
func TestStructureScoring(t *testing.T) {
	// ids: root=1 P=2 A=3 B=4 B1=5 C=6. P's recovered end-order is A,C,B (B last).
	b1 := &STNode{ID: 5, Real: true, Ord: 1, Children: map[int]*STNode{}}
	b := &STNode{ID: 4, Ord: 2, Children: map[int]*STNode{1: b1}, EndOrder: []int{1}}
	a := &STNode{ID: 3, Real: true, Ord: 1, Children: map[int]*STNode{}}
	c := &STNode{ID: 6, Real: true, Ord: 3, Children: map[int]*STNode{}}
	p := &STNode{ID: 2, Ord: 1, Children: map[int]*STNode{1: a, 2: b, 3: c}, EndOrder: []int{1, 3, 2}}
	root := &STNode{ID: 1, Real: true, Ord: 0, Children: map[int]*STNode{1: p}, EndOrder: []int{1}}

	// Truth where B ends last (agrees with EndOrder): bottleneck root->P->B->B1.
	te := map[uint64]int64{1: 100, 2: 91, 3: 20, 4: 91, 5: 40, 6: 90}
	if !CriticalPathMatch(root, te) {
		t.Error("critical path should match when EndOrder agrees with the true ends")
	}
	// Truth where C ends last: the recovered order (B last) now disagrees.
	teW := map[uint64]int64{1: 100, 2: 95, 3: 20, 4: 40, 5: 38, 6: 90}
	if CriticalPathMatch(root, teW) {
		t.Error("critical path should NOT match when the true bottleneck child differs")
	}
}

// TestReconstructStructure exercises both passes + reconcile:
//
//	Root(real)[0,100]
//	  P(reconstructed)            children A,B,C ; end order = A, C, B
//	    A(real)[10,20]  ord1
//	    B(reconstructed) ord2      child B1(real)[30,40]   -- must end LAST
//	    C(real)[50,90]  ord3
//
// B's only surviving descendant ends at 40, but the end order says B ends after
// C (90). So B's end must widen to 90+eps, and P (its parent) must widen to
// cover it — neither narrowing anything, survivors fixed.
func TestReconstructStructure(t *testing.T) {
	const eps = int64(1)
	b1 := &STNode{Real: true, Start: 30, End: 40, Ord: 1, Children: map[int]*STNode{}}
	b := &STNode{Ord: 2, Children: map[int]*STNode{1: b1}, EndOrder: []int{1}}
	a := &STNode{Real: true, Start: 10, End: 20, Ord: 1, Children: map[int]*STNode{}}
	c := &STNode{Real: true, Start: 50, End: 90, Ord: 3, Children: map[int]*STNode{}}
	p := &STNode{Ord: 1, Children: map[int]*STNode{1: a, 2: b, 3: c}, EndOrder: []int{1, 3, 2}}
	root := &STNode{Real: true, Start: 0, End: 100, Ord: 0, Children: map[int]*STNode{1: p}, EndOrder: []int{1}}

	ReconstructStructure(root, eps)

	// survivors never move
	for name, n := range map[string]*STNode{"A": a, "C": c, "B1": b1, "Root": root} {
		want := map[string][2]int64{"A": {10, 20}, "C": {50, 90}, "B1": {30, 40}, "Root": {0, 100}}[name]
		if n.Start != want[0] || n.End != want[1] {
			t.Errorf("survivor %s moved: got [%d,%d] want [%d,%d]", name, n.Start, n.End, want[0], want[1])
		}
	}
	// B's end widened to end after C (90+eps); start unchanged (already in order)
	if b.End != 91 || b.Start != 30 {
		t.Errorf("B = [%d,%d], want [30,91]", b.Start, b.End)
	}
	// P widened to cover B's pushed-out end; start unchanged
	if p.Start != 10 || p.End != 91 {
		t.Errorf("P = [%d,%d], want [10,91]", p.Start, p.End)
	}
	// orders hold: ends A<C<B, starts A<B<C
	if !(a.End < c.End && c.End < b.End) {
		t.Errorf("end order broken: A=%d C=%d B=%d", a.End, c.End, b.End)
	}
	if !(a.Start < b.Start && b.Start < c.Start) {
		t.Errorf("start order broken: A=%d B=%d C=%d", a.Start, b.Start, c.Start)
	}
}
